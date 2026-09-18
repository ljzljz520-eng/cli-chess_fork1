"""Unified lifecycle contract for long lived Lichess NDJSON streams.

The account event stream (IEM), the owned game state stream (GSD) and the TV
feed all share the same requirements:

* A single, observable connection state (connecting, live, recovering, rate
  limited, permanently down).
* Exponential backoff with jitter, honoring ``Retry-After`` on HTTP 429.
* Bounded cancellation. Backoff waits use an interruptible ``Event`` and the
  underlying HTTP response is closed from the cancelling thread so a blocked
  read unblocks immediately instead of waiting for the next line.
* Generation fencing. Every (re)connection gets a new generation. Events
  delivered by a superseded session and any callback attempted after
  cancellation are discarded.

Streams are opened through :func:`open_stream_response` (rather than the
berserk iterator helpers) so the ``requests.Response`` stays accessible and
can be closed to unblock a read.
"""
from __future__ import annotations

from enum import Enum, auto
from threading import Lock, Thread
from time import monotonic
from typing import Any, Callable, Iterator, Optional, Tuple
from urllib.parse import urljoin
import random
import threading

import requests
from berserk.exceptions import ApiError, ResponseError
from berserk.formats import FormatHandler, JSON

from cli_chess.utils.event import Event
from cli_chess.utils.logging import log

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 25

DEFAULT_INITIAL_DELAY = 1.0
DEFAULT_MAX_DELAY = 60.0
DEFAULT_BACKOFF_MULTIPLIER = 2.0
DEFAULT_JITTER_RATIO = 0.25
DEFAULT_RATE_LIMIT_DELAY = 60.0
DEFAULT_MAX_ATTEMPTS = 10
DEFAULT_MAX_RETRY_WINDOW = 300.0
DEFAULT_SHUTDOWN_JOIN_TIMEOUT = CONNECT_TIMEOUT + 2

_TERMINAL_GAME_STATUSES = ("started", "created")


class ConnectionState(Enum):
    STOPPED = auto()
    CONNECTING = auto()
    LIVE = auto()
    RECOVERING = auto()
    RATE_LIMITED = auto()
    AUTH_FAILURE = auto()
    NOT_FOUND = auto()
    RETRIES_EXHAUSTED = auto()


class ErrorCategory(Enum):
    RATE_LIMITED = auto()
    AUTH_FATAL = auto()
    NOT_FOUND = auto()
    SERVER_TRANSIENT = auto()
    NETWORK_TRANSIENT = auto()
    OTHER_TRANSIENT = auto()
    FATAL = auto()


# States from which the stream does not attempt further reconnects
PERMANENT_FAILURE_STATES = frozenset({
    ConnectionState.AUTH_FAILURE,
    ConnectionState.NOT_FOUND,
    ConnectionState.RETRIES_EXHAUSTED,
})


class StreamUnavailableError(Warning):
    """Raised when a command cannot be delivered because the stream is not live.
       Callers should surface this to the user and rely on stream
       resynchronization rather than retrying the command.
    """


def is_terminal_game_status(status: Optional[str]) -> bool:
    """Returns True if the passed in Lichess game status represents a finished game"""
    return bool(status) and status not in _TERMINAL_GAME_STATUSES


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Parses an RFC 7231 Retry-After header expressed in delta-seconds.
       HTTP-date values (and anything unparseable) return None.
    """
    if not value:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, seconds)


def classify_stream_error(exc: Exception) -> Tuple[ErrorCategory, Optional[float]]:
    """Classifies a stream exception into a category and an optional Retry-After
       delay (seconds). Fatal categories must not be retried.
    """
    if isinstance(exc, ResponseError):
        code = exc.status_code
        if code == 429:
            return ErrorCategory.RATE_LIMITED, parse_retry_after(exc.response.headers.get("Retry-After"))
        if code in (401, 403):
            return ErrorCategory.AUTH_FATAL, None
        if code == 404:
            return ErrorCategory.NOT_FOUND, None
        if code == 408 or code == 425 or 500 <= code < 600:
            return ErrorCategory.SERVER_TRANSIENT, None
        return ErrorCategory.FATAL, None

    if isinstance(exc, requests.RequestException):
        return ErrorCategory.NETWORK_TRANSIENT, None

    if isinstance(exc, ApiError):
        return ErrorCategory.NETWORK_TRANSIENT, None

    # Parsing/decoding and other unexpected errors are treated as transient; the
    # retry budget ensures a persistently broken stream eventually gives up.
    return ErrorCategory.OTHER_TRANSIENT, None


def open_stream_response(*, requestor, path: str, method: str = "GET",
                         fmt: Optional[FormatHandler] = None,
                         converter: Optional[Callable] = None,
                         params=None, data=None,
                         timeout: Tuple[float, Optional[float]] = (CONNECT_TIMEOUT, READ_TIMEOUT)
                         ) -> Tuple[requests.Response, Iterator]:
    """Opens an NDJSON stream using a berserk ``Requestor`` session while keeping
       a reference to the raw response (so reads can be interrupted with
       ``response.close()``). Raises ``berserk.exceptions.ResponseError`` on HTTP
       errors, mirroring berserk's own behavior.
    """
    fmt = fmt or JSON
    url = urljoin(requestor.base_url, path)
    response = requestor.session.request(
        method, url,
        stream=True,
        params=params,
        data=data,
        headers=fmt.headers,
        timeout=timeout,
    )

    if not response.ok:
        raise ResponseError(response)

    parser = fmt.parse_stream(response)
    stream: Iterator = map(converter, parser) if converter else parser
    return response, stream


class ReconnectingStream(Thread):
    """Base class providing the unified stream lifecycle contract.

       Subclasses implement :meth:`_open_stream` and :meth:`_handle_event`.
       Connection state changes are broadcast on ``e_stream_state_changed``
       as ``notify(state, message=...)``.
    """
    def __init__(self, name: str, *,
                 eof_delay: float = 1.0,
                 initial_delay: float = DEFAULT_INITIAL_DELAY,
                 max_delay: float = DEFAULT_MAX_DELAY,
                 multiplier: float = DEFAULT_BACKOFF_MULTIPLIER,
                 jitter_ratio: float = DEFAULT_JITTER_RATIO,
                 rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY,
                 max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 max_retry_window: float = DEFAULT_MAX_RETRY_WINDOW,
                 join_timeout: float = DEFAULT_SHUTDOWN_JOIN_TIMEOUT):
        super().__init__(daemon=True, name=name)
        self.stream_name = name
        self.eof_delay = eof_delay
        self._initial_delay = initial_delay
        self._max_delay = max_delay
        self._multiplier = multiplier
        self._jitter_ratio = jitter_ratio
        self._rate_limit_delay = rate_limit_delay
        self._max_attempts = max_attempts
        self._max_retry_window = max_retry_window
        self._join_timeout = join_timeout

        self.e_stream_state_changed = Event()
        self.state = ConnectionState.STOPPED

        self._cancel_event = threading.Event()
        self._generation_lock = Lock()
        self._generation = 0
        self._active_response: Optional[requests.Response] = None

        self._failure_count = 0
        self._window_started_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Hooks to implement
    # ------------------------------------------------------------------
    def _open_stream(self, generation: int) -> Tuple[Any, Iterator]:
        """Returns a ``(response, event_iterator)`` tuple for the current generation"""
        raise NotImplementedError

    def _handle_event(self, generation: int, event: Any) -> None:
        """Processes a single event received on the current generation"""
        raise NotImplementedError

    def _should_reconnect_after_eof(self) -> bool:
        """Whether a cleanly closed stream should be reopened. Defaults to True"""
        return True

    def _on_stream_live(self, generation: int) -> None:
        """Called once a new connection has been established (pre resync events)"""
        pass

    def _on_connection_state_changed(self, state: ConnectionState, message: str) -> None:
        """Called before ``e_stream_state_changed`` listeners are notified"""
        pass

    def _on_cancel(self) -> None:
        """Called from :meth:`cancel`; subclasses clear domain event listeners here"""
        pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def is_live(self) -> bool:
        """Returns True if the stream currently has an established connection"""
        return self.state is ConnectionState.LIVE and not self._cancel_event.is_set()

    def cancel(self, timeout: Optional[float] = None) -> None:
        """Cancels backoff waits, closes the active response and joins the thread
           within a bounded timeout. No UI callbacks are emitted afterwards.
        """
        self._cancel_event.set()

        with self._generation_lock:
            self._generation += 1
            response = self._active_response

        if response is not None:
            try:
                response.close()
            except Exception as e:
                log.debug(f"Error closing {self.stream_name} stream response: {e}")

        self.e_stream_state_changed.remove_all_listeners()
        self._on_cancel()
        self.state = ConnectionState.STOPPED

        # Never join the current thread (cancel can be invoked from a stream callback)
        if threading.current_thread() is not self and self.is_alive():
            self.join(timeout if timeout is not None else self._join_timeout)

    # ------------------------------------------------------------------
    # Thread entrypoint
    # ------------------------------------------------------------------
    def run(self) -> None:
        log.info(f"{self.stream_name} stream started")
        try:
            self._run_loop()
        except Exception:
            log.exception(f"Unhandled exception in {self.stream_name} stream")
        finally:
            # Terminal failure states must survive thread exit so observers can
            # surface actionable status; cancellation and clean shutdown report STOPPED.
            if self.state not in PERMANENT_FAILURE_STATES:
                self.state = ConnectionState.STOPPED
            log.info(f"{self.stream_name} stream stopped")

    def _run_loop(self) -> None:
        while not self._cancel_event.is_set():
            with self._generation_lock:
                self._generation += 1
                generation = self._generation

            response = None
            next_delay: Optional[float] = None
            fatal_state: Optional[ConnectionState] = None
            fatal_message = ""

            try:
                self._set_state(ConnectionState.CONNECTING)
                response, stream = self._open_stream(generation)

                with self._generation_lock:
                    self._active_response = response

                if self._is_stale(generation):
                    break

                self._reset_retry_budget()
                self._set_state(ConnectionState.LIVE)
                self._on_stream_live(generation)

                for event in stream:
                    if self._is_stale(generation):
                        break
                    self._handle_event(generation, event)

            except Exception as e:
                if self._is_stale(generation):
                    break

                category, retry_after = classify_stream_error(e)
                log.warning(f"{self.stream_name} stream error: {type(e).__name__}: {e} "
                            f"(category={category.name}, attempt={self._failure_count + 1})")

                if category is ErrorCategory.AUTH_FATAL or category is ErrorCategory.FATAL:
                    fatal_state = ConnectionState.AUTH_FAILURE
                    fatal_message = ("Lichess rejected this connection. Check that a valid API "
                                     "token is linked in Settings and try again.")
                elif category is ErrorCategory.NOT_FOUND:
                    fatal_state = ConnectionState.NOT_FOUND
                    fatal_message = "The requested Lichess resource no longer exists. Stream stopped."
                else:
                    self._register_failure()
                    if self._retry_budget_exhausted():
                        fatal_state = ConnectionState.RETRIES_EXHAUSTED
                        fatal_message = ("Unable to reconnect to Lichess after repeated attempts. "
                                         "Check your network connection, then return to the main "
                                         "menu and try again.")
                    elif category is ErrorCategory.RATE_LIMITED:
                        next_delay = retry_after if retry_after is not None else self._rate_limit_delay
                        message = (f"Rate limited by Lichess. Waiting {next_delay:.0f} "
                                   f"second{'s' if next_delay != 1 else ''} before reconnecting.")
                        self._set_state(ConnectionState.RATE_LIMITED, message)
                    else:
                        next_delay = self._compute_backoff()
                        attempts_left = max(0, self._max_attempts - self._failure_count)
                        message = (f"Connection to Lichess lost. Reconnecting in {next_delay:.0f} "
                                   f"second{'s' if next_delay != 1 else ''} ({attempts_left} "
                                   f"attempt{'s' if attempts_left != 1 else ''} remaining).")
                        self._set_state(ConnectionState.RECOVERING, message)

            else:
                # The iterator was exhausted without an exception (clean EOF)
                if self._is_stale(generation) or not self._should_reconnect_after_eof():
                    break
                next_delay = self.eof_delay
                self._set_state(ConnectionState.RECOVERING, "")

            finally:
                with self._generation_lock:
                    if self._active_response is response:
                        self._active_response = None
                if response is not None:
                    try:
                        response.close()
                    except Exception as e:
                        log.debug(f"Error closing {self.stream_name} response: {e}")

            if self._cancel_event.is_set():
                break

            if fatal_state is not None:
                self._set_state(fatal_state, fatal_message)
                break

            if next_delay is not None and self._cancel_event.wait(next_delay):
                break

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _is_stale(self, generation: int) -> bool:
        return self._cancel_event.is_set() or self._generation != generation

    def _set_state(self, state: ConnectionState, message: str = "") -> None:
        if self._cancel_event.is_set():
            return
        self.state = state
        log.debug(f"{self.stream_name} state -> {state.name} // {message}")
        try:
            self._on_connection_state_changed(state, message)
        except Exception:
            log.exception(f"Error in {self.stream_name} state change listener")
        self.e_stream_state_changed.notify(state, message=message)

    def _register_failure(self) -> None:
        if self._window_started_at is None:
            self._window_started_at = monotonic()
        self._failure_count += 1

    def _retry_budget_exhausted(self) -> bool:
        if self._failure_count >= self._max_attempts:
            return True
        if self._max_retry_window and self._window_started_at is not None:
            if monotonic() - self._window_started_at >= self._max_retry_window:
                return True
        return False

    def _reset_retry_budget(self) -> None:
        self._failure_count = 0
        self._window_started_at = None

    def _compute_backoff(self) -> float:
        delay = min(
            self._max_delay,
            self._initial_delay * (self._multiplier ** max(0, self._failure_count - 1))
        )
        if self._jitter_ratio:
            delay = max(0.0, delay * (1 + random.uniform(-self._jitter_ratio, self._jitter_ratio)))
        return delay
