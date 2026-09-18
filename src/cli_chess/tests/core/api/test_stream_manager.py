import threading
import time
import pytest
import requests
from berserk.exceptions import ResponseError
from requests import Response as RequestsResponse

from cli_chess.core.api import stream_manager
from cli_chess.core.api.stream_manager import ConnectionState, ErrorCategory, ReconnectingStream
from cli_chess.core.api.stream_manager import classify_stream_error, is_terminal_game_status
from cli_chess.core.api.stream_manager import parse_retry_after


# ----------------------------------------------------------------------
# Test doubles
# ----------------------------------------------------------------------
class FakeResponse:
    """A stand-in for requests.Response whose read blocks until close()"""
    def __init__(self, events=(), *, idle_after_events=True, stale_events_on_close=()):
        self.events = list(events)
        self.stale_events_on_close = list(stale_events_on_close)
        self.idle_after_events = idle_after_events
        self.closed = threading.Event()
        self.iter_started = threading.Event()

    def close(self):
        self.closed.set()

    def __iter__(self):
        self.iter_started.set()
        for event in self.events:
            if self.closed.is_set():
                return
            yield event
        if self.idle_after_events:
            # Emulate a long-lived stream blocked waiting for the next line
            self.closed.wait(10)
        # Lines that "arrive from the socket" as it is being torn down.
        # A correct implementation must drop these via generation fencing.
        for event in self.stale_events_on_close:
            yield event


class ScriptedOpener:
    """Returns scripted responses/exceptions per generation (last item repeats)"""
    def __init__(self, script):
        self.script = script
        self.calls = 0
        self.lock = threading.Lock()

    def __call__(self, generation):
        with self.lock:
            item = self.script[min(self.calls, len(self.script) - 1)]
            self.calls += 1
        if isinstance(item, Exception):
            raise item
        return item, iter(item)


class FakeStream(ReconnectingStream):
    def __init__(self, opener, **kwargs):
        super().__init__(name="fake-stream", **kwargs)
        self._opener = opener
        self.received = []
        self.states_seen = []
        self.e_stream_state_changed.add_listener(self._record_state)

    def _open_stream(self, generation):
        return self._opener(generation)

    def _handle_event(self, generation, event):
        self.received.append((generation, event))

    def _record_state(self, state, *, message=""):
        self.states_seen.append((state, message))

    def wait_for_state(self, state, timeout=2.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(s is state for s, _ in self.states_seen):
                return True
            time.sleep(0.005)
        return False


@pytest.fixture(autouse=True)
def _no_jitter(monkeypatch):
    monkeypatch.setattr(stream_manager.random, "uniform", lambda a, b: 0.0)


def make_response_error(status_code, retry_after=None):
    response = RequestsResponse()
    response.status_code = status_code
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    return ResponseError(response)


# ----------------------------------------------------------------------
# Classification
# ----------------------------------------------------------------------
def test_classify_rate_limited_with_retry_after():
    category, retry_after = classify_stream_error(make_response_error(429, retry_after="7"))
    assert category is ErrorCategory.RATE_LIMITED
    assert retry_after == 7.0


def test_classify_auth_and_not_found_fatal():
    assert classify_stream_error(make_response_error(401))[0] is ErrorCategory.AUTH_FATAL
    assert classify_stream_error(make_response_error(403))[0] is ErrorCategory.AUTH_FATAL
    assert classify_stream_error(make_response_error(404))[0] is ErrorCategory.NOT_FOUND


def test_classify_server_and_network_transient():
    assert classify_stream_error(make_response_error(500))[0] is ErrorCategory.SERVER_TRANSIENT
    assert classify_stream_error(make_response_error(503))[0] is ErrorCategory.SERVER_TRANSIENT
    assert classify_stream_error(make_response_error(408))[0] is ErrorCategory.SERVER_TRANSIENT
    assert classify_stream_error(requests.ConnectionError("boom"))[0] is ErrorCategory.NETWORK_TRANSIENT
    assert classify_stream_error(ValueError("bad json"))[0] is ErrorCategory.OTHER_TRANSIENT
    assert classify_stream_error(make_response_error(400))[0] is ErrorCategory.FATAL


def test_parse_retry_after():
    assert parse_retry_after("42") == 42.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("Wed, 21 Oct 2099 07:28:00 GMT") is None
    assert parse_retry_after("garbage") is None


def test_terminal_game_status():
    assert is_terminal_game_status("mate")
    assert is_terminal_game_status("resign")
    assert not is_terminal_game_status("started")
    assert not is_terminal_game_status("created")
    assert not is_terminal_game_status(None)
    assert not is_terminal_game_status("")


# ----------------------------------------------------------------------
# Lifecycle
# ----------------------------------------------------------------------
def test_stream_goes_live_and_delivers_events():
    response = FakeResponse(["a", "b"])
    stream = FakeStream(ScriptedOpener([response]), eof_delay=100)
    stream.start()

    assert stream.wait_for_state(ConnectionState.LIVE)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and len(stream.received) < 2:
        time.sleep(0.005)
    assert stream.received == [(1, "a"), (1, "b")]
    assert stream.is_live()

    stream.cancel(timeout=2)
    assert not stream.is_alive()
    assert response.closed.is_set()


def test_clean_eof_reconnects_with_backoff():
    first = FakeResponse(["x"], idle_after_events=False)
    blocking = FakeResponse([])
    opener = ScriptedOpener([first, blocking])
    stream = FakeStream(opener, eof_delay=0.01)
    stream.start()

    assert stream.wait_for_state(ConnectionState.LIVE)
    assert blocking.iter_started.wait(2)
    assert opener.calls >= 2
    assert stream.received  # "x" delivered on the first generation

    # The EOF transition is a quiet recovery (empty message)
    assert any(s is ConnectionState.RECOVERING and m == "" for s, m in stream.states_seen)
    stream.cancel(timeout=2)


def test_network_error_triggers_backoff_then_resync():
    blocking = FakeResponse(["snapshot"])
    opener = ScriptedOpener([requests.ConnectionError("down"), blocking])
    stream = FakeStream(opener, initial_delay=0.01, eof_delay=0.01)
    stream.start()

    assert stream.wait_for_state(ConnectionState.RECOVERING)
    assert stream.wait_for_state(ConnectionState.LIVE)
    assert blocking.iter_started.wait(2)

    # The recovered event is generation fenced to the new session
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not stream.received:
        time.sleep(0.005)
    assert stream.received == [(2, "snapshot")]
    stream.cancel(timeout=2)


def test_auth_failure_stops_without_retries():
    opener = ScriptedOpener([make_response_error(401)])
    stream = FakeStream(opener, initial_delay=0.01)
    stream.start()
    stream.join(2)

    assert not stream.is_alive()
    assert opener.calls == 1
    assert stream.state is ConnectionState.AUTH_FAILURE
    assert any(s is ConnectionState.AUTH_FAILURE and m for s, m in stream.states_seen)


def test_429_honors_retry_after_and_reconnects():
    blocking = FakeResponse([])
    opener = ScriptedOpener([make_response_error(429, retry_after="0"), blocking])
    stream = FakeStream(opener)
    stream.start()

    assert stream.wait_for_state(ConnectionState.RATE_LIMITED)
    assert blocking.iter_started.wait(2)
    rate_states = [m for s, m in stream.states_seen if s is ConnectionState.RATE_LIMITED]
    assert rate_states and "0 seconds" in rate_states[0]
    assert stream.wait_for_state(ConnectionState.LIVE)
    stream.cancel(timeout=2)


def test_retry_budget_is_exhausted_and_stream_stops():
    opener = ScriptedOpener([requests.ConnectionError("down")])
    stream = FakeStream(opener, max_attempts=3, initial_delay=0.01, max_retry_window=0)
    start = time.monotonic()
    stream.start()
    stream.join(5)

    assert not stream.is_alive()
    assert opener.calls == 3
    assert stream.state is ConnectionState.RETRIES_EXHAUSTED
    assert time.monotonic() - start < 3
    assert any(s is ConnectionState.RETRIES_EXHAUSTED for s, _ in stream.states_seen)


def test_cancel_interrupts_backoff_wait_immediately():
    opener = ScriptedOpener([requests.ConnectionError("down")])
    stream = FakeStream(opener, max_attempts=50, initial_delay=30.0)
    stream.start()
    assert stream.wait_for_state(ConnectionState.RECOVERING)

    start = time.monotonic()
    stream.cancel(timeout=2)
    elapsed = time.monotonic() - start

    assert not stream.is_alive()
    assert elapsed < 1.0
    state_count = len(stream.states_seen)
    # No further state callbacks are emitted after cancellation
    time.sleep(0.05)
    assert len(stream.states_seen) == state_count


def test_events_from_cancelled_session_are_fenced_off():
    response = FakeResponse(stale_events_on_close=["stale1", "stale2"])
    stream = FakeStream(ScriptedOpener([response]), eof_delay=100)
    stream.start()

    assert response.iter_started.wait(2)
    stream.cancel(timeout=2)

    assert not stream.is_alive()
    assert stream.received == []  # late events from the dead session are dropped
    # State listeners are purged on cancel: a late notify must not reach the UI
    state_count = len(stream.states_seen)
    stream.e_stream_state_changed.notify(ConnectionState.LIVE, message="late")
    assert len(stream.states_seen) == state_count
