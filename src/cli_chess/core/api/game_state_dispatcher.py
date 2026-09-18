from berserk import models
from berserk.exceptions import ApiError, ResponseError
from cli_chess.core.api.stream_manager import ReconnectingStream, StreamUnavailableError
from cli_chess.core.api.stream_manager import is_terminal_game_status, open_stream_response
from cli_chess.utils import Event, EventTopics, log
from typing import Callable
from enum import Enum, auto
from types import MappingProxyType
import requests


class GSDEventTopics(Enum):
    OPPONENT_GONE = auto()
    NOT_IMPLEMENTED = auto()


gsd_type_to_event_dict = MappingProxyType({
    "gameFull": EventTopics.GAME_START,
    "gameState": EventTopics.MOVE_MADE,
    "chatLine": EventTopics.CHAT_RECEIVED,
    "opponentGone": GSDEventTopics.OPPONENT_GONE,
})


class GameStateDispatcher(ReconnectingStream):
    """Handles streaming a game and sending game commands (make move, offer draw, etc)
       using the Board API. The game that is streamed using this class must be owned
       by the account linked to the api token.

       The stream reconnects automatically with backoff. Every reconnection starts
       with a ``gameFull`` snapshot which is replayed by the model to resynchronize
       board move order, side to move, clocks and terminal state. Events are
       generation fenced, so data from a superseded session can never be applied.
    """
    def __init__(self, game_id=""):
        super().__init__(name=f"lichess-game-state-{game_id}", eof_delay=1.0)
        self.game_id = game_id
        self.is_game_over = False
        self.e_game_state_dispatcher_event = Event()

        try:
            from cli_chess.core.api.api_manager import api_client
            self.api_client = api_client
        except ImportError:
            # TODO: Clean this up so the error is displayed on the main screen
            log.error("Failed to import api_client")
            raise ImportError("API client not setup. Do you have an API token linked?")

    def _open_stream(self, generation: int):
        """Opens (or reopens) the board game state stream for this game"""
        return open_stream_response(
            requestor=self.api_client.board._r,  # noqa
            path=f"/api/board/game/stream/{self.game_id}",
            converter=models.GameState.convert,
        )

    def _handle_event(self, generation: int, event: dict) -> None:
        """Entrypoint for events received on the current generation.
           Listeners (typically the OnlineGameModel) are only notified while
           this generation is still authoritative.
        """
        event_topic = gsd_type_to_event_dict.get(event.get('type'), GSDEventTopics.NOT_IMPLEMENTED)
        log.debug(f"GSD Stream event type received: {event.get('type')} // topic: {event_topic}")

        if event_topic is EventTopics.GAME_START:
            # gameFull is the authoritative snapshot. It is sent both on the initial
            # connection and after every reconnect, so replaying it always leaves the
            # model consistent with the server (resync after a mid half-move drop).
            self.e_game_state_dispatcher_event.notify(event_topic, data=event)

            snapshot = event.get('state', {})
            if is_terminal_game_status(snapshot.get('status')):
                # The game finished while the stream was down. Apply the terminal
                # snapshot so the model ends in the server's state exactly once.
                self.is_game_over = True
                self.e_game_state_dispatcher_event.notify(EventTopics.MOVE_MADE, EventTopics.GAME_END, data=snapshot)
                self._game_ended()
            return

        if event_topic is EventTopics.MOVE_MADE:
            status = event.get('status', None)
            self.is_game_over = is_terminal_game_status(status)

        elif event_topic is GSDEventTopics.OPPONENT_GONE:
            is_gone = event.get('gone', False)
            secs_until_claim = event.get('claimWinInSeconds', None)

            if is_gone and secs_until_claim:
                pass  # TODO implement call to auto-claim win when `secs_until_claim` elapses

            if not is_gone:
                pass  # TODO: Cancel auto-claim countdown
        elif event_topic is EventTopics.CHAT_RECEIVED:
            pass

        game_end_event = EventTopics.GAME_END if self.is_game_over else None
        self.e_game_state_dispatcher_event.notify(event_topic, game_end_event, data=event)

        if self.is_game_over:
            self._game_ended()

    def _should_reconnect_after_eof(self) -> bool:
        """A finished game never needs to be re-streamed; any other EOF reconnects"""
        return not self.is_game_over

    def _on_cancel(self) -> None:
        self.e_game_state_dispatcher_event.remove_all_listeners()

    def make_move(self, move: str) -> None:
        """Sends the move to lichess. This move should have already
           been verified as valid in the current context of the board.
           The move must be in UCI format.

           A single attempt is made (no blind retries): since POSTs are not
           idempotent, retrying could duplicate side effects (e.g. accepting a
           draw that arrived between attempts, or a duplicate chat message). The
           authoritative stream reconciles state on failure.
        """
        log.debug(f"Sending move ({move}) to lichess")
        self._send_command(lambda: self.api_client.board.make_move(self.game_id, move))

    def send_takeback_request(self) -> None:
        """Sends a takeback request to our opponent"""
        log.debug("Sending takeback offer to opponent")
        self._send_command(lambda: self.api_client.board.offer_takeback(self.game_id))

    def send_draw_offer(self) -> None:
        """Sends a draw offer to our opponent"""
        log.debug("Sending draw offer to opponent")
        self._send_command(lambda: self.api_client.board.offer_draw(self.game_id))

    def resign(self) -> None:
        """Resigns the game"""
        log.debug("Sending resignation")
        self._send_command(lambda: self.api_client.board.resign_game(self.game_id))

    def post_message(self, text: str) -> None:
        """Send message to our opponent"""
        log.debug("Sending message to opponent")
        self._send_command(lambda: self.api_client.board.post_message(self.game_id, text))

    def claim_victory(self) -> None:
        """Submits a claim of victory to lichess as the opponent is gone.
           This is to only be called when the opponentGone timer has elapsed.
        """
        pass

    def _send_command(self, send: Callable) -> None:
        """Runs a single board API POST, enforcing the live connection contract.
           Raises StreamUnavailableError when the stream is not live (caller
           rejects/queues the command) or when Lichess cannot be reached (the
           stream resync reconciles authoritative state without a retry).
        """
        if not self.is_live():
            raise StreamUnavailableError(
                "Not connected to Lichess. Command not sent; waiting for the live connection to resume."
            )

        try:
            send()
        except StreamUnavailableError:
            raise
        except ResponseError as e:
            if e.status_code == 429:
                raise Warning("Rate limited by Lichess. Wait a moment and try again.")
            if 500 <= e.status_code < 600:
                raise StreamUnavailableError(
                    "Lichess is temporarily unavailable. The game will resynchronize automatically; "
                    "wait for the live connection before sending another command."
                )
            raise Warning(str(e))
        except (ApiError, requests.RequestException) as e:
            log.warning(f"Board command could not be delivered: {e}")
            raise StreamUnavailableError(
                "Couldn't reach Lichess. The game will resynchronize automatically; "
                "do not repeat this command until reconnected."
            )

    def _game_ended(self) -> None:
        """Handles removing all event listeners since the game has completed"""
        log.info("GAME ENDED: Removing existing GSD listeners")
        self.is_game_over = True
        self.e_game_state_dispatcher_event.remove_all_listeners()

    def add_event_listener(self, listener: Callable) -> None:
        """Subscribes the passed in method to GSD events"""
        self.e_game_state_dispatcher_event.add_listener(listener)

    def unsubscribe_from_events(self, listener: Callable) -> None:
        """Unsubscribes the passed in method from GSD events"""
        self.e_game_state_dispatcher_event.remove_listener(listener)
