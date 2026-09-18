from cli_chess.core.game import PlayableGameModelBase
from cli_chess.core.game.game_options import GameOption
from cli_chess.core.api import GameStateDispatcher
from cli_chess.core.api.incoming_event_manger import IEMEventTopics
from cli_chess.core.api.stream_manager import ConnectionState, PERMANENT_FAILURE_STATES
from cli_chess.core.api.stream_manager import StreamUnavailableError, open_stream_response
from cli_chess.utils import log, threaded, RequestSuccessfullySent, EventTopics
from chess import COLORS, COLOR_NAMES, WHITE, BLACK, Color
from berserk.formats import TEXT
from enum import Enum, auto
from typing import Optional, Dict
from cli_chess.modules.chat import ChatModel

RECOVERY_DEFAULT_MESSAGE = ("Connection to Lichess lost. Reconnecting and resynchronizing... "
                            "Moves, chat, draw offers and resign are paused until the game is live again.")


class EventSender(Enum):
    LOCAL = auto()
    FROM_IEM = auto()
    FROM_GSD = auto()


class OnlineGameModel(PlayableGameModelBase):
    """This model must only be used for games owned by the linked lichess user.
       Games not owned by this account must directly use the base model instead.
    """
    def __init__(self, game_parameters: dict, is_vs_ai: bool):
        super().__init__(play_as_color=game_parameters[GameOption.COLOR], variant=game_parameters[GameOption.VARIANT], fen=None, side_confirmed=is_vs_ai)  # noqa: E501
        self.vs_ai = is_vs_ai
        self.playing_game_id = None
        self.searching = False
        self.vs_opponent = game_parameters.get(GameOption.OPPONENT)
        self.challenge_color = game_parameters.get(GameOption.COLOR)
        self.sent_challenge_id = None
        self._update_game_metadata(EventTopics.GAME_PARAMS, sender=EventSender.LOCAL, data=game_parameters)
        self.game_state_dispatcher = Optional[GameStateDispatcher]

        # Connection/resync contract with the game state stream
        self._gsd_live = False                # True only once an authoritative snapshot is applied
        self._gsd_synced_once = False         # False until the first gameFull of this game
        self._game_over_reported = False      # Guards duplicate game over/PGN side effects
        self._seek_response = None            # Active seek response (closed to unblock on exit)

        self.chat_model = ChatModel()

        try:
            from cli_chess.core.api.api_manager import api_client, api_iem
            self.api_iem = api_iem
            self.api_client = api_client
        except ImportError:
            # TODO: Clean this up so the error is displayed on the main screen
            log.error("Failed to import api_iem and api_client")
            raise ImportError("API client not setup. Do you have an API token linked?")

    @threaded
    def create_game(self) -> None:
        """Sends a request to lichess to start a game using the selected game parameters. Depending
           on the parameters this will either challenge the Lichess AI (stockfish), send a challenge
           to a specific player, or create a seek against a random opponent.
        """
        try:
            # Note: Only subscribe to IEM events right before creating challenge to lessen chance of grabbing another game
            self.api_iem.add_event_listener(self._handle_iem_event)
            self._notify_game_model_updated(EventTopics.GAME_SEARCH)
            self.searching = True

            if self.vs_ai:  # Challenge Lichess AI (stockfish)
                self.api_client.challenges.create_ai(level=self.game_metadata.players[not self.my_color].ai_level,
                                                     clock_limit=self.game_metadata.clocks[WHITE].time * 60,  # challenges need time in seconds
                                                     clock_increment=self.game_metadata.clocks[WHITE].increment,
                                                     color=COLOR_NAMES[self.my_color],
                                                     variant=self.game_metadata.variant)
            elif self.vs_opponent:  # Challenge a specific player
                response = self.api_client.challenges.create(username=self.vs_opponent,
                                                             rated=self.game_metadata.rated,
                                                             clock_limit=self.game_metadata.clocks[WHITE].time * 60,
                                                             clock_increment=self.game_metadata.clocks[WHITE].increment,
                                                             color=self.challenge_color,
                                                             variant=self.game_metadata.variant)
                self.sent_challenge_id = response.get('id')
                self._notify_game_model_updated(EventTopics.GAME_SEARCH, msg=f"Challenge sent to {self.vs_opponent}. Waiting for a response...")
            else:  # Find a random opponent
                payload = {
                    "rated": str(self.game_metadata.rated).lower(),
                    "time": self.game_metadata.clocks[WHITE].time,
                    "increment": self.game_metadata.clocks[WHITE].increment,
                    "variant": self.game_metadata.variant,
                    "color": "random",  # lila PR# 15969
                    "ratingRange": "",
                }
                # The seek hangs until an opponent is found. Keep the response reference so
                # exiting/cancelling while searching unblocks the read immediately.
                response, stream = open_stream_response(
                    method="POST",
                    requestor=self.api_client.board._r,  # noqa
                    path="/api/board/seek",
                    fmt=TEXT,
                    data=payload,
                    timeout=(10, None),
                )
                self._seek_response = response
                try:
                    for _ in stream:
                        if not self.searching:
                            break
                finally:
                    self._seek_response = None
                    try:
                        response.close()
                    except Exception as e:
                        log.debug(f"Error closing seek response: {e}")
        except Exception as e:
            # Since this exception happened in a thread, notify via the model event instead of raising
            self.searching = False
            msg = f"Error creating online game: {e}"
            log.error(msg)
            self._notify_game_model_updated(EventTopics.ERROR, msg=msg)

    def _start_game(self, game_id: str) -> None:
        """Called when a game is started. Sets proper class variables
           and starts and registers game stream event callback
        """
        if game_id and not self.game_in_progress:
            self._notify_game_model_updated(EventTopics.GAME_START)
            self.game_in_progress = True
            self.searching = False
            self.playing_game_id = game_id
            self._gsd_live = False
            self._gsd_synced_once = False
            self._game_over_reported = False

            self.game_state_dispatcher = GameStateDispatcher(game_id)
            self.game_state_dispatcher.add_event_listener(self._handle_gsd_event)
            self.game_state_dispatcher.e_stream_state_changed.add_listener(self._handle_gsd_connection_state)
            self.game_state_dispatcher.start()

    def _game_end(self) -> None:
        """The game we are playing has ended. Handle cleaning up."""
        self.game_in_progress = False
        self.searching = False
        self.playing_game_id = None
        self._gsd_live = False
        self._close_seek_response()
        try:
            self.api_iem.unsubscribe_from_events(self._handle_iem_event)
        except Exception as e:
            log.debug(f"Unable to unsubscribe from IEM events: {e}")

    def _close_seek_response(self) -> None:
        """Closes any in progress seek response so a blocked read unblocks immediately"""
        seek_response = self._seek_response
        self._seek_response = None
        if seek_response is not None:
            try:
                seek_response.close()
            except Exception as e:
                log.debug(f"Error closing seek response: {e}")

    def _reject_if_stream_not_live(self) -> None:
        """Commands (move, chat, draw, resign, takeback) are only accepted against a
           live, synchronized stream. During recovery they are explicitly rejected
           so they cannot be lost or applied twice after reconnection.
        """
        if not self._gsd_live:
            raise StreamUnavailableError(
                "Not connected to Lichess. Waiting for live game synchronization... "
                "command not sent."
            )

    def make_move(self, move: str):
        """Sends the move to the board model for a validity check. If valid this
           function will pass the move over to the game state dispatcher to be sent
           Raises an exception on move or API errors.
        """
        if self.game_in_progress:
            self._reject_if_stream_not_live()
            try:
                move = move.strip()
                if not move:
                    raise Warning("No move specified")

                if move == "0000":
                    raise Warning("Null moves are not supported in online games")

                move = self.board_model.verify_move(move)
                self.game_state_dispatcher.make_move(move)
            except Exception:
                raise
        else:
            log.warning("Attempted to make a move in a game that's not in progress")
            if self.searching:
                raise Warning("Still searching for opponent")
            else:
                raise Warning("Game has already ended")

    def set_premove(self, move: str) -> None:
        """Sets the premove. Raises an exception on an invalid premove.
           Premoves are rejected during recovery so a stale move can never
           be applied against a resynchronized position.
        """
        if self.game_in_progress and move and not self.is_my_turn():
            self._reject_if_stream_not_live()
            if move == "0000":
                raise Warning("Null moves are not supported in online games")
            self.premove_model.set_premove(move)

    def propose_takeback(self) -> None:
        """Notifies the game state dispatcher to propose a takeback"""
        if self.game_in_progress:
            self._reject_if_stream_not_live()
            try:
                if len(self.board_model.get_move_stack()) < 2:
                    raise Warning("Cannot send takeback with less than two moves")

                self.premove_model.clear_premove()
                self.game_state_dispatcher.send_takeback_request()

                if not self.vs_ai:
                    raise RequestSuccessfullySent("Takeback request sent")
            except Exception:
                raise
        else:
            log.warning("Attempted to propose a takeback in a game that's not in progress")
            if self.searching:
                raise Warning("Still searching for opponent")
            else:
                raise Warning("Game has already ended")

    def offer_draw(self) -> None:
        """Notifies the game state dispatcher to offer a draw"""
        if self.game_in_progress:
            self._reject_if_stream_not_live()
            if self.vs_ai:
                raise Warning("Lichess AI does not accept draw offers")

            try:
                self.game_state_dispatcher.send_draw_offer()
                raise RequestSuccessfullySent("Draw offer sent")
            except Exception:
                raise
        else:
            log.warning("Attempted to offer a draw to a game that's not in progress")
            if self.searching:
                raise Warning("Still searching for opponent")
            else:
                raise Warning("Game has already ended")

    def resign(self) -> None:
        """Notifies the game state dispatcher to resign the game"""
        if self.game_in_progress:
            self._reject_if_stream_not_live()
            try:
                self.game_state_dispatcher.resign()
            except Exception:
                raise
        else:
            log.warning("Attempted to resign a game that's not in progress")
            if self.searching:
                raise Warning("Still searching for opponent")
            else:
                raise Warning("Game has already ended")

    def post_message(self, text: str):
        """Send message to opponent"""
        if self.game_in_progress:
            self._reject_if_stream_not_live()
            try:
                self.game_state_dispatcher.post_message(text)
            except Exception:
                raise
        else:
            log.warning("Attempted to chat in a game that's not in progress")
            if self.searching:
                raise Warning("Still searching for opponent")
            else:
                raise Warning("Unable to chat as the game has ended")

    def _handle_iem_event(self, *args, data: Optional[Dict] = None) -> None:
        """Handles events received from the IncomingEventManager. NOTE: Events coming in
           are global to the account, therefore multiple game start/end events can come in based
           on how many games are being played. Updates to game_metadata should only happen for
           the game actively being played.
        """
        if not data:
            return
        try:
            if EventTopics.GAME_START in args:
                # TODO: There has to be a better way to ensure this is the right game...
                #  add some further specific clauses like color, time control, date, etc?
                if not self.game_in_progress and not data.get('hasMoved') and data.get('compat', {}).get('board'):
                    self._update_game_metadata(*args, sender=EventSender.FROM_IEM, data=data)
                    self._start_game(data.get('gameId'))

            elif EventTopics.GAME_END in args:
                if self.game_in_progress and self.playing_game_id == data.get('gameId'):
                    # The game must be marked as ended before listeners are notified, otherwise
                    # the game over is presented while the game still reads as in progress
                    self._game_end()
                    self._update_game_metadata(*args, sender=EventSender.FROM_IEM, data=data)

            elif IEMEventTopics.CHALLENGE_DECLINED in args:
                if self.searching and data.get('id') == self.sent_challenge_id:
                    self.searching = False
                    self.sent_challenge_id = None
                    self._notify_game_model_updated(EventTopics.ERROR, msg=data.get('declineReason', f"{self.vs_opponent} declined the challenge"))  # noqa: E501

            elif IEMEventTopics.CHALLENGE_CANCELLED in args:
                if self.searching and data.get('id') == self.sent_challenge_id:
                    self.searching = False
                    self.sent_challenge_id = None
                    self._notify_game_model_updated(EventTopics.ERROR, msg="The challenge has been cancelled")
        except Exception as e:
            log.error(f"Error handling IncomingEventManager event: {e}")
            raise

    def _handle_gsd_connection_state(self, state: ConnectionState, *, message: str = "") -> None:
        """Listens to the game state stream connection lifecycle. While the stream
           is recovering, commands are gated and a status alert is shown; permanent
           failures surface an actionable message instead of leaving the game live.
        """
        if state in (ConnectionState.RECOVERING, ConnectionState.RATE_LIMITED):
            if self.game_in_progress and self._gsd_live:
                self._gsd_live = False
                self.premove_model.clear_premove()
                self._notify_game_model_updated(EventTopics.ERROR, msg=message or RECOVERY_DEFAULT_MESSAGE)
            elif self.game_in_progress:
                # Initial connection attempts before the first snapshot
                self._gsd_live = False

        elif state in PERMANENT_FAILURE_STATES and self.game_in_progress:
            self._gsd_live = False
            self.premove_model.clear_premove()
            self._notify_game_model_updated(EventTopics.ERROR, msg=message or RECOVERY_DEFAULT_MESSAGE)

    def _handle_gsd_event(self, *args, data: Optional[Dict] = None) -> None:
        """Handles received from the GameStateDispatcher. Incoming events are
           specific to this game being played. ``gameFull`` is treated as the
           authoritative snapshot both at game start and after any reconnect,
           leaving move order, side to move, clocks and terminal state exactly
           as the server reports them.
        """
        if not data:
            return
        reconnecting = False
        try:
            if EventTopics.GAME_START in args:
                # This runs at start and on every reconnect (resync). The board is
                # reset to the initial FEN and the full move list is replayed, so a
                # drop at any point in a half-move cannot duplicate or lose a ply.
                reconnecting = self._gsd_synced_once and not self._gsd_live
                self.board_model.reinitialize_board(variant=self.game_metadata.variant,
                                                    orientation=(self.my_color if self.board_model.get_variant_name() != "racingkings" else WHITE),
                                                    fen=data.get('initialFen', ""))
                self.board_model.make_moves_from_list(data.get('state', {}).get('moves', '').split())
                self._gsd_live = True
                self._gsd_synced_once = True

            elif EventTopics.MOVE_MADE in args:
                # TODO: Take some time measurements to see how much of an impact this approach is
                # Resetting and replaying the moves guarantees the game between lichess
                # and our local board are in sync (eg. takebacks, moves played on website, etc)
                self.board_model.reset(notify=False)
                self.board_model.make_moves_from_list(data.get('moves', []).split())

                if self.is_my_turn():
                    premove = self.premove_model.pop_premove()
                    try:
                        if premove:
                            self.make_move(premove)
                    except Exception as e:
                        if isinstance(e, ValueError):
                            log.debug(f"The premove set was invalid in the new context, skipping: {e}")
                        else:
                            log.exception(e)

                if EventTopics.GAME_END in args:
                    self._report_game_over(status=data.get('status'), winner=data.get('winner', ""))

            elif EventTopics.CHAT_RECEIVED in args:
                username = data.get("username", "?")
                text = data.get("text", "")
                self.chat_model.add_message(username, text)

            self._update_game_metadata(*args, sender=EventSender.FROM_GSD, data=data)

            if reconnecting:
                # Sent after the GAME_START snapshot notification (which clears any
                # stale recovery alert) so the resync confirmation stays visible.
                self._notify_game_model_updated(
                    EventTopics.GAME_SEARCH,
                    msg="Reconnected to Lichess. Board, clocks and game state synchronized."
                )
        except Exception as e:
            log.error(f"Error handling GameStateDispatcher event: {e}")
            raise

    def _start_clock_on_side_to_move(self) -> None:
        """Starts the clock of the side to move. Lichess does not charge either
           player until they have both made their first move.
        """
        if len(self.board_model.get_move_stack()) >= 2:
            self.game_metadata.set_clock_ticking(self.board_model.get_turn())

    def _update_game_metadata(self, *args, sender: Optional[EventSender] = None, data: Optional[Dict] = None, **kwargs) -> None:
        """Parses and saves the data of the game being played. Events come from senders
           in differing formats, which is why they are separated
        """
        if not sender or not data:
            return
        try:
            if sender is EventSender.LOCAL:
                if EventTopics.GAME_PARAMS in args:  # This is the data that came from the menu selections
                    self.game_metadata.variant = data.get(GameOption.VARIANT)
                    self.game_metadata.rated = data.get(GameOption.RATED, False)
                    self.game_metadata.players[not self.my_color].ai_level = data.get(GameOption.COMPUTER_SKILL_LEVEL) if self.vs_ai else None

                    for color in COLORS:
                        self.game_metadata.clocks[color].time = data.get(GameOption.TIME_CONTROL)[0]       # mins
                        self.game_metadata.clocks[color].increment = data.get(GameOption.TIME_CONTROL)[1]  # secs

            elif sender is EventSender.FROM_IEM:
                if EventTopics.GAME_START in args:
                    self.game_metadata.reset()
                    self.game_metadata.game_id = data.get('gameId')
                    self.game_metadata.rated = data.get('rated')
                    # Since lila PR# 15969, color seeks can only be random. This resets `my_color` to the color received
                    self.my_color = Color(COLOR_NAMES.index(data.get('color')))
                    self.game_metadata.variant = data.get('variant', {}).get('name')
                    self.game_metadata.speed = data['speed']

                elif EventTopics.GAME_END in args:
                    # The game end can be reported by the IEM before the game state dispatcher
                    # sends the final game state, so the status is taken from here as well
                    self.game_metadata.game_status.status = data.get('status', {}).get('name', "")
                    self.game_metadata.game_status.winner = data.get('winner', "")
                    self.game_metadata.players[self.my_color].rating_diff = data.get('ratingDiff', "")
                    self.game_metadata.players[not self.my_color].rating_diff = data.get('opponent', {}).get('ratingDiff', "")

            elif sender is EventSender.FROM_GSD:
                if EventTopics.GAME_START in args:
                    self._start_clock_on_side_to_move()
                    for color in COLOR_NAMES:
                        side_data = data.get(color, {})
                        color_as_bool = Color(COLOR_NAMES.index(color))
                        if side_data.get('name'):
                            self.game_metadata.players[color_as_bool].title = side_data.get('title')
                            self.game_metadata.players[color_as_bool].name = side_data.get('name', "?")
                            self.game_metadata.players[color_as_bool].rating = side_data.get('rating', "?")
                            self.game_metadata.players[color_as_bool].is_provisional_rating = side_data.get(
                                'provisional', False)
                        elif self.vs_ai:
                            self.game_metadata.players[
                                color_as_bool].name = f"Stockfish level {side_data.get('aiLevel', '?')}"

                        self.game_metadata.clocks[color_as_bool].units = "ms"
                        self.game_metadata.clocks[color_as_bool].time = data.get('state', {}).get('wtime' if color == "white" else 'btime')
                        self.game_metadata.clocks[color_as_bool].increment = data.get('state', {}).get('winc' if color == "white" else 'binc')

                elif EventTopics.MOVE_MADE in args:
                    self.game_metadata.clocks[WHITE].time = data.get('wtime')
                    self.game_metadata.clocks[BLACK].time = data.get('btime')
                    self._start_clock_on_side_to_move() if EventTopics.GAME_END not in args else None

            self._notify_game_model_updated(*args, **kwargs)
        except Exception as e:
            log.exception(f"Error saving online game metadata: {e}")
            raise

    def _report_game_over(self, status: str, winner: str) -> None:
        """Saves game information and notifies listeners that the game has ended.
           This should only ever be called if the game is confirmed to be over.
           Guarded so duplicate terminal events (e.g. IEM gameFinish plus the
           GSD terminal snapshot) cannot produce duplicate game over/PGN output.
        """
        if self._game_over_reported:
            log.debug("Game over already reported; ignoring duplicate terminal event")
            return

        self._game_over_reported = True
        self._game_end()
        self.game_metadata.game_status.status = status  # status list can be found in lila status.ts
        self.game_metadata.game_status.winner = winner
        self.game_metadata.set_clock_ticking(None)
        self._notify_game_model_updated(EventTopics.GAME_END)

    def _cancel_game_stream(self) -> None:
        """Cancels the game state stream thread (bounded join, immediate read unblock)"""
        gsd = self.game_state_dispatcher
        if isinstance(gsd, GameStateDispatcher):
            log.debug(f"Cancelling {type(gsd).__name__} (id={id(gsd)})")
            gsd.cancel()

    def cleanup(self) -> None:
        """Cleans up after this model by clearing event listeners and subscriptions.
           This should only ever be run when the models are no longer needed. This is
           called automatically on exit.
        """
        self._cancel_game_stream()
        super().cleanup()

        if self.api_iem:
            try:
                self.api_iem.unsubscribe_from_events(self._handle_iem_event)
                log.debug(f"Cleared subscription from {type(self.api_iem).__name__} (id={id(self.api_iem)})")
            except Exception as e:
                log.debug(f"Unable to clear IEM subscription: {e}")

    def exit(self):
        """Gracefully exit the online game model. Ensure subscriptions are
           cleaned up and any active game searches and streams are closed
        """
        if self.searching and self.sent_challenge_id:
            try:
                self.api_client.challenges.cancel(self.sent_challenge_id)
            except Exception as e:
                log.debug(f"Unable to cancel pending challenge: {e}")
            self.sent_challenge_id = None

        self._game_end()
        self._cancel_game_stream()
