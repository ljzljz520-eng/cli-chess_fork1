from cli_chess.core.game import GameModelBase
from cli_chess.core.api.stream_manager import ConnectionState, PERMANENT_FAILURE_STATES
from cli_chess.core.api.stream_manager import ReconnectingStream, open_stream_response
from cli_chess.menus.tv_channel_menu import TVChannelMenuOptions
from cli_chess.utils.event import Event, EventTopics
from cli_chess.utils.logging import log
from chess import COLOR_NAMES, COLORS, Color, WHITE
from typing import Optional, Dict


class WatchTVModel(GameModelBase):
    def __init__(self, channel: TVChannelMenuOptions):
        super().__init__(variant=channel.variant, fen=None)
        self.channel = channel
        self._tv_stream = StreamTVChannel(self.channel)
        self._tv_stream.e_tv_stream_event.add_listener(self.stream_event_received)
        self._tv_stream.e_stream_state_changed.add_listener(self._handle_stream_state)

    def start_watching(self):
        """Notify the TV stream thread to start"""
        self._tv_stream.start()

    def stop_watching(self):
        """Stop the TV stream thread (bounded, interruptible)"""
        self._tv_stream.stop_watching()

    def _handle_stream_state(self, state: ConnectionState, *, message: str = "") -> None:
        """Translates unified stream states into TV model update topics"""
        if state is ConnectionState.CONNECTING:
            self._notify_game_model_updated(EventTopics.GAME_SEARCH)
        elif state is ConnectionState.RATE_LIMITED or (state is ConnectionState.RECOVERING and message):
            # A message-less RECOVERING is the normal gap between two featured games
            self._notify_game_model_updated(
                EventTopics.ERROR,
                msg=message or "Error streaming. Reconnecting..."
            )
        elif state in PERMANENT_FAILURE_STATES:
            self._notify_game_model_updated(
                EventTopics.ERROR,
                msg=message or "Retries exhausted. Stopping TV."
            )

    def _update_game_metadata(self, *args, data: Optional[Dict] = None) -> None:
        """Parses and saves the data of the game being played"""
        if not data:
            return
        try:
            if EventTopics.GAME_START in args:
                self.game_metadata.reset()
                self.game_metadata.game_id = data.get('id')
                self.game_metadata.variant = self.channel

                for i, color in enumerate(COLOR_NAMES[::-1]):
                    color_as_bool = Color(COLOR_NAMES.index(color))
                    side_data = data.get('players', {})[i]
                    player_data = side_data.get('user', {})
                    ai_level = side_data.get('ai')
                    if side_data and not ai_level:
                        if player_data:
                            self.game_metadata.players[color_as_bool].title = player_data.get('title')
                            self.game_metadata.players[color_as_bool].name = player_data.get('name')
                            self.game_metadata.players[color_as_bool].rating = side_data.get('rating', "?")
                            self.game_metadata.players[color_as_bool].is_provisional_rating = side_data.get('provisional', False)
                        else:
                            self.game_metadata.players[color_as_bool].name = "Anonymous"
                    elif ai_level:
                        self.game_metadata.players[color_as_bool].name = f"Stockfish level {ai_level}"

            if EventTopics.MOVE_MADE in args:
                self.game_metadata.set_clock_ticking(self.board_model.get_turn())
                for color in COLORS:
                    self.game_metadata.clocks[color].units = "sec"
                    self.game_metadata.clocks[color].time = data.get('wc' if color == WHITE else 'bc')

        except Exception as e:
            log.error(f"Error saving game metadata: {e}")
            raise

    def stream_event_received(self, *args, data: Optional[Dict] = None, **kwargs):
        """An event was received from the TV thread. Raises exception on invalid data"""
        try:
            if data:
                if EventTopics.GAME_START in args:
                    orientation = Color(COLOR_NAMES.index(data.get('orientation', 'white')))
                    self.board_model.reinitialize_board(self.channel.variant, orientation, data.get('fen'))

                if EventTopics.MOVE_MADE in args:
                    # NOTE: the `lm` field that lichess sends for TV feeds and 'lastMove' field sent
                    # during game spectator streams is not valid UCI. It should only be used
                    # for highlighting move squares (invalid castle notation, missing promotion piece,
                    # crazyhouse drop notation, etc).
                    self.board_model.set_board_position(data.get('fen'), uci_last_move=data.get('lm'))

            self._update_game_metadata(*args, data=data)
            self._notify_game_model_updated(*args, **kwargs)
        except Exception as e:
            log.error(f"Error parsing stream data: {e}")
            raise


# To restore old TV streaming logic see commit 23ca5cd
class StreamTVChannel(ReconnectingStream):
    """Streams the featured games of a Lichess TV channel. Uses the unified
       reconnect/backoff/cancel contract: waits are interruptible and the
       blocking HTTP read is closed immediately on stop.
    """
    def __init__(self, channel: TVChannelMenuOptions):
        super().__init__(name=f"lichess-tv-{channel.key}", eof_delay=2.0)
        self.channel = channel
        self.e_tv_stream_event = Event()

        try:
            from cli_chess.core.api.api_manager import api_client
            self.api_client = api_client
        except Exception as e:
            log.error(f"Failed to import api_client for TV stream: {e}")
            raise

    def _open_stream(self, generation: int):
        # TODO: Update to use berserk TV specific method once implemented
        return open_stream_response(
            requestor=self.api_client.tv._r,  # noqa
            path=f"/api/tv/{self.channel.key}/feed",
        )

    def _handle_event(self, generation: int, event: dict) -> None:
        t = event.get('t')
        d = event.get('d')
        if not t or not d:
            raise ValueError(f"Unable to stream TV as the data is malformed: {event}")

        if t == 'featured':
            log.info(f"Started streaming TV game: {d.get('id')}")
            self.e_tv_stream_event.notify(EventTopics.GAME_START, data=d)

        if t == 'fen':
            self.e_tv_stream_event.notify(EventTopics.MOVE_MADE, data=d)

    def _on_cancel(self) -> None:
        self.e_tv_stream_event.remove_all_listeners()

    def stop_watching(self):
        """Cancels the stream immediately (no sleeps, bounded thread join)"""
        log.info("Stopping TV stream")
        self.cancel()
