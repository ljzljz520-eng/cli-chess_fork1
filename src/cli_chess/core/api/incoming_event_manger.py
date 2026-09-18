from cli_chess.core.api.stream_manager import ConnectionState, ReconnectingStream, open_stream_response
from cli_chess.utils.event import Event, EventTopics
from cli_chess.utils.logging import log
from typing import Callable
from enum import Enum, auto
from types import MappingProxyType


class IEMEventTopics(Enum):
    CHALLENGE = auto()  # A challenge sent by us or to us
    CHALLENGE_CANCELLED = auto()
    CHALLENGE_DECLINED = auto()
    NOT_IMPLEMENTED = auto()


iem_type_to_event_dict = MappingProxyType({
    "gameStart": EventTopics.GAME_START,
    "gameFinish": EventTopics.GAME_END,
    "challenge": IEMEventTopics.CHALLENGE,
    "challengeCanceled": IEMEventTopics.CHALLENGE_CANCELLED,
    "challengeDeclined": IEMEventTopics.CHALLENGE_DECLINED,
})


class IncomingEventManager(ReconnectingStream):
    """Opens the account event stream and keeps track of Lichess incoming
       events (such as game start, game finish). The stream automatically
       reconnects with backoff and is generation fenced so events delivered
       by a superseded session are discarded.
    """

    def __init__(self):
        super().__init__(name="lichess-account-events", eof_delay=1.0)
        self.e_new_event_received = Event()
        self.my_games = []

    def _open_stream(self, generation: int):
        """Opens the realtime incoming event stream for the linked account"""
        try:
            from cli_chess.core.api.api_manager import api_client
        except ImportError:
            # TODO: Clean this up so the error is displayed on the main screen
            log.error("Failed to import api_client")
            raise ImportError("API client not setup. Do you have an API token linked?")

        return open_stream_response(requestor=api_client.board._r, path="/api/stream/event")

    def _handle_event(self, generation: int, event: dict) -> None:
        data = None
        event_topic = iem_type_to_event_dict.get(event['type'], IEMEventTopics.NOT_IMPLEMENTED)
        log.debug(f"IEM event received: {event}")

        if event_topic is EventTopics.GAME_START:
            data = event['game']
            game_id = data['gameId']
            if game_id not in self.my_games:
                self.my_games.append(game_id)

        elif event_topic is EventTopics.GAME_END:
            try:
                data = event['game']
                self.my_games.remove(data['gameId'])
            except ValueError:
                pass

        elif (event_topic is IEMEventTopics.CHALLENGE or
              event_topic is IEMEventTopics.CHALLENGE_CANCELLED or
              event_topic is IEMEventTopics.CHALLENGE_DECLINED):
            data = event['challenge']

        self.e_new_event_received.notify(event_topic, data=data)

    def _on_connection_state_changed(self, state: ConnectionState, message: str) -> None:
        """Reflects the account stream connection state in api_manager (api_ready)"""
        from cli_chess.core.api import api_manager
        api_manager.handle_iem_state_change(state, message)

    def _on_cancel(self) -> None:
        self.e_new_event_received.remove_all_listeners()

    def get_active_games(self) -> list:
        """Returns a list of games in progress for this account"""
        return self.my_games

    def add_event_listener(self, listener: Callable) -> None:
        """Subscribes the passed in method to IEM events"""
        self.e_new_event_received.add_listener(listener)

    def unsubscribe_from_events(self, listener: Callable) -> None:
        """Unsubscribes the passed in method from IEM events"""
        self.e_new_event_received.remove_listener(listener)
