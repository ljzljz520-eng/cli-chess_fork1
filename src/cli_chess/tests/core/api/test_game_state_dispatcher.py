import pytest
import requests
from unittest.mock import Mock
from requests import Response as RequestsResponse
from berserk.exceptions import ResponseError

from cli_chess.core.api import api_manager
from cli_chess.core.api.game_state_dispatcher import GameStateDispatcher
from cli_chess.core.api.stream_manager import ConnectionState, StreamUnavailableError
from cli_chess.utils import EventTopics


def _response_error(status_code):
    response = RequestsResponse()
    response.status_code = status_code
    return ResponseError(response)


@pytest.fixture
def gsd(monkeypatch):
    monkeypatch.setattr(api_manager, "api_client", Mock(), raising=False)
    dispatcher = GameStateDispatcher("abc123")
    dispatcher._open_stream = Mock()  # never open a real socket in these tests
    yield dispatcher
    dispatcher.cancel(timeout=2)


def _terminal_game_full(status="mate", winner="white"):
    return {
        "type": "gameFull",
        "initialFen": "",
        "white": {"name": "alice", "rating": 1500},
        "black": {"name": "bob", "rating": 1600},
        "state": {"moves": "e2e4 e7e5", "wtime": 900000, "btime": 900000,
                  "winc": 0, "binc": 0, "status": status, "winner": winner},
    }


def _started_game_full():
    event = _terminal_game_full(status="started")
    event["state"].pop("winner")
    return event


def test_terminal_game_full_snapshot_reports_game_end_once(gsd):
    notifications = []
    gsd.add_event_listener(lambda *args, **kwargs: notifications.append(args))

    gsd._handle_event(1, _terminal_game_full())

    assert gsd.is_game_over
    assert notifications == [
        (EventTopics.GAME_START,),
        (EventTopics.MOVE_MADE, EventTopics.GAME_END),
    ]
    # Listeners are torn down with the game and EOF must not restart the stream
    assert gsd.e_game_state_dispatcher_event.listeners == []
    assert not gsd._should_reconnect_after_eof()


def test_started_game_full_does_not_report_game_end(gsd):
    notifications = []
    gsd.add_event_listener(lambda *args, **kwargs: notifications.append(args))

    gsd._handle_event(1, _started_game_full())

    assert not gsd.is_game_over
    assert notifications == [(EventTopics.GAME_START,)]
    assert gsd._should_reconnect_after_eof()


def test_terminal_game_state_marks_game_over(gsd):
    notifications = []
    gsd.add_event_listener(lambda *args, **kwargs: notifications.append(args))

    gsd._handle_event(1, {"type": "gameState", "moves": "", "status": "resign", "winner": "black"})

    assert gsd.is_game_over
    assert notifications == [(EventTopics.MOVE_MADE, EventTopics.GAME_END)]


@pytest.mark.parametrize("method_name,args", [
    ("make_move", ("e2e4",)),
    ("resign", ()),
    ("post_message", ("hello",)),
    ("send_draw_offer", ()),
    ("send_takeback_request", ()),
])
def test_commands_rejected_while_stream_not_live(gsd, method_name, args):
    # State is STOPPED before the first connection is established
    with pytest.raises(StreamUnavailableError):
        getattr(gsd, method_name)(*args)
    gsd.api_client.board.make_move.assert_not_called()


def test_move_is_sent_exactly_once_when_live(gsd):
    gsd.state = ConnectionState.LIVE
    gsd.make_move("e2e4")
    gsd.api_client.board.make_move.assert_called_once_with("abc123", "e2e4")


def test_5xx_fails_command_once_without_retry(gsd):
    gsd.state = ConnectionState.LIVE
    gsd.api_client.board.make_move.side_effect = _response_error(503)

    with pytest.raises(StreamUnavailableError):
        gsd.make_move("e2e4")

    assert gsd.api_client.board.make_move.call_count == 1


def test_network_error_fails_command_once_without_retry(gsd):
    gsd.state = ConnectionState.LIVE
    gsd.api_client.board.offer_draw.side_effect = requests.ConnectionError("unreachable")

    with pytest.raises(StreamUnavailableError):
        gsd.send_draw_offer()

    gsd.api_client.board.offer_draw.assert_called_once_with("abc123")


def test_rate_limited_command_raises_warning_once(gsd):
    gsd.state = ConnectionState.LIVE
    gsd.api_client.board.resign_game.side_effect = _response_error(429)

    with pytest.raises(Warning):
        gsd.resign()

    gsd.api_client.board.resign_game.assert_called_once_with("abc123")


def test_cancel_purged_listeners_emit_no_callbacks(gsd):
    received = []
    gsd.add_event_listener(lambda *args, **kwargs: received.append(args))

    gsd.cancel(timeout=2)

    gsd.e_game_state_dispatcher_event.notify(EventTopics.MOVE_MADE)
    assert received == []
    assert gsd.state is ConnectionState.STOPPED
