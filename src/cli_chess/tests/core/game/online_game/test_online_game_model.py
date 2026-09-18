from cli_chess.core.game.game_options import GameOption
from cli_chess.core.game.online_game import OnlineGameModel
from cli_chess.core.api.incoming_event_manger import IEMEventTopics
from cli_chess.core.api.game_state_dispatcher import GameStateDispatcher
from cli_chess.core.api.stream_manager import ConnectionState, StreamUnavailableError
from cli_chess.utils import EventTopics
from chess import WHITE, BLACK
from unittest.mock import Mock
import threading
import pytest


@pytest.fixture
def model(monkeypatch):
    from cli_chess.core.api import api_manager
    monkeypatch.setattr(api_manager, "api_client", Mock(), raising=False)
    monkeypatch.setattr(api_manager, "api_iem", Mock(), raising=False)

    game_parameters = {
        GameOption.COLOR: "random",
        GameOption.VARIANT: "standard",
        GameOption.TIME_CONTROL: (10, 5),
        GameOption.RATED: False,
        GameOption.OPPONENT: "testOpponent",
    }
    return OnlineGameModel(game_parameters, is_vs_ai=False)


def test_create_game_sends_direct_challenge(model):
    model.api_client.challenges.create.return_value = {'id': 'abc123'}
    challenge_sent = threading.Event()
    received = {}

    def listener(*args, **kwargs):
        if kwargs.get('msg'):
            received.update(args=args, msg=kwargs.get('msg'))
            challenge_sent.set()

    model.e_game_model_updated.add_listener(listener)
    model.create_game()

    assert challenge_sent.wait(timeout=5)
    model.api_client.challenges.create.assert_called_once_with(username="testOpponent",
                                                               rated=False,
                                                               clock_limit=600,
                                                               clock_increment=5,
                                                               color="random",
                                                               variant="standard")
    assert model.sent_challenge_id == 'abc123'
    assert EventTopics.GAME_SEARCH in received['args']
    assert received['msg'] == "Challenge sent to testOpponent. Waiting for a response..."


def test_iem_challenge_declined_stops_search(model):
    received = {}

    def listener(*args, **kwargs):
        received.update(args=args, msg=kwargs.get('msg'))

    model.e_game_model_updated.add_listener(listener)
    model.searching = True
    model.sent_challenge_id = 'abc123'
    model._handle_iem_event(IEMEventTopics.CHALLENGE_DECLINED, data={'id': 'abc123', 'declineReason': 'Too fast'})

    assert not model.searching
    assert model.sent_challenge_id is None
    assert EventTopics.ERROR in received['args']
    assert received['msg'] == 'Too fast'


def test_iem_challenge_declined_other_id_ignored(model):
    received = {}

    def listener(*args, **kwargs):
        received.update(args=args, msg=kwargs.get('msg'))

    model.e_game_model_updated.add_listener(listener)
    model.searching = True
    model.sent_challenge_id = 'abc123'
    model._handle_iem_event(IEMEventTopics.CHALLENGE_DECLINED, data={'id': 'zzz999'})

    assert model.searching
    assert model.sent_challenge_id == 'abc123'
    assert not received


def test_iem_challenge_cancelled_stops_search(model):
    received = {}

    def listener(*args, **kwargs):
        received.update(args=args, msg=kwargs.get('msg'))

    model.e_game_model_updated.add_listener(listener)
    model.searching = True
    model.sent_challenge_id = 'abc123'
    model._handle_iem_event(IEMEventTopics.CHALLENGE_CANCELLED, data={'id': 'abc123'})

    assert not model.searching
    assert EventTopics.ERROR in received['args']
    assert received['msg'] == "The challenge has been cancelled"


def test_iem_game_end_records_result_before_notifying(model):
    observed = {}

    def listener(*args, **kwargs):
        if EventTopics.GAME_END in args:
            observed.update(game_in_progress=model.game_in_progress,
                            status=model.game_metadata.game_status.status,
                            winner=model.game_metadata.game_status.winner)

    model.e_game_model_updated.add_listener(listener)
    model.game_in_progress = True
    model.playing_game_id = 'abc123'
    model._handle_iem_event(EventTopics.GAME_END, data={'gameId': 'abc123',
                                                        'status': {'id': 31, 'name': 'resign'},
                                                        'winner': 'white'})

    assert observed['game_in_progress'] is False
    assert observed['status'] == 'resign'
    assert observed['winner'] == 'white'


def test_exit_cancels_pending_challenge(model):
    model.searching = True
    model.sent_challenge_id = 'abc123'
    model.exit()

    model.api_client.challenges.cancel.assert_called_once_with('abc123')
    assert not model.game_in_progress
    assert model.sent_challenge_id is None


def test_exit_without_pending_challenge(model):
    model.exit()

    model.api_client.challenges.cancel.assert_not_called()
    assert not model.game_in_progress


# ----------------------------------------------------------------------
# Stream recovery / resynchronization contract
# ----------------------------------------------------------------------
def _put_game_in_progress(model):
    model.game_in_progress = True
    model.searching = False
    model.playing_game_id = "abc123"
    model._gsd_live = True
    model.game_state_dispatcher = Mock()


def test_commands_rejected_while_stream_recovering(model):
    _put_game_in_progress(model)
    model._gsd_live = False

    with pytest.raises(StreamUnavailableError):
        model.make_move("e4")
    with pytest.raises(StreamUnavailableError):
        model.offer_draw()
    with pytest.raises(StreamUnavailableError):
        model.resign()
    with pytest.raises(StreamUnavailableError):
        model.post_message("hello")
    with pytest.raises(StreamUnavailableError):
        model.propose_takeback()

    gsd = model.game_state_dispatcher
    gsd.make_move.assert_not_called()
    gsd.send_draw_offer.assert_not_called()
    gsd.resign.assert_not_called()
    gsd.post_message.assert_not_called()
    gsd.send_takeback_request.assert_not_called()


def test_premove_rejected_while_stream_recovering(model):
    _put_game_in_progress(model)
    model.my_color = BLACK  # it is white's turn, so a premove would otherwise be stored
    model._gsd_live = False

    with pytest.raises(StreamUnavailableError):
        model.set_premove("e7e5")


def test_commands_go_through_when_live(model):
    _put_game_in_progress(model)
    model.my_color = WHITE
    model.board_model.reinitialize_board(variant="standard", orientation=WHITE, fen="")

    model.make_move("e4")
    model.game_state_dispatcher.make_move.assert_called_once_with("e2e4")


def test_recovering_state_gates_commands_and_alerts(model):
    alerts = []
    model.e_game_model_updated.add_listener(lambda *args, msg="", **kwargs: alerts.append(msg))
    _put_game_in_progress(model)

    model._handle_gsd_connection_state(ConnectionState.RECOVERING)

    assert model._gsd_live is False
    assert any("resynchronizing" in msg.lower() for msg in alerts if msg)
    with pytest.raises(StreamUnavailableError):
        model.resign()


def test_game_full_snapshot_resynchronizes_after_disconnect(model):
    notifications = []
    model.e_game_model_updated.add_listener(lambda *args, msg="", **kwargs: notifications.append((args, msg)))
    _put_game_in_progress(model)
    model._gsd_live = False  # stream dropped before the snapshot arrives
    model._gsd_synced_once = True  # but an earlier snapshot had already been applied

    snapshot = {
        "type": "gameFull",
        "initialFen": "",
        "white": {"name": "alice", "rating": 1500},
        "black": {"name": "bob", "rating": 1600},
        "state": {"moves": "e2e4", "wtime": 900000, "btime": 899000,
                  "winc": 0, "binc": 0, "status": "started"},
    }
    model._handle_gsd_event(EventTopics.GAME_START, data=snapshot)

    assert model._gsd_live is True
    assert [str(m) for m in model.board_model.get_move_stack()] == ["e2e4"]
    assert model.board_model.get_turn() == BLACK
    assert not model._game_over_reported
    assert any(EventTopics.GAME_START in args for args, _ in notifications)
    assert any("synchronized" in msg for _, msg in notifications)


def test_first_game_full_snapshot_is_not_treated_as_reconnect(model):
    notifications = []
    model.e_game_model_updated.add_listener(lambda *args, msg="", **kwargs: notifications.append((args, msg)))
    _put_game_in_progress(model)
    model._gsd_live = False
    model._gsd_synced_once = False  # exactly the state _start_game leaves before the first gameFull

    snapshot = {
        "type": "gameFull",
        "initialFen": "",
        "white": {"name": "alice", "rating": 1500},
        "black": {"name": "bob", "rating": 1600},
        "state": {"moves": "", "wtime": 900000, "btime": 900000,
                  "winc": 0, "binc": 0, "status": "started"},
    }
    model._handle_gsd_event(EventTopics.GAME_START, data=snapshot)

    assert model._gsd_live is True
    assert not any("synchronized" in msg for _, msg in notifications)


def test_duplicate_terminal_events_report_game_over_only_once(model):
    # Only the bare (GAME_END,) notification drives the game over/PGN side effect
    game_ends = []
    model.e_game_model_updated.add_listener(
        lambda *args, **kwargs: game_ends.append(args) if args == (EventTopics.GAME_END,) else None
    )
    _put_game_in_progress(model)
    model.board_model.reinitialize_board(variant="standard", orientation=WHITE, fen="")

    terminal_state = {"moves": "e2e4 e7e5", "wtime": 900000, "btime": 900000,
                      "status": "mate", "winner": "white"}
    model._handle_gsd_event(EventTopics.MOVE_MADE, EventTopics.GAME_END, data=terminal_state)
    # IEM gameFinish or a re-delivered snapshot must not duplicate the game over/PGN
    model._handle_gsd_event(EventTopics.MOVE_MADE, EventTopics.GAME_END, data=terminal_state)

    assert len(game_ends) == 1
    assert not model.game_in_progress
    assert model.game_metadata.game_status.status == "mate"
    assert model.game_metadata.game_status.winner == "white"


def test_exit_cancels_the_game_stream(monkeypatch, model):
    from cli_chess.core.api import api_manager
    monkeypatch.setattr(api_manager, "api_client", Mock(), raising=False)

    gsd = GameStateDispatcher("abc123")
    model.game_in_progress = True
    model.playing_game_id = "abc123"
    model.game_state_dispatcher = gsd

    model.exit()

    assert gsd.state is ConnectionState.STOPPED
    assert not model.game_in_progress
