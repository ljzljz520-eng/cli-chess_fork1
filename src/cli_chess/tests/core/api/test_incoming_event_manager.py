import pytest
from unittest.mock import Mock

from cli_chess.core.api import api_manager
from cli_chess.core.api.incoming_event_manger import IEMEventTopics, IncomingEventManager
from cli_chess.core.api.stream_manager import ConnectionState
from cli_chess.utils import EventTopics


@pytest.fixture
def iem(monkeypatch):
    monkeypatch.setattr(api_manager, "api_client", Mock(), raising=False)
    manager = IncomingEventManager()
    yield manager
    manager.cancel(timeout=2)


def test_game_start_dedup_and_game_finish_removal(iem):
    event_start = {"type": "gameStart", "game": {"gameId": "abc123", "compat": {"board": True}}}
    iem._handle_event(1, event_start)
    iem._handle_event(2, event_start)  # duplicate after reconnect must not double register
    assert iem.get_active_games() == ["abc123"]

    iem._handle_event(3, {"type": "gameFinish", "game": {"gameId": "abc123"}})
    assert iem.get_active_games() == []

    # An unmatched finish (e.g. arrived before reconnect) must not raise
    iem._handle_event(4, {"type": "gameFinish", "game": {"gameId": "abc123"}})
    assert iem.get_active_games() == []


def test_listener_receives_mapped_topic_and_data(iem):
    received = []
    iem.add_event_listener(lambda topic, data=None: received.append((topic, data)))

    challenge = {"id": "c1", "declineReason": "Too fast"}
    iem._handle_event(1, {"type": "challengeDeclined", "challenge": challenge})

    assert received == [(IEMEventTopics.CHALLENGE_DECLINED, challenge)]


def test_state_changes_update_api_ready_and_message(iem, monkeypatch):
    monkeypatch.setattr(api_manager, "api_ready", False)
    monkeypatch.setattr(api_manager, "api_status_message", "")
    state_notifications = []
    api_manager.e_api_connection_state_changed.add_listener(
        lambda state, message="": state_notifications.append((state, message))
    )

    iem._on_connection_state_changed(ConnectionState.LIVE, "")
    assert api_manager.api_ready is True
    assert api_manager.api_status_message == ""

    iem._on_connection_state_changed(ConnectionState.RECOVERING, "Reconnecting in 2 seconds")
    # Transient states do not revoke readiness but surface the status text
    assert api_manager.api_ready is True
    assert api_manager.api_status_message == "Reconnecting in 2 seconds"

    iem._on_connection_state_changed(ConnectionState.AUTH_FAILURE, "Check your token")
    assert api_manager.api_ready is False
    assert api_manager.api_status_message == "Check your token"

    states = [s for s, _ in state_notifications]
    assert ConnectionState.LIVE in states
    assert ConnectionState.AUTH_FAILURE in states
    api_manager.e_api_connection_state_changed.remove_all_listeners()


def test_cancel_purges_listeners(iem):
    received = []
    iem.add_event_listener(lambda topic, data=None: received.append(topic))

    iem.cancel(timeout=2)

    iem.e_new_event_received.notify(EventTopics.GAME_START, data={"gameId": "x"})
    assert received == []
    assert iem.state is ConnectionState.STOPPED
