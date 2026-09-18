from cli_chess.core.api.incoming_event_manger import IncomingEventManager
from cli_chess.core.api.stream_manager import ConnectionState, PERMANENT_FAILURE_STATES
from cli_chess.utils.event import Event
from cli_chess.utils.logging import log
from berserk import Client, TokenSession
from typing import Optional

required_token_scopes: set = {"board:play"}
optional_token_scopes: set = {"challenge:write"}
api_session: Optional[TokenSession] = None
api_client: Optional[Client] = None
api_iem: Optional[IncomingEventManager] = None
api_ready = False
api_status_message = ""

# Emitted whenever the account stream connection state changes. Listeners
# (e.g. the main menu) can repaint so availability/actionable status is current.
e_api_connection_state_changed = Event()


def _start_api(token: str, base_url: str):
    """Handles creating a new API session, client, and IEM
       when the API token has been updated. This generally
       should only ever be called via the Token Manager on
       token verification. Any previously running account
       stream is cancelled (bounded) before the new one starts.
    """
    global api_session, api_client, api_iem, api_ready, api_status_message
    try:
        if api_iem is not None:
            log.info("Cancelling previous account event stream")
            api_iem.cancel()

        api_session = TokenSession(token)
        api_client = Client(api_session, base_url)
        api_ready = False
        api_status_message = "Connecting to Lichess..."
        api_iem = IncomingEventManager()
        api_iem.start()
    except Exception as e:
        api_ready = False
        api_status_message = "Failed to connect to Lichess. Check your network and try again."
        e_api_connection_state_changed.notify(ConnectionState.RETRIES_EXHAUSTED,
                                              message=api_status_message)
        log.exception(f"Failed to start api: {e}")


def handle_iem_state_change(state: ConnectionState, message: str = "") -> None:
    """Keeps ``api_ready`` consistent with the account stream lifecycle:
       ready only while the stream is live; a permanent auth failure or an
       exhausted retry budget clears it and exposes an actionable message.
    """
    global api_ready, api_status_message

    if state is ConnectionState.LIVE:
        api_ready = True
        api_status_message = ""
    elif state in PERMANENT_FAILURE_STATES:
        api_ready = False
        api_status_message = message
    else:
        # Transient connecting/recovering states keep the current readiness,
        # but surface the status message (e.g. rate limited, reconnecting).
        api_status_message = message

    e_api_connection_state_changed.notify(state, message=message)


def api_is_ready() -> bool:
    """Check the status of the api connection. Currently,
       this is used for toggling the online menu availability
    """
    return api_ready


def api_token_has_scope(scope: str) -> bool:
    """Checks if the linked token has the passed in scope"""
    from cli_chess.modules.token_manager import token_manager_model
    return scope in token_manager_model.linked_token_scopes
