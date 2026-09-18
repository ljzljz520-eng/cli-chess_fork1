from __future__ import annotations
from cli_chess.menus import MenuPresenter
from cli_chess.menus.main_menu import MainMenuView
from cli_chess.menus.online_games_menu import OnlineGamesMenuModel, OnlineGamesMenuPresenter
from cli_chess.menus.offline_games_menu import OfflineGamesMenuModel, OfflineGamesMenuPresenter
from cli_chess.menus.settings_menu import SettingsMenuModel, SettingsMenuPresenter
from cli_chess.modules.about import AboutPresenter
from cli_chess.core.api.api_manager import e_api_connection_state_changed
from cli_chess.utils import log
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from cli_chess.menus.main_menu import MainMenuModel


class MainMenuPresenter(MenuPresenter):
    """Defines the Main Menu"""
    def __init__(self, model: MainMenuModel):
        self.model = model
        self.online_games_menu_presenter = OnlineGamesMenuPresenter(OnlineGamesMenuModel())
        self.offline_games_menu_presenter = OfflineGamesMenuPresenter(OfflineGamesMenuModel())
        self.settings_menu_presenter = SettingsMenuPresenter(SettingsMenuModel())
        self.about_presenter = AboutPresenter()
        self.view = MainMenuView(self)
        self.selection = self.model.get_menu_options()[0].option

        e_api_connection_state_changed.add_listener(self._handle_api_connection_state_changed)

        super().__init__(self.model, self.view)

    @staticmethod
    def _handle_api_connection_state_changed(*args, **kwargs) -> None:
        """Repaints so the online menu availability reflects the live connection state"""
        try:
            from cli_chess.utils.ui_common import repaint_ui
            repaint_ui()
        except Exception as e:
            # No application running yet (e.g. unit tests)
            log.debug(f"Unable to repaint for api connection state change: {e}")
