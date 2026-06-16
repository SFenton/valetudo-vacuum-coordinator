"""Tests for Valetudo Vacuum Coordinator event handling."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import importlib.util
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "custom_components" / "valetudo_vacuum_coordinator"
TEST_PACKAGE_NAME = "valetudo_vacuum_coordinator_coordinator_test"


def _install_homeassistant_stubs() -> None:
    """Install minimal Home Assistant stubs needed to import coordinator.py."""
    homeassistant = types.ModuleType("homeassistant")
    const_module = types.ModuleType("homeassistant.const")
    const_module.ATTR_ENTITY_ID = "entity_id"

    core_module = types.ModuleType("homeassistant.core")

    class Event:
        def __init__(self, entity_id: str, state: str) -> None:
            self.data = {"entity_id": entity_id, "new_state": State(state)}

    class HomeAssistant:
        pass

    class State:
        def __init__(self, state: str) -> None:
            self.state = state

    def callback(func):
        return func

    core_module.Event = Event
    core_module.HomeAssistant = HomeAssistant
    core_module.State = State
    core_module.callback = callback

    helpers_module = types.ModuleType("homeassistant.helpers")
    event_module = types.ModuleType("homeassistant.helpers.event")
    event_module.async_call_later = lambda *args, **kwargs: (lambda: None)
    event_module.async_track_state_change_event = lambda *args, **kwargs: (lambda: None)

    storage_module = types.ModuleType("homeassistant.helpers.storage")

    class Store:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def async_load(self):
            return None

        async def async_save(self, data):
            return None

    storage_module.Store = Store

    util_module = types.ModuleType("homeassistant.util")
    dt_module = types.ModuleType("homeassistant.util.dt")
    dt_module.utcnow = lambda: datetime.now(UTC)
    dt_module.now = lambda: datetime.now(UTC)
    util_module.dt = dt_module

    sys.modules.setdefault("homeassistant", homeassistant)
    sys.modules.setdefault("homeassistant.const", const_module)
    sys.modules.setdefault("homeassistant.core", core_module)
    sys.modules.setdefault("homeassistant.helpers", helpers_module)
    sys.modules.setdefault("homeassistant.helpers.event", event_module)
    sys.modules.setdefault("homeassistant.helpers.storage", storage_module)
    sys.modules.setdefault("homeassistant.util", util_module)
    sys.modules.setdefault("homeassistant.util.dt", dt_module)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_install_homeassistant_stubs()
package = types.ModuleType(TEST_PACKAGE_NAME)
package.__path__ = [str(PACKAGE)]
sys.modules[package.__name__] = package
const = _load_module(f"{package.__name__}.const", PACKAGE / "const.py")
_load_module(f"{package.__name__}.logic", PACKAGE / "logic.py")
coordinator_module = _load_module(
    f"{package.__name__}.coordinator",
    PACKAGE / "coordinator.py",
)


class _EventHandlingCoordinator(coordinator_module.ValetudoVacuumCoordinator):
    """Minimal coordinator that records scheduler calls."""

    def __init__(self) -> None:
        self.people_entities = []
        self.vacuum_entity = "vacuum.robot"
        self.config = {
            const.CONF_BATTERY_ENTITY: "sensor.robot_battery",
            const.CONF_DOCK_STATUS_ENTITY: "sensor.robot_dock_status",
            const.CONF_STATUS_FLAG_ENTITY: "sensor.robot_status_flag",
        }
        self.next_room_checks = 0

    def _observe_active_run(self, entity_id, new_state, now) -> None:
        pass

    def _observe_manual_run(self, entity_id, new_state, now) -> None:
        pass

    async def _async_handle_vacuum_state(self, vacuum_state, now) -> None:
        raise AssertionError("battery changes must not be handled as vacuum state changes")

    async def _async_maybe_start_next_room(self) -> None:
        self.next_room_checks += 1


def test_battery_state_change_rechecks_next_room_dispatch() -> None:
    test_coordinator = _EventHandlingCoordinator()
    event = sys.modules["homeassistant.core"].Event("sensor.robot_battery", "41")

    asyncio.run(test_coordinator._async_handle_state_change_event(event))

    assert test_coordinator.next_room_checks == 1

