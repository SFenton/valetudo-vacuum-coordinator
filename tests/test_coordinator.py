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
logic = _load_module(f"{package.__name__}.logic", PACKAGE / "logic.py")
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
        self.active_run = None
        self.manual_run = None
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


class _FakeStates:
    def __init__(self, state_cls) -> None:
        self._state_cls = state_cls
        self._states = {}

    def set(self, entity_id: str, state: str) -> None:
        self._states[entity_id] = self._state_cls(state)

    def get(self, entity_id: str):
        return self._states.get(entity_id)


class _FakeServices:
    def __init__(self) -> None:
        self.calls = []

    async def async_call(self, domain, service, data, blocking=False) -> None:
        self.calls.append(
            {
                "domain": domain,
                "service": service,
                "data": data,
                "blocking": blocking,
            }
        )


class _FakeHass:
    def __init__(self) -> None:
        state_cls = sys.modules["homeassistant.core"].State
        self.states = _FakeStates(state_cls)
        self.services = _FakeServices()

    def async_create_task(self, coroutine) -> None:
        raise AssertionError("test calls awaited handlers directly")


class _RecoverableFailureCoordinator(coordinator_module.ValetudoVacuumCoordinator):
    """Coordinator fixture for recoverable room-failure flow."""

    def __init__(self) -> None:
        self.hass = _FakeHass()
        self.name = "Main Floor Coordinator"
        self.coordinator_id = "main_floor"
        self.vacuum_entity = "vacuum.robot"
        self.people_entities = ["person.owner"]
        self.segment_command_topic = "valetudo/robot/MapSegmentationCapability/clean/set"
        self.rooms = [
            logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
            logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
        ]
        self.room_by_id = {room.room_id: room for room in self.rooms}
        self.room_by_segment = {room.segment_id: room for room in self.rooms}
        self.room_by_name = {room.name.lower(): room for room in self.rooms}
        self.config = {
            const.CONF_ERROR_ENTITY: "sensor.robot_error",
            const.CONF_STATUS_FLAG_ENTITY: "sensor.robot_status_flag",
            const.CONF_DOCK_STATUS_ENTITY: "sensor.robot_dock_status",
            const.CONF_BATTERY_ENTITY: "sensor.robot_battery",
            const.CONF_CURRENT_AREA_ENTITY: "sensor.robot_area",
            const.CONF_CURRENT_TIME_ENTITY: "sensor.robot_time",
            const.CONF_ESTIMATED_SEGMENT_ENTITY: "sensor.robot_estimated_segment",
            const.CONF_MIN_BATTERY: 20,
        }
        self.ledgers = {room.room_id: logic.RoomLedger() for room in self.rooms}
        self.disabled_room_ids = set()
        self.paused = False
        self.pause_reason = None
        self.away_since = None
        self.session = logic.SessionState(session_id="session", started_at=logic.utcnow_iso())
        self.active_run = logic.ActiveRun(
            room_id="room_one",
            segment_id="1",
            session_id="session",
            started_at=logic.utcnow_iso(),
        )
        self.manual_run = None
        self.settings_snapshot = None
        self.while_away_outcomes = []
        self.last_error = None
        self.started_rooms = []
        self._away_timer_cancel = None
        self._next_day_timer_cancel = None

        self.set_state("person.owner", "not_home")
        self.set_state(self.vacuum_entity, "error")
        self.set_state("sensor.robot_error", "No error")
        self.set_state("sensor.robot_status_flag", "none")
        self.set_state("sensor.robot_dock_status", "idle")
        self.set_state("sensor.robot_battery", "100")
        self.set_state("sensor.robot_estimated_segment", "unknown")

    def set_state(self, entity_id: str, state: str) -> None:
        self.hass.states.set(entity_id, state)

    async def _async_save_store(self) -> None:
        return None

    @coordinator_module.callback
    def _notify_listeners(self) -> None:
        return None

    async def _async_start_room(self, room, *, vacuum_only: bool) -> None:
        if self.session and room.room_id in self.session.retry_room_ids:
            self.session.mark_retry_started(room.room_id)
        if self.session:
            self.session.mark_attempted(room.room_id)
            self.session.active_room_id = room.room_id
        self.started_rooms.append(room.room_id)


def test_error_95_recovery_clears_warning_and_requeues_room_after_docking() -> None:
    coordinator = _RecoverableFailureCoordinator()
    event_cls = sys.modules["homeassistant.core"].Event

    coordinator.set_state("sensor.robot_error", "Unknown error 95")
    asyncio.run(
        coordinator._async_handle_state_change_event(
            event_cls("sensor.robot_error", "Unknown error 95")
        )
    )

    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.needs_help is False
    assert coordinator.session.pending_recovery_room_id == "room_one"
    assert coordinator.session.failed_room_ids == ["room_one"]
    assert coordinator.while_away_issue_messages == [
        "Could not clean Room One because it detected a ramp or fall hazard"
    ]
    assert coordinator.hass.services.calls[-1]["service"] == "return_to_base"
    assert coordinator.started_rooms == []

    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    asyncio.run(
        coordinator._async_handle_state_change_event(event_cls(coordinator.vacuum_entity, "docked"))
    )

    assert coordinator.session.failed_room_ids == []
    assert coordinator.session.failed_room_reasons == {}
    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.while_away_issue_messages == []
    assert coordinator.session.retry_room_ids == ["room_one"]
    assert coordinator.started_rooms == ["room_two"]


def test_restored_active_run_seeds_current_cleaning_observations() -> None:
    coordinator = _RecoverableFailureCoordinator()

    assert coordinator.active_run is not None
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "segment")
    coordinator.set_state("sensor.robot_estimated_segment", "1")

    changed = coordinator._restore_active_run_observations(datetime(2026, 6, 16, tzinfo=UTC))

    assert changed is True
    assert coordinator.active_run.observed_cleaning is True
    assert coordinator.active_run.observed_segment_cleaning is True
    assert coordinator.active_run.last_estimated_room_id == "room_one"


def test_state_event_seeds_restored_active_run_observations() -> None:
    coordinator = _RecoverableFailureCoordinator()
    event_cls = sys.modules["homeassistant.core"].Event

    assert coordinator.active_run is not None
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "segment")
    coordinator.set_state("sensor.robot_estimated_segment", "1")

    asyncio.run(
        coordinator._async_handle_state_change_event(event_cls("sensor.robot_area", "12"))
    )

    assert coordinator.active_run.observed_cleaning is True
    assert coordinator.active_run.observed_segment_cleaning is True


def test_manual_run_snapshots_selected_credit_entities() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.rooms = [
        logic.RoomConfig(
            room_id="room_one",
            name="Room One",
            segment_id="1",
            manual_credit_entity="input_boolean.room_one_selected",
        ),
        logic.RoomConfig(
            room_id="room_two",
            name="Room Two",
            segment_id="2",
            manual_credit_entity="input_boolean.room_two_selected",
        ),
    ]
    coordinator.set_state("input_boolean.room_one_selected", "off")
    coordinator.set_state("input_boolean.room_two_selected", "on")

    coordinator._start_manual_run(datetime(2026, 6, 20, tzinfo=UTC))

    assert coordinator.manual_run is not None
    assert coordinator.manual_run.manual_credit_room_ids == ["room_two"]


def test_room_auto_clean_disable_switch_excludes_room_from_active_session() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(session_id="session", started_at=logic.utcnow_iso())
    coordinator.set_state(coordinator.vacuum_entity, "docked")

    asyncio.run(coordinator.async_set_room_auto_clean_disabled("room_one", True))

    assert coordinator.is_room_auto_clean_disabled("room_one") is True
    assert coordinator.started_rooms == ["room_two"]
    assert [room.room_id for room in coordinator.pending_rooms] == []
