"""Tests for Valetudo Vacuum Coordinator event handling."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import importlib.util
import json
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
            self.last_changed = datetime.now(UTC)

    def callback(func):
        return func

    core_module.Event = Event
    core_module.HomeAssistant = HomeAssistant
    core_module.State = State
    core_module.CALLBACK_TYPE = object
    core_module.callback = callback

    components_module = types.ModuleType("homeassistant.components")
    binary_sensor_module = types.ModuleType("homeassistant.components.binary_sensor")

    class BinarySensorEntity:
        pass

    binary_sensor_module.BinarySensorEntity = BinarySensorEntity

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

    entity_module = types.ModuleType("homeassistant.helpers.entity")

    class DeviceInfo(dict):
        pass

    class Entity:
        pass

    entity_module.DeviceInfo = DeviceInfo
    entity_module.Entity = Entity

    entity_platform_module = types.ModuleType(
        "homeassistant.helpers.entity_platform"
    )
    entity_platform_module.AddEntitiesCallback = object

    entity_registry_module = types.ModuleType(
        "homeassistant.helpers.entity_registry"
    )
    entity_registry_module.async_get = lambda hass: None

    util_module = types.ModuleType("homeassistant.util")
    dt_module = types.ModuleType("homeassistant.util.dt")
    dt_module.utcnow = lambda: datetime.now(UTC)
    dt_module.now = lambda: datetime.now(UTC)
    util_module.dt = dt_module

    sys.modules.setdefault("homeassistant", homeassistant)
    sys.modules.setdefault("homeassistant.components", components_module)
    sys.modules.setdefault(
        "homeassistant.components.binary_sensor",
        binary_sensor_module,
    )
    sys.modules.setdefault("homeassistant.const", const_module)
    sys.modules.setdefault("homeassistant.core", core_module)
    sys.modules.setdefault("homeassistant.helpers", helpers_module)
    sys.modules.setdefault("homeassistant.helpers.event", event_module)
    sys.modules.setdefault("homeassistant.helpers.entity", entity_module)
    sys.modules.setdefault(
        "homeassistant.helpers.entity_platform",
        entity_platform_module,
    )
    sys.modules.setdefault(
        "homeassistant.helpers.entity_registry",
        entity_registry_module,
    )
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
entity_module = _load_module(
    f"{package.__name__}.entity",
    PACKAGE / "entity.py",
)
binary_sensor_module = _load_module(
    f"{package.__name__}.binary_sensor",
    PACKAGE / "binary_sensor.py",
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
        self.session = None
        self.next_room_checks = 0
        self._event_lock = asyncio.Lock()
        self._active_run_restored = False
        self._restored_dispatch_intent_deadline = None

    def _observe_active_run(self, entity_id, new_state, now) -> bool:
        return False

    def _observe_manual_run(self, entity_id, new_state, now) -> bool:
        return False

    async def _async_handle_vacuum_state(self, vacuum_state, now) -> None:
        raise AssertionError("battery changes must not be handled as vacuum state changes")

    async def _async_maybe_start_next_room(self) -> None:
        self.next_room_checks += 1

    async def _async_maybe_send_auto_clean_summary(self) -> None:
        return None


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
            const.CONF_MIN_BATTERY: 40,
            const.CONF_NATIVE_RESUME_ENABLED: True,
            const.CONF_NATIVE_RESUME_TIMEOUT: 10800,
            const.CONF_DOCK_SETTLE: 0,
            const.CONF_RESUME_NUDGE_ENABLED: False,
            const.CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL: True,
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
            command_published=True,
        )
        self.manual_run = None
        self.settings_snapshot = None
        self.while_away_outcomes = []
        self.last_error = None
        self.started_rooms = []
        self._away_timer_cancel = None
        self._next_day_timer_cancel = None
        self._terminal_cleanup_retry_cancel = None
        self._dock_settle_cancel = None
        self._native_resume_timeout_cancel = None
        self._terminal_cleanup_retry_attempts = 0
        self._event_lock = asyncio.Lock()
        self._active_run_restored = False
        self._restored_dispatch_intent_deadline = None

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
        if self.session:
            is_retry = (
                room.room_id in self.session.retry_room_ids
                or room.room_id in self.session.priority_retry_room_ids
            )
            if is_retry:
                previous_reason = self.session.failed_room_reasons.get(room.room_id)
                self.session.mark_retry_started(room.room_id)
                self.session.clear_room_issue(room.room_id)
                self._remove_while_away_failure(room.room_id, previous_reason)
            self.session.mark_attempted(room.room_id)
            self.session.active_room_id = room.room_id
            self.active_run = logic.ActiveRun(
                room_id=room.room_id,
                segment_id=room.segment_id,
                session_id=self.session.session_id,
                started_at=logic.utcnow_iso(),
                vacuum_only=vacuum_only,
            )
        self.started_rooms.append(room.room_id)


def _handle_event(
    coordinator: _RecoverableFailureCoordinator,
    entity_id: str,
    state: str,
) -> None:
    coordinator.set_state(entity_id, state)
    event_cls = sys.modules["homeassistant.core"].Event
    asyncio.run(coordinator._async_handle_state_change_event(event_cls(entity_id, state)))


def _service_names(coordinator: _RecoverableFailureCoordinator) -> list[str]:
    return [call["service"] for call in coordinator.hass.services.calls]


def _trigger_low_battery(
    coordinator: _RecoverableFailureCoordinator,
    *,
    battery: str = "15",
) -> None:
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_battery", battery)
    _handle_event(coordinator, "sensor.robot_error", "Low battery")


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

    assert coordinator.session.failed_room_ids == ["room_one"]
    assert coordinator.session.failed_room_reasons == {
        "room_one": "Unknown error 95"
    }
    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.while_away_issue_messages == [
        "Could not clean Room One because it detected a ramp or fall hazard"
    ]
    assert coordinator.session.retry_room_ids == ["room_one"]
    assert coordinator.started_rooms == ["room_two"]


def test_low_battery_suspends_same_run_without_commands() -> None:
    coordinator = _RecoverableFailureCoordinator()
    active_run = coordinator.active_run

    _trigger_low_battery(coordinator)

    assert coordinator.active_run is active_run
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.failed_room_ids == []
    assert active_run is not None
    assert active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert active_run.suspend_reason == "Low battery"
    assert active_run.interruption_count == 1
    assert active_run.recovery_deadline is not None
    assert coordinator.hass.services.calls == []


def test_stale_low_battery_event_is_ignored_after_error_clears() -> None:
    coordinator = _RecoverableFailureCoordinator()
    event_cls = sys.modules["homeassistant.core"].Event
    assert coordinator.active_run is not None
    coordinator.active_run.phase = logic.RUN_PHASE_CLEANING
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(
        coordinator._async_handle_state_change_event(
            event_cls("sensor.robot_error", "Low battery")
        )
    )

    assert coordinator.active_run.phase == logic.RUN_PHASE_CLEANING
    assert coordinator.native_resume_pending is False


def test_stale_docked_event_is_ignored_while_currently_cleaning() -> None:
    coordinator = _RecoverableFailureCoordinator()
    event_cls = sys.modules["homeassistant.core"].Event
    assert coordinator.active_run is not None
    coordinator.active_run.phase = logic.RUN_PHASE_CLEANING
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(
        coordinator._async_handle_state_change_event(
            event_cls(coordinator.vacuum_entity, "docked")
        )
    )

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CLEANING
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []


def test_stale_estimated_segment_event_is_ignored() -> None:
    coordinator = _RecoverableFailureCoordinator()
    event_cls = sys.modules["homeassistant.core"].Event
    assert coordinator.active_run is not None
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_estimated_segment", "room_one")

    asyncio.run(
        coordinator._async_handle_state_change_event(
            event_cls("sensor.robot_estimated_segment", "room_two")
        )
    )

    assert coordinator.active_run.last_estimated_room_id != "room_two"
    assert "room_two" not in coordinator.active_run.estimated_dwell_seconds


def test_dispatch_start_resumable_blip_is_ignored() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_DISPATCHING

    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")

    assert coordinator.active_run.phase == logic.RUN_PHASE_DISPATCHING
    assert coordinator.active_run.interruption_count == 0
    assert coordinator.active_run.resumable_latched is False
    assert coordinator.native_resume_pending is False

    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "segment")

    assert coordinator.active_run.phase == logic.RUN_PHASE_CLEANING
    assert coordinator.active_run.resumed_after_suspend is False
    assert coordinator.native_resume_pending is False


def test_dispatch_start_resumable_blip_is_ignored_at_dock() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    assert coordinator.active_run is not None
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_dock_status", "idle")

    asyncio.run(
        coordinator._async_reconcile_active_run_at_dock(datetime.now(UTC))
    )

    assert coordinator.active_run.phase == logic.RUN_PHASE_DISPATCHING
    assert coordinator.active_run.interruption_count == 0
    assert coordinator.active_run.resumable_latched is False
    assert coordinator.native_resume_pending is False


def test_native_resume_without_optional_status_sensor_uses_cleaning_observation() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config.pop(const.CONF_STATUS_FLAG_ENTITY)
    _trigger_low_battery(coordinator)
    coordinator.set_state("sensor.robot_error", "No error")

    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_RESUMED_CLEANING
    assert coordinator.active_run.resumed_after_suspend is True
    assert coordinator.native_resume_pending is False


def test_secondary_error_keeps_guard_until_stop_succeeds() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    original_async_call = coordinator.hass.services.async_call

    async def fail_stop(domain, service, data, blocking=False) -> None:
        if domain == "vacuum" and service == "stop":
            raise RuntimeError("stop failed")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_stop
    _handle_event(coordinator, "sensor.robot_error", "Unknown error 120")

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.session is not None
    assert coordinator.session.native_resume_guard_latched is True
    assert coordinator.native_resume_pending is True
    assert coordinator.session.terminal_reason == "needs_help"


def test_recoverable_mop_error_during_dock_interrupt_continues_queue() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_dock_status", "cleaning")

    assert coordinator.active_run.phase == logic.RUN_PHASE_DOCK_INTERRUPT
    assert coordinator.active_run.resume_required is False

    _handle_event(coordinator, "sensor.robot_error", "Clean water tank empty")

    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.needs_help is False
    assert coordinator.session.failed_room_ids == ["room_one"]
    assert _service_names(coordinator).count("stop") == 1

    coordinator.set_state("sensor.robot_error", "No error")
    _handle_event(coordinator, "sensor.robot_dock_status", "idle")

    assert coordinator.started_rooms == ["room_two"]


def test_recoverable_dock_error_retries_failed_stop_then_continues() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_dock_status", "cleaning")
    original_async_call = coordinator.hass.services.async_call
    failed_once = False

    async def fail_first_stop(domain, service, data, blocking=False) -> None:
        nonlocal failed_once
        if domain == "vacuum" and service == "stop" and not failed_once:
            failed_once = True
            raise RuntimeError("stop failed")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_first_stop
    _handle_event(coordinator, "sensor.robot_error", "Clean water tank empty")

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None

    _handle_event(coordinator, "sensor.robot_battery", "50")
    coordinator.set_state("sensor.robot_error", "No error")
    _handle_event(coordinator, "sensor.robot_dock_status", "idle")

    assert coordinator.session.active is True
    assert coordinator.started_rooms == ["room_two"]


def test_arrival_stops_suspended_task_exactly_once() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    saved_runs = []

    async def save_state() -> None:
        saved_runs.append(
            coordinator.active_run.to_dict()
            if coordinator.active_run
            else None
        )

    coordinator._async_save_store = save_state
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("person.owner", "home")

    asyncio.run(coordinator.async_cancel_session("Tracked person arrived home"))
    asyncio.run(coordinator.async_cancel_session("Tracked person arrived home"))

    assert _service_names(coordinator).count("stop") == 1
    assert _service_names(coordinator).count("return_to_base") == 0
    assert coordinator.session is not None
    assert coordinator.session.active is False
    assert coordinator.active_run is None
    assert any(
        run
        and run["phase"] == logic.RUN_PHASE_CANCEL_PENDING
        and run["cancel_requested_at"] is not None
        for run in saved_runs
    )
    assert any(
        run and run["cancel_stop_attempted"] is True
        for run in saved_runs
    )


def test_cancel_returns_idle_or_returning_robot_to_base() -> None:
    for vacuum_state in ("idle", "returning"):
        coordinator = _RecoverableFailureCoordinator()
        _trigger_low_battery(coordinator)
        coordinator.set_state(coordinator.vacuum_entity, vacuum_state)

        asyncio.run(coordinator.async_cancel_session("test cancel"))

        assert _service_names(coordinator)[-2:] == ["stop", "return_to_base"]


def test_suspended_run_blocks_queue_at_all_battery_levels_and_status_none() -> None:
    coordinator = _RecoverableFailureCoordinator()
    active_run = coordinator.active_run
    _trigger_low_battery(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_battery", "40")
    event_cls = sys.modules["homeassistant.core"].Event

    async def dispatch_burst() -> None:
        await asyncio.gather(
            coordinator._async_handle_state_change_event(
                event_cls("sensor.robot_battery", "40")
            ),
            coordinator._async_handle_state_change_event(
                event_cls("sensor.robot_status_flag", "none")
            ),
            coordinator._async_handle_state_change_event(
                event_cls("sensor.robot_dock_status", "idle")
            ),
        )

    asyncio.run(dispatch_burst())

    assert coordinator.active_run is active_run
    assert coordinator.started_rooms == []
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.hass.services.calls == []


def test_resumable_latch_survives_later_none() -> None:
    coordinator = _RecoverableFailureCoordinator()

    _trigger_low_battery(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")
    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert coordinator.active_run.resumable_latched is True
    assert coordinator.active_run.resume_required is True
    assert coordinator.active_run.docked_at is not None
    assert coordinator.started_rooms == []


def test_low_battery_suspension_survives_restart() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)

    restored = _RecoverableFailureCoordinator()
    assert coordinator.session is not None
    restored.session = logic.SessionState.from_dict(coordinator.session.to_dict())
    restored.active_run = logic.ActiveRun.from_dict(
        coordinator.active_run.to_dict() if coordinator.active_run else None
    )
    restored._active_run_restored = True
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_error", "No error")
    restored.set_state("sensor.robot_status_flag", "none")
    restored.set_state("sensor.robot_battery", "100")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.active_run is not None
    assert restored.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert restored.active_run.suspend_reason == "Low battery"
    assert restored.started_rooms == []
    assert restored.hass.services.calls == []


def test_restart_with_active_low_battery_error_stays_command_free() -> None:
    coordinator = _RecoverableFailureCoordinator()

    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_error", "Low battery")
    coordinator.set_state("sensor.robot_battery", "15")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.failed_room_ids == []
    assert coordinator.hass.services.calls == []


def test_expired_native_resume_deadline_reconciles_across_restart() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    assert coordinator.active_run is not None
    coordinator.active_run.recovery_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "unavailable")
    coordinator.set_state("sensor.robot_status_flag", "none")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.terminal_reason == "needs_help"
    assert coordinator.hass.services.calls == []


def test_cleaning_observation_is_persisted_for_restart_reconciliation() -> None:
    coordinator = _RecoverableFailureCoordinator()
    saved_active_runs = []

    async def save_state() -> None:
        saved_active_runs.append(
            coordinator.active_run.to_dict() if coordinator.active_run else None
        )

    coordinator._async_save_store = save_state
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")

    assert saved_active_runs
    assert saved_active_runs[-1]["observed_cleaning"] is True


def test_mop_rinse_dock_interrupt_survives_restart() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_dock_status", "cleaning")
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_DOCK_INTERRUPT

    restored = _RecoverableFailureCoordinator()
    assert coordinator.session is not None
    restored.session = logic.SessionState.from_dict(coordinator.session.to_dict())
    restored.active_run = logic.ActiveRun.from_dict(coordinator.active_run.to_dict())
    restored._active_run_restored = True
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_status_flag", "none")
    restored.set_state("sensor.robot_dock_status", "cleaning")
    restored.set_state("sensor.robot_error", "No error")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.active_run is not None
    assert restored.active_run.phase == logic.RUN_PHASE_DOCK_INTERRUPT
    assert restored.started_rooms == []
    assert restored.hass.services.calls == []


def test_restored_published_dispatch_latches_current_resumable_task() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.phase = logic.RUN_PHASE_DISPATCHING
    coordinator.active_run.command_published = True
    coordinator.active_run.observed_cleaning = False
    coordinator.active_run.observed_segment_cleaning = False
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert coordinator.active_run.resumable_latched is True
    assert coordinator.native_resume_pending is True
    assert coordinator.hass.services.calls == []


def test_startup_cancels_restored_native_task_when_person_is_home() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    coordinator.set_state("person.owner", "home")
    coordinator.set_state(coordinator.vacuum_entity, "idle")

    async def keep_loaded_state() -> None:
        return None

    coordinator._async_load_store = keep_loaded_state
    coordinator._unsubscribers = []
    coordinator._listeners = []

    asyncio.run(coordinator.async_setup())

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.active is False
    assert _service_names(coordinator)[-2:] == ["stop", "return_to_base"]


def test_startup_honors_arrival_cancellation_opt_out() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    coordinator.config[const.CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL] = False
    coordinator.set_state("person.owner", "home")
    coordinator.set_state(coordinator.vacuum_entity, "idle")

    async def keep_loaded_state() -> None:
        return None

    coordinator._async_load_store = keep_loaded_state
    coordinator._unsubscribers = []
    coordinator._listeners = []

    asyncio.run(coordinator.async_setup())

    assert coordinator.active_run is not None
    assert coordinator.native_resume_pending is True
    assert coordinator.hass.services.calls == []


def test_startup_between_rooms_honors_arrival_cancellation_opt_out() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=True,
    )
    coordinator.config[const.CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL] = False
    coordinator.set_state("person.owner", "home")
    coordinator.set_state(coordinator.vacuum_entity, "docked")

    async def keep_loaded_state() -> None:
        return None

    coordinator._async_load_store = keep_loaded_state
    coordinator._unsubscribers = []
    coordinator._listeners = []

    asyncio.run(coordinator.async_setup())

    assert coordinator.session.active is True
    assert coordinator.active_run is None
    assert coordinator.started_rooms == []
    assert coordinator.hass.services.calls == []


def test_opted_out_session_resumes_when_everyone_leaves_again() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=True,
    )
    coordinator.config[const.CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL] = False
    coordinator.set_state("person.owner", "home")
    _handle_event(coordinator, "person.owner", "home")

    assert coordinator.started_rooms == []

    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")
    _handle_event(coordinator, "person.owner", "not_home")

    assert coordinator.started_rooms == ["room_one"]


def test_startup_replays_recoverable_cancel_then_continues_queue() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.phase = logic.RUN_PHASE_CANCEL_PENDING
    coordinator.active_run.cancel_requested_at = logic.utcnow_iso()
    coordinator.active_run.cancel_reason = "Clean water tank empty"
    coordinator.active_run.cancel_continue_session = True
    coordinator._active_run_restored = True
    coordinator.session.attempted_room_ids = ["room_one"]
    coordinator.set_state(coordinator.vacuum_entity, "docked")

    async def keep_loaded_state() -> None:
        return None

    coordinator._async_load_store = keep_loaded_state
    coordinator._unsubscribers = []
    coordinator._listeners = []

    asyncio.run(coordinator.async_setup())

    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.started_rooms == ["room_two"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"


def test_mid_job_mop_rinse_resumes_same_active_run() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    active_run = coordinator.active_run
    assert active_run is not None
    active_run.observed_cleaning = True
    active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_dock_status", "cleaning")

    _handle_event(coordinator, "sensor.robot_dock_status", "idle")
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "segment")

    assert coordinator.active_run is active_run
    assert active_run.phase == logic.RUN_PHASE_RESUMED_CLEANING
    assert active_run.interruption_count == 1
    assert active_run.resumed_after_suspend is True
    assert coordinator.started_rooms == []
    assert coordinator.hass.services.calls == []


def test_native_cleaning_and_segment_resume_same_run_without_publish() -> None:
    coordinator = _RecoverableFailureCoordinator()
    active_run = coordinator.active_run
    _trigger_low_battery(coordinator)
    _handle_event(coordinator, "sensor.robot_error", "No error")
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "segment")

    assert coordinator.active_run is active_run
    assert active_run is not None
    assert active_run.phase == logic.RUN_PHASE_RESUMED_CLEANING
    assert active_run.resumed_after_suspend is True
    assert active_run.resume_source == "native_segment"
    assert active_run.recovery_deadline is None
    assert coordinator.started_rooms == []
    assert coordinator.hass.services.calls == []


def test_mid_job_dock_busy_and_resumable_never_complete_or_dispatch() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    _handle_event(coordinator, "sensor.robot_dock_status", "emptying")
    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")
    _handle_event(coordinator, "sensor.robot_status_flag", "none")
    _handle_event(coordinator, "sensor.robot_battery", "100")

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert coordinator.active_run.resumable_latched is True
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.started_rooms == []
    assert coordinator.hass.services.calls == []


def test_legacy_low_battery_retry_becomes_needs_help_without_republish() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Low battery"},
        pending_recovery_room_id="room_one",
        pending_recovery_reason="Low battery",
        pending_recovery_priority=True,
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "60")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "needs_help"
    assert coordinator.started_rooms == []
    assert coordinator.hass.services.calls == []


def test_native_resume_timeout_needs_help_without_restart_command() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    assert coordinator.active_run is not None
    coordinator.active_run.recovery_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(
        coordinator._async_reconcile_active_run(datetime.now(UTC))
    )

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "needs_help"
    assert coordinator.session.completed_room_ids == []
    assert coordinator.session.failed_room_ids == ["room_one"]
    assert "Native resume timed out" in (
        coordinator.session.terminal_message or ""
    )
    assert coordinator.session.native_resume_guard_latched is True
    assert coordinator.native_resume_pending is True
    assert coordinator.native_resume_attributes["phase"] == (
        logic.RUN_PHASE_RECOVERY_STALLED
    )
    assert coordinator.started_rooms == []
    assert coordinator.hass.services.calls == []


def test_timeout_defers_settings_restore_until_guard_is_cancelled() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="original"
    )
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    _trigger_low_battery(coordinator)
    assert coordinator.active_run is not None
    coordinator.active_run.recovery_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(coordinator._async_reconcile_active_run(datetime.now(UTC)))

    assert coordinator.settings_snapshot is not None
    assert "select_option" not in _service_names(coordinator)

    for battery in ("16", "17", "18"):
        _handle_event(coordinator, "sensor.robot_battery", battery)

    assert coordinator._terminal_cleanup_retry_attempts == 0
    assert coordinator.settings_snapshot is not None

    coordinator.set_state(coordinator.vacuum_entity, "docked")
    asyncio.run(coordinator.async_cancel_session("test cancel"))

    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("select_option") == 1


def test_arrival_clears_timed_out_native_resume_guard() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    assert coordinator.active_run is not None
    coordinator.active_run.recovery_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.set_state("sensor.robot_error", "No error")
    asyncio.run(coordinator._async_reconcile_active_run(datetime.now(UTC)))

    coordinator.set_state(coordinator.vacuum_entity, "idle")
    coordinator.set_state("person.owner", "home")
    asyncio.run(coordinator.async_cancel_session("Tracked person arrived home"))

    assert _service_names(coordinator)[-2:] == ["stop", "return_to_base"]
    assert coordinator.session is not None
    assert coordinator.session.native_resume_guard_latched is False
    assert coordinator.native_resume_pending is False


def test_latched_guard_cancellation_retries_failed_stop_durably() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    assert coordinator.active_run is not None
    coordinator.active_run.recovery_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.set_state("sensor.robot_error", "No error")
    asyncio.run(coordinator._async_reconcile_active_run(datetime.now(UTC)))
    coordinator.set_state(coordinator.vacuum_entity, "idle")
    original_async_call = coordinator.hass.services.async_call
    stop_attempts = 0

    async def fail_first_stop(domain, service, data, blocking=False) -> None:
        nonlocal stop_attempts
        if domain == "vacuum" and service == "stop":
            stop_attempts += 1
            if stop_attempts == 1:
                raise RuntimeError("stop failed")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_first_stop
    asyncio.run(coordinator.async_cancel_session("test cancel"))

    assert coordinator.session is not None
    assert coordinator.session.native_resume_guard_latched is True
    assert coordinator.session.native_guard_cancel_pending is True
    assert coordinator.session.native_guard_stop_confirmed is False

    _handle_event(coordinator, "sensor.robot_battery", "50")

    assert stop_attempts == 2
    assert coordinator.session.native_resume_guard_latched is False
    assert coordinator.session.native_guard_cancel_pending is False
    assert _service_names(coordinator)[-2:] == ["stop", "return_to_base"]


def test_latched_guard_cancel_intent_is_saved_before_stop() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    assert coordinator.active_run is not None
    coordinator.active_run.recovery_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.set_state("sensor.robot_error", "No error")
    asyncio.run(coordinator._async_reconcile_active_run(datetime.now(UTC)))
    coordinator.set_state(coordinator.vacuum_entity, "idle")
    saved_pending = []
    original_async_call = coordinator.hass.services.async_call

    async def save_state() -> None:
        saved_pending.append(
            bool(
                coordinator.session
                and coordinator.session.native_guard_cancel_pending
            )
        )

    async def assert_saved_before_stop(domain, service, data, blocking=False) -> None:
        if domain == "vacuum" and service == "stop":
            assert any(saved_pending)
        await original_async_call(domain, service, data, blocking)

    coordinator._async_save_store = save_state
    coordinator.hass.services.async_call = assert_saved_before_stop

    asyncio.run(coordinator.async_cancel_session("test cancel"))

    assert any(saved_pending)
    assert coordinator.session is not None
    assert coordinator.session.native_guard_cancel_pending is False


def test_successful_cancel_retry_reconciles_departure_event() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "idle")
    coordinator.set_state("person.owner", "home")
    original_async_call = coordinator.hass.services.async_call
    failed_once = False

    async def fail_first_stop(domain, service, data, blocking=False) -> None:
        nonlocal failed_once
        if domain == "vacuum" and service == "stop" and not failed_once:
            failed_once = True
            raise RuntimeError("stop failed")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_first_stop
    asyncio.run(coordinator.async_cancel_session("Tracked person arrived home"))

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING

    _handle_event(coordinator, "person.owner", "not_home")

    assert coordinator.active_run is None
    assert coordinator.away_since is not None
    assert coordinator._away_timer_cancel is not None


def test_startup_replays_pending_latched_guard_cancellation() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="needs_help",
        native_resume_guard_latched=True,
        native_guard_cancel_pending=True,
        native_guard_cancel_reason="test cancel",
    )
    coordinator.set_state(coordinator.vacuum_entity, "idle")

    async def keep_loaded_state() -> None:
        return None

    coordinator._async_load_store = keep_loaded_state
    coordinator._unsubscribers = []
    coordinator._listeners = []

    asyncio.run(coordinator.async_setup())

    assert _service_names(coordinator)[-2:] == ["stop", "return_to_base"]
    assert coordinator.session.native_resume_guard_latched is False
    assert coordinator.session.native_guard_cancel_pending is False


def test_timed_out_native_resume_guard_blocks_new_session_and_manual_adoption() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    assert coordinator.active_run is not None
    coordinator.active_run.recovery_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.set_state("sensor.robot_error", "No error")
    asyncio.run(coordinator._async_reconcile_active_run(datetime.now(UTC)))

    coordinator.started_rooms.clear()
    asyncio.run(coordinator.async_start_session("away timer"))
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")

    assert coordinator.started_rooms == []
    assert coordinator.manual_run is None
    assert coordinator.session is not None
    assert coordinator.session.native_resume_guard_latched is True


def test_cancel_failure_keeps_guard_and_retries_unconfirmed_stop() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "idle")
    original_async_call = coordinator.hass.services.async_call
    stop_attempts = 0

    async def fail_first_stop(domain, service, data, blocking=False) -> None:
        nonlocal stop_attempts
        if domain == "vacuum" and service == "stop":
            stop_attempts += 1
            if stop_attempts == 1:
                raise RuntimeError("stop failed")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_first_stop
    asyncio.run(coordinator.async_cancel_session("test cancel"))

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.active_run.cancel_stop_attempted is False
    assert coordinator.native_resume_pending is True

    original_session = coordinator.session
    asyncio.run(coordinator.async_start_session("away timer"))

    assert coordinator.session is original_session
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING

    _handle_event(coordinator, "sensor.robot_battery", "50")

    assert stop_attempts == 2
    assert coordinator.active_run is None
    assert coordinator.native_resume_pending is False
    assert _service_names(coordinator)[-2:] == ["stop", "return_to_base"]


def test_completed_restored_run_is_not_misclassified_as_low_battery() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "38")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.session.failed_room_ids == []
    assert coordinator.active_run is None
    assert [room.room_id for room in coordinator.pending_rooms] == ["room_two"]
    assert "stop" not in _service_names(coordinator)


def test_low_battery_native_resume_does_not_require_battery_sensor() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config.pop(const.CONF_BATTERY_ENTITY)

    _trigger_low_battery(coordinator)

    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.failed_room_ids == []
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert coordinator.hass.services.calls == []


def test_low_battery_first_persisted_state_keeps_same_active_run() -> None:
    coordinator = _RecoverableFailureCoordinator()
    saved_states = []

    async def save_state() -> None:
        saved_states.append(
            {
                "session": coordinator.session.to_dict() if coordinator.session else None,
                "active_run": (
                    coordinator.active_run.to_dict()
                    if coordinator.active_run
                    else None
                ),
            }
        )

    coordinator._async_save_store = save_state
    _trigger_low_battery(coordinator)

    assert saved_states
    assert saved_states[-1]["session"]["active"] is True
    assert saved_states[-1]["active_run"]["phase"] == logic.RUN_PHASE_SUSPENDED
    assert saved_states[-1]["active_run"]["room_id"] == "room_one"


def test_resume_nudge_disabled_never_calls_vacuum_start() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_RESUME_NUDGE_ENABLED] = False
    _trigger_low_battery(coordinator)
    _handle_event(coordinator, "sensor.robot_error", "No error")
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "segment")

    assert "start" not in _service_names(coordinator)
    assert "publish" not in _service_names(coordinator)
    assert coordinator.started_rooms == []
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_RESUMED_CLEANING


def test_disabled_native_resume_needs_help_without_recovery_command() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_NATIVE_RESUME_ENABLED] = False

    _trigger_low_battery(coordinator)

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.terminal_reason == "needs_help"
    assert coordinator.session.completed_room_ids == []
    assert coordinator.hass.services.calls == []


def test_statistics_accumulate_across_native_resume_counter_reset() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.start_time = 0
    coordinator.active_run.last_time = 0
    coordinator.set_state("sensor.robot_time", "100")
    _trigger_low_battery(coordinator)
    coordinator.set_state("sensor.robot_time", "0")
    _handle_event(coordinator, "sensor.robot_error", "No error")
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "segment")
    coordinator.set_state("sensor.robot_time", "30")
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.ledgers["room_one"].successful_count == 1
    assert coordinator.hass.services.calls == []


def test_dock_settle_prevents_millisecond_premature_completion() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")

    assert coordinator.active_run is not None
    assert coordinator.active_run.docked_at is not None
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.started_rooms == []


def test_normal_final_dock_completes_after_stable_settle() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.active_run.docked_at = (
        datetime.now(UTC) - timedelta(seconds=61)
    ).isoformat()
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")

    asyncio.run(
        coordinator._async_reconcile_active_run(datetime.now(UTC))
    )

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"
    assert coordinator.started_rooms == ["room_two"]


def test_final_mop_drying_does_not_trigger_native_resume_timeout() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.active_run.docked_at = (
        datetime.now(UTC) - timedelta(seconds=61)
    ).isoformat()
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "drying")

    asyncio.run(coordinator._async_reconcile_active_run(datetime.now(UTC)))

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.session.needs_help is False
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"


def test_dominant_wrong_room_estimated_dwell_is_never_credited() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.rooms[0] = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        require_estimated_segment=True,
        min_estimated_dwell=30,
    )
    coordinator.room_by_id["room_one"] = coordinator.rooms[0]
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.active_run.estimated_dwell_seconds = {
        "room_one": 30,
        "room_two": 90,
    }
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.session.failed_room_ids == ["room_one"]
    assert coordinator.session.terminal_reason == "needs_help"
    assert coordinator.ledgers["room_one"].successful_count == 0
    assert coordinator.started_rooms == []
    assert coordinator.hass.services.calls == []


def test_unresolvable_room_does_not_trigger_wrong_room_needs_help() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.active_run.estimated_dwell_seconds = {"room_two": 90}
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")
    assert coordinator.active_run is not None
    coordinator.active_run.docked_at = (
        datetime.now(UTC) - timedelta(seconds=61)
    ).isoformat()
    asyncio.run(coordinator._async_reconcile_active_run(datetime.now(UTC)))

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.session.needs_help is False


def test_restored_downtime_is_not_counted_as_wrong_room_dwell() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.rooms[0] = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        require_estimated_segment=True,
        min_estimated_dwell=30,
    )
    coordinator.room_by_id["room_one"] = coordinator.rooms[0]
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.active_run.estimated_dwell_seconds = {"room_one": 300}
    coordinator.active_run.last_estimated_room_id = "room_two"
    coordinator.active_run.last_estimated_changed_at = (
        datetime.now(UTC) - timedelta(minutes=40)
    ).isoformat()
    coordinator._active_run_restored = True
    coordinator.config[const.CONF_DOCK_SETTLE] = 0
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(
        coordinator._async_reconcile_active_run_at_dock(datetime.now(UTC))
    )

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.session.needs_help is False


def test_native_resume_binary_sensor_and_attributes() -> None:
    coordinator = _RecoverableFailureCoordinator()
    sensor = binary_sensor_module.ValetudoNativeResumePendingBinarySensor(
        coordinator
    )

    _trigger_low_battery(coordinator)

    assert sensor.is_on is True
    assert sensor.extra_state_attributes["native_resume_pending"] is True
    assert sensor.extra_state_attributes["phase"] == logic.RUN_PHASE_SUSPENDED
    assert sensor.extra_state_attributes["suspended_at"] is not None
    assert sensor.extra_state_attributes["suspend_reason"] == "Low battery"
    assert sensor.extra_state_attributes["interruption_count"] == 1
    assert sensor.extra_state_attributes["native_resume_observed"] is False

    _handle_event(coordinator, "sensor.robot_error", "No error")
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "segment")

    assert sensor.is_on is False
    assert sensor.extra_state_attributes["native_resume_pending"] is False
    assert sensor.extra_state_attributes["phase"] == (
        logic.RUN_PHASE_RESUMED_CLEANING
    )
    assert sensor.extra_state_attributes["resume_source"] == "native_segment"
    assert sensor.extra_state_attributes["native_resume_observed"] is True


def test_native_resume_guard_transitions_notify_listeners() -> None:
    coordinator = _RecoverableFailureCoordinator()
    callbacks = 0

    def listener() -> None:
        nonlocal callbacks
        callbacks += 1

    coordinator._notify_listeners = listener
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_dock_status", "cleaning")

    pending_callbacks = callbacks
    assert pending_callbacks > 0
    assert coordinator.native_resume_pending is True

    _handle_event(coordinator, "sensor.robot_dock_status", "idle")
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "segment")

    assert callbacks > pending_callbacks
    assert coordinator.native_resume_pending is False


def test_configured_unavailable_battery_blocks_normal_dispatch() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")

    _handle_event(coordinator, "sensor.robot_battery", "unavailable")

    assert coordinator.started_rooms == []


def test_configured_unavailable_dock_status_blocks_dispatch() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "unavailable")

    _handle_event(coordinator, "sensor.robot_battery", "100")

    assert coordinator.started_rooms == []


def test_normal_dispatch_works_without_status_flag_sensor() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config.pop(const.CONF_STATUS_FLAG_ENTITY)
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")

    _handle_event(coordinator, "sensor.robot_battery", "100")

    assert coordinator.started_rooms == ["room_one"]


def test_completion_works_without_optional_status_flag_sensor() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config.pop(const.CONF_STATUS_FLAG_ENTITY)
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"


def test_docked_run_finalizes_when_error_sensor_recovers() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_error", "unavailable")

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")

    assert coordinator.active_run is not None

    _handle_event(coordinator, "sensor.robot_error", "No error")

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"


def test_configured_unavailable_status_waits_before_completion() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "unavailable")

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")

    assert coordinator.active_run is not None

    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]


def test_configured_unavailable_dock_status_waits_before_completion() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "unavailable")

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")

    assert coordinator.active_run is not None

    _handle_event(coordinator, "sensor.robot_dock_status", "idle")

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]


def test_restored_run_waits_for_configured_status_sensor_recovery() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "unavailable")
    coordinator.set_state("sensor.robot_battery", "100")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.active_run is not None

    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]


def test_publish_failure_is_durable_needs_help() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_battery", "40")
    saved = []

    async def save_state() -> None:
        saved.append(
            {
                "session": coordinator.session.to_dict(),
                "active_run": (
                    coordinator.active_run.to_dict()
                    if coordinator.active_run
                    else None
                ),
            }
        )

    async def fail_publish(domain, service, data, blocking=False) -> None:
        if domain == "mqtt" and service == "publish":
            raise RuntimeError("broker unavailable")
        await _FakeServices.async_call(
            coordinator.hass.services,
            domain,
            service,
            data,
            blocking,
        )

    coordinator._async_save_store = save_state
    coordinator.hass.services.async_call = fail_publish

    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room(
            coordinator,
            coordinator.room_by_id["room_one"],
            vacuum_only=True,
        )
    )

    assert coordinator.active_run is None
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "needs_help"
    assert coordinator.session.priority_retry_room_ids == []
    assert coordinator.session.failed_room_ids == ["room_one"]
    assert "Could not dispatch Room One" in (
        coordinator.session.failed_room_reasons["room_one"]
    )
    assert saved[-1]["active_run"] is None
    assert saved[-1]["session"]["terminal_reason"] == "needs_help"


def test_restored_unpublished_dispatch_waits_then_requeues() -> None:
    restored = _RecoverableFailureCoordinator()
    restored.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active_room_id="room_one",
    )
    restored.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        command_published=False,
    )
    restored._active_run_restored = True
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_status_flag", "none")
    restored.set_state("sensor.robot_error", "No error")
    restored.set_state("sensor.robot_battery", "100")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.active_run is not None
    assert restored.started_rooms == []

    _handle_event(restored, "sensor.robot_battery", "100")

    assert restored.active_run is not None
    assert restored.started_rooms == []

    restored._restored_dispatch_intent_deadline = datetime.now(UTC) - timedelta(
        seconds=1
    )
    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.started_rooms == ["room_one"]
    assert restored.active_run is not None
    assert restored.active_run.room_id == "room_one"


def test_restored_unpublished_dispatch_handles_current_error_first() -> None:
    restored = _RecoverableFailureCoordinator()
    restored.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active_room_id="room_one",
    )
    restored.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        command_published=False,
    )
    restored._active_run_restored = True
    restored.set_state(restored.vacuum_entity, "error")
    restored.set_state("sensor.robot_error", "Robot is stuck")
    restored.set_state("sensor.robot_battery", "15")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.active_run is None
    assert restored.session.pending_recovery_room_id is None
    assert restored.session.failed_room_reasons == {"room_one": "Robot is stuck"}
    assert restored.session.terminal_reason == "needs_help"
    assert _service_names(restored) == ["return_to_base"]


def test_restored_published_dispatch_is_adopted_from_cleaning_state() -> None:
    restored = _RecoverableFailureCoordinator()
    restored.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Low battery"},
        priority_retry_room_ids=["room_one"],
        active_room_id="room_one",
    )
    restored.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        command_published=False,
    )
    restored._active_run_restored = True
    restored.set_state(restored.vacuum_entity, "cleaning")
    restored.set_state("sensor.robot_status_flag", "segment")
    restored.set_state("sensor.robot_error", "No error")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.active_run is not None
    assert restored.active_run.command_published is True
    assert restored.session.priority_retry_room_ids == []
    assert restored.session.retried_room_ids == ["room_one"]


def test_resource_change_before_publish_aborts_and_clear_event_retries() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DUSTBAG_ENTITY] = "sensor.robot_dustbag"
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Unknown error 95"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_battery", "40")
    coordinator.set_state("sensor.robot_dustbag", "ok")
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    async def block_with_full_dustbag(*, vacuum_only: bool) -> None:
        coordinator.set_state("sensor.robot_dustbag", "full")

    coordinator._async_apply_mode = block_with_full_dustbag

    asyncio.run(
        coordinator._async_start_room(
            coordinator.room_by_id["room_one"],
            vacuum_only=True,
        )
    )

    assert "publish" not in _service_names(coordinator)
    assert coordinator.active_run is None
    assert coordinator.session.priority_retry_room_ids == ["room_one"]
    assert coordinator.session.failed_room_reasons == {
        "room_one": "Unknown error 95"
    }

    async def apply_mode(*, vacuum_only: bool) -> None:
        return None

    coordinator._async_apply_mode = apply_mode
    _handle_event(coordinator, "sensor.robot_dustbag", "ok")

    assert _service_names(coordinator).count("publish") == 1
    assert coordinator.active_run is not None
    assert coordinator.session.retried_room_ids == ["room_one"]
    assert coordinator.session.failed_room_ids == []
    publish_call = next(
        call
        for call in coordinator.hass.services.calls
        if call["domain"] == "mqtt" and call["service"] == "publish"
    )
    assert json.loads(publish_call["data"]["payload"])["segment_ids"] == ["1"]


def test_room_specific_resource_block_waits_then_starts_ordinary_room() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.rooms[0] = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        mop_required=True,
    )
    coordinator.room_by_id["room_one"] = coordinator.rooms[0]
    coordinator.config[const.CONF_DIRTY_WATER_ENTITY] = "sensor.robot_dirty_water"
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.disabled_room_ids = {"room_two"}
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")
    coordinator.set_state("sensor.robot_dirty_water", "full")

    _handle_event(coordinator, "sensor.robot_dirty_water", "full")

    assert coordinator.started_rooms == []
    assert coordinator.session.active is True
    assert coordinator.session.attempted_room_ids == []
    assert coordinator.session.skipped_room_ids == []

    _handle_event(coordinator, "sensor.robot_dirty_water", "ok")

    assert coordinator.started_rooms == ["room_one"]


def test_recoverable_mop_error_allows_vacuum_only_queue_progress() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.set_state("sensor.robot_error", "Unknown error 120")

    _handle_event(coordinator, "sensor.robot_error", "Unknown error 120")

    assert coordinator.session is not None
    assert coordinator.session.failed_room_ids == ["room_one"]
    assert _service_names(coordinator)[-1] == "stop"

    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    _handle_event(coordinator, coordinator.vacuum_entity, "docked")

    assert coordinator.started_rooms == ["room_two"]


def test_error_change_before_publish_aborts_then_supersedes_recovery() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Unknown error 95"},
        pending_recovery_room_id="room_one",
        pending_recovery_reason="Unknown error 95",
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_battery", "40")

    async def apply_mode_with_error(*, vacuum_only: bool) -> None:
        coordinator.set_state("sensor.robot_error", "Robot is stuck")

    coordinator._async_apply_mode = apply_mode_with_error

    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room(
            coordinator,
            coordinator.room_by_id["room_one"],
            vacuum_only=True,
        )
    )

    assert "publish" not in _service_names(coordinator)
    assert coordinator.session.pending_recovery_room_id == "room_one"
    assert coordinator.session.failed_room_reasons == {
        "room_one": "Unknown error 95"
    }

    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.failed_room_reasons == {"room_one": "Robot is stuck"}
    assert coordinator.session.terminal_reason == "needs_help"


def test_unavailable_error_before_publish_aborts_and_clear_event_retries() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Unknown error 95"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_battery", "40")
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    async def apply_mode_with_unavailable_error(*, vacuum_only: bool) -> None:
        coordinator.set_state("sensor.robot_error", "unavailable")

    coordinator._async_apply_mode = apply_mode_with_unavailable_error

    asyncio.run(
        coordinator._async_start_room(
            coordinator.room_by_id["room_one"],
            vacuum_only=True,
        )
    )

    assert "publish" not in _service_names(coordinator)
    assert coordinator.session.priority_retry_room_ids == ["room_one"]
    assert coordinator.session.failed_room_reasons == {
        "room_one": "Unknown error 95"
    }

    async def apply_mode(*, vacuum_only: bool) -> None:
        return None

    coordinator._async_apply_mode = apply_mode
    _handle_event(coordinator, "sensor.robot_error", "No error")

    assert _service_names(coordinator).count("publish") == 1
    assert coordinator.session.retried_room_ids == ["room_one"]
    assert coordinator.session.failed_room_ids == []


def test_nonrecoverable_error_is_terminal_in_first_persisted_failure_state() -> None:
    coordinator = _RecoverableFailureCoordinator()
    saved_sessions = []

    async def save_state() -> None:
        saved_sessions.append(
            coordinator.session.to_dict() if coordinator.session else None
        )

    coordinator._async_save_store = save_state
    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    assert saved_sessions
    assert saved_sessions[0]["active"] is False
    assert saved_sessions[0]["terminal_reason"] == "needs_help"


def test_terminal_session_ignores_stale_recovery_errors() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Low battery"},
        priority_retry_room_ids=["room_one"],
        terminal_reason="complete",
    )
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")

    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    assert coordinator.hass.services.calls == []
    assert coordinator.session.pending_recovery_room_id is None


def test_disabled_priority_retry_is_dropped_before_selecting_next_room() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Unknown error 95"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.disabled_room_ids = {"room_one"}
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")

    _handle_event(coordinator, "sensor.robot_battery", "100")

    assert coordinator.started_rooms == ["room_two"]
    assert coordinator.session.priority_retry_room_ids == []


def test_presence_change_before_publish_preserves_existing_retry_failure() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Unknown error 95"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_battery", "40")

    async def apply_mode_with_arrival(*, vacuum_only: bool) -> None:
        coordinator.set_state("person.owner", "home")

    coordinator._async_apply_mode = apply_mode_with_arrival

    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room(
            coordinator,
            coordinator.room_by_id["room_one"],
            vacuum_only=True,
        )
    )

    assert "publish" not in _service_names(coordinator)
    assert coordinator.active_run is None
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "returned_home"
    assert coordinator.session.failed_room_reasons == {
        "room_one": "Unknown error 95"
    }
    assert coordinator.session.retried_room_ids == []


def test_public_cancel_waits_for_mqtt_publish_then_stops_command() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = None
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_battery", "100")
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )
    publish_started = asyncio.Event()
    release_publish = asyncio.Event()
    original_async_call = coordinator.hass.services.async_call

    async def block_publish(domain, service, data, blocking=False) -> None:
        if domain == "mqtt" and service == "publish":
            publish_started.set()
            await release_publish.wait()
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = block_publish

    async def run_scenario() -> None:
        start_task = asyncio.create_task(coordinator.async_start_session("test"))
        await publish_started.wait()
        cancel_task = asyncio.create_task(
            coordinator.async_cancel_session("test cancel")
        )
        await asyncio.sleep(0)
        assert cancel_task.done() is False
        release_publish.set()
        await start_task
        await cancel_task

    asyncio.run(run_scenario())

    assert coordinator.session is not None
    assert coordinator.session.active is False
    assert coordinator.active_run is None
    assert _service_names(coordinator).count("publish") == 1
    assert _service_names(coordinator).count("stop") == 1


def test_terminal_summary_retries_settings_restore_without_duplicate_notice() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Low battery"},
        terminal_reason="blocked",
        terminal_message="Low battery",
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(mode="original")
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    failed_once = False
    original_async_call = coordinator.hass.services.async_call

    async def fail_first_restore(domain, service, data, blocking=False) -> None:
        nonlocal failed_once
        if service == "select_option" and not failed_once:
            failed_once = True
            raise RuntimeError("select unavailable")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_first_restore

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.session.notification_sent is True
    assert coordinator.settings_snapshot is not None
    assert coordinator.auto_cleaning is True
    assert _service_names(coordinator).count("household") == 1
    assert coordinator._terminal_cleanup_retry_cancel is not None

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.settings_snapshot is None
    assert coordinator.auto_cleaning is False
    assert _service_names(coordinator).count("household") == 1
    assert _service_names(coordinator).count("select_option") == 1
    assert coordinator._terminal_cleanup_retry_cancel is None


def test_terminal_cleanup_accepts_idle_state() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="complete",
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="original"
    )
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.set_state(coordinator.vacuum_entity, "idle")
    coordinator.set_state("sensor.robot_status_flag", "none")

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.session.notification_sent is True
    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("select_option") == 1


def test_notification_failure_does_not_block_settings_restore() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Low battery"},
        terminal_reason="blocked",
        terminal_message="Low battery",
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(mode="original")
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    original_async_call = coordinator.hass.services.async_call
    fail_notification = True

    async def fail_then_recover(domain, service, data, blocking=False) -> None:
        if domain == "notify" and service == "household" and fail_notification:
            raise RuntimeError("notify unavailable")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_then_recover

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.session.notification_sent is False
    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("select_option") == 1
    assert coordinator._terminal_cleanup_retry_cancel is not None

    fail_notification = False
    asyncio.run(coordinator._async_retry_terminal_cleanup_serialized())

    assert coordinator.session.notification_sent is True
    assert coordinator._terminal_cleanup_retry_cancel is None
    notify_call = next(
        call
        for call in coordinator.hass.services.calls
        if call["domain"] == "notify"
    )
    assert notify_call["blocking"] is True


def test_terminal_cleanup_budget_applies_to_sensor_events() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="blocked",
        terminal_message="Low battery",
    )
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    original_async_call = coordinator.hass.services.async_call

    async def fail_notification(domain, service, data, blocking=False) -> None:
        if domain == "notify" and service == "household":
            await original_async_call(domain, service, data, blocking)
            raise RuntimeError("notify unavailable")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_notification

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())
    for battery in ("99", "98", "97", "96", "95"):
        _handle_event(coordinator, "sensor.robot_battery", battery)

    assert _service_names(coordinator).count("household") == 3
    assert coordinator._terminal_cleanup_retry_attempts == 3


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
