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


def test_low_battery_stops_task_and_reissues_same_room_at_threshold() -> None:
    coordinator = _RecoverableFailureCoordinator()

    _trigger_low_battery(coordinator)

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.pending_recovery_room_id == "room_one"
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}
    assert _service_names(coordinator) == ["stop", "return_to_base"]
    assert all(call["blocking"] for call in coordinator.hass.services.calls)

    _handle_event(coordinator, coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")
    _handle_event(coordinator, "sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "21")
    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert coordinator.started_rooms == []
    assert _service_names(coordinator).count("stop") == 1

    _handle_event(coordinator, "sensor.robot_battery", "40")

    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_one"
    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.priority_retry_room_ids == []
    assert coordinator.session.retried_room_ids == ["room_one"]
    assert coordinator.session.failed_room_ids == []
    assert "start" not in _service_names(coordinator)


def test_arrival_stops_pending_low_battery_resumable_task_again() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("person.owner", "home")

    asyncio.run(coordinator.async_cancel_session("Tracked person arrived home"))

    assert _service_names(coordinator).count("stop") == 2
    assert coordinator.session is not None
    assert coordinator.session.active is False


def test_recovery_event_burst_dispatches_priority_room_once() -> None:
    coordinator = _RecoverableFailureCoordinator()
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

    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_one"


def test_second_low_battery_hit_waits_for_charge_then_continues_queue() -> None:
    coordinator = _RecoverableFailureCoordinator()

    _trigger_low_battery(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    _handle_event(coordinator, "sensor.robot_battery", "40")

    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.session is not None
    assert coordinator.session.retried_room_ids == ["room_one"]

    _trigger_low_battery(coordinator)

    assert coordinator.session.pending_recovery_room_id == "room_one"
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}
    assert _service_names(coordinator).count("stop") == 2
    assert _service_names(coordinator).count("return_to_base") == 2

    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    _handle_event(coordinator, "sensor.robot_battery", "40")

    assert coordinator.started_rooms == ["room_one", "room_two"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"
    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.priority_retry_room_ids == []
    assert coordinator.session.retried_room_ids == ["room_one"]
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}


def test_low_battery_pending_recovery_survives_restart() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _trigger_low_battery(coordinator)

    assert coordinator.session is not None
    restored = _RecoverableFailureCoordinator()
    restored.session = logic.SessionState.from_dict(coordinator.session.to_dict())
    restored.active_run = None
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_error", "No error")
    restored.set_state("sensor.robot_status_flag", "resumable")
    restored.set_state("sensor.robot_battery", "21")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.started_rooms == []
    assert restored.session is not None
    assert restored.session.pending_recovery_room_id == "room_one"

    _handle_event(restored, "sensor.robot_battery", "40")

    assert restored.started_rooms == ["room_one"]
    assert _service_names(restored).count("stop") == 0


def test_restart_with_active_low_battery_error_cancels_native_task() -> None:
    coordinator = _RecoverableFailureCoordinator()

    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_error", "Low battery")
    coordinator.set_state("sensor.robot_battery", "15")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.pending_recovery_room_id == "room_one"
    assert _service_names(coordinator) == ["stop", "return_to_base"]


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


def test_restart_waits_for_late_sensors_then_infers_low_battery() -> None:
    coordinator = _RecoverableFailureCoordinator()

    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "unavailable")
    coordinator.set_state("sensor.robot_battery", "unavailable")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.active_run is not None
    assert coordinator.hass.services.calls == []

    _handle_event(coordinator, "sensor.robot_error", "No error")

    assert coordinator.active_run is not None

    _handle_event(coordinator, "sensor.robot_battery", "15")

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.pending_recovery_room_id == "room_one"
    assert _service_names(coordinator) == ["stop"]

    _handle_event(coordinator, "sensor.robot_battery", "40")

    assert coordinator.started_rooms == ["room_one"]


def test_charged_restored_resumable_run_is_stopped_and_retried() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "60")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.session is not None
    assert coordinator.session.retried_room_ids == ["room_one"]


def test_published_restored_resumable_run_without_observations_is_recovered() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = False
    coordinator.active_run.observed_segment_cleaning = False
    coordinator.active_run.command_published = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "60")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.started_rooms == ["room_one"]


def test_restored_pending_recovery_reconciles_existing_actual_error() -> None:
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
    coordinator.set_state("sensor.robot_error", "Robot is stuck")
    coordinator.set_state("sensor.robot_battery", "60")

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.failed_room_reasons == {
        "room_one": "Robot is stuck"
    }
    assert coordinator.session.terminal_reason == "needs_help"


def test_actual_error_supersedes_inferred_low_battery_recovery() -> None:
    coordinator = _RecoverableFailureCoordinator()

    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "No error")
    _handle_event(coordinator, "sensor.robot_battery", "15")

    assert coordinator.session is not None
    assert coordinator.session.pending_recovery_reason == "Low battery"

    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.priority_retry_room_ids == []
    assert coordinator.session.failed_room_reasons == {"room_one": "Robot is stuck"}
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "needs_help"


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


def test_missing_battery_sensor_turns_low_battery_into_needs_help() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config.pop(const.CONF_BATTERY_ENTITY)

    _trigger_low_battery(coordinator)

    assert coordinator.session is not None
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "needs_help"
    assert coordinator.session.terminal_message == (
        "Low-battery recovery requires a configured battery_entity"
    )
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}
    assert _service_names(coordinator)[:2] == ["stop", "return_to_base"]


def test_missing_battery_needs_help_is_first_persisted_failure_state() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config.pop(const.CONF_BATTERY_ENTITY)
    saved_sessions = []

    async def save_state() -> None:
        saved_sessions.append(
            coordinator.session.to_dict() if coordinator.session else None
        )

    coordinator._async_save_store = save_state
    _trigger_low_battery(coordinator)

    assert saved_sessions
    assert all(
        saved is None
        or (
            saved["active"] is False
            and saved["terminal_reason"] == "needs_help"
        )
        for saved in saved_sessions
    )


def test_failed_low_battery_stop_is_terminal_instead_of_dispatching() -> None:
    coordinator = _RecoverableFailureCoordinator()
    original_async_call = coordinator.hass.services.async_call

    async def fail_stop(domain, service, data, blocking=False) -> None:
        if service == "stop":
            raise RuntimeError("stop unavailable")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_stop
    _trigger_low_battery(coordinator)

    assert coordinator.session is not None
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "needs_help"
    assert "Could not cancel the low-battery task" in (
        coordinator.session.terminal_message or ""
    )
    assert coordinator.started_rooms == []
    assert _service_names(coordinator) == ["return_to_base"]


def test_failed_stop_needs_help_is_first_persisted_failure_state() -> None:
    coordinator = _RecoverableFailureCoordinator()
    saved_sessions = []
    original_async_call = coordinator.hass.services.async_call

    async def fail_stop(domain, service, data, blocking=False) -> None:
        if service == "stop":
            raise RuntimeError("stop unavailable")
        await original_async_call(domain, service, data, blocking)

    async def save_state() -> None:
        saved_sessions.append(
            coordinator.session.to_dict() if coordinator.session else None
        )

    coordinator.hass.services.async_call = fail_stop
    coordinator._async_save_store = save_state
    _trigger_low_battery(coordinator)

    assert saved_sessions
    assert all(
        saved is None
        or (
            saved["active"] is False
            and saved["terminal_reason"] == "needs_help"
        )
        for saved in saved_sessions
    )


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
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Low battery"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
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
    restored.set_state("sensor.robot_error", "Low battery")
    restored.set_state("sensor.robot_battery", "15")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.active_run is None
    assert restored.session.pending_recovery_room_id == "room_one"
    assert restored.session.failed_room_reasons == {"room_one": "Low battery"}
    assert _service_names(restored)[:2] == ["stop", "return_to_base"]


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
        failed_room_reasons={"room_one": "Low battery"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
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
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}

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
        failed_room_reasons={"room_one": "Low battery"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
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
    assert coordinator.session.priority_retry_room_ids == ["room_one"]
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}

    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    assert coordinator.session.priority_retry_room_ids == []
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
        failed_room_reasons={"room_one": "Low battery"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
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
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}

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
        failed_room_reasons={"room_one": "Low battery"},
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


def test_presence_change_before_publish_preserves_low_battery_failure() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={"room_one": "Low battery"},
        priority_retry_room_ids=["room_one"],
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
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
    assert coordinator.session.failed_room_reasons == {"room_one": "Low battery"}
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
    coordinator.set_state("sensor.robot_status_flag", "resumable")
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
