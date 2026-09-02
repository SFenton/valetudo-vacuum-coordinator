"""Tests for Valetudo Vacuum Coordinator event handling."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import importlib.util
import json
from pathlib import Path
import sys
import types

import pytest


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
    sensor_module = types.ModuleType("homeassistant.components.sensor")

    class BinarySensorEntity:
        pass

    class SensorEntity:
        pass

    binary_sensor_module.BinarySensorEntity = BinarySensorEntity
    sensor_module.SensorEntity = SensorEntity

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
    sys.modules.setdefault("homeassistant.components.sensor", sensor_module)
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
sensor_module = _load_module(
    f"{package.__name__}.sensor",
    PACKAGE / "sensor.py",
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
        self.retained_task_guard = None
        self.session = None
        self.next_room_checks = 0
        self._event_lock = asyncio.Lock()
        self._active_run_restored = False
        self._restored_dispatch_intent_deadline = None
        self._retained_task_timer_cancel = None
        self._retained_task_timer_deadline = None
        self._retained_task_reconcile_scheduled = False

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


class _MemoryStore:
    def __init__(self, data=None) -> None:
        self.data = data

    async def async_load(self):
        return self.data

    async def async_save(self, data) -> None:
        self.data = data


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
            const.CONF_AUTO_CLEAN_ITERATIONS: 1,
            const.CONF_MIN_BATTERY: 40,
            const.CONF_NATIVE_RESUME_ENABLED: True,
            const.CONF_NATIVE_RESUME_TIMEOUT: 10800,
            const.CONF_DOCK_SETTLE: 0,
            const.CONF_STALE_RESUME_AUTO_CLEAR: False,
            const.CONF_STALE_RESUME_AGE: 1800,
            const.CONF_STALE_RESUME_SETTLE: 0,
            const.CONF_STALE_RESUME_CLEAR_TIMEOUT: 30,
            const.CONF_RESUME_NUDGE_ENABLED: False,
            const.CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL: True,
            const.CONF_MANUAL_TRACKING: True,
            const.CONF_TRACK_MANUAL_WHEN_PAUSED: True,
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
            requested_iterations=1,
        )
        self.manual_run = None
        self.retained_task_guard = None
        self.settings_snapshot = None
        self.while_away_outcomes = []
        self._while_away_outcome_sequence = 0
        self.last_error = None
        self.started_rooms = []
        self._away_timer_cancel = None
        self._next_day_timer_cancel = None
        self._terminal_cleanup_retry_cancel = None
        self._dock_settle_cancel = None
        self._native_resume_timeout_cancel = None
        self._dispatch_start_timeout_cancel = None
        self._cancel_ack_timeout_cancel = None
        self._blocked_session_watchdog_cancel = None
        self._retained_task_timer_cancel = None
        self._retained_task_timer_deadline = None
        self._retained_task_reconcile_scheduled = False
        self._terminal_cleanup_retry_attempts = 0
        self._terminal_settings_restore_deferred = False
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

    async def _async_start_room(
        self,
        room,
        *,
        vacuum_only: bool,
        fallback_vacuum: bool = False,
    ) -> None:
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
            if fallback_vacuum:
                self.session.mark_fallback_attempted(room.room_id)
            else:
                self.session.mark_attempted(room.room_id)
            self._clear_blocked_session_watchdog()
            self.session.active_room_id = room.room_id
            self.active_run = logic.ActiveRun(
                room_id=room.room_id,
                segment_id=room.segment_id,
                session_id=self.session.session_id,
                started_at=logic.utcnow_iso(),
                vacuum_only=vacuum_only,
                fallback_vacuum=fallback_vacuum,
                requested_iterations=1,
                allowed_error_fingerprint=(
                    logic.allowed_error_fingerprint(self._resource_state())
                    if vacuum_only and self.session.degraded_reason
                    else None
                ),
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


def _set_rooms(
    coordinator: _RecoverableFailureCoordinator,
    rooms: list[logic.RoomConfig],
) -> None:
    coordinator.rooms = rooms
    coordinator.room_by_id = {room.room_id: room for room in rooms}
    coordinator.room_by_segment = {room.segment_id: room for room in rooms}
    coordinator.room_by_name = {room.name.lower(): room for room in rooms}
    coordinator.ledgers = {
        room.room_id: coordinator.ledgers.get(room.room_id, logic.RoomLedger())
        for room in rooms
    }


def _trigger_low_battery(
    coordinator: _RecoverableFailureCoordinator,
    *,
    battery: str = "15",
) -> None:
    if coordinator.active_run is not None:
        coordinator.active_run.phase = logic.RUN_PHASE_CLEANING
        coordinator.active_run.observed_cleaning = True
        coordinator.active_run.observed_segment_cleaning = True
        coordinator.active_run.start_confirmed_at = logic.utcnow_iso()
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_battery", battery)
    _handle_event(coordinator, "sensor.robot_error", "Low battery")


def _observe_iteration_cycles(
    coordinator: _RecoverableFailureCoordinator,
    count: int,
) -> None:
    """Drive distinct live status-flag segment cycles."""
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "none")
    for _ in range(count):
        _handle_event(coordinator, "sensor.robot_status_flag", "segment")
        _handle_event(coordinator, "sensor.robot_status_flag", "none")


def _trigger_final_resumable_return(
    coordinator: _RecoverableFailureCoordinator,
) -> logic.ActiveRun:
    active_run = coordinator.active_run
    assert active_run is not None
    active_run.phase = logic.RUN_PHASE_RESUMED_CLEANING
    active_run.observed_cleaning = True
    active_run.observed_segment_cleaning = True
    active_run.resumed_after_suspend = True
    active_run.resume_source = "native_segment"
    coordinator.set_state(coordinator.vacuum_entity, "returning")
    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")

    assert coordinator.active_run is active_run
    assert active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert active_run.resume_required is True
    return active_run


def _prepare_preflight_coordinator(
    *,
    now: datetime,
    auto_clear: bool,
    stale: bool,
    owner: str = logic.RETAINED_TASK_OWNER_UNKNOWN,
    phase: str = logic.RETAINED_TASK_PHASE_STALE_CANDIDATE,
) -> _RecoverableFailureCoordinator:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.manual_run = None
    coordinator.session = logic.SessionState(
        session_id="preflight-session",
        started_at=now.isoformat(),
    )
    coordinator.config[const.CONF_STALE_RESUME_AUTO_CLEAR] = auto_clear
    coordinator.config[const.CONF_STALE_RESUME_AGE] = 1800
    coordinator.config[const.CONF_STALE_RESUME_SETTLE] = 0
    coordinator.config[const.CONF_STALE_RESUME_CLEAR_TIMEOUT] = 30
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state("sensor.robot_error", "No error")
    observed_at = now - timedelta(seconds=3600 if stale else 60)
    coordinator.retained_task_guard = logic.RetainedTaskGuard(
        owner=owner,
        phase=phase,
        first_observed_at=observed_at.isoformat(),
        last_material_activity_at=observed_at.isoformat(),
        coherent_since=observed_at.isoformat(),
        last_vacuum_state="docked",
        last_status_flag="resumable",
        last_dock_status="pause",
        last_error="No error",
    )
    return coordinator


def test_clean_water_degraded_records_idempotent_typed_room_deferrals() -> None:
    coordinator = _RecoverableFailureCoordinator()
    rooms = [
        logic.RoomConfig(
            room_id="dining",
            name="Dining Room",
            segment_id="1",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="kitchen",
            name="Kitchen",
            segment_id="2",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="office",
            name="Office",
            segment_id="3",
        ),
    ]
    _set_rooms(coordinator, rooms)
    reason = "Mop Dock Clean Water Tank empty"

    coordinator._activate_clean_water_degraded(reason)
    coordinator._activate_clean_water_degraded(reason)

    assert coordinator.session is not None
    assert coordinator.session.deferred_full_clean_room_ids == [
        "dining",
        "kitchen",
    ]
    assert len(coordinator.while_away_outcomes) == 2
    assert [outcome.sequence for outcome in coordinator.while_away_outcomes] == [
        1,
        2,
    ]
    assert len(
        {outcome.outcome_id for outcome in coordinator.while_away_outcomes}
    ) == 2
    contract = coordinator.while_away_outcome_contract
    assert contract["complete"] is True
    assert [room["status"] for room in contract["rooms"]] == [
        "deferred",
        "deferred",
    ]
    assert all(room["latest_attempt"] is None for room in contract["rooms"])
    assert coordinator.while_away_cleaned_messages == []
    assert coordinator.while_away_issue_messages == []


@pytest.mark.parametrize(
    ("fallback_vacuum", "vacuum_only", "expected_mode"),
    [
        (False, False, "vacuum_mop"),
        (True, True, "fallback_vacuum"),
    ],
)
def test_dispatch_service_failure_records_interrupted_typed_attempt(
    fallback_vacuum,
    vacuum_only,
    expected_mode,
) -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="dining",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at="2026-08-19T10:00:00+00:00",
        degraded_reason=(
            "Mop Dock Clean Water Tank empty"
            if fallback_vacuum
            else None
        ),
        degraded_preparation_completed=fallback_vacuum,
        deferred_full_clean_room_ids=(
            [room.room_id] if fallback_vacuum else []
        ),
        deferred_full_clean_reasons=(
            {room.room_id: "Mop Dock Clean Water Tank empty"}
            if fallback_vacuum
            else {}
        ),
    )
    if fallback_vacuum:
        coordinator._record_deferral_outcome(
            room,
            "Mop Dock Clean Water Tank empty",
        )
        coordinator.set_state(coordinator.vacuum_entity, "error")
        coordinator.set_state(
            "sensor.robot_error",
            "Mop Dock Clean Water Tank empty",
        )
        coordinator.set_state("sensor.robot_dock_status", "pause")
    else:
        coordinator.set_state(coordinator.vacuum_entity, "docked")

    original_async_call = coordinator.hass.services.async_call

    async def fail_publish(domain, service, data, blocking=False) -> None:
        if domain == "mqtt" and service == "publish":
            raise RuntimeError("publish failed")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_publish

    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room(
            coordinator,
            room,
            vacuum_only=vacuum_only,
            fallback_vacuum=fallback_vacuum,
        )
    )

    assert coordinator.active_run is None
    contract = coordinator.while_away_outcome_contract
    assert contract["complete"] is True
    projection = contract["rooms"][0]
    attempt_events = [
        event for event in contract["events"] if event["type"] == "attempt"
    ]
    assert len(attempt_events) == 1
    assert attempt_events[0]["attempt_mode"] == expected_mode
    assert attempt_events[0]["attempt_result"] == "interrupted"
    assert attempt_events[0]["reason"]["code"] == "dispatch.failed"
    assert projection["status"] == "interrupted"
    assert projection["latest_attempt"]["mode"] == expected_mode
    assert projection["latest_attempt"]["result"] == "interrupted"
    assert projection["occurrence_count"] == 1
    assert projection["credit"] == {
        "status": "none",
        "operation": None,
    }
    assert projection["outstanding"]["operation"] == "vacuum_mop"
    assert projection["credit"]["status"] != "full"
    assert projection["outstanding"]["reason"]["code"] == (
        "mop.clean_water_empty"
        if fallback_vacuum
        else "dispatch.failed"
    )
    assert projection["reasons_coincide"] is (not fallback_vacuum)


def test_typed_outcome_sequence_survives_restart_and_duplicate_replay() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="dining",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session-one",
        started_at="2026-08-19T10:00:00+00:00",
    )
    coordinator._record_deferral_outcome(
        room,
        "Mop Dock Clean Water Tank empty",
    )
    coordinator._store = _MemoryStore()
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_save_store(
            coordinator
        )
    )

    restored = _RecoverableFailureCoordinator()
    _set_rooms(restored, [room])
    restored._store = _MemoryStore(coordinator._store.data)
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_load_store(restored)
    )

    assert restored._while_away_outcome_sequence == 1
    assert len(restored.while_away_outcomes) == 1
    restored._record_deferral_outcome(
        room,
        "Mop Dock Clean Water Tank empty",
    )
    assert restored._while_away_outcome_sequence == 1
    assert len(restored.while_away_outcomes) == 1

    restored.session = logic.SessionState(
        session_id="session-two",
        started_at="2026-08-19T12:00:00+00:00",
    )
    restored._record_deferral_outcome(
        room,
        "Mop Dock Clean Water Tank empty",
    )
    assert restored._while_away_outcome_sequence == 2
    assert [outcome.sequence for outcome in restored.while_away_outcomes] == [
        1,
        2,
    ]
    restored._prune_while_away_outcomes_for_day("2099-01-01")
    restored.session = logic.SessionState(
        session_id="session-three",
        started_at="2099-01-01T12:00:00+00:00",
    )
    restored._record_deferral_outcome(
        room,
        "Mop Dock Clean Water Tank empty",
    )
    assert restored._while_away_outcome_sequence == 3
    assert [outcome.sequence for outcome in restored.while_away_outcomes] == [
        3
    ]


def test_attempt_event_ids_deduplicate_replay_not_identical_real_attempts() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="office",
        name="Office",
        segment_id="1",
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at="2026-08-19T10:00:00+00:00",
    )
    first_run = logic.ActiveRun(
        room_id=room.room_id,
        segment_id=room.segment_id,
        session_id=coordinator.session.session_id,
        started_at="2026-08-19T10:01:00+00:00",
        vacuum_only=True,
        command_published=True,
    )
    second_run = logic.ActiveRun(
        room_id=room.room_id,
        segment_id=room.segment_id,
        session_id=coordinator.session.session_id,
        started_at="2026-08-19T10:05:00+00:00",
        vacuum_only=True,
        command_published=True,
    )
    reason = "Auto-Empty Dock dust bag full or dust duct clogged"

    coordinator._record_attempt_outcome(
        kind="failed",
        run=first_run,
        result="failed",
        reason=reason,
    )
    coordinator._record_attempt_outcome(
        kind="failed",
        run=first_run,
        result="failed",
        reason=reason,
    )
    coordinator._record_attempt_outcome(
        kind="failed",
        run=second_run,
        result="failed",
        reason=reason,
    )

    assert len(coordinator.while_away_outcomes) == 2
    assert [outcome.sequence for outcome in coordinator.while_away_outcomes] == [
        1,
        2,
    ]
    projection = coordinator.while_away_outcome_contract["rooms"][0]
    assert projection["occurrence_count"] == 2
    assert len(projection["event_ids"]) == 2

    coordinator._store = _MemoryStore()
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_save_store(
            coordinator
        )
    )
    restored = _RecoverableFailureCoordinator()
    _set_rooms(restored, [room])
    restored._store = _MemoryStore(coordinator._store.data)
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_load_store(restored)
    )
    restored._record_attempt_outcome(
        kind="failed",
        run=second_run,
        result="failed",
        reason=reason,
    )
    assert len(restored.while_away_outcomes) == 2
    assert restored._while_away_outcome_sequence == 2


def test_first_interruption_event_wins_over_later_cancel_collision(caplog) -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    run = coordinator.active_run
    run.vacuum_only = True
    run.command_published = True
    run.phase = logic.RUN_PHASE_SUSPENDED
    run.resume_required = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")

    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    contract = coordinator.while_away_outcome_contract
    assert len(contract["events"]) == 1
    original_event = json.loads(json.dumps(contract["events"][0]))
    assert original_event["attempt_result"] == "interrupted"
    assert original_event["reason"]["code"] == "navigation.stuck"
    assert contract["rooms"][0]["status"] == "interrupted"

    coordinator._record_attempt_outcome(
        kind="failed",
        run=run,
        result="interrupted",
        reason="Tracked person arrived home",
    )

    updated_contract = coordinator.while_away_outcome_contract
    assert updated_contract["events"] == [original_event]
    assert updated_contract["rooms"][0]["status"] == "interrupted"
    assert "Ignoring conflicting terminal outcome" in caplog.text


def test_legacy_updates_are_session_scoped_and_typed_events_are_immutable() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="office",
        name="Office",
        segment_id="1",
    )
    _set_rooms(coordinator, [room])
    session_one = logic.SessionState(
        session_id="session-one",
        started_at="2026-08-19T10:00:00+00:00",
    )
    coordinator.session = session_one
    first_run = logic.ActiveRun(
        room_id=room.room_id,
        segment_id=room.segment_id,
        session_id=session_one.session_id,
        started_at="2026-08-19T10:01:00+00:00",
        vacuum_only=True,
        command_published=True,
    )
    coordinator._record_attempt_outcome(
        kind="failed",
        run=first_run,
        result="failed",
        reason="Cannot reach target",
    )

    session_two = logic.SessionState(
        session_id="session-two",
        started_at="2026-08-19T12:00:00+00:00",
    )
    coordinator.session = session_two
    second_run = logic.ActiveRun(
        room_id=room.room_id,
        segment_id=room.segment_id,
        session_id=session_two.session_id,
        started_at="2026-08-19T12:01:00+00:00",
        vacuum_only=True,
        command_published=True,
    )
    coordinator._record_attempt_outcome(
        kind="failed",
        run=second_run,
        result="failed",
        reason="Cannot reach target",
    )
    cached_events = {
        event["id"]: json.loads(json.dumps(event))
        for event in coordinator.while_away_outcome_contract["events"]
    }

    assert coordinator._replace_while_away_failure(
        room.room_id,
        None,
        "Unknown error 95",
    )
    outcomes_by_session = {
        outcome.session_id: outcome
        for outcome in coordinator.while_away_outcomes
    }
    assert outcomes_by_session["session-one"].reason == "Cannot reach target"
    assert outcomes_by_session["session-two"].reason == "Unknown error 95"
    assert outcomes_by_session["session-one"].legacy_visible is True
    assert outcomes_by_session["session-two"].legacy_visible is True
    assert {
        event["id"]: event
        for event in coordinator.while_away_outcome_contract["events"]
    } == cached_events

    coordinator._remove_while_away_failure(room.room_id, None)

    assert outcomes_by_session["session-one"].legacy_visible is True
    assert outcomes_by_session["session-two"].legacy_visible is False
    assert {
        event["id"]: event
        for event in coordinator.while_away_outcome_contract["events"]
    } == cached_events
    assert coordinator.while_away_issue_messages == [
        "Could not clean Office because it could not reach the room"
    ]


def test_session_sensor_exposes_additive_typed_outcome_contract() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="dining",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator._record_deferral_outcome(
        room,
        "Mop Dock Clean Water Tank empty",
    )
    coordinator.session.enter_recovery(
        code="mop.clean_water_empty",
        disposition=logic.BLOCKER_RECOVERABLE,
        reason="Mop Dock Clean Water Tank empty",
        phase="operator_required",
        next_retry_at="2026-09-02T18:00:00+00:00",
        waiting_for_physical_fix=True,
        operator_action="Refill the clean-water tank.",
    )

    sensor = sensor_module.ValetudoSessionStateSensor(coordinator)
    attributes = sensor.extra_state_attributes

    assert attributes[const.ATTR_WHILE_AWAY_OUTCOMES] == (
        coordinator.while_away_outcome_contract
    )
    assert attributes[const.ATTR_WHILE_AWAY_CLEANED] == []
    assert attributes[const.ATTR_WHILE_AWAY_ISSUES] == []
    assert attributes[const.ATTR_BLOCKER_CODE] == "mop.clean_water_empty"
    assert attributes[const.ATTR_BLOCKER_DISPOSITION] == "recoverable"
    assert attributes[const.ATTR_BLOCKER_OPERATOR_ACTION] == (
        "Refill the clean-water tank."
    )
    assert attributes[const.ATTR_RECOVERY_PHASE] == "operator_required"
    assert attributes[const.ATTR_WAITING_FOR_PHYSICAL_FIX] is True
    assert attributes[const.ATTR_PRESERVED_ROOMS] == ["dining"]


def test_returned_home_cancellation_records_typed_interruption_only() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="hallway",
        name="Hallway",
        segment_id="1",
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at="2026-08-19T12:00:00+00:00",
    )
    coordinator.active_run = logic.ActiveRun(
        room_id=room.room_id,
        segment_id=room.segment_id,
        session_id=coordinator.session.session_id,
        started_at="2026-08-19T12:10:00+00:00",
        vacuum_only=True,
        command_published=True,
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")

    asyncio.run(
        coordinator.async_cancel_session("Tracked person arrived home")
    )

    assert coordinator.session.terminal_reason == "returned_home"
    projection = coordinator.while_away_outcome_contract["rooms"][0]
    assert projection["status"] == "interrupted"
    assert projection["latest_attempt"]["mode"] == "vacuum"
    assert projection["latest_attempt"]["result"] == "interrupted"
    assert projection["latest_attempt"]["reason"]["code"] == (
        "occupancy.person_arrived"
    )
    assert projection["outstanding"]["operation"] == "vacuum"
    assert coordinator.while_away_issue_messages == []


def test_two_2026_08_19_sessions_build_authoritative_day_projection() -> None:
    coordinator = _RecoverableFailureCoordinator()
    rooms = [
        logic.RoomConfig(
            room_id="dining_room",
            name="Dining Room",
            segment_id="1",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="kitchen",
            name="Kitchen",
            segment_id="2",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="master_bathroom",
            name="Master Bathroom",
            segment_id="3",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="guest_bathroom",
            name="Guest Bathroom",
            segment_id="4",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="gym",
            name="Gym",
            segment_id="5",
        ),
        logic.RoomConfig(
            room_id="office",
            name="Office",
            segment_id="6",
        ),
        logic.RoomConfig(
            room_id="guest_room",
            name="Guest Room",
            segment_id="7",
        ),
        logic.RoomConfig(
            room_id="master_bedroom_closet",
            name="Master Bedroom Closet",
            segment_id="8",
        ),
        logic.RoomConfig(
            room_id="hallway",
            name="Hallway",
            segment_id="9",
        ),
    ]
    _set_rooms(coordinator, rooms)
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    ordered_native_dates = {
        "gym": "2026-07-01T00:00:00+00:00",
        "office": "2026-07-02T00:00:00+00:00",
        "guest_room": "2026-07-03T00:00:00+00:00",
        "master_bedroom_closet": "2026-07-04T00:00:00+00:00",
        "hallway": "2026-07-05T00:00:00+00:00",
    }
    for room_id, timestamp in ordered_native_dates.items():
        coordinator.ledgers[room_id].last_successful_clean = timestamp

    dt_module = sys.modules["homeassistant.util.dt"]
    original_now = dt_module.now
    dt_module.now = lambda: datetime(2026, 8, 19, 14, 0, tzinfo=UTC)
    try:
        coordinator.session = logic.SessionState(
            session_id="session-one",
            started_at="2026-08-19T16:00:00+00:00",
        )
        coordinator.active_run = logic.ActiveRun(
            room_id="dining_room",
            segment_id="1",
            session_id="session-one",
            started_at="2026-08-19T16:01:00+00:00",
            command_published=True,
            observed_cleaning=True,
            observed_segment_cleaning=True,
            requested_iterations=1,
        )
        coordinator.active_run.observe_segment_iteration(
            source="status_segment_transition",
            count_new_iteration=True,
        )
        coordinator.session.active_room_id = "dining_room"
        coordinator.set_state(coordinator.vacuum_entity, "error")
        coordinator.set_state("sensor.robot_dock_status", "pause")
        _handle_event(
            coordinator,
            "sensor.robot_error",
            "Mop Dock Clean Water Tank empty",
        )

        assert coordinator.active_run is not None
        assert coordinator.active_run.room_id == "gym"
        assert coordinator.active_run.vacuum_only is True
        assert coordinator.active_run.fallback_vacuum is False
        asyncio.run(
            coordinator._async_finish_active_run(success_override=True)
        )

        assert coordinator.active_run is not None
        assert coordinator.active_run.room_id == "office"
        _handle_event(
            coordinator,
            "sensor.robot_error",
            "Auto-Empty Dock dust bag full or dust duct clogged",
        )
        assert coordinator.session.terminal_reason is None
        assert coordinator.session.active is True
        assert coordinator.session.recovery_phase == "suspended"
        assert coordinator.active_run is not None
        assert coordinator.active_run.room_id == "office"
        assert coordinator.active_run.allowed_error_fingerprint == (
            "dock.dustbag_full_or_duct_blocked"
        )
        asyncio.run(
            coordinator._async_finish_active_run(
                success_override=True,
                continue_session=False,
                send_summary=False,
            )
        )
        assert coordinator.active_run is None

        coordinator.set_state("person.owner", "not_home")
        coordinator.set_state(coordinator.vacuum_entity, "docked")
        coordinator.set_state("sensor.robot_error", "No error")
        coordinator.set_state("sensor.robot_status_flag", "none")
        coordinator.set_state("sensor.robot_dock_status", "idle")
        coordinator.session = logic.SessionState(
            session_id="session-two",
            started_at="2026-08-19T19:00:00+00:00",
        )
        coordinator.active_run = logic.ActiveRun(
            room_id="dining_room",
            segment_id="1",
            session_id="session-two",
            started_at="2026-08-19T19:01:00+00:00",
            command_published=True,
            observed_cleaning=True,
            observed_segment_cleaning=True,
            requested_iterations=1,
        )
        coordinator.active_run.observe_segment_iteration(
            source="status_segment_transition",
            count_new_iteration=True,
        )
        coordinator.session.active_room_id = "dining_room"
        coordinator.set_state(coordinator.vacuum_entity, "error")
        coordinator.set_state("sensor.robot_dock_status", "pause")
        _handle_event(
            coordinator,
            "sensor.robot_error",
            "Mop Dock Clean Water Tank empty",
        )

        for expected_room in (
            "guest_room",
            "master_bedroom_closet",
        ):
            assert coordinator.active_run is not None
            assert coordinator.active_run.room_id == expected_room
            assert coordinator.active_run.vacuum_only is True
            assert coordinator.active_run.fallback_vacuum is False
            asyncio.run(
                coordinator._async_finish_active_run(success_override=True)
            )

        assert coordinator.active_run is not None
        assert coordinator.active_run.room_id == "hallway"
        coordinator.set_state(coordinator.vacuum_entity, "docked")
        _handle_event(coordinator, "person.owner", "home")

        assert coordinator.session.terminal_reason == "returned_home"
        assert coordinator.session.fallback_completed_room_ids == []
        assert coordinator.active_run is None
        contract = coordinator.while_away_outcome_contract
        assert contract["version"] == 2
        assert contract["complete"] is True
        assert contract["day"] == "2026-08-19"
        projections = {
            room["room_id"]: room for room in contract["rooms"]
        }

        assert projections["gym"]["status"] == "completed"
        assert projections["office"]["status"] == "completed"
        assert projections["office"]["occurrence_count"] == 1
        assert [
            event["attempt_result"]
            for event in contract["events"]
            if event["room_id"] == "office"
            and event["type"] == "attempt"
        ] == ["completed"]
        assert projections["guest_room"]["status"] == "completed"
        assert projections["master_bedroom_closet"]["status"] == "completed"

        dining = projections["dining_room"]
        assert dining["status"] == "uncertain"
        assert dining["latest_attempt"]["mode"] == "vacuum_mop"
        assert dining["latest_attempt"]["reason"]["code"] == (
            "mop.clean_water_empty"
        )
        assert dining["outstanding"]["operation"] == "vacuum_mop"
        assert dining["outstanding"]["reason"]["code"] == (
            "mop.clean_water_empty"
        )
        assert dining["occurrence_count"] == 2
        assert dining["reasons_coincide"] is False

        hallway = projections["hallway"]
        assert hallway["status"] == "interrupted"
        assert hallway["latest_attempt"]["mode"] == "vacuum"
        assert hallway["latest_attempt"]["reason"]["code"] == (
            "occupancy.person_arrived"
        )
        assert hallway["outstanding"]["operation"] == "vacuum"

        for room_id in (
            "kitchen",
            "master_bathroom",
            "guest_bathroom",
        ):
            projection = projections[room_id]
            assert projection["status"] == "deferred"
            assert projection["latest_attempt"] is None
            assert projection["outstanding"]["operation"] == "vacuum_mop"
            assert projection["outstanding"]["reason"]["code"] == (
                "mop.clean_water_empty"
            )

        assert not any(
            event.get("attempt_mode") == "fallback_vacuum"
            and event.get("attempt_result") == "completed"
            for event in contract["events"]
        )
        assert coordinator.while_away_cleaned_messages == [
            "Cleaned Gym",
            "Cleaned Office",
            "Cleaned Guest Room",
            "Cleaned Master Bedroom Closet",
        ]
        assert coordinator.while_away_issue_messages == [
            "Could not clean Dining Room because the clean water tank is empty"
        ]
    finally:
        dt_module.now = original_now


def test_error_95_recovery_preserves_and_retries_room_after_clear() -> None:
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
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.session.failed_room_ids == []
    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.started_rooms == []

    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    asyncio.run(
        coordinator._async_handle_state_change_event(event_cls(coordinator.vacuum_entity, "docked"))
    )

    assert coordinator.session.failed_room_ids == []
    assert coordinator.session.failed_room_reasons == {}
    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.retry_room_ids == []
    assert coordinator.started_rooms == ["room_one"]


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


def test_dispatch_start_resumable_race_is_cancelled_once() -> None:
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_DISPATCHING
    original_async_call = coordinator.hass.services.async_call

    async def lose_stop_ack(domain, service, data, blocking=False) -> None:
        await original_async_call(domain, service, data, blocking)
        if domain == "vacuum" and service == "stop":
            raise RuntimeError("stop acknowledgement unavailable")

    coordinator.hass.services.async_call = lose_stop_ack

    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.active_run.cancel_stop_attempts == 1
    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.recovery_phase == "operator_required"
    assert "room_one" in coordinator.preserved_room_ids

    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.hass.services.async_call = original_async_call
    assert asyncio.run(coordinator._async_execute_cancel_pending()) is True
    assert _service_names(coordinator).count("stop") == 1
    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_one"


def test_dispatch_start_resumable_race_at_dock_is_cancelled_once() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    assert coordinator.active_run is not None
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    original_async_call = coordinator.hass.services.async_call

    async def lose_stop_ack(domain, service, data, blocking=False) -> None:
        await original_async_call(domain, service, data, blocking)
        if domain == "vacuum" and service == "stop":
            raise RuntimeError("stop acknowledgement unavailable")

    coordinator.hass.services.async_call = lose_stop_ack

    asyncio.run(
        coordinator._async_reconcile_active_run_at_dock(datetime.now(UTC))
    )

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.session is not None
    assert coordinator.session.recovery_phase == "operator_required"

    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.hass.services.async_call = original_async_call
    assert asyncio.run(coordinator._async_execute_cancel_pending()) is True
    asyncio.run(coordinator._async_maybe_start_next_room())

    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.started_rooms == ["room_one"]


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
    assert coordinator.session.active is True
    assert coordinator.native_resume_pending is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "operator_required"


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
    assert coordinator.session.uncertain_room_ids == ["room_one"]
    assert _service_names(coordinator).count("stop") == 1

    assert coordinator.started_rooms == []
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    _handle_event(coordinator, "sensor.robot_dock_status", "idle")

    assert coordinator.started_rooms == ["room_two"]


def test_latched_clean_water_error_dispatches_native_vacuum_room_from_pause() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="room_one",
                name="Dining Room",
                segment_id="1",
                mop_required=True,
            ),
            logic.RoomConfig(
                room_id="room_two",
                name="Office",
                segment_id="2",
            ),
        ],
    )
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )
    assert coordinator.active_run is not None
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "error")

    _handle_event(coordinator, "sensor.robot_dock_status", "pause")
    _handle_event(
        coordinator,
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    assert coordinator.session is not None
    assert coordinator.state == const.STATE_RUNNING
    assert coordinator.session.degraded_reason == "Mop Dock Clean Water Tank empty"
    assert coordinator.session.failed_room_ids == []
    assert coordinator.session.uncertain_room_ids == ["room_one"]
    assert coordinator.session.deferred_full_clean_room_ids == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"
    assert coordinator.active_run.vacuum_only is True
    assert coordinator.active_run.fallback_vacuum is False
    assert coordinator.active_run.allowed_error_fingerprint == (
        "mop.clean_water_empty"
    )
    assert coordinator.error_state == "Mop Dock Clean Water Tank empty"
    assert coordinator._state("sensor.robot_dock_status") == "pause"
    publish_call = next(
        call
        for call in coordinator.hass.services.calls
        if call["domain"] == "mqtt" and call["service"] == "publish"
    )
    assert json.loads(publish_call["data"]["payload"])["segment_ids"] == ["2"]


def test_active_clean_water_session_reports_degraded_instead_of_raw_error() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
    )
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    assert coordinator.state == const.STATE_DEGRADED


def test_degraded_lane_orders_all_native_rooms_before_fallbacks() -> None:
    coordinator = _RecoverableFailureCoordinator()
    rooms = [
        logic.RoomConfig(
            room_id="dual_old",
            name="Dual Old",
            segment_id="1",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="native_old",
            name="Native Old",
            segment_id="2",
        ),
        logic.RoomConfig(
            room_id="dual_new",
            name="Dual New",
            segment_id="3",
            mop_required=True,
        ),
        logic.RoomConfig(
            room_id="native_new",
            name="Native New",
            segment_id="4",
        ),
    ]
    _set_rooms(coordinator, rooms)
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["dual_old"],
        failed_room_ids=["dual_old"],
        failed_room_reasons={
            "dual_old": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = None
    coordinator.ledgers["dual_old"].last_successful_clean = "2026-08-01T00:00:00+00:00"
    coordinator.ledgers["native_old"].last_successful_clean = "2026-08-02T00:00:00+00:00"
    coordinator.ledgers["dual_new"].last_successful_clean = "2026-08-03T00:00:00+00:00"
    coordinator.ledgers["native_new"].last_successful_clean = "2026-08-04T00:00:00+00:00"
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert coordinator.started_rooms == ["native_old"]
    asyncio.run(coordinator._async_finish_active_run(success_override=True))
    assert coordinator.started_rooms == ["native_old", "native_new"]
    asyncio.run(coordinator._async_finish_active_run(success_override=True))
    assert coordinator.started_rooms == ["native_old", "native_new", "dual_old"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.fallback_vacuum is True
    asyncio.run(coordinator._async_finish_active_run(success_override=True))
    assert coordinator.started_rooms == [
        "native_old",
        "native_new",
        "dual_old",
        "dual_new",
    ]
    assert coordinator.active_run is not None
    assert coordinator.active_run.fallback_vacuum is True


def test_fallback_partial_credit_stays_due_for_new_same_day_session() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        fallback_attempted_room_ids=["room_one"],
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        fallback_vacuum=True,
        allowed_error_fingerprint="mop.clean_water_empty",
    )
    ledger = coordinator.ledgers["room_one"]
    ledger.last_successful_clean = "2026-08-01T00:00:00+00:00"
    ledger.last_mopped = "2026-08-01T00:00:00+00:00"
    ledger.last_failed_reason = "Mop Dock Clean Water Tank empty"
    ledger.successful_count = 3
    coordinator._record_deferral_outcome(
        room,
        "Mop Dock Clean Water Tank empty",
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(coordinator._async_finish_active_run(success_override=True))

    assert ledger.last_fallback_vacuumed is not None
    assert ledger.last_vacuumed == ledger.last_fallback_vacuumed
    assert ledger.last_successful_clean == "2026-08-01T00:00:00+00:00"
    assert ledger.last_mopped == "2026-08-01T00:00:00+00:00"
    assert ledger.last_auto_cleaned_day is None
    assert ledger.successful_count == 3
    assert ledger.last_failed_reason == "Mop Dock Clean Water Tank empty"
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.session.fallback_completed_room_ids == ["room_one"]
    projection = coordinator.while_away_outcome_contract["rooms"][0]
    assert projection["status"] == "partial"
    assert projection["latest_attempt"]["mode"] == "fallback_vacuum"
    assert projection["credit"] == {
        "status": "partial",
        "operation": "vacuum",
    }
    assert projection["outstanding"]["operation"] == "mop"
    assert projection["outstanding"]["reason"]["code"] == (
        "mop.clean_water_empty"
    )

    selection, _skipped = logic.select_next_room(
        [room],
        coordinator.ledgers,
        set(),
        logic.ResourceState(),
        False,
        auto_clean_day=coordinator._current_auto_clean_day(),
    )
    assert selection is not None
    assert selection.room.room_id == "room_one"
    assert selection.vacuum_only is False


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


def test_cancel_returns_to_base_only_when_robot_is_moving() -> None:
    for vacuum_state in ("idle", "returning"):
        coordinator = _RecoverableFailureCoordinator()
        _trigger_low_battery(coordinator)
        coordinator.set_state(coordinator.vacuum_entity, vacuum_state)

        asyncio.run(coordinator.async_cancel_session("test cancel"))

        if vacuum_state == "returning":
            assert _service_names(coordinator)[-2:] == [
                "stop",
                "return_to_base",
            ]
        else:
            assert _service_names(coordinator)[-1:] == ["stop"]


def test_cleared_low_battery_task_auto_retries_after_recharge() -> None:
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

    assert coordinator.active_run is not active_run
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_one"
    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert _service_names(coordinator).count("stop") == 1


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
    assert restored.active_run.phase == logic.RUN_PHASE_DISPATCHING
    assert restored.active_run.room_id == "room_one"
    assert restored.started_rooms == ["room_one"]
    assert _service_names(restored).count("stop") == 1
    assert "publish" not in _service_names(restored)


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

    assert coordinator.active_run is not None
    assert coordinator.session is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_RECOVERY_STALLED
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "suspended"
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
    assert coordinator.active_run is not None
    coordinator.active_run.phase = logic.RUN_PHASE_CLEANING
    coordinator.active_run.start_confirmed_at = logic.utcnow_iso()
    coordinator.active_run.observed_cleaning = True
    coordinator.active_run.observed_segment_cleaning = True
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    _handle_event(coordinator, "sensor.robot_dock_status", "cleaning")
    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert coordinator.active_run.resume_required is True

    restored = _RecoverableFailureCoordinator()
    assert coordinator.session is not None
    restored.session = logic.SessionState.from_dict(coordinator.session.to_dict())
    restored.active_run = logic.ActiveRun.from_dict(coordinator.active_run.to_dict())
    restored._active_run_restored = True
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_status_flag", "resumable")
    restored.set_state("sensor.robot_dock_status", "cleaning")
    restored.set_state("sensor.robot_error", "No error")

    asyncio.run(restored._async_reconcile_restored_session())

    assert restored.active_run is not None
    assert restored.active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert restored.active_run.room_id == "room_one"
    assert restored.started_rooms == []
    assert restored.hass.services.calls == []

    _handle_event(restored, "sensor.robot_dock_status", "idle")
    _handle_event(restored, restored.vacuum_entity, "cleaning")
    _handle_event(restored, "sensor.robot_status_flag", "segment")

    assert restored.active_run is not None
    assert restored.active_run.phase == logic.RUN_PHASE_RESUMED_CLEANING
    assert restored.active_run.room_id == "room_one"
    assert restored.started_rooms == []
    assert "publish" not in _service_names(restored)


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
    original_async_call = coordinator.hass.services.async_call

    async def lose_stop_ack(domain, service, data, blocking=False) -> None:
        await original_async_call(domain, service, data, blocking)
        if domain == "vacuum" and service == "stop":
            raise RuntimeError("stop acknowledgement unavailable")

    coordinator.hass.services.async_call = lose_stop_ack

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.active_run.cancel_stop_attempts == 1
    assert coordinator.native_resume_pending is True
    assert _service_names(coordinator).count("stop") == 1

    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.hass.services.async_call = original_async_call
    assert asyncio.run(coordinator._async_execute_cancel_pending()) is True
    asyncio.run(coordinator._async_maybe_start_next_room())

    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.started_rooms == ["room_one"]


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
    assert _service_names(coordinator)[-1:] == ["stop"]


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


def test_final_resumable_task_clears_at_stable_dock_and_advances() -> None:
    coordinator = _RecoverableFailureCoordinator()
    active_run = _trigger_final_resumable_return(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_dock_status", "drying")

    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert active_run.resume_required is False
    assert active_run.resume_source == "native_task_cleared"
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.ledgers["room_one"].successful_count == 1
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"
    assert coordinator.started_rooms == ["room_two"]
    assert coordinator.hass.services.calls == []


def test_final_resumable_task_waits_for_status_and_dock_settle() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    active_run = _trigger_final_resumable_return(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_dock_status", "drying")

    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert coordinator.active_run is active_run
    assert active_run.resume_required is True
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []

    now = datetime.now(UTC)
    active_run.docked_at = (now - timedelta(seconds=61)).isoformat()
    active_run.suspended_at = (now - timedelta(seconds=120)).isoformat()
    coordinator.hass.states.get(
        "sensor.robot_status_flag"
    ).last_changed = now - timedelta(seconds=61)

    asyncio.run(coordinator._async_reconcile_active_run(now))

    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"
    assert coordinator.started_rooms == ["room_two"]


def test_final_resumable_none_while_returning_does_not_release() -> None:
    coordinator = _RecoverableFailureCoordinator()
    active_run = _trigger_final_resumable_return(coordinator)

    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    assert coordinator.active_run is active_run
    assert active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert active_run.resume_required is True
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.started_rooms == []


def test_final_resumable_reasserted_before_settle_remains_suspended() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    active_run = _trigger_final_resumable_return(coordinator)
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_dock_status", "drying")
    _handle_event(coordinator, "sensor.robot_status_flag", "none")

    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")

    now = datetime.now(UTC)
    active_run.docked_at = (now - timedelta(seconds=61)).isoformat()
    asyncio.run(coordinator._async_reconcile_active_run(now))

    assert coordinator.active_run is active_run
    assert active_run.phase == logic.RUN_PHASE_SUSPENDED
    assert active_run.resume_required is True
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.started_rooms == []


def test_final_resumable_ignores_none_that_predates_suspension() -> None:
    coordinator = _RecoverableFailureCoordinator()
    active_run = coordinator.active_run
    assert active_run is not None
    now = datetime.now(UTC)
    active_run.phase = logic.RUN_PHASE_SUSPENDED
    active_run.observed_cleaning = True
    active_run.observed_segment_cleaning = True
    active_run.suspended_at = (now - timedelta(seconds=60)).isoformat()
    active_run.suspend_reason = "Valetudo reported a resumable native task"
    active_run.resumable_latched = True
    active_run.resume_required = True
    active_run.docked_at = (now - timedelta(seconds=30)).isoformat()
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "drying")
    coordinator.hass.states.get(
        "sensor.robot_status_flag"
    ).last_changed = now - timedelta(seconds=120)

    asyncio.run(coordinator._async_reconcile_active_run(now))

    assert coordinator.active_run is active_run
    assert active_run.resume_required is True
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.started_rooms == []


def test_restored_final_resumable_task_clears_after_stable_dock() -> None:
    coordinator = _RecoverableFailureCoordinator()
    active_run = coordinator.active_run
    assert active_run is not None
    now = datetime.now(UTC)
    active_run.phase = logic.RUN_PHASE_SUSPENDED
    active_run.observed_cleaning = True
    active_run.observed_segment_cleaning = True
    active_run.suspended_at = (now - timedelta(seconds=120)).isoformat()
    active_run.suspend_reason = "Valetudo reported a resumable native task"
    active_run.resumable_latched = True
    active_run.resume_required = True
    active_run.docked_at = (now - timedelta(seconds=61)).isoformat()
    coordinator.config[const.CONF_DOCK_SETTLE] = 60
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "drying")
    coordinator.hass.states.get(
        "sensor.robot_status_flag"
    ).last_changed = now - timedelta(seconds=61)
    coordinator._active_run_restored = True

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_two"
    assert coordinator.started_rooms == ["room_two"]


def test_legacy_low_battery_retry_auto_resumes_after_charge() -> None:
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

    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.hass.services.calls == []


def test_native_resume_timeout_stays_suspended_without_restart_command() -> None:
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

    assert coordinator.active_run is not None
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.session.failed_room_ids == []
    assert "Native resume timed out" in (
        coordinator.active_run.suspend_reason or ""
    )
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

    assert _service_names(coordinator)[-1:] == ["stop"]
    assert coordinator.session is not None
    assert coordinator.session.native_resume_guard_latched is False
    assert coordinator.native_resume_pending is False


def test_stalled_run_cancellation_does_not_duplicate_uncertain_stop() -> None:
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
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.active_run.cancel_stop_attempts == 1

    _handle_event(coordinator, "sensor.robot_battery", "50")

    assert stop_attempts == 1
    assert coordinator.active_run is None
    assert coordinator.session.native_resume_guard_latched is False


def test_stalled_run_cancel_intent_is_saved_before_stop() -> None:
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
                coordinator.active_run
                and coordinator.active_run.phase
                == logic.RUN_PHASE_CANCEL_PENDING
                and coordinator.active_run.cancel_stop_attempts >= 1
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
    assert coordinator.active_run is None


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

    assert _service_names(coordinator)[-1:] == ["stop"]
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
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_RECOVERY_STALLED


def test_cancel_failure_keeps_guard_without_duplicate_stop() -> None:
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
    assert coordinator.active_run.cancel_stop_attempted is True
    assert coordinator.active_run.cancel_stop_attempts == 1
    assert coordinator.native_resume_pending is True

    original_session = coordinator.session
    asyncio.run(coordinator.async_start_session("away timer"))

    assert coordinator.session is original_session
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING

    _handle_event(coordinator, "sensor.robot_battery", "50")

    assert stop_attempts == 1
    assert coordinator.active_run is None
    assert coordinator.native_resume_pending is False


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


def test_disabled_native_resume_still_preserves_recoverable_work() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_NATIVE_RESUME_ENABLED] = False

    _trigger_low_battery(coordinator)

    assert coordinator.active_run is not None
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.active_run.phase == logic.RUN_PHASE_SUSPENDED
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

    assert coordinator.active_run is not None
    assert coordinator.session is not None
    assert coordinator.session.completed_room_ids == []
    assert coordinator.session.terminal_reason is None
    assert coordinator.ledgers["room_one"].successful_count == 0
    assert coordinator.started_rooms == ["room_one"]
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


def test_publish_failure_is_cancelled_and_retried_without_terminalizing() -> None:
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
        if domain == "vacuum" and service == "stop":
            await _FakeServices.async_call(
                coordinator.hass.services,
                domain,
                service,
                data,
                blocking,
            )
            raise RuntimeError("stop acknowledgement unavailable")
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

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.active_run.cancel_stop_attempts == 1
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert _service_names(coordinator).count("stop") == 1
    assert saved[-1]["active_run"]["phase"] == (
        logic.RUN_PHASE_CANCEL_PENDING
    )

    coordinator.hass.services.async_call = types.MethodType(
        _FakeServices.async_call,
        coordinator.hass.services,
    )
    assert asyncio.run(coordinator._async_execute_cancel_pending()) is True
    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.active_run is None
    assert coordinator.session.dispatch_failure_counts == {"room_one": 1}
    projection = coordinator.while_away_outcome_contract["rooms"][0]
    assert projection["status"] == "interrupted"

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert coordinator.started_rooms == []

    _handle_event(coordinator, "sensor.robot_dock_status", "drying")

    assert coordinator.started_rooms == ["room_one"]


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
    assert restored.session.failed_room_reasons == {}
    assert restored.session.terminal_reason is None
    assert restored.session.active is True
    assert restored.session.recovery_phase == "operator_required"
    assert _service_names(restored) == []


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

    async def block_with_low_battery(*, vacuum_only: bool) -> None:
        coordinator.set_state("sensor.robot_battery", "20")

    coordinator._async_apply_mode = block_with_low_battery

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
    _handle_event(coordinator, "sensor.robot_battery", "100")

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


def test_recoverable_mop_hardware_error_restarts_native_room_vacuum_only() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.set_state("sensor.robot_error", "Unknown error 120")

    _handle_event(coordinator, "sensor.robot_error", "Unknown error 120")

    assert coordinator.session is not None
    assert coordinator.session.failed_room_ids == []
    assert _service_names(coordinator)[-1] == "stop"
    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.vacuum_only is True


def test_degraded_queue_exhaustion_waits_for_refill_same_session() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        fallback_attempted_room_ids=["room_one"],
        fallback_completed_room_ids=["room_one"],
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = None
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="vacuum_and_mop"
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )
    coordinator.set_state("select.robot_mode", "vacuum")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.needs_help is False
    assert coordinator.state == const.STATE_SUSPENDED
    assert coordinator.pending_rooms == []
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.settings_snapshot is not None
    calls_after_wait = len(coordinator.hass.services.calls)

    _handle_event(
        coordinator,
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    assert len(coordinator.hass.services.calls) == calls_after_wait


def test_refill_keeps_native_lane_then_runs_untouched_dual_normally() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="native",
                name="Office",
                segment_id="1",
            ),
            logic.RoomConfig(
                room_id="dual",
                name="Bathroom",
                segment_id="2",
                mop_required=True,
            ),
        ],
    )
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        deferred_full_clean_room_ids=["dual"],
        deferred_full_clean_reasons={
            "dual": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = None
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert coordinator.started_rooms == ["native"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.fallback_vacuum is False

    asyncio.run(coordinator._async_finish_active_run(success_override=True))
    assert coordinator.started_rooms == ["native", "dual"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.vacuum_only is False
    assert coordinator.active_run.fallback_vacuum is False


def test_refill_queues_one_normal_retry_for_original_water_failed_room() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        failed_room_ids=["room_one"],
        failed_room_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = None
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.fallback_vacuum is False
    assert coordinator.active_run.vacuum_only is False
    assert coordinator.session.retried_room_ids == ["room_one"]


def test_refill_during_fallback_finishes_it_then_runs_other_dual_normally() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="fallback_room",
                name="Dining Room",
                segment_id="1",
                mop_required=True,
            ),
            logic.RoomConfig(
                room_id="untouched_room",
                name="Bathroom",
                segment_id="2",
                mop_required=True,
            ),
        ],
    )
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        fallback_attempted_room_ids=["fallback_room"],
        deferred_full_clean_room_ids=["fallback_room", "untouched_room"],
        deferred_full_clean_reasons={
            "fallback_room": "Mop Dock Clean Water Tank empty",
            "untouched_room": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="fallback_room",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        fallback_vacuum=True,
        allowed_error_fingerprint="mop.clean_water_empty",
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")

    asyncio.run(coordinator._async_finish_active_run(success_override=True))

    assert coordinator.session is not None
    assert coordinator.session.fallback_completed_room_ids == ["fallback_room"]
    assert coordinator.started_rooms == ["untouched_room"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.fallback_vacuum is False
    assert coordinator.active_run.vacuum_only is False
    assert "fallback_room" in coordinator.session.deferred_full_clean_room_ids


def test_v020_terminal_degraded_session_migrates_and_revives_after_refill() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    stored_session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="mop_resource_deferred",
        degraded_reason="Mop Dock Clean Water Tank empty",
        notification_sent=True,
    )
    coordinator.session = None
    coordinator._store = _MemoryStore(
        {"session": stored_session.to_dict(), "rooms": {}}
    )
    coordinator.settings_snapshot = None
    asyncio.run(coordinator._async_load_store())

    _handle_event(coordinator, "sensor.robot_error", "No error")

    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None


def test_same_away_period_does_not_schedule_terminal_session_restart() -> None:
    for terminal_reason in ("mop_resource_deferred", "blocked"):
        coordinator = _RecoverableFailureCoordinator()
        coordinator.active_run = None
        away_since = datetime.now(UTC) - timedelta(minutes=30)
        coordinator.away_since = away_since.isoformat()
        coordinator.session = logic.SessionState(
            session_id="session",
            started_at=(away_since + timedelta(minutes=10)).isoformat(),
            active=False,
            terminal_reason=terminal_reason,
            notification_sent=True,
        )

        async def keep_loaded_state() -> None:
            return None

        coordinator._async_load_store = keep_loaded_state
        coordinator._unsubscribers = []
        coordinator._listeners = []

        asyncio.run(coordinator.async_setup())

        assert coordinator._away_timer_cancel is None
        assert coordinator.session.session_id == "session"


def test_newer_away_period_allows_terminal_session_restart_timer() -> None:
    for terminal_reason in ("mop_resource_deferred", "blocked"):
        coordinator = _RecoverableFailureCoordinator()
        coordinator.active_run = None
        now = datetime.now(UTC)
        coordinator.away_since = (now - timedelta(seconds=1)).isoformat()
        coordinator.session = logic.SessionState(
            session_id="session",
            started_at=(now - timedelta(hours=1)).isoformat(),
            active=False,
            terminal_reason=terminal_reason,
            notification_sent=True,
        )

        coordinator._schedule_away_timer_if_needed()

        assert coordinator._away_timer_cancel is not None


def test_explicit_start_is_allowed_during_same_terminal_away_period() -> None:
    for terminal_reason in ("mop_resource_deferred", "blocked"):
        coordinator = _RecoverableFailureCoordinator()
        coordinator.active_run = None
        away_since = datetime.now(UTC) - timedelta(minutes=30)
        coordinator.away_since = away_since.isoformat()
        old_session = logic.SessionState(
            session_id="old-session",
            started_at=(away_since + timedelta(minutes=10)).isoformat(),
            active=False,
            terminal_reason=terminal_reason,
            notification_sent=True,
        )
        coordinator.session = old_session
        coordinator.set_state(coordinator.vacuum_entity, "docked")
        coordinator.set_state("sensor.robot_status_flag", "none")
        coordinator.set_state("sensor.robot_dock_status", "idle")
        coordinator.set_state("sensor.robot_error", "No error")

        asyncio.run(coordinator.async_start_session("service"))

        assert coordinator.session is not old_session
        assert coordinator.session.session_id != "old-session"
        assert coordinator.session.active is True
        assert coordinator.started_rooms == ["room_one"]


def test_restored_consumed_fallback_token_is_not_redispatched() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    stored = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        fallback_attempted_room_ids=["room_one"],
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.session = logic.SessionState.from_dict(stored.to_dict())
    coordinator.active_run = None
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(coordinator._async_reconcile_restored_session())

    assert coordinator.started_rooms == []
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.session.fallback_attempted_room_ids == ["room_one"]


def test_restored_allowed_clean_water_run_finishes_without_error_reclassification() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        fallback_attempted_room_ids=["room_one"],
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        fallback_vacuum=True,
        allowed_error_fingerprint="mop.clean_water_empty",
        command_published=True,
        observed_cleaning=True,
        observed_segment_cleaning=True,
        requested_iterations=1,
    )
    coordinator._active_run_restored = True
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(coordinator._async_reconcile_restored_active_run())

    assert coordinator.active_run is None
    assert coordinator.session is not None
    assert coordinator.session.fallback_completed_room_ids == ["room_one"]
    assert coordinator.session.needs_help is False


def test_allowed_clean_water_error_is_tolerated_at_dock_completion_gate() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        fallback_attempted_room_ids=["room_one"],
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        fallback_vacuum=True,
        allowed_error_fingerprint="mop.clean_water_empty",
        command_published=True,
        observed_cleaning=True,
        observed_segment_cleaning=True,
        requested_iterations=1,
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(
        coordinator._async_reconcile_active_run_at_dock(datetime.now(UTC))
    )

    assert coordinator.active_run is None
    assert coordinator.session.fallback_completed_room_ids == ["room_one"]
    assert coordinator.session.needs_help is False


def test_changed_error_during_allowed_fallback_waits_for_recovery() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        fallback_attempted_room_ids=["room_one"],
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        fallback_vacuum=True,
        allowed_error_fingerprint="mop.clean_water_empty",
        command_published=True,
        observed_cleaning=True,
        observed_segment_cleaning=True,
    )

    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.needs_help is False
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "operator_required"


def test_navigation_error_between_degraded_rooms_waits_for_recovery() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
    )

    _handle_event(coordinator, "sensor.robot_error", "Robot is stuck")

    assert coordinator.session.active is True
    assert coordinator.session.needs_help is False
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "operator_required"


def test_changed_safe_resource_errors_keep_vacuum_only_run_active() -> None:
    for changed_error in (
        "Unknown error 120",
        "Mop Dock Wastewater Tank not installed or full",
    ):
        coordinator = _RecoverableFailureCoordinator()
        room = logic.RoomConfig(
            room_id="room_one",
            name="Dining Room",
            segment_id="1",
            mop_required=True,
        )
        _set_rooms(coordinator, [room])
        coordinator.session = logic.SessionState(
            session_id="session",
            started_at=logic.utcnow_iso(),
            degraded_reason="Mop Dock Clean Water Tank empty",
            fallback_attempted_room_ids=["room_one"],
            deferred_full_clean_room_ids=["room_one"],
            deferred_full_clean_reasons={
                "room_one": "Mop Dock Clean Water Tank empty",
            },
        )
        coordinator.active_run = logic.ActiveRun(
            room_id="room_one",
            segment_id="1",
            session_id="session",
            started_at=logic.utcnow_iso(),
            vacuum_only=True,
            fallback_vacuum=True,
            allowed_error_fingerprint="mop.clean_water_empty",
            command_published=True,
        )
        coordinator.set_state(coordinator.vacuum_entity, "error")

        _handle_event(coordinator, "sensor.robot_error", changed_error)

        assert coordinator.session.active is True
        assert coordinator.session.needs_help is False
        assert coordinator.session.terminal_reason is None
        assert coordinator.active_run is not None
        assert coordinator.active_run.vacuum_only is True
        assert coordinator.active_run.allowed_error_fingerprint in {
            "mop.hardware_unavailable",
            "mop.dirty_water_unavailable",
        }


def test_changed_safe_resource_errors_continue_native_rooms() -> None:
    for changed_error in (
        "Unknown error 120",
        "Mop Dock Wastewater Tank not installed or full",
    ):
        coordinator = _RecoverableFailureCoordinator()
        coordinator.active_run = None
        coordinator.session = logic.SessionState(
            session_id="session",
            started_at=logic.utcnow_iso(),
            degraded_reason="Mop Dock Clean Water Tank empty",
            degraded_at=logic.utcnow_iso(),
        )

        _handle_event(coordinator, "sensor.robot_error", changed_error)

        assert coordinator.session.active is True
        assert coordinator.session.needs_help is False
        assert coordinator.session.terminal_reason is None
        assert coordinator.active_run is not None
        assert coordinator.active_run.room_id == "room_one"
        assert coordinator.active_run.vacuum_only is True


def test_fallback_navigation_failure_is_not_republished() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        fallback_attempted_room_ids=["room_one"],
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        vacuum_only=True,
        fallback_vacuum=True,
        command_published=True,
    )
    coordinator._record_deferral_outcome(
        room,
        "Mop Dock Clean Water Tank empty",
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_error", "Unknown error 95")

    _handle_event(coordinator, "sensor.robot_error", "Unknown error 95")

    assert coordinator.session.pending_recovery_room_id is None
    assert coordinator.session.retry_room_ids == []
    assert coordinator.session.fallback_failed_room_ids == []
    assert coordinator.session.fallback_attempted_room_ids == []
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.started_rooms == []
    projection = coordinator.while_away_outcome_contract["rooms"][0]
    assert projection["latest_attempt"]["mode"] == "fallback_vacuum"
    assert projection["latest_attempt"]["reason"]["code"] == (
        "navigation.stuck"
    )
    assert projection["outstanding"]["reason"]["code"] == (
        "mop.clean_water_empty"
    )
    assert projection["reasons_coincide"] is False


def test_unknown_error_120_defers_mop_without_fallback() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "Unknown error 120")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.started_rooms == []
    assert coordinator.session.degraded_reason == "Unknown error 120"
    assert coordinator.session.fallback_attempted_room_ids == []
    assert coordinator.session.deferred_full_clean_room_ids == ["room_one"]


def test_generic_sticky_mop_error_watchdog_rearms_without_terminalizing() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="vacuum_and_mop"
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state("select.robot_mode", "vacuum")

    _handle_event(coordinator, "sensor.robot_error", "Unknown error 120")

    assert coordinator.session.blocked_deadline is not None
    assert coordinator.session.blocked_reason == "Unknown error 120"
    assert coordinator.session.active is True

    asyncio.run(coordinator._async_expire_blocked_session_serialized())

    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.needs_help is False
    assert coordinator.pending_rooms == []
    assert coordinator.preserved_room_ids == ["room_one"]
    assert coordinator.session.deferred_full_clean_room_ids == ["room_one"]
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.settings_snapshot is not None
    calls_after_wait = len(coordinator.hass.services.calls)

    _handle_event(coordinator, "sensor.robot_error", "Unknown error 120")

    assert len(coordinator.hass.services.calls) == calls_after_wait

    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    _handle_event(coordinator, "sensor.robot_error", "No error")

    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.vacuum_only is False


def test_blocked_watchdog_uses_long_battery_bound_without_extending_deadline() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.config[const.CONF_BLOCKED_SESSION_TIMEOUT] = 300
    coordinator.config[const.CONF_NATIVE_RESUME_TIMEOUT] = 10800
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "charging")
    coordinator.set_state("sensor.robot_battery", "20")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "Low battery")

    before_long = datetime.now(UTC)
    asyncio.run(
        coordinator._async_arm_blocked_session_watchdog(
            "battery is 20%, below 40%"
        )
    )
    long_deadline = logic.parse_datetime(coordinator.session.blocked_deadline)
    assert long_deadline is not None
    assert (long_deadline - before_long).total_seconds() > 10000

    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_battery", "100")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_error", "Unknown error 120")

    before_short = datetime.now(UTC)
    asyncio.run(
        coordinator._async_arm_blocked_session_watchdog("Unknown error 120")
    )
    short_deadline = logic.parse_datetime(coordinator.session.blocked_deadline)
    assert short_deadline is not None
    assert 250 < (short_deadline - before_short).total_seconds() < 301
    assert short_deadline < long_deadline

    coordinator.set_state(coordinator.vacuum_entity, "charging")
    coordinator.set_state("sensor.robot_battery", "20")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_error", "Low battery")
    asyncio.run(
        coordinator._async_arm_blocked_session_watchdog("Low battery")
    )

    assert logic.parse_datetime(coordinator.session.blocked_deadline) == short_deadline


def test_blocked_watchdog_runtime_clamps_zero_to_one_second() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_BLOCKED_SESSION_TIMEOUT] = 0

    assert coordinator._blocked_session_timeout_seconds("dock busy") == 1


def test_resource_terminal_cleanup_restores_despite_resumable() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="blocked",
        terminal_message="Unknown error 120",
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="vacuum_and_mop"
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state("sensor.robot_error", "Unknown error 120")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("select.robot_mode", "vacuum")

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.session.notification_sent is True
    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("select_option") == 1

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())
    assert _service_names(coordinator).count("select_option") == 1

    deferred = _RecoverableFailureCoordinator()
    _set_rooms(deferred, [room])
    deferred.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    deferred.active_run = None
    deferred.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="mop_resource_deferred",
        terminal_message="Mop Dock Clean Water Tank empty",
        degraded_reason="Mop Dock Clean Water Tank empty",
    )
    deferred.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="vacuum_and_mop"
    )
    deferred.set_state(deferred.vacuum_entity, "error")
    deferred.set_state("sensor.robot_dock_status", "pause")
    deferred.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )
    deferred.set_state("sensor.robot_status_flag", "segment")
    deferred.set_state("select.robot_mode", "vacuum")

    asyncio.run(deferred._async_maybe_send_auto_clean_summary())

    assert deferred.session.notification_sent is True
    assert deferred.settings_snapshot is None
    assert _service_names(deferred).count("select_option") == 1


def test_mid_session_terminal_resumable_notifies_and_restores_immediately() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.manual_run = None
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.config[const.CONF_PASSES_ENTITY] = "input_select.robot_passes"
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="blocked",
        terminal_message="status flag is resumable",
        terminal_cause="blocked_timeout",
        completed_room_ids=["room_one"],
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        passes="3",
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("input_select.robot_passes", "2")

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.session.notification_sent is True
    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("household") == 1
    assert _service_names(coordinator).count("select_option") == 1
    assert "stop" not in _service_names(coordinator)


def test_terminal_notification_does_not_restore_during_external_activity() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.manual_run = None
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason="blocked",
        terminal_message="external task is active",
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="vacuum_and_mop",
    )
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "segment")
    coordinator.set_state("select.robot_mode", "vacuum")

    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.session.notification_sent is True
    assert coordinator.settings_snapshot is not None
    assert _service_names(coordinator).count("household") == 1
    assert _service_names(coordinator).count("select_option") == 0
    assert coordinator._terminal_cleanup_retry_attempts == 0

    coordinator.set_state(coordinator.vacuum_entity, "idle")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    asyncio.run(coordinator._async_maybe_send_auto_clean_summary())

    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("household") == 1
    assert _service_names(coordinator).count("select_option") == 1


def test_blocked_deadline_survives_preparation_and_repeated_tentative_aborts() -> None:
    deadline = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()

    degraded = _RecoverableFailureCoordinator()
    degraded.active_run = None
    degraded.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        blocked_deadline=deadline,
        blocked_reason="original block",
    )
    degraded.set_state(degraded.vacuum_entity, "error")
    degraded.set_state("sensor.robot_status_flag", "none")
    degraded.set_state("sensor.robot_dock_status", "pause")
    degraded.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    assert asyncio.run(degraded._async_prepare_degraded_vacuuming()) is True
    assert degraded.session.blocked_deadline == deadline

    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        blocked_deadline=deadline,
        blocked_reason="original block",
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    async def block_before_publish(*, vacuum_only: bool) -> None:
        coordinator.set_state("sensor.robot_error", "Robot is stuck")

    coordinator._async_apply_mode = block_before_publish

    for _attempt in range(2):
        coordinator.set_state("sensor.robot_error", "No error")
        asyncio.run(
            coordinator._async_start_room(
                coordinator.room_by_id["room_one"],
                vacuum_only=True,
            )
        )
        assert coordinator.active_run is None
        assert coordinator.session.blocked_deadline == deadline
        assert "publish" not in _service_names(coordinator)

    coordinator.set_state("sensor.robot_error", "Robot is stuck")
    asyncio.run(coordinator._async_expire_blocked_session_serialized())

    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "operator_required"


def test_degraded_preparation_failure_waits_without_dispatch() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Office",
        segment_id="1",
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state("select.robot_mode", "vacuum_and_mop")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    async def fail_mode(*, vacuum_only: bool) -> None:
        raise RuntimeError("mode failed")

    coordinator._async_apply_mode = fail_mode

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.started_rooms == []
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase in {
        "operator_required",
        "waiting_for_retry",
    }


def test_dispatch_and_blocked_watchdogs_suspend_degraded_work_finitely() -> None:
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining Room",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert coordinator.active_run is not None
    assert coordinator.session.fallback_attempted_room_ids == ["room_one"]
    assert _service_names(coordinator).count("publish") == 1

    asyncio.run(coordinator._async_expire_dispatch_start())

    assert coordinator.active_run is None
    assert coordinator.session.fallback_failed_room_ids == []
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase in {
        "operator_required",
        "waiting_for_retry",
    }
    assert _service_names(coordinator).count("publish") == 1

    blocked = _RecoverableFailureCoordinator()
    _set_rooms(blocked, [room])
    blocked.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    blocked.active_run = None
    blocked.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        deferred_full_clean_room_ids=["room_one"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
        },
    )
    blocked.set_state(blocked.vacuum_entity, "returning")
    blocked.set_state("sensor.robot_status_flag", "none")
    blocked.set_state("sensor.robot_dock_status", "pause")
    blocked.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(blocked._async_maybe_start_next_room())
    assert blocked.retained_task_guard is not None
    assert blocked.retained_task_guard.observation_deadline is not None
    blocked.retained_task_guard.observation_deadline = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    asyncio.run(
        blocked._async_reconcile_retained_task(
            datetime.now(UTC),
            allow_clear=True,
        )
    )
    assert blocked.session.active is True
    assert (
        blocked.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED
    )


def test_dispatch_timeout_stops_late_command_before_next_publish() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="room_one",
                name="Dining Room",
                segment_id="1",
                mop_required=True,
            ),
            logic.RoomConfig(
                room_id="room_two",
                name="Guest Bathroom",
                segment_id="2",
                mop_required=True,
            ),
        ],
    )
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        deferred_full_clean_room_ids=["room_one", "room_two"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
            "room_two": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    asyncio.run(coordinator._async_expire_dispatch_start())

    service_names = _service_names(coordinator)
    first_publish = service_names.index("publish")
    stop = service_names.index("stop", first_publish + 1)
    assert service_names.count("publish") == 1
    assert first_publish < stop
    assert coordinator.active_run is None

    coordinator.session.dispatch_retry_not_before["room_one"] = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.session.next_retry_at = (
        coordinator.session.dispatch_retry_not_before["room_one"]
    )
    coordinator.session.blocked_deadline = coordinator.session.next_retry_at
    asyncio.run(coordinator._async_expire_blocked_session_serialized())

    service_names = _service_names(coordinator)
    second_publish = service_names.index("publish", stop + 1)
    assert first_publish < stop < second_publish
    assert service_names.count("publish") == 2
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "room_one"


def test_dispatch_timeout_cancel_failure_prevents_next_publish() -> None:
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="room_one",
                name="Dining Room",
                segment_id="1",
                mop_required=True,
            ),
            logic.RoomConfig(
                room_id="room_two",
                name="Guest Bathroom",
                segment_id="2",
                mop_required=True,
            ),
        ],
    )
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = True
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        deferred_full_clean_room_ids=["room_one", "room_two"],
        deferred_full_clean_reasons={
            "room_one": "Mop Dock Clean Water Tank empty",
            "room_two": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    original_async_call = coordinator.hass.services.async_call

    async def fail_stop(domain, service, data, blocking=False) -> None:
        if domain == "vacuum" and service == "stop":
            raise RuntimeError("stop failed")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_stop
    asyncio.run(coordinator._async_expire_dispatch_start())

    assert _service_names(coordinator).count("publish") == 1
    assert coordinator.session.active is True
    assert coordinator.session.needs_help is False
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING


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

    assert coordinator.session.pending_recovery_room_id == "room_one"
    assert coordinator.session.failed_room_reasons == {"room_one": "Robot is stuck"}
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.active is True


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
    _handle_event(
        coordinator,
        "sensor.robot_error",
        "Unrecoverable wheel motor failure",
    )

    assert saved_sessions
    assert saved_sessions[0]["active"] is False
    assert saved_sessions[0]["terminal_reason"] == "unrecoverable_error"


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
    assert coordinator.while_away_outcome_contract["events"] == []


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


def test_unowned_resumable_waits_until_stale_before_one_stop(monkeypatch) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    fresh = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=False,
    )

    assert (
        asyncio.run(
            fresh._async_reconcile_retained_task(now, allow_clear=True)
        )
        is False
    )
    assert "stop" not in _service_names(fresh)
    assert fresh.retained_task_guard is not None
    assert (
        fresh.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_STALE_CANDIDATE
    )

    stale = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
    )

    assert (
        asyncio.run(
            stale._async_reconcile_retained_task(now, allow_clear=True)
        )
        is False
    )
    assert _service_names(stale).count("stop") == 1
    assert stale.retained_task_guard is not None
    assert stale.retained_task_guard.clear_attempts == 1
    assert (
        stale.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_VERIFYING
    )


def test_stale_shadow_preflight_notifies_without_settings_or_stop(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=False,
        stale=True,
    )
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.config[const.CONF_PASSES_ENTITY] = "input_select.robot_passes"
    coordinator.set_state("input_select.robot_passes", "3")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED
    )
    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.settings_prepared is False
    assert coordinator.started_rooms == []
    assert "stop" not in _service_names(coordinator)
    assert "select_option" not in _service_names(coordinator)
    assert _service_names(coordinator).count("household") == 1

    sensor = sensor_module.ValetudoSessionStateSensor(coordinator)
    attributes = sensor.extra_state_attributes
    assert attributes["retained_task_owner"] == "unknown"
    assert attributes["retained_task_phase"] == "operator_required"
    assert attributes["retained_task_stale_age_seconds"] == 3600
    assert attributes["retained_task_clear_attempts"] == 0
    assert attributes["retained_task_operator_required_reason"]


def test_no_ack_becomes_operator_required_without_second_stop(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
    )
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert _service_names(coordinator).count("stop") == 1

    asyncio.run(
        coordinator._async_reconcile_retained_task(
            now + timedelta(seconds=31),
            allow_clear=True,
        )
    )

    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED
    )
    assert _service_names(coordinator).count("stop") == 1
    assert _service_names(coordinator).count("household") == 1
    assert coordinator.session is not None
    assert coordinator.session.settings_prepared is False


def test_flag_clear_with_latched_dock_pause_requires_operator(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    coordinator.set_state("sensor.robot_status_flag", "none")

    asyncio.run(
        coordinator._async_reconcile_retained_task(
            now + timedelta(seconds=31),
            allow_clear=True,
        )
    )

    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED
    )
    assert "requires a configured Valetudo identifier" in (
        coordinator.retained_task_guard.operator_required_reason or ""
    )
    assert _service_names(coordinator).count("stop") == 1
    assert "dock_action" not in _service_names(coordinator)


def test_task_ack_then_dock_pause_uses_one_persisted_dock_stop(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(
        coordinator_module.dt_util,
        "utcnow",
        lambda: current[0],
    )
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
    )
    coordinator.config[const.CONF_IDENTIFIER] = "robot"

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert _service_names(coordinator).count("stop") == 1

    current[0] += timedelta(seconds=1)
    coordinator.set_state("sensor.robot_status_flag", "none")
    assert (
        asyncio.run(
            coordinator._async_reconcile_retained_task(
                current[0],
                allow_clear=True,
            )
        )
        is False
    )

    guard = coordinator.retained_task_guard
    assert guard is not None
    assert guard.clear_acknowledged_at == current[0].isoformat()
    assert guard.dock_clear_attempts == 1
    assert guard.dock_clear_requested_at == current[0].isoformat()
    assert guard.dock_clear_published_at == current[0].isoformat()
    assert guard.phase == logic.RETAINED_TASK_PHASE_DOCK_VERIFYING
    assert _service_names(coordinator).count("dock_action") == 1
    diagnostics = coordinator.retained_task_attributes
    assert diagnostics["retained_task_clear_attempts"] == 1
    assert diagnostics["retained_task_dock_clear_attempts"] == 1
    assert diagnostics["retained_task_clear_acknowledged_at"]
    assert diagnostics["retained_task_dock_clear_acknowledged_at"] is None
    dock_call = next(
        call
        for call in coordinator.hass.services.calls
        if call["service"] == "dock_action"
    )
    assert dock_call["domain"] == const.DOMAIN
    assert dock_call["data"] == {
        const.CONF_IDENTIFIER: "robot",
        "capability": "clean",
        "action": "stop",
    }

    asyncio.run(
        coordinator._async_reconcile_retained_task(
            current[0] + timedelta(seconds=1),
            allow_clear=True,
        )
    )
    assert _service_names(coordinator).count("dock_action") == 1

    current[0] += timedelta(seconds=2)
    coordinator.set_state(coordinator.vacuum_entity, "idle")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    assert (
        asyncio.run(
            coordinator._async_reconcile_retained_task(
                current[0],
                allow_clear=True,
            )
        )
        is True
    )

    assert guard.phase == logic.RETAINED_TASK_PHASE_CLEARED
    assert guard.dock_clear_acknowledged_at == current[0].isoformat()
    assert _service_names(coordinator).count("stop") == 1
    assert _service_names(coordinator).count("dock_action") == 1


def test_dock_stop_waits_for_stable_pause(monkeypatch) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(
        coordinator_module.dt_util,
        "utcnow",
        lambda: current[0],
    )
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
    )
    coordinator.config[const.CONF_IDENTIFIER] = "robot"
    coordinator.config[const.CONF_STALE_RESUME_SETTLE] = 5

    asyncio.run(coordinator._async_maybe_start_next_room())
    current[0] += timedelta(seconds=1)
    coordinator.set_state("sensor.robot_status_flag", "none")
    asyncio.run(
        coordinator._async_reconcile_retained_task(
            current[0],
            allow_clear=True,
        )
    )

    assert "dock_action" not in _service_names(coordinator)
    current[0] += timedelta(seconds=6)
    asyncio.run(
        coordinator._async_reconcile_retained_task(
            current[0],
            allow_clear=True,
        )
    )
    assert _service_names(coordinator).count("dock_action") == 1


def test_restart_before_dock_stop_publish_never_replays_uncertain_command(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
        phase=logic.RETAINED_TASK_PHASE_DOCK_CLEAR_PENDING,
    )
    coordinator.config[const.CONF_IDENTIFIER] = "robot"
    assert coordinator.retained_task_guard is not None
    coordinator.retained_task_guard.clear_attempts = 1
    coordinator.retained_task_guard.clear_acknowledged_at = now.isoformat()
    coordinator.retained_task_guard.dock_clear_attempts = 1
    coordinator.retained_task_guard.dock_clear_requested_at = now.isoformat()
    coordinator._store = _MemoryStore()
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_save_store(
            coordinator
        )
    )

    restored = _RecoverableFailureCoordinator()
    restored.active_run = None
    restored.manual_run = None
    restored.config[const.CONF_STALE_RESUME_AUTO_CLEAR] = True
    restored.config[const.CONF_IDENTIFIER] = "robot"
    restored._store = _MemoryStore(coordinator._store.data)
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_load_store(restored)
    )
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_status_flag", "none")
    restored.set_state("sensor.robot_dock_status", "pause")
    restored.set_state("sensor.robot_error", "No error")

    asyncio.run(
        restored._async_reconcile_retained_task(
            now,
            allow_clear=True,
            restored=True,
        )
    )

    assert restored.retained_task_guard is not None
    assert (
        restored.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED
    )
    assert "dock_action" not in _service_names(restored)


@pytest.mark.parametrize(
    "clear_order",
    [
        ("status", "dock"),
        ("dock", "status"),
    ],
)
def test_stop_ack_waits_for_coherent_flag_and_dock_then_continues_session(
    monkeypatch,
    clear_order,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(
        coordinator_module.dt_util,
        "utcnow",
        lambda: current[0],
    )
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert coordinator.started_rooms == []

    for index, transition in enumerate(clear_order, start=1):
        current[0] = now + timedelta(seconds=index)
        if transition == "status":
            coordinator.set_state("sensor.robot_status_flag", "none")
        else:
            coordinator.set_state("sensor.robot_dock_status", "idle")
        if index == 2:
            coordinator.set_state(coordinator.vacuum_entity, "idle")
        ready = asyncio.run(
            coordinator._async_reconcile_retained_task(
                current[0],
                allow_clear=True,
            )
        )
        assert ready is (index == 2)

    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_CLEARED
    )
    original_session = coordinator.session
    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.session is original_session
    assert coordinator.started_rooms == ["room_one"]
    assert _service_names(coordinator).count("stop") == 1


def test_stop_ack_requires_retained_settle_window(monkeypatch) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(
        coordinator_module.dt_util,
        "utcnow",
        lambda: current[0],
    )
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
    )
    coordinator.config[const.CONF_STALE_RESUME_SETTLE] = 60

    asyncio.run(coordinator._async_maybe_start_next_room())
    current[0] = now + timedelta(seconds=2)
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")

    assert (
        asyncio.run(
            coordinator._async_reconcile_retained_task(
                current[0],
                allow_clear=True,
            )
        )
        is False
    )
    assert coordinator.retained_task_guard is not None
    assert coordinator.retained_task_guard.clear_deadline is None
    assert coordinator._retained_task_next_deadline() == (
        current[0] + timedelta(seconds=60)
    )
    current[0] += timedelta(seconds=61)
    assert (
        asyncio.run(
            coordinator._async_reconcile_retained_task(
                current[0],
                allow_clear=True,
            )
        )
        is True
    )


def test_elapsed_retained_deadline_schedules_immediate_reconciliation(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=False,
        stale=True,
    )
    coordinator.session = None
    assert coordinator.retained_task_guard is not None
    coordinator.retained_task_guard.stale_deadline = (
        now - timedelta(seconds=1)
    ).isoformat()
    reconciliations = []

    async def reconcile_retained_task(
        observed_at,
        *,
        allow_clear,
        restored=False,
    ) -> bool:
        reconciliations.append((observed_at, allow_clear, restored))
        coordinator.retained_task_guard.stale_deadline = (
            now + timedelta(seconds=60)
        ).isoformat()
        return False

    coordinator._async_reconcile_retained_task = reconcile_retained_task
    coordinator.hass.async_create_task = lambda coroutine: asyncio.run(
        coroutine
    )

    coordinator._schedule_retained_task_timer()

    assert reconciliations == [(now, False, False)]
    assert coordinator._retained_task_reconcile_scheduled is False

    coordinator._schedule_retained_task_timer()

    assert reconciliations == [(now, False, False)]
    assert coordinator._retained_task_timer_cancel is not None


def test_restart_before_stop_publish_never_replays_uncertain_command(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
        phase=logic.RETAINED_TASK_PHASE_CLEAR_PENDING,
    )
    assert coordinator.retained_task_guard is not None
    coordinator.retained_task_guard.clear_attempts = 1
    coordinator.retained_task_guard.clear_requested_at = now.isoformat()
    coordinator._store = _MemoryStore()
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_save_store(
            coordinator
        )
    )

    restored = _RecoverableFailureCoordinator()
    restored.active_run = None
    restored.manual_run = None
    restored._store = _MemoryStore(coordinator._store.data)
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_load_store(restored)
    )
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_status_flag", "resumable")
    restored.set_state("sensor.robot_dock_status", "pause")
    restored.set_state("sensor.robot_error", "No error")

    asyncio.run(
        restored._async_reconcile_retained_task(
            now,
            allow_clear=True,
            restored=True,
        )
    )

    assert restored.retained_task_guard is not None
    assert (
        restored.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED
    )
    assert "stop" not in _service_names(restored)


def test_restart_after_stop_publish_waits_without_duplicate(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
        phase=logic.RETAINED_TASK_PHASE_VERIFYING,
    )
    assert coordinator.retained_task_guard is not None
    coordinator.retained_task_guard.clear_attempts = 1
    coordinator.retained_task_guard.clear_requested_at = now.isoformat()
    coordinator.retained_task_guard.clear_published_at = now.isoformat()
    coordinator.retained_task_guard.clear_deadline = (
        now + timedelta(seconds=30)
    ).isoformat()
    coordinator._store = _MemoryStore()
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_save_store(
            coordinator
        )
    )

    restored = _RecoverableFailureCoordinator()
    restored.active_run = None
    restored.manual_run = None
    restored._store = _MemoryStore(coordinator._store.data)
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_load_store(restored)
    )
    restored.set_state(restored.vacuum_entity, "docked")
    restored.set_state("sensor.robot_status_flag", "resumable")
    restored.set_state("sensor.robot_dock_status", "pause")
    restored.set_state("sensor.robot_error", "No error")

    asyncio.run(
        restored._async_reconcile_retained_task(
            now + timedelta(seconds=1),
            allow_clear=True,
            restored=True,
        )
    )

    assert restored.retained_task_guard is not None
    assert (
        restored.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_VERIFYING
    )
    assert "stop" not in _service_names(restored)


def test_owned_native_resume_never_enters_stale_clear_path() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_STALE_RESUME_AUTO_CLEAR] = True
    _trigger_low_battery(coordinator)

    ready = asyncio.run(
        coordinator._async_reconcile_retained_task(
            datetime.now(UTC),
            allow_clear=True,
        )
    )

    assert ready is False
    assert coordinator.active_run is not None
    assert coordinator.active_run.native_resume_pending is True
    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
    )
    assert "stop" not in _service_names(coordinator)


def test_external_cleaning_is_observed_without_commands_or_settings() -> None:
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.manual_run = None
    coordinator.session = logic.SessionState(
        session_id="session",
        started_at=logic.utcnow_iso(),
    )
    coordinator.config[const.CONF_STALE_RESUME_AUTO_CLEAR] = True
    coordinator.config[const.CONF_PASSES_ENTITY] = "input_select.robot_passes"
    coordinator.set_state("input_select.robot_passes", "3")
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "segment")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.owner
        == logic.RETAINED_TASK_OWNER_UNKNOWN
    )
    assert (
        coordinator.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OBSERVED_ACTIVE
    )
    assert coordinator.started_rooms == []
    assert "stop" not in _service_names(coordinator)
    assert "start" not in _service_names(coordinator)
    assert "select_option" not in _service_names(coordinator)


def test_arrival_cancels_session_without_stopping_unknown_task() -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=False,
        stale=True,
    )
    coordinator.set_state("person.owner", "home")

    asyncio.run(
        coordinator.async_cancel_session("Tracked person arrived home")
    )

    assert coordinator.session is not None
    assert coordinator.session.terminal_reason == "returned_home"
    assert "stop" not in _service_names(coordinator)
    assert "return_to_base" not in _service_names(coordinator)


def test_arrival_does_not_repeat_consumed_retained_stop() -> None:
    now = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=True,
        stale=True,
        owner=logic.RETAINED_TASK_OWNER_COORDINATOR,
        phase=logic.RETAINED_TASK_PHASE_VERIFYING,
    )
    assert coordinator.retained_task_guard is not None
    coordinator.retained_task_guard.clear_attempts = 1
    coordinator.retained_task_guard.clear_requested_at = now.isoformat()
    coordinator.retained_task_guard.clear_published_at = now.isoformat()
    coordinator.set_state("person.owner", "home")

    asyncio.run(
        coordinator.async_cancel_session("Tracked person arrived home")
    )

    assert coordinator.session is not None
    assert coordinator.session.terminal_reason == "returned_home"
    assert "stop" not in _service_names(coordinator)
    assert "return_to_base" not in _service_names(coordinator)


def test_incident_event_order_retains_manual_ownership_and_blocks_preflight(
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 24, 17, 48, 9, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(
        coordinator_module.dt_util,
        "utcnow",
        lambda: current[0],
    )
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = None
    coordinator.set_state("person.owner", "home")
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "segment")
    _handle_event(coordinator, coordinator.vacuum_entity, "cleaning")

    assert coordinator.manual_run is not None
    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.owner
        == logic.RETAINED_TASK_OWNER_MANUAL
    )

    current[0] += timedelta(seconds=43)
    coordinator.set_state(coordinator.vacuum_entity, "error")
    _handle_event(
        coordinator,
        "sensor.robot_error",
        "Mop Dock Wastewater Tank not installed or full",
    )
    _handle_event(coordinator, "sensor.robot_dock_status", "pause")

    current[0] = datetime(2026, 8, 24, 22, 28, 7, tzinfo=UTC)
    _handle_event(coordinator, coordinator.vacuum_entity, "docked")
    assert coordinator.manual_run is not None
    _handle_event(coordinator, "sensor.robot_status_flag", "resumable")
    _handle_event(coordinator, "sensor.robot_error", "No error")

    assert coordinator.manual_run is not None
    assert coordinator.retained_task_guard is not None
    assert (
        coordinator.retained_task_guard.owner
        == logic.RETAINED_TASK_OWNER_MANUAL
    )

    current[0] = datetime(2026, 8, 25, 16, 29, 11, tzinfo=UTC)
    coordinator.set_state("person.owner", "not_home")
    coordinator.session = logic.SessionState(
        session_id="next-away",
        started_at=current[0].isoformat(),
    )
    coordinator.retained_task_guard.last_material_activity_at = (
        current[0] - timedelta(hours=11)
    ).isoformat()
    coordinator.retained_task_guard.coherent_since = (
        current[0] - timedelta(hours=11)
    ).isoformat()
    coordinator.config[const.CONF_STALE_RESUME_AUTO_CLEAR] = False

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.started_rooms == []
    assert coordinator.session.settings_prepared is False
    assert (
        coordinator.retained_task_guard.phase
        == logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED
    )
    assert "stop" not in _service_names(coordinator)


def test_post_publish_pre_start_clean_water_race_stops_then_runs_native_rooms():
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="dining",
                name="Dining",
                segment_id="1",
                mop_required=True,
            ),
            logic.RoomConfig(
                room_id="office",
                name="Office",
                segment_id="2",
            ),
        ],
    )
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    coordinator.config[const.CONF_FRESH_WATER_ENTITY] = "sensor.robot_fresh_water"
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    asyncio.run(
        coordinator._async_start_room(
            coordinator.room_by_id["dining"],
            vacuum_only=False,
        )
    )
    assert coordinator.active_run is not None
    assert coordinator.active_run.command_published is True
    assert coordinator.active_run.start_confirmed_at is None

    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    _handle_event(
        coordinator,
        "sensor.robot_fresh_water",
        "empty",
    )

    service_names = _service_names(coordinator)
    assert service_names.count("stop") == 1
    assert service_names.count("return_to_base") == 0
    assert service_names.count("publish") == 2
    assert service_names.index("stop") < service_names.index(
        "publish",
        service_names.index("stop") + 1,
    )
    assert coordinator.session is not None
    assert coordinator.session.session_id == "same-away"
    assert coordinator.session.active is True
    assert coordinator.session.deferred_full_clean_room_ids == ["dining"]
    assert coordinator.session.last_command_recovery["stop_attempts"] == 1
    assert (
        coordinator.session.last_command_recovery["stop_acknowledged_at"]
        is not None
    )
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "office"
    assert coordinator.active_run.vacuum_only is True
    assert coordinator.active_run.fallback_vacuum is False


@pytest.mark.parametrize(
    ("checkpoint", "stop_attempts", "published_at", "acknowledged_at", "status", "completes"),
    [
        ("before_stop_publish", 0, None, None, "none", True),
        ("after_stop_publish", 1, "2026-09-02T17:00:00+00:00", None, "segment", False),
        ("before_stop_ack", 1, None, None, "segment", False),
        (
            "after_stop_ack",
            1,
            "2026-09-02T17:00:00+00:00",
            "2026-09-02T17:00:01+00:00",
            "none",
            True,
        ),
    ],
)
def test_pending_stop_restart_checkpoints_never_duplicate(
    checkpoint,
    stop_attempts,
    published_at,
    acknowledged_at,
    status,
    completes,
):
    coordinator = _RecoverableFailureCoordinator()
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
    )
    stored_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        command_published=True,
        phase=logic.RUN_PHASE_CANCEL_PENDING,
        cancelled=True,
        cancel_reason="Mop Dock Clean Water Tank empty",
        cancel_continue_session=True,
        cancel_requeue_room=True,
        cancel_stop_attempts=stop_attempts,
        cancel_stop_requested_at=published_at,
        cancel_stop_published_at=published_at,
        cancel_stop_acknowledged_at=acknowledged_at,
        cancel_ack_deadline=(
            datetime.now(UTC) + timedelta(seconds=30)
        ).isoformat(),
    )
    coordinator.active_run = logic.ActiveRun.from_dict(stored_run.to_dict())
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", status)
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    completed = asyncio.run(coordinator._async_execute_cancel_pending())

    assert completed is completes, checkpoint
    assert _service_names(coordinator).count("stop") == (
        1 if checkpoint == "before_stop_publish" else 0
    )
    if not completes:
        assert coordinator.active_run is not None
        coordinator.set_state("sensor.robot_status_flag", "none")
        assert asyncio.run(coordinator._async_execute_cancel_pending()) is True
        assert _service_names(coordinator).count("stop") == 0
    assert coordinator.active_run is None
    assert coordinator.session.last_command_recovery["stop_attempts"] == 1


def test_clean_water_fallback_false_still_runs_all_native_vacuum_rooms():
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="mop_room",
                name="Mop Room",
                segment_id="1",
                mop_required=True,
            ),
            logic.RoomConfig(room_id="native_one", name="Office", segment_id="2"),
            logic.RoomConfig(room_id="native_two", name="Studio", segment_id="3"),
        ],
    )
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    assert coordinator.started_rooms == ["native_one"]
    asyncio.run(coordinator._async_finish_active_run(success_override=True))
    assert coordinator.started_rooms == ["native_one", "native_two"]
    asyncio.run(coordinator._async_finish_active_run(success_override=True))

    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.session.deferred_full_clean_room_ids == ["mop_room"]


def test_resource_clear_resumes_deferred_mop_same_away_session():
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="bathroom",
        name="Bathroom",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        degraded_reason="Mop Dock Clean Water Tank empty",
        degraded_at=logic.utcnow_iso(),
        degraded_preparation_attempted=True,
        degraded_preparation_completed=True,
        attempted_room_ids=["bathroom"],
        fallback_attempted_room_ids=["bathroom"],
        fallback_completed_room_ids=["bathroom"],
        deferred_full_clean_room_ids=["bathroom"],
        deferred_full_clean_reasons={
            "bathroom": "Mop Dock Clean Water Tank empty",
        },
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "drying")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.session.session_id == "same-away"
    assert coordinator.started_rooms == ["bathroom"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.vacuum_only is False
    assert coordinator.active_run.fallback_vacuum is False


def test_healthy_dock_drying_does_not_strand_dispatch():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "drying")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.started_rooms == ["room_one"]


def test_operator_required_wait_is_bounded_and_resumes_when_clear():
    now = datetime.now(UTC)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=False,
        stale=True,
        phase=logic.RETAINED_TASK_PHASE_OPERATOR_REQUIRED,
    )
    assert coordinator.retained_task_guard is not None
    coordinator.retained_task_guard.operator_required_reason = (
        "physically clear the retained task"
    )
    asyncio.run(
        coordinator._async_reconcile_retained_task(
            now,
            allow_clear=True,
        )
    )

    assert coordinator.session is not None
    assert coordinator.session.active is True
    assert coordinator.session.recovery_phase == "operator_required"
    assert coordinator.session.next_retry_at is not None

    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "drying")
    coordinator.set_state("sensor.robot_error", "No error")
    asyncio.run(
        coordinator._async_reconcile_retained_task(
            now + timedelta(seconds=1),
            allow_clear=True,
        )
    )
    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.retained_task_guard.phase == (
        logic.RETAINED_TASK_PHASE_CLEARED
    )
    assert coordinator.started_rooms == ["room_one"]


def test_recoverable_resource_wait_rearms_and_resumes_on_clear():
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="native_room",
                name="Office",
                segment_id="1",
            ),
            logic.RoomConfig(
                room_id="mop_room",
                name="Bathroom",
                segment_id="2",
                mop_required=True,
            ),
        ],
    )
    coordinator.config[const.CONF_DIRTY_WATER_ENTITY] = "sensor.robot_dirty_water"
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")
    coordinator.set_state("sensor.robot_dirty_water", "full")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.session.active is True
    assert coordinator.session.recovery_phase == "degraded_vacuum"
    assert coordinator.started_rooms == ["native_room"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.vacuum_only is True
    assert coordinator.session.deferred_full_clean_room_ids == ["mop_room"]

    asyncio.run(coordinator._async_finish_active_run(success_override=True))

    first_retry = coordinator.session.next_retry_at
    assert coordinator.active_run is None
    assert coordinator.session.recovery_phase == "operator_required"
    assert first_retry is not None

    coordinator.session.next_retry_at = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    coordinator.session.blocked_deadline = coordinator.session.next_retry_at
    asyncio.run(coordinator._async_expire_blocked_session_serialized())
    assert coordinator.session.active is True
    assert coordinator.session.next_retry_at is not None

    _handle_event(coordinator, "sensor.robot_dirty_water", "ok")
    assert coordinator.started_rooms == ["native_room", "mop_room"]
    assert coordinator.active_run is not None
    assert coordinator.active_run.vacuum_only is False
    assert coordinator.session.terminal_reason is None


def test_away_to_away_transition_never_creates_duplicate_session():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    away_since = datetime.now(UTC) - timedelta(hours=1)
    coordinator.away_since = away_since.isoformat()
    coordinator.session = logic.SessionState(
        session_id="finished-same-away",
        started_at=(away_since + timedelta(minutes=5)).isoformat(),
        active=False,
        terminal_reason="complete",
        notification_sent=True,
    )

    _handle_event(coordinator, "person.owner", "work")

    assert coordinator.session.session_id == "finished-same-away"
    assert coordinator._away_timer_cancel is None
    assert coordinator.started_rooms == []


@pytest.mark.parametrize(
    "resource_error",
    [
        "Mop Dock Clean Water Tank empty",
        "Mop Dock Wastewater Tank not installed or full",
    ],
)
def test_post_clean_resource_fault_preserves_completed_floor_credit(
    resource_error,
):
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="dining",
        name="Dining",
        segment_id="1",
        mop_required=True,
        min_duration=150,
        min_area=10,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    coordinator.active_run = logic.ActiveRun(
        room_id="dining",
        segment_id="1",
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        start_area=0,
        start_time=0,
        command_published=True,
        command_publish_acknowledged_at=logic.utcnow_iso(),
        start_confirmed_at=logic.utcnow_iso(),
        phase=logic.RUN_PHASE_CLEANING,
        requested_iterations=2,
    )
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["dining"],
        active_room_id="dining",
    )
    coordinator.set_state("sensor.robot_area", "15")
    coordinator.set_state("sensor.robot_time", "2040")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    _observe_iteration_cycles(coordinator, 2)
    assert coordinator.active_run.observed_iteration_count == 2
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "cleaning")

    _handle_event(
        coordinator,
        "sensor.robot_error",
        resource_error,
    )

    assert coordinator.ledgers["dining"].successful_count == 1
    assert coordinator.session.completed_room_ids == ["dining"]
    assert coordinator.session.failed_room_ids == []
    projection = coordinator.while_away_outcome_contract["rooms"][0]
    assert projection["status"] == "completed"
    assert projection["credit"]["status"] == "full"


@pytest.mark.parametrize("status_configured", [True, False])
def test_ordinary_two_iteration_completion_preserves_legacy_credit(
    status_configured,
):
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        min_duration=120,
    )
    _set_rooms(coordinator, [room])
    if not status_configured:
        coordinator.config.pop(const.CONF_STATUS_FLAG_ENTITY)
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        active_room_id="room_one",
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        start_time=0,
        command_published=True,
        start_confirmed_at=logic.utcnow_iso(),
        phase=logic.RUN_PHASE_CLEANING,
        observed_cleaning=True,
        observed_segment_cleaning=status_configured,
        requested_iterations=2,
    )
    coordinator.set_state("sensor.robot_time", "200")
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")

    asyncio.run(coordinator._async_finish_active_run())

    assert coordinator.ledgers["room_one"].successful_count == 1
    assert coordinator.session.completed_room_ids == ["room_one"]
    assert coordinator.session.uncertain_room_ids == []


def test_partial_iterations_record_uncertain_without_full_credit_or_repeat():
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="dining",
        name="Dining",
        segment_id="1",
        mop_required=True,
        min_duration=150,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    coordinator.active_run = logic.ActiveRun(
        room_id="dining",
        segment_id="1",
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        start_time=0,
        command_published=True,
        start_confirmed_at=logic.utcnow_iso(),
        phase=logic.RUN_PHASE_CLEANING,
        requested_iterations=2,
    )
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["dining"],
        active_room_id="dining",
    )
    coordinator.set_state("sensor.robot_time", "500")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    _observe_iteration_cycles(coordinator, 1)
    assert coordinator.active_run.observed_iteration_count == 1
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "resumable")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )

    asyncio.run(
        coordinator._async_handle_active_run_error(
            "Mop Dock Clean Water Tank empty"
        )
    )

    assert coordinator.ledgers["dining"].successful_count == 0
    assert coordinator.session.completed_room_ids == []
    assert coordinator.session.uncertain_room_ids == ["dining"]
    assert coordinator.started_rooms == []
    projection = coordinator.while_away_outcome_contract["rooms"][0]
    assert projection["status"] == "uncertain"
    assert projection["credit"]["status"] == "none"

    coordinator.set_state("sensor.robot_dock_status", "drying")
    coordinator.set_state("sensor.robot_status_flag", "none")
    _handle_event(coordinator, "sensor.robot_error", "No error")

    assert coordinator.started_rooms == []
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "complete_with_uncertainty"


def test_resource_flapping_is_coherent_and_commands_notifications_do_not_storm():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_DIRTY_WATER_ENTITY] = "sensor.robot_dirty_water"
    coordinator.config[const.CONF_RESOURCE_SETTLE] = 3
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        command_published=True,
        start_confirmed_at=logic.utcnow_iso(),
        phase=logic.RUN_PHASE_CLEANING,
        observed_cleaning=True,
        observed_segment_cleaning=True,
    )
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        active_room_id="room_one",
    )
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_dirty_water", "ok")
    original_async_call = coordinator.hass.services.async_call

    async def acknowledge_return(domain, service, data, blocking=False):
        await original_async_call(domain, service, data, blocking)
        if domain == "vacuum" and service == "return_to_base":
            coordinator.set_state(coordinator.vacuum_entity, "docked")

    coordinator.hass.services.async_call = acknowledge_return

    _handle_event(coordinator, "sensor.robot_dirty_water", "full")
    assert _service_names(coordinator).count("stop") == 0
    _handle_event(coordinator, "sensor.robot_dirty_water", "ok")
    assert _service_names(coordinator).count("stop") == 0
    assert coordinator.active_run is not None

    _handle_event(coordinator, "sensor.robot_dirty_water", "full")
    assert coordinator._resource_candidates
    coordinator._resource_candidates["sensor.robot_dirty_water"]["deadline"] = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    asyncio.run(coordinator._async_confirm_resource_faults_serialized())

    assert _service_names(coordinator).count("stop") == 1
    assert _service_names(coordinator).count("return_to_base") == 1
    assert _service_names(coordinator).count("household") == 1
    _handle_event(coordinator, "sensor.robot_dirty_water", "full")
    _handle_event(coordinator, "sensor.robot_dirty_water", "full")
    assert _service_names(coordinator).count("stop") == 1
    assert _service_names(coordinator).count("household") == 1

    _handle_event(coordinator, "sensor.robot_dirty_water", "ok")
    assert _service_names(coordinator).count("household") == 2
    _handle_event(coordinator, "sensor.robot_dirty_water", "ok")
    assert _service_names(coordinator).count("household") == 2


def test_clean_water_flapping_does_not_preempt_pending_floor_command():
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Dining",
        segment_id="1",
        mop_required=True,
    )
    _set_rooms(coordinator, [room])
    coordinator.config[const.CONF_FRESH_WATER_ENTITY] = "sensor.robot_fresh_water"
    coordinator.config[const.CONF_RESOURCE_SETTLE] = 3
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        command_published=True,
        start_confirmed_at=logic.utcnow_iso(),
        phase=logic.RUN_PHASE_CLEANING,
        observed_cleaning=True,
        observed_segment_cleaning=True,
        requested_iterations=1,
    )
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        attempted_room_ids=["room_one"],
        active_room_id="room_one",
    )
    coordinator.set_state(coordinator.vacuum_entity, "cleaning")
    coordinator.set_state("sensor.robot_status_flag", "segment")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_fresh_water", "ok")

    _handle_event(coordinator, "sensor.robot_fresh_water", "empty")
    _handle_event(coordinator, "sensor.robot_fresh_water", "ok")

    assert coordinator.active_run is not None
    assert _service_names(coordinator).count("stop") == 0

    _handle_event(coordinator, "sensor.robot_fresh_water", "empty")
    coordinator._resource_candidates["sensor.robot_fresh_water"]["deadline"] = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    asyncio.run(coordinator._async_confirm_resource_faults_serialized())

    assert coordinator.active_run is None
    assert _service_names(coordinator).count("stop") == 1
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.deferred_full_clean_room_ids == ["room_one"]


def test_explicit_unrecoverable_error_terminalizes_without_auto_resume():
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.command_published = True
    coordinator.active_run.phase = logic.RUN_PHASE_DISPATCHING
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")

    _handle_event(
        coordinator,
        "sensor.robot_error",
        "Unrecoverable motor controller failure",
    )

    assert coordinator.session is not None
    assert coordinator.session.active is False
    assert coordinator.session.terminal_reason == "unrecoverable_error"
    assert coordinator.session.needs_help is True
    assert _service_names(coordinator).count("stop") == 1

    _handle_event(coordinator, "person.owner", "work")
    _handle_event(
        coordinator,
        "sensor.robot_error",
        "Unrecoverable motor controller failure",
    )
    assert coordinator.started_rooms == []
    assert _service_names(coordinator).count("stop") == 1


def test_battery_ticks_send_one_waiting_and_one_recovery_notice():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")

    for battery in ("20", "21", "22", "23"):
        _handle_event(coordinator, "sensor.robot_battery", battery)

    assert _service_names(coordinator).count("household") == 1
    assert coordinator.session.blocker_code == "power.charging"
    assert coordinator.session.blocked_reason == "battery is 23%, below 40%"

    _handle_event(coordinator, "sensor.robot_battery", "100")

    assert _service_names(coordinator).count("household") == 2
    assert coordinator.started_rooms == ["room_one"]
    assert coordinator.session.blocker_code is None


def test_same_blocker_notifies_again_after_a_genuine_clear():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )

    for battery in (20, 21):
        blocker = logic.classify_blocker(
            logic.ResourceState(),
            vacuum_state="docked",
            dock_status="idle",
            status_flag="none",
            battery=battery,
            minimum_battery=40,
        )
        assert blocker is not None
        asyncio.run(
            coordinator._async_enter_recoverable_wait(
                blocker,
                phase="waiting_for_retry",
                next_retry_at=None,
            )
        )

    assert _service_names(coordinator).count("household") == 1
    asyncio.run(coordinator._async_clear_recoverable_wait())
    assert _service_names(coordinator).count("household") == 2

    for battery in (22, 23):
        blocker = logic.classify_blocker(
            logic.ResourceState(),
            vacuum_state="docked",
            dock_status="idle",
            status_flag="none",
            battery=battery,
            minimum_battery=40,
        )
        assert blocker is not None
        asyncio.run(
            coordinator._async_enter_recoverable_wait(
                blocker,
                phase="waiting_for_retry",
                next_retry_at=None,
            )
        )

    assert _service_names(coordinator).count("household") == 3
    asyncio.run(coordinator._async_clear_recoverable_wait())
    assert _service_names(coordinator).count("household") == 4


def test_v020_terminal_migration_resets_notice_and_settings_lifecycle():
    coordinator = _RecoverableFailureCoordinator()
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
    )
    _set_rooms(coordinator, [room])
    coordinator.active_run = None
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.config[const.CONF_AUTO_CLEAN_ITERATIONS] = 2
    coordinator.set_state("select.robot_mode", "vacuum_and_mop")
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "20")
    coordinator._store = _MemoryStore(
        {
            "session": {
                "session_id": "v020-session",
                "started_at": "2026-09-02T15:00:00+00:00",
                "active": False,
                "cancelled": False,
                "attempted_room_ids": [],
                "terminal_reason": "blocked",
                "terminal_message": "dirty water is full",
                "terminal_cause": "blocked_timeout",
                "needs_help": False,
                "notification_sent": True,
                "settings_prepared": True,
            },
            "settings_snapshot": None,
            "rooms": {},
        }
    )
    coordinator._async_save_store = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_save_store,
        coordinator,
    )

    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_load_store(
            coordinator
        )
    )

    assert coordinator.session is not None
    assert coordinator.session.session_id == "v020-session"
    assert coordinator.session.active is True
    assert coordinator.session.terminal_reason is None
    assert coordinator.session.notification_sent is False
    assert coordinator.session.settings_prepared is False
    assert coordinator.settings_snapshot is None
    assert coordinator.session.recovery_notification_sent is False
    assert coordinator._store.data["coordinator_data_version"] == (
        const.COORDINATOR_DATA_VERSION
    )

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.session.settings_prepared is True
    assert coordinator.settings_snapshot is not None
    assert coordinator.settings_snapshot.mode == "vacuum_and_mop"
    assert coordinator.started_rooms == []
    assert _service_names(coordinator).count("household") == 1

    _handle_event(coordinator, "sensor.robot_battery", "100")

    assert coordinator.started_rooms == ["room_one"]
    assert _service_names(coordinator).count("household") == 2

    asyncio.run(coordinator._async_finish_active_run(success_override=True))

    assert coordinator.session.active is False
    assert coordinator.session.notification_sent is True
    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("select_option") == 1


@pytest.mark.parametrize("terminal_reason", ["blocked", "needs_help"])
def test_v03_terminal_session_remains_terminal_on_second_reload(
    terminal_reason,
):
    first = _RecoverableFailureCoordinator()
    first.active_run = None
    first.session = logic.SessionState(
        session_id="v03-terminal",
        started_at=logic.utcnow_iso(),
        active=False,
        terminal_reason=terminal_reason,
        terminal_message="dispatch operator review",
        terminal_cause="dispatch.operator_required",
        needs_help=terminal_reason == "needs_help",
        notification_sent=True,
        settings_prepared=True,
    )
    first._store = _MemoryStore()
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_save_store(first)
    )

    restored = _RecoverableFailureCoordinator()
    restored.active_run = None
    restored._store = _MemoryStore(first._store.data)
    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_load_store(
            restored
        )
    )

    assert restored.session is not None
    assert restored.session.session_id == "v03-terminal"
    assert restored.session.active is False
    assert restored.session.terminal_reason == terminal_reason
    assert restored.session.notification_sent is True
    assert restored.session.settings_prepared is True
    assert restored.settings_snapshot is None


def test_ignored_dispatch_escalates_and_waits_for_material_change():
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="room_one",
                name="Room One",
                segment_id="1",
            )
        ],
    )
    coordinator.active_run = None
    coordinator.config[const.CONF_BLOCKED_SESSION_TIMEOUT] = 1
    coordinator.config[const.CONF_DISPATCH_START_TIMEOUT] = 1
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")
    coordinator._async_start_room = types.MethodType(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room,
        coordinator,
    )

    asyncio.run(coordinator._async_maybe_start_next_room())
    retry_delays = []
    for expected_count in (1, 2, 3):
        assert coordinator.active_run is not None
        asyncio.run(coordinator._async_expire_dispatch_start())
        assert coordinator.active_run is None
        assert coordinator.session.dispatch_failure_counts == {
            "room_one": expected_count
        }
        if expected_count < 3:
            retry_at = logic.parse_datetime(
                coordinator.session.dispatch_retry_not_before["room_one"]
            )
            assert retry_at is not None
            retry_delays.append(
                (retry_at - datetime.now(UTC)).total_seconds()
            )
            coordinator.session.dispatch_retry_not_before["room_one"] = (
                datetime.now(UTC) - timedelta(seconds=1)
            ).isoformat()
            coordinator.session.next_retry_at = (
                coordinator.session.dispatch_retry_not_before["room_one"]
            )
            asyncio.run(coordinator._async_expire_blocked_session_serialized())

    assert retry_delays[1] > retry_delays[0]
    assert _service_names(coordinator).count("publish") == 3
    assert _service_names(coordinator).count("stop") == 3
    assert coordinator.session.dispatch_escalated_room_ids == ["room_one"]
    assert coordinator.session.blocker_code == "dispatch.operator_required"
    notices_before_recheck = _service_names(coordinator).count("household")
    assert notices_before_recheck <= 4

    for _ in range(3):
        asyncio.run(coordinator._async_expire_blocked_session_serialized())

    assert _service_names(coordinator).count("publish") == 3
    assert _service_names(coordinator).count("stop") == 3
    assert _service_names(coordinator).count("household") == (
        notices_before_recheck
    )

    _handle_event(coordinator, "sensor.robot_dock_status", "drying")

    assert _service_names(coordinator).count("publish") == 4
    assert coordinator.session.dispatch_failure_counts == {}
    assert coordinator.active_run is not None


def test_degraded_mode_retries_after_more_than_three_failures():
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="native",
                name="Office",
                segment_id="1",
            )
        ],
    )
    coordinator.active_run = None
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.config[const.CONF_BLOCKED_SESSION_TIMEOUT] = 1
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state("select.robot_mode", "vacuum_and_mop")
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "pause")
    coordinator.set_state(
        "sensor.robot_error",
        "Mop Dock Clean Water Tank empty",
    )
    attempts = 0

    async def fail_then_recover(*, vacuum_only: bool) -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 4:
            raise RuntimeError("mode service unavailable")
        coordinator.set_state("select.robot_mode", "vacuum")

    coordinator._async_apply_mode = fail_then_recover

    for _ in range(5):
        asyncio.run(coordinator._async_maybe_start_next_room())
        if coordinator.active_run:
            break
        assert coordinator.session.active is True
        assert coordinator.session.terminal_reason is None
        coordinator.session.degraded_mode_next_retry_at = (
            datetime.now(UTC) - timedelta(seconds=1)
        ).isoformat()
        coordinator.session.next_retry_at = (
            coordinator.session.degraded_mode_next_retry_at
        )

    assert attempts == 5
    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "native"
    assert coordinator.session.degraded_mode_attempts == 5
    assert _service_names(coordinator).count("household") == 2


@pytest.mark.parametrize(
    ("config_key", "entity_id", "state", "error"),
    [
        (
            const.CONF_DIRTY_WATER_ENTITY,
            "sensor.robot_dirty_water",
            "full",
            "No error",
        ),
        (
            const.CONF_DETERGENT_ENTITY,
            "sensor.robot_detergent",
            "empty",
            "No error",
        ),
        (
            const.CONF_DUSTBAG_ENTITY,
            "sensor.robot_dustbag",
            "full",
            "No error",
        ),
        (
            None,
            None,
            None,
            "Unknown error 120",
        ),
    ],
)
def test_safe_resource_blockers_continue_native_vacuum_rooms(
    config_key,
    entity_id,
    state,
    error,
):
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="mop_room",
                name="Bathroom",
                segment_id="1",
                mop_required=True,
            ),
            logic.RoomConfig(
                room_id="native_room",
                name="Office",
                segment_id="2",
            ),
        ],
    )
    coordinator.active_run = None
    coordinator.config[const.CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED] = False
    if config_key and entity_id:
        coordinator.config[config_key] = entity_id
        coordinator.set_state(entity_id, state)
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(
        coordinator.vacuum_entity,
        "error" if error != "No error" else "docked",
    )
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", error)
    coordinator.set_state("sensor.robot_battery", "100")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.active_run is not None
    assert coordinator.active_run.room_id == "native_room"
    assert coordinator.active_run.vacuum_only is True
    assert coordinator.active_run.fallback_vacuum is False
    assert coordinator.session.deferred_full_clean_room_ids == ["mop_room"]


@pytest.mark.parametrize(
    "error",
    [
        "Main brush jammed",
        "Cannot reach target",
        "Unrecoverable motor controller failure",
    ],
)
def test_real_robot_error_wins_over_unknown_resource_components(error):
    coordinator = _RecoverableFailureCoordinator()
    _set_rooms(
        coordinator,
        [
            logic.RoomConfig(
                room_id="native_room",
                name="Office",
                segment_id="1",
            )
        ],
    )
    coordinator.active_run = None
    coordinator.config[const.CONF_DIRTY_WATER_ENTITY] = (
        "sensor.robot_dirty_water"
    )
    coordinator.config[const.CONF_DETERGENT_ENTITY] = (
        "sensor.robot_detergent"
    )
    coordinator.config[const.CONF_DUSTBAG_ENTITY] = "sensor.robot_dustbag"
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "error")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", error)
    coordinator.set_state("sensor.robot_battery", "100")
    coordinator.set_state("sensor.robot_dirty_water", "unknown")
    coordinator.set_state("sensor.robot_detergent", "unavailable")
    coordinator.set_state("sensor.robot_dustbag", "unknown")

    asyncio.run(coordinator._async_maybe_start_next_room())

    assert coordinator.started_rooms == []
    assert coordinator.active_run is None


def test_cancel_ack_timer_is_independent_and_restored():
    coordinator = _RecoverableFailureCoordinator()
    assert coordinator.active_run is not None
    coordinator.active_run.phase = logic.RUN_PHASE_CANCEL_PENDING
    coordinator.active_run.cancelled = True
    coordinator.active_run.cancel_ack_deadline = (
        datetime.now(UTC) + timedelta(seconds=30)
    ).isoformat()
    dispatch_timer = lambda: None
    coordinator._dispatch_start_timeout_cancel = dispatch_timer

    coordinator._schedule_cancel_ack_timeout()

    assert coordinator._dispatch_start_timeout_cancel is dispatch_timer
    assert coordinator._cancel_ack_timeout_cancel is not None

    restored = _RecoverableFailureCoordinator()
    restored.active_run = logic.ActiveRun.from_dict(
        coordinator.active_run.to_dict()
    )
    restored._schedule_active_run_timers()

    assert restored._cancel_ack_timeout_cancel is not None
    assert restored._dispatch_start_timeout_cancel is None


def test_publish_failure_arms_cancel_ack_timer_before_restart():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
    )
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.set_state("sensor.robot_dock_status", "idle")
    coordinator.set_state("sensor.robot_error", "No error")
    coordinator.set_state("sensor.robot_battery", "100")
    original_async_call = coordinator.hass.services.async_call

    async def fail_publish_and_stop(domain, service, data, blocking=False):
        if (
            (domain == "mqtt" and service == "publish")
            or (domain == "vacuum" and service == "stop")
        ):
            raise RuntimeError("transport unavailable")
        await original_async_call(domain, service, data, blocking)

    coordinator.hass.services.async_call = fail_publish_and_stop

    asyncio.run(
        coordinator_module.ValetudoVacuumCoordinator._async_start_room(
            coordinator,
            coordinator.room_by_id["room_one"],
            vacuum_only=True,
        )
    )

    assert coordinator.active_run is not None
    assert coordinator.active_run.phase == logic.RUN_PHASE_CANCEL_PENDING
    assert coordinator.active_run.cancel_stop_attempts == 1
    assert coordinator._cancel_ack_timeout_cancel is not None

    restored = _RecoverableFailureCoordinator()
    restored.active_run = logic.ActiveRun.from_dict(
        coordinator.active_run.to_dict()
    )
    restored._schedule_active_run_timers()

    assert restored._cancel_ack_timeout_cancel is not None


def test_cancel_timer_completion_runs_terminal_cleanup():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.config[const.CONF_NOTIFY_SERVICE] = "notify.household"
    coordinator.config[const.CONF_MODE_ENTITY] = "select.robot_mode"
    coordinator.set_state("select.robot_mode", "vacuum")
    coordinator.set_state(coordinator.vacuum_entity, "docked")
    coordinator.set_state("sensor.robot_status_flag", "none")
    coordinator.session = logic.SessionState(
        session_id="cancelled",
        started_at=logic.utcnow_iso(),
        active=False,
        cancelled=True,
        completed_room_ids=["room_two"],
        terminal_reason="cancelled",
        terminal_message="service",
    )
    coordinator.settings_snapshot = logic.AutoCleanSettingsSnapshot(
        mode="vacuum_and_mop"
    )
    coordinator.active_run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="cancelled",
        started_at=logic.utcnow_iso(),
        command_published=True,
        phase=logic.RUN_PHASE_CANCEL_PENDING,
        cancelled=True,
        cancel_reason="service",
        cancel_continue_session=False,
        cancel_stop_attempts=1,
        cancel_stop_acknowledged_at=None,
        cancel_ack_deadline=(
            datetime.now(UTC) - timedelta(seconds=1)
        ).isoformat(),
    )

    asyncio.run(
        coordinator._async_reconcile_active_run_timers_serialized()
    )

    assert coordinator.active_run is None
    assert coordinator.session.notification_sent is True
    assert coordinator.settings_snapshot is None
    assert _service_names(coordinator).count("household") == 1
    assert _service_names(coordinator).count("select_option") == 1


def test_operator_required_without_session_keeps_reconciliation_timer():
    now = datetime.now(UTC)
    coordinator = _prepare_preflight_coordinator(
        now=now,
        auto_clear=False,
        stale=True,
    )
    coordinator.session = None

    asyncio.run(
        coordinator._async_mark_retained_task_operator_required(
            "physical task clear required"
        )
    )

    assert coordinator.retained_task_guard is not None
    first_deadline = logic.parse_datetime(
        coordinator.retained_task_guard.observation_deadline
    )
    assert first_deadline is not None
    assert coordinator._retained_task_timer_cancel is not None

    coordinator._retained_task_timer_cancel = None
    coordinator._retained_task_timer_deadline = None
    asyncio.run(
        coordinator._async_reconcile_retained_task(
            first_deadline + timedelta(seconds=1),
            allow_clear=False,
        )
    )

    second_deadline = logic.parse_datetime(
        coordinator.retained_task_guard.observation_deadline
    )
    assert second_deadline is not None
    assert second_deadline > first_deadline
    assert coordinator._retained_task_timer_cancel is not None


def test_auto_clean_binary_sensor_exposes_public_preserved_rooms():
    coordinator = _RecoverableFailureCoordinator()
    coordinator.active_run = None
    coordinator.session = logic.SessionState(
        session_id="same-away",
        started_at=logic.utcnow_iso(),
        dispatch_failure_counts={"room_one": 3},
        dispatch_escalated_room_ids=["room_one"],
    )
    sensor = binary_sensor_module.ValetudoAutoCleaningBinarySensor(
        coordinator
    )

    assert coordinator.preserved_room_ids == ["room_one", "room_two"]
    assert sensor.extra_state_attributes["preserved_rooms"] == [
        "room_one",
        "room_two",
    ]
