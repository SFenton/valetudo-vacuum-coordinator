"""Tests for the independent vacuum status observer."""

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
TEST_PACKAGE_NAME = "valetudo_vacuum_status_test"


def _install_homeassistant_stubs() -> None:
    homeassistant = types.ModuleType("homeassistant")
    core_module = types.ModuleType("homeassistant.core")

    class HomeAssistant:
        pass

    class State:
        def __init__(
            self,
            entity_id: str,
            state: str,
            when: datetime,
            attributes: dict | None = None,
            *,
            last_changed: datetime | None = None,
        ) -> None:
            self.entity_id = entity_id
            self.state = state
            self.attributes = attributes or {}
            self.last_changed = last_changed or when
            self.last_updated = when

    class Event:
        def __init__(self, entity_id: str, new_state: State | None) -> None:
            self.data = {"entity_id": entity_id, "new_state": new_state}

    def callback(func):
        return func

    core_module.Event = Event
    core_module.HomeAssistant = HomeAssistant
    core_module.State = State
    core_module.callback = callback

    components_module = types.ModuleType("homeassistant.components")
    sensor_module = types.ModuleType("homeassistant.components.sensor")

    class SensorEntity:
        def __init__(self) -> None:
            self._remove_callbacks = []
            self.write_count = 0

        def async_on_remove(self, callback) -> None:
            self._remove_callbacks.append(callback)

        def async_write_ha_state(self) -> None:
            self.write_count += 1

    sensor_module.SensorEntity = SensorEntity

    helpers_module = types.ModuleType("homeassistant.helpers")
    entity_module = types.ModuleType("homeassistant.helpers.entity")

    class DeviceInfo(dict):
        pass

    entity_module.DeviceInfo = DeviceInfo

    entity_platform_module = types.ModuleType(
        "homeassistant.helpers.entity_platform"
    )
    entity_platform_module.AddEntitiesCallback = object

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
    util_module.dt = dt_module

    sys.modules.setdefault("homeassistant", homeassistant)
    sys.modules.setdefault("homeassistant.components", components_module)
    sys.modules.setdefault("homeassistant.components.sensor", sensor_module)
    sys.modules.setdefault("homeassistant.core", core_module)
    sys.modules.setdefault("homeassistant.helpers", helpers_module)
    sys.modules.setdefault("homeassistant.helpers.entity", entity_module)
    sys.modules.setdefault(
        "homeassistant.helpers.entity_platform",
        entity_platform_module,
    )
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
status_module = _load_module(f"{package.__name__}.status", PACKAGE / "status.py")
entity_stub = types.ModuleType(f"{package.__name__}.entity")


class _ValetudoCoordinatorEntity:
    pass


entity_stub.ValetudoCoordinatorEntity = _ValetudoCoordinatorEntity
entity_stub.get_coordinator_from_discovery = lambda *args, **kwargs: None
sys.modules[entity_stub.__name__] = entity_stub
sensor_module = _load_module(f"{package.__name__}.sensor", PACKAGE / "sensor.py")


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def advance(self, seconds: int) -> datetime:
        self.now += timedelta(seconds=seconds)
        return self.now


class _ObserverState:
    def __init__(
        self,
        entity_id: str,
        state: str,
        when: datetime,
        attributes: dict | None = None,
        *,
        last_changed: datetime | None = None,
    ) -> None:
        self.entity_id = entity_id
        self.state = state
        self.attributes = attributes or {}
        self.last_changed = last_changed or when
        self.last_updated = when


class _ObserverEvent:
    def __init__(self, entity_id: str, new_state: _ObserverState | None) -> None:
        self.data = {"entity_id": entity_id, "new_state": new_state}


class _FakeStates:
    def __init__(self, clock: _Clock) -> None:
        self._clock = clock
        self._states: dict[str, object] = {}

    def set(
        self,
        entity_id: str,
        state: str,
        *,
        attributes: dict | None = None,
        last_changed: datetime | None = None,
    ):
        value = _ObserverState(
            entity_id,
            state,
            self._clock.now,
            attributes,
            last_changed=last_changed,
        )
        self._states[entity_id] = value
        return value

    def remove(self, entity_id: str) -> None:
        self._states.pop(entity_id, None)

    def get(self, entity_id: str):
        return self._states.get(entity_id)


class _ForbiddenServices:
    def __init__(self) -> None:
        self.calls: list[object] = []

    async def async_call(self, *args, **kwargs) -> None:
        self.calls.append((args, kwargs))
        raise AssertionError("Status observers must not call Home Assistant services")


class _FakeHass:
    def __init__(self, clock: _Clock) -> None:
        self.data = {}
        self.states = _FakeStates(clock)
        self.services = _ForbiddenServices()
        self.created_tasks = []

    def async_create_task(self, coroutine) -> None:
        self.created_tasks.append(coroutine)


class _MemoryStore:
    def __init__(self, data=None, *, fail_load: bool = False) -> None:
        self.data = data
        self.fail_load = fail_load
        self.save_count = 0

    async def async_load(self):
        if self.fail_load:
            raise ValueError("corrupt store")
        return self.data

    async def async_save(self, data) -> None:
        self.data = data
        self.save_count += 1


class _ScheduledCall:
    def __init__(self, delay: float, callback) -> None:
        self.delay = delay
        self.callback = callback
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class _Harness:
    def __init__(
        self,
        monkeypatch,
        *,
        config: dict | None = None,
        store=None,
        start: datetime | None = None,
    ) -> None:
        self.clock = _Clock(start or datetime(2026, 8, 27, 12, 0, tzinfo=UTC))
        self.hass = _FakeHass(self.clock)
        self.scheduled: list[_ScheduledCall] = []
        monkeypatch.setattr(status_module.dt_util, "utcnow", lambda: self.clock.now)

        def schedule(_hass, delay, callback):
            call = _ScheduledCall(delay, callback)
            self.scheduled.append(call)
            return call.cancel

        monkeypatch.setattr(status_module, "async_call_later", schedule)
        monkeypatch.setattr(
            status_module,
            "async_track_state_change_event",
            lambda *args, **kwargs: (lambda: None),
        )
        self.config = status_module.StatusObserverConfig.from_mapping(
            config or observer_mapping("main_floor")
        )
        self.observer = status_module.ValetudoVacuumStatusObserver(
            self.hass,
            self.config,
        )
        self.observer._store = store or _MemoryStore()

    def set_healthy(self, state: str = "docked") -> None:
        self.hass.states.set(self.config.vacuum_entity, state)
        self.hass.states.set(self.config.error_entity, "No error")
        self.hass.states.set(self.config.status_flag_entity, "none")
        self.hass.states.set(self.config.dock_status_entity, "idle")
        self.hass.states.set(self.config.battery_entity, "100")
        for condition in self.config.conditions:
            self.hass.states.set(condition.entity_id, "off")

    def set_all_unavailable(self) -> None:
        for entity_id in self.config.watched_entity_ids:
            self.hass.states.set(entity_id, "unavailable")

    async def setup(self) -> None:
        await self.observer.async_setup()

    async def update(
        self,
        entity_id: str,
        state: str,
        *,
        attributes: dict | None = None,
        last_changed: datetime | None = None,
    ) -> None:
        new_state = self.hass.states.set(
            entity_id,
            state,
            attributes=attributes,
            last_changed=last_changed,
        )
        await self.observer._async_handle_state_change_event(
            _ObserverEvent(entity_id, new_state)
        )


def observer_mapping(observer_id: str, *, conditions=None) -> dict:
    prefix = {
        "main_floor": "valetudo_exaltedsneakydeer",
        "music_room": "valetudo_elatedusedram",
        "theater_room": "valetudo_politefatherlykingfisher",
    }[observer_id]
    return {
        "id": observer_id,
        "name": observer_id.replace("_", " ").title(),
        "vacuum_entity": f"vacuum.{prefix}",
        "error_entity": f"sensor.{prefix}_error",
        "status_flag_entity": f"sensor.{prefix}_status_flag",
        "dock_status_entity": f"sensor.{prefix}_dock_status",
        "battery_entity": f"sensor.{prefix}_battery_level",
        "conditions": conditions or [],
        "outage_confirmation_seconds": 180,
    }


def run(coroutine):
    return asyncio.run(coroutine)


def test_issue_code_mapping_is_stable_and_preserves_unknown_errors() -> None:
    assert status_module.issue_code_for_raw("Low battery") == "power.low_battery"
    assert status_module.issue_code_for_raw("Unknown error 75") == "power.critical_battery"
    assert status_module.issue_code_for_raw("Unknown error 444") == "vendor.unknown_error_444"
    first = status_module.issue_code_for_raw("Unexpected vendor fault")
    assert first == status_module.issue_code_for_raw("  Unexpected   vendor fault ")
    assert first.startswith("unmapped.unexpected_vendor_fault.")


def test_manifest_and_runtime_versions_match() -> None:
    manifest = json.loads(
        (PACKAGE / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["version"] == const.VERSION == "0.3.1"


def test_legacy_and_structured_configs_split_without_changing_legacy_shape() -> None:
    legacy = {"name": "Legacy", "vacuum_entity": "vacuum.robot"}
    assert status_module.split_integration_config(legacy) == ([legacy], [])

    coordinators = [legacy]
    observers = [observer_mapping("main_floor")]
    assert status_module.split_integration_config(
        {"coordinators": coordinators, "status_observers": observers}
    ) == (coordinators, observers)
    assert status_module.split_integration_config(
        {"status_observers": observers[0]}
    ) == ([], observers)

    with pytest.raises(ValueError, match="move the existing coordinator"):
        status_module.split_integration_config(
            {
                **legacy,
                "people": ["person.owner"],
                "segment_command_topic": "valetudo/robot/clean/set",
                "rooms": [],
                "status_observers": observers,
            }
        )


def test_all_three_observer_configs_are_independent_from_room_configuration() -> None:
    main = observer_mapping(
        "main_floor",
        conditions=[
            {
                "entity_id": "binary_sensor.main_floor_vacuum_coordinator_native_resume_pending",
                "code": "vacuum.task_resume_pending",
                "source": "coordinator",
            }
        ],
    )
    music = observer_mapping(
        "music_room",
        conditions=[
            {
                "entity_id": "input_boolean.music_room_vacuum_fallback_active",
                "code": "vacuum.scheduled_vacuum_only_fallback",
                "source": "automation",
            }
        ],
    )
    configs = [
        status_module.StatusObserverConfig.from_mapping(raw)
        for raw in (main, music, observer_mapping("theater_room"))
    ]
    assert [config.observer_id for config in configs] == [
        "main_floor",
        "music_room",
        "theater_room",
    ]
    assert [len(config.watched_entity_ids) for config in configs] == [6, 6, 5]


def test_invalid_observer_configuration_is_rejected() -> None:
    invalid_id = observer_mapping("main_floor")
    invalid_id["id"] = "Main Floor"
    with pytest.raises(ValueError, match="lowercase"):
        status_module.StatusObserverConfig.from_mapping(invalid_id)

    missing_error = observer_mapping("main_floor")
    missing_error["error_entity"] = ""
    with pytest.raises(ValueError, match="error_entity"):
        status_module.StatusObserverConfig.from_mapping(missing_error)

    invalid_source = observer_mapping(
        "main_floor",
        conditions=[
            {
                "entity_id": "binary_sensor.condition",
                "code": "vacuum.condition",
                "source": "device",
            }
        ],
    )
    with pytest.raises(ValueError, match="Unsupported"):
        status_module.StatusObserverConfig.from_mapping(invalid_source)


def test_healthy_observer_is_clear_and_normal_without_service_calls(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())

    contract = harness.observer.contract
    assert contract["version"] == 1
    assert contract["vacuum_entity_id"] == harness.config.vacuum_entity
    assert contract["observed_vacuum_state"] == "docked"
    assert contract["availability"] == {"status": "available", "since": None}
    assert contract["current_issue"] == {"status": "clear"}
    assert contract["active_conditions"] == []
    assert contract["last_issue"] is None
    assert contract["command_policy"] == {"mode": "normal", "reason": None}
    assert harness.hass.services.calls == []


def test_status_sensor_exposes_stable_identity_and_contract(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    sensor = sensor_module.ValetudoVacuumStatusSensor(harness.observer)

    assert sensor.native_value == "docked"
    assert sensor.extra_state_attributes == harness.observer.contract
    assert sensor._attr_name == "Main Floor Status"
    assert sensor._attr_unique_id == "main_floor_vacuum_status"
    assert sensor._attr_suggested_object_id == "main_floor_vacuum_status"
    assert sensor.device_info["identifiers"] == {
        (const.DOMAIN, "status_observer_main_floor")
    }


def test_sensor_platform_discovers_one_status_sensor(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    harness.hass.data = {
        const.DOMAIN: {
            const.STATUS_DATA_KEY: {
                harness.observer.observer_id: harness.observer,
            }
        }
    }
    added = []

    run(
        sensor_module.async_setup_platform(
            harness.hass,
            {},
            lambda entities: added.extend(entities),
            {"status_observer_id": harness.observer.observer_id},
        )
    )

    assert len(added) == 1
    assert isinstance(added[0], sensor_module.ValetudoVacuumStatusSensor)


def test_primary_unavailable_is_immediate_but_history_waits_for_confirmation(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())

    harness.clock.advance(10)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))

    contract = harness.observer.contract
    assert contract["observed_vacuum_state"] == "unavailable"
    assert contract["availability"] == {"status": "unavailable", "since": None}
    assert contract["current_issue"] == {"status": "unknown"}
    assert contract["last_issue"] is None
    assert contract["command_policy"] == {
        "mode": "none",
        "reason": "primary_unavailable",
    }
    assert harness.scheduled[-1].delay == pytest.approx(180)


def test_startup_uses_original_unavailable_time_for_confirmation(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_all_unavailable()
    unavailable_at = harness.clock.now - timedelta(seconds=300)
    harness.hass.states.set(
        harness.config.vacuum_entity,
        "unavailable",
        last_changed=unavailable_at,
    )
    run(harness.setup())

    assert harness.scheduled[-1].delay == pytest.approx(0)
    run(harness.observer._async_confirm_outage(harness.clock.now))
    assert harness.observer.contract["availability"]["since"] == unavailable_at.isoformat()


@pytest.mark.parametrize("outage_seconds", [77, 114])
def test_transient_outage_does_not_latch_history_before_180_seconds(
    monkeypatch,
    outage_seconds,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    issue_time = harness.observer.contract["current_issue"]["reported_at"]

    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(outage_seconds)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    assert harness.observer.contract["availability"]["since"] is None
    assert harness.observer.contract["last_issue"] is None
    assert harness.observer._pending_issue.reported_at == issue_time

    harness.set_healthy()
    run(
        harness.observer._async_reconcile(
            harness.clock.now,
            event_entity_id=harness.config.vacuum_entity,
        )
    )
    assert harness.observer.contract["availability"]["status"] == "available"
    assert harness.observer.contract["last_issue"] is None


def test_confirmed_outage_latches_original_time_and_prior_issue(monkeypatch) -> None:
    store = _MemoryStore()
    harness = _Harness(monkeypatch, store=store)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Unknown error 75"))
    issue = harness.observer.contract["current_issue"]

    harness.clock.advance(5)
    unavailable_at = harness.clock.now
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    contract = harness.observer.contract
    assert contract["availability"]["since"] == unavailable_at.isoformat()
    assert contract["last_issue"] == {
        "code": "power.critical_battery",
        "raw": "Unknown error 75",
        "reported_at": issue["reported_at"],
        "provenance": "observed",
        "cleared_at": None,
    }
    assert store.data["availability_since"] == unavailable_at.isoformat()
    assert store.data["last_issue"] == contract["last_issue"]


def test_error_unreadable_before_primary_unavailable_retains_last_readable_issue(
    monkeypatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    original_issue = harness.observer.contract["current_issue"]

    harness.clock.advance(1)
    run(harness.update(harness.config.error_entity, "unavailable"))
    assert harness.observer.contract["current_issue"] == {"status": "unknown"}

    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    assert harness.observer.contract["last_issue"] == {
        "code": "navigation.target_unreachable",
        "raw": "Cannot reach target",
        "reported_at": original_issue["reported_at"],
        "provenance": "observed",
        "cleared_at": None,
    }


def test_primary_unavailable_before_error_unreadable_retains_current_issue(
    monkeypatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    original_issue = harness.observer.contract["current_issue"]

    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(1)
    run(harness.update(harness.config.error_entity, "unavailable"))
    harness.clock.advance(179)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    assert harness.observer.contract["last_issue"]["raw"] == "Cannot reach target"
    assert (
        harness.observer.contract["last_issue"]["reported_at"]
        == original_issue["reported_at"]
    )


@pytest.mark.parametrize(
    "degraded_entity",
    [
        "status_flag_entity",
        "battery_entity",
        "dock_status_entity",
    ],
)
def test_degraded_source_prevents_clear_from_discarding_issue_candidate(
    monkeypatch,
    degraded_entity,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Robot stuck or trapped"))
    original_issue = harness.observer.contract["current_issue"]

    harness.clock.advance(1)
    run(harness.update(getattr(harness.config, degraded_entity), "unavailable"))
    harness.clock.advance(1)
    run(harness.update(harness.config.error_entity, "No error"))
    assert harness.observer.contract["current_issue"] == {"status": "clear"}

    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    assert harness.observer.contract["last_issue"]["raw"] == "Robot stuck or trapped"
    assert (
        harness.observer.contract["last_issue"]["reported_at"]
        == original_issue["reported_at"]
    )


def test_coherent_explicit_clear_discards_issue_candidate(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    harness.clock.advance(1)
    run(harness.update(harness.config.error_entity, "unavailable"))
    harness.clock.advance(1)
    run(harness.update(harness.config.error_entity, "No error"))

    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))
    assert harness.observer.contract["last_issue"] is None


def test_rapid_disconnect_burst_processes_queued_event_snapshots_in_order(
    monkeypatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())

    harness.clock.advance(1)
    readable_issue = harness.hass.states.set(
        harness.config.error_entity,
        "Unknown error 75",
    )
    issue_event = _ObserverEvent(harness.config.error_entity, readable_issue)
    harness.clock.advance(1)
    unreadable_error = harness.hass.states.set(
        harness.config.error_entity,
        "unavailable",
    )
    error_event = _ObserverEvent(harness.config.error_entity, unreadable_error)
    harness.clock.advance(1)
    unavailable_primary = harness.hass.states.set(
        harness.config.vacuum_entity,
        "unavailable",
    )
    primary_event = _ObserverEvent(
        harness.config.vacuum_entity,
        unavailable_primary,
    )

    harness.observer._handle_state_change_event(issue_event)
    harness.observer._handle_state_change_event(error_event)
    harness.observer._handle_state_change_event(primary_event)
    assert len(harness.hass.created_tasks) == 1
    run(harness.hass.created_tasks.pop())

    assert harness.observer.contract["current_issue"] == {"status": "unknown"}
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))
    assert harness.observer.contract["last_issue"]["raw"] == "Unknown error 75"
    assert harness.observer.contract["last_issue"]["reported_at"] == (
        readable_issue.last_changed.isoformat()
    )


@pytest.mark.parametrize("disconnect_order", ["error_first", "primary_first"])
def test_latest_issue_wins_across_queued_multi_issue_disconnect_burst(
    monkeypatch,
    disconnect_order,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())

    harness.clock.advance(1)
    issue_a = harness.hass.states.set(
        harness.config.error_entity,
        "Cannot reach target",
    )
    issue_a_event = _ObserverEvent(harness.config.error_entity, issue_a)
    harness.clock.advance(1)
    issue_b = harness.hass.states.set(
        harness.config.error_entity,
        "Unknown error 75",
    )
    issue_b_event = _ObserverEvent(harness.config.error_entity, issue_b)

    disconnect_events = []
    for entity_id in (
        (
            harness.config.error_entity,
            harness.config.vacuum_entity,
        )
        if disconnect_order == "error_first"
        else (
            harness.config.vacuum_entity,
            harness.config.error_entity,
        )
    ):
        harness.clock.advance(1)
        state = harness.hass.states.set(entity_id, "unavailable")
        disconnect_events.append(_ObserverEvent(entity_id, state))

    harness.observer._handle_state_change_event(issue_a_event)
    harness.observer._handle_state_change_event(issue_b_event)
    for event in disconnect_events:
        harness.observer._handle_state_change_event(event)
    run(harness.hass.created_tasks.pop())

    assert harness.observer._pending_issue.raw == "Unknown error 75"
    assert harness.observer._pending_issue.reported_at == issue_b.last_changed.isoformat()
    pending_since = datetime.fromisoformat(
        harness.observer._pending_unavailable_since
    )
    run(
        harness.observer._async_confirm_outage(
            pending_since + timedelta(seconds=180)
        )
    )
    assert harness.observer.contract["last_issue"] == {
        "code": "power.critical_battery",
        "raw": "Unknown error 75",
        "reported_at": issue_b.last_changed.isoformat(),
        "provenance": "observed",
        "cleared_at": None,
    }


def test_newer_candidate_beats_existing_current_issue_when_outage_starts(
    monkeypatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))

    harness.clock.advance(1)
    issue_b = harness.hass.states.set(
        harness.config.error_entity,
        "Unknown error 75",
    )
    issue_b_event = _ObserverEvent(harness.config.error_entity, issue_b)
    harness.clock.advance(1)
    harness.hass.states.set(harness.config.error_entity, "unavailable")
    primary = harness.hass.states.set(
        harness.config.vacuum_entity,
        "unavailable",
    )
    harness.observer._handle_state_change_event(issue_b_event)
    harness.observer._handle_state_change_event(
        _ObserverEvent(harness.config.vacuum_entity, primary)
    )
    run(harness.hass.created_tasks.pop())

    assert harness.observer._pending_issue.raw == "Unknown error 75"
    assert harness.observer._pending_issue.reported_at == issue_b.last_changed.isoformat()


def test_event_queued_during_drain_completion_is_rescheduled(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    initial_event = _ObserverEvent(
        harness.config.status_flag_entity,
        harness.hass.states.get(harness.config.status_flag_entity),
    )
    late_event = _ObserverEvent(
        harness.config.error_entity,
        harness.hass.states.get(harness.config.error_entity),
    )
    original_finish = harness.observer._finish_event_drain
    injected = False

    def finish_with_late_event() -> None:
        nonlocal injected
        if not injected:
            injected = True
            harness.observer._event_queue.append(late_event)
        original_finish()

    monkeypatch.setattr(
        harness.observer,
        "_finish_event_drain",
        finish_with_late_event,
    )
    harness.observer._handle_state_change_event(initial_event)
    first_drain = harness.hass.created_tasks.pop()
    run(first_drain)

    assert len(harness.hass.created_tasks) == 1
    assert harness.observer._event_drain_scheduled is True
    assert list(harness.observer._event_queue) == [late_event]

    run(harness.hass.created_tasks.pop())
    assert harness.observer._event_drain_scheduled is False
    assert list(harness.observer._event_queue) == []
    assert harness.hass.created_tasks == []


def test_recovery_clears_outage_and_marks_historical_issue_cleared(monkeypatch) -> None:
    store = _MemoryStore()
    harness = _Harness(monkeypatch, store=store)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    harness.clock.advance(10)
    recovered_at = harness.clock.now
    harness.set_healthy()
    run(
        harness.observer._async_reconcile(
            recovered_at,
            event_entity_id=harness.config.vacuum_entity,
        )
    )

    contract = harness.observer.contract
    assert contract["availability"] == {"status": "available", "since": None}
    assert contract["current_issue"] == {"status": "clear"}
    assert contract["last_issue"]["cleared_at"] == recovered_at.isoformat()
    assert contract["command_policy"] == {"mode": "normal", "reason": None}
    assert store.data["availability_since"] is None


def test_recovery_waits_for_coherent_clear_error_before_clearing_history(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    harness.clock.advance(10)
    harness.set_healthy()
    harness.hass.states.set(harness.config.error_entity, "unavailable")
    run(
        harness.observer._async_reconcile(
            harness.clock.now,
            event_entity_id=harness.config.vacuum_entity,
        )
    )
    assert harness.observer.contract["last_issue"]["cleared_at"] is None
    assert harness.observer.contract["current_issue"] == {"status": "unknown"}

    harness.clock.advance(1)
    run(harness.update(harness.config.error_entity, "No error"))
    assert (
        harness.observer.contract["last_issue"]["cleared_at"]
        == harness.clock.now.isoformat()
    )


def test_later_outage_without_new_issue_keeps_old_history_cleared(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    harness.clock.advance(1)
    harness.set_healthy()
    run(
        harness.observer._async_reconcile(
            harness.clock.now,
            event_entity_id=harness.config.vacuum_entity,
        )
    )
    cleared_at = harness.observer.contract["last_issue"]["cleared_at"]
    assert cleared_at is not None

    harness.clock.advance(10)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    assert harness.observer.contract["last_issue"]["cleared_at"] == cleared_at
    assert harness.observer.contract["last_issue"]["raw"] == "Cannot reach target"


def test_startup_restores_history_but_never_restores_current_issue(monkeypatch) -> None:
    reported_at = datetime(2026, 8, 20, 9, 15, tzinfo=UTC).isoformat()
    unavailable_since = datetime(2026, 8, 20, 10, 0, tzinfo=UTC).isoformat()
    store = _MemoryStore(
        {
            "availability_since": unavailable_since,
            "last_issue": {
                "code": "navigation.stuck",
                "raw": "Robot stuck or trapped",
                "reported_at": reported_at,
                "provenance": "observed",
                "cleared_at": None,
            },
            "current_issue": {
                "status": "present",
                "raw": "Must never restore",
            },
        }
    )
    harness = _Harness(monkeypatch, store=store)
    harness.set_all_unavailable()
    run(harness.setup())

    contract = harness.observer.contract
    assert contract["availability"] == {
        "status": "unavailable",
        "since": unavailable_since,
    }
    assert contract["current_issue"] == {"status": "unknown"}
    assert contract["last_issue"]["raw"] == "Robot stuck or trapped"


@pytest.mark.parametrize("stored", [None, [], "broken", {"last_issue": {"raw": 1}}])
def test_missing_or_corrupt_store_is_ignored(monkeypatch, stored) -> None:
    harness = _Harness(monkeypatch, store=_MemoryStore(stored))
    harness.set_healthy()
    run(harness.setup())
    assert harness.observer.contract["last_issue"] is None


def test_store_load_failure_is_safe(monkeypatch, caplog) -> None:
    harness = _Harness(monkeypatch, store=_MemoryStore(fail_load=True))
    harness.set_healthy()
    run(harness.setup())
    assert harness.observer.contract["availability"]["status"] == "available"
    assert "Unable to load vacuum status observer store" in caplog.text


def test_same_state_attribute_event_is_processed_but_unchanged_contract_is_not_published(
    monkeypatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    notifications = 0

    def updated() -> None:
        nonlocal notifications
        notifications += 1

    harness.observer.async_add_listener(updated)
    run(
        harness.update(
            harness.config.error_entity,
            "Unknown error 75",
            attributes={"severity": {"kind": "transient", "level": "info"}},
        )
    )
    first = harness.observer.contract["current_issue"]
    assert notifications == 1

    harness.clock.advance(1)
    run(
        harness.update(
            harness.config.error_entity,
            "Unknown error 75",
            attributes={"severity": {"kind": "unknown", "level": "unknown"}},
            last_changed=datetime.fromisoformat(first["reported_at"]),
        )
    )
    assert harness.observer.contract["current_issue"] == first
    assert notifications == 1


def test_same_raw_error_after_new_availability_epoch_is_a_new_observation(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Unknown error 75"))
    first_reported_at = harness.observer.contract["current_issue"]["reported_at"]

    harness.clock.advance(1)
    harness.set_all_unavailable()
    run(
        harness.observer._async_reconcile(
            harness.clock.now,
            event_entity_id=harness.config.vacuum_entity,
        )
    )
    harness.clock.advance(30)
    for entity_id, state in (
        (harness.config.error_entity, "Unknown error 75"),
        (harness.config.status_flag_entity, "none"),
        (harness.config.dock_status_entity, "idle"),
        (harness.config.battery_entity, "20"),
        (harness.config.vacuum_entity, "error"),
    ):
        harness.hass.states.set(entity_id, state)
    run(
        harness.observer._async_reconcile(
            harness.clock.now,
            event_entity_id=harness.config.vacuum_entity,
        )
    )

    second = harness.observer.contract["current_issue"]
    assert second["raw"] == "Unknown error 75"
    assert second["reported_at"] != first_reported_at
    assert second["reported_at"] == harness.clock.now.isoformat()
    assert harness.observer.contract["last_issue"] is None


def test_missing_primary_and_unknown_error_source_fail_closed(monkeypatch) -> None:
    missing = _Harness(monkeypatch)
    run(missing.setup())
    assert missing.observer.contract["observed_vacuum_state"] is None
    assert missing.observer.contract["availability"]["status"] == "missing"
    assert missing.observer.contract["command_policy"] == {
        "mode": "none",
        "reason": "primary_missing",
    }

    partial = _Harness(monkeypatch)
    partial.set_healthy()
    partial.hass.states.set(partial.config.error_entity, "unavailable")
    run(partial.setup())
    assert partial.observer.contract["current_issue"] == {"status": "unknown"}
    assert partial.observer.contract["command_policy"] == {
        "mode": "restricted",
        "reason": "source_unreadable",
    }


def test_primary_unknown_is_immediate_and_starts_pending_history(
    monkeypatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unknown"))

    assert harness.observer.contract["availability"] == {
        "status": "unknown",
        "since": None,
    }
    assert harness.observer.contract["current_issue"] == {"status": "unknown"}
    assert harness.observer.contract["command_policy"] == {
        "mode": "none",
        "reason": "primary_unknown",
    }
    first_unknown = harness.clock.now
    assert harness.observer._pending_unavailable_since == first_unknown.isoformat()
    assert harness.scheduled[-1].delay == pytest.approx(180)

    harness.clock.advance(3)
    run(harness.update(harness.config.vacuum_entity, "unavailable"))
    assert harness.observer._pending_unavailable_since == first_unknown.isoformat()


@pytest.mark.parametrize("outage_seconds", [77, 114])
def test_short_unknown_flap_does_not_latch_prior_issue(
    monkeypatch,
    outage_seconds,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Cannot reach target"))
    harness.clock.advance(1)
    run(harness.update(harness.config.vacuum_entity, "unknown"))

    harness.clock.advance(outage_seconds)
    run(harness.observer._async_confirm_outage(harness.clock.now))
    assert harness.observer.contract["availability"]["since"] is None
    assert harness.observer.contract["last_issue"] is None

    harness.set_healthy()
    run(
        harness.observer._async_reconcile(
            harness.clock.now,
            event_entity_id=harness.config.vacuum_entity,
        )
    )
    assert harness.observer.contract["last_issue"] is None


def test_persistent_unknown_latches_original_time_and_prior_issue(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    run(harness.setup())
    run(harness.update(harness.config.error_entity, "Unknown error 75"))
    issue = harness.observer.contract["current_issue"]

    harness.clock.advance(1)
    unknown_at = harness.clock.now
    run(harness.update(harness.config.vacuum_entity, "unknown"))
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))

    assert harness.observer.contract["availability"] == {
        "status": "unknown",
        "since": unknown_at.isoformat(),
    }
    assert harness.observer.contract["last_issue"] == {
        "code": "power.critical_battery",
        "raw": "Unknown error 75",
        "reported_at": issue["reported_at"],
        "provenance": "observed",
        "cleared_at": None,
    }


@pytest.mark.parametrize(
    ("entity_key", "state"),
    [
        ("status_flag_entity", "unavailable"),
        ("dock_status_entity", "unknown"),
        ("battery_entity", "not-a-number"),
    ],
)
def test_unreadable_required_sources_restrict_commands(
    monkeypatch,
    entity_key,
    state,
) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    harness.hass.states.set(getattr(harness.config, entity_key), state)
    run(harness.setup())
    assert harness.observer.contract["command_policy"] == {
        "mode": "restricted",
        "reason": "source_unreadable",
    }


def test_status_flag_resumable_is_derived_condition_and_restricts_commands(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy()
    harness.hass.states.set(harness.config.status_flag_entity, "resumable")
    run(harness.setup())

    assert harness.observer.contract["active_conditions"] == [
        {
            "code": "vacuum.task_resume_pending",
            "source": "derived",
            "since": harness.clock.now.isoformat(),
        }
    ]
    assert harness.observer.contract["command_policy"] == {
        "mode": "restricted",
        "reason": "vacuum.task_resume_pending",
    }


def test_optional_coordinator_condition_overrides_derived_resume_source(monkeypatch) -> None:
    condition_entity = "binary_sensor.main_floor_vacuum_coordinator_native_resume_pending"
    config = observer_mapping(
        "main_floor",
        conditions=[
            {
                "entity_id": condition_entity,
                "code": "vacuum.task_resume_pending",
                "source": "coordinator",
                "active_states": ["on"],
            }
        ],
    )
    harness = _Harness(monkeypatch, config=config)
    harness.set_healthy()
    harness.hass.states.set(harness.config.status_flag_entity, "resumable")
    harness.hass.states.set(condition_entity, "on")
    run(harness.setup())

    assert harness.observer.contract["active_conditions"] == [
        {
            "code": "vacuum.task_resume_pending",
            "source": "coordinator",
            "since": harness.clock.now.isoformat(),
        }
    ]


def test_missing_optional_condition_source_restricts_commands(monkeypatch) -> None:
    config = observer_mapping(
        "main_floor",
        conditions=[
            {
                "entity_id": "binary_sensor.missing_condition",
                "code": "vacuum.task_resume_pending",
                "source": "coordinator",
            }
        ],
    )
    harness = _Harness(monkeypatch, config=config)
    harness.set_healthy()
    harness.hass.states.remove("binary_sensor.missing_condition")
    run(harness.setup())
    assert harness.observer.contract["active_conditions"] == []
    assert harness.observer.contract["command_policy"] == {
        "mode": "restricted",
        "reason": "source_unreadable",
    }


def test_optional_music_fallback_condition_is_source_labelled(monkeypatch) -> None:
    condition_entity = "input_boolean.music_room_vacuum_fallback_active"
    config = observer_mapping(
        "music_room",
        conditions=[
            {
                "entity_id": condition_entity,
                "code": "vacuum.scheduled_vacuum_only_fallback",
                "source": "automation",
            }
        ],
    )
    harness = _Harness(monkeypatch, config=config)
    harness.set_healthy("cleaning")
    harness.hass.states.set(condition_entity, "on")
    run(harness.setup())

    assert harness.observer.contract["active_conditions"] == [
        {
            "code": "vacuum.scheduled_vacuum_only_fallback",
            "source": "automation",
            "since": harness.clock.now.isoformat(),
        }
    ]


@pytest.mark.parametrize(
    ("vacuum_state", "dock_state", "expected"),
    [
        ("idle", "idle", {"mode": "normal", "reason": None}),
        ("docked", "idle", {"mode": "normal", "reason": None}),
        ("cleaning", "idle", {"mode": "restricted", "reason": "vacuum_cleaning"}),
        ("paused", "idle", {"mode": "restricted", "reason": "vacuum_paused"}),
        ("returning", "idle", {"mode": "restricted", "reason": "vacuum_returning"}),
        ("docked", "drying", {"mode": "restricted", "reason": "dock_drying"}),
    ],
)
def test_command_policy_modes(monkeypatch, vacuum_state, dock_state, expected) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy(vacuum_state)
    harness.hass.states.set(harness.config.dock_status_entity, dock_state)
    run(harness.setup())
    assert harness.observer.contract["command_policy"] == expected


def test_current_issue_restricts_command_policy(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_healthy("idle")
    harness.hass.states.set(harness.config.error_entity, "Robot stuck or trapped")
    run(harness.setup())
    assert harness.observer.contract["current_issue"]["status"] == "present"
    assert harness.observer.contract["command_policy"] == {
        "mode": "restricted",
        "reason": "current_issue",
    }


def test_unavailable_install_does_not_implicitly_backfill_last_issue(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    harness.set_all_unavailable()
    run(harness.setup())
    harness.clock.advance(180)
    run(harness.observer._async_confirm_outage(harness.clock.now))
    assert harness.observer.contract["last_issue"] is None
    assert harness.hass.services.calls == []


def test_typed_recorder_backfill_can_be_loaded_only_as_history(monkeypatch) -> None:
    reported_at = datetime(2026, 8, 21, 21, 20, 11, tzinfo=UTC).isoformat()
    harness = _Harness(
        monkeypatch,
        store=_MemoryStore(
            {
                "last_issue": {
                    "code": "power.critical_battery",
                    "raw": "Unknown error 75",
                    "reported_at": reported_at,
                    "provenance": "recorder_backfill",
                    "cleared_at": None,
                }
            }
        ),
    )
    harness.set_all_unavailable()
    run(harness.setup())

    assert harness.observer.contract["current_issue"] == {"status": "unknown"}
    assert harness.observer.contract["last_issue"] == {
        "code": "power.critical_battery",
        "raw": "Unknown error 75",
        "reported_at": reported_at,
        "provenance": "recorder_backfill",
        "cleared_at": None,
    }
