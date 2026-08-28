"""Read-only Valetudo vacuum status observers."""

from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import logging
import re
from typing import Any, Callable, Mapping

from homeassistant.core import Event, HomeAssistant, State, callback
from homeassistant.helpers.event import async_call_later, async_track_state_change_event
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .const import (
    DEFAULT_STATUS_OUTAGE_CONFIRMATION,
    STATUS_CONDITION_SOURCES,
    STATUS_CONTRACT_VERSION,
    STATUS_DATA_KEY,
    STATUS_ISSUE_CODE_VERSION,
    STATUS_STORE_KEY,
    STATUS_STORE_VERSION,
)

_LOGGER = logging.getLogger(__name__)

_CLEAR_ERROR_STATES = {"", "no error", "none", "ok"}
_UNREADABLE_STATES = {"unknown", "unavailable"}
_NORMAL_COMMAND_STATES = {"docked", "idle"}
_BUSY_DOCK_STATES = {"cleaning", "drying", "emptying", "error", "pause", "paused"}
_SOURCE_PRIORITY = {"derived": 0, "automation": 1, "coordinator": 2}

_KNOWN_ISSUE_CODES = {
    "auto-empty dock cover open or missing dust bag": "dock.cover_open",
    "auto-empty dock dust bag full or dust duct clogged": "dock.dustbag_blocked",
    "cannot navigate to the dock": "navigation.dock_unreachable",
    "cannot reach target": "navigation.target_unreachable",
    "low battery": "power.low_battery",
    "battery low": "power.low_battery",
    "main brush jammed": "maintenance.main_brush_jammed",
    "mop dock clean water tank empty": "mop.clean_water_empty",
    "mop dock wastewater tank not installed or full": "mop.dirty_water_unavailable",
    "robot stuck or trapped": "navigation.stuck",
    "side brush jammed": "maintenance.side_brush_jammed",
    "unknown error 75": "power.critical_battery",
    "unknown error 95": "navigation.ramp_hazard",
    "unknown error 120": "mop.attachment_missing",
}


def _normalized(value: Any) -> str:
    return str(value).strip()


def _normalized_lower(value: Any) -> str:
    return _normalized(value).lower()


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _state_observed_at(state: State | None, fallback: datetime) -> datetime:
    if state is None:
        return fallback
    observed = getattr(state, "last_updated", None) or getattr(state, "last_changed", None)
    if isinstance(observed, datetime):
        if observed.tzinfo is None:
            return observed.replace(tzinfo=UTC)
        return observed.astimezone(UTC)
    return fallback


def _state_changed_at(state: State | None, fallback: datetime) -> datetime:
    if state is None:
        return fallback
    changed = getattr(state, "last_changed", None)
    if isinstance(changed, datetime):
        if changed.tzinfo is None:
            return changed.replace(tzinfo=UTC)
        return changed.astimezone(UTC)
    return _state_observed_at(state, fallback)


def _state_is_readable(state: State | None) -> bool:
    return bool(state and _normalized_lower(state.state) not in _UNREADABLE_STATES)


def issue_code_for_raw(raw: str) -> str:
    """Return a stable semantic code for a raw Valetudo error state."""
    normalized = " ".join(raw.split())
    lowered = normalized.lower()
    known = _KNOWN_ISSUE_CODES.get(lowered)
    if known:
        return known

    unknown_error = re.fullmatch(r"unknown error\s+([0-9]+)", lowered)
    if unknown_error:
        return f"vendor.unknown_error_{unknown_error.group(1)}"

    slug = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")[:48] or "unknown"
    digest = hashlib.sha256(lowered.encode("utf-8")).hexdigest()[:8]
    return f"unmapped.{slug}.{digest}"


@dataclass(frozen=True, slots=True)
class StatusConditionConfig:
    """One typed Home Assistant condition projected into the status contract."""

    entity_id: str
    code: str
    source: str
    active_states: tuple[str, ...] = ("on",)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "StatusConditionConfig":
        """Build and validate a condition configuration."""
        entity_id = _normalized(value.get("entity_id", ""))
        code = _normalized(value.get("code", ""))
        source = _normalized(value.get("source", "automation")).lower()
        raw_active_states = value.get("active_states", ["on"])
        active_states = (
            tuple(_normalized_lower(item) for item in raw_active_states)
            if isinstance(raw_active_states, list)
            else (_normalized_lower(raw_active_states),)
        )
        if not entity_id or not code:
            raise ValueError("Status conditions require entity_id and code")
        if source not in STATUS_CONDITION_SOURCES:
            raise ValueError(f"Unsupported status condition source: {source}")
        if not active_states or any(not item for item in active_states):
            raise ValueError("Status condition active_states cannot be empty")
        return cls(
            entity_id=entity_id,
            code=code,
            source=source,
            active_states=active_states,
        )


@dataclass(frozen=True, slots=True)
class StatusObserverConfig:
    """Configuration for one independent vacuum status observer."""

    observer_id: str
    name: str
    vacuum_entity: str
    error_entity: str
    status_flag_entity: str
    dock_status_entity: str
    battery_entity: str
    conditions: tuple[StatusConditionConfig, ...] = ()
    outage_confirmation_seconds: int = DEFAULT_STATUS_OUTAGE_CONFIRMATION

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "StatusObserverConfig":
        """Build and validate an observer configuration."""
        observer_id = _normalized(value.get("id", ""))
        if not re.fullmatch(r"[a-z0-9_]+", observer_id):
            raise ValueError("Status observer id must contain lowercase letters, numbers, and underscores")

        name = _normalized(value.get("name", "")) or observer_id.replace("_", " ").title()
        required_entities = {
            "vacuum_entity": _normalized(value.get("vacuum_entity", "")),
            "error_entity": _normalized(value.get("error_entity", "")),
            "status_flag_entity": _normalized(value.get("status_flag_entity", "")),
            "dock_status_entity": _normalized(value.get("dock_status_entity", "")),
            "battery_entity": _normalized(value.get("battery_entity", "")),
        }
        missing = [key for key, entity_id in required_entities.items() if not entity_id]
        if missing:
            raise ValueError(f"Status observer is missing required entities: {', '.join(missing)}")

        raw_conditions = value.get("conditions", [])
        if not isinstance(raw_conditions, list):
            raw_conditions = [raw_conditions]
        conditions = tuple(StatusConditionConfig.from_mapping(item) for item in raw_conditions)
        outage_confirmation_seconds = int(
            value.get("outage_confirmation_seconds", DEFAULT_STATUS_OUTAGE_CONFIRMATION)
        )
        if outage_confirmation_seconds <= 0:
            raise ValueError("outage_confirmation_seconds must be positive")

        return cls(
            observer_id=observer_id,
            name=name,
            vacuum_entity=required_entities["vacuum_entity"],
            error_entity=required_entities["error_entity"],
            status_flag_entity=required_entities["status_flag_entity"],
            dock_status_entity=required_entities["dock_status_entity"],
            battery_entity=required_entities["battery_entity"],
            conditions=conditions,
            outage_confirmation_seconds=outage_confirmation_seconds,
        )

    @property
    def watched_entity_ids(self) -> tuple[str, ...]:
        """Return every entity whose state can change the contract."""
        return tuple(
            dict.fromkeys(
                (
                    self.vacuum_entity,
                    self.error_entity,
                    self.status_flag_entity,
                    self.dock_status_entity,
                    self.battery_entity,
                    *(condition.entity_id for condition in self.conditions),
                )
            )
        )


def split_integration_config(raw_config: Any) -> tuple[list[Any], list[Any]]:
    """Split legacy or structured domain configuration without changing legacy behavior."""
    structured = isinstance(raw_config, dict) and (
        "coordinators" in raw_config or "status_observers" in raw_config
    )
    if structured:
        legacy_keys = {
            "vacuum_entity",
            "people",
            "segment_command_topic",
            "rooms",
        }
        mixed_keys = sorted(legacy_keys.intersection(raw_config))
        if mixed_keys:
            raise ValueError(
                "Cannot mix legacy coordinator keys with structured configuration; "
                "move the existing coordinator under coordinators"
            )
        coordinators = raw_config.get("coordinators", [])
        observers = raw_config.get("status_observers", [])
        return (
            coordinators if isinstance(coordinators, list) else [coordinators],
            observers if isinstance(observers, list) else [observers],
        )
    return (
        raw_config if isinstance(raw_config, list) else [raw_config],
        [],
    )


@dataclass(slots=True)
class IssueObservation:
    """One observed raw device issue."""

    code: str
    raw: str
    reported_at: str
    provenance: str = "observed"
    cleared_at: str | None = None

    def to_current_dict(self) -> dict[str, Any]:
        """Return the current-issue contract shape."""
        return {
            "status": "present",
            "code": self.code,
            "raw": self.raw,
            "reported_at": self.reported_at,
        }

    def to_last_dict(self) -> dict[str, Any]:
        """Return the last-issue contract shape."""
        return {
            "code": self.code,
            "raw": self.raw,
            "reported_at": self.reported_at,
            "provenance": self.provenance,
            "cleared_at": self.cleared_at,
        }

    @classmethod
    def from_store(cls, value: Any) -> "IssueObservation | None":
        """Load only a valid persisted historical issue."""
        if not isinstance(value, dict):
            return None
        code = value.get("code")
        raw = value.get("raw")
        reported_at = value.get("reported_at")
        provenance = value.get("provenance")
        cleared_at = value.get("cleared_at")
        if (
            not isinstance(code, str)
            or not code
            or not isinstance(raw, str)
            or not raw
            or _parse_datetime(reported_at) is None
            or provenance not in {"observed", "recorder_backfill"}
            or (cleared_at is not None and _parse_datetime(cleared_at) is None)
        ):
            return None
        return cls(
            code=code,
            raw=raw,
            reported_at=reported_at,
            provenance=provenance,
            cleared_at=cleared_at,
        )


def _latest_issue(
    *issues: IssueObservation | None,
) -> IssueObservation | None:
    latest: IssueObservation | None = None
    latest_at: datetime | None = None
    for issue in issues:
        if issue is None:
            continue
        reported_at = _parse_datetime(issue.reported_at)
        if reported_at is None:
            continue
        if latest_at is None or reported_at >= latest_at:
            latest = issue
            latest_at = reported_at
    return deepcopy(latest)


class ValetudoVacuumStatusObserver:
    """Project multiple HA entities into one source-labelled status contract."""

    def __init__(
        self,
        hass: HomeAssistant,
        config: StatusObserverConfig,
    ) -> None:
        """Initialize a status observer."""
        self.hass = hass
        self.config = config
        self.observer_id = config.observer_id
        self.name = config.name
        self._listeners: list[Callable[[], None]] = []
        self._unsubscribers: list[Callable[[], None]] = []
        self._event_lock = asyncio.Lock()
        self._event_queue: deque[Event] = deque()
        self._event_drain_scheduled = False
        self._store = Store(
            hass,
            STATUS_STORE_VERSION,
            f"{STATUS_STORE_KEY}.{self.observer_id}",
        )
        self._contract = self._empty_contract()
        self._availability_status: str | None = None
        self._availability_epoch = 0
        self._confirmed_unavailable_since: str | None = None
        self._pending_unavailable_since: str | None = None
        self._pending_issue: IssueObservation | None = None
        self._current_issue: IssueObservation | None = None
        self._current_issue_epoch: int | None = None
        self._last_readable_issue_candidate: IssueObservation | None = None
        self._last_issue: IssueObservation | None = None
        self._outage_confirmation_cancel: Callable[[], None] | None = None

    async def async_setup(self) -> None:
        """Load durable history, subscribe to sources, and reconcile current state."""
        await self._async_load_store()
        self._unsubscribers.append(
            async_track_state_change_event(
                self.hass,
                list(self.config.watched_entity_ids),
                self._handle_state_change_event,
            )
        )
        async with self._event_lock:
            await self._async_reconcile(dt_util.utcnow())

    @callback
    def async_add_listener(self, update_callback: Callable[[], None]) -> Callable[[], None]:
        """Subscribe a status sensor to contract updates."""
        self._listeners.append(update_callback)

        def remove_listener() -> None:
            if update_callback in self._listeners:
                self._listeners.remove(update_callback)

        return remove_listener

    @property
    def contract(self) -> dict[str, Any]:
        """Return an isolated copy of the current contract."""
        return deepcopy(self._contract)

    @property
    def native_value(self) -> str:
        """Return a compact state value for the HA sensor."""
        observed = self._contract.get("observed_vacuum_state")
        if isinstance(observed, str) and observed:
            return observed
        return str(self._contract["availability"]["status"])

    @callback
    def _handle_state_change_event(self, event: Event) -> None:
        """Queue every source snapshot, including attribute-only events."""
        self._event_queue.append(event)
        self._schedule_event_drain()

    @callback
    def _schedule_event_drain(self) -> None:
        """Ensure queued events have exactly one active drain task."""
        if self._event_drain_scheduled:
            return
        self._event_drain_scheduled = True
        self.hass.async_create_task(self._async_drain_event_queue())

    async def _async_handle_state_change_event(self, event: Event) -> None:
        """Reconcile after one source event."""
        async with self._event_lock:
            await self._async_process_state_change_event(event)

    async def _async_drain_event_queue(self) -> None:
        """Process queued source snapshots in callback order."""
        try:
            async with self._event_lock:
                while self._event_queue:
                    await self._async_process_state_change_event(
                        self._event_queue.popleft()
                    )
        finally:
            self._finish_event_drain()

    @callback
    def _finish_event_drain(self) -> None:
        """Release the drain flag and reschedule anything queued at completion."""
        self._event_drain_scheduled = False
        if self._event_queue:
            self._schedule_event_drain()

    async def _async_process_state_change_event(self, event: Event) -> None:
        """Retain event evidence before reconciling the latest HA snapshot."""
        new_state: State | None = event.data.get("new_state")
        observed_at = _state_observed_at(new_state, dt_util.utcnow())
        entity_id = event.data.get("entity_id")
        self._observe_readable_issue_event(entity_id, new_state, observed_at)
        await self._async_reconcile(
            observed_at,
            event_entity_id=entity_id,
            event_state=new_state,
        )

    async def _async_reconcile(
        self,
        observed_at: datetime,
        *,
        event_entity_id: str | None = None,
        event_state: State | None = None,
    ) -> None:
        """Rebuild the contract from current HA states."""
        vacuum_state = self._state(self.config.vacuum_entity)
        availability = self._availability_for(vacuum_state)
        previous_availability = self._availability_status
        prior_current_issue = self._current_issue

        if availability == "available" and previous_availability != "available":
            self._availability_epoch += 1
            self._current_issue = None
            self._current_issue_epoch = None
            await self._async_end_outage()
        elif availability != "available" and previous_availability == "available":
            self._pending_issue = _latest_issue(
                prior_current_issue,
                self._last_readable_issue_candidate,
            )
            if availability in {"unavailable", "unknown"}:
                self._begin_pending_outage(
                    _state_changed_at(vacuum_state, observed_at),
                    self._pending_issue,
                )
            self._current_issue = None
            self._current_issue_epoch = None
        elif (
            availability in {"unavailable", "unknown"}
            and previous_availability not in {"unavailable", "unknown"}
        ):
            if self._confirmed_unavailable_since is None:
                self._begin_pending_outage(
                    _state_changed_at(vacuum_state, observed_at),
                    self._pending_issue,
                )

        self._availability_status = availability
        error_state = self._state(self.config.error_entity)
        status_flag_state = self._state(self.config.status_flag_entity)
        dock_state = self._state(self.config.dock_status_entity)
        battery_state = self._state(self.config.battery_entity)
        coherent = self._sources_are_coherent(
            availability,
            error_state,
            status_flag_state,
            dock_state,
            battery_state,
        )

        current_issue_contract = self._current_issue_contract(
            availability,
            error_state,
            coherent,
            observed_at,
            event_entity_id,
            event_state,
        )
        await self._async_mark_last_issue_cleared(
            availability,
            coherent,
            current_issue_contract,
            observed_at,
        )
        active_conditions = self._active_conditions(status_flag_state)
        command_policy = self._command_policy(
            availability,
            coherent,
            vacuum_state,
            dock_state,
            current_issue_contract,
            active_conditions,
        )
        contract = {
            "version": STATUS_CONTRACT_VERSION,
            "issue_code_version": STATUS_ISSUE_CODE_VERSION,
            "vacuum_entity_id": self.config.vacuum_entity,
            "observed_vacuum_state": vacuum_state.state if vacuum_state else None,
            "availability": {
                "status": availability,
                "since": (
                    self._confirmed_unavailable_since
                    if availability != "available"
                    else None
                ),
            },
            "current_issue": current_issue_contract,
            "active_conditions": active_conditions,
            "last_issue": self._last_issue.to_last_dict() if self._last_issue else None,
            "command_policy": command_policy,
        }
        self._publish_contract(contract)

    def _current_issue_contract(
        self,
        availability: str,
        error_state: State | None,
        coherent: bool,
        observed_at: datetime,
        event_entity_id: str | None,
        event_state: State | None,
    ) -> dict[str, Any]:
        if availability != "available" or not _state_is_readable(error_state):
            self._current_issue = None
            self._current_issue_epoch = None
            return {"status": "unknown"}

        raw = _normalized(error_state.state)
        if _normalized_lower(raw) in _CLEAR_ERROR_STATES:
            self._current_issue = None
            self._current_issue_epoch = None
            if coherent:
                self._last_readable_issue_candidate = None
            return {"status": "clear"}

        if (
            self._current_issue is None
            or self._current_issue.raw != raw
            or self._current_issue_epoch != self._availability_epoch
        ):
            event_matches_error = bool(
                event_entity_id == self.config.error_entity
                and event_state is not None
                and _normalized(event_state.state) == raw
            )
            reported_at = (
                observed_at
                if event_entity_id == self.config.vacuum_entity or event_matches_error
                else _state_changed_at(error_state, observed_at)
            )
            self._current_issue = IssueObservation(
                code=issue_code_for_raw(raw),
                raw=raw,
                reported_at=_iso(reported_at) or _iso(observed_at) or "",
            )
            self._current_issue_epoch = self._availability_epoch
        self._last_readable_issue_candidate = _latest_issue(
            self._last_readable_issue_candidate,
            self._current_issue,
        )
        return self._current_issue.to_current_dict()

    def _observe_readable_issue_event(
        self,
        entity_id: str | None,
        new_state: State | None,
        observed_at: datetime,
    ) -> None:
        if entity_id != self.config.error_entity or not _state_is_readable(new_state):
            return
        issue_at = _state_changed_at(new_state, observed_at)
        pending_since = _parse_datetime(self._pending_unavailable_since)
        if self._availability_status != "available" and (
            pending_since is None or issue_at > pending_since
        ):
            return
        raw = _normalized(new_state.state)
        if _normalized_lower(raw) in _CLEAR_ERROR_STATES:
            return
        if self._current_issue is not None and self._current_issue.raw == raw:
            return
        candidate = IssueObservation(
            code=issue_code_for_raw(raw),
            raw=raw,
            reported_at=_iso(issue_at)
            or _iso(observed_at)
            or "",
        )
        self._last_readable_issue_candidate = _latest_issue(
            self._last_readable_issue_candidate,
            candidate,
        )
        if pending_since is not None and issue_at <= pending_since:
            self._pending_issue = _latest_issue(
                self._pending_issue,
                candidate,
            )

    def _active_conditions(self, status_flag_state: State | None) -> list[dict[str, str]]:
        conditions: dict[str, dict[str, str]] = {}
        if _state_is_readable(status_flag_state) and _normalized_lower(status_flag_state.state) == "resumable":
            conditions["vacuum.task_resume_pending"] = {
                "code": "vacuum.task_resume_pending",
                "source": "derived",
                "since": _iso(_state_changed_at(status_flag_state, dt_util.utcnow())) or "",
            }

        for condition in self.config.conditions:
            state = self._state(condition.entity_id)
            if (
                not _state_is_readable(state)
                or _normalized_lower(state.state) not in condition.active_states
            ):
                continue
            candidate = {
                "code": condition.code,
                "source": condition.source,
                "since": _iso(_state_changed_at(state, dt_util.utcnow())) or "",
            }
            existing = conditions.get(condition.code)
            if existing is None or _SOURCE_PRIORITY[condition.source] > _SOURCE_PRIORITY[existing["source"]]:
                conditions[condition.code] = candidate
        return [conditions[code] for code in sorted(conditions)]

    def _command_policy(
        self,
        availability: str,
        coherent: bool,
        vacuum_state: State | None,
        dock_state: State | None,
        current_issue: dict[str, Any],
        active_conditions: list[dict[str, str]],
    ) -> dict[str, str | None]:
        if availability != "available":
            return {"mode": "none", "reason": f"primary_{availability}"}
        if not coherent:
            return {"mode": "restricted", "reason": "source_unreadable"}
        if current_issue["status"] == "present":
            return {"mode": "restricted", "reason": "current_issue"}
        if active_conditions:
            return {"mode": "restricted", "reason": active_conditions[0]["code"]}

        vacuum_value = _normalized_lower(vacuum_state.state if vacuum_state else "")
        if vacuum_value not in _NORMAL_COMMAND_STATES:
            return {
                "mode": "restricted",
                "reason": f"vacuum_{vacuum_value or 'unknown'}",
            }
        dock_value = _normalized_lower(dock_state.state if dock_state else "")
        if dock_value in _BUSY_DOCK_STATES:
            return {"mode": "restricted", "reason": f"dock_{dock_value}"}
        return {"mode": "normal", "reason": None}

    def _sources_are_coherent(
        self,
        availability: str,
        error_state: State | None,
        status_flag_state: State | None,
        dock_state: State | None,
        battery_state: State | None,
    ) -> bool:
        if availability != "available":
            return False
        if not all(
            _state_is_readable(state)
            for state in (error_state, status_flag_state, dock_state, battery_state)
        ):
            return False
        try:
            float(battery_state.state)
        except (TypeError, ValueError):
            return False
        return all(
            _state_is_readable(self._state(condition.entity_id))
            for condition in self.config.conditions
        )

    def _begin_pending_outage(
        self,
        unavailable_at: datetime,
        prior_issue: IssueObservation | None,
    ) -> None:
        if self._confirmed_unavailable_since is not None:
            return
        if self._pending_unavailable_since is None:
            self._pending_unavailable_since = _iso(unavailable_at)
        self._pending_issue = _latest_issue(self._pending_issue, prior_issue)
        self._schedule_outage_confirmation(dt_util.utcnow())

    def _schedule_outage_confirmation(self, now: datetime) -> None:
        if self._outage_confirmation_cancel is not None:
            self._outage_confirmation_cancel()
        pending_since = _parse_datetime(self._pending_unavailable_since)
        if pending_since is None:
            return
        elapsed = max(0.0, (now.astimezone(UTC) - pending_since).total_seconds())
        remaining = max(0.0, self.config.outage_confirmation_seconds - elapsed)
        self._outage_confirmation_cancel = async_call_later(
            self.hass,
            remaining,
            self._handle_outage_confirmation,
        )

    @callback
    def _handle_outage_confirmation(self, now: datetime) -> None:
        self._outage_confirmation_cancel = None
        self.hass.async_create_task(self._async_confirm_outage(now))

    async def _async_confirm_outage(self, now: datetime) -> None:
        """Persist a nonavailable epoch only after its confirmation interval."""
        async with self._event_lock:
            pending_since = _parse_datetime(self._pending_unavailable_since)
            if (
                pending_since is None
                or self._availability_for(self._state(self.config.vacuum_entity)) == "available"
            ):
                return
            elapsed = max(0.0, (now.astimezone(UTC) - pending_since).total_seconds())
            if elapsed < self.config.outage_confirmation_seconds:
                self._schedule_outage_confirmation(now)
                return
            self._confirmed_unavailable_since = _iso(pending_since)
            if self._pending_issue is not None:
                self._last_issue = deepcopy(self._pending_issue)
                self._last_issue.cleared_at = None
            self._pending_issue = None
            await self._async_save_store()
            await self._async_reconcile(now)

    async def _async_end_outage(self) -> None:
        durable_changed = self._confirmed_unavailable_since is not None
        self._cancel_outage_confirmation()
        self._pending_unavailable_since = None
        self._pending_issue = None
        self._confirmed_unavailable_since = None

        if durable_changed:
            await self._async_save_store()

    async def _async_mark_last_issue_cleared(
        self,
        availability: str,
        coherent: bool,
        current_issue: Mapping[str, Any],
        observed_at: datetime,
    ) -> None:
        if (
            self._last_issue is not None
            and self._last_issue.cleared_at is None
            and availability == "available"
            and coherent
            and current_issue.get("status") == "clear"
        ):
            self._last_issue.cleared_at = _iso(observed_at)
            self._last_readable_issue_candidate = None
            await self._async_save_store()

    def _cancel_outage_confirmation(self) -> None:
        if self._outage_confirmation_cancel is not None:
            self._outage_confirmation_cancel()
            self._outage_confirmation_cancel = None

    async def _async_load_store(self) -> None:
        try:
            stored = await self._store.async_load()
        except Exception:
            _LOGGER.exception("Unable to load vacuum status observer store for %s", self.observer_id)
            return
        if not isinstance(stored, dict):
            return

        availability_since = stored.get("availability_since")
        if _parse_datetime(availability_since) is not None:
            self._confirmed_unavailable_since = availability_since
        self._last_issue = IssueObservation.from_store(stored.get("last_issue"))

    async def _async_save_store(self) -> None:
        await self._store.async_save(
            {
                "availability_since": self._confirmed_unavailable_since,
                "last_issue": self._last_issue.to_last_dict() if self._last_issue else None,
            }
        )

    def _publish_contract(self, contract: dict[str, Any]) -> None:
        if contract == self._contract:
            return
        self._contract = contract
        for update_callback in list(self._listeners):
            update_callback()

    def _state(self, entity_id: str) -> State | None:
        return self.hass.states.get(entity_id)

    @staticmethod
    def _availability_for(vacuum_state: State | None) -> str:
        if vacuum_state is None:
            return "missing"
        normalized = _normalized_lower(vacuum_state.state)
        if normalized == "unavailable":
            return "unavailable"
        if normalized == "unknown" or normalized == "":
            return "unknown"
        return "available"

    def _empty_contract(self) -> dict[str, Any]:
        return {
            "version": STATUS_CONTRACT_VERSION,
            "issue_code_version": STATUS_ISSUE_CODE_VERSION,
            "vacuum_entity_id": self.config.vacuum_entity,
            "observed_vacuum_state": None,
            "availability": {"status": "missing", "since": None},
            "current_issue": {"status": "unknown"},
            "active_conditions": [],
            "last_issue": None,
            "command_policy": {"mode": "none", "reason": "primary_missing"},
        }


def get_status_observer(
    hass_data: Mapping[str, Any],
    observer_id: str,
) -> ValetudoVacuumStatusObserver:
    """Resolve one configured status observer."""
    observers = hass_data.get(STATUS_DATA_KEY, {})
    if not isinstance(observers, Mapping) or observer_id not in observers:
        raise ValueError(f"Unknown status observer: {observer_id}")
    observer = observers[observer_id]
    if not isinstance(observer, ValetudoVacuumStatusObserver):
        raise ValueError(f"Invalid status observer: {observer_id}")
    return observer
