"""Pure scheduling and success logic for Valetudo Vacuum Coordinator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

DOCK_COMPONENT_BAD_VALUES = {"empty", "full", "missing", "unknown", "unavailable"}
NO_ERROR_VALUES = {None, "", "No error", "no error", "none", "unknown", "unavailable"}

MOP_RESOURCE_ERROR_KEYWORDS = (
    "clean water",
    "fresh water",
    "freshwater",
    "water tank empty",
    "water tank missing",
    "wastewater",
    "dirty tank",
    "dirty water",
    "detergent",
    "cleaning liquid",
    "fortified liquid",
    "mop dock tray full",
)

# Dreame X40 Ultra firmware error 120 is in the mop-dock/self-wash-base cluster,
# but neither Valetudo nor the dreame-vacuum integration map it, so it surfaces as "Unknown error 120".
MOP_HARDWARE_ERROR_KEYWORDS = (
    "unknown error 120",
)

RECOVERABLE_MOP_ERROR_KEYWORDS = MOP_RESOURCE_ERROR_KEYWORDS + MOP_HARDWARE_ERROR_KEYWORDS

RECOVERABLE_NAVIGATION_FAILURE_KEYWORDS = (
    "cannot reach",
    "cannot arrive",
    "cannot navigate",
    "unknown error 95",
    "easy-to-fall",
    "fall hazard",
    "robot_stuck_on_ramp",
    "stuck on ramp",
)

LOW_BATTERY_ERROR_KEYWORDS = (
    "low battery",
    "battery low",
)

CLEAN_WATER_EMPTY_ERROR_VALUES = {
    "mop dock clean water tank empty",
    "clean water tank empty",
    "fresh water is empty",
    "fresh water tank empty",
    "freshwater tank empty",
}
CLEAN_WATER_EMPTY_DISPOSITION = "__clean_water_empty__"

RUN_PHASE_DISPATCHING = "dispatching"
RUN_PHASE_CLEANING = "cleaning"
RUN_PHASE_DOCK_INTERRUPT = "dock_interrupt"
RUN_PHASE_SUSPENDED = "suspended"
RUN_PHASE_RESUMED_CLEANING = "resumed_cleaning"
RUN_PHASE_CANCEL_PENDING = "cancel_pending"
RUN_PHASE_RECOVERY_STALLED = "recovery_stalled"

RUN_PHASES = {
    RUN_PHASE_DISPATCHING,
    RUN_PHASE_CLEANING,
    RUN_PHASE_DOCK_INTERRUPT,
    RUN_PHASE_SUSPENDED,
    RUN_PHASE_RESUMED_CLEANING,
    RUN_PHASE_CANCEL_PENDING,
    RUN_PHASE_RECOVERY_STALLED,
}
NATIVE_RESUME_PENDING_PHASES = {
    RUN_PHASE_DOCK_INTERRUPT,
    RUN_PHASE_SUSPENDED,
    RUN_PHASE_CANCEL_PENDING,
    RUN_PHASE_RECOVERY_STALLED,
}

WRONG_ROOM_FAILURE_PREFIX = "Estimated segment dwell was dominated by"


def schedule_hass_task(hass: Any, coroutine: Any) -> None:
    """Schedule a coroutine from either the event loop or a timer thread."""
    create_task = getattr(hass, "create_task", None)
    if create_task is not None:
        create_task(coroutine)
        return
    hass.async_create_task(coroutine)


@dataclass(slots=True)
class RoomConfig:
    """Configuration for one Valetudo map segment."""

    room_id: str
    name: str
    segment_id: str
    mop_required: bool = False
    enabled: bool = True
    min_duration: int = 120
    min_area: float = 0.0
    min_estimated_dwell: int = 30
    require_estimated_segment: bool = False
    manual_credit_entity: str | None = None


@dataclass(slots=True)
class RoomLedger:
    """Persisted cleaning history for one room."""

    last_successful_clean: str | None = None
    last_vacuumed: str | None = None
    last_fallback_vacuumed: str | None = None
    last_mopped: str | None = None
    last_auto_cleaned: str | None = None
    last_auto_cleaned_day: str | None = None
    last_attempted: str | None = None
    last_failed_reason: str | None = None
    successful_count: int = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "RoomLedger":
        """Build a ledger from stored JSON."""
        if not isinstance(data, dict):
            return cls()

        return cls(
            last_successful_clean=data.get("last_successful_clean"),
            last_vacuumed=data.get("last_vacuumed"),
            last_fallback_vacuumed=data.get("last_fallback_vacuumed"),
            last_mopped=data.get("last_mopped"),
            last_auto_cleaned=data.get("last_auto_cleaned"),
            last_auto_cleaned_day=data.get("last_auto_cleaned_day"),
            last_attempted=data.get("last_attempted"),
            last_failed_reason=data.get("last_failed_reason"),
            successful_count=int(data.get("successful_count", 0) or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the ledger to JSON-safe data."""
        return {
            "last_successful_clean": self.last_successful_clean,
            "last_vacuumed": self.last_vacuumed,
            "last_fallback_vacuumed": self.last_fallback_vacuumed,
            "last_mopped": self.last_mopped,
            "last_auto_cleaned": self.last_auto_cleaned,
            "last_auto_cleaned_day": self.last_auto_cleaned_day,
            "last_attempted": self.last_attempted,
            "last_failed_reason": self.last_failed_reason,
            "successful_count": self.successful_count,
        }


@dataclass(slots=True)
class ResourceState:
    """Resource and error state used to decide if a room can be cleaned."""

    error: str | None = None
    fresh_water: str | None = None
    dirty_water: str | None = None
    detergent: str | None = None
    dustbag: str | None = None
    mop_attached: bool | None = None


@dataclass(slots=True)
class RoomSelection:
    """Result of selecting a room to clean."""

    room: RoomConfig
    vacuum_only: bool = False
    mop_block_reason: str | None = None
    fallback_vacuum: bool = False


@dataclass(slots=True)
class ActiveRun:
    """State tracked while a room or manual run is active."""

    room_id: str | None
    segment_id: str | None
    session_id: str | None
    started_at: str
    start_area: float | None = None
    start_time: float | None = None
    manual: bool = False
    vacuum_only: bool = False
    fallback_vacuum: bool = False
    allowed_error_fingerprint: str | None = None
    cancelled: bool = False
    command_published: bool = False
    phase: str = RUN_PHASE_DISPATCHING
    observed_cleaning: bool = False
    observed_segment_cleaning: bool = False
    suspended_at: str | None = None
    suspend_reason: str | None = None
    resumable_latched: bool = False
    resumed_after_suspend: bool = False
    resume_source: str | None = None
    docked_at: str | None = None
    interruption_count: int = 0
    requested_iterations: int = 2
    dispatch_deadline: str | None = None
    recovery_deadline: str | None = None
    resume_required: bool = False
    post_suspend_cleaning_observed: bool = False
    post_suspend_segment_observed: bool = False
    accumulated_area: float = 0.0
    accumulated_time: float = 0.0
    last_area: float | None = None
    last_time: float | None = None
    cancel_requested_at: str | None = None
    cancel_reason: str | None = None
    cancel_stop_attempted: bool = False
    cancel_continue_session: bool = False
    last_estimated_room_id: str | None = None
    last_estimated_changed_at: str | None = None
    estimated_dwell_seconds: dict[str, float] = field(default_factory=dict)
    manual_credit_room_ids: list[str] | None = None

    @property
    def native_resume_pending(self) -> bool:
        """Return whether the run is waiting on retained native-task recovery."""
        return self.phase in NATIVE_RESUME_PENDING_PHASES

    def checkpoint_statistics(
        self,
        current_area: float | None,
        current_time: float | None,
    ) -> None:
        """Accumulate counters before a dock interruption can reset them."""
        area_baseline = self.last_area if self.last_area is not None else self.start_area
        if area_baseline is not None and current_area is not None:
            self.accumulated_area += counter_delta(area_baseline, current_area)
            self.last_area = current_area
        elif current_area is not None:
            self.last_area = current_area

        time_baseline = self.last_time if self.last_time is not None else self.start_time
        if time_baseline is not None and current_time is not None:
            self.accumulated_time += counter_delta(time_baseline, current_time)
            self.last_time = current_time
        elif current_time is not None:
            self.last_time = current_time

    def total_area(self, end_area: float | None) -> float | None:
        """Return accumulated cleaned area across counter resets."""
        baseline = self.last_area if self.last_area is not None else self.start_area
        if baseline is None or end_area is None:
            return None
        return self.accumulated_area + counter_delta(baseline, end_area)

    def total_time(self, end_time: float | None) -> float | None:
        """Return accumulated cleaning time across counter resets."""
        baseline = self.last_time if self.last_time is not None else self.start_time
        if baseline is None or end_time is None:
            return None
        return self.accumulated_time + counter_delta(baseline, end_time)

    def observe_estimated_room(self, room_id: str | None, observed_at: datetime) -> None:
        """Accumulate dwell time for estimated room updates."""
        if self.last_estimated_room_id and self.last_estimated_changed_at:
            previous_time = parse_datetime(self.last_estimated_changed_at)
            if previous_time is not None:
                dwell = max(0.0, (observed_at - previous_time).total_seconds())
                current = self.estimated_dwell_seconds.get(self.last_estimated_room_id, 0.0)
                self.estimated_dwell_seconds[self.last_estimated_room_id] = current + dwell

        self.last_estimated_room_id = room_id
        self.last_estimated_changed_at = observed_at.isoformat()

    def finalize_estimated_room(self, observed_at: datetime) -> None:
        """Flush the current estimated-room dwell counter."""
        self.observe_estimated_room(self.last_estimated_room_id, observed_at)
        self.last_estimated_room_id = None
        self.last_estimated_changed_at = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ActiveRun | None":
        """Build an active run from stored JSON."""
        if not isinstance(data, dict):
            return None
        command_published = bool(data.get("command_published", True))
        observed_cleaning = bool(data.get("observed_cleaning", False))
        phase = data.get("phase")
        if phase not in RUN_PHASES:
            if bool(data.get("cancelled", False)):
                phase = RUN_PHASE_CANCEL_PENDING
            elif observed_cleaning:
                phase = RUN_PHASE_CLEANING
            else:
                phase = RUN_PHASE_DISPATCHING
        start_area = parse_float(data.get("start_area"))
        start_time = parse_float(data.get("start_time"))
        return cls(
            room_id=data.get("room_id"),
            segment_id=data.get("segment_id"),
            session_id=data.get("session_id"),
            started_at=data.get("started_at") or utcnow_iso(),
            start_area=start_area,
            start_time=start_time,
            manual=bool(data.get("manual", False)),
            vacuum_only=bool(data.get("vacuum_only", False)),
            fallback_vacuum=bool(data.get("fallback_vacuum", False)),
            allowed_error_fingerprint=data.get("allowed_error_fingerprint"),
            cancelled=bool(data.get("cancelled", False)),
            command_published=command_published,
            phase=phase,
            observed_cleaning=observed_cleaning,
            observed_segment_cleaning=bool(data.get("observed_segment_cleaning", False)),
            suspended_at=data.get("suspended_at"),
            suspend_reason=data.get("suspend_reason"),
            resumable_latched=bool(data.get("resumable_latched", False)),
            resumed_after_suspend=bool(data.get("resumed_after_suspend", False)),
            resume_source=data.get("resume_source"),
            docked_at=data.get("docked_at"),
            interruption_count=int(parse_float(data.get("interruption_count")) or 0),
            requested_iterations=max(
                1, int(parse_float(data.get("requested_iterations")) or 2)
            ),
            dispatch_deadline=data.get("dispatch_deadline"),
            recovery_deadline=data.get("recovery_deadline"),
            resume_required=bool(data.get("resume_required", False)),
            post_suspend_cleaning_observed=bool(
                data.get("post_suspend_cleaning_observed", False)
            ),
            post_suspend_segment_observed=bool(
                data.get("post_suspend_segment_observed", False)
            ),
            accumulated_area=parse_float(data.get("accumulated_area")) or 0.0,
            accumulated_time=parse_float(data.get("accumulated_time")) or 0.0,
            last_area=parse_float(data.get("last_area", start_area)),
            last_time=parse_float(data.get("last_time", start_time)),
            cancel_requested_at=data.get("cancel_requested_at"),
            cancel_reason=data.get("cancel_reason"),
            cancel_stop_attempted=bool(data.get("cancel_stop_attempted", False)),
            cancel_continue_session=bool(
                data.get("cancel_continue_session", False)
            ),
            last_estimated_room_id=data.get("last_estimated_room_id"),
            last_estimated_changed_at=data.get("last_estimated_changed_at"),
            estimated_dwell_seconds={
                str(room_id): float(seconds)
                for room_id, seconds in (data.get("estimated_dwell_seconds") or {}).items()
            },
            manual_credit_room_ids=(
                [str(room_id) for room_id in data["manual_credit_room_ids"]]
                if isinstance(data.get("manual_credit_room_ids"), list)
                else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize active run state to JSON-safe data."""
        return {
            "room_id": self.room_id,
            "segment_id": self.segment_id,
            "session_id": self.session_id,
            "started_at": self.started_at,
            "start_area": self.start_area,
            "start_time": self.start_time,
            "manual": self.manual,
            "vacuum_only": self.vacuum_only,
            "fallback_vacuum": self.fallback_vacuum,
            "allowed_error_fingerprint": self.allowed_error_fingerprint,
            "cancelled": self.cancelled,
            "command_published": self.command_published,
            "phase": self.phase,
            "observed_cleaning": self.observed_cleaning,
            "observed_segment_cleaning": self.observed_segment_cleaning,
            "suspended_at": self.suspended_at,
            "suspend_reason": self.suspend_reason,
            "resumable_latched": self.resumable_latched,
            "resumed_after_suspend": self.resumed_after_suspend,
            "resume_source": self.resume_source,
            "docked_at": self.docked_at,
            "interruption_count": self.interruption_count,
            "requested_iterations": self.requested_iterations,
            "dispatch_deadline": self.dispatch_deadline,
            "recovery_deadline": self.recovery_deadline,
            "resume_required": self.resume_required,
            "post_suspend_cleaning_observed": self.post_suspend_cleaning_observed,
            "post_suspend_segment_observed": self.post_suspend_segment_observed,
            "accumulated_area": self.accumulated_area,
            "accumulated_time": self.accumulated_time,
            "last_area": self.last_area,
            "last_time": self.last_time,
            "cancel_requested_at": self.cancel_requested_at,
            "cancel_reason": self.cancel_reason,
            "cancel_stop_attempted": self.cancel_stop_attempted,
            "cancel_continue_session": self.cancel_continue_session,
            "last_estimated_room_id": self.last_estimated_room_id,
            "last_estimated_changed_at": self.last_estimated_changed_at,
            "estimated_dwell_seconds": self.estimated_dwell_seconds,
            "manual_credit_room_ids": self.manual_credit_room_ids,
        }


@dataclass(slots=True)
class AutoCleanSettingsSnapshot:
    """User cleaning settings captured before an auto-clean session mutates them."""

    mode: str | None = None
    fan: str | None = None
    water: str | None = None
    passes: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "AutoCleanSettingsSnapshot | None":
        """Build a settings snapshot from stored JSON."""
        if not isinstance(data, dict):
            return None
        return cls(
            mode=data.get("mode"),
            fan=data.get("fan"),
            water=data.get("water"),
            passes=data.get("passes"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize settings snapshot state to JSON-safe data."""
        return {
            "mode": self.mode,
            "fan": self.fan,
            "water": self.water,
            "passes": self.passes,
        }


@dataclass(slots=True)
class WhileAwayOutcome:
    """One retained while-away outcome for dashboard display."""

    day: str
    room_id: str
    kind: str
    reason: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "WhileAwayOutcome | None":
        """Build an outcome from stored JSON."""
        if not isinstance(data, dict):
            return None
        day = normalize_state(data.get("day"))
        room_id = normalize_state(data.get("room_id"))
        kind = normalize_state(data.get("kind"))
        if not day or not room_id or kind not in {"cleaned", "skipped", "failed", "fallback"}:
            return None
        return cls(day=day, room_id=room_id, kind=kind, reason=data.get("reason"))

    def to_dict(self) -> dict[str, Any]:
        """Serialize outcome state to JSON-safe data."""
        return {
            "day": self.day,
            "room_id": self.room_id,
            "kind": self.kind,
            "reason": self.reason,
        }


def build_while_away_messages(
    outcomes: list[WhileAwayOutcome],
    room_names_by_id: dict[str, str],
    day: str,
) -> tuple[list[str], list[str]]:
    """Build dashboard while-away messages for one local day."""
    cleaned_room_names: list[str] = []
    skipped_reasons: dict[str, str] = {}
    failed_reasons: dict[str, str] = {}
    fallback_reasons: dict[str, str] = {}

    for outcome in outcomes:
        if outcome.day != day:
            continue
        room_name = room_names_by_id.get(outcome.room_id)
        if not room_name:
            continue
        if outcome.kind == "cleaned":
            if room_name not in cleaned_room_names:
                cleaned_room_names.append(room_name)
            skipped_reasons.pop(room_name, None)
            failed_reasons.pop(room_name, None)
            fallback_reasons.pop(room_name, None)
        elif outcome.kind == "skipped":
            if room_name not in cleaned_room_names:
                skipped_reasons[room_name] = outcome.reason or "Unknown issue"
        elif outcome.kind == "failed":
            if room_name not in cleaned_room_names:
                failed_reasons[room_name] = outcome.reason or "Unknown failure"
        elif outcome.kind == "fallback":
            if room_name not in cleaned_room_names:
                failed_reasons.pop(room_name, None)
                skipped_reasons.pop(room_name, None)
                fallback_reasons[room_name] = outcome.reason or "Mopping remains due"

    issues = build_issue_messages(skipped_reasons, failed_reasons)
    issues.extend(
        f"Vacuumed {room_name}; mopping remains due because "
        f"{friendly_failure_reason(reason)}"
        for room_name, reason in fallback_reasons.items()
    )
    return build_cleaned_messages(cleaned_room_names), issues


@dataclass(slots=True)
class SessionState:
    """State for one away-session cleaning cycle."""

    session_id: str
    started_at: str
    active: bool = True
    cancelled: bool = False
    attempted_room_ids: list[str] = field(default_factory=list)
    completed_room_ids: list[str] = field(default_factory=list)
    skipped_room_ids: list[str] = field(default_factory=list)
    failed_room_ids: list[str] = field(default_factory=list)
    skipped_room_reasons: dict[str, str] = field(default_factory=dict)
    failed_room_reasons: dict[str, str] = field(default_factory=dict)
    fallback_attempted_room_ids: list[str] = field(default_factory=list)
    fallback_completed_room_ids: list[str] = field(default_factory=list)
    fallback_failed_room_ids: list[str] = field(default_factory=list)
    fallback_failed_room_reasons: dict[str, str] = field(default_factory=dict)
    deferred_full_clean_room_ids: list[str] = field(default_factory=list)
    deferred_full_clean_reasons: dict[str, str] = field(default_factory=dict)
    retry_room_ids: list[str] = field(default_factory=list)
    priority_retry_room_ids: list[str] = field(default_factory=list)
    retried_room_ids: list[str] = field(default_factory=list)
    pending_recovery_room_id: str | None = None
    pending_recovery_reason: str | None = None
    pending_recovery_priority: bool = False
    active_room_id: str | None = None
    terminal_reason: str | None = None
    terminal_message: str | None = None
    terminal_cause: str | None = None
    needs_help: bool = False
    notification_sent: bool = False
    degraded_reason: str | None = None
    degraded_at: str | None = None
    degraded_preparation_attempted: bool = False
    degraded_preparation_completed: bool = False
    blocked_deadline: str | None = None
    blocked_reason: str | None = None
    native_resume_guard_latched: bool = False
    native_guard_cancel_pending: bool = False
    native_guard_stop_confirmed: bool = False
    native_guard_return_confirmed: bool = False
    native_guard_cancel_reason: str | None = None

    def mark_attempted(self, room_id: str) -> None:
        """Record that a room has consumed its one attempt for this session."""
        if room_id not in self.attempted_room_ids:
            self.attempted_room_ids.append(room_id)

    def mark_completed(self, room_id: str) -> None:
        """Record a completed room for this session."""
        self.mark_attempted(room_id)
        if room_id not in self.completed_room_ids:
            self.completed_room_ids.append(room_id)
        self.clear_deferred_full_clean(room_id)
        self._remove_room_issue(room_id)
        self.active_room_id = None

    def mark_fallback_attempted(self, room_id: str) -> None:
        """Record that a room consumed its one fallback-vacuum token."""
        if room_id not in self.fallback_attempted_room_ids:
            self.fallback_attempted_room_ids.append(room_id)

    def mark_fallback_completed(self, room_id: str) -> None:
        """Record partial vacuum-only success without normal completion credit."""
        self.mark_fallback_attempted(room_id)
        if room_id not in self.fallback_completed_room_ids:
            self.fallback_completed_room_ids.append(room_id)
        if room_id in self.fallback_failed_room_ids:
            self.fallback_failed_room_ids.remove(room_id)
        self.fallback_failed_room_reasons.pop(room_id, None)
        self.active_room_id = None

    def mark_fallback_failed(self, room_id: str, reason: str | None = None) -> None:
        """Record a consumed fallback token that did not finish successfully."""
        self.mark_fallback_attempted(room_id)
        if room_id not in self.fallback_failed_room_ids:
            self.fallback_failed_room_ids.append(room_id)
        if reason:
            self.fallback_failed_room_reasons[room_id] = reason
        self.active_room_id = None

    def defer_full_clean(self, room_id: str, reason: str) -> None:
        """Record that a room still requires its configured full cleaning mode."""
        if room_id not in self.deferred_full_clean_room_ids:
            self.deferred_full_clean_room_ids.append(room_id)
        self.deferred_full_clean_reasons[room_id] = reason

    def clear_deferred_full_clean(self, room_id: str) -> None:
        """Clear a deferred full-clean obligation after normal completion."""
        if room_id in self.deferred_full_clean_room_ids:
            self.deferred_full_clean_room_ids.remove(room_id)
        self.deferred_full_clean_reasons.pop(room_id, None)

    def mark_skipped(self, room_id: str, reason: str | None = None) -> None:
        """Record a skipped room for this session."""
        self.mark_attempted(room_id)
        if room_id not in self.skipped_room_ids:
            self.skipped_room_ids.append(room_id)
        if reason:
            self.skipped_room_reasons[room_id] = reason

    def mark_failed(self, room_id: str, reason: str | None = None) -> None:
        """Record a failed room for this session."""
        self.mark_attempted(room_id)
        if room_id not in self.failed_room_ids:
            self.failed_room_ids.append(room_id)
        if reason:
            self.failed_room_reasons[room_id] = reason

    def begin_recovering(
        self,
        room_id: str,
        reason: str | None,
        *,
        priority: bool = False,
    ) -> None:
        """Track a failed room that can be retried after the robot docks cleanly."""
        self.pending_recovery_room_id = room_id
        self.pending_recovery_reason = reason
        self.pending_recovery_priority = priority

    def can_retry_room(self, room_id: str) -> bool:
        """Return whether the session can retry a recoverable room failure."""
        return (
            room_id not in self.retried_room_ids
            and room_id not in self.retry_room_ids
            and room_id not in self.priority_retry_room_ids
        )

    def queue_retry(self, room_id: str, *, priority: bool = False) -> None:
        """Queue a room for one bounded retry."""
        if room_id in self.retried_room_ids:
            return
        if priority:
            if room_id in self.retry_room_ids:
                self.retry_room_ids.remove(room_id)
            if room_id not in self.priority_retry_room_ids:
                self.priority_retry_room_ids.append(room_id)
            return
        if room_id not in self.retry_room_ids and room_id not in self.priority_retry_room_ids:
            self.retry_room_ids.append(room_id)

    def mark_retry_started(self, room_id: str) -> None:
        """Record that a queued retry has started so it is not retried forever."""
        self.discard_retry(room_id)
        if room_id not in self.retried_room_ids:
            self.retried_room_ids.append(room_id)

    def discard_retry(self, room_id: str) -> None:
        """Remove a queued retry without recording that it started."""
        if room_id in self.retry_room_ids:
            self.retry_room_ids.remove(room_id)
        if room_id in self.priority_retry_room_ids:
            self.priority_retry_room_ids.remove(room_id)

    def resolve_recoverable_failure(self, room_id: str, *, queue_retry: bool = True) -> None:
        """Clear recovery waiting state and optionally queue the room for retry."""
        priority = (
            self.pending_recovery_priority
            if self.pending_recovery_room_id == room_id
            else False
        )
        if queue_retry:
            self.queue_retry(room_id, priority=priority)
        if self.pending_recovery_room_id == room_id:
            self.pending_recovery_room_id = None
            self.pending_recovery_reason = None
            self.pending_recovery_priority = False

    def clear_room_issue(self, room_id: str) -> None:
        """Clear failure and skip bookkeeping after a retry is published."""
        self._remove_room_issue(room_id)

    def _remove_room_issue(self, room_id: str) -> None:
        """Remove failed/skipped issue bookkeeping for a room."""
        if room_id in self.failed_room_ids:
            self.failed_room_ids.remove(room_id)
        if room_id in self.skipped_room_ids:
            self.skipped_room_ids.remove(room_id)
        self.failed_room_reasons.pop(room_id, None)
        self.skipped_room_reasons.pop(room_id, None)
        if room_id in self.retry_room_ids:
            self.retry_room_ids.remove(room_id)
        if room_id in self.priority_retry_room_ids:
            self.priority_retry_room_ids.remove(room_id)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "SessionState | None":
        """Build session state from stored JSON."""
        if not isinstance(data, dict):
            return None
        return cls(
            session_id=data.get("session_id") or utcnow_iso(),
            started_at=data.get("started_at") or utcnow_iso(),
            active=bool(data.get("active", True)),
            cancelled=bool(data.get("cancelled", False)),
            attempted_room_ids=list(data.get("attempted_room_ids") or []),
            completed_room_ids=list(data.get("completed_room_ids") or []),
            skipped_room_ids=list(data.get("skipped_room_ids") or []),
            failed_room_ids=list(data.get("failed_room_ids") or []),
            skipped_room_reasons=dict(data.get("skipped_room_reasons") or {}),
            failed_room_reasons=dict(data.get("failed_room_reasons") or {}),
            fallback_attempted_room_ids=list(
                data.get("fallback_attempted_room_ids") or []
            ),
            fallback_completed_room_ids=list(
                data.get("fallback_completed_room_ids") or []
            ),
            fallback_failed_room_ids=list(
                data.get("fallback_failed_room_ids") or []
            ),
            fallback_failed_room_reasons=dict(
                data.get("fallback_failed_room_reasons") or {}
            ),
            deferred_full_clean_room_ids=list(
                data.get("deferred_full_clean_room_ids") or []
            ),
            deferred_full_clean_reasons=dict(
                data.get("deferred_full_clean_reasons") or {}
            ),
            retry_room_ids=list(data.get("retry_room_ids") or []),
            priority_retry_room_ids=list(data.get("priority_retry_room_ids") or []),
            retried_room_ids=list(data.get("retried_room_ids") or []),
            pending_recovery_room_id=data.get("pending_recovery_room_id"),
            pending_recovery_reason=data.get("pending_recovery_reason"),
            pending_recovery_priority=bool(data.get("pending_recovery_priority", False)),
            active_room_id=data.get("active_room_id"),
            terminal_reason=data.get("terminal_reason"),
            terminal_message=data.get("terminal_message"),
            terminal_cause=data.get("terminal_cause"),
            needs_help=bool(data.get("needs_help", False)),
            notification_sent=bool(data.get("notification_sent", False)),
            degraded_reason=data.get("degraded_reason"),
            degraded_at=data.get("degraded_at"),
            degraded_preparation_attempted=bool(
                data.get("degraded_preparation_attempted", False)
            ),
            degraded_preparation_completed=bool(
                data.get("degraded_preparation_completed", False)
            ),
            blocked_deadline=data.get("blocked_deadline"),
            blocked_reason=data.get("blocked_reason"),
            native_resume_guard_latched=bool(
                data.get("native_resume_guard_latched", False)
            ),
            native_guard_cancel_pending=bool(
                data.get("native_guard_cancel_pending", False)
            ),
            native_guard_stop_confirmed=bool(
                data.get("native_guard_stop_confirmed", False)
            ),
            native_guard_return_confirmed=bool(
                data.get("native_guard_return_confirmed", False)
            ),
            native_guard_cancel_reason=data.get("native_guard_cancel_reason"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize session state to JSON-safe data."""
        return {
            "session_id": self.session_id,
            "started_at": self.started_at,
            "active": self.active,
            "cancelled": self.cancelled,
            "attempted_room_ids": self.attempted_room_ids,
            "completed_room_ids": self.completed_room_ids,
            "skipped_room_ids": self.skipped_room_ids,
            "failed_room_ids": self.failed_room_ids,
            "skipped_room_reasons": self.skipped_room_reasons,
            "failed_room_reasons": self.failed_room_reasons,
            "fallback_attempted_room_ids": self.fallback_attempted_room_ids,
            "fallback_completed_room_ids": self.fallback_completed_room_ids,
            "fallback_failed_room_ids": self.fallback_failed_room_ids,
            "fallback_failed_room_reasons": self.fallback_failed_room_reasons,
            "deferred_full_clean_room_ids": self.deferred_full_clean_room_ids,
            "deferred_full_clean_reasons": self.deferred_full_clean_reasons,
            "retry_room_ids": self.retry_room_ids,
            "priority_retry_room_ids": self.priority_retry_room_ids,
            "retried_room_ids": self.retried_room_ids,
            "pending_recovery_room_id": self.pending_recovery_room_id,
            "pending_recovery_reason": self.pending_recovery_reason,
            "pending_recovery_priority": self.pending_recovery_priority,
            "active_room_id": self.active_room_id,
            "terminal_reason": self.terminal_reason,
            "terminal_message": self.terminal_message,
            "terminal_cause": self.terminal_cause,
            "needs_help": self.needs_help,
            "notification_sent": self.notification_sent,
            "degraded_reason": self.degraded_reason,
            "degraded_at": self.degraded_at,
            "degraded_preparation_attempted": self.degraded_preparation_attempted,
            "degraded_preparation_completed": self.degraded_preparation_completed,
            "blocked_deadline": self.blocked_deadline,
            "blocked_reason": self.blocked_reason,
            "native_resume_guard_latched": self.native_resume_guard_latched,
            "native_guard_cancel_pending": self.native_guard_cancel_pending,
            "native_guard_stop_confirmed": self.native_guard_stop_confirmed,
            "native_guard_return_confirmed": self.native_guard_return_confirmed,
            "native_guard_cancel_reason": self.native_guard_cancel_reason,
        }


@dataclass(slots=True)
class AutoCleanSummary:
    """Human-facing summary for an auto-clean session."""

    title: str
    message: str


def utcnow_iso() -> str:
    """Return an ISO timestamp in UTC."""
    return datetime.now(UTC).isoformat()


def format_room_list(room_names: list[str]) -> str:
    """Format room names as a human-readable list."""
    if not room_names:
        return ""
    if len(room_names) == 1:
        return room_names[0]
    if len(room_names) == 2:
        return f"{room_names[0]} and {room_names[1]}"
    return f"{', '.join(room_names[:-1])}, and {room_names[-1]}"


def room_count_label(count: int) -> str:
    """Return a room-count label."""
    return f"{count} {'Room' if count == 1 else 'Rooms'}"


def friendly_failure_reason(reason: str | None) -> str:
    """Normalize a technical failure reason for summary notifications."""
    normalized = (normalize_state(reason) or "an unknown error").strip()
    lowered = normalized.lower()
    if lowered.startswith("cleaned for ") and "below" in lowered:
        duration = normalized.split(",", 1)[0][len("Cleaned for ") :].strip()
        return f"it only ran {duration}"
    if "cannot reach target" in lowered:
        return "it could not reach the room"
    if "unknown error 95" in lowered or "easy-to-fall" in lowered or "fall hazard" in lowered:
        return "it detected a ramp or fall hazard"
    if "cannot navigate to the dock" in lowered or "cannot reach dock" in lowered:
        return "it cannot reach the dock"
    if "mop attachment is missing" in lowered:
        return "the mop attachment was not detected"
    if "tracked person arrived home" in lowered:
        return "someone came home"
    if (
        "clean water" in lowered
        or "fresh water" in lowered
        or "freshwater" in lowered
        or "water tank empty" in lowered
    ):
        return "the clean water tank is empty"
    if "dirty tank" in lowered or "dirty water" in lowered or "wastewater" in lowered:
        return "the dirty water tank is full"
    if "detergent" in lowered or "cleaning liquid" in lowered or "fortified liquid" in lowered:
        return "the detergent is empty"
    if "dustbag" in lowered or "dust bag" in lowered:
        return "the dock dustbag needs attention"
    return normalized[0].lower() + normalized[1:] if normalized else "an unknown error"


def cleaned_summary_sentence(vacuum_name: str, room_names: list[str]) -> str:
    """Return a compact notification sentence for cleaned rooms."""
    if len(room_names) == 1:
        return f"{vacuum_name} cleaned {room_names[0]} while everyone was away."
    return f"{vacuum_name} cleaned {room_count_label(len(room_names)).lower()} while everyone was away."


def has_reportable_issues(
    skipped_room_reasons: dict[str, str],
    failed_room_reasons: dict[str, str],
    *,
    needs_help: bool = False,
) -> bool:
    """Return whether a session has issues worth mentioning in a notification."""
    if needs_help or skipped_room_reasons:
        return True
    return any(
        friendly_failure_reason(reason) != "someone came home"
        for reason in failed_room_reasons.values()
    )


def build_cleaned_messages(room_names: list[str]) -> list[str]:
    """Return dashboard messages for successfully cleaned rooms."""
    return [f"Cleaned {room_name}" for room_name in room_names]


def build_issue_messages(
    skipped_room_reasons: dict[str, str],
    failed_room_reasons: dict[str, str],
) -> list[str]:
    """Return dashboard messages for skipped or failed rooms."""
    messages: list[str] = []
    for room_name, reason in skipped_room_reasons.items():
        friendly = friendly_failure_reason(reason)
        messages.append(f"Could not {issue_action(friendly)} {room_name} because {friendly}")
    for room_name, reason in failed_room_reasons.items():
        friendly = friendly_failure_reason(reason)
        if friendly == "someone came home":
            continue
        messages.append(f"Could not clean {room_name} because {friendly}")
    return messages


def issue_action(friendly_reason: str) -> str:
    """Return the most natural verb for an issue reason."""
    if friendly_reason in {
        "the mop attachment was not detected",
        "the clean water tank is empty",
        "the dirty water tank is full",
        "the detergent is empty",
    }:
        return "mop"
    return "clean"


def build_auto_clean_summary(
    *,
    vacuum_name: str,
    completed_room_names: list[str],
    skipped_room_reasons: dict[str, str],
    failed_room_reasons: dict[str, str],
    terminal_reason: str | None,
    terminal_message: str | None = None,
    needs_help: bool = False,
    all_rooms_cleaned: bool = False,
    total_room_count: int | None = None,
    fallback_room_names: list[str] | None = None,
    deferred_room_names: list[str] | None = None,
) -> AutoCleanSummary | None:
    """Build the one notification for an auto-clean session."""
    completed_count = len(completed_room_names)
    fallback_room_names = fallback_room_names or []
    deferred_room_names = deferred_room_names or []
    fallback_count = len(fallback_room_names)
    friendly_terminal = friendly_failure_reason(terminal_message)

    if terminal_reason == "mop_resource_deferred":
        parts: list[str] = []
        if completed_count:
            parts.append(
                f"{vacuum_name} finished "
                f"{room_count_label(completed_count).lower()}"
            )
        if fallback_count:
            verb = "vacuumed" if parts else f"{vacuum_name} vacuumed"
            subject = (
                fallback_room_names[0]
                if fallback_count == 1
                else f"{fallback_count} additional rooms"
            )
            parts.append(f"{verb} {subject}")
        message = " and ".join(parts) if parts else f"{vacuum_name} could not finish"
        if deferred_room_names:
            message += (
                f"; mopping remains due in {format_room_list(deferred_room_names)} "
                f"because {friendly_terminal}"
            )
        return AutoCleanSummary(
            title=f"{vacuum_name} · Mopping Deferred",
            message=f"{message}.",
        )

    if needs_help:
        if completed_count:
            message = cleaned_summary_sentence(vacuum_name, completed_room_names)
            if has_reportable_issues(
                skipped_room_reasons,
                failed_room_reasons,
                needs_help=True,
            ):
                message += " While everyone was away, the vacuum ran into some errors."
            return AutoCleanSummary(
                title=f"{vacuum_name} · Needs Help",
                message=message,
            )
        return AutoCleanSummary(
            title=f"{vacuum_name} · Needs Help",
            message=f"Stopped before any room finished: {friendly_terminal}.",
        )

    if terminal_reason == "blocked":
        if completed_count:
            return AutoCleanSummary(
                title=f"{vacuum_name} · Stopped Early",
                message=(
                    f"{cleaned_summary_sentence(vacuum_name, completed_room_names)} "
                    f"Automatic cleaning stopped because {friendly_terminal}."
                ),
            )
        return AutoCleanSummary(
            title=f"{vacuum_name} · Auto-Clean Blocked",
            message=f"Could not start: {friendly_terminal}.",
        )

    if completed_count == 0:
        if terminal_reason == "returned_home" or terminal_reason == "cancelled":
            return None
        return None

    if all_rooms_cleaned:
        count = total_room_count or completed_count
        return AutoCleanSummary(
            title=f"{vacuum_name} · Auto-Clean Complete",
            message=f"{vacuum_name} cleaned all {room_count_label(count).lower()} while everyone was away.",
        )

    message = cleaned_summary_sentence(vacuum_name, completed_room_names)
    if has_reportable_issues(skipped_room_reasons, failed_room_reasons):
        message += " While everyone was away, the vacuum ran into some errors."

    return AutoCleanSummary(
        title=(
            f"{vacuum_name} · Stopped Early"
            if terminal_reason in {"returned_home", "cancelled"}
            else f"{vacuum_name} · Auto-Cleaned {room_count_label(completed_count)}"
        ),
        message=message,
    )


def no_selection_terminal_reason(
    *,
    completed_room_ids: list[str],
    skipped_room_ids: list[str],
    failed_room_ids: list[str],
    current_skipped_count: int,
) -> str:
    """Return the terminal reason when room selection has no next candidate."""
    if completed_room_ids:
        return "complete"
    if skipped_room_ids or failed_room_ids or current_skipped_count:
        return "blocked"
    return "complete"


def group_room_reasons(room_reasons: dict[str, str]) -> dict[str, list[str]]:
    """Group room names by friendly failure reason."""
    reason_groups: dict[str, list[str]] = {}
    for room_name, reason in room_reasons.items():
        reason_groups.setdefault(friendly_failure_reason(reason), []).append(room_name)
    return reason_groups


def parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO datetime string."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_float(value: Any) -> float | None:
    """Parse HA state strings into floats."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_state(value: Any) -> str | None:
    """Normalize a Home Assistant state string."""
    if value is None:
        return None
    return str(value).strip()


def is_error_clear(error: str | None) -> bool:
    """Return True if the Valetudo error sensor is clear."""
    return normalize_state(error) in NO_ERROR_VALUES


def error_contains_any(error: str | None, keywords: tuple[str, ...]) -> bool:
    """Return True when an error message contains any keyword."""
    normalized = normalize_state(error)
    if not normalized or normalized in NO_ERROR_VALUES:
        return False
    lowered = normalized.lower()
    return any(keyword in lowered for keyword in keywords)


def is_recoverable_navigation_error(error: str | None) -> bool:
    """Return whether a navigation failure can recover after the robot docks."""
    normalized = normalize_state(error)
    if not normalized or is_error_clear(normalized):
        return False
    lowered = normalized.lower()
    return (
        error_contains_any(normalized, RECOVERABLE_NAVIGATION_FAILURE_KEYWORDS)
        and "dock" not in lowered
    )


def is_low_battery_error(error: str | None) -> bool:
    """Return whether an error is a temporary low-battery interruption."""
    return error_contains_any(error, LOW_BATTERY_ERROR_KEYWORDS)


def is_clean_water_empty_error(error: str | None) -> bool:
    """Return whether Valetudo reports the supported clean-water-empty fault."""
    normalized = normalize_state(error)
    return bool(normalized and normalized.lower() in CLEAN_WATER_EMPTY_ERROR_VALUES)


def clean_water_empty_reason(resources: ResourceState) -> str | None:
    """Return the exact clean-water-empty reason, if present."""
    if is_clean_water_empty_error(resources.error):
        return normalize_state(resources.error)
    fresh_water = normalize_state(resources.fresh_water)
    if fresh_water and fresh_water.lower() == "empty":
        return "fresh water is empty"
    return None


def allowed_error_fingerprint(resources: ResourceState) -> str | None:
    """Return the persisted fingerprint for an allowed degraded vacuum run."""
    if not clean_water_empty_reason(resources):
        return None
    normalized_error = normalize_state(resources.error)
    if is_clean_water_empty_error(normalized_error):
        return normalized_error.lower()
    return CLEAN_WATER_EMPTY_DISPOSITION


def run_allows_error(run: ActiveRun, error: str | None) -> bool:
    """Return whether a run may ignore this exact pre-existing water warning."""
    if not run.vacuum_only or not run.allowed_error_fingerprint:
        return False
    normalized = normalize_state(error)
    if not is_clean_water_empty_error(normalized):
        return False
    fingerprint = run.allowed_error_fingerprint
    return (
        fingerprint == CLEAN_WATER_EMPTY_DISPOSITION
        or fingerprint == normalized.lower()
    )


def mop_block_reason(room: RoomConfig, resources: ResourceState) -> str | None:
    """Return the reason a mop-required room cannot run, if any."""
    if not room.mop_required:
        return None

    component_checks = [
        ("fresh water", resources.fresh_water, {"empty", "missing", "unknown", "unavailable"}),
        ("dirty water", resources.dirty_water, {"full", "missing", "unknown", "unavailable"}),
        ("detergent", resources.detergent, {"empty", "missing", "unknown", "unavailable"}),
    ]
    for label, state, bad_values in component_checks:
        if state is not None and normalize_state(state).lower() in bad_values:
            return f"{label} is {state}"

    if error_contains_any(resources.error, RECOVERABLE_MOP_ERROR_KEYWORDS):
        return normalize_state(resources.error)

    return None


def room_sort_key(room: RoomConfig, ledger: dict[str, RoomLedger]) -> tuple[int, str, str]:
    """Sort rooms by oldest successful clean, with never-cleaned rooms first."""
    room_ledger = ledger.get(room.room_id, RoomLedger())
    if not room_ledger.last_successful_clean:
        return (0, "", room.name)
    return (1, room_ledger.last_successful_clean, room.name)


def room_auto_cleaned_on(ledger: RoomLedger, auto_clean_day: str | None) -> bool:
    """Return whether a room already consumed its auto-clean slot for a day."""
    return bool(auto_clean_day and ledger.last_auto_cleaned_day == auto_clean_day)


def select_next_room(
    rooms: list[RoomConfig],
    ledger: dict[str, RoomLedger],
    attempted_room_ids: set[str],
    resources: ResourceState,
    allow_vacuum_only_when_mop_blocked: bool,
    auto_clean_day: str | None = None,
    retry_room_ids: list[str] | None = None,
    priority_retry_room_ids: list[str] | None = None,
) -> tuple[RoomSelection | None, list[tuple[RoomConfig, str]]]:
    """Select the next eligible room and return any skipped rooms with reasons."""
    skipped: list[tuple[RoomConfig, str]] = []
    room_by_id = {room.room_id: room for room in rooms}

    priority_rooms = [
        room_by_id[room_id]
        for room_id in priority_retry_room_ids or []
        if room_id in room_by_id
        and room_by_id[room_id].enabled
        and not room_auto_cleaned_on(
            ledger.get(room_id, RoomLedger()),
            auto_clean_day,
        )
    ]
    priority_room_ids = {room.room_id for room in priority_rooms}
    pending_rooms = priority_rooms + [
        room
        for room in sorted((item for item in rooms if item.enabled), key=lambda item: room_sort_key(item, ledger))
        if room.room_id not in attempted_room_ids
        and room.room_id not in priority_room_ids
        and not room_auto_cleaned_on(ledger.get(room.room_id, RoomLedger()), auto_clean_day)
    ]
    pending_room_ids = {room.room_id for room in pending_rooms}
    for room_id in retry_room_ids or []:
        room = room_by_id.get(room_id)
        if (
            room
            and room.enabled
            and room.room_id not in pending_room_ids
            and not room_auto_cleaned_on(ledger.get(room.room_id, RoomLedger()), auto_clean_day)
        ):
            pending_rooms.append(room)
            pending_room_ids.add(room.room_id)

    general_block_reason = cleaning_block_reason(resources)
    if general_block_reason:
        return None, [(room, general_block_reason) for room in pending_rooms]

    for room in pending_rooms:
        reason = mop_block_reason(room, resources)
        if reason is None:
            return RoomSelection(room=room, vacuum_only=not room.mop_required), skipped

        if (
            allow_vacuum_only_when_mop_blocked
            and clean_water_empty_reason(resources)
        ):
            return RoomSelection(
                room=room,
                vacuum_only=True,
                mop_block_reason=reason,
                fallback_vacuum=True,
            ), skipped

        skipped.append((room, reason))

    return None, skipped


def cleaning_block_reason(resources: ResourceState) -> str | None:
    """Return a reason no cleaning should start at all."""
    dustbag = normalize_state(resources.dustbag)
    if dustbag is not None and dustbag.lower() in {"full", "missing", "unknown", "unavailable"}:
        return f"dustbag is {dustbag}"

    normalized_error = normalize_state(resources.error)
    if (
        not is_error_clear(normalized_error)
        and not error_contains_any(normalized_error, RECOVERABLE_MOP_ERROR_KEYWORDS)
        and not is_recoverable_navigation_error(normalized_error)
    ):
        return normalized_error

    return None


def evaluate_run_success(
    room: RoomConfig,
    run: ActiveRun,
    end_area: float | None,
    end_time: float | None,
    error: str | None,
) -> tuple[bool, str | None]:
    """Evaluate whether a commanded room run should count as successful."""
    if run.cancelled:
        return False, "Run was cancelled"
    if not is_error_clear(error) and not run_allows_error(run, error):
        return False, normalize_state(error)
    if not run.observed_cleaning:
        return False, "Vacuum never entered cleaning state"
    if not run.observed_segment_cleaning:
        return False, "Vacuum never reported segment cleaning"

    if room.require_estimated_segment:
        wrong_room = dominant_wrong_room(run, room)
        if wrong_room is not None:
            wrong_room_id, wrong_dwell, commanded_dwell = wrong_room
            return (
                False,
                f"{WRONG_ROOM_FAILURE_PREFIX} {wrong_room_id} "
                f"({wrong_dwell:.0f}s versus {commanded_dwell:.0f}s in {room.room_id})",
            )

    duration_delta = run.total_time(end_time)
    if duration_delta is not None:
        if duration_delta < room.min_duration:
            return False, f"Cleaned for {duration_delta:.0f}s, below {room.min_duration}s threshold"

    area_delta = run.total_area(end_area)
    if room.min_area > 0 and area_delta is not None:
        if area_delta < room.min_area:
            return False, f"Cleaned area {area_delta:.1f}, below {room.min_area:.1f} threshold"

    if room.require_estimated_segment:
        dwell = run.estimated_dwell_seconds.get(room.room_id, 0.0)
        if dwell < room.min_estimated_dwell:
            return False, f"Estimated in-room dwell {dwell:.0f}s, below {room.min_estimated_dwell}s threshold"

    return True, None


def counter_delta(start_value: float, end_value: float) -> float:
    """Return a delta for counters that may reset at the start of a run."""
    if end_value >= start_value:
        return end_value - start_value
    return max(0.0, end_value)


def dominant_wrong_room(
    run: ActiveRun,
    room: RoomConfig,
) -> tuple[str, float, float] | None:
    """Return a materially dominant non-commanded room, excluding transit."""
    commanded_dwell = run.estimated_dwell_seconds.get(room.room_id, 0.0)
    if commanded_dwell < max(10.0, float(room.min_estimated_dwell)):
        return None
    wrong_rooms = [
        (room_id, dwell)
        for room_id, dwell in run.estimated_dwell_seconds.items()
        if room_id != room.room_id
    ]
    if not wrong_rooms:
        return None

    wrong_room_id, wrong_dwell = max(wrong_rooms, key=lambda item: item[1])
    minimum_dwell = max(60.0, float(room.min_estimated_dwell))
    if (
        wrong_dwell >= minimum_dwell
        and wrong_dwell >= commanded_dwell * 1.5
        and wrong_dwell - commanded_dwell >= 10.0
    ):
        return wrong_room_id, wrong_dwell, commanded_dwell
    return None


def is_wrong_room_failure(reason: str | None) -> bool:
    """Return whether a failure indicates the firmware cleaned another room."""
    return bool(reason and reason.startswith(WRONG_ROOM_FAILURE_PREFIX))


def manual_rooms_to_credit(
    rooms: list[RoomConfig],
    run: ActiveRun,
) -> list[RoomConfig]:
    """Determine which rooms from a manual run should receive credit."""
    credited: list[RoomConfig] = []
    room_by_id = {room.room_id: room for room in rooms}
    allowed_room_ids = (
        set(run.manual_credit_room_ids) if run.manual_credit_room_ids is not None else None
    )

    for room_id, dwell in run.estimated_dwell_seconds.items():
        if allowed_room_ids is not None and room_id not in allowed_room_ids:
            continue
        room = room_by_id.get(room_id)
        if room and dwell >= room.min_estimated_dwell:
            credited.append(room)

    return credited


def mark_success(
    ledger: RoomLedger,
    when: str,
    mop: bool,
    vacuum: bool = True,
    *,
    auto_clean: bool = False,
    auto_clean_day: str | None = None,
) -> None:
    """Update a room ledger after a successful clean."""
    ledger.last_attempted = when
    ledger.last_successful_clean = when
    if vacuum:
        ledger.last_vacuumed = when
    if mop:
        ledger.last_mopped = when
    if auto_clean:
        ledger.last_auto_cleaned = when
        ledger.last_auto_cleaned_day = auto_clean_day or auto_clean_date_from_timestamp(when)
    ledger.last_failed_reason = None
    ledger.successful_count += 1


def mark_fallback_vacuum_success(ledger: RoomLedger, when: str) -> None:
    """Record physical vacuuming without granting normal/full clean credit."""
    ledger.last_attempted = when
    ledger.last_vacuumed = when
    ledger.last_fallback_vacuumed = when


def auto_clean_date_from_timestamp(value: str | None) -> str | None:
    """Return an ISO date from a timestamp for legacy callers."""
    parsed = parse_datetime(value)
    return parsed.date().isoformat() if parsed else None


def mark_failure(ledger: RoomLedger, when: str, reason: str | None) -> None:
    """Update a room ledger after a failed or skipped run."""
    ledger.last_attempted = when
    ledger.last_failed_reason = reason or "Unknown failure"
