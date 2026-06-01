"""Pure scheduling and success logic for Valetudo Vacuum Coordinator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

DOCK_COMPONENT_BAD_VALUES = {"empty", "full", "missing", "unknown", "unavailable"}
NO_ERROR_VALUES = {None, "", "No error", "no error", "none", "unknown", "unavailable"}

MOP_RESOURCE_ERROR_KEYWORDS = (
    "clean water",
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
)


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


@dataclass(slots=True)
class RoomLedger:
    """Persisted cleaning history for one room."""

    last_successful_clean: str | None = None
    last_vacuumed: str | None = None
    last_mopped: str | None = None
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
            last_mopped=data.get("last_mopped"),
            last_attempted=data.get("last_attempted"),
            last_failed_reason=data.get("last_failed_reason"),
            successful_count=int(data.get("successful_count", 0) or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the ledger to JSON-safe data."""
        return {
            "last_successful_clean": self.last_successful_clean,
            "last_vacuumed": self.last_vacuumed,
            "last_mopped": self.last_mopped,
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
    cancelled: bool = False
    observed_cleaning: bool = False
    observed_segment_cleaning: bool = False
    last_estimated_room_id: str | None = None
    last_estimated_changed_at: str | None = None
    estimated_dwell_seconds: dict[str, float] = field(default_factory=dict)

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
        return cls(
            room_id=data.get("room_id"),
            segment_id=data.get("segment_id"),
            session_id=data.get("session_id"),
            started_at=data.get("started_at") or utcnow_iso(),
            start_area=parse_float(data.get("start_area")),
            start_time=parse_float(data.get("start_time")),
            manual=bool(data.get("manual", False)),
            vacuum_only=bool(data.get("vacuum_only", False)),
            cancelled=bool(data.get("cancelled", False)),
            observed_cleaning=bool(data.get("observed_cleaning", False)),
            observed_segment_cleaning=bool(data.get("observed_segment_cleaning", False)),
            last_estimated_room_id=data.get("last_estimated_room_id"),
            last_estimated_changed_at=data.get("last_estimated_changed_at"),
            estimated_dwell_seconds={
                str(room_id): float(seconds)
                for room_id, seconds in (data.get("estimated_dwell_seconds") or {}).items()
            },
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
            "cancelled": self.cancelled,
            "observed_cleaning": self.observed_cleaning,
            "observed_segment_cleaning": self.observed_segment_cleaning,
            "last_estimated_room_id": self.last_estimated_room_id,
            "last_estimated_changed_at": self.last_estimated_changed_at,
            "estimated_dwell_seconds": self.estimated_dwell_seconds,
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
    active_room_id: str | None = None
    terminal_reason: str | None = None
    terminal_message: str | None = None
    needs_help: bool = False
    notification_sent: bool = False

    def mark_attempted(self, room_id: str) -> None:
        """Record that a room has consumed its one attempt for this session."""
        if room_id not in self.attempted_room_ids:
            self.attempted_room_ids.append(room_id)

    def mark_completed(self, room_id: str) -> None:
        """Record a completed room for this session."""
        self.mark_attempted(room_id)
        if room_id not in self.completed_room_ids:
            self.completed_room_ids.append(room_id)
        self.active_room_id = None

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
            active_room_id=data.get("active_room_id"),
            terminal_reason=data.get("terminal_reason"),
            terminal_message=data.get("terminal_message"),
            needs_help=bool(data.get("needs_help", False)),
            notification_sent=bool(data.get("notification_sent", False)),
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
            "active_room_id": self.active_room_id,
            "terminal_reason": self.terminal_reason,
            "terminal_message": self.terminal_message,
            "needs_help": self.needs_help,
            "notification_sent": self.notification_sent,
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
    if "cannot navigate to the dock" in lowered or "cannot reach dock" in lowered:
        return "it cannot reach the dock"
    if "mop attachment is missing" in lowered:
        return "the mop attachment was not detected"
    if "tracked person arrived home" in lowered:
        return "someone came home"
    if "clean water" in lowered or "water tank empty" in lowered:
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
) -> AutoCleanSummary | None:
    """Build the one notification for an auto-clean session."""
    completed_count = len(completed_room_names)
    friendly_terminal = friendly_failure_reason(terminal_message)

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

    if completed_count == 0:
        if terminal_reason == "returned_home" or terminal_reason == "cancelled":
            return None
        if terminal_reason == "blocked":
            return AutoCleanSummary(
                title=f"{vacuum_name} · Auto-Clean Blocked",
                message=f"Could not start: {friendly_terminal}.",
            )
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


def select_next_room(
    rooms: list[RoomConfig],
    ledger: dict[str, RoomLedger],
    attempted_room_ids: set[str],
    resources: ResourceState,
    allow_vacuum_only_when_mop_blocked: bool,
) -> tuple[RoomSelection | None, list[tuple[RoomConfig, str]]]:
    """Select the next eligible room and return any skipped rooms with reasons."""
    skipped: list[tuple[RoomConfig, str]] = []

    pending_rooms = [
        room
        for room in sorted((item for item in rooms if item.enabled), key=lambda item: room_sort_key(item, ledger))
        if room.room_id not in attempted_room_ids
    ]

    general_block_reason = cleaning_block_reason(resources)
    if general_block_reason:
        return None, [(room, general_block_reason) for room in pending_rooms]

    for room in pending_rooms:
        reason = mop_block_reason(room, resources)
        if reason is None:
            return RoomSelection(room=room, vacuum_only=not room.mop_required), skipped

        if allow_vacuum_only_when_mop_blocked:
            return RoomSelection(room=room, vacuum_only=True, mop_block_reason=reason), skipped

        skipped.append((room, reason))

    return None, skipped


def cleaning_block_reason(resources: ResourceState) -> str | None:
    """Return a reason no cleaning should start at all."""
    dustbag = normalize_state(resources.dustbag)
    if dustbag is not None and dustbag.lower() in {"full", "missing", "unknown", "unavailable"}:
        return f"dustbag is {dustbag}"

    normalized_error = normalize_state(resources.error)
    recoverable_navigation = (
        error_contains_any(normalized_error, RECOVERABLE_NAVIGATION_FAILURE_KEYWORDS)
        and "dock" not in normalized_error.lower()
        if normalized_error
        else False
    )

    if (
        not is_error_clear(normalized_error)
        and not error_contains_any(normalized_error, RECOVERABLE_MOP_ERROR_KEYWORDS)
        and not recoverable_navigation
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
    if not is_error_clear(error):
        return False, normalize_state(error)
    if not run.observed_cleaning:
        return False, "Vacuum never entered cleaning state"
    if not run.observed_segment_cleaning:
        return False, "Vacuum never reported segment cleaning"

    start_time = run.start_time
    if start_time is not None and end_time is not None:
        duration_delta = counter_delta(start_time, end_time)
        if duration_delta < room.min_duration:
            return False, f"Cleaned for {duration_delta:.0f}s, below {room.min_duration}s threshold"

    start_area = run.start_area
    if room.min_area > 0 and start_area is not None and end_area is not None:
        area_delta = counter_delta(start_area, end_area)
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


def manual_rooms_to_credit(
    rooms: list[RoomConfig],
    run: ActiveRun,
) -> list[RoomConfig]:
    """Determine which rooms from a manual run should receive credit."""
    credited: list[RoomConfig] = []
    room_by_id = {room.room_id: room for room in rooms}

    for room_id, dwell in run.estimated_dwell_seconds.items():
        room = room_by_id.get(room_id)
        if room and dwell >= room.min_estimated_dwell:
            credited.append(room)

    return credited


def mark_success(ledger: RoomLedger, when: str, mop: bool, vacuum: bool = True) -> None:
    """Update a room ledger after a successful clean."""
    ledger.last_attempted = when
    ledger.last_successful_clean = when
    if vacuum:
        ledger.last_vacuumed = when
    if mop:
        ledger.last_mopped = when
    ledger.last_failed_reason = None
    ledger.successful_count += 1


def mark_failure(ledger: RoomLedger, when: str, reason: str | None) -> None:
    """Update a room ledger after a failed or skipped run."""
    ledger.last_attempted = when
    ledger.last_failed_reason = reason or "Unknown failure"
