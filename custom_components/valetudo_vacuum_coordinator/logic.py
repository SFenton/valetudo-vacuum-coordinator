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

NAVIGATION_FAILURE_KEYWORDS = (
    "cannot reach",
    "cannot arrive",
    "cannot navigate",
    "trapped",
    "stuck",
    "blocked",
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
    active_room_id: str | None = None

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

    def mark_skipped(self, room_id: str) -> None:
        """Record a skipped room for this session."""
        self.mark_attempted(room_id)
        if room_id not in self.skipped_room_ids:
            self.skipped_room_ids.append(room_id)


def utcnow_iso() -> str:
    """Return an ISO timestamp in UTC."""
    return datetime.now(UTC).isoformat()


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

    if resources.mop_attached is False:
        return "Mop attachment is missing"

    component_checks = [
        ("fresh water", resources.fresh_water, {"empty", "missing", "unknown", "unavailable"}),
        ("dirty water", resources.dirty_water, {"full", "missing", "unknown", "unavailable"}),
        ("detergent", resources.detergent, {"empty", "missing", "unknown", "unavailable"}),
    ]
    for label, state, bad_values in component_checks:
        if state is not None and normalize_state(state).lower() in bad_values:
            return f"{label} is {state}"

    if error_contains_any(resources.error, MOP_RESOURCE_ERROR_KEYWORDS):
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
            return RoomSelection(room=room), skipped

        if allow_vacuum_only_when_mop_blocked:
            return RoomSelection(room=room, vacuum_only=True, mop_block_reason=reason), skipped

        skipped.append((room, reason))

    return None, skipped


def cleaning_block_reason(resources: ResourceState) -> str | None:
    """Return a reason no cleaning should start at all."""
    dustbag = normalize_state(resources.dustbag)
    if dustbag is not None and dustbag.lower() in {"full", "missing", "unknown", "unavailable"}:
        return f"dustbag is {dustbag}"

    if not is_error_clear(resources.error) and not error_contains_any(
        resources.error, MOP_RESOURCE_ERROR_KEYWORDS
    ):
        return normalize_state(resources.error)

    if error_contains_any(resources.error, NAVIGATION_FAILURE_KEYWORDS):
        return normalize_state(resources.error)

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
        duration_delta = max(0.0, end_time - start_time)
        if duration_delta < room.min_duration:
            return False, f"Cleaned for {duration_delta:.0f}s, below {room.min_duration}s threshold"

    start_area = run.start_area
    if room.min_area > 0 and start_area is not None and end_area is not None:
        area_delta = max(0.0, end_area - start_area)
        if area_delta < room.min_area:
            return False, f"Cleaned area {area_delta:.1f}, below {room.min_area:.1f} threshold"

    if room.require_estimated_segment:
        dwell = run.estimated_dwell_seconds.get(room.room_id, 0.0)
        if dwell < room.min_estimated_dwell:
            return False, f"Estimated in-room dwell {dwell:.0f}s, below {room.min_estimated_dwell}s threshold"

    return True, None


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
