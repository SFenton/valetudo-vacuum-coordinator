"""Coordinator for away-only Valetudo room cleaning."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
import json
import logging
import re
from typing import Any

from homeassistant.const import ATTR_ENTITY_ID
from homeassistant.core import Event, HomeAssistant, State, callback
from homeassistant.helpers.event import async_call_later, async_track_state_change_event
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .const import (
    CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED,
    CONF_AUTO_CLEAN_ITERATIONS,
    CONF_BATTERY_ENTITY,
    CONF_BLOCKED_SESSION_TIMEOUT,
    CONF_RESOURCE_SETTLE,
    CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL,
    CONF_CURRENT_AREA_ENTITY,
    CONF_CURRENT_TIME_ENTITY,
    CONF_DETERGENT_ENTITY,
    CONF_DIRTY_WATER_ENTITY,
    CONF_DOCK_STATUS_ENTITY,
    CONF_DISPATCH_START_TIMEOUT,
    CONF_DUSTBAG_ENTITY,
    CONF_ERROR_ENTITY,
    CONF_ESTIMATED_SEGMENT_ENTITY,
    CONF_FAN_AUTO_CLEAN_OPTION,
    CONF_FAN_ENTITY,
    CONF_FRESH_WATER_ENTITY,
    CONF_IDENTIFIER,
    CONF_MANUAL_TRACKING,
    CONF_MIN_BATTERY,
    CONF_NATIVE_RESUME_ENABLED,
    CONF_NATIVE_RESUME_TIMEOUT,
    CONF_NAVIGATION_ERROR_RETURN_ENABLED,
    CONF_DOCK_SETTLE,
    CONF_RESUME_NUDGE_ENABLED,
    CONF_MODE_ENTITY,
    CONF_MODE_MOP_OPTION,
    CONF_MODE_VACUUM_OPTION,
    CONF_MOP_ATTACHMENT_ENTITY,
    CONF_NOTIFICATION_URL,
    CONF_NOTIFY_SERVICE,
    CONF_PASSES_ENTITY,
    CONF_STATUS_FLAG_ENTITY,
    CONF_STALE_RESUME_AGE,
    CONF_STALE_RESUME_AUTO_CLEAR,
    CONF_STALE_RESUME_CLEAR_TIMEOUT,
    CONF_STALE_RESUME_SETTLE,
    CONF_TRACK_MANUAL_WHEN_PAUSED,
    CONF_WATER_ENTITY,
    CONF_WATER_MOP_OPTION,
    COORDINATOR_DATA_VERSION,
    DEFAULT_DOCK_SETTLE,
    DEFAULT_BLOCKED_SESSION_TIMEOUT,
    DEFAULT_RESOURCE_SETTLE,
    DEFAULT_DISPATCH_START_TIMEOUT,
    DEFAULT_MIN_BATTERY,
    DEFAULT_NATIVE_RESUME_ENABLED,
    DEFAULT_NATIVE_RESUME_TIMEOUT,
    DEFAULT_NAVIGATION_ERROR_RETURN_ENABLED,
    DEFAULT_STALE_RESUME_AGE,
    DEFAULT_STALE_RESUME_AUTO_CLEAR,
    DEFAULT_STALE_RESUME_CLEAR_TIMEOUT,
    DEFAULT_STALE_RESUME_SETTLE,
    DOMAIN,
    SERVICE_DOCK_ACTION,
    STATE_DEFERRED,
    STATE_DEGRADED,
    STATE_ERROR,
    STATE_IDLE,
    STATE_PAUSED,
    STATE_RUNNING,
    STATE_SUSPENDED,
    STATE_WAITING,
    STORE_KEY,
    STORE_VERSION,
)
from .logic import (
    ActiveRun,
    AutoCleanSettingsSnapshot,
    BLOCKER_RECOVERABLE,
    BlockerClassification,
    NATIVE_RESUME_PENDING_PHASES,
    RECOVERABLE_MOP_ERROR_KEYWORDS,
    RUN_PHASE_CANCEL_PENDING,
    RUN_PHASE_CLEANING,
    RUN_PHASE_DISPATCHING,
    RUN_PHASE_DOCK_INTERRUPT,
    RUN_PHASE_RECOVERY_STALLED,
    RUN_PHASE_RESUMED_CLEANING,
    RUN_PHASE_SUSPENDED,
    ResourceState,
    RetainedTaskGuard,
    RETAINED_TASK_OWNER_COORDINATOR,
    RETAINED_TASK_OWNER_MANUAL,
    RETAINED_TASK_OWNER_UNKNOWN,
    RETAINED_TASK_PHASE_CLEARED,
    RETAINED_TASK_PHASE_CLEAR_PENDING,
    RETAINED_TASK_PHASE_DOCK_CLEAR_PENDING,
    RETAINED_TASK_PHASE_DOCK_VERIFYING,
    RETAINED_TASK_PHASE_OBSERVED_ACTIVE,
    RETAINED_TASK_PHASE_OPERATOR_REQUIRED,
    RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME,
    RETAINED_TASK_PHASE_STALE_CANDIDATE,
    RETAINED_TASK_PHASE_VERIFYING,
    RoomConfig,
    RoomLedger,
    RoomSelection,
    SessionState,
    WhileAwayOutcome,
    attempt_mode_for_run,
    build_auto_clean_summary,
    build_while_away_outcome_contract,
    build_while_away_messages,
    classify_outcome_reason,
    classify_blocker,
    clean_water_vacuum_only_reason,
    cleaning_block_reason,
    evaluate_floor_completion_evidence,
    evaluate_run_success,
    error_contains_any,
    allowed_error_fingerprint,
    is_clean_water_empty_error,
    is_low_battery_error,
    is_unrecoverable_error,
    is_wrong_room_failure,
    is_error_clear,
    manual_rooms_to_credit,
    mark_failure,
    mark_fallback_vacuum_success,
    mark_success,
    mop_block_reason,
    no_selection_terminal_reason,
    normalize_state,
    parse_datetime,
    parse_float,
    required_operation_for_room,
    room_auto_cleaned_on,
    room_sort_key,
    run_allows_error,
    schedule_hass_task,
    select_next_room,
    utcnow_iso,
)

_LOGGER = logging.getLogger(__name__)

_READY_VACUUM_STATES = {"docked", "idle", "charging"}
_AT_DOCK_VACUUM_STATES = {"docked", "idle", "charging"}
_BUSY_DOCK_STATES = {"cleaning", "emptying", "pause"}
_READY_DOCK_STATES = {"idle", "drying"}
_MOVING_VACUUM_STATES = {
    "cleaning",
    "returning",
    "paused",
    "moving",
    "manual_control",
}
_ACTIVE_RETAINED_VACUUM_STATES = {
    "cleaning",
    "returning",
    "paused",
    "moving",
    "manual_control",
}
_STALE_CLEAR_DOCK_STATES = {"idle", "pause", "drying"}
_UNKNOWN_OR_CLEAR_STATES = {None, "", "unknown", "unavailable", "none"}
_UNKNOWN_PERSON_STATES = {None, "", "unknown", "unavailable"}
_RESTORE_RECONCILE_DELAY_SECONDS = 15
_TERMINAL_CLEANUP_RETRY_SECONDS = 30
_MAX_TERMINAL_CLEANUP_RETRIES = 3
_MAX_AUTOMATIC_DISPATCH_FAILURES = 3
_VALIDATED_NAVIGATION_RETURN_ERRORS = {"cannot reach target"}
_NAVIGATION_RETRY_LATER_POLICY = "navigation_retry_later"


class ValetudoVacuumCoordinator:
    """Coordinate away-only room cleaning for one Valetudo vacuum."""

    def __init__(
        self,
        *,
        hass: HomeAssistant,
        name: str,
        vacuum_entity: str,
        people_entities: list[str],
        segment_command_topic: str,
        rooms: list[RoomConfig],
        config: dict[str, Any],
    ) -> None:
        """Initialize the coordinator."""
        self.hass = hass
        self.name = name
        self.coordinator_id = _slugify(name)
        self.vacuum_entity = vacuum_entity
        self.people_entities = people_entities
        self.segment_command_topic = segment_command_topic
        self.rooms = rooms
        self.room_by_id = {room.room_id: room for room in rooms}
        self.room_by_segment = {room.segment_id: room for room in rooms}
        self.room_by_name = {room.name.lower(): room for room in rooms}
        self.config = config

        self.ledgers: dict[str, RoomLedger] = {room.room_id: RoomLedger() for room in rooms}
        self.disabled_room_ids: set[str] = set()
        self.paused = False
        self.pause_reason: str | None = None
        self.away_since: str | None = None
        self.session: SessionState | None = None
        self.active_run: ActiveRun | None = None
        self.manual_run: ActiveRun | None = None
        self.retained_task_guard: RetainedTaskGuard | None = None
        self.settings_snapshot: AutoCleanSettingsSnapshot | None = None
        self.while_away_outcomes: list[WhileAwayOutcome] = []
        self._while_away_outcome_sequence = 0
        self.last_error: str | None = None

        self._listeners: list[Callable[[], None]] = []
        self._unsubscribers: list[Callable[[], None]] = []
        self._away_timer_cancel: Callable[[], None] | None = None
        self._next_day_timer_cancel: Callable[[], None] | None = None
        self._terminal_cleanup_retry_cancel: Callable[[], None] | None = None
        self._dock_settle_cancel: Callable[[], None] | None = None
        self._native_resume_timeout_cancel: Callable[[], None] | None = None
        self._dispatch_start_timeout_cancel: Callable[[], None] | None = None
        self._cancel_ack_timeout_cancel: Callable[[], None] | None = None
        self._blocked_session_watchdog_cancel: Callable[[], None] | None = None
        self._resource_settle_cancel: Callable[[], None] | None = None
        self._resource_settle_deadline: datetime | None = None
        self._resource_candidates: dict[str, dict[str, str]] = {}
        self._retained_task_timer_cancel: Callable[[], None] | None = None
        self._retained_task_timer_deadline: datetime | None = None
        self._retained_task_reconcile_scheduled = False
        self._blocked_watchdog_expiring = False
        self._terminal_cleanup_retry_attempts = 0
        self._terminal_settings_restore_deferred = False
        self._event_lock = asyncio.Lock()
        self._active_run_restored = False
        self._restored_dispatch_intent_deadline: datetime | None = None
        self._store = Store(hass, STORE_VERSION, f"{STORE_KEY}.{self.coordinator_id}")

    async def async_setup(self) -> None:
        """Load persisted state and attach listeners."""
        await self._async_load_store()
        entities_to_watch = set(self.people_entities)
        for entity_id in self._configured_sensor_entities():
            if entity_id:
                entities_to_watch.add(entity_id)

        self._unsubscribers.append(
            async_track_state_change_event(
                self.hass,
                list(entities_to_watch),
                self._handle_state_change_event,
            )
        )
        async with self._event_lock:
            await self._async_reconcile_retained_task(
                dt_util.utcnow(),
                allow_clear=False,
                restored=True,
            )
            if self.session and self.session.native_guard_cancel_pending:
                if await self._async_execute_native_guard_cancel_pending():
                    await self._async_save_store()
                    self._notify_listeners()
                    await self._async_maybe_send_auto_clean_summary()
            elif (
                self._any_tracked_person_home()
                and self.config.get(CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL)
                and (
                    self.active_run
                    or (self.session and self.session.active)
                    or (
                        self.session
                        and self.session.native_resume_guard_latched
                    )
                )
            ):
                await self._async_cancel_session("Tracked person arrived home")
            else:
                await self._async_reconcile_restored_session()
        if self.active_run:
            self._schedule_active_run_timers()
            self._unsubscribers.append(
                async_call_later(
                    self.hass,
                    _RESTORE_RECONCILE_DELAY_SECONDS,
                    self._handle_delayed_restore_reconcile,
                )
            )
        if self.retained_task_guard:
            self._schedule_retained_task_timer()
        if getattr(self, "_resource_candidates", {}):
            self._schedule_resource_settle_timer()
        if self._all_people_away() and self.away_since is None:
            self.away_since = self._latest_person_away_since()
            await self._async_save_store()
        elif self._any_tracked_person_home():
            self.away_since = None
            await self._async_save_store()
        if self._prune_while_away_outcomes_for_day(self._current_auto_clean_day()):
            await self._async_save_store()
        self._schedule_next_day_timer_if_needed()
        self._schedule_away_timer_if_needed()
        self._notify_listeners()

    @callback
    def async_add_listener(self, update_callback: Callable[[], None]) -> Callable[[], None]:
        """Add a listener for entity updates."""
        self._listeners.append(update_callback)

        def remove_listener() -> None:
            if update_callback in self._listeners:
                self._listeners.remove(update_callback)

        return remove_listener

    @property
    def state(self) -> str:
        """Return a user-facing coordinator state."""
        if self.paused:
            return STATE_PAUSED
        if (
            self.session
            and self.session.active
            and self.session.recovery_phase
            in {
                "cancelling_pending_command",
                "operator_required",
                "suspended",
                "waiting_for_clear",
                "waiting_for_retry",
            }
        ):
            return STATE_SUSPENDED
        if self.active_run:
            return STATE_RUNNING
        if (
            self.session
            and self.session.terminal_reason == "mop_resource_deferred"
            and (
                is_error_clear(self.error_state)
                or is_clean_water_empty_error(self.error_state)
            )
        ):
            return STATE_DEFERRED
        if (
            self.session
            and self.session.active
            and self.session.degraded_reason
            and (
                is_error_clear(self.error_state)
                or (
                    (blocker := self._current_blocker()) is not None
                    and blocker.vacuum_only_safe
                )
                or is_low_battery_error(self.error_state)
            )
        ):
            return STATE_DEGRADED
        if (
            not is_error_clear(self.error_state)
            and (not self.session or not self.session.active)
        ):
            return STATE_ERROR
        if self.session and self.session.active:
            return STATE_WAITING
        return STATE_IDLE

    @property
    def auto_cleaning(self) -> bool:
        """Return whether an away auto-clean session is active or pending summary."""
        if not self.session:
            return False
        if self.session.active or self.active_run:
            return True
        if self.session.notification_sent and self.settings_snapshot is None:
            return False
        return bool(self.session.terminal_reason)

    @property
    def active_room(self) -> RoomConfig | None:
        """Return the currently commanded room, if any."""
        if not self.active_run or not self.active_run.room_id:
            return None
        return self.room_by_id.get(self.active_run.room_id)

    @property
    def native_resume_pending(self) -> bool:
        """Return whether a retained native task is waiting to resume."""
        return bool(
            (self.session and self.session.native_resume_guard_latched)
            or (
                self.retained_task_guard
                and self.retained_task_guard.owner
                == RETAINED_TASK_OWNER_COORDINATOR
                and self.retained_task_guard.phase
                == RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
            )
            or (
                self.active_run
                and self.active_run.phase in NATIVE_RESUME_PENDING_PHASES
            )
        )

    @property
    def native_resume_attributes(self) -> dict[str, Any]:
        """Return recovery details shared by coordinator entities."""
        run = self.active_run
        session = self.session
        return {
            "native_resume_pending": self.native_resume_pending,
            "phase": (
                run.phase
                if run
                else (
                    RUN_PHASE_RECOVERY_STALLED
                    if self.session and self.session.native_resume_guard_latched
                    else None
                )
            ),
            "suspended_at": run.suspended_at if run else None,
            "suspend_reason": run.suspend_reason if run else None,
            "resume_source": run.resume_source if run else None,
            "interruption_count": run.interruption_count if run else 0,
            "native_resume_observed": (
                run.resumed_after_suspend if run else False
            ),
            "resumable_latched": run.resumable_latched if run else False,
            "recovery_deadline": run.recovery_deadline if run else None,
            "requested_iterations": run.requested_iterations if run else None,
            "observed_iteration_count": (
                run.observed_iteration_count if run else 0
            ),
            "iteration_evidence_source": (
                run.iteration_evidence_source if run else None
            ),
            "command_published": run.command_published if run else False,
            "command_publish_requested_at": (
                run.command_publish_requested_at if run else None
            ),
            "command_publish_acknowledged_at": (
                run.command_publish_acknowledged_at if run else None
            ),
            "start_confirmed_at": run.start_confirmed_at if run else None,
            "cancel_stop_attempts": run.cancel_stop_attempts if run else 0,
            "cancel_stop_requested_at": (
                run.cancel_stop_requested_at if run else None
            ),
            "cancel_stop_published_at": (
                run.cancel_stop_published_at if run else None
            ),
            "cancel_stop_acknowledged_at": (
                run.cancel_stop_acknowledged_at if run else None
            ),
            "cancel_stop_physical_acknowledged_at": (
                run.cancel_stop_physical_acknowledged_at if run else None
            ),
            "cancel_return_attempts": (
                run.cancel_return_attempts if run else 0
            ),
            "cancel_return_requested_at": (
                run.cancel_return_requested_at if run else None
            ),
            "cancel_return_acknowledged_at": (
                (
                    run.cancel_return_service_acknowledged_at
                    or run.cancel_return_state_acknowledged_at
                    or run.cancel_return_acknowledged_at
                )
                if run
                else None
            ),
            "cancel_return_service_acknowledged_at": (
                run.cancel_return_service_acknowledged_at if run else None
            ),
            "cancel_return_state_acknowledged_at": (
                run.cancel_return_state_acknowledged_at if run else None
            ),
            "floor_completion_status": (
                run.floor_completion_status if run else None
            ),
            "floor_completion_reason": (
                run.floor_completion_reason if run else None
            ),
            "blocker_code": session.blocker_code if session else None,
            "blocker_disposition": (
                session.blocker_disposition if session else None
            ),
            "blocker_operator_action": (
                session.blocker_operator_action if session else None
            ),
            "recovery_phase": session.recovery_phase if session else None,
            "next_retry_at": session.next_retry_at if session else None,
            "waiting_for_physical_fix": (
                session.waiting_for_physical_fix if session else False
            ),
            "preserved_rooms": self.preserved_room_ids,
            "resume_nudge_enabled": bool(
                self.config.get(CONF_RESUME_NUDGE_ENABLED, False)
            ),
            **self.retained_task_attributes,
        }

    @property
    def retained_task_attributes(self) -> dict[str, Any]:
        """Return persisted retained-task ownership and recovery diagnostics."""
        guard = self.retained_task_guard
        last_material = (
            parse_datetime(guard.last_material_activity_at) if guard else None
        )
        stale_age = (
            max(0, int((dt_util.utcnow() - last_material).total_seconds()))
            if last_material
            else None
        )
        return {
            "retained_task_owner": guard.owner if guard else None,
            "retained_task_phase": guard.phase if guard else None,
            "retained_task_reason": guard.reason if guard else None,
            "retained_task_first_observed_at": (
                guard.first_observed_at if guard else None
            ),
            "retained_task_last_material_activity_at": (
                guard.last_material_activity_at if guard else None
            ),
            "retained_task_stale_age_seconds": stale_age,
            "retained_task_coherent_since": (
                guard.coherent_since if guard else None
            ),
            "retained_task_stale_deadline": (
                guard.stale_deadline if guard else None
            ),
            "retained_task_observation_deadline": (
                guard.observation_deadline if guard else None
            ),
            "retained_task_clear_attempts": (
                guard.clear_attempts if guard else 0
            ),
            "retained_task_clear_requested_at": (
                guard.clear_requested_at if guard else None
            ),
            "retained_task_clear_published_at": (
                guard.clear_published_at if guard else None
            ),
            "retained_task_clear_deadline": (
                guard.clear_deadline if guard else None
            ),
            "retained_task_clear_acknowledged_at": (
                guard.clear_acknowledged_at if guard else None
            ),
            "retained_task_dock_clear_attempts": (
                guard.dock_clear_attempts if guard else 0
            ),
            "retained_task_dock_clear_requested_at": (
                guard.dock_clear_requested_at if guard else None
            ),
            "retained_task_dock_clear_published_at": (
                guard.dock_clear_published_at if guard else None
            ),
            "retained_task_dock_clear_deadline": (
                guard.dock_clear_deadline if guard else None
            ),
            "retained_task_dock_clear_acknowledged_at": (
                guard.dock_clear_acknowledged_at if guard else None
            ),
            "retained_task_operator_required_reason": (
                guard.operator_required_reason if guard else None
            ),
            "retained_task_auto_clear_enabled": bool(
                self.config.get(
                    CONF_STALE_RESUME_AUTO_CLEAR,
                    DEFAULT_STALE_RESUME_AUTO_CLEAR,
                )
            ),
        }

    @property
    def pending_rooms(self) -> list[RoomConfig]:
        """Return rooms not yet consumed in the current session."""
        if (
            self.session
            and not self.session.active
            and self.session.terminal_reason
        ):
            return []
        if self.session and self.session.active and self.session.degraded_reason:
            return self._degraded_actionable_rooms()
        attempted = set(self.session.attempted_room_ids if self.session else [])
        held_room_ids = self._dispatch_held_room_ids_snapshot()
        attempted.update(held_room_ids)
        auto_clean_day = self._current_auto_clean_day()
        pending: list[RoomConfig] = []
        pending_ids: set[str] = set()

        priority_retry_room_ids = (
            self.session.priority_retry_room_ids if self.session else []
        )
        for room_id in priority_retry_room_ids:
            room = self.room_by_id.get(room_id)
            if (
                room
                and room.room_id not in held_room_ids
                and self._room_auto_clean_enabled(room)
                and room.room_id not in pending_ids
            ):
                pending.append(room)
                pending_ids.add(room.room_id)

        pending.extend(
            room
            for room in self._sorted_auto_clean_rooms()
            if self._room_auto_clean_enabled(room)
            and room.room_id not in attempted
            and room.room_id not in pending_ids
            and not room_auto_cleaned_on(
                self.ledgers.get(room.room_id, RoomLedger()), auto_clean_day
            )
        )
        pending_ids.update(room.room_id for room in pending)
        retry_room_ids = self.session.retry_room_ids if self.session else []
        for room_id in retry_room_ids:
            room = self.room_by_id.get(room_id)
            if (
                room
                and room.room_id not in held_room_ids
                and self._room_auto_clean_enabled(room)
                and room.room_id not in pending_ids
            ):
                pending.append(room)
                pending_ids.add(room.room_id)
        return pending

    @property
    def next_candidate_room_id(self) -> str | None:
        """Return the first room planned after the current blocker clears."""
        selection = self._planned_next_room(
            assume_pending_recovery_resolved=True
        )
        return selection.room.room_id if selection else None

    @property
    def navigation_error_return_enabled(self) -> bool:
        """Return whether target-unreachable errors may request one dock return."""
        return bool(
            self.config.get(
                CONF_NAVIGATION_ERROR_RETURN_ENABLED,
                DEFAULT_NAVIGATION_ERROR_RETURN_ENABLED,
            )
        )

    def _projected_room_queues(
        self,
        *,
        assume_pending_recovery_resolved: bool,
        include_refill_priority: bool = False,
    ) -> tuple[set[str], list[str], list[str]]:
        """Return local attempted and retry queues for side-effect-free planning."""
        session = self.session
        if not session:
            return set(), [], []
        attempted = set(session.attempted_room_ids)
        attempted.update(self._dispatch_held_room_ids_snapshot())
        retry_room_ids = list(session.retry_room_ids)
        priority_retry_room_ids = list(session.priority_retry_room_ids)
        if include_refill_priority:
            for room_id in self._refilled_normal_retry_room_ids():
                if room_id in retry_room_ids:
                    retry_room_ids.remove(room_id)
                if room_id not in priority_retry_room_ids:
                    priority_retry_room_ids.append(room_id)
        recovery_room_id = session.pending_recovery_room_id
        if (
            assume_pending_recovery_resolved
            and recovery_room_id
            and session.can_retry_room(recovery_room_id)
            and (room := self.room_by_id.get(recovery_room_id))
            and self._room_auto_clean_enabled(room)
        ):
            if session.pending_recovery_priority:
                priority_retry_room_ids.append(recovery_room_id)
            else:
                retry_room_ids.append(recovery_room_id)
        return attempted, retry_room_ids, priority_retry_room_ids

    def _planned_normal_room(
        self,
        resources: ResourceState,
        *,
        assume_pending_recovery_resolved: bool,
        include_refill_priority: bool = False,
    ) -> tuple[RoomSelection | None, list[tuple[RoomConfig, str]]]:
        """Return the normal-lane selection using the dispatch scheduler."""
        attempted, retry_room_ids, priority_retry_room_ids = (
            self._projected_room_queues(
                assume_pending_recovery_resolved=(
                    assume_pending_recovery_resolved
                ),
                include_refill_priority=include_refill_priority,
            )
        )
        return select_next_room(
            self._auto_clean_rooms(),
            self.ledgers,
            attempted,
            resources,
            False,
            self._current_auto_clean_day(),
            retry_room_ids,
            priority_retry_room_ids,
        )

    def _planned_degraded_room(
        self,
        resources: ResourceState,
        *,
        assume_pending_recovery_resolved: bool,
    ) -> RoomSelection | None:
        """Return the degraded-lane selection in actual dispatch order."""
        session = self.session
        if not session:
            return None
        attempted, retry_room_ids, _priority_retry_room_ids = (
            self._projected_room_queues(
                assume_pending_recovery_resolved=(
                    assume_pending_recovery_resolved
                ),
            )
        )
        held_room_ids = self._dispatch_held_room_ids_snapshot()
        fallback_attempted = set(session.fallback_attempted_room_ids)
        fallback_attempted.update(held_room_ids)
        completed = set(session.completed_room_ids)
        auto_clean_day = self._current_auto_clean_day()
        ordered = self._sorted_auto_clean_rooms()
        for room in ordered:
            if (
                not room.mop_required
                and room.room_id not in attempted
                and not room_auto_cleaned_on(
                    self.ledgers.get(room.room_id, RoomLedger()),
                    auto_clean_day,
                )
            ):
                return RoomSelection(room=room, vacuum_only=True)

        current_water_reason = clean_water_vacuum_only_reason(resources)
        if (
            current_water_reason
            and self.config.get(CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED)
        ):
            for room in ordered:
                if (
                    room.mop_required
                    and room.room_id not in completed
                    and room.room_id not in fallback_attempted
                    and not room_auto_cleaned_on(
                        self.ledgers.get(room.room_id, RoomLedger()),
                        auto_clean_day,
                    )
                ):
                    return RoomSelection(
                        room=room,
                        vacuum_only=True,
                        mop_block_reason=current_water_reason,
                        fallback_vacuum=True,
                    )

        for room_id in retry_room_ids:
            room = self.room_by_id.get(room_id)
            if (
                room
                and not room.mop_required
                and room_id not in held_room_ids
                and self._room_auto_clean_enabled(room)
                and not room_auto_cleaned_on(
                    self.ledgers.get(room_id, RoomLedger()),
                    auto_clean_day,
                )
            ):
                return RoomSelection(room=room, vacuum_only=True)
        return None

    def _planned_next_room(
        self,
        *,
        assume_pending_recovery_resolved: bool,
    ) -> RoomSelection | None:
        """Return the same room projection consumed by dispatch."""
        if not self.session or not self.session.active:
            return None
        resources = self._resource_state()
        if (
            assume_pending_recovery_resolved
            and self.session.pending_recovery_room_id
        ):
            resources = replace(resources, error="No error")
        resource_blocker = classify_blocker(resources)
        if (
            self.session.degraded_reason
            and resource_blocker
            and resource_blocker.vacuum_only_safe
        ):
            return self._planned_degraded_room(
                resources,
                assume_pending_recovery_resolved=(
                    assume_pending_recovery_resolved
                ),
            )
        if self.session.degraded_reason and resource_blocker is None:
            native_room = self._next_degraded_native_room()
            if native_room:
                return RoomSelection(room=native_room, vacuum_only=True)
        selection, _skipped = self._planned_normal_room(
            resources,
            assume_pending_recovery_resolved=(
                assume_pending_recovery_resolved
            ),
            include_refill_priority=bool(
                self.session.degraded_reason
                and resource_blocker is None
            ),
        )
        return selection

    @property
    def while_away_cleaned_messages(self) -> list[str]:
        """Return retained while-away cleaned messages for the current day."""
        cleaned, _issues = build_while_away_messages(
            self.while_away_outcomes,
            {room.room_id: room.name for room in self.rooms},
            self._current_auto_clean_day(),
        )
        return cleaned

    @property
    def while_away_issue_messages(self) -> list[str]:
        """Return retained while-away issue messages for the current day."""
        _cleaned, issues = build_while_away_messages(
            self.while_away_outcomes,
            {room.room_id: room.name for room in self.rooms},
            self._current_auto_clean_day(),
        )
        return issues

    @property
    def while_away_outcome_contract(self) -> dict[str, Any]:
        """Return the versioned typed while-away outcome contract."""
        return build_while_away_outcome_contract(
            self.while_away_outcomes,
            self.room_by_id,
            self._current_auto_clean_day(),
        )

    @property
    def error_state(self) -> str | None:
        """Return the current Valetudo error sensor state."""
        return self._state(self.config.get(CONF_ERROR_ENTITY))

    async def async_start_session(self, reason: str = "auto") -> None:
        """Start a new away cleaning session if possible."""
        async with self._event_lock:
            await self._async_start_session(reason)

    async def _async_start_session(self, reason: str) -> None:
        """Start a new away cleaning session while mutations are serialized."""
        if self.active_run:
            _LOGGER.warning(
                "Not starting %s because an active or cancelling run still exists",
                self.name,
            )
            return
        if self.session and self.session.native_resume_guard_latched:
            _LOGGER.warning(
                "Not starting %s because a retained native task still requires cancellation",
                self.name,
            )
            return
        if (
            self.retained_task_guard
            and self.retained_task_guard.owner
            == RETAINED_TASK_OWNER_COORDINATOR
            and self.retained_task_guard.phase
            == RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
        ):
            _LOGGER.warning(
                "Not starting %s because its owned native task is still retained",
                self.name,
            )
            return
        if self.paused:
            _LOGGER.info("Not starting %s because coordinator is paused", self.name)
            return
        if not self._all_people_away():
            _LOGGER.info("Not starting %s because not all tracked people are away", self.name)
            return
        if self.session and self.session.active:
            await self._async_maybe_start_next_room()
            return

        self._cancel_terminal_cleanup_retry()
        self._cancel_away_timer()
        self._clear_blocked_session_watchdog()
        self._terminal_cleanup_retry_attempts = 0
        self._terminal_settings_restore_deferred = False
        self.session = SessionState(session_id=utcnow_iso(), started_at=utcnow_iso())
        _LOGGER.info("Starting Valetudo away-cleaning session %s (%s)", self.session.session_id, reason)
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_start_next_room()

    async def async_cancel_session(self, reason: str) -> None:
        """Cancel the active away session and active run."""
        async with self._event_lock:
            await self._async_cancel_session(reason)

    async def _async_cancel_session(self, reason: str) -> None:
        """Cancel the active away session while mutations are serialized."""
        self._clear_blocked_session_watchdog()
        had_active_session = bool(self.session and self.session.active)
        had_native_resume_guard = bool(
            self.session and self.session.native_resume_guard_latched
        )
        retained_stop_consumed = bool(
            self.retained_task_guard
            and self.retained_task_guard.clear_attempts >= 1
        )

        if self.session:
            self.session.cancelled = True
            self.session.active = False
            self.session.active_room_id = None
            self.session.pending_recovery_room_id = None
            self.session.pending_recovery_reason = None
            self.session.pending_recovery_priority = False
            self.session.pending_recovery_policy = None
            self.session.retry_room_ids = []
            self.session.priority_retry_room_ids = []
            self.session.terminal_reason = (
                "returned_home" if reason == "Tracked person arrived home" else "cancelled"
            )
            self.session.terminal_message = reason
            if had_native_resume_guard and not self.active_run:
                self.session.native_guard_cancel_pending = True
                self.session.native_guard_cancel_reason = reason
                await self._async_save_store()
                self._notify_listeners()

        if self.active_run:
            self.active_run.cancelled = True
            self.active_run.phase = RUN_PHASE_CANCEL_PENDING
            self.active_run.cancel_requested_at = utcnow_iso()
            self.active_run.cancel_reason = reason
            self.active_run.cancel_continue_session = False
            self._cancel_active_run_timers()
            self.active_run.checkpoint_statistics(
                parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
                parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
            )
            await self._async_save_store()
            self._notify_listeners()
            if not await self._async_execute_cancel_pending():
                return
        elif had_native_resume_guard:
            if not await self._async_execute_native_guard_cancel_pending():
                return
        elif (
            had_active_session
            and not retained_stop_consumed
            and not (
                self.retained_task_guard
                and self.retained_task_guard.owner
                in {
                    RETAINED_TASK_OWNER_MANUAL,
                    RETAINED_TASK_OWNER_UNKNOWN,
                }
            )
        ):
            try:
                await self._async_return_to_dock_or_stop_resumable(
                    reason,
                )
            except Exception:
                _LOGGER.exception(
                    "Failed to stop %s while cancelling session",
                    self.vacuum_entity,
                )
                if self.session:
                    self.session.terminal_reason = "needs_help"
                    self.session.terminal_message = (
                        "Vacuum command may still be active after cancellation failed"
                    )
                    self.session.needs_help = True
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    async def _async_execute_cancel_pending(self) -> bool:
        """Execute one restart-safe acknowledged cancellation for an active run."""
        run = self.active_run
        if not run or run.phase != RUN_PHASE_CANCEL_PENDING:
            return True

        reason = run.cancel_reason or "Cancelled"
        if (
            (run.command_published or run.command_publish_requested_at)
            and not run.cancel_stop_acknowledged_at
        ):
            if run.cancel_ack_deadline is None:
                run.cancel_ack_deadline = (
                    dt_util.utcnow()
                    + timedelta(
                        seconds=int(
                            self.config.get(
                                CONF_DISPATCH_START_TIMEOUT,
                                DEFAULT_DISPATCH_START_TIMEOUT,
                            )
                        )
                    )
                ).isoformat()
                await self._async_save_store()
            self._schedule_cancel_ack_timeout()
        if not run.command_published and not run.command_publish_requested_at:
            run.cancel_stop_acknowledged_at = (
                run.cancel_stop_acknowledged_at or utcnow_iso()
            )
        elif (
            run.cancel_stop_attempts >= 1
            and not run.cancel_stop_acknowledged_at
            and self._cancel_stop_physically_acknowledged()
        ):
            run.cancel_stop_physical_acknowledged_at = utcnow_iso()
            run.cancel_stop_acknowledged_at = (
                run.cancel_stop_physical_acknowledged_at
            )
            run.cancel_ack_deadline = None
            self._cancel_cancel_ack_timeout()
            await self._async_save_store()
        elif run.cancel_stop_attempts == 0:
            now = dt_util.utcnow()
            run.cancel_stop_attempts = 1
            run.cancel_stop_attempted = True
            run.cancel_stop_requested_at = now.isoformat()
            run.cancel_stop_published_at = now.isoformat()
            run.cancel_ack_deadline = (
                now
                + timedelta(
                    seconds=int(
                        self.config.get(
                            CONF_DISPATCH_START_TIMEOUT,
                            DEFAULT_DISPATCH_START_TIMEOUT,
                        )
                    )
                )
            ).isoformat()
            await self._async_save_store()
            self._notify_listeners()
            try:
                await self.hass.services.async_call(
                    "vacuum",
                    "stop",
                    {ATTR_ENTITY_ID: self.vacuum_entity},
                    blocking=True,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception(
                    "The one stop attempt for %s did not return an acknowledgement",
                    self.vacuum_entity,
                )
                if self.session:
                    retry_at = parse_datetime(run.cancel_ack_deadline)
                    self.session.enter_recovery(
                        code="command.stop_ack_pending",
                        disposition=BLOCKER_RECOVERABLE,
                        reason=(
                            "The vacuum.stop delivery is uncertain after "
                            f"cancelling for {reason}: {err}"
                        ),
                        phase="operator_required",
                        next_retry_at=(
                            retry_at.isoformat() if retry_at else None
                        ),
                        waiting_for_physical_fix=False,
                        operator_action=(
                            "Wait for the robot state to confirm that the "
                            "single stop attempt took effect."
                        ),
                    )
                await self._async_save_store()
                self._notify_listeners()
                self._schedule_cancel_ack_timeout()
                return False
            run.cancel_stop_acknowledged_at = utcnow_iso()
            run.cancel_ack_deadline = None
            self._cancel_cancel_ack_timeout()
            await self._async_save_store()

        if not run.cancel_stop_acknowledged_at:
            if self.session:
                retry_at = parse_datetime(run.cancel_ack_deadline)
                self.session.enter_recovery(
                    code="command.stop_ack_pending",
                    disposition=BLOCKER_RECOVERABLE,
                    reason=(
                        "Waiting for physical acknowledgement of the one "
                        f"vacuum.stop attempt made for {reason}"
                    ),
                    phase="operator_required",
                    next_retry_at=retry_at.isoformat() if retry_at else None,
                    waiting_for_physical_fix=False,
                    operator_action=(
                        "Wait for the robot state to confirm that the single "
                        "stop attempt took effect."
                    ),
                )
            await self._async_save_store()
            self._notify_listeners()
            self._schedule_cancel_ack_timeout()
            return False

        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if (
            run.cancel_return_attempts >= 1
            and not run.return_acknowledged
        ):
            if (
                vacuum_state in _MOVING_VACUUM_STATES
                or (
                    run.cancel_recover_room
                    and vacuum_state == "error"
                )
                or vacuum_state not in _AT_DOCK_VACUUM_STATES
            ):
                if self.session:
                    self.session.enter_recovery(
                        code="command.return_ack_pending",
                        disposition=BLOCKER_RECOVERABLE,
                        reason=(
                            "Waiting for physical acknowledgement of the one "
                            "return-to-base attempt"
                        ),
                        phase="operator_required",
                        next_retry_at=run.cancel_ack_deadline,
                        waiting_for_physical_fix=False,
                        operator_action=(
                            "Leave the robot reachable so its return state can "
                            "be confirmed."
                        ),
                        retry_cadence_reason="command_ack_recheck",
                    )
                await self._async_save_store()
                self._notify_listeners()
                self._schedule_cancel_ack_timeout()
                return False
            run.cancel_return_state_acknowledged_at = utcnow_iso()
            run.cancel_ack_deadline = None
            self._cancel_cancel_ack_timeout()
            await self._async_save_store()
        if (
            (
                vacuum_state in _MOVING_VACUUM_STATES
                and run.cancel_return_attempts == 0
            )
            or self._navigation_error_return_allowed(run, vacuum_state)
        ):
            run.cancel_return_attempts = 1
            run.cancel_return_requested_at = utcnow_iso()
            run.cancel_ack_deadline = (
                dt_util.utcnow()
                + timedelta(
                    seconds=int(
                        self.config.get(
                            CONF_DISPATCH_START_TIMEOUT,
                            DEFAULT_DISPATCH_START_TIMEOUT,
                        )
                    )
                )
            ).isoformat()
            await self._async_save_store()
            self._schedule_cancel_ack_timeout()
            try:
                await self.hass.services.async_call(
                    "vacuum",
                    "return_to_base",
                    {ATTR_ENTITY_ID: self.vacuum_entity},
                    blocking=True,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception(
                    "The one return-to-base attempt for %s was not acknowledged",
                    self.vacuum_entity,
                )
                if self.session:
                    self.session.enter_recovery(
                        code="command.return_ack_pending",
                        disposition=BLOCKER_RECOVERABLE,
                        reason=(
                            "The vacuum stopped, but return-to-base acknowledgement "
                            f"is uncertain: {err}"
                        ),
                        phase="operator_required",
                        next_retry_at=run.cancel_ack_deadline,
                        waiting_for_physical_fix=False,
                        operator_action=(
                            "Leave the robot reachable so its return state can "
                            "be confirmed."
                        ),
                        retry_cadence_reason="command_ack_recheck",
                    )
                await self._async_save_store()
                self._notify_listeners()
                self._schedule_cancel_ack_timeout()
                return False
            run.cancel_return_service_acknowledged_at = utcnow_iso()
            run.cancel_ack_deadline = None
            self._cancel_cancel_ack_timeout()
            await self._async_save_store()

        if run.room_id:
            ledger = self.ledgers.setdefault(run.room_id, RoomLedger())
            when = utcnow_iso()
            mark_failure(ledger, when, reason)
            if self.session:
                if run.cancel_continue_session and run.cancel_outcome_result == "uncertain":
                    self.session.mark_uncertain(run.room_id, reason)
                    self._record_attempt_outcome(
                        kind="failed",
                        run=run,
                        result="uncertain",
                        reason=reason,
                        occurred_at=when,
                    )
                elif run.cancel_continue_session and run.cancel_requeue_room:
                    self.session.unmark_attempted(run.room_id)
                    if run.dispatch_failure_code:
                        self._record_dispatch_failure(run, reason)
                    if run.fallback_vacuum:
                        if run.room_id in self.session.fallback_attempted_room_ids:
                            self.session.fallback_attempted_room_ids.remove(run.room_id)
                    self._record_attempt_outcome(
                        kind="failed",
                        run=run,
                        result="interrupted",
                        reason=reason,
                        occurred_at=when,
                    )
                elif run.cancel_continue_session and run.cancel_recover_room:
                    retry_eligible = self.session.can_retry_room(run.room_id)
                    run.cancel_outcome_result = (
                        "interrupted" if retry_eligible else "failed"
                    )
                    self.session.mark_failed(run.room_id, reason)
                    self.session.begin_recovering(
                        run.room_id,
                        reason,
                        policy=_NAVIGATION_RETRY_LATER_POLICY,
                    )
                    self._record_attempt_outcome(
                        kind="failed",
                        run=run,
                        result=run.cancel_outcome_result,
                        reason=reason,
                        occurred_at=when,
                    )
                elif run.fallback_vacuum:
                    self.session.mark_fallback_failed(run.room_id, reason)
                else:
                    self.session.mark_failed(run.room_id, reason)
                if self.session.terminal_reason in {
                    "returned_home",
                    "cancelled",
                }:
                    self._record_attempt_outcome(
                        kind="failed",
                        run=run,
                        result="interrupted",
                        reason=reason,
                        occurred_at=when,
                    )
        if self.session:
            self.session.native_resume_guard_latched = False
            self.session.last_command_recovery = {
                "room_id": run.room_id,
                "reason": reason,
                "command_publish_requested_at": (
                    run.command_publish_requested_at
                ),
                "command_publish_acknowledged_at": (
                    run.command_publish_acknowledged_at
                ),
                "stop_attempts": run.cancel_stop_attempts,
                "stop_requested_at": run.cancel_stop_requested_at,
                "stop_published_at": run.cancel_stop_published_at,
                "stop_acknowledged_at": (
                    run.cancel_stop_acknowledged_at
                ),
                "stop_physical_acknowledged_at": (
                    run.cancel_stop_physical_acknowledged_at
                ),
                "return_attempts": run.cancel_return_attempts,
                "return_requested_at": run.cancel_return_requested_at,
                "return_service_acknowledged_at": (
                    run.cancel_return_service_acknowledged_at
                ),
                "return_state_acknowledged_at": (
                    run.cancel_return_state_acknowledged_at
                ),
                "return_legacy_acknowledged_at": (
                    run.cancel_return_acknowledged_at
                ),
                "return_recovery_confirmed_at": None,
                "outcome": run.cancel_outcome_result,
                "recovery_policy": (
                    "recover_then_retry_later"
                    if run.cancel_recover_room
                    else (
                        "requeue_same"
                        if run.cancel_requeue_room
                        else None
                    )
                ),
                "retry_eligible": bool(
                    run.cancel_recover_room
                    and run.room_id
                    and self.session.can_retry_room(run.room_id)
                ),
                "dispatch_failure_code": run.dispatch_failure_code,
                "dispatch_failure_count": (
                    self.session.dispatch_failure_counts.get(
                        run.room_id or "",
                        0,
                    )
                ),
            }
        self._clear_active_run()
        if self.session and self.session.last_command_recovery:
            self.session.last_command_recovery["next_candidate_room"] = (
                self.next_candidate_room_id
            )
        return True

    def _cancel_stop_physically_acknowledged(self) -> bool:
        """Return whether robot state proves the pending command was cleared."""
        vacuum_state = (
            normalize_state(self._state(self.vacuum_entity)) or ""
        ).lower()
        return bool(
            vacuum_state not in _MOVING_VACUUM_STATES
            and self._status_flag() not in {"segment", "resumable"}
        )

    async def _async_execute_native_guard_cancel_pending(self) -> bool:
        """Retry durable cancellation of a latched native task."""
        session = self.session
        if not session or not session.native_guard_cancel_pending:
            return True

        if not session.native_guard_stop_confirmed:
            try:
                await self.hass.services.async_call(
                    "vacuum",
                    "stop",
                    {ATTR_ENTITY_ID: self.vacuum_entity},
                    blocking=True,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception(
                    "Failed to stop latched native task for %s",
                    self.vacuum_entity,
                )
                session.terminal_reason = "needs_help"
                session.terminal_message = (
                    f"Native task may still be active after cancellation failed: {err}"
                )
                session.needs_help = True
                await self._async_save_store()
                self._notify_listeners()
                return False
            session.native_guard_stop_confirmed = True
            await self._async_save_store()

        if (
            normalize_state(self._state(self.vacuum_entity))
            in _MOVING_VACUUM_STATES
            and not session.native_guard_return_confirmed
        ):
            try:
                await self.hass.services.async_call(
                    "vacuum",
                    "return_to_base",
                    {ATTR_ENTITY_ID: self.vacuum_entity},
                    blocking=True,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception(
                    "Failed to return latched native task for %s",
                    self.vacuum_entity,
                )
                session.terminal_reason = "needs_help"
                session.terminal_message = (
                    f"Native task stopped but could not return to base: {err}"
                )
                session.needs_help = True
                await self._async_save_store()
                self._notify_listeners()
                return False
            session.native_guard_return_confirmed = True
            await self._async_save_store()

        session.native_resume_guard_latched = False
        session.native_guard_cancel_pending = False
        session.native_guard_stop_confirmed = False
        session.native_guard_return_confirmed = False
        session.native_guard_cancel_reason = None
        self._clear_retained_task_guard()
        return True

    async def async_set_paused(self, paused: bool, reason: str | None = None) -> None:
        """Pause or resume automatic cleaning behavior."""
        async with self._event_lock:
            await self._async_set_paused(paused, reason)

    async def _async_set_paused(self, paused: bool, reason: str | None) -> None:
        """Update pause state while mutations are serialized."""
        self.paused = paused
        self.pause_reason = reason if paused else None
        self._cancel_away_timer()
        if paused:
            await self._async_cancel_session(reason or "paused")
        else:
            if self._all_people_away() and self.away_since is None:
                self.away_since = self._latest_person_away_since()
            self._schedule_away_timer_if_needed()
        await self._async_save_store()
        self._notify_listeners()

    async def async_mark_room_cleaned(self, room_id: str, *, mop: bool, vacuum: bool = True) -> None:
        """Manually mark a room as cleaned."""
        async with self._event_lock:
            self._require_room(room_id)
            ledger = self.ledgers.setdefault(room_id, RoomLedger())
            mark_success(ledger, utcnow_iso(), mop=mop, vacuum=vacuum)
            if self.session:
                self.session.clear_dispatch_failure(room_id)
            await self._async_save_store()
            self._notify_listeners()

    async def async_reset_room(self, room_id: str) -> None:
        """Reset one room ledger."""
        async with self._event_lock:
            self._require_room(room_id)
            self.ledgers[room_id] = RoomLedger()
            await self._async_save_store()
            self._notify_listeners()

    def is_room_auto_clean_disabled(self, room_id: str) -> bool:
        """Return whether a room is disabled for away auto-clean sessions."""
        self._require_room(room_id)
        return room_id in self.disabled_room_ids

    async def async_set_room_auto_clean_disabled(self, room_id: str, disabled: bool) -> None:
        """Enable or disable one room for automatic away clean selection."""
        async with self._event_lock:
            await self._async_set_room_auto_clean_disabled(room_id, disabled)

    async def _async_set_room_auto_clean_disabled(
        self,
        room_id: str,
        disabled: bool,
    ) -> None:
        """Update one room's auto-clean eligibility while mutations are serialized."""
        self._require_room(room_id)
        was_disabled = room_id in self.disabled_room_ids
        if disabled == was_disabled:
            return

        if disabled:
            self.disabled_room_ids.add(room_id)
        else:
            self.disabled_room_ids.discard(room_id)

        await self._async_save_store()
        self._notify_listeners()
        if self.session and self.session.active and not self.active_run:
            await self._async_maybe_start_next_room()

    @callback
    def _handle_state_change_event(self, event: Event) -> None:
        """Schedule handling for HA state changes."""
        self.hass.async_create_task(self._async_handle_state_change_event(event))

    @callback
    def _handle_delayed_restore_reconcile(self, _now: datetime) -> None:
        """Re-check restored active runs after HA entities have settled."""
        self.hass.async_create_task(self._async_reconcile_restored_session_serialized())

    async def _async_reconcile_restored_session_serialized(self) -> None:
        """Reconcile restored state without racing live entity events."""
        async with self._event_lock:
            await self._async_reconcile_restored_session()

    async def _async_handle_state_change_event(self, event: Event) -> None:
        """Handle a monitored Home Assistant state change."""
        async with self._event_lock:
            await self._async_process_state_change_event(event)

    async def _async_process_state_change_event(self, event: Event) -> None:
        """Process one monitored state change while event handling is serialized."""
        entity_id = event.data.get("entity_id")
        new_state: State | None = event.data.get("new_state")
        if new_state is None:
            return

        if self.session and self.session.native_guard_cancel_pending:
            completed = await self._async_execute_native_guard_cancel_pending()
            if completed:
                await self._async_save_store()
                self._notify_listeners()
                if entity_id in self.people_entities:
                    await self._async_handle_presence_change()
                await self._async_maybe_send_auto_clean_summary()
            return

        if (
            self.active_run
            and self.active_run.phase == RUN_PHASE_CANCEL_PENDING
        ):
            continue_session = self.active_run.cancel_continue_session
            completed = await self._async_execute_cancel_pending()
            if completed:
                await self._async_save_store()
                self._notify_listeners()
                if entity_id in self.people_entities:
                    await self._async_handle_presence_change()
                if (
                    continue_session
                    and self.session
                    and self.session.active
                    and self._all_people_away()
                ):
                    await self._async_maybe_start_next_room()
                else:
                    await self._async_maybe_send_auto_clean_summary()
            return

        now = dt_util.utcnow()
        if self._restore_active_run_observations(now):
            await self._async_save_store()
            self._notify_listeners()
        if entity_id in self.people_entities:
            await self._async_handle_presence_change()
            return

        observations_changed = self._observe_active_run(entity_id, new_state, now)
        observations_changed = (
            self._observe_manual_run(entity_id, new_state, now)
            or observations_changed
        )
        observations_changed = (
            self._observe_retained_task(entity_id, new_state, now)
            or observations_changed
        )
        if observations_changed:
            await self._async_save_store()
            self._notify_listeners()

        if (
            entity_id in self._retained_task_sensor_entities()
            and not self.active_run
        ):
            await self._async_reconcile_retained_task(
                now,
                allow_clear=bool(self.session and self.session.active),
            )

        if (
            self.active_run
            and self._run_waiting_for_start_ack(self.active_run)
            and entity_id
            in {
                self.config.get(CONF_BATTERY_ENTITY),
                self.config.get(CONF_DOCK_STATUS_ENTITY),
                self.config.get(CONF_ERROR_ENTITY),
                self.config.get(CONF_STATUS_FLAG_ENTITY),
            }
            and (pending_blocker := self._current_blocker()) is not None
            and not self._active_run_allows_current_blocker()
        ):
            await self._async_handle_active_run_error(
                pending_blocker.reason
            )
            return

        if (
            entity_id == self.config.get(CONF_ERROR_ENTITY)
            and not is_error_clear(new_state.state)
        ):
            if normalize_state(self._state(entity_id)) != normalize_state(
                new_state.state
            ):
                return
            self.last_error = new_state.state
            if self.active_run:
                if self._active_run_allows_current_blocker():
                    await self._async_reconcile_active_run(now)
                else:
                    await self._async_handle_active_run_error(new_state.state)
            else:
                blocker = classify_blocker(self._resource_state())
                recovery_room_id, recovery_reason = self._recovering_room_for_error()
                if recovery_room_id and not (
                    is_low_battery_error(new_state.state)
                    and is_low_battery_error(recovery_reason)
                ):
                    await self._async_handle_pending_recovery_error(new_state.state)
                elif self.session and self.session.active:
                    degraded_reason = clean_water_vacuum_only_reason(
                        self._resource_state()
                    ) or (
                        blocker.reason
                        if blocker and blocker.vacuum_only_safe
                        else None
                    )
                    if degraded_reason:
                        self._activate_vacuum_only_degraded(degraded_reason)
                        await self._async_maybe_start_next_room()
                    elif blocker and not blocker.recoverable:
                        await self._async_terminalize_unrecoverable(blocker)
                    elif blocker:
                        await self._async_arm_blocked_session_watchdog(
                            blocker.reason
                        )
                    else:
                        await self._async_maybe_start_next_room()
                else:
                    self._notify_listeners()
            return

        if entity_id in {
            self.vacuum_entity,
            self.config.get(CONF_STATUS_FLAG_ENTITY),
            self.config.get(CONF_DOCK_STATUS_ENTITY),
            self.config.get(CONF_BATTERY_ENTITY),
            self.config.get(CONF_ERROR_ENTITY),
        } and self._active_run_restored:
            if await self._async_reconcile_restored_active_run():
                return

        if entity_id == self.vacuum_entity:
            if normalize_state(self._state(entity_id)) != normalize_state(
                new_state.state
            ):
                return
            await self._async_handle_vacuum_state(new_state.state, now)
            return
        if entity_id == self.config.get(CONF_DOCK_STATUS_ENTITY):
            if self.active_run:
                await self._async_reconcile_active_run(now)
                return
            await self._async_maybe_start_next_room()
            await self._async_maybe_send_auto_clean_summary()
            return
        if entity_id == self.config.get(CONF_BATTERY_ENTITY):
            if self.active_run:
                await self._async_reconcile_active_run(now)
                return
            await self._async_maybe_start_next_room()
            await self._async_maybe_send_auto_clean_summary()
            return
        if entity_id == self.config.get(CONF_ERROR_ENTITY):
            if self.active_run:
                await self._async_reconcile_active_run(now)
                return
            await self._async_maybe_start_next_room()
            await self._async_maybe_send_auto_clean_summary()
            self._notify_listeners()
            return
        if entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY):
            if self.active_run:
                await self._async_reconcile_active_run(now)
                return
            await self._async_maybe_start_next_room()
            await self._async_maybe_send_auto_clean_summary()
            return
        if entity_id in self._resource_sensor_entities():
            if self.active_run and self._resource_sample_is_blocking(
                entity_id,
                new_state.state,
            ):
                if not self._run_waiting_for_start_ack(self.active_run):
                    if not await self._async_resource_fault_confirmed(
                        entity_id,
                        now,
                    ):
                        return
            elif not self._resource_sample_is_blocking(
                entity_id,
                new_state.state,
            ):
                await self._async_resource_fault_confirmed(entity_id, now)

            blocker = classify_blocker(self._resource_state())
            if self.active_run and blocker:
                await self._async_handle_active_run_error(blocker.reason)
                return
            if self.active_run:
                if (
                    self.session
                    and self.session.active
                    and self.session.blocker_code
                ):
                    await self._async_clear_recoverable_wait()
                await self._async_reconcile_active_run(now)
                return
            await self._async_maybe_start_next_room()
            await self._async_maybe_send_auto_clean_summary()
            self._notify_listeners()
            return
        if entity_id == self.config.get(CONF_MODE_ENTITY):
            if self.session and self.session.active and not self.active_run:
                await self._async_maybe_start_next_room()
            self._notify_listeners()

    async def _async_handle_presence_change(self) -> None:
        """React to someone leaving or arriving."""
        if self._all_people_away():
            if self.away_since is None:
                self.away_since = self._latest_person_away_since()
                await self._async_save_store()
            if self.session and self.session.active and not self.active_run:
                await self._async_maybe_start_next_room()
                return
            self._schedule_away_timer_if_needed()
            return

        self.away_since = None
        self._cancel_away_timer()
        if (
            self.session
            and (
                self.session.active
                or self.session.native_resume_guard_latched
            )
            and self.config.get(CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL)
        ):
            await self._async_cancel_session("Tracked person arrived home")
        else:
            await self._async_save_store()

    def _schedule_away_timer_if_needed(self) -> None:
        """Schedule the configured away grace period."""
        if self.paused or not self._all_people_away():
            return
        if self.active_run:
            return
        if self.session and self.session.native_resume_guard_latched:
            return
        if (
            self.retained_task_guard
            and self.retained_task_guard.owner
            == RETAINED_TASK_OWNER_COORDINATOR
            and self.retained_task_guard.phase
            == RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
        ):
            return
        if self.session and self.session.active:
            return
        if self._terminal_session_is_from_current_away_period():
            self._cancel_away_timer()
            return
        if self._away_timer_cancel is not None:
            return

        remaining_delay = self._remaining_away_delay_seconds()
        if remaining_delay is None:
            return
        if remaining_delay <= 0:
            schedule_hass_task(self.hass, self.async_start_session(reason="away delay already elapsed"))
            return

        def timer_finished(_now: datetime) -> None:
            self._away_timer_cancel = None
            schedule_hass_task(self.hass, self.async_start_session(reason="away timer"))

        self._away_timer_cancel = async_call_later(self.hass, remaining_delay, timer_finished)
        self._notify_listeners()

    def _remaining_away_delay_seconds(self) -> int | None:
        """Return seconds left before the tracked people have been away long enough."""
        away_delay = int(self.config.get("away_delay", 300))
        away_since = parse_datetime(self.away_since)
        if away_since is None:
            return away_delay

        elapsed = (dt_util.utcnow() - away_since).total_seconds()
        return max(0, int(away_delay - elapsed))

    def _terminal_session_is_from_current_away_period(self) -> bool:
        """Return whether a non-restartable terminal session belongs to this away cycle."""
        if (
            not self.session
            or self.session.active
            or not self.session.terminal_reason
        ):
            return False
        away_since = parse_datetime(self.away_since)
        session_started = parse_datetime(self.session.started_at)
        return bool(
            away_since
            and session_started
            and session_started >= away_since
        )

    def _cancel_away_timer(self) -> None:
        """Cancel pending away timer."""
        if self._away_timer_cancel is not None:
            self._away_timer_cancel()
            self._away_timer_cancel = None

    async def _async_handle_vacuum_state(self, vacuum_state: str, now: datetime) -> None:
        """React to vacuum entity state."""
        normalized_state = normalize_state(vacuum_state)
        if normalized_state == "paused" and self.session and self.session.active and not self.active_run:
            await self._async_clear_paused_between_rooms()
            return
        if (
            normalized_state == "error"
            and self.active_run
            and self._configured_error_is_available()
            and not is_error_clear(self.error_state)
        ):
            if self._active_run_allows_current_blocker():
                await self._async_reconcile_active_run(now)
            else:
                await self._async_handle_active_run_error(
                    self.error_state or "Unknown error"
                )
            return

        if normalized_state == "cleaning":
            self._active_run_restored = False
            if self.active_run:
                run = self.active_run
                changed = False
                if not run.observed_cleaning:
                    run.observed_cleaning = True
                    changed = True
                if run.phase == RUN_PHASE_DISPATCHING:
                    run.phase = RUN_PHASE_CLEANING
                    run.start_confirmed_at = now.isoformat()
                    run.dispatch_deadline = None
                    self._cancel_dispatch_start_timeout()
                    if self.session and run.room_id:
                        self.session.clear_dispatch_failure(run.room_id)
                    changed = True
                if (
                    not run.native_resume_pending
                    and self._status_flag() == "segment"
                ):
                    changed = run.observe_segment_iteration(
                        source="cleaning_with_segment",
                        count_new_iteration=True,
                    ) or changed
                if run.native_resume_pending:
                    if (
                        not run.post_suspend_cleaning_observed
                        and self._entity_changed_after(
                            self.vacuum_entity,
                            run.suspended_at,
                        )
                    ):
                        run.post_suspend_cleaning_observed = True
                        changed = True
                    if (
                        self._status_flag() == "segment"
                        and self._entity_changed_after(
                            self.config.get(CONF_STATUS_FLAG_ENTITY),
                            run.suspended_at,
                        )
                    ):
                        changed = run.observe_segment_iteration(
                            source="native_resume_segment",
                            count_new_iteration=False,
                        ) or changed
                        if not run.post_suspend_segment_observed:
                            run.post_suspend_segment_observed = True
                            changed = True
                    changed = self._mark_active_run_resumed(run) or changed
                if run.docked_at is not None:
                    run.docked_at = None
                    changed = True
                estimated_room_id = self._room_id_from_estimated(
                    self._state(self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY))
                )
                if (
                    normalized_state == "cleaning"
                    and estimated_room_id
                    and run.last_estimated_room_id is None
                ):
                    run.observe_estimated_room(estimated_room_id, now)
                    changed = True
                self._cancel_dock_settle_timer()
                if changed:
                    await self._async_save_store()
                    self._notify_listeners()
                if not run.native_resume_pending:
                    await self._async_clear_recoverable_wait()
            else:
                if self.session and (
                    self.session.active
                    or self.session.native_resume_guard_latched
                    or not self.session.notification_sent
                    or self.settings_snapshot is not None
                ):
                    _LOGGER.debug(
                        "Ignoring uncommanded cleaning transition while session %s is retained",
                        self.session.session_id,
                    )
                    await self._async_maybe_send_auto_clean_summary()
                    return
                cleared = self._clear_while_away_after_manual_clean_started()
                manual_started = False
                if self._manual_tracking_allowed():
                    manual_started = self._start_manual_run(now)
                elif cleared:
                    self._notify_listeners()
                if cleared or manual_started:
                    await self._async_save_store()

        if normalized_state == "returning" and self.active_run:
            changed = False
            if not self.active_run.observed_cleaning:
                self.active_run.observed_cleaning = True
                changed = True
            if self.active_run.docked_at is not None:
                self.active_run.docked_at = None
                changed = True
            self._cancel_dock_settle_timer()
            if changed:
                await self._async_save_store()

        if normalized_state in _AT_DOCK_VACUUM_STATES:
            if self.active_run:
                await self._async_reconcile_active_run_at_dock(now)
            elif self.manual_run:
                await self._async_reconcile_retained_task(
                    now,
                    allow_clear=bool(self.session and self.session.active),
                )
            else:
                await self._async_maybe_send_auto_clean_summary()
                await self._async_maybe_start_next_room()

    def _observe_active_run(
        self,
        entity_id: str,
        new_state: State,
        now: datetime,
    ) -> bool:
        """Update active commanded run observations."""
        run = self.active_run
        if not run:
            return False

        changed = False
        state = normalize_state(new_state.state)
        normalized_state = state.lower() if state else None
        if (
            entity_id == self.vacuum_entity
            and normalize_state(self._state(self.vacuum_entity)) != state
        ):
            return False
        if (
            entity_id == self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY)
            and normalize_state(
                self._state(self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY))
            )
            != state
        ):
            return False
        if (
            entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY)
            and self._status_flag() != normalized_state
        ):
            return False
        if (
            entity_id == self.config.get(CONF_DOCK_STATUS_ENTITY)
            and normalize_state(
                self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
            )
            != state
        ):
            return False
        if entity_id == self.vacuum_entity and normalized_state == "cleaning":
            if not run.observed_cleaning:
                run.observed_cleaning = True
                changed = True
            if run.phase == RUN_PHASE_DISPATCHING:
                run.phase = RUN_PHASE_CLEANING
                run.start_confirmed_at = now.isoformat()
                run.dispatch_deadline = None
                self._cancel_dispatch_start_timeout()
                if self.session and run.room_id:
                    self.session.clear_dispatch_failure(run.room_id)
                changed = True
            if (
                not run.native_resume_pending
                and self._status_flag() == "segment"
            ):
                changed = run.observe_segment_iteration(
                    source="cleaning_with_segment",
                    count_new_iteration=True,
                ) or changed
            if run.native_resume_pending:
                if (
                    not run.post_suspend_cleaning_observed
                    and self._entity_changed_after(
                        self.vacuum_entity,
                        run.suspended_at,
                    )
                ):
                    run.post_suspend_cleaning_observed = True
                    changed = True
                if (
                    self._status_flag() == "segment"
                    and self._entity_changed_after(
                        self.config.get(CONF_STATUS_FLAG_ENTITY),
                        run.suspended_at,
                    )
                ):
                    changed = run.observe_segment_iteration(
                        source="native_resume_segment",
                        count_new_iteration=False,
                    ) or changed
                    if not run.post_suspend_segment_observed:
                        run.post_suspend_segment_observed = True
                        changed = True
                changed = self._mark_active_run_resumed(run) or changed

        if entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY):
            if normalized_state == "segment":
                if (
                    run.native_resume_pending
                    or normalize_state(self._state(self.vacuum_entity))
                    == "cleaning"
                    or run.observed_cleaning
                ):
                    changed = run.observe_segment_iteration(
                        source="status_segment_transition",
                        count_new_iteration=not run.native_resume_pending,
                    ) or changed
                elif not run.observed_segment_cleaning:
                    run.observed_segment_cleaning = True
                    changed = True
                if run.native_resume_pending:
                    if (
                        not run.post_suspend_segment_observed
                        and self._entity_changed_after(
                            self.config.get(CONF_STATUS_FLAG_ENTITY),
                            run.suspended_at,
                        )
                    ):
                        run.post_suspend_segment_observed = True
                        changed = True
                    if (
                        normalize_state(self._state(self.vacuum_entity))
                        == "cleaning"
                        and self._entity_changed_after(
                            self.vacuum_entity,
                            run.suspended_at,
                        )
                    ):
                        if not run.post_suspend_cleaning_observed:
                            run.post_suspend_cleaning_observed = True
                            changed = True
                    changed = self._mark_active_run_resumed(run) or changed
            elif normalized_state == "resumable":
                if (
                    (
                        run.phase == RUN_PHASE_DISPATCHING
                        and not (
                            self._active_run_restored
                            and run.command_published
                        )
                    )
                    or (
                        not run.observed_segment_cleaning
                        and not run.native_resume_pending
                        and not (
                            self._active_run_restored
                            and run.command_published
                        )
                    )
                ):
                    return changed
                if not run.resumable_latched:
                    run.resumable_latched = True
                    changed = True
                changed = self._mark_active_run_interrupted(
                    run,
                    now,
                    phase=RUN_PHASE_SUSPENDED,
                    reason="Valetudo reported a resumable native task",
                    require_resume=True,
                ) or changed
            else:
                changed = run.clear_segment_iteration() or changed

        if (
            entity_id == self.config.get(CONF_DOCK_STATUS_ENTITY)
            and normalized_state in _BUSY_DOCK_STATES
            and normalize_state(self._state(self.vacuum_entity)) != "cleaning"
            and not self._active_run_allows_current_blocker()
        ):
            changed = self._mark_active_run_interrupted(
                run,
                now,
                phase=RUN_PHASE_DOCK_INTERRUPT,
                reason=f"Dock interruption: {normalized_state}",
                require_resume=False,
            ) or changed

        if entity_id == self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY):
            if normalize_state(self._state(self.vacuum_entity)) == "cleaning":
                run.observe_estimated_room(
                    self._room_id_from_estimated(new_state.state),
                    now,
                )
            elif run.last_estimated_room_id is not None:
                run.finalize_estimated_room(now)
            changed = True
        return changed

    def _mark_active_run_interrupted(
        self,
        run: ActiveRun,
        now: datetime,
        *,
        phase: str,
        reason: str,
        require_resume: bool,
    ) -> bool:
        """Latch one native-task interruption without issuing a command."""
        if run.phase in {RUN_PHASE_CANCEL_PENDING, RUN_PHASE_RECOVERY_STALLED}:
            return False

        changed = False
        previous_phase = run.phase
        already_pending = run.native_resume_pending
        if not already_pending:
            if self._active_run_restored:
                run.last_estimated_room_id = None
                run.last_estimated_changed_at = None
            else:
                run.finalize_estimated_room(now)
            run.checkpoint_statistics(
                parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
                parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
            )
            run.interruption_count += 1
            changed = run.clear_segment_iteration() or changed
            run.suspended_at = now.isoformat()
            run.suspend_reason = reason
            run.post_suspend_cleaning_observed = False
            run.post_suspend_segment_observed = False
            run.recovery_deadline = (
                now
                + timedelta(
                    seconds=int(
                        self.config.get(
                            CONF_NATIVE_RESUME_TIMEOUT,
                            DEFAULT_NATIVE_RESUME_TIMEOUT,
                        )
                    )
                )
            ).isoformat()
            changed = True
        elif run.recovery_deadline is None:
            run.recovery_deadline = (
                now
                + timedelta(
                    seconds=int(
                        self.config.get(
                            CONF_NATIVE_RESUME_TIMEOUT,
                            DEFAULT_NATIVE_RESUME_TIMEOUT,
                        )
                    )
                )
            ).isoformat()
            changed = True

        target_phase = (
            RUN_PHASE_SUSPENDED
            if require_resume or run.phase == RUN_PHASE_SUSPENDED
            else phase
        )
        if run.phase != target_phase:
            run.phase = target_phase
            changed = True
        if require_resume and not run.resume_required:
            run.resume_required = True
            changed = True
        if (
            not run.suspend_reason
            or (
                require_resume
                and (
                    previous_phase == RUN_PHASE_DOCK_INTERRUPT
                    or is_low_battery_error(reason)
                )
                and run.suspend_reason != reason
            )
        ):
            run.suspend_reason = reason
            changed = True
        if phase == RUN_PHASE_DOCK_INTERRUPT and run.docked_at is not None:
            run.docked_at = None
            changed = True
        guard = self._ensure_retained_task_guard(
            now,
            owner=RETAINED_TASK_OWNER_COORDINATOR,
            phase=RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME,
            reason=reason,
            run=run,
        )
        if guard.phase != RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME:
            guard.phase = RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
            changed = True
        if guard.reason != reason:
            guard.reason = reason
            changed = True
        changed = self._update_retained_task_snapshot(
            guard,
            now,
            material=True,
        ) or changed
        return changed

    def _mark_active_run_resumed(self, run: ActiveRun) -> bool:
        """Adopt a native cleaning+segment resume for the same active run."""
        if (
            run.phase not in {
                RUN_PHASE_SUSPENDED,
                RUN_PHASE_DOCK_INTERRUPT,
                RUN_PHASE_RECOVERY_STALLED,
            }
            or
            not run.native_resume_pending
            or not run.post_suspend_cleaning_observed
            or (
                self.config.get(CONF_STATUS_FLAG_ENTITY)
                and not run.post_suspend_segment_observed
            )
        ):
            return False
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        if dock_status and dock_status.lower() in _BUSY_DOCK_STATES:
            return False

        run.checkpoint_statistics(
            parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
            parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
        )
        estimated_room_id = self._room_id_from_estimated(
            self._state(self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY))
        )
        if estimated_room_id and run.last_estimated_room_id is None:
            run.observe_estimated_room(estimated_room_id, dt_util.utcnow())
        run.phase = RUN_PHASE_RESUMED_CLEANING
        run.resumed_after_suspend = True
        run.resume_source = "native_segment"
        run.resume_required = False
        run.recovery_deadline = None
        run.docked_at = None
        guard = self._ensure_retained_task_guard(
            dt_util.utcnow(),
            owner=RETAINED_TASK_OWNER_COORDINATOR,
            phase=RETAINED_TASK_PHASE_OBSERVED_ACTIVE,
            run=run,
        )
        guard.phase = RETAINED_TASK_PHASE_OBSERVED_ACTIVE
        guard.reason = None
        self._update_retained_task_snapshot(
            guard,
            dt_util.utcnow(),
            material=True,
        )
        self._cancel_dock_settle_timer()
        self._cancel_native_resume_timeout()
        return True

    def _cleared_native_task_settle_started_at(
        self,
        run: ActiveRun,
    ) -> datetime | None:
        """Return when a retained non-battery task became safely clear at the dock."""
        if (
            not run.native_resume_pending
            or not run.resume_required
            or is_low_battery_error(run.suspend_reason)
            or run.cancel_requested_at is not None
            or not self.session
            or not self.session.active
            or normalize_state(self._state(self.vacuum_entity)) != "docked"
            or not self._configured_error_is_clear()
        ):
            return None

        status_entity = self.config.get(CONF_STATUS_FLAG_ENTITY)
        if (
            not status_entity
            or not self._configured_status_is_available()
            or self._status_flag() != "none"
        ):
            return None

        dock_status_entity = self.config.get(CONF_DOCK_STATUS_ENTITY)
        dock_status = normalize_state(self._state(dock_status_entity))
        if dock_status_entity and (
            dock_status is None
            or dock_status.lower() in {"unknown", "unavailable"}
            or dock_status.lower() in _BUSY_DOCK_STATES
        ):
            return None

        suspended_at = parse_datetime(run.suspended_at)
        docked_at = parse_datetime(run.docked_at)
        status_state = self.hass.states.get(status_entity)
        if (
            suspended_at is None
            or docked_at is None
            or status_state is None
            or status_state.last_changed <= suspended_at
        ):
            return None
        return max(docked_at, status_state.last_changed)

    async def _async_reconcile_active_run(self, now: datetime) -> bool:
        """Reconcile retained-run recovery from the current HA sensor snapshot."""
        run = self.active_run
        if not run:
            return False
        if run.phase == RUN_PHASE_CANCEL_PENDING:
            continue_session = run.cancel_continue_session
            completed = await self._async_execute_cancel_pending()
            if completed:
                await self._async_save_store()
                self._notify_listeners()
                if continue_session:
                    await self._async_maybe_start_next_room()
                else:
                    await self._async_maybe_send_auto_clean_summary()
            return True
        if (
            self._run_waiting_for_start_ack(run)
            and (pending_blocker := self._current_blocker()) is not None
            and not self._active_run_allows_current_blocker()
        ):
            await self._async_handle_active_run_error(
                pending_blocker.reason
            )
            return True

        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        status_flag = self._status_flag()
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        dock_status = dock_status.lower() if dock_status else None
        allowed_degraded_error = bool(
            self._active_run_allows_error(self.error_state)
            or self._active_run_allows_current_blocker()
        )
        changed = False

        if (
            status_flag == "resumable"
            and (
                run.phase != RUN_PHASE_DISPATCHING
                or (
                    self._active_run_restored
                    and run.command_published
                )
            )
            and (
                run.observed_segment_cleaning
                or run.native_resume_pending
                or (
                    self._active_run_restored
                    and run.command_published
                )
            )
        ):
            if not run.resumable_latched:
                run.resumable_latched = True
                changed = True
            changed = self._mark_active_run_interrupted(
                run,
                now,
                phase=RUN_PHASE_SUSPENDED,
                reason="Valetudo reported a resumable native task",
                require_resume=True,
            ) or changed

        if (
            dock_status in _BUSY_DOCK_STATES
            and vacuum_state != "cleaning"
            and not allowed_degraded_error
        ):
            changed = self._mark_active_run_interrupted(
                run,
                now,
                phase=RUN_PHASE_DOCK_INTERRUPT,
                reason=f"Dock interruption: {dock_status}",
                require_resume=False,
            ) or changed

        if (
            run.phase == RUN_PHASE_DOCK_INTERRUPT
            and dock_status not in _BUSY_DOCK_STATES
            and vacuum_state == "cleaning"
        ):
            if not run.post_suspend_cleaning_observed:
                run.post_suspend_cleaning_observed = True
                changed = True
            if (
                not self.config.get(CONF_STATUS_FLAG_ENTITY)
                or status_flag == "segment"
            ):
                if not run.post_suspend_segment_observed:
                    run.post_suspend_segment_observed = True
                    changed = True
            changed = self._mark_active_run_resumed(run) or changed

        if run.native_resume_pending and vacuum_state == "cleaning":
            if (
                not run.post_suspend_cleaning_observed
                and self._entity_changed_after(
                    self.vacuum_entity,
                    run.suspended_at,
                )
            ):
                run.post_suspend_cleaning_observed = True
                changed = True
            if (
                status_flag == "segment"
                and self._entity_changed_after(
                    self.config.get(CONF_STATUS_FLAG_ENTITY),
                    run.suspended_at,
                )
            ):
                changed = run.observe_segment_iteration(
                    source="native_resume_segment",
                    count_new_iteration=False,
                ) or changed
                if not run.post_suspend_segment_observed:
                    run.post_suspend_segment_observed = True
                    changed = True
            changed = self._mark_active_run_resumed(run) or changed

        if changed:
            await self._async_save_store()
            self._notify_listeners()

        if (
            run.native_resume_pending
            and self._low_battery_task_cleared_for_retry(run)
        ):
            await self._async_requeue_cleared_low_battery_run(run)
            return True

        if (
            allowed_degraded_error
            and vacuum_state == "error"
            and dock_status == "pause"
            and not run.native_resume_pending
            and run.observed_cleaning
            and run.observed_segment_cleaning
        ):
            await self._async_finish_active_run()
            return True

        if run.native_resume_pending:
            deadline = parse_datetime(run.recovery_deadline)
            if deadline is not None and now >= deadline:
                await self._async_expire_native_resume()
                return True
            blocker = self._current_blocker() or BlockerClassification(
                "task.resume_pending",
                BLOCKER_RECOVERABLE,
                "recovery",
                run.suspend_reason or "Valetudo retained the current room task",
                "Leave the vacuum docked so the retained task can resume.",
            )
            if self.session and self.session.active:
                await self._async_enter_recoverable_wait(
                    blocker,
                    phase="suspended",
                    next_retry_at=deadline,
                )
            self._schedule_native_resume_timeout()
        elif (
            self.session
            and self.session.active
            and self.session.recovery_phase == "suspended"
        ):
            await self._async_clear_recoverable_wait()

        if vacuum_state in _AT_DOCK_VACUUM_STATES:
            await self._async_reconcile_active_run_at_dock(now)
            return True

        if run.docked_at is not None:
            run.docked_at = None
            self._cancel_dock_settle_timer()
            await self._async_save_store()
        return run.native_resume_pending

    def _low_battery_task_cleared_for_retry(self, run: ActiveRun) -> bool:
        """Return whether a charged docked robot lost its retained battery task."""
        if not is_low_battery_error(run.suspend_reason):
            return False
        if normalize_state(self._state(self.vacuum_entity)) not in _AT_DOCK_VACUUM_STATES:
            return False
        if not self._configured_error_is_clear():
            return False
        battery_entity = self.config.get(CONF_BATTERY_ENTITY)
        if battery_entity:
            battery = parse_float(self._state(battery_entity))
            if (
                battery is None
                or battery
                < float(
                    self.config.get(
                        CONF_MIN_BATTERY,
                        DEFAULT_MIN_BATTERY,
                    )
                )
            ):
                return False
        status_entity = self.config.get(CONF_STATUS_FLAG_ENTITY)
        if status_entity and (
            self._status_flag() != "none"
            or not self._entity_changed_after(
                status_entity,
                run.suspended_at,
            )
        ):
            return False
        dock_entity = self.config.get(CONF_DOCK_STATUS_ENTITY)
        dock_status = normalize_state(self._state(dock_entity))
        return bool(
            not dock_entity
            or (dock_status or "").lower() in _READY_DOCK_STATES
        )

    async def _async_requeue_cleared_low_battery_run(
        self,
        run: ActiveRun,
    ) -> None:
        """Safely clear and requeue a battery-interrupted task that vanished."""
        if self.active_run is not run:
            return
        reason = "Low-battery retained task cleared after recharge"
        run.cancelled = True
        run.phase = RUN_PHASE_CANCEL_PENDING
        run.cancel_requested_at = utcnow_iso()
        run.cancel_reason = reason
        run.cancel_continue_session = True
        run.cancel_requeue_room = True
        run.cancel_outcome_result = "interrupted"
        self._cancel_active_run_timers()
        await self._async_enter_recoverable_wait(
            BlockerClassification(
                "power.recharged_task_cleared",
                BLOCKER_RECOVERABLE,
                "power",
                reason,
                "The coordinator will clear the stale command and retry the room.",
            ),
            phase="cancelling_pending_command",
            next_retry_at=None,
        )
        if await self._async_execute_cancel_pending():
            await self._async_maybe_start_next_room()

    async def _async_reconcile_active_run_at_dock(self, now: datetime) -> None:
        """Wait for a stable dock snapshot before finalizing a retained run."""
        run = self.active_run
        if not run:
            return
        if (
            self._run_waiting_for_start_ack(run)
            and (pending_blocker := self._current_blocker()) is not None
            and not self._active_run_allows_current_blocker()
        ):
            await self._async_handle_active_run_error(
                pending_blocker.reason
            )
            return
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        dock_status = dock_status.lower() if dock_status else None
        allowed_degraded_error = bool(
            self._active_run_allows_error(self.error_state)
            or self._active_run_allows_current_blocker()
        )
        effective_dock_busy = (
            dock_status in _BUSY_DOCK_STATES
            and not (allowed_degraded_error and dock_status == "pause")
        )
        dock_status_unavailable = bool(
            self.config.get(CONF_DOCK_STATUS_ENTITY)
            and dock_status in {None, "unknown", "unavailable"}
        )
        if dock_status_unavailable and run.docked_at is not None:
            run.docked_at = None
            self._cancel_dock_settle_timer()
            await self._async_save_store()
            self._notify_listeners()
        elif (
            not dock_status_unavailable
            and not effective_dock_busy
            and run.docked_at is None
        ):
            self._finalize_estimated_room_at_dock(run, now)
            run.docked_at = now.isoformat()
            await self._async_save_store()
            self._notify_listeners()
        if not self._configured_error_is_available():
            if run.docked_at is not None:
                run.docked_at = None
                self._cancel_dock_settle_timer()
                await self._async_save_store()
                self._notify_listeners()
            return
        if (
            not is_error_clear(self.error_state)
            and not allowed_degraded_error
        ):
            await self._async_handle_active_run_error(
                self.error_state or "Unknown error"
            )
            return
        if dock_status_unavailable:
            return

        status_flag = self._status_flag()
        if (
            self.config.get(CONF_STATUS_FLAG_ENTITY)
            and not self._configured_status_is_available()
        ):
            if run.docked_at is not None:
                run.docked_at = None
                self._cancel_dock_settle_timer()
                await self._async_save_store()
                self._notify_listeners()
            return
        if (
            status_flag == "resumable"
            and (
                run.phase != RUN_PHASE_DISPATCHING
                or (
                    self._active_run_restored
                    and run.command_published
                )
            )
            and (
                run.observed_segment_cleaning
                or run.native_resume_pending
                or (
                    self._active_run_restored
                    and run.command_published
                )
            )
        ):
            changed = False
            if not run.resumable_latched:
                run.resumable_latched = True
                changed = True
            changed = self._mark_active_run_interrupted(
                run,
                now,
                phase=RUN_PHASE_SUSPENDED,
                reason="Valetudo reported a resumable native task",
                require_resume=True,
            ) or changed
            if changed:
                await self._async_save_store()
                self._notify_listeners()
            blocker = self._current_blocker() or BlockerClassification(
                "task.resume_pending",
                BLOCKER_RECOVERABLE,
                "recovery",
                (
                    run.suspend_reason
                    or "Valetudo retained the current room task"
                ),
                "Leave the vacuum docked so the retained task can resume.",
            )
            if self.session and self.session.active:
                await self._async_enter_recoverable_wait(
                    blocker,
                    phase="suspended",
                    next_retry_at=parse_datetime(run.recovery_deadline),
                )
            self._schedule_native_resume_timeout()
            return

        if run.phase == RUN_PHASE_DISPATCHING and not run.observed_cleaning:
            self._schedule_dispatch_start_timeout()
            return

        if effective_dock_busy:
            changed = self._mark_active_run_interrupted(
                run,
                now,
                phase=RUN_PHASE_DOCK_INTERRUPT,
                reason=f"Dock interruption: {dock_status}",
                require_resume=False,
            )
            if changed:
                await self._async_save_store()
                self._notify_listeners()
            self._schedule_native_resume_timeout()
            return

        changed = False
        if run.docked_at is None:
            self._finalize_estimated_room_at_dock(run, now)
            run.docked_at = now.isoformat()
            changed = True
        if changed:
            await self._async_save_store()
            self._notify_listeners()

        if run.native_resume_pending and run.resume_required:
            cleared_task_settle_started_at = (
                self._cleared_native_task_settle_started_at(run)
            )
            if cleared_task_settle_started_at is None:
                self._schedule_native_resume_timeout()
                return

            settle_seconds = int(
                self.config.get(CONF_DOCK_SETTLE, DEFAULT_DOCK_SETTLE)
            )
            if (
                now - cleared_task_settle_started_at
            ).total_seconds() < settle_seconds:
                self._schedule_dock_settle_timer(
                    cleared_task_settle_started_at
                )
                self._schedule_native_resume_timeout()
                return

            _LOGGER.info(
                "Finalizing %s room %s after Valetudo cleared the retained task at the dock",
                self.name,
                run.room_id,
            )
            run.resume_required = False
            run.resume_source = "native_task_cleared"
            run.recovery_deadline = None
            self._cancel_native_resume_timeout()

        docked_at = parse_datetime(run.docked_at) or now
        settle_seconds = int(
            self.config.get(CONF_DOCK_SETTLE, DEFAULT_DOCK_SETTLE)
        )
        if (now - docked_at).total_seconds() < settle_seconds:
            self._schedule_dock_settle_timer()
            if run.native_resume_pending:
                self._schedule_native_resume_timeout()
            return

        await self._async_finish_active_run()

    def _finalize_estimated_room_at_dock(
        self,
        run: ActiveRun,
        now: datetime,
    ) -> None:
        """Avoid counting Home Assistant downtime as estimated-room dwell."""
        if self._active_run_restored:
            run.last_estimated_room_id = None
            run.last_estimated_changed_at = None
            return
        run.finalize_estimated_room(now)

    async def _async_expire_native_resume(self) -> None:
        """Keep a stalled native task suspended and continue bounded checks."""
        run = self.active_run
        if not run or not run.native_resume_pending:
            return
        run.phase = RUN_PHASE_RECOVERY_STALLED
        reason = (
            f"Native resume timed out after "
            f"{int(self.config.get(CONF_NATIVE_RESUME_TIMEOUT, DEFAULT_NATIVE_RESUME_TIMEOUT))}s"
        )
        run.suspend_reason = reason
        retry_at = dt_util.utcnow() + timedelta(
            seconds=max(
                1,
                int(
                    self.config.get(
                        CONF_BLOCKED_SESSION_TIMEOUT,
                        DEFAULT_BLOCKED_SESSION_TIMEOUT,
                    )
                ),
            )
        )
        run.recovery_deadline = retry_at.isoformat()
        if self.session and self.session.active:
            await self._async_enter_recoverable_wait(
                self._current_blocker()
                or BlockerClassification(
                    "recovery.native_resume_stalled",
                    BLOCKER_RECOVERABLE,
                    "recovery",
                    reason,
                    (
                        "Leave the vacuum docked and clear any physical fault; "
                        "the retained task remains preserved."
                    ),
                ),
                phase="suspended",
                next_retry_at=retry_at,
            )
        await self._async_save_store()
        self._notify_listeners()
        self._schedule_native_resume_timeout()

    def _clear_active_run(self, *, retain_task_guard: bool = False) -> None:
        """Clear the active run and all timers tied to it."""
        self.active_run = None
        self._active_run_restored = False
        self._restored_dispatch_intent_deadline = None
        if self.session:
            self.session.active_room_id = None
        self._cancel_active_run_timers()
        if not retain_task_guard:
            self._clear_retained_task_guard()

    def _schedule_active_run_timers(self) -> None:
        """Recreate persisted active-run timers after startup."""
        if not self.active_run:
            return
        if self.active_run.native_resume_pending:
            self._schedule_native_resume_timeout()
        if (
            self.active_run.phase == RUN_PHASE_DISPATCHING
            and self.active_run.command_published
        ):
            self._schedule_dispatch_start_timeout()
        if self.active_run.phase == RUN_PHASE_CANCEL_PENDING:
            self._schedule_cancel_ack_timeout()
        if self.active_run.docked_at is not None:
            self._schedule_dock_settle_timer()

    def _schedule_dispatch_start_timeout(self) -> None:
        """Schedule a finite deadline for an acknowledged segment publish."""
        run = self.active_run
        if (
            not run
            or run.phase != RUN_PHASE_DISPATCHING
            or not run.command_published
            or getattr(self, "_dispatch_start_timeout_cancel", None) is not None
        ):
            return
        deadline = parse_datetime(run.dispatch_deadline)
        if deadline is None:
            deadline = dt_util.utcnow() + timedelta(
                seconds=max(
                    1,
                    int(
                        self.config.get(
                            CONF_DISPATCH_START_TIMEOUT,
                            DEFAULT_DISPATCH_START_TIMEOUT,
                        )
                    ),
                )
            )
            run.dispatch_deadline = deadline.isoformat()
        remaining = (deadline - dt_util.utcnow()).total_seconds()

        def timer_finished(_now: datetime) -> None:
            self._dispatch_start_timeout_cancel = None
            schedule_hass_task(
                self.hass,
                self._async_reconcile_active_run_timers_serialized(),
            )

        if remaining <= 0:
            timer_finished(dt_util.utcnow())
            return
        self._dispatch_start_timeout_cancel = async_call_later(
            self.hass,
            remaining,
            timer_finished,
        )

    def _schedule_native_resume_timeout(self) -> None:
        """Schedule reconciliation at the persisted native-resume deadline."""
        run = self.active_run
        if (
            not run
            or not run.native_resume_pending
            or self._native_resume_timeout_cancel is not None
        ):
            return
        deadline = parse_datetime(run.recovery_deadline)
        if deadline is None:
            return
        remaining = (deadline - dt_util.utcnow()).total_seconds()
        if remaining <= 0:
            return

        def timer_finished(_now: datetime) -> None:
            self._native_resume_timeout_cancel = None
            schedule_hass_task(
                self.hass,
                self._async_reconcile_active_run_timers_serialized(),
            )

        self._native_resume_timeout_cancel = async_call_later(
            self.hass,
            remaining,
            timer_finished,
        )

    def _schedule_cancel_ack_timeout(self) -> None:
        """Schedule bounded reconciliation without repeating a stop command."""
        run = self.active_run
        if (
            not run
            or run.phase != RUN_PHASE_CANCEL_PENDING
            or (
                run.cancel_stop_acknowledged_at
                and not (
                    run.cancel_return_attempts >= 1
                    and not run.return_acknowledged
                )
            )
            or getattr(self, "_cancel_ack_timeout_cancel", None) is not None
        ):
            return
        deadline = parse_datetime(run.cancel_ack_deadline)
        if deadline is None:
            deadline = dt_util.utcnow() + timedelta(
                seconds=max(
                    1,
                    int(
                        self.config.get(
                            CONF_DISPATCH_START_TIMEOUT,
                            DEFAULT_DISPATCH_START_TIMEOUT,
                        )
                    ),
                )
            )
            run.cancel_ack_deadline = deadline.isoformat()
        remaining = (deadline - dt_util.utcnow()).total_seconds()

        def timer_finished(_now: datetime) -> None:
            self._cancel_ack_timeout_cancel = None
            schedule_hass_task(
                self.hass,
                self._async_reconcile_active_run_timers_serialized(),
            )

        if remaining <= 0:
            timer_finished(dt_util.utcnow())
            return
        self._cancel_ack_timeout_cancel = async_call_later(
            self.hass,
            remaining,
            timer_finished,
        )

    def _schedule_dock_settle_timer(
        self,
        settle_started_at: datetime | None = None,
    ) -> None:
        """Schedule reconciliation after the stable dock settle window."""
        run = self.active_run
        if (
            not run
            or run.docked_at is None
            or self._dock_settle_cancel is not None
        ):
            return
        settle_started_at = settle_started_at or parse_datetime(run.docked_at)
        if settle_started_at is None:
            return
        settle_seconds = int(
            self.config.get(CONF_DOCK_SETTLE, DEFAULT_DOCK_SETTLE)
        )
        remaining = settle_seconds - (
            dt_util.utcnow() - settle_started_at
        ).total_seconds()
        if remaining <= 0:
            return

        def timer_finished(_now: datetime) -> None:
            self._dock_settle_cancel = None
            schedule_hass_task(
                self.hass,
                self._async_reconcile_active_run_timers_serialized(),
            )

        self._dock_settle_cancel = async_call_later(
            self.hass,
            remaining,
            timer_finished,
        )

    async def _async_reconcile_active_run_timers_serialized(self) -> None:
        """Reconcile active-run timers without racing entity events."""
        async with self._event_lock:
            run = self.active_run
            if run and run.phase == RUN_PHASE_CANCEL_PENDING:
                continue_session = run.cancel_continue_session
                deadline = parse_datetime(run.cancel_ack_deadline)
                pending_stop_ack = not run.cancel_stop_acknowledged_at
                pending_return_ack = bool(
                    run.cancel_return_attempts >= 1
                    and not run.return_acknowledged
                )
                if (
                    (pending_stop_ack or pending_return_ack)
                    and deadline is not None
                    and dt_util.utcnow() >= deadline
                ):
                    retry_at = dt_util.utcnow() + timedelta(
                        seconds=max(
                            1,
                            int(
                                self.config.get(
                                    CONF_BLOCKED_SESSION_TIMEOUT,
                                    DEFAULT_BLOCKED_SESSION_TIMEOUT,
                                )
                            ),
                        )
                    )
                    run.cancel_ack_deadline = retry_at.isoformat()
                    if self.session:
                        self.session.enter_recovery(
                            code=(
                                "command.return_ack_pending"
                                if pending_return_ack
                                else "command.stop_ack_pending"
                            ),
                            disposition=BLOCKER_RECOVERABLE,
                            reason=(
                                "The one acknowledged cancellation command is "
                                "still awaiting physical confirmation"
                            ),
                            phase="operator_required",
                            next_retry_at=retry_at.isoformat(),
                            waiting_for_physical_fix=False,
                            operator_action=(
                                "Wait for physical confirmation; no duplicate "
                                "command will be sent."
                            ),
                            retry_cadence_reason="command_ack_recheck",
                        )
                    await self._async_save_store()
                    self._notify_listeners()
                completed = await self._async_execute_cancel_pending()
                if completed:
                    await self._async_save_store()
                    self._notify_listeners()
                    if (
                        continue_session
                        and self.session
                        and self.session.active
                        and self._all_people_away()
                    ):
                        await self._async_maybe_start_next_room()
                    else:
                        await self._async_maybe_send_auto_clean_summary()
                return
            if (
                run
                and run.phase == RUN_PHASE_DISPATCHING
                and run.command_published
                and (deadline := parse_datetime(run.dispatch_deadline)) is not None
                and dt_util.utcnow() >= deadline
            ):
                await self._async_expire_dispatch_start()
                return
            await self._async_reconcile_active_run(dt_util.utcnow())

    async def _async_expire_dispatch_start(self) -> None:
        """Cancel an unacknowledged publish before any later redispatch."""
        run = self.active_run
        if (
            not run
            or run.phase != RUN_PHASE_DISPATCHING
            or not run.command_published
        ):
            return
        run.dispatch_deadline = None
        failure_reason = (
            f"Segment dispatch did not start within "
            f"{int(self.config.get(CONF_DISPATCH_START_TIMEOUT, DEFAULT_DISPATCH_START_TIMEOUT))}s"
        )
        blocker = BlockerClassification(
            "dispatch.start_timeout",
            BLOCKER_RECOVERABLE,
            "dispatch",
            failure_reason,
            (
                "The coordinator will clear the pending command and retry "
                "without publishing another command first."
            ),
        )
        run.cancelled = True
        run.phase = RUN_PHASE_CANCEL_PENDING
        run.cancel_requested_at = utcnow_iso()
        run.cancel_reason = failure_reason
        run.cancel_continue_session = True
        run.cancel_requeue_room = True
        run.cancel_outcome_result = "interrupted"
        run.dispatch_failure_code = "dispatch.start_timeout"
        self._cancel_active_run_timers()
        await self._async_enter_recoverable_wait(
            blocker,
            phase="cancelling_pending_command",
            next_retry_at=None,
        )
        if not await self._async_execute_cancel_pending():
            return
        if self.session and self.session.active:
            await self._async_maybe_start_next_room()

    def _cancel_dock_settle_timer(self) -> None:
        """Cancel the current dock-settle timer."""
        if self._dock_settle_cancel is not None:
            self._dock_settle_cancel()
            self._dock_settle_cancel = None

    def _cancel_native_resume_timeout(self) -> None:
        """Cancel the current native-resume timeout."""
        if self._native_resume_timeout_cancel is not None:
            self._native_resume_timeout_cancel()
            self._native_resume_timeout_cancel = None

    def _cancel_dispatch_start_timeout(self) -> None:
        """Cancel the current dispatch-start watchdog."""
        if getattr(self, "_dispatch_start_timeout_cancel", None) is not None:
            self._dispatch_start_timeout_cancel()
            self._dispatch_start_timeout_cancel = None

    def _cancel_cancel_ack_timeout(self) -> None:
        """Cancel the pending-command acknowledgement watchdog."""
        if getattr(self, "_cancel_ack_timeout_cancel", None) is not None:
            self._cancel_ack_timeout_cancel()
            self._cancel_ack_timeout_cancel = None

    def _cancel_active_run_timers(self) -> None:
        """Cancel all timers owned by the active run."""
        self._cancel_dock_settle_timer()
        self._cancel_native_resume_timeout()
        self._cancel_dispatch_start_timeout()
        self._cancel_cancel_ack_timeout()

    def _retained_task_sensor_entities(self) -> set[str]:
        """Return entities whose transitions can materially change task ownership."""
        return {
            entity_id
            for entity_id in (
                self.vacuum_entity,
                self.config.get(CONF_STATUS_FLAG_ENTITY),
                self.config.get(CONF_DOCK_STATUS_ENTITY),
                self.config.get(CONF_ERROR_ENTITY),
                self.config.get(CONF_CURRENT_AREA_ENTITY),
                self.config.get(CONF_CURRENT_TIME_ENTITY),
                self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY),
            )
            if entity_id
        }

    def _retained_task_snapshot(
        self,
    ) -> tuple[str | None, str | None, str | None, str | None]:
        """Return the normalized task-relevant robot snapshot."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        return (
            vacuum_state.lower() if vacuum_state else None,
            self._status_flag(),
            dock_status.lower() if dock_status else None,
            normalize_state(self.error_state),
        )

    def _ensure_retained_task_guard(
        self,
        now: datetime,
        *,
        owner: str,
        phase: str,
        reason: str | None = None,
        run: ActiveRun | None = None,
    ) -> RetainedTaskGuard:
        """Return a persisted guard, upgrading ownership when evidence improves."""
        guard = self.retained_task_guard
        if guard is None:
            vacuum_state, status_flag, dock_status, error = (
                self._retained_task_snapshot()
            )
            guard = RetainedTaskGuard(
                owner=owner,
                phase=phase,
                first_observed_at=now.isoformat(),
                last_material_activity_at=now.isoformat(),
                origin_session_id=(
                    run.session_id
                    if run
                    else (self.session.session_id if self.session else None)
                ),
                origin_run_started_at=run.started_at if run else None,
                reason=reason,
                coherent_since=now.isoformat(),
                last_vacuum_state=vacuum_state,
                last_status_flag=status_flag,
                last_dock_status=dock_status,
                last_error=error,
            )
            self.retained_task_guard = guard
            return guard

        if owner == RETAINED_TASK_OWNER_COORDINATOR:
            guard.owner = owner
        elif (
            owner == RETAINED_TASK_OWNER_MANUAL
            and guard.owner == RETAINED_TASK_OWNER_UNKNOWN
        ):
            guard.owner = owner
        if run:
            guard.origin_session_id = run.session_id
            guard.origin_run_started_at = run.started_at
        elif guard.origin_session_id is None and self.session:
            guard.origin_session_id = self.session.session_id
        if reason is not None:
            guard.reason = reason
        return guard

    def _set_retained_task_guard_for_run(self, run: ActiveRun) -> None:
        """Claim a freshly prepared coordinator dispatch before publication."""
        now = parse_datetime(run.started_at) or dt_util.utcnow()
        vacuum_state, status_flag, dock_status, error = (
            self._retained_task_snapshot()
        )
        self.retained_task_guard = RetainedTaskGuard(
            owner=RETAINED_TASK_OWNER_COORDINATOR,
            phase=RETAINED_TASK_PHASE_OBSERVED_ACTIVE,
            first_observed_at=now.isoformat(),
            last_material_activity_at=now.isoformat(),
            origin_session_id=run.session_id,
            origin_run_started_at=run.started_at,
            reason="Coordinator room dispatch prepared",
            coherent_since=now.isoformat(),
            last_vacuum_state=vacuum_state,
            last_status_flag=status_flag,
            last_dock_status=dock_status,
            last_error=error,
        )
        self._cancel_retained_task_timer()

    def _mark_retained_task_abandoned(self, reason: str) -> None:
        """Keep ownership evidence after a terminal run error clears accounting."""
        run = self.active_run
        if not run:
            return
        now = dt_util.utcnow()
        guard = self._ensure_retained_task_guard(
            now,
            owner=RETAINED_TASK_OWNER_COORDINATOR,
            phase=RETAINED_TASK_PHASE_STALE_CANDIDATE,
            reason=reason,
            run=run,
        )
        guard.phase = RETAINED_TASK_PHASE_STALE_CANDIDATE
        guard.reason = reason
        guard.last_material_activity_at = now.isoformat()
        guard.coherent_since = now.isoformat()
        guard.stale_deadline = (
            now
            + timedelta(
                seconds=int(
                    self.config.get(
                        CONF_STALE_RESUME_AGE,
                        DEFAULT_STALE_RESUME_AGE,
                    )
                )
            )
        ).isoformat()
        self._update_retained_task_snapshot(guard, now, material=False)
        self._schedule_retained_task_timer()

    def _update_retained_task_snapshot(
        self,
        guard: RetainedTaskGuard,
        now: datetime,
        *,
        material: bool,
        reset_coherent: bool = True,
    ) -> bool:
        """Update the guard from the coherent Home Assistant snapshot."""
        vacuum_state, status_flag, dock_status, error = (
            self._retained_task_snapshot()
        )
        snapshot_changed = (
            guard.last_vacuum_state,
            guard.last_status_flag,
            guard.last_dock_status,
            guard.last_error,
        ) != (vacuum_state, status_flag, dock_status, error)
        changed = snapshot_changed
        guard.last_vacuum_state = vacuum_state
        guard.last_status_flag = status_flag
        guard.last_dock_status = dock_status
        guard.last_error = error
        if material:
            current = now.isoformat()
            if guard.last_material_activity_at != current:
                guard.last_material_activity_at = current
                changed = True
            guard.stale_deadline = None
        if reset_coherent and (snapshot_changed or guard.coherent_since is None):
            guard.coherent_since = now.isoformat()
            changed = True
        return changed

    def _observe_retained_task(
        self,
        entity_id: str,
        new_state: State,
        now: datetime,
    ) -> bool:
        """Observe task activity without issuing robot commands."""
        if entity_id not in self._retained_task_sensor_entities():
            return False

        vacuum_state, status_flag, _dock_status, _error = (
            self._retained_task_snapshot()
        )
        owner: str | None = None
        phase: str | None = None
        reason: str | None = None
        run: ActiveRun | None = None
        if self.active_run:
            owner = RETAINED_TASK_OWNER_COORDINATOR
            run = self.active_run
            phase = (
                RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
                if self.active_run.native_resume_pending
                else RETAINED_TASK_PHASE_OBSERVED_ACTIVE
            )
        elif self.manual_run:
            owner = RETAINED_TASK_OWNER_MANUAL
            run = self.manual_run
            phase = RETAINED_TASK_PHASE_OBSERVED_ACTIVE
        elif (
            vacuum_state in _ACTIVE_RETAINED_VACUUM_STATES
            or status_flag == "resumable"
            or self.retained_task_guard is not None
        ):
            owner = (
                self.retained_task_guard.owner
                if self.retained_task_guard
                else RETAINED_TASK_OWNER_UNKNOWN
            )
            phase = (
                RETAINED_TASK_PHASE_STALE_CANDIDATE
                if status_flag == "resumable"
                and vacuum_state not in _ACTIVE_RETAINED_VACUUM_STATES
                else RETAINED_TASK_PHASE_OBSERVED_ACTIVE
            )
            reason = (
                "Valetudo reported an unowned resumable task"
                if status_flag == "resumable"
                else "Observed external vacuum activity"
            )
        else:
            return False

        guard = self._ensure_retained_task_guard(
            now,
            owner=owner,
            phase=phase,
            reason=reason,
            run=run,
        )
        if (
            guard.phase == RETAINED_TASK_PHASE_CLEARED
            and (
                vacuum_state in _ACTIVE_RETAINED_VACUUM_STATES
                or status_flag == "resumable"
            )
        ):
            guard.owner = owner
            guard.phase = phase
            guard.first_observed_at = now.isoformat()
            guard.last_material_activity_at = now.isoformat()
            guard.coherent_since = now.isoformat()
            guard.stale_deadline = None
            guard.observation_deadline = None
            guard.clear_requested_at = None
            guard.clear_published_at = None
            guard.clear_deadline = None
            guard.clear_attempts = 0
            guard.clear_acknowledged_at = None
            guard.dock_clear_requested_at = None
            guard.dock_clear_published_at = None
            guard.dock_clear_deadline = None
            guard.dock_clear_attempts = 0
            guard.dock_clear_acknowledged_at = None
            guard.operator_required_reason = None
            guard.operator_notification_sent = False
        protected_phase = guard.phase in {
            RETAINED_TASK_PHASE_CLEAR_PENDING,
            RETAINED_TASK_PHASE_VERIFYING,
            RETAINED_TASK_PHASE_DOCK_CLEAR_PENDING,
            RETAINED_TASK_PHASE_DOCK_VERIFYING,
            RETAINED_TASK_PHASE_OPERATOR_REQUIRED,
        }
        changed = False
        if not protected_phase and guard.phase != phase:
            guard.phase = phase
            changed = True
        if reason is not None and guard.reason != reason:
            guard.reason = reason
            changed = True

        material = bool(
            entity_id == self.vacuum_entity
            or entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY)
            or entity_id == self.config.get(CONF_DOCK_STATUS_ENTITY)
            or entity_id == self.config.get(CONF_ERROR_ENTITY)
            or (
                entity_id
                in {
                    self.config.get(CONF_CURRENT_AREA_ENTITY),
                    self.config.get(CONF_CURRENT_TIME_ENTITY),
                    self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY),
                }
                and vacuum_state == "cleaning"
            )
        )
        changed = self._update_retained_task_snapshot(
            guard,
            now,
            material=material,
        ) or changed

        if vacuum_state in _ACTIVE_RETAINED_VACUUM_STATES:
            if not protected_phase:
                guard.phase = RETAINED_TASK_PHASE_OBSERVED_ACTIVE
            if guard.observation_deadline is None:
                guard.observation_deadline = (
                    (parse_datetime(guard.first_observed_at) or now)
                    + timedelta(
                        seconds=int(
                            self.config.get(
                                CONF_NATIVE_RESUME_TIMEOUT,
                                DEFAULT_NATIVE_RESUME_TIMEOUT,
                            )
                        )
                    )
                ).isoformat()
                changed = True
        elif status_flag == "resumable" and not protected_phase:
            guard.phase = RETAINED_TASK_PHASE_STALE_CANDIDATE
            guard.observation_deadline = None

        if (
            guard.phase != RETAINED_TASK_PHASE_CLEARED
            and self.session
            and self.session.active
        ):
            self.session.preflight_complete = False
        return changed

    def _retained_task_next_deadline(self) -> datetime | None:
        """Return the next persisted retained-task transition deadline."""
        guard = self.retained_task_guard
        if not guard:
            return None
        values = [
            guard.clear_deadline,
            guard.dock_clear_deadline,
            guard.stale_deadline,
            guard.observation_deadline,
        ]
        deadlines = [
            parsed
            for value in values
            if (parsed := parse_datetime(value)) is not None
        ]
        return min(deadlines) if deadlines else None

    def _schedule_retained_task_timer(self) -> None:
        """Schedule the next persisted retained-task reconciliation."""
        deadline = self._retained_task_next_deadline()
        if deadline is None:
            self._cancel_retained_task_timer()
            return
        now = dt_util.utcnow()
        if deadline <= now:
            self._cancel_retained_task_timer()
            if getattr(
                self,
                "_retained_task_reconcile_scheduled",
                False,
            ):
                return
            self._retained_task_reconcile_scheduled = True
            try:
                schedule_hass_task(
                    self.hass,
                    self._async_reconcile_retained_task_timer_serialized(),
                )
            except Exception:
                self._retained_task_reconcile_scheduled = False
                raise
            return
        existing_deadline = getattr(
            self,
            "_retained_task_timer_deadline",
            None,
        )
        if (
            getattr(self, "_retained_task_timer_cancel", None) is not None
            and existing_deadline is not None
            and existing_deadline <= deadline
        ):
            return
        self._cancel_retained_task_timer()

        def timer_finished(_now: datetime) -> None:
            self._retained_task_timer_cancel = None
            self._retained_task_timer_deadline = None
            if not self._retained_task_reconcile_scheduled:
                self._retained_task_reconcile_scheduled = True
                try:
                    schedule_hass_task(
                        self.hass,
                        self._async_reconcile_retained_task_timer_serialized(),
                    )
                except Exception:
                    self._retained_task_reconcile_scheduled = False
                    raise

        self._retained_task_timer_deadline = deadline
        self._retained_task_timer_cancel = async_call_later(
            self.hass,
            (deadline - now).total_seconds(),
            timer_finished,
        )

    def _cancel_retained_task_timer(self) -> None:
        """Cancel the retained-task deadline timer."""
        if getattr(self, "_retained_task_timer_cancel", None) is not None:
            self._retained_task_timer_cancel()
            self._retained_task_timer_cancel = None
        self._retained_task_timer_deadline = None

    async def _async_reconcile_retained_task_timer_serialized(self) -> None:
        """Reconcile a retained-task deadline without racing entity events."""
        try:
            async with self._event_lock:
                if self.session and self.session.active and not self.active_run:
                    await self._async_maybe_start_next_room()
                    return
                await self._async_reconcile_retained_task(
                    dt_util.utcnow(),
                    allow_clear=False,
                )
                if self.session and not self.session.active:
                    await self._async_maybe_send_auto_clean_summary()
        finally:
            self._retained_task_reconcile_scheduled = False

    async def _async_mark_retained_task_operator_required(
        self,
        reason: str,
    ) -> None:
        """Expose and notify an operator-required retained-task outcome."""
        guard = self.retained_task_guard
        if not guard:
            return
        guard.phase = RETAINED_TASK_PHASE_OPERATOR_REQUIRED
        guard.operator_required_reason = reason
        guard.reason = reason
        guard.stale_deadline = None
        guard.clear_deadline = None
        guard.dock_clear_deadline = None
        retry_at = dt_util.utcnow() + timedelta(
            seconds=max(
                1,
                int(
                    self.config.get(
                        CONF_BLOCKED_SESSION_TIMEOUT,
                        DEFAULT_BLOCKED_SESSION_TIMEOUT,
                    )
                ),
            )
        )
        guard.observation_deadline = retry_at.isoformat()
        if self.session and self.session.active:
            self.session.preflight_complete = False
            self.session.enter_recovery(
                code="task.operator_required",
                disposition=BLOCKER_RECOVERABLE,
                reason=reason,
                phase="operator_required",
                next_retry_at=retry_at.isoformat(),
                waiting_for_physical_fix=True,
                operator_action=(
                    "Clear the retained task or dock condition."
                ),
            )
        await self._async_save_store()
        self._notify_listeners()
        self._schedule_retained_task_timer()

        if guard.operator_notification_sent:
            return
        try:
            preserved = self.preserved_room_ids
            await self._async_send_notification(
                f"{self.name.replace(' Coordinator', '')} · Action Needed",
                (
                    f"Automatic cleaning is waiting because {reason}. "
                    "Clear the retained task or dock condition; the same "
                    "away session will resume. "
                    "No room command was sent. Recovery phase: "
                    f"operator_required. Next check: {retry_at.isoformat()}. "
                    f"Preserved rooms: {', '.join(preserved) or 'none'}. "
                    f"Stop attempts: {guard.clear_attempts}; "
                    f"dock-stop attempts: {guard.dock_clear_attempts}."
                ),
            )
        except Exception:  # noqa: BLE001
            _LOGGER.exception(
                "Could not send retained-task notification for %s",
                self.name,
            )
            return
        guard.operator_notification_sent = True
        if self.session and self.session.active:
            self.session.recovery_notification_fingerprint = (
                f"task.operator_required:{reason}"
            )
            self.session.last_recovery_notification_fingerprint = (
                self.session.recovery_notification_fingerprint
            )
            self.session.last_recovery_notification_at = utcnow_iso()
            self.session.recovery_notification_attempts = max(
                1,
                self.session.recovery_notification_attempts,
            )
            self.session.recovery_notification_sent = True
        await self._async_save_store()
        self._notify_listeners()

    async def _async_acknowledge_retained_task_clear(
        self,
        now: datetime,
    ) -> bool:
        """Record a clear retained task and finish any manual accounting."""
        guard = self.retained_task_guard
        if not guard:
            return True
        guard.phase = RETAINED_TASK_PHASE_CLEARED
        guard.reason = "Retained task cleared and dock settled"
        if guard.clear_attempts:
            guard.clear_acknowledged_at = (
                guard.clear_acknowledged_at or now.isoformat()
            )
        if guard.dock_clear_attempts:
            guard.dock_clear_acknowledged_at = now.isoformat()
        guard.operator_required_reason = None
        guard.stale_deadline = None
        guard.observation_deadline = None
        guard.clear_deadline = None
        guard.dock_clear_deadline = None
        if self.session and self.session.active:
            self.session.preflight_complete = True
            await self._async_clear_recoverable_wait()
        self._cancel_retained_task_timer()
        if self.manual_run:
            await self._async_finish_manual_run(now)
        else:
            await self._async_save_store()
            self._notify_listeners()
        return True

    async def _async_publish_retained_dock_stop(
        self,
        now: datetime,
    ) -> bool:
        """Publish one persisted mop-dock clean stop after task acknowledgement."""
        guard = self.retained_task_guard
        if not guard:
            return False
        if not bool(
            self.config.get(
                CONF_STALE_RESUME_AUTO_CLEAR,
                DEFAULT_STALE_RESUME_AUTO_CLEAR,
            )
        ):
            await self._async_mark_retained_task_operator_required(
                "the retained task cleared but automatic dock-pause recovery is disabled"
            )
            return False
        identifier = self.config.get(CONF_IDENTIFIER)
        if not identifier:
            await self._async_mark_retained_task_operator_required(
                "the retained task cleared but dock pause requires a configured Valetudo identifier"
            )
            return False
        if guard.dock_clear_attempts >= 1:
            await self._async_mark_retained_task_operator_required(
                "the dock pause already consumed its one automatic clean-stop attempt"
            )
            return False

        guard.phase = RETAINED_TASK_PHASE_DOCK_CLEAR_PENDING
        guard.dock_clear_attempts = 1
        guard.dock_clear_requested_at = now.isoformat()
        guard.dock_clear_deadline = (
            now
            + timedelta(
                seconds=int(
                    self.config.get(
                        CONF_STALE_RESUME_CLEAR_TIMEOUT,
                        DEFAULT_STALE_RESUME_CLEAR_TIMEOUT,
                    )
                )
            )
        ).isoformat()
        guard.stale_deadline = None
        guard.reason = "publishing one bounded mop-dock clean stop"
        if self.session and self.session.active:
            self.session.preflight_complete = False
            self.session.blocked_reason = guard.reason
        await self._async_save_store()
        self._notify_listeners()

        try:
            await self.hass.services.async_call(
                DOMAIN,
                SERVICE_DOCK_ACTION,
                {
                    CONF_IDENTIFIER: identifier,
                    "capability": "clean",
                    "action": "stop",
                },
                blocking=True,
            )
        except Exception as err:  # noqa: BLE001
            await self._async_mark_retained_task_operator_required(
                f"the one mop-dock clean-stop attempt failed: {err}"
            )
            return False

        guard.dock_clear_published_at = dt_util.utcnow().isoformat()
        guard.phase = RETAINED_TASK_PHASE_DOCK_VERIFYING
        guard.reason = "waiting for dock idle acknowledgement"
        await self._async_save_store()
        self._notify_listeners()
        self._schedule_retained_task_timer()
        return False

    async def _async_reconcile_retained_task(
        self,
        now: datetime,
        *,
        allow_clear: bool,
        restored: bool = False,
    ) -> bool:
        """Reconcile retained-task ownership before changing settings or dispatching."""
        vacuum_state, status_flag, dock_status, _error = (
            self._retained_task_snapshot()
        )
        guard = self.retained_task_guard
        if self.active_run:
            guard = self._ensure_retained_task_guard(
                now,
                owner=RETAINED_TASK_OWNER_COORDINATOR,
                phase=(
                    RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
                    if self.active_run.native_resume_pending
                    else RETAINED_TASK_PHASE_OBSERVED_ACTIVE
                ),
                run=self.active_run,
            )
        elif self.manual_run:
            guard = self._ensure_retained_task_guard(
                now,
                owner=RETAINED_TASK_OWNER_MANUAL,
                phase=RETAINED_TASK_PHASE_OBSERVED_ACTIVE,
                reason="Observed manual cleaning",
                run=self.manual_run,
            )
        elif guard is None and (
            vacuum_state in _ACTIVE_RETAINED_VACUUM_STATES
            or status_flag == "resumable"
        ):
            guard = self._ensure_retained_task_guard(
                now,
                owner=RETAINED_TASK_OWNER_UNKNOWN,
                phase=(
                    RETAINED_TASK_PHASE_STALE_CANDIDATE
                    if status_flag == "resumable"
                    else RETAINED_TASK_PHASE_OBSERVED_ACTIVE
                ),
                reason=(
                    "Valetudo reported an unowned resumable task"
                    if status_flag == "resumable"
                    else "Observed external vacuum activity"
                ),
            )

        if guard is None:
            if self.session and self.session.active:
                self.session.preflight_complete = True
                if (
                    self.session.blocker_code
                    and self.session.blocker_code.startswith("task.")
                ):
                    await self._async_clear_recoverable_wait()
            self._cancel_retained_task_timer()
            return True

        if restored:
            guard.coherent_since = now.isoformat()
        self._update_retained_task_snapshot(
            guard,
            now,
            material=False,
            reset_coherent=not restored,
        )
        blocker = self._current_blocker()
        if (
            self.session
            and self.session.active
            and not self.active_run
            and not self.manual_run
            and blocker
            and blocker.recoverable
            and blocker.vacuum_only_safe
            and guard.owner == RETAINED_TASK_OWNER_UNKNOWN
            and guard.phase == RETAINED_TASK_PHASE_OPERATOR_REQUIRED
            and guard.clear_attempts == 0
            and guard.clear_requested_at is None
            and guard.clear_published_at is None
            and guard.clear_acknowledged_at is None
            and guard.dock_clear_attempts == 0
            and guard.dock_clear_requested_at is None
            and guard.dock_clear_published_at is None
            and guard.dock_clear_acknowledged_at is None
            and status_flag == "none"
            and vacuum_state in {"docked", "idle", "error", "charging"}
            and dock_status in _STALE_CLEAR_DOCK_STATES
        ):
            guard.phase = RETAINED_TASK_PHASE_CLEARED
            guard.reason = (
                "Retained task yielded to safe vacuum-only resource recovery"
            )
            guard.operator_required_reason = None
            guard.stale_deadline = None
            guard.observation_deadline = None
            guard.clear_deadline = None
            guard.dock_clear_deadline = None
            self.session.preflight_complete = True
            if (
                self.session.blocker_code
                and self.session.blocker_code.startswith("task.")
            ):
                await self._async_clear_recoverable_wait()
            self._cancel_retained_task_timer()
            await self._async_save_store()
            self._notify_listeners()
            return True
        if guard.phase == RETAINED_TASK_PHASE_CLEARED:
            if (
                status_flag != "resumable"
                and vacuum_state not in _ACTIVE_RETAINED_VACUUM_STATES
            ):
                if self.session and self.session.active:
                    self.session.preflight_complete = True
                return True
            guard.phase = (
                RETAINED_TASK_PHASE_STALE_CANDIDATE
                if status_flag == "resumable"
                else RETAINED_TASK_PHASE_OBSERVED_ACTIVE
            )
            guard.first_observed_at = now.isoformat()
            guard.last_material_activity_at = now.isoformat()
            guard.clear_attempts = 0
            guard.clear_requested_at = None
            guard.clear_published_at = None
            guard.clear_acknowledged_at = None
            guard.dock_clear_requested_at = None
            guard.dock_clear_published_at = None
            guard.dock_clear_deadline = None
            guard.dock_clear_attempts = 0
            guard.dock_clear_acknowledged_at = None
            guard.operator_notification_sent = False

        if self.active_run and self.active_run.native_resume_pending:
            guard.owner = RETAINED_TASK_OWNER_COORDINATOR
            guard.phase = RETAINED_TASK_PHASE_OWNED_NATIVE_RESUME
            guard.reason = self.active_run.suspend_reason
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.blocked_reason = guard.reason
            await self._async_save_store()
            self._notify_listeners()
            return False

        if vacuum_state in _ACTIVE_RETAINED_VACUUM_STATES:
            was_operator_required = (
                guard.phase == RETAINED_TASK_PHASE_OPERATOR_REQUIRED
            )
            guard.phase = RETAINED_TASK_PHASE_OBSERVED_ACTIVE
            guard.reason = (
                f"{guard.owner} retained task is currently {vacuum_state}"
            )
            if was_operator_required:
                guard.operator_required_reason = None
                guard.operator_notification_sent = False
            if guard.observation_deadline is None:
                guard.observation_deadline = (
                    (parse_datetime(guard.first_observed_at) or now)
                    + timedelta(
                        seconds=int(
                            self.config.get(
                                CONF_NATIVE_RESUME_TIMEOUT,
                                DEFAULT_NATIVE_RESUME_TIMEOUT,
                            )
                        )
                    )
                ).isoformat()
            observation_deadline = parse_datetime(guard.observation_deadline)
            if (
                allow_clear
                and observation_deadline is not None
                and now >= observation_deadline
            ):
                await self._async_mark_retained_task_operator_required(
                    "an external or manual vacuum task remained active beyond its observation deadline"
                )
                return False
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.blocked_reason = guard.reason
            await self._async_save_store()
            self._notify_listeners()
            self._schedule_retained_task_timer()
            return False

        guard.observation_deadline = None
        status_configured = bool(self.config.get(CONF_STATUS_FLAG_ENTITY))
        status_clear = not status_configured or status_flag not in {
            None,
            "unknown",
            "unavailable",
            "resumable",
        }
        dock_configured = bool(self.config.get(CONF_DOCK_STATUS_ENTITY))
        dock_available = not dock_configured or dock_status not in {
            None,
            "unknown",
            "unavailable",
        }
        error_available = self._configured_error_is_available()
        error_clear = self._configured_error_is_clear()
        task_clear_error_safe = bool(
            error_available
            and (
                error_clear
                or self._error_is_mop_resource(self.error_state)
            )
        )
        coherent_since = parse_datetime(guard.coherent_since) or now
        settle_seconds = int(
            self.config.get(
                CONF_STALE_RESUME_SETTLE,
                DEFAULT_STALE_RESUME_SETTLE,
            )
        )
        settle_deadline = coherent_since + timedelta(seconds=settle_seconds)

        if guard.phase == RETAINED_TASK_PHASE_CLEAR_PENDING:
            if guard.clear_published_at is None:
                await self._async_mark_retained_task_operator_required(
                    "a persisted stale-task clear intent has unknown command delivery after restart"
                )
                return False
            guard.phase = RETAINED_TASK_PHASE_VERIFYING

        if guard.phase == RETAINED_TASK_PHASE_DOCK_CLEAR_PENDING:
            if guard.dock_clear_published_at is None:
                await self._async_mark_retained_task_operator_required(
                    "a persisted dock clean-stop intent has unknown command delivery after restart"
                )
                return False
            guard.phase = RETAINED_TASK_PHASE_DOCK_VERIFYING

        vacuum_ack_safe = vacuum_state in {"docked", "idle"}
        dock_idle = not dock_configured or dock_status in _READY_DOCK_STATES
        if guard.phase in {
            RETAINED_TASK_PHASE_VERIFYING,
            RETAINED_TASK_PHASE_DOCK_VERIFYING,
            RETAINED_TASK_PHASE_OPERATOR_REQUIRED,
        } and status_clear:
            if (
                vacuum_ack_safe
                and dock_available
                and dock_idle
                and task_clear_error_safe
            ):
                if guard.clear_attempts:
                    guard.clear_acknowledged_at = (
                        guard.clear_acknowledged_at or now.isoformat()
                    )
                if now >= settle_deadline:
                    return await self._async_acknowledge_retained_task_clear(now)
                guard.clear_deadline = None
                guard.dock_clear_deadline = None
                guard.stale_deadline = settle_deadline.isoformat()
                if self.session and self.session.active:
                    self.session.preflight_complete = False
                    self.session.blocked_reason = (
                        "retained task cleared; waiting for dock settle"
                    )
                await self._async_save_store()
                self._notify_listeners()
                self._schedule_retained_task_timer()
                return False

        if (
            guard.phase == RETAINED_TASK_PHASE_VERIFYING
            and status_clear
            and vacuum_ack_safe
            and dock_available
            and dock_status == "pause"
            and task_clear_error_safe
        ):
            guard.clear_acknowledged_at = (
                guard.clear_acknowledged_at or now.isoformat()
            )
            guard.clear_deadline = None
            if now < settle_deadline:
                guard.stale_deadline = settle_deadline.isoformat()
                guard.reason = (
                    "retained task cleared; waiting for stable dock pause"
                )
                if self.session and self.session.active:
                    self.session.preflight_complete = False
                    self.session.blocked_reason = guard.reason
                await self._async_save_store()
                self._notify_listeners()
                self._schedule_retained_task_timer()
                return False
            return await self._async_publish_retained_dock_stop(now)

        if guard.phase == RETAINED_TASK_PHASE_VERIFYING:
            clear_deadline = parse_datetime(guard.clear_deadline)
            if clear_deadline is not None and now >= clear_deadline:
                await self._async_mark_retained_task_operator_required(
                    "the stale-task stop was published but the robot did not acknowledge a clear task"
                )
                return False
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.blocked_reason = (
                    "waiting for stale-task stop acknowledgement"
                )
            await self._async_save_store()
            self._notify_listeners()
            self._schedule_retained_task_timer()
            return False

        if guard.phase == RETAINED_TASK_PHASE_DOCK_VERIFYING:
            dock_clear_deadline = parse_datetime(guard.dock_clear_deadline)
            if (
                dock_clear_deadline is not None
                and now >= dock_clear_deadline
            ):
                await self._async_mark_retained_task_operator_required(
                    "the mop-dock clean stop was published but dock status did not acknowledge idle"
                )
                return False
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.blocked_reason = (
                    "waiting for dock idle acknowledgement"
                )
            await self._async_save_store()
            self._notify_listeners()
            self._schedule_retained_task_timer()
            return False

        if guard.phase == RETAINED_TASK_PHASE_OPERATOR_REQUIRED:
            retry_at = parse_datetime(guard.observation_deadline)
            if retry_at is None or now >= retry_at:
                retry_at = now + timedelta(
                    seconds=max(
                        1,
                        int(
                            self.config.get(
                                CONF_BLOCKED_SESSION_TIMEOUT,
                                DEFAULT_BLOCKED_SESSION_TIMEOUT,
                            )
                        ),
                    )
                )
                guard.observation_deadline = retry_at.isoformat()
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.enter_recovery(
                    code="task.operator_required",
                    disposition=BLOCKER_RECOVERABLE,
                    reason=(
                        guard.operator_required_reason
                        or "a retained vacuum task requires operator action"
                    ),
                    phase="operator_required",
                    next_retry_at=retry_at.isoformat(),
                    waiting_for_physical_fix=True,
                    operator_action=(
                        "Clear the retained task or dock condition."
                    ),
                )
            if allow_clear and not guard.operator_notification_sent:
                await self._async_mark_retained_task_operator_required(
                    guard.operator_required_reason
                    or "a retained vacuum task requires operator action"
                )
            else:
                await self._async_save_store()
                self._notify_listeners()
                self._schedule_retained_task_timer()
            return False

        if (
            status_clear
            and vacuum_ack_safe
            and dock_available
            and dock_idle
            and task_clear_error_safe
        ):
            if now >= settle_deadline:
                return await self._async_acknowledge_retained_task_clear(now)
            guard.stale_deadline = settle_deadline.isoformat()
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.blocked_reason = (
                    "retained task appears clear; waiting for dock settle"
                )
            await self._async_save_store()
            self._notify_listeners()
            self._schedule_retained_task_timer()
            return False

        if status_flag != "resumable":
            guard.phase = RETAINED_TASK_PHASE_STALE_CANDIDATE
            guard.reason = "retained task state is not yet coherently clear"
            unresolved_deadline = coherent_since + timedelta(
                seconds=int(
                    self.config.get(
                        CONF_STALE_RESUME_AGE,
                        DEFAULT_STALE_RESUME_AGE,
                    )
                )
            )
            guard.stale_deadline = unresolved_deadline.isoformat()
            if allow_clear and now >= unresolved_deadline:
                await self._async_mark_retained_task_operator_required(
                    "the retained task did not reach a clear idle dock state before its deadline"
                )
                return False
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.blocked_reason = guard.reason
            await self._async_save_store()
            self._notify_listeners()
            self._schedule_retained_task_timer()
            return False

        guard.phase = RETAINED_TASK_PHASE_STALE_CANDIDATE
        last_material = parse_datetime(guard.last_material_activity_at) or now
        stale_deadline = last_material + timedelta(
            seconds=int(
                self.config.get(
                    CONF_STALE_RESUME_AGE,
                    DEFAULT_STALE_RESUME_AGE,
                )
            )
        )
        ready_at = max(stale_deadline, settle_deadline)
        guard.stale_deadline = ready_at.isoformat()
        stale_clear_snapshot = bool(
            vacuum_state == "docked"
            and dock_available
            and dock_status in _STALE_CLEAR_DOCK_STATES
            and error_available
            and error_clear
        )
        if now < ready_at:
            guard.reason = "unowned resumable task has not reached its stale deadline"
            if self.session and self.session.active:
                self.session.preflight_complete = False
                self.session.blocked_reason = guard.reason
            await self._async_save_store()
            self._notify_listeners()
            self._schedule_retained_task_timer()
            return False

        if not allow_clear:
            guard.reason = "unowned resumable task is stale and awaiting session preflight"
            await self._async_save_store()
            self._notify_listeners()
            return False

        if not stale_clear_snapshot:
            await self._async_mark_retained_task_operator_required(
                "the stale resumable task is not in a coherent docked and error-clear state"
            )
            return False

        if not bool(
            self.config.get(
                CONF_STALE_RESUME_AUTO_CLEAR,
                DEFAULT_STALE_RESUME_AUTO_CLEAR,
            )
        ):
            await self._async_mark_retained_task_operator_required(
                "a stale docked resumable task was detected and automatic clearing is disabled"
            )
            return False

        if guard.clear_attempts >= 1:
            await self._async_mark_retained_task_operator_required(
                "the stale resumable task already consumed its one automatic clear attempt"
            )
            return False

        guard.phase = RETAINED_TASK_PHASE_CLEAR_PENDING
        guard.clear_attempts = 1
        guard.clear_requested_at = now.isoformat()
        guard.clear_deadline = (
            now
            + timedelta(
                seconds=int(
                    self.config.get(
                        CONF_STALE_RESUME_CLEAR_TIMEOUT,
                        DEFAULT_STALE_RESUME_CLEAR_TIMEOUT,
                    )
                )
            )
        ).isoformat()
        guard.stale_deadline = None
        guard.reason = "publishing one bounded stop for a stale docked task"
        if self.session and self.session.active:
            self.session.preflight_complete = False
            self.session.blocked_reason = guard.reason
        await self._async_save_store()
        self._notify_listeners()

        try:
            await self.hass.services.async_call(
                "vacuum",
                "stop",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=True,
            )
        except Exception as err:  # noqa: BLE001
            await self._async_mark_retained_task_operator_required(
                f"the one stale-task stop attempt failed to publish: {err}"
            )
            return False

        guard.clear_published_at = dt_util.utcnow().isoformat()
        guard.phase = RETAINED_TASK_PHASE_VERIFYING
        guard.reason = "waiting for stale-task stop acknowledgement"
        await self._async_save_store()
        self._notify_listeners()
        self._schedule_retained_task_timer()
        return False

    def _clear_retained_task_guard(self) -> None:
        """Clear retained-task ownership after definitive task completion."""
        self.retained_task_guard = None
        self._cancel_retained_task_timer()

    def _current_blocker(
        self,
        resources: ResourceState | None = None,
    ) -> BlockerClassification | None:
        """Return the typed blocker for the current coherent snapshot."""
        resources = resources or self._resource_state()
        battery_entity = self.config.get(CONF_BATTERY_ENTITY)
        return classify_blocker(
            resources,
            vacuum_state=self._state(self.vacuum_entity),
            dock_status=self._state(self.config.get(CONF_DOCK_STATUS_ENTITY)),
            status_flag=(
                self._status_flag()
                if self.config.get(CONF_STATUS_FLAG_ENTITY)
                else None
            ),
            battery=(
                parse_float(self._state(battery_entity))
                if battery_entity
                else None
            ),
            minimum_battery=(
                float(
                    self.config.get(
                        CONF_MIN_BATTERY,
                        DEFAULT_MIN_BATTERY,
                    )
                )
                if battery_entity
                else None
            ),
        )

    @property
    def preserved_room_ids(self) -> list[str]:
        """Return the queue retained while a recoverable blocker is active."""
        if not self.session:
            return []
        room_ids = [room.room_id for room in self.pending_rooms]
        if self._dispatch_held_room_ids_snapshot():
            attempted = set(self.session.attempted_room_ids)
            completed = set(self.session.completed_room_ids)
            uncertain = set(self.session.uncertain_room_ids)
            auto_clean_day = self._current_auto_clean_day()
            for room in self._auto_clean_rooms():
                if (
                    room.room_id in completed
                    or room.room_id in uncertain
                    or (
                        room.room_id in attempted
                        and room.room_id
                        not in self.session.dispatch_failure_counts
                    )
                    or room_auto_cleaned_on(
                        self.ledgers.get(room.room_id, RoomLedger()),
                        auto_clean_day,
                    )
                ):
                    continue
                if room.room_id not in room_ids:
                    room_ids.append(room.room_id)
        for room_id in self.session.dispatch_failure_counts:
            if room_id in self.room_by_id and room_id not in room_ids:
                room_ids.append(room_id)
        recovery_room_id = self.session.pending_recovery_room_id
        if (
            recovery_room_id
            and self.session.can_retry_room(recovery_room_id)
            and recovery_room_id in self.room_by_id
            and recovery_room_id not in room_ids
        ):
            room_ids.append(recovery_room_id)
        for room_id in self.session.deferred_full_clean_room_ids:
            if room_id not in room_ids:
                room_ids.append(room_id)
        return room_ids

    def _dispatch_state_fingerprint(self) -> str:
        """Return a stable robot-state fingerprint for dispatch retry gating."""
        resources = self._resource_state()
        blocker = self._current_blocker(resources)
        battery_entity = self.config.get(CONF_BATTERY_ENTITY)
        battery = (
            parse_float(self._state(battery_entity))
            if battery_entity
            else None
        )
        minimum = float(
            self.config.get(CONF_MIN_BATTERY, DEFAULT_MIN_BATTERY)
        )
        battery_bucket = (
            "unavailable"
            if battery_entity and battery is None
            else "low"
            if battery is not None and battery < minimum
            else "ready"
        )
        return json.dumps(
            {
                "vacuum": normalize_state(self._state(self.vacuum_entity)),
                "status": self._status_flag(),
                "dock": normalize_state(
                    self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
                ),
                "blocker": blocker.code if blocker else None,
                "battery": battery_bucket,
                "fresh_water": normalize_state(resources.fresh_water),
                "dirty_water": normalize_state(resources.dirty_water),
                "detergent": normalize_state(resources.detergent),
                "dustbag": normalize_state(resources.dustbag),
                "mode": normalize_state(
                    self._state(self.config.get(CONF_MODE_ENTITY))
                ),
            },
            sort_keys=True,
        )

    def _record_dispatch_failure(
        self,
        run: ActiveRun,
        reason: str,
    ) -> None:
        """Persist bounded per-room dispatch backoff and escalation."""
        session = self.session
        room_id = run.room_id
        if not session or not room_id:
            return
        count = session.dispatch_failure_counts.get(room_id, 0) + 1
        session.dispatch_failure_counts[room_id] = count
        session.dispatch_failure_reasons[room_id] = reason
        session.dispatch_failure_fingerprints[room_id] = (
            self._dispatch_state_fingerprint()
        )
        if count >= _MAX_AUTOMATIC_DISPATCH_FAILURES:
            if room_id not in session.dispatch_escalated_room_ids:
                session.dispatch_escalated_room_ids.append(room_id)
            session.dispatch_retry_not_before.pop(room_id, None)
            return
        base_seconds = max(
            1,
            int(
                self.config.get(
                    CONF_BLOCKED_SESSION_TIMEOUT,
                    DEFAULT_BLOCKED_SESSION_TIMEOUT,
                )
            ),
        )
        cap_seconds = max(
            base_seconds,
            int(
                self.config.get(
                    CONF_NATIVE_RESUME_TIMEOUT,
                    DEFAULT_NATIVE_RESUME_TIMEOUT,
                )
            ),
        )
        delay_seconds = min(
            cap_seconds,
            base_seconds * (2 ** max(0, count - 1)),
        )
        session.dispatch_retry_not_before[room_id] = (
            dt_util.utcnow() + timedelta(seconds=delay_seconds)
        ).isoformat()

    async def _async_reconcile_dispatch_failures(self) -> set[str]:
        """Clear escalations only after a material robot-state change."""
        session = self.session
        if not session or not session.dispatch_failure_counts:
            return set()
        current_fingerprint = self._dispatch_state_fingerprint()
        changed = False
        for room_id in list(session.dispatch_failure_counts):
            room = self.room_by_id.get(room_id)
            if not room or not self._room_auto_clean_enabled(room):
                session.clear_dispatch_failure(room_id)
                changed = True
            elif (
                session.dispatch_failure_fingerprints.get(room_id)
                != current_fingerprint
            ):
                session.clear_dispatch_failure(room_id)
                changed = True
        if changed:
            await self._async_save_store()
            self._notify_listeners()
        now = dt_util.utcnow()
        held: set[str] = set()
        for room_id in session.dispatch_failure_counts:
            if room_id in session.dispatch_escalated_room_ids:
                held.add(room_id)
                continue
            retry_at = parse_datetime(
                session.dispatch_retry_not_before.get(room_id)
            )
            if retry_at is not None and now < retry_at:
                held.add(room_id)
        if not held:
            return set()
        return {
            room.room_id
            for room in self._auto_clean_rooms()
        }

    def _dispatch_held_room_ids_snapshot(self) -> set[str]:
        """Return held rooms without mutating persisted retry state."""
        session = self.session
        if not session:
            return set()
        now = dt_util.utcnow()
        held = set(session.dispatch_escalated_room_ids)
        held.update(
            room_id
            for room_id, value in session.dispatch_retry_not_before.items()
            if (retry_at := parse_datetime(value)) is not None
            and now < retry_at
        )
        if not held:
            return set()
        return {
            room.room_id
            for room in self._auto_clean_rooms()
        }

    async def _async_wait_for_dispatch_failures(
        self,
        held_room_ids: set[str],
    ) -> None:
        """Wait without republishing until backoff or material state change."""
        session = self.session
        if not session or not held_room_ids:
            return
        escalated = bool(
            held_room_ids.intersection(session.dispatch_escalated_room_ids)
        )
        deadlines = [
            parsed
            for room_id in held_room_ids
            if (
                parsed := parse_datetime(
                    session.dispatch_retry_not_before.get(room_id)
                )
            ) is not None
        ]
        retry_at = min(deadlines) if deadlines else (
            dt_util.utcnow()
            + timedelta(
                seconds=max(
                    1,
                    int(
                        self.config.get(
                            CONF_BLOCKED_SESSION_TIMEOUT,
                            DEFAULT_BLOCKED_SESSION_TIMEOUT,
                        )
                    ),
                )
            )
        )
        max_count = max(
            session.dispatch_failure_counts.get(room_id, 0)
            for room_id in held_room_ids
        )
        blocker = BlockerClassification(
            (
                "dispatch.operator_required"
                if escalated
                else "dispatch.retry_backoff"
            ),
            BLOCKER_RECOVERABLE,
            "dispatch",
            (
                f"Room dispatch has failed {max_count} times; "
                + (
                    "waiting for a material robot-state change"
                    if escalated
                    else "waiting before the next bounded retry"
                )
            ),
            (
                "Check the robot, dock, and Valetudo command path; changing "
                "their state will release the preserved room."
            ),
            waiting_for_physical_fix=escalated,
        )
        await self._async_arm_blocked_session_watchdog(
            blocker.reason,
            blocker=blocker,
            retry_at=retry_at,
        )

    async def _async_enter_recoverable_wait(
        self,
        blocker: BlockerClassification,
        *,
        phase: str,
        next_retry_at: datetime | None,
        retry_cadence_reason: str | None = None,
    ) -> None:
        """Persist one deduplicated recoverable wait and operator alert."""
        session = self.session
        if not session:
            return
        session.enter_recovery(
            code=blocker.code,
            disposition=blocker.disposition,
            reason=blocker.reason,
            phase=phase,
            next_retry_at=(
                next_retry_at.isoformat() if next_retry_at else None
            ),
            waiting_for_physical_fix=blocker.waiting_for_physical_fix,
            operator_action=blocker.operator_action,
            retry_cadence_reason=retry_cadence_reason,
        )
        if (
            blocker.code == "recovery.degraded_preparation"
        ):
            fingerprint = blocker.code
        elif (
            blocker.category == "dispatch"
            and blocker.code != "dispatch.operator_required"
        ):
            fingerprint = "dispatch.recovery"
        else:
            fingerprint = ":".join(
                (
                    blocker.code,
                    blocker.category,
                    blocker.operator_action,
                )
            )
        should_notify = (
            session.last_recovery_notification_fingerprint != fingerprint
        )
        session.recovery_notification_fingerprint = fingerprint
        if should_notify:
            session.recovery_notification_attempts += 1
            session.last_recovery_notification_fingerprint = fingerprint
            session.last_recovery_notification_at = utcnow_iso()
        await self._async_save_store()
        self._notify_listeners()
        if not should_notify:
            return

        preserved = self.preserved_room_ids
        retry_text = (
            f" Next check: {session.next_retry_at}."
            if session.next_retry_at
            else ""
        )
        rooms_text = (
            f" Preserved rooms: {', '.join(preserved)}."
            if preserved
            else ""
        )
        try:
            await self._async_send_notification(
                f"{self.name.replace(' Coordinator', '')} · Waiting",
                (
                    f"{blocker.reason}. {blocker.operator_action}"
                    f" Recovery phase: {phase}.{retry_text}{rooms_text}"
                ),
            )
        except Exception:  # noqa: BLE001
            _LOGGER.exception(
                "Could not send recoverable-blocker notification for %s",
                self.name,
            )
            return
        session.recovery_notification_sent = True
        await self._async_save_store()
        self._notify_listeners()

    async def _async_clear_recoverable_wait(self) -> None:
        """Clear a recovered blocker and announce the preserved-session resume."""
        session = self.session
        if not session or not session.blocker_code:
            return
        should_announce = bool(
            session.recovery_notification_sent
            and not session.recovery_announcement_sent
        )
        recovered_reason = session.blocked_reason
        if getattr(self, "_blocked_session_watchdog_cancel", None) is not None:
            self._blocked_session_watchdog_cancel()
            self._blocked_session_watchdog_cancel = None
        session.clear_recovery()
        session.recovery_announcement_sent = should_announce
        await self._async_save_store()
        self._notify_listeners()
        if not should_announce:
            return
        try:
            await self._async_send_notification(
                f"{self.name.replace(' Coordinator', '')} · Resuming",
                (
                    f"Recovered from {recovered_reason or 'the prior blocker'}. "
                    "Automatic cleaning is resuming the preserved away session."
                ),
            )
        except Exception:  # noqa: BLE001
            _LOGGER.exception(
                "Could not send recovery notification for %s",
                self.name,
            )

    async def _async_terminalize_unrecoverable(
        self,
        blocker: BlockerClassification,
    ) -> None:
        """Permanently end only an explicitly unrecoverable session."""
        if not self.session:
            return
        self._clear_blocked_session_watchdog()
        self.session.clear_recovery()
        self.session.active = False
        self.session.terminal_reason = "unrecoverable_error"
        self.session.terminal_message = blocker.reason
        self.session.terminal_cause = blocker.code
        self.session.needs_help = True
        self.session.blocker_code = blocker.code
        self.session.blocker_disposition = blocker.disposition
        self.session.blocker_operator_action = blocker.operator_action
        self.session.waiting_for_physical_fix = True
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    async def _async_arm_blocked_session_watchdog(
        self,
        reason: str | None = None,
        *,
        blocker: BlockerClassification | None = None,
        retry_at: datetime | None = None,
    ) -> None:
        """Persist a recoverable wait and schedule its next bounded probe."""
        session = self.session
        if not session or not session.active or self.active_run:
            return
        blocker = blocker or self._current_blocker()
        if blocker is None:
            blocker = BlockerClassification(
                "session.waiting",
                BLOCKER_RECOVERABLE,
                "recovery",
                reason or "Automatic cleaning is waiting for a safe dispatch state",
                "No action is required unless the condition persists.",
            )
        if not blocker.recoverable:
            await self._async_terminalize_unrecoverable(blocker)
            return
        now = dt_util.utcnow()
        timeout_seconds, retry_cadence_reason = (
            self._blocked_session_timeout_details(
                reason or blocker.reason,
                blocker=blocker,
            )
        )
        candidate_deadline = (
            retry_at
            if retry_at is not None
            else now
            + timedelta(seconds=timeout_seconds)
        )
        deadline = parse_datetime(session.next_retry_at)
        selected_cadence_reason = session.retry_cadence_reason
        if retry_at is not None:
            deadline = candidate_deadline
            selected_cadence_reason = "explicit_deadline"
        elif (
            deadline is None
            or deadline <= now
            or candidate_deadline < deadline
        ):
            deadline = candidate_deadline
            selected_cadence_reason = retry_cadence_reason
        elif selected_cadence_reason is None:
            selected_cadence_reason = "restored_deadline"
        previous_deadline = session.next_retry_at
        previous_cadence_reason = session.retry_cadence_reason
        await self._async_enter_recoverable_wait(
            blocker,
            phase=(
                "operator_required"
                if blocker.waiting_for_physical_fix
                else "waiting_for_retry"
            ),
            next_retry_at=deadline,
            retry_cadence_reason=selected_cadence_reason,
        )
        if (
            previous_deadline != session.next_retry_at
            or previous_cadence_reason != session.retry_cadence_reason
        ):
            _LOGGER.info(
                "%s recovery wait: blocker=%s cadence=%s next_retry_at=%s "
                "next_candidate_room=%s pending_recovery_room=%s",
                self.name,
                blocker.code,
                session.retry_cadence_reason,
                session.next_retry_at,
                self.next_candidate_room_id,
                session.pending_recovery_room_id,
            )
        if getattr(self, "_blocked_session_watchdog_cancel", None) is not None:
            self._blocked_session_watchdog_cancel()
            self._blocked_session_watchdog_cancel = None
        remaining = (deadline - now).total_seconds()

        def timer_finished(_now: datetime) -> None:
            self._blocked_session_watchdog_cancel = None
            schedule_hass_task(
                self.hass,
                self._async_expire_blocked_session_serialized(),
            )

        if remaining <= 0:
            timer_finished(dt_util.utcnow())
            return
        self._blocked_session_watchdog_cancel = async_call_later(
            self.hass,
            remaining,
            timer_finished,
        )

    def _blocked_session_timeout_seconds(self, reason: str | None) -> int:
        """Return the bounded wait for the current blocked condition."""
        timeout, _cadence_reason = self._blocked_session_timeout_details(
            reason,
            blocker=self._current_blocker(),
        )
        return timeout

    def _blocked_session_timeout_details(
        self,
        reason: str | None,
        *,
        blocker: BlockerClassification | None,
    ) -> tuple[int, str]:
        """Return the recovery delay and the policy that selected it."""
        normalized_reason = (normalize_state(reason) or "").lower()
        battery_entity = self.config.get(CONF_BATTERY_ENTITY)
        battery = parse_float(self._state(battery_entity)) if battery_entity else None
        minimum_battery = float(
            self.config.get(CONF_MIN_BATTERY, DEFAULT_MIN_BATTERY)
        )
        if (
            blocker
            and blocker.category == "navigation"
        ) or (
            self.session
            and self.session.pending_recovery_policy
            == _NAVIGATION_RETRY_LATER_POLICY
        ):
            return (
                max(
                    1,
                    int(
                        self.config.get(
                            CONF_BLOCKED_SESSION_TIMEOUT,
                            DEFAULT_BLOCKED_SESSION_TIMEOUT,
                        )
                    ),
                ),
                "navigation_recheck",
            )
        if is_low_battery_error(reason):
            long_recovery_reason = "low_battery_error"
        elif "battery" in normalized_reason:
            long_recovery_reason = "battery_reason"
        elif "charging" in normalized_reason:
            long_recovery_reason = "charging_reason"
        elif normalize_state(self._state(self.vacuum_entity)) == "charging":
            long_recovery_reason = "vacuum_charging"
        elif self.native_resume_pending:
            long_recovery_reason = "native_resume_pending"
        elif battery is not None and battery < minimum_battery:
            long_recovery_reason = "battery_below_minimum"
        else:
            long_recovery_reason = None
        if long_recovery_reason:
            return (
                max(
                    1,
                    int(
                        self.config.get(
                            CONF_NATIVE_RESUME_TIMEOUT,
                            DEFAULT_NATIVE_RESUME_TIMEOUT,
                        )
                    ),
                ),
                long_recovery_reason,
            )
        return (
            max(
                1,
                int(
                    self.config.get(
                        CONF_BLOCKED_SESSION_TIMEOUT,
                        DEFAULT_BLOCKED_SESSION_TIMEOUT,
                    )
                ),
            ),
            "blocked_session_recheck",
        )

    async def _async_expire_blocked_session_serialized(self) -> None:
        """Re-evaluate and re-arm recoverable waits without losing the queue."""
        async with self._event_lock:
            if (
                not self.session
                or not self.session.active
                or self.active_run
            ):
                return
            self._blocked_watchdog_expiring = True
            try:
                await self._async_maybe_start_next_room()
            finally:
                self._blocked_watchdog_expiring = False
            if not self.session or not self.session.active or self.active_run:
                return
            if self._blocked_session_watchdog_cancel is not None:
                return
            blocker = self._current_blocker()
            if blocker and not blocker.recoverable:
                await self._async_terminalize_unrecoverable(blocker)
                return
            await self._async_arm_blocked_session_watchdog(
                (
                    blocker.reason
                    if blocker
                    else self.session.blocked_reason
                )
            )

    def _navigation_error_return_allowed(
        self,
        run: ActiveRun,
        vacuum_state: str | None,
    ) -> bool:
        """Return whether the exact validated live error permits one return."""
        status_flag = (
            normalize_state(self._status_flag())
            if self.config.get(CONF_STATUS_FLAG_ENTITY)
            else None
        )
        return bool(
            self.navigation_error_return_enabled
            and run.cancel_recover_room
            and run.room_id in self.room_by_id
            and run.cancel_return_attempts == 0
            and run.cancel_stop_acknowledged_at
            and vacuum_state == "error"
            and (normalize_state(run.cancel_reason) or "").lower()
            in _VALIDATED_NAVIGATION_RETURN_ERRORS
            and (normalize_state(self.error_state) or "").lower()
            in _VALIDATED_NAVIGATION_RETURN_ERRORS
            and status_flag
            not in {"segment", "resumable", "unknown", "unavailable"}
        )

    def _clear_blocked_session_watchdog(self) -> None:
        """Clear the no-progress deadline, reason, and timer."""
        if getattr(self, "_blocked_session_watchdog_cancel", None) is not None:
            self._blocked_session_watchdog_cancel()
            self._blocked_session_watchdog_cancel = None
        if self.session:
            if (
                self.session.degraded_reason
                and (blocker := self._current_blocker())
                and blocker.vacuum_only_safe
            ):
                self.session.blocked_deadline = None
                self.session.next_retry_at = None
                self.session.retry_cadence_reason = None
                self.session.recovery_phase = "degraded_vacuum"
                return
            self.session.clear_recovery()

    def _observe_manual_run(
        self,
        entity_id: str,
        new_state: State,
        now: datetime,
    ) -> bool:
        """Update active manual run observations."""
        if not self.manual_run:
            return False
        if (
            entity_id == self.vacuum_entity
            and new_state.state == "cleaning"
            and not self.manual_run.observed_cleaning
        ):
            self.manual_run.observed_cleaning = True
            return True
        if (
            entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY)
            and new_state.state == "segment"
            and not self.manual_run.observed_segment_cleaning
        ):
            self.manual_run.observed_segment_cleaning = True
            return True
        if entity_id == self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY):
            self.manual_run.observe_estimated_room(self._room_id_from_estimated(new_state.state), now)
            return True
        return False

    async def _async_maybe_start_next_room(self) -> None:
        """Start the next room when the session and Valetudo state allow it."""
        if self.paused or not self.session or not self.session.active or self.active_run:
            self._notify_listeners()
            return
        if not self._all_people_away():
            if self.config.get(CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL):
                await self._async_cancel_session("Tracked person arrived home")
            else:
                self._notify_listeners()
            return
        if not await self._async_reconcile_retained_task(
            dt_util.utcnow(),
            allow_clear=True,
        ):
            if (
                self.session
                and self.session.active
                and not self.session.blocker_code
            ):
                guard = self.retained_task_guard
                self.session.enter_recovery(
                    code="task.retained",
                    disposition=BLOCKER_RECOVERABLE,
                    reason=(
                        guard.reason
                        if guard and guard.reason
                        else "A retained Valetudo task is being reconciled"
                    ),
                    phase="suspended",
                    next_retry_at=(
                        deadline.isoformat()
                        if (deadline := self._retained_task_next_deadline())
                        else None
                    ),
                    waiting_for_physical_fix=False,
                    operator_action=(
                        "Leave the vacuum docked while the task is reconciled."
                    ),
                )
                await self._async_save_store()
            self._notify_listeners()
            return
        if not self.session or not self.session.active:
            return
        if not self.session.settings_prepared:
            await self._async_prepare_auto_clean_settings()
            self.session.settings_prepared = True
            await self._async_save_store()
            self._notify_listeners()
        if (
            self.session.pending_recovery_room_id
            and not await self._async_recover_pending_room_failure_if_ready()
        ):
            await self._async_arm_blocked_session_watchdog(
                self.session.pending_recovery_reason
                or "waiting for recoverable room failure to clear"
            )
            self._notify_listeners()
            return
        if not self._all_people_away():
            if self.config.get(CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL):
                await self._async_cancel_session("Tracked person arrived home")
            else:
                self._notify_listeners()
            return
        if self.paused or not self.session or not self.session.active or self.active_run:
            self._notify_listeners()
            return

        dispatch_held_room_ids = await self._async_reconcile_dispatch_failures()
        resources = self._resource_state()
        blocker = self._current_blocker(resources)
        degraded_reason = (
            clean_water_vacuum_only_reason(resources)
            or (
                blocker.reason
                if blocker and blocker.vacuum_only_safe
                else None
            )
        )
        if degraded_reason:
            self._activate_vacuum_only_degraded(degraded_reason)
        if self.session.degraded_reason:
            await self._async_maybe_start_degraded_room(resources)
            return

        if blocker:
            if not blocker.recoverable:
                await self._async_terminalize_unrecoverable(blocker)
                return
            await self._async_arm_blocked_session_watchdog(blocker.reason)
            self._notify_listeners()
            return
        if not self._vacuum_ready_for_next_room():
            await self._async_arm_blocked_session_watchdog(
                self._current_blocked_reason(resources)
            )
            self._notify_listeners()
            return
        if block_reason := cleaning_block_reason(resources):
            await self._async_arm_blocked_session_watchdog(block_reason)
            self._notify_listeners()
            return

        self.session.retry_room_ids = [
            room_id
            for room_id in self.session.retry_room_ids
            if (
                (room := self.room_by_id.get(room_id))
                and self._room_auto_clean_enabled(room)
            )
        ]
        self.session.priority_retry_room_ids = [
            room_id
            for room_id in self.session.priority_retry_room_ids
            if (
                (room := self.room_by_id.get(room_id))
                and self._room_auto_clean_enabled(room)
            )
        ]

        if self.session.priority_retry_room_ids:
            priority_room = self.room_by_id.get(
                self.session.priority_retry_room_ids[0]
            )
            block_reason = cleaning_block_reason(resources)
            if priority_room and not block_reason:
                block_reason = mop_block_reason(priority_room, resources)
            if block_reason:
                await self._async_arm_blocked_session_watchdog(block_reason)
                self._notify_listeners()
                return

        selection, skipped = self._planned_normal_room(
            resources,
            assume_pending_recovery_resolved=False,
        )

        for room, reason in skipped:
            _LOGGER.info(
                "Deferring %s room %s while %s",
                self.name,
                room.room_id,
                reason,
            )

        if selection is None:
            if skipped:
                await self._async_arm_blocked_session_watchdog(skipped[0][1])
                await self._async_save_store()
                self._notify_listeners()
                return
            if dispatch_held_room_ids:
                await self._async_wait_for_dispatch_failures(
                    dispatch_held_room_ids
                )
                return
            self._clear_blocked_session_watchdog()
            self.session.active = False
            self.session.active_room_id = None
            self.session.pending_recovery_room_id = None
            self.session.pending_recovery_reason = None
            self.session.pending_recovery_priority = False
            self.session.pending_recovery_policy = None
            self.session.retry_room_ids = []
            self.session.priority_retry_room_ids = []
            self.session.terminal_reason = no_selection_terminal_reason(
                completed_room_ids=self.session.completed_room_ids,
                skipped_room_ids=self.session.skipped_room_ids,
                failed_room_ids=self.session.failed_room_ids,
                current_skipped_count=len(skipped),
                uncertain_room_ids=self.session.uncertain_room_ids,
            )
            self.session.terminal_message = self._first_session_block_reason()
            await self._async_save_store()
            self._notify_listeners()
            await self._async_maybe_send_auto_clean_summary()
            return

        await self._async_clear_recoverable_wait()
        await self._async_start_room(
            selection.room,
            vacuum_only=selection.vacuum_only,
            fallback_vacuum=selection.fallback_vacuum,
        )

    async def _async_maybe_start_degraded_room(
        self,
        resources: ResourceState,
    ) -> None:
        """Process safe vacuum-only work while dock resources are blocked."""
        session = self.session
        if not session or not session.active or self.active_run:
            return
        blocker = self._current_blocker(resources)
        if blocker is None:
            await self._async_resume_degraded_after_refill(resources)
            return
        if not blocker.recoverable:
            await self._async_terminalize_unrecoverable(blocker)
            return
        if not blocker.vacuum_only_safe:
            await self._async_arm_blocked_session_watchdog(blocker.reason)
            return
        if not await self._async_prepare_degraded_vacuuming():
            retry_at = (
                parse_datetime(session.degraded_mode_next_retry_at)
                or dt_util.utcnow()
                + timedelta(
                    seconds=max(
                        1,
                        int(
                            self.config.get(
                                CONF_BLOCKED_SESSION_TIMEOUT,
                                DEFAULT_BLOCKED_SESSION_TIMEOUT,
                            )
                        ),
                    )
                )
            )
            await self._async_arm_blocked_session_watchdog(
                "Could not prepare vacuum-only cleaning",
                blocker=BlockerClassification(
                    "recovery.degraded_preparation",
                    BLOCKER_RECOVERABLE,
                    "recovery",
                    (
                        session.degraded_mode_last_error
                        or "Could not prepare vacuum-only cleaning"
                    ),
                    "Check the dock and mode selector; preparation will retry.",
                ),
                retry_at=retry_at,
            )
            return

        resources = self._resource_state()
        blocker = self._current_blocker(resources)
        if blocker is None:
            await self._async_resume_degraded_after_refill(resources)
            return
        if not blocker.vacuum_only_safe:
            await self._async_arm_blocked_session_watchdog(blocker.reason)
            return
        selection = self._planned_degraded_room(
            resources,
            assume_pending_recovery_resolved=False,
        )
        if selection:
            if not self._vacuum_ready_for_selection(vacuum_only=True):
                await self._async_arm_blocked_session_watchdog(
                    self._current_blocked_reason(resources)
                    or session.degraded_reason
                )
                self._notify_listeners()
                return
            await self._async_enter_recoverable_wait(
                blocker,
                phase="degraded_vacuum",
                next_retry_at=None,
            )
            await self._async_start_room(
                selection.room,
                vacuum_only=selection.vacuum_only,
                fallback_vacuum=selection.fallback_vacuum,
            )
            return

        actionable_deferred = [
            room_id
            for room_id in session.deferred_full_clean_room_ids
            if room_id not in session.uncertain_room_ids
        ]
        if not actionable_deferred:
            await self._async_finish_session_no_selection()
            return
        held_room_ids = await self._async_reconcile_dispatch_failures()
        if held_room_ids:
            await self._async_wait_for_dispatch_failures(held_room_ids)
            return
        await self._async_arm_blocked_session_watchdog(blocker.reason)

    async def _async_resume_degraded_after_refill(
        self,
        resources: ResourceState,
    ) -> None:
        """Resume every deferred normal mop operation in the same session."""
        session = self.session
        if not session or not session.active or self.active_run:
            return
        held_room_ids = await self._async_reconcile_dispatch_failures()
        native_room = self._next_degraded_native_room()
        if native_room:
            if not self._vacuum_ready_for_selection(vacuum_only=True):
                await self._async_arm_blocked_session_watchdog(
                    self._current_blocked_reason(resources)
                )
                return
            await self._async_clear_recoverable_wait()
            await self._async_start_room(
                native_room,
                vacuum_only=True,
                fallback_vacuum=False,
            )
            return
        self._queue_refilled_normal_retries()
        selection, skipped = self._planned_normal_room(
            resources,
            assume_pending_recovery_resolved=False,
        )
        if selection:
            if not self._vacuum_ready_for_selection(
                vacuum_only=selection.vacuum_only
            ):
                await self._async_arm_blocked_session_watchdog(
                    self._current_blocked_reason(resources)
                )
                self._notify_listeners()
                return
            session.degraded_reason = None
            self._reset_degraded_preparation()
            await self._async_clear_recoverable_wait()
            await self._async_start_room(
                selection.room,
                vacuum_only=selection.vacuum_only,
                fallback_vacuum=False,
            )
            return
        if skipped:
            await self._async_arm_blocked_session_watchdog(skipped[0][1])
            self._notify_listeners()
            return
        if held_room_ids:
            await self._async_wait_for_dispatch_failures(held_room_ids)
            return
        actionable_deferred = [
            room_id
            for room_id in session.deferred_full_clean_room_ids
            if room_id not in session.uncertain_room_ids
        ]
        if actionable_deferred:
            await self._async_arm_blocked_session_watchdog(
                "Deferred mop rooms are waiting for a safe normal dispatch"
            )
            return
        session.degraded_reason = None
        self._reset_degraded_preparation()
        await self._async_clear_recoverable_wait()
        await self._async_finish_session_no_selection()

    def _reset_degraded_preparation(self) -> None:
        """Clear bounded degraded-preparation command metadata after recovery."""
        if not self.session:
            return
        self.session.degraded_preparation_attempted = False
        self.session.degraded_preparation_completed = False
        self.session.degraded_dock_stop_attempts = 0
        self.session.degraded_dock_stop_requested_at = None
        self.session.degraded_dock_stop_acknowledged_at = None
        self.session.degraded_mode_attempts = 0
        self.session.degraded_mode_acknowledged_at = None
        self.session.degraded_mode_next_retry_at = None
        self.session.degraded_mode_last_error = None
        self.session.degraded_mode_state_fingerprint = None

    def _activate_vacuum_only_degraded(self, reason: str) -> None:
        """Latch a safe vacuum-only lane and defer outstanding mop work."""
        session = self.session
        if not session or not session.active:
            return
        if session.degraded_reason is None:
            session.degraded_reason = reason
            session.degraded_at = utcnow_iso()
        auto_clean_day = self._current_auto_clean_day()
        completed = set(session.completed_room_ids)
        for room in self._auto_clean_rooms():
            if (
                room.mop_required
                and room.room_id not in completed
                and not room_auto_cleaned_on(
                    self.ledgers.get(room.room_id, RoomLedger()),
                    auto_clean_day,
                )
            ):
                session.defer_full_clean(room.room_id, reason)
                self._record_deferral_outcome(room, reason)

    def _activate_clean_water_degraded(self, reason: str) -> None:
        """Compatibility helper for the original clean-water degraded lane."""
        self._activate_vacuum_only_degraded(reason)

    def _sorted_auto_clean_rooms(self) -> list[RoomConfig]:
        """Return enabled rooms in the normal fairness order."""
        return sorted(
            self._auto_clean_rooms(),
            key=lambda room: room_sort_key(room, self.ledgers),
        )

    def _next_degraded_native_room(self) -> RoomConfig | None:
        """Return the next configured vacuum-only room in degraded lane one."""
        if not self.session:
            return None
        attempted = set(self.session.attempted_room_ids)
        attempted.update(self._dispatch_held_room_ids_snapshot())
        auto_clean_day = self._current_auto_clean_day()
        return next(
            (
                room
                for room in self._sorted_auto_clean_rooms()
                if not room.mop_required
                and room.room_id not in attempted
                and not room_auto_cleaned_on(
                    self.ledgers.get(room.room_id, RoomLedger()),
                    auto_clean_day,
                )
            ),
            None,
        )

    def _next_degraded_retry_room(self) -> RoomConfig | None:
        """Return the next safe native retry after unattempted degraded work."""
        if not self.session:
            return None
        auto_clean_day = self._current_auto_clean_day()
        return next(
            (
                room
                for room_id in self.session.retry_room_ids
                if (room := self.room_by_id.get(room_id))
                and not room.mop_required
                and self._room_auto_clean_enabled(room)
                and not room_auto_cleaned_on(
                    self.ledgers.get(room.room_id, RoomLedger()),
                    auto_clean_day,
                )
            ),
            None,
        )

    def _next_degraded_fallback_room(self) -> RoomConfig | None:
        """Return the next incomplete dual-mode room with an unused fallback token."""
        if not self.session:
            return None
        fallback_attempted = set(self.session.fallback_attempted_room_ids)
        fallback_attempted.update(self._dispatch_held_room_ids_snapshot())
        completed = set(self.session.completed_room_ids)
        auto_clean_day = self._current_auto_clean_day()
        return next(
            (
                room
                for room in self._sorted_auto_clean_rooms()
                if room.mop_required
                and room.room_id not in completed
                and room.room_id not in fallback_attempted
                and not room_auto_cleaned_on(
                    self.ledgers.get(room.room_id, RoomLedger()),
                    auto_clean_day,
                )
            ),
            None,
        )

    def _degraded_actionable_rooms(self) -> list[RoomConfig]:
        """Return the currently dispatchable degraded queue in actual lane order."""
        if not self.session or not self.session.active:
            return []
        rooms: list[RoomConfig] = []
        attempted = set(self.session.attempted_room_ids)
        held_room_ids = self._dispatch_held_room_ids_snapshot()
        attempted.update(held_room_ids)
        fallback_attempted = set(self.session.fallback_attempted_room_ids)
        fallback_attempted.update(held_room_ids)
        auto_clean_day = self._current_auto_clean_day()
        ordered = self._sorted_auto_clean_rooms()
        rooms.extend(
            room
            for room in ordered
            if not room.mop_required
            and room.room_id not in attempted
            and not room_auto_cleaned_on(
                self.ledgers.get(room.room_id, RoomLedger()),
                auto_clean_day,
            )
        )
        blocker = self._current_blocker()
        if blocker is None:
            pending_ids = {room.room_id for room in rooms}
            for room_id in self.session.priority_retry_room_ids:
                room = self.room_by_id.get(room_id)
                if (
                    room
                    and self._room_auto_clean_enabled(room)
                    and room_id not in pending_ids
                ):
                    rooms.append(room)
                    pending_ids.add(room_id)
            rooms.extend(
                room
                for room in ordered
                if room.mop_required
                and room.room_id not in attempted
                and room.room_id not in pending_ids
                and not room_auto_cleaned_on(
                    self.ledgers.get(room.room_id, RoomLedger()),
                    auto_clean_day,
                )
            )
            pending_ids.update(room.room_id for room in rooms)
            for room_id in self.session.retry_room_ids:
                room = self.room_by_id.get(room_id)
                if (
                    room
                    and self._room_auto_clean_enabled(room)
                    and room_id not in pending_ids
                ):
                    rooms.append(room)
                    pending_ids.add(room_id)
        elif (
            blocker.vacuum_only_safe
            and clean_water_vacuum_only_reason(self._resource_state())
            and self.config.get(CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED)
        ):
            rooms.extend(
                room
                for room in ordered
                if room.mop_required
                and room.room_id not in self.session.completed_room_ids
                and room.room_id not in fallback_attempted
                and not room_auto_cleaned_on(
                    self.ledgers.get(room.room_id, RoomLedger()),
                    auto_clean_day,
                )
            )
        pending_ids = {room.room_id for room in rooms}
        for room_id in self.session.retry_room_ids:
            room = self.room_by_id.get(room_id)
            if (
                room
                and not room.mop_required
                and self._room_auto_clean_enabled(room)
                and room_id not in pending_ids
                and not room_auto_cleaned_on(
                    self.ledgers.get(room_id, RoomLedger()),
                    auto_clean_day,
                )
            ):
                rooms.append(room)
                pending_ids.add(room_id)
        return rooms

    def _refilled_normal_retry_room_ids(self) -> list[str]:
        """Return deferred mop rooms in their post-refill priority order."""
        session = self.session
        if not session:
            return []
        fallback_completed = set(session.fallback_completed_room_ids)
        return [
            room_id
            for room_id in sorted(
                (
                    room_id
                    for room_id in session.deferred_full_clean_room_ids
                    if room_id not in session.uncertain_room_ids
                ),
                key=lambda room_id: (
                    room_id in fallback_completed,
                    session.deferred_full_clean_room_ids.index(room_id),
                ),
            )
            if (
                (room := self.room_by_id.get(room_id))
                and room.mop_required
            )
        ]

    def _queue_refilled_normal_retries(self) -> None:
        """Queue every deferred mop obligation after clean water recovers."""
        session = self.session
        if not session:
            return
        for room_id in self._refilled_normal_retry_room_ids():
            if room_id in session.retried_room_ids:
                session.retried_room_ids.remove(room_id)
            session.queue_retry(room_id, priority=True)

    async def _async_prepare_degraded_vacuuming(self) -> bool:
        """Perform the one bounded dock-stop and vacuum-mode preparation."""
        session = self.session
        if not session:
            return False
        if session.degraded_preparation_completed:
            return True
        if not session.degraded_preparation_attempted:
            session.degraded_preparation_attempted = True
            await self._async_save_store()
        identifier = self.config.get(CONF_IDENTIFIER)
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        dock_stop_needed = bool(
            identifier
            and dock_status is not None
            and dock_status.lower() in _BUSY_DOCK_STATES
        )

        if dock_stop_needed and not session.degraded_dock_stop_acknowledged_at:
            if session.degraded_dock_stop_attempts >= 1:
                if (
                    dock_status is not None
                    and dock_status.lower() not in _BUSY_DOCK_STATES
                ):
                    session.degraded_dock_stop_acknowledged_at = utcnow_iso()
                    await self._async_save_store()
                else:
                    return False
            else:
                session.degraded_dock_stop_attempts = 1
                session.degraded_dock_stop_requested_at = utcnow_iso()
                await self._async_save_store()
                try:
                    async with asyncio.timeout(20):
                        await self.hass.services.async_call(
                            DOMAIN,
                            SERVICE_DOCK_ACTION,
                            {
                                CONF_IDENTIFIER: identifier,
                                "capability": "clean",
                                "action": "stop",
                            },
                            blocking=True,
                        )
                except Exception:  # noqa: BLE001
                    _LOGGER.warning(
                        "Could not stop the mop-dock clean cycle for %s",
                        self.name,
                        exc_info=True,
                    )
                    return False
                session.degraded_dock_stop_acknowledged_at = utcnow_iso()
                await self._async_save_store()

        mode_entity = self.config.get(CONF_MODE_ENTITY)
        desired_mode = self.config.get(CONF_MODE_VACUUM_OPTION)
        current_mode = normalize_state(self._state(mode_entity))
        mode_fingerprint = f"{mode_entity}:{current_mode}:{desired_mode}"
        if (
            session.degraded_mode_state_fingerprint
            and session.degraded_mode_state_fingerprint != mode_fingerprint
        ):
            session.degraded_mode_attempts = 0
            session.degraded_mode_next_retry_at = None
            session.degraded_mode_last_error = None
        session.degraded_mode_state_fingerprint = mode_fingerprint
        retry_at = parse_datetime(session.degraded_mode_next_retry_at)
        if retry_at is not None and dt_util.utcnow() < retry_at:
            return False
        mode_needs_command = False
        if not mode_entity:
            session.degraded_mode_acknowledged_at = (
                session.degraded_mode_acknowledged_at or utcnow_iso()
            )
            session.degraded_mode_next_retry_at = None
            session.degraded_mode_last_error = None
        elif (
            mode_entity
            and current_mode == desired_mode
        ):
            session.degraded_mode_acknowledged_at = (
                session.degraded_mode_acknowledged_at or utcnow_iso()
            )
            session.degraded_mode_next_retry_at = None
            session.degraded_mode_last_error = None
        else:
            session.degraded_mode_attempts += 1
            mode_needs_command = True
            await self._async_save_store()
        if mode_needs_command:
            try:
                async with asyncio.timeout(15):
                    await self._async_apply_mode(vacuum_only=True)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning(
                    "Could not select vacuum mode for degraded session %s",
                    self.name,
                    exc_info=True,
                )
                base_seconds = max(
                    1,
                    int(
                        self.config.get(
                            CONF_BLOCKED_SESSION_TIMEOUT,
                            DEFAULT_BLOCKED_SESSION_TIMEOUT,
                        )
                    ),
                )
                cap_seconds = max(
                    base_seconds,
                    int(
                        self.config.get(
                            CONF_NATIVE_RESUME_TIMEOUT,
                            DEFAULT_NATIVE_RESUME_TIMEOUT,
                        )
                    ),
                )
                delay_seconds = min(
                    cap_seconds,
                    base_seconds
                    * (
                        2
                        ** min(
                            12,
                            max(0, session.degraded_mode_attempts - 1),
                        )
                    ),
                )
                session.degraded_mode_last_error = (
                    f"Could not select vacuum mode: {err}"
                )
                session.degraded_mode_next_retry_at = (
                    dt_util.utcnow() + timedelta(seconds=delay_seconds)
                ).isoformat()
                await self._async_save_store()
                return False
            session.degraded_mode_acknowledged_at = utcnow_iso()
            session.degraded_mode_next_retry_at = None
            session.degraded_mode_last_error = None
        session.degraded_preparation_completed = True
        await self._async_save_store()
        return True

    async def _async_finish_session_no_selection(self) -> None:
        """Finish an exhausted ordinary session."""
        if not self.session:
            return
        actionable_deferred = [
            room_id
            for room_id in self.session.deferred_full_clean_room_ids
            if room_id not in self.session.uncertain_room_ids
        ]
        if actionable_deferred:
            await self._async_arm_blocked_session_watchdog(
                self.session.degraded_reason
                or "Deferred full-clean rooms are waiting for resources"
            )
            return
        self._cancel_away_timer()
        self._clear_blocked_session_watchdog()
        self.session.active = False
        self.session.active_room_id = None
        self.session.pending_recovery_room_id = None
        self.session.pending_recovery_reason = None
        self.session.pending_recovery_priority = False
        self.session.pending_recovery_policy = None
        self.session.retry_room_ids = []
        self.session.priority_retry_room_ids = []
        self.session.terminal_reason = no_selection_terminal_reason(
            completed_room_ids=self.session.completed_room_ids,
            skipped_room_ids=self.session.skipped_room_ids,
            failed_room_ids=self.session.failed_room_ids,
            current_skipped_count=0,
            uncertain_room_ids=self.session.uncertain_room_ids,
        )
        self.session.terminal_message = self._first_session_block_reason()
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    def _current_blocked_reason(
        self,
        resources: ResourceState | None = None,
    ) -> str | None:
        """Return a concise reason the next dispatch is currently refused."""
        resources = resources or self._resource_state()
        blocker = self._current_blocker(resources)
        if blocker:
            return blocker.reason
        general_reason = cleaning_block_reason(resources)
        if general_reason:
            return general_reason
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        dock_state = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        recovered_error_ready = bool(
            vacuum_state == "error"
            and self._configured_error_is_clear()
            and (dock_state or "").lower() in _READY_DOCK_STATES
        )
        if vacuum_state not in _READY_VACUUM_STATES and not recovered_error_ready:
            if not is_error_clear(resources.error):
                return normalize_state(resources.error)
            return f"vacuum state is {vacuum_state or 'unknown'}"
        status_entity = self.config.get(CONF_STATUS_FLAG_ENTITY)
        if status_entity:
            status_flag = self._status_flag()
            if status_flag in {None, "unknown", "unavailable", "resumable"}:
                return f"status flag is {status_flag or 'unknown'}"
        dock_entity = self.config.get(CONF_DOCK_STATUS_ENTITY)
        if dock_entity:
            dock_status = normalize_state(self._state(dock_entity))
            if not dock_status or dock_status.lower() in {
                "unknown",
                "unavailable",
            }:
                return f"dock status is {dock_status or 'unknown'}"
            if dock_status.lower() in _BUSY_DOCK_STATES:
                return f"dock status is {dock_status}"
        battery_entity = self.config.get(CONF_BATTERY_ENTITY)
        if battery_entity:
            battery = parse_float(self._state(battery_entity))
            if battery is None:
                return "battery state is unavailable"
            minimum = float(self.config.get(CONF_MIN_BATTERY))
            if battery < minimum:
                return f"battery is {battery:.0f}%, below {minimum:.0f}%"
        return None

    async def _async_start_room(
        self,
        room: RoomConfig,
        *,
        vacuum_only: bool,
        fallback_vacuum: bool = False,
    ) -> None:
        """Command Valetudo to clean one segment."""
        if not self.session:
            return

        session = self.session
        was_priority_retry = room.room_id in session.priority_retry_room_ids
        was_retry = room.room_id in session.retry_room_ids
        requested_iterations = int(
            self.config.get(CONF_AUTO_CLEAN_ITERATIONS, 2)
        )
        session.active_room_id = room.room_id
        run = ActiveRun(
            room_id=room.room_id,
            segment_id=room.segment_id,
            session_id=session.session_id,
            started_at=utcnow_iso(),
            start_area=parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
            start_time=parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
            vacuum_only=vacuum_only,
            fallback_vacuum=fallback_vacuum,
            allowed_error_fingerprint=(
                allowed_error_fingerprint(self._resource_state())
                if vacuum_only and session.degraded_reason
                else None
            ),
            phase=RUN_PHASE_DISPATCHING,
            requested_iterations=requested_iterations,
        )
        self.active_run = run
        self._set_retained_task_guard_for_run(run)
        self._active_run_restored = False
        self._restored_dispatch_intent_deadline = None
        if self._status_flag() == "segment":
            run.observed_segment_cleaning = True

        await self._async_save_store()

        try:
            await self._async_apply_mode(vacuum_only=vacuum_only)
            if not self._dispatch_still_safe(session, run, room):
                await self._async_abort_tentative_dispatch(
                    session=session,
                    run=run,
                )
                return
            run.command_publish_requested_at = utcnow_iso()
            await self._async_save_store()
            await self.hass.services.async_call(
                "mqtt",
                "publish",
                {
                    "topic": self.segment_command_topic,
                    "payload": json.dumps(
                        {
                            "segment_ids": [room.segment_id],
                            "iterations": requested_iterations,
                            "customOrder": True,
                        }
                    ),
                },
                blocking=True,
            )
            run.command_published = True
            run.command_publish_acknowledged_at = utcnow_iso()
            run.dispatch_deadline = (
                dt_util.utcnow()
                + timedelta(
                    seconds=int(
                        self.config.get(
                            CONF_DISPATCH_START_TIMEOUT,
                            DEFAULT_DISPATCH_START_TIMEOUT,
                        )
                    )
                )
            ).isoformat()
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("Failed to start Valetudo segment clean for %s", room.name)
            failure_reason = f"Could not dispatch {room.name}: {err}"
            if run.command_publish_requested_at:
                run.cancelled = True
                run.phase = RUN_PHASE_CANCEL_PENDING
                run.cancel_requested_at = utcnow_iso()
                run.cancel_reason = failure_reason
                run.cancel_continue_session = True
                run.cancel_requeue_room = True
                run.cancel_outcome_result = "interrupted"
                run.dispatch_failure_code = "dispatch.publish_ack_pending"
                await self._async_enter_recoverable_wait(
                    BlockerClassification(
                        "dispatch.publish_ack_pending",
                        BLOCKER_RECOVERABLE,
                        "dispatch",
                        failure_reason,
                        (
                            "The coordinator will issue one stop and wait for "
                            "acknowledgement before retrying."
                        ),
                    ),
                    phase="cancelling_pending_command",
                    next_retry_at=None,
                )
                if await self._async_execute_cancel_pending():
                    await self._async_maybe_start_next_room()
                return

            self._clear_active_run()
            await self._async_save_store()
            self._notify_listeners()
            if session.active:
                await self._async_arm_blocked_session_watchdog(
                    failure_reason
                )
            return

        self._record_published_run(
            session,
            run,
            room,
            was_retry=was_retry,
            was_priority_retry=was_priority_retry,
        )

        await self._async_save_store()
        self._notify_listeners()
        self._schedule_dispatch_start_timeout()

    def _record_published_run(
        self,
        session: SessionState,
        run: ActiveRun,
        room: RoomConfig,
        *,
        was_retry: bool,
        was_priority_retry: bool,
    ) -> None:
        """Record a segment command only after MQTT accepts the publish."""
        if run.fallback_vacuum:
            if session.pending_recovery_room_id == room.room_id:
                session.resolve_recoverable_failure(
                    room.room_id,
                    queue_retry=session.can_retry_room(room.room_id),
                )
            session.mark_fallback_attempted(room.room_id)
            self._clear_blocked_session_watchdog()
            return
        if was_retry or was_priority_retry:
            previous_reason = session.failed_room_reasons.get(room.room_id)
            session.mark_retry_started(room.room_id)
            session.clear_room_issue(room.room_id)
            recovery = session.last_command_recovery
            if recovery.get("room_id") == room.room_id:
                recovery["retry_started_at"] = utcnow_iso()
                recovery["retry_eligible"] = False
                recovery["retry_queued"] = False
            ledger = self.ledgers.setdefault(room.room_id, RoomLedger())
            if previous_reason and ledger.last_failed_reason == previous_reason:
                ledger.last_failed_reason = None
            self._remove_while_away_failure(room.room_id, previous_reason)
        session.mark_attempted(room.room_id)
        self._clear_blocked_session_watchdog()

    def _dispatch_still_safe(
        self,
        session: SessionState,
        run: ActiveRun,
        room: RoomConfig,
    ) -> bool:
        """Return whether a tentatively claimed room may still be published."""
        base_safe = bool(
            self.session is session
            and session.active
            and self.active_run is run
            and not self.paused
            and self._all_people_away()
            and self._room_auto_clean_enabled(room)
            and self._vacuum_ready_for_selection(vacuum_only=run.vacuum_only)
        )
        if not base_safe:
            return False

        resources = self._resource_state()
        blocker = self._current_blocker(resources)
        if blocker and not (
            run.vacuum_only
            and self.session
            and self.session.degraded_reason
            and blocker.vacuum_only_safe
        ):
            return False
        if cleaning_block_reason(resources) and not (
            run.vacuum_only
            and blocker
            and blocker.vacuum_only_safe
        ):
            return False
        return not mop_block_reason(room, resources) or run.vacuum_only

    async def _async_abort_tentative_dispatch(
        self,
        *,
        session: SessionState,
        run: ActiveRun,
    ) -> None:
        """Undo a claimed room when a safety gate changes before publish."""
        if self.session is not session or self.active_run is not run:
            return

        self._clear_active_run()

        if not self._all_people_away():
            if self.config.get(CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL):
                await self._async_cancel_session("Tracked person arrived home")
            else:
                await self._async_save_store()
                self._notify_listeners()
            return

        await self._async_save_store()
        self._notify_listeners()

    async def _async_apply_mode(self, *, vacuum_only: bool) -> None:
        """Apply optional Valetudo cleaning mode selects."""
        mode_entity = self.config.get(CONF_MODE_ENTITY)
        if not mode_entity:
            return
        await self.hass.services.async_call(
            "select",
            "select_option",
            {
                ATTR_ENTITY_ID: mode_entity,
                "option": self.config.get(
                    CONF_MODE_VACUUM_OPTION if vacuum_only else CONF_MODE_MOP_OPTION
                ),
            },
            blocking=True,
        )

        water_entity = self.config.get(CONF_WATER_ENTITY)
        if water_entity and not vacuum_only:
            await self.hass.services.async_call(
                "select",
                "select_option",
                {ATTR_ENTITY_ID: water_entity, "option": self.config.get(CONF_WATER_MOP_OPTION)},
                blocking=False,
            )

    async def _async_finish_active_run(
        self,
        *,
        success_override: bool | None = None,
        failure_reason: str | None = None,
        continue_session: bool = True,
        send_summary: bool = True,
        retain_task_guard: bool = False,
    ) -> None:
        """Finalize the active commanded run."""
        run = self.active_run
        if not run or not run.room_id:
            return
        if success_override is None and run.resume_required:
            return

        room = self.room_by_id[run.room_id]
        run.finalize_estimated_room(dt_util.utcnow())
        ledger = self.ledgers.setdefault(room.room_id, RoomLedger())
        if self.session and run.observed_cleaning:
            self.session.clear_dispatch_failure(room.room_id)

        if success_override is None:
            if not self.config.get(CONF_STATUS_FLAG_ENTITY):
                run.observed_segment_cleaning = True
            success, reason = evaluate_run_success(
                room,
                run,
                parse_float(
                    self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))
                ),
                parse_float(
                    self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))
                ),
                self.error_state,
            )
        else:
            success = success_override
            reason = failure_reason

        wrong_room_failure = is_wrong_room_failure(reason)
        if success:
            when = utcnow_iso()
            if run.fallback_vacuum:
                mark_fallback_vacuum_success(ledger, when)
                if self.session:
                    self.session.mark_fallback_completed(room.room_id)
                    deferred_reason = (
                        self.session.deferred_full_clean_reasons.get(
                            room.room_id,
                            self.session.degraded_reason,
                        )
                    )
                    self._record_attempt_outcome(
                        kind="fallback",
                        run=run,
                        result="completed",
                        legacy_reason=deferred_reason,
                        occurred_at=when,
                    )
            else:
                mark_success(
                    ledger,
                    when,
                    mop=room.mop_required and not run.vacuum_only,
                    auto_clean=self.session is not None,
                    auto_clean_day=self._current_auto_clean_day(),
                )
                if self.session:
                    self.session.mark_completed(room.room_id)
                    self._record_attempt_outcome(
                        kind="cleaned",
                        run=run,
                        result="completed",
                        occurred_at=when,
                    )
        else:
            when = utcnow_iso()
            mark_failure(ledger, when, reason)
            if self.session:
                if run.fallback_vacuum:
                    self.session.mark_fallback_failed(room.room_id, reason)
                else:
                    self.session.mark_failed(room.room_id, reason)
                self._record_attempt_outcome(
                    kind="failed",
                    run=run,
                    result="failed",
                    reason=reason,
                    occurred_at=when,
                )
                if (
                    wrong_room_failure
                    and self.session.can_retry_room(room.room_id)
                ):
                    self.session.queue_retry(room.room_id, priority=True)

        self._clear_active_run(retain_task_guard=retain_task_guard)

        await self._async_save_store()
        self._notify_listeners()
        if continue_session:
            await self._async_maybe_start_next_room()
        elif send_summary:
            await self._async_maybe_send_auto_clean_summary()

    async def _async_handle_active_run_error(self, error: str) -> None:
        """Protect floor credit, cancel pending work, and preserve recovery."""
        run = self.active_run
        if not run:
            return
        resources = self._resource_state()
        blocker = self._current_blocker(resources)
        if blocker is None:
            blocker = classify_blocker(ResourceState(error=error))
        if blocker is None:
            blocker = BlockerClassification(
                "error.recoverable_unknown",
                BLOCKER_RECOVERABLE,
                "error",
                error,
                "Clear the vacuum error; the preserved session will retry.",
                waiting_for_physical_fix=True,
            )

        if blocker.vacuum_only_safe and run.vacuum_only:
            run.allowed_error_fingerprint = blocker.code
            self._activate_vacuum_only_degraded(blocker.reason)
            await self._async_save_store()
            await self._async_reconcile_active_run(dt_util.utcnow())
            return

        if (
            blocker.code == "power.low_battery"
            and not self._run_waiting_for_start_ack(run)
        ):
            await self._async_handle_low_battery_active_run_error(error)
            return

        degraded_reason = (
            clean_water_vacuum_only_reason(resources)
            or (blocker.reason if blocker.vacuum_only_safe else None)
        )
        if degraded_reason:
            self._activate_vacuum_only_degraded(degraded_reason)

        room = self.active_room
        evidence = (
            evaluate_floor_completion_evidence(
                room,
                run,
                end_area=parse_float(
                    self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))
                ),
                end_time=parse_float(
                    self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))
                ),
                vacuum_state=self._state(self.vacuum_entity),
                status_flag=(
                    self._status_flag()
                    if self.config.get(CONF_STATUS_FLAG_ENTITY)
                    else None
                ),
                dock_status=self._state(
                    self.config.get(CONF_DOCK_STATUS_ENTITY)
                ),
            )
            if room
            else None
        )
        if evidence:
            run.floor_completion_status = evidence.status
            run.floor_completion_reason = evidence.reason
            run.floor_completion_recorded_at = utcnow_iso()

        if evidence and evidence.status == "completed" and blocker.recoverable:
            await self._async_finish_active_run(
                success_override=True,
                continue_session=False,
                send_summary=False,
            )
            if degraded_reason:
                await self._async_maybe_start_next_room()
            else:
                await self._async_arm_blocked_session_watchdog(blocker.reason)
            return

        if not blocker.recoverable and self.session:
            self._clear_blocked_session_watchdog()
            self.session.clear_recovery()
            self.session.active = False
            self.session.terminal_reason = "unrecoverable_error"
            self.session.terminal_message = blocker.reason
            self.session.terminal_cause = blocker.code
            self.session.needs_help = True
            self.session.blocker_code = blocker.code
            self.session.blocker_disposition = blocker.disposition
            self.session.blocker_operator_action = blocker.operator_action

        run.cancelled = True
        run.phase = RUN_PHASE_CANCEL_PENDING
        run.cancel_requested_at = run.cancel_requested_at or utcnow_iso()
        run.cancel_reason = (
            f"{evidence.reason} after {blocker.reason}"
            if evidence
            and evidence.status == "uncertain"
            and evidence.reason
            else blocker.reason
        )
        run.cancel_continue_session = blocker.recoverable
        navigation_room_failure = bool(
            blocker.code == "navigation.recoverable"
            and run.room_id
            and run.room_id in self.room_by_id
            and not run.manual
            and not run.fallback_vacuum
            and not run.dispatch_failure_code
            and run.confirmed_room_start
            and (evidence is None or evidence.status == "incomplete")
        )
        if (
            blocker.code == "navigation.recoverable"
            and not run.confirmed_room_start
            and not run.dispatch_failure_code
            and (evidence is None or evidence.status == "incomplete")
        ):
            run.dispatch_failure_code = "dispatch.navigation_before_start"
        run.cancel_recover_room = bool(
            navigation_room_failure
        )
        run.cancel_requeue_room = bool(
            blocker.recoverable
            and not run.cancel_recover_room
            and (evidence is None or evidence.status == "incomplete")
        )
        run.cancel_outcome_result = (
            "uncertain"
            if evidence and evidence.status == "uncertain"
            else "interrupted"
        )
        self._cancel_active_run_timers()
        run.checkpoint_statistics(
            parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
            parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
        )
        if blocker.recoverable and self.session:
            if run.cancel_recover_room and run.room_id:
                self.session.begin_recovering(
                    run.room_id,
                    run.cancel_reason,
                    policy=_NAVIGATION_RETRY_LATER_POLICY,
                )
            await self._async_enter_recoverable_wait(
                blocker,
                phase="cancelling_pending_command",
                next_retry_at=parse_datetime(run.cancel_ack_deadline),
            )
        else:
            await self._async_save_store()
            self._notify_listeners()

        if not await self._async_execute_cancel_pending():
            return
        await self._async_save_store()
        self._notify_listeners()
        if blocker.recoverable and self.session and self.session.active:
            await self._async_maybe_start_next_room()
        else:
            await self._async_maybe_send_auto_clean_summary()

    async def _async_handle_low_battery_active_run_error(self, error: str) -> None:
        """Passively retain a low-battery native task until firmware resumes it."""
        run = self.active_run
        if not run:
            return
        if not bool(
            self.config.get(
                CONF_NATIVE_RESUME_ENABLED,
                DEFAULT_NATIVE_RESUME_ENABLED,
            )
        ):
            _LOGGER.warning(
                "%s has native_resume_enabled=false; v0.3.0 still preserves "
                "recoverable low-battery work to honor automatic recovery",
                self.name,
            )

        changed = self._mark_active_run_interrupted(
            run,
            dt_util.utcnow(),
            phase=RUN_PHASE_SUSPENDED,
            reason=error,
            require_resume=True,
        )
        if self._status_flag() == "resumable" and not run.resumable_latched:
            run.resumable_latched = True
            changed = True
        if changed:
            await self._async_save_store()
            self._notify_listeners()
        self._schedule_native_resume_timeout()

    async def _async_recover_pending_room_failure_if_ready(self) -> bool:
        """Queue or finish a recovery once the robot is safely docked and ready."""
        if not self.session or not self.session.pending_recovery_room_id:
            return False
        if self.active_run or not self.session.active:
            return False
        room_id = self.session.pending_recovery_room_id
        reason = self.session.pending_recovery_reason
        if (
            not self._vacuum_successfully_docked()
            or not self._configured_error_is_clear()
        ):
            return False

        if room_id not in self.room_by_id:
            self.session.pending_recovery_room_id = None
            self.session.pending_recovery_reason = None
            self.session.pending_recovery_priority = False
            self.session.pending_recovery_policy = None
            await self._async_save_store()
            self._notify_listeners()
            return True

        should_retry = self.session.can_retry_room(room_id)
        self.session.resolve_recoverable_failure(room_id, queue_retry=should_retry)
        recovery = self.session.last_command_recovery
        if recovery.get("room_id") == room_id:
            recovered_at = utcnow_iso()
            recovery["recovered_at"] = recovered_at
            recovery["retry_queued"] = should_retry
            if (
                recovery.get("return_attempts", 0)
                and not recovery.get("return_recovery_confirmed_at")
            ):
                recovery["return_recovery_confirmed_at"] = recovered_at
            recovery["next_candidate_room"] = self.next_candidate_room_id
        if should_retry:
            _LOGGER.info(
                "Recovered %s room %s after %s; queued it for one%s retry",
                self.name,
                room_id,
                reason or "recoverable failure",
                " priority" if is_low_battery_error(reason) else "",
            )
        else:
            _LOGGER.info(
                "Recovered %s after room %s exhausted its retry; continuing "
                "the queue",
                self.name,
                room_id,
            )
        await self._async_save_store()
        self._notify_listeners()
        return True

    def _recovering_room_for_error(self) -> tuple[str | None, str | None]:
        """Return a room whose low-battery recovery can be superseded by an error."""
        if not self.session or not self.session.active:
            return None, None
        if self.session.pending_recovery_room_id:
            return (
                self.session.pending_recovery_room_id,
                self.session.pending_recovery_reason,
            )
        for room_id in self.session.priority_retry_room_ids:
            reason = self.session.failed_room_reasons.get(room_id)
            if is_low_battery_error(reason):
                return room_id, reason
        return None, None

    async def _async_handle_pending_recovery_error(self, error: str) -> None:
        """Replace a pending or queued recovery with the latest actual error."""
        if not self.session:
            return
        room_id, previous_reason = self._recovering_room_for_error()
        if not room_id:
            return

        replaced_outcome = self._replace_while_away_failure(
            room_id,
            previous_reason,
            error,
        )
        mark_failure(
            self.ledgers.setdefault(room_id, RoomLedger()),
            utcnow_iso(),
            error,
        )
        self.session.mark_failed(room_id, error)
        if not replaced_outcome:
            self._record_legacy_while_away_outcome("failed", room_id, error)

        blocker = self._current_blocker() or classify_blocker(
            ResourceState(error=error)
        )
        if blocker and not blocker.recoverable:
            await self._async_terminalize_unrecoverable(blocker)
            return

        navigation_retry_later = (
            self.session.pending_recovery_policy
            == _NAVIGATION_RETRY_LATER_POLICY
        )
        self.session.discard_retry(room_id)
        self.session.begin_recovering(
            room_id,
            error,
            priority=(
                False
                if navigation_retry_later
                else is_low_battery_error(error)
            ),
            policy=(
                _NAVIGATION_RETRY_LATER_POLICY
                if navigation_retry_later
                else None
            ),
        )
        await self._async_save_store()
        self._notify_listeners()
        await self._async_arm_blocked_session_watchdog(error)

    async def _async_reconcile_restored_session(self) -> None:
        """Resume or finalize a persisted auto-clean session after Home Assistant restarts."""
        if not self.session:
            return
        if (
            not self.active_run
            and self._recovering_room_for_error()[0]
            and self._configured_error_is_available()
            and not is_error_clear(self.error_state)
        ):
            await self._async_handle_pending_recovery_error(
                self.error_state or "Unknown error"
            )
            return
        if self._restore_active_run_observations(dt_util.utcnow()):
            await self._async_save_store()
        if self.active_run and await self._async_reconcile_restored_active_run():
            return
        if self.session.active and not self.active_run:
            await self._async_maybe_start_next_room()
            return
        await self._async_maybe_send_auto_clean_summary()

    async def _async_reconcile_restored_active_run(self) -> bool:
        """Reconcile one persisted run after its entity states become available."""
        if not self.active_run or not self._active_run_restored:
            return False

        if self.active_run.phase == RUN_PHASE_CANCEL_PENDING:
            await self._async_reconcile_active_run(dt_util.utcnow())
            return True
        if self.active_run.native_resume_pending:
            await self._async_reconcile_active_run(dt_util.utcnow())
            if not self.active_run:
                return True
        if (
            self._run_waiting_for_start_ack(self.active_run)
            and (blocker := self._current_blocker()) is not None
        ):
            await self._async_handle_active_run_error(blocker.reason)
            return True

        if (
            self._configured_error_is_available()
            and not is_error_clear(self.error_state)
            and not self._active_run_allows_current_blocker()
        ):
            await self._async_handle_active_run_error(
                self.error_state or "Unknown error"
            )
            return True

        if not self.active_run.command_published:
            room = self.room_by_id.get(self.active_run.room_id or "")
            if self.active_run.observed_cleaning or self.active_run.observed_segment_cleaning:
                self.active_run.command_published = True
                self.active_run.command_publish_acknowledged_at = (
                    self.active_run.command_publish_acknowledged_at
                    or utcnow_iso()
                )
                if room and self.session:
                    self._record_published_run(
                        self.session,
                        self.active_run,
                        room,
                        was_retry=room.room_id in self.session.retry_room_ids,
                        was_priority_retry=(
                            room.room_id in self.session.priority_retry_room_ids
                        ),
                    )
                await self._async_save_store()
            elif self.active_run.command_publish_requested_at:
                self.active_run.cancelled = True
                self.active_run.phase = RUN_PHASE_CANCEL_PENDING
                self.active_run.cancel_requested_at = (
                    self.active_run.cancel_requested_at or utcnow_iso()
                )
                self.active_run.cancel_reason = (
                    "Restarted with an unacknowledged segment publish"
                )
                self.active_run.cancel_continue_session = True
                self.active_run.cancel_requeue_room = True
                self.active_run.cancel_outcome_result = "interrupted"
                await self._async_save_store()
                await self._async_execute_cancel_pending()
                return True
            elif self._restored_dispatch_intent_deadline is None:
                self._restored_dispatch_intent_deadline = (
                    dt_util.utcnow()
                    + timedelta(seconds=_RESTORE_RECONCILE_DELAY_SECONDS)
                )
                return True
            elif dt_util.utcnow() < self._restored_dispatch_intent_deadline:
                return True
            else:
                self._clear_active_run()
                await self._async_save_store()
                await self._async_maybe_start_next_room()
                return True

        if not self._configured_error_is_available():
            return True
        if (
            not is_error_clear(self.error_state)
            and not self._active_run_allows_current_blocker()
        ):
            await self._async_handle_active_run_error(
                self.error_state or "Unknown error"
            )
            return True

        if (
            self.active_run.phase == RUN_PHASE_DISPATCHING
            and self.active_run.command_published
            and self.active_run.dispatch_deadline is None
        ):
            self.active_run.dispatch_deadline = (
                dt_util.utcnow()
                + timedelta(
                    seconds=int(
                        self.config.get(
                            CONF_DISPATCH_START_TIMEOUT,
                            DEFAULT_DISPATCH_START_TIMEOUT,
                        )
                    )
                )
            ).isoformat()
            await self._async_save_store()

        await self._async_reconcile_active_run(dt_util.utcnow())
        if (
            self.active_run
            and normalize_state(self._state(self.vacuum_entity)) == "cleaning"
        ):
            self._active_run_restored = False
        self._schedule_active_run_timers()
        return self.active_run is not None

    def _restore_active_run_observations(self, now: datetime) -> bool:
        """Seed active-run observations from current HA state after a restart."""
        if not self.active_run:
            return False

        changed = False
        run = self.active_run
        if (
            self._active_run_restored
            and run.last_estimated_changed_at is not None
        ):
            run.last_estimated_room_id = None
            run.last_estimated_changed_at = None
            changed = True
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if vacuum_state in {"cleaning", "returning"} and not run.observed_cleaning:
            run.observed_cleaning = True
            changed = True
        if vacuum_state == "cleaning" and run.phase == RUN_PHASE_DISPATCHING:
            run.phase = RUN_PHASE_CLEANING
            run.start_confirmed_at = now.isoformat()
            run.dispatch_deadline = None
            self._cancel_dispatch_start_timeout()
            changed = True

        status_flag = self._status_flag()
        if status_flag == "segment":
            changed = run.observe_segment_iteration(
                source=(
                    "restored_segment_snapshot"
                    if self._active_run_restored
                    else "coherent_segment_transition"
                ),
                count_new_iteration=bool(
                    not self._active_run_restored
                    and vacuum_state == "cleaning"
                    and not run.native_resume_pending
                ),
            ) or changed
            if run.native_resume_pending:
                if (
                    not run.post_suspend_segment_observed
                    and self._entity_changed_after(
                        self.config.get(CONF_STATUS_FLAG_ENTITY),
                        run.suspended_at,
                    )
                ):
                    run.post_suspend_segment_observed = True
                    changed = True
                if (
                    vacuum_state == "cleaning"
                    and not run.post_suspend_cleaning_observed
                    and self._entity_changed_after(
                        self.vacuum_entity,
                        run.suspended_at,
                    )
                ):
                    run.post_suspend_cleaning_observed = True
                    changed = True
                changed = self._mark_active_run_resumed(run) or changed
        elif (
            status_flag == "resumable"
            and run.phase != RUN_PHASE_DISPATCHING
            and run.observed_segment_cleaning
        ):
            if not run.resumable_latched:
                run.resumable_latched = True
                changed = True
            changed = self._mark_active_run_interrupted(
                run,
                now,
                phase=RUN_PHASE_SUSPENDED,
                reason="Valetudo reported a resumable native task",
                require_resume=True,
            ) or changed

        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        dock_status = dock_status.lower() if dock_status else None
        if (
            dock_status in _BUSY_DOCK_STATES
            and vacuum_state != "cleaning"
            and not self._active_run_allows_current_blocker()
        ):
            changed = self._mark_active_run_interrupted(
                run,
                now,
                phase=RUN_PHASE_DOCK_INTERRUPT,
                reason=f"Dock interruption: {dock_status}",
                require_resume=False,
            ) or changed

        estimated_room_id = self._room_id_from_estimated(
            self._state(self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY))
        )
        if (
            vacuum_state == "cleaning"
            and estimated_room_id
            and run.last_estimated_room_id is None
        ):
            run.observe_estimated_room(estimated_room_id, now)
            changed = True

        return changed

    async def _async_maybe_send_auto_clean_summary(self) -> None:
        """Send the one final auto-clean notification when the session is terminal."""
        if not self.session or self.session.active or self.active_run:
            return
        if not self.session.terminal_reason:
            return
        if self.session.notification_sent and self.settings_snapshot is None:
            return
        guarded_cleanup = bool(
            self.session.native_resume_guard_latched
            or self.session.native_guard_cancel_pending
        )
        retry_budget_exhausted = (
            self._terminal_cleanup_retry_attempts
            >= _MAX_TERMINAL_CLEANUP_RETRIES
        )

        notification_failed = False
        if not self.session.notification_sent:
            if retry_budget_exhausted:
                notification_failed = True
            else:
                summary = build_auto_clean_summary(
                    vacuum_name=self.name.replace(" Coordinator", ""),
                    completed_room_names=[
                        self.room_by_id[room_id].name
                        for room_id in self.session.completed_room_ids
                    ],
                    skipped_room_reasons=self._named_reasons(
                        self.session.skipped_room_reasons
                    ),
                    failed_room_reasons=self._named_reasons(
                        self.session.failed_room_reasons
                    ),
                    terminal_reason=self.session.terminal_reason,
                    terminal_message=self.session.terminal_message,
                    needs_help=self.session.needs_help,
                    all_rooms_cleaned=self._all_enabled_rooms_completed(),
                    total_room_count=len(self._auto_clean_rooms()),
                    fallback_room_names=[
                        self.room_by_id[room_id].name
                        for room_id in self.session.fallback_completed_room_ids
                        if room_id in self.room_by_id
                    ],
                    deferred_room_names=[
                        self.room_by_id[room_id].name
                        for room_id in self.session.deferred_full_clean_room_ids
                        if room_id in self.room_by_id
                    ],
                    uncertain_room_names=[
                        self.room_by_id[room_id].name
                        for room_id in self.session.uncertain_room_ids
                        if room_id in self.room_by_id
                    ],
                )
                if summary:
                    try:
                        await self._async_send_notification(
                            summary.title,
                            summary.message,
                        )
                    except Exception:  # noqa: BLE001
                        _LOGGER.exception(
                            "Could not send auto-clean summary for %s",
                            self.name,
                        )
                        notification_failed = True
                if not notification_failed:
                    self.session.notification_sent = True

        if guarded_cleanup:
            if self.settings_snapshot is not None:
                self._terminal_settings_restore_deferred = True
            retry_needed = notification_failed and not retry_budget_exhausted
            if retry_needed:
                self._terminal_cleanup_retry_attempts += 1
            elif not notification_failed:
                self._terminal_cleanup_retry_attempts = 0
            await self._async_save_store()
            self._notify_listeners()
            if retry_needed:
                self._schedule_terminal_cleanup_retry()
            else:
                self._cancel_terminal_cleanup_retry()
            return

        restoration_deferred = bool(
            self.settings_snapshot is not None
            and not self._settings_restoration_safe()
        )
        if restoration_deferred:
            self._terminal_settings_restore_deferred = True
        restoration_failed = False
        if self.settings_snapshot is not None and not restoration_deferred:
            deferred_allowance = self._terminal_settings_restore_deferred
            if retry_budget_exhausted and not deferred_allowance:
                restoration_failed = True
            else:
                self._terminal_settings_restore_deferred = False
                try:
                    restoration_failed = (
                        not await self._async_restore_auto_clean_settings()
                    )
                except Exception:  # noqa: BLE001
                    _LOGGER.exception(
                        "Could not restore auto-clean settings for %s",
                        self.name,
                    )
                    restoration_failed = True
        elif self.settings_snapshot is None:
            self._terminal_settings_restore_deferred = False

        retry_needed = bool(notification_failed or restoration_failed)
        if retry_needed and not retry_budget_exhausted:
            self._terminal_cleanup_retry_attempts += 1
        elif not retry_needed:
            self._terminal_cleanup_retry_attempts = 0
        await self._async_save_store()
        self._notify_listeners()
        if retry_needed and not retry_budget_exhausted:
            self._schedule_terminal_cleanup_retry()
        else:
            self._cancel_terminal_cleanup_retry()

    def _schedule_terminal_cleanup_retry(self) -> None:
        """Schedule a bounded retry for notification or settings cleanup."""
        if (
            self._terminal_cleanup_retry_cancel is not None
            or self._terminal_cleanup_retry_attempts >= _MAX_TERMINAL_CLEANUP_RETRIES
        ):
            return
        def timer_finished(_now: datetime) -> None:
            self._terminal_cleanup_retry_cancel = None
            schedule_hass_task(
                self.hass,
                self._async_retry_terminal_cleanup_serialized(),
            )

        self._terminal_cleanup_retry_cancel = async_call_later(
            self.hass,
            _TERMINAL_CLEANUP_RETRY_SECONDS,
            timer_finished,
        )

    async def _async_retry_terminal_cleanup_serialized(self) -> None:
        """Retry terminal notification and settings cleanup safely."""
        async with self._event_lock:
            await self._async_maybe_send_auto_clean_summary()

    def _cancel_terminal_cleanup_retry(self) -> None:
        """Cancel a scheduled terminal cleanup retry."""
        if self._terminal_cleanup_retry_cancel is not None:
            self._terminal_cleanup_retry_cancel()
            self._terminal_cleanup_retry_cancel = None

    async def _async_prepare_auto_clean_settings(self) -> None:
        """Snapshot current user settings and apply auto-clean settings."""
        if self.settings_snapshot is None:
            self.settings_snapshot = AutoCleanSettingsSnapshot(
                mode=self._restorable_state(self.config.get(CONF_MODE_ENTITY)),
                fan=self._restorable_state(self.config.get(CONF_FAN_ENTITY)),
                water=self._restorable_state(self.config.get(CONF_WATER_ENTITY)),
                passes=self._restorable_state(self.config.get(CONF_PASSES_ENTITY)),
            )
        await self._async_select_option(
            self.config.get(CONF_PASSES_ENTITY),
            str(self.config.get(CONF_AUTO_CLEAN_ITERATIONS, 2)),
        )
        await self._async_select_option(
            self.config.get(CONF_FAN_ENTITY),
            self.config.get(CONF_FAN_AUTO_CLEAN_OPTION),
        )
        await self._async_select_option(
            self.config.get(CONF_WATER_ENTITY),
            self.config.get(CONF_WATER_MOP_OPTION),
        )

    async def _async_restore_auto_clean_settings(self) -> bool:
        """Restore user settings, retaining the snapshot if any call fails."""
        if self.settings_snapshot is None:
            return True
        snapshot = self.settings_snapshot
        restored = all(
            [
                await self._async_select_option(
                    self.config.get(CONF_MODE_ENTITY), snapshot.mode
                ),
                await self._async_select_option(
                    self.config.get(CONF_FAN_ENTITY), snapshot.fan
                ),
                await self._async_select_option(
                    self.config.get(CONF_WATER_ENTITY), snapshot.water
                ),
                await self._async_select_option(
                    self.config.get(CONF_PASSES_ENTITY), snapshot.passes
                ),
            ]
        )
        if restored:
            self.settings_snapshot = None
        return restored

    async def _async_select_option(
        self,
        entity_id: str | None,
        option: str | None,
    ) -> bool:
        """Select an option on a select or input_select entity when available."""
        if not entity_id or option in _UNKNOWN_OR_CLEAR_STATES:
            return True
        domain = entity_id.split(".", 1)[0]
        if domain not in {"select", "input_select"}:
            return True
        try:
            await self.hass.services.async_call(
                domain,
                "select_option",
                {ATTR_ENTITY_ID: entity_id, "option": option},
                blocking=True,
            )
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Could not select %s on %s", option, entity_id, exc_info=True)
            return False
        return True

    def _restorable_state(self, entity_id: str | None) -> str | None:
        """Return a setting value safe to restore later."""
        state = normalize_state(self._state(entity_id))
        return None if state in _UNKNOWN_OR_CLEAR_STATES else state

    async def _async_send_notification(self, title: str, message: str) -> None:
        """Send a notification through the configured Home Assistant notify service."""
        notify_service = self.config.get(CONF_NOTIFY_SERVICE)
        if not notify_service:
            _LOGGER.info("Auto-clean summary for %s: %s - %s", self.name, title, message)
            return
        domain, service = notify_service.split(".", 1) if "." in notify_service else ("notify", notify_service)
        data: dict[str, Any] = {"title": title, "message": message}
        notification_url = self.config.get(CONF_NOTIFICATION_URL)
        if notification_url:
            data["data"] = {
                "group": "vacuum",
                "url": notification_url,
                "clickAction": notification_url,
            }
        await self.hass.services.async_call(domain, service, data, blocking=True)

    def _named_reasons(self, room_reasons: dict[str, str]) -> dict[str, str]:
        """Return failure reasons keyed by friendly room name."""
        return {
            self.room_by_id[room_id].name: reason
            for room_id, reason in room_reasons.items()
            if room_id in self.room_by_id
        }

    def _all_enabled_rooms_completed(self) -> bool:
        """Return whether every enabled room completed successfully this session."""
        if not self.session:
            return False
        completed = set(self.session.completed_room_ids)
        return all(room.room_id in completed for room in self._auto_clean_rooms())

    def _auto_clean_rooms(self) -> list[RoomConfig]:
        """Return rooms currently eligible for automatic away cleaning."""
        return [room for room in self.rooms if self._room_auto_clean_enabled(room)]

    def _room_auto_clean_enabled(self, room: RoomConfig) -> bool:
        """Return whether a configured room is enabled for automatic away cleaning."""
        return room.enabled and room.room_id not in self.disabled_room_ids

    def _first_session_block_reason(self) -> str | None:
        """Return the first recorded room skip/failure reason for a terminal session."""
        if not self.session:
            return None
        for reasons in (
            self.session.skipped_room_reasons,
            self.session.failed_room_reasons,
            self.session.uncertain_room_reasons,
        ):
            for reason in reasons.values():
                return reason
        return None

    def _settings_restoration_safe(self) -> bool:
        """Return whether terminal cleanup may mutate cleaning settings."""
        if self.active_run or self.manual_run:
            return False
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        vacuum_state = vacuum_state.lower() if vacuum_state else None
        return vacuum_state not in _ACTIVE_RETAINED_VACUUM_STATES

    def _vacuum_successfully_docked(self) -> bool:
        """Return whether the robot reached the dock and is no longer resumable."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if vacuum_state not in _AT_DOCK_VACUUM_STATES:
            return False
        dock_entity = self.config.get(CONF_DOCK_STATUS_ENTITY)
        dock_status = normalize_state(self._state(dock_entity))
        if (
            dock_entity
            and (dock_status or "").lower() not in _READY_DOCK_STATES
        ):
            return False
        if not self.config.get(CONF_STATUS_FLAG_ENTITY):
            return True
        status_flag = self._status_flag()
        return (
            status_flag not in {None, "unknown", "unavailable", "resumable"}
        )

    def _error_is_mop_resource(self, error: str | None) -> bool:
        """Return whether an error is a recoverable mop resource issue."""
        return error_contains_any(error, RECOVERABLE_MOP_ERROR_KEYWORDS)

    def _start_manual_run(self, now: datetime) -> bool:
        """Begin observing a manual segment run."""
        if self.manual_run:
            return False
        self.manual_run = ActiveRun(
            room_id=None,
            segment_id=None,
            session_id=None,
            started_at=now.isoformat(),
            start_area=parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
            start_time=parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
            manual=True,
            manual_credit_room_ids=self._manual_credit_room_ids(),
        )
        if self._status_flag() == "segment":
            self.manual_run.observed_segment_cleaning = True
        self._ensure_retained_task_guard(
            now,
            owner=RETAINED_TASK_OWNER_MANUAL,
            phase=RETAINED_TASK_PHASE_OBSERVED_ACTIVE,
            reason="Observed manual cleaning",
            run=self.manual_run,
        )
        self._notify_listeners()
        return True

    async def _async_finish_manual_run(self, now: datetime) -> None:
        """Credit rooms observed during a manual run."""
        run = self.manual_run
        if not run:
            return

        run.finalize_estimated_room(now)
        if is_error_clear(self.error_state):
            for room in manual_rooms_to_credit(self.rooms, run):
                mark_success(self.ledgers.setdefault(room.room_id, RoomLedger()), utcnow_iso(), mop=room.mop_required)

        self.manual_run = None
        guard = self.retained_task_guard
        if guard and guard.owner == RETAINED_TASK_OWNER_MANUAL:
            guard.phase = RETAINED_TASK_PHASE_CLEARED
            guard.reason = "Manual task reached a clear docked state"
            guard.clear_acknowledged_at = (
                guard.clear_acknowledged_at or now.isoformat()
            )
            guard.operator_required_reason = None
            guard.stale_deadline = None
            guard.observation_deadline = None
            guard.clear_deadline = None
            self._cancel_retained_task_timer()
        self._clear_terminal_session_after_manual_run()
        await self._async_save_store()
        self._notify_listeners()

    def _clear_terminal_session_after_manual_run(self) -> None:
        """Clear stale away-session outcome details after a manual run completes."""
        if (
            self.session
            and not self.session.active
            and not self.session.native_resume_guard_latched
            and not self.active_run
        ):
            self.session = None

    def _manual_credit_room_ids(self) -> list[str] | None:
        """Return selected manual-credit room IDs, or None when unrestricted."""
        selected_room_ids = [
            room.room_id
            for room in self.rooms
            if room.manual_credit_entity
            and normalize_state(self._state(room.manual_credit_entity)) == "on"
        ]
        return selected_room_ids or None

    def _clear_while_away_after_manual_clean_started(self) -> bool:
        """Clear retained away outcome details when a manual clean starts."""
        changed = False
        if self.while_away_outcomes:
            self.while_away_outcomes = []
            self._cancel_next_day_timer()
            changed = True
        if (
            self.session
            and not self.session.active
            and not self.session.native_resume_guard_latched
            and not self.active_run
        ):
            self.session = None
            changed = True
        return changed

    def _next_while_away_outcome_sequence(self) -> int:
        """Return the next persisted monotonic typed-outcome sequence."""
        self._while_away_outcome_sequence = max(
            int(getattr(self, "_while_away_outcome_sequence", 0)),
            max(
                (
                    outcome.sequence or 0
                    for outcome in self.while_away_outcomes
                ),
                default=0,
            ),
        )
        self._while_away_outcome_sequence += 1
        return self._while_away_outcome_sequence

    def _append_while_away_outcome(
        self,
        outcome: WhileAwayOutcome,
    ) -> WhileAwayOutcome:
        """Append one outcome unless its stable event ID already exists."""
        day = self._current_auto_clean_day()
        self._prune_while_away_outcomes_for_day(day)
        if outcome.outcome_id:
            existing = next(
                (
                    item
                    for item in self.while_away_outcomes
                    if item.outcome_id == outcome.outcome_id
                ),
                None,
            )
            if existing is not None:
                return existing
        if outcome.event_type and outcome.sequence is None:
            outcome.sequence = self._next_while_away_outcome_sequence()
        self.while_away_outcomes.append(outcome)
        self._schedule_next_day_timer_if_needed()
        return outcome

    def _record_attempt_outcome(
        self,
        *,
        kind: str,
        run: ActiveRun,
        result: str,
        reason: str | None = None,
        legacy_reason: str | None = None,
        occurred_at: str | None = None,
    ) -> WhileAwayOutcome | None:
        """Record one immutable terminal result for a room command attempt."""
        if not run.room_id or not run.session_id:
            return None
        room = self.room_by_id.get(run.room_id)
        if room is None:
            return None
        when = occurred_at or utcnow_iso()
        outcome_id = (
            f"{run.session_id}:{room.room_id}:attempt:{run.started_at}"
        )
        reason_descriptor = (
            None
            if result == "completed"
            else classify_outcome_reason(reason)
        )
        existing = next(
            (
                outcome
                for outcome in self.while_away_outcomes
                if outcome.outcome_id == outcome_id
            ),
            None,
        )
        if existing is not None:
            existing_reason = (
                existing.reason_descriptor.to_dict()
                if existing.reason_descriptor
                else None
            )
            incoming_reason = (
                reason_descriptor.to_dict()
                if reason_descriptor
                else None
            )
            if (
                existing.event_type != "attempt"
                or existing.attempt_mode != attempt_mode_for_run(run)
                or existing.attempt_result != result
                or existing_reason != incoming_reason
            ):
                _LOGGER.warning(
                    "Ignoring conflicting terminal outcome for run %s; "
                    "retaining immutable %s result instead of %s",
                    outcome_id,
                    existing.attempt_result,
                    result,
                )
            return existing
        return self._append_while_away_outcome(
            WhileAwayOutcome(
                day=self._current_auto_clean_day(),
                room_id=room.room_id,
                kind=kind,
                reason=legacy_reason if legacy_reason is not None else reason,
                outcome_id=outcome_id,
                session_id=run.session_id,
                occurred_at=when,
                room_name=room.name,
                event_type="attempt",
                attempt_mode=attempt_mode_for_run(run),
                attempt_result=result,
                reason_descriptor=reason_descriptor,
            )
        )

    def _record_deferral_outcome(
        self,
        room: RoomConfig,
        reason: str,
        *,
        event_key: str = "full_clean",
        kind: str = "skipped",
        legacy_visible: bool = False,
        occurred_at: str | None = None,
    ) -> WhileAwayOutcome | None:
        """Record one idempotent typed no-attempt outstanding obligation."""
        if not self.session:
            return None
        when = occurred_at or utcnow_iso()
        return self._append_while_away_outcome(
            WhileAwayOutcome(
                day=self._current_auto_clean_day(),
                room_id=room.room_id,
                kind=kind,
                reason=reason,
                outcome_id=(
                    f"{self.session.session_id}:{room.room_id}:deferral:{event_key}"
                ),
                session_id=self.session.session_id,
                occurred_at=when,
                room_name=room.name,
                event_type="deferral",
                outstanding_operation=required_operation_for_room(room),
                reason_descriptor=classify_outcome_reason(reason),
                legacy_visible=legacy_visible,
            )
        )

    def _record_legacy_while_away_outcome(
        self,
        kind: str,
        room_id: str,
        reason: str | None = None,
    ) -> None:
        """Record a compatibility outcome when precise run metadata is absent."""
        room = self.room_by_id.get(room_id)
        self._append_while_away_outcome(
            WhileAwayOutcome(
                day=self._current_auto_clean_day(),
                room_id=room_id,
                kind=kind,
                reason=reason,
                session_id=(
                    self.session.session_id
                    if self.session
                    else None
                ),
                occurred_at=utcnow_iso(),
                room_name=room.name if room else None,
            )
        )

    def _replace_while_away_failure(
        self,
        room_id: str,
        previous_reason: str | None,
        reason: str,
    ) -> bool:
        """Update only current-session compatibility copy for one failure."""
        if not self.session:
            return False
        current_session_id = self.session.session_id
        for outcome in reversed(self.while_away_outcomes):
            if (
                outcome.day == self._current_auto_clean_day()
                and outcome.session_id == current_session_id
                and outcome.room_id == room_id
                and outcome.kind == "failed"
                and outcome.legacy_visible
                and (
                    previous_reason is None
                    or outcome.reason == previous_reason
                )
            ):
                outcome.reason = reason
                return True
        return False

    def _remove_while_away_failure(self, room_id: str, reason: str | None) -> None:
        """Hide only the latest current-session failure from legacy copy."""
        if not self.session:
            return
        current_session_id = self.session.session_id
        for outcome in reversed(self.while_away_outcomes):
            if (
                outcome.kind == "failed"
                and outcome.session_id == current_session_id
                and outcome.room_id == room_id
                and outcome.legacy_visible
                and (reason is None or outcome.reason == reason)
            ):
                outcome.legacy_visible = False
                return

    def _prune_while_away_outcomes_for_day(self, day: str) -> bool:
        """Keep only retained outcomes for the requested local day."""
        retained = [outcome for outcome in self.while_away_outcomes if outcome.day == day]
        if len(retained) == len(self.while_away_outcomes):
            return False
        self.while_away_outcomes = retained
        if not retained:
            self._cancel_next_day_timer()
        return True

    def _schedule_next_day_timer_if_needed(self) -> None:
        """Schedule a rollover refresh for retained while-away messages."""
        if not self.while_away_outcomes or self._next_day_timer_cancel is not None:
            return

        def timer_finished(_now: datetime) -> None:
            self._next_day_timer_cancel = None
            schedule_hass_task(self.hass, self._async_handle_next_day_rollover_serialized())

        self._next_day_timer_cancel = async_call_later(
            self.hass, self._seconds_until_next_auto_clean_day(), timer_finished
        )

    async def _async_handle_next_day_rollover(self) -> None:
        """Drop stale retained outcomes when the local day changes."""
        changed = self._prune_while_away_outcomes_for_day(self._current_auto_clean_day())
        if changed:
            await self._async_save_store()
            self._notify_listeners()
        self._schedule_next_day_timer_if_needed()

    async def _async_handle_next_day_rollover_serialized(self) -> None:
        """Process the day rollover without racing active dispatch."""
        async with self._event_lock:
            await self._async_handle_next_day_rollover()

    def _seconds_until_next_auto_clean_day(self) -> int:
        """Return seconds until just after the next Home Assistant local midnight."""
        now = dt_util.now()
        next_day = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=1, microsecond=0
        )
        return max(1, int((next_day - now).total_seconds()))

    def _cancel_next_day_timer(self) -> None:
        """Cancel any pending retained-outcome day rollover."""
        if self._next_day_timer_cancel is not None:
            self._next_day_timer_cancel()
            self._next_day_timer_cancel = None

    async def _async_return_to_dock_or_stop_resumable(
        self,
        reason: str,
    ) -> None:
        """Cancel a moving or resumable Valetudo task."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if vacuum_state == "error" and self._error_is_mop_resource(reason):
            _LOGGER.info("Stopping %s after dock resource error: %s", self.vacuum_entity, reason)
            await self.hass.services.async_call(
                "vacuum",
                "stop",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=True,
            )
            return

        if (
            vacuum_state in _AT_DOCK_VACUUM_STATES
            and self._status_flag() == "resumable"
        ):
            await self.hass.services.async_call(
                "vacuum",
                "stop",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=True,
            )
            return

        if vacuum_state in _MOVING_VACUUM_STATES:
            _LOGGER.info("Returning %s to dock because %s", self.vacuum_entity, reason)
            await self.hass.services.async_call(
                "vacuum",
                "return_to_base",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=True,
            )

    async def _async_clear_paused_between_rooms(self) -> None:
        """Clear Valetudo's paused state between room attempts."""
        if self._status_flag() == "resumable":
            return
        _LOGGER.info("Stopping %s to clear paused state before next room", self.vacuum_entity)
        await self.hass.services.async_call(
            "vacuum",
            "stop",
            {ATTR_ENTITY_ID: self.vacuum_entity},
            blocking=True,
        )

    def _vacuum_ready_for_next_room(self) -> bool:
        """Return whether dispatching another segment is safe."""
        return self._vacuum_ready_for_selection(vacuum_only=False)

    def _vacuum_ready_for_selection(self, *, vacuum_only: bool) -> bool:
        """Return whether the current snapshot safely permits this mode."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        resources = self._resource_state()
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        dock_status_lower = dock_status.lower() if dock_status else None
        resource_blocker = classify_blocker(resources)
        degraded_ready = bool(
            vacuum_only
            and self.session
            and self.session.active
            and self.session.degraded_reason
            and self.session.degraded_preparation_completed
            and resource_blocker
            and resource_blocker.recoverable
            and resource_blocker.vacuum_only_safe
            and (
                is_error_clear(resources.error)
                or resource_blocker.vacuum_only_safe
            )
            and vacuum_state in {"error", "docked", "idle", "charging"}
        )
        recovered_error_ready = bool(
            vacuum_state == "error"
            and self._configured_error_is_clear()
            and dock_status_lower in _READY_DOCK_STATES
        )
        if (
            vacuum_state not in _READY_VACUUM_STATES
            and not degraded_ready
            and not recovered_error_ready
        ):
            return False
        if self.config.get(CONF_STATUS_FLAG_ENTITY):
            status_flag = self._status_flag()
            if status_flag != "none":
                return False

        if not self._configured_error_is_available():
            return False
        if cleaning_block_reason(resources) and not degraded_ready:
            return False

        dock_status_entity = self.config.get(CONF_DOCK_STATUS_ENTITY)
        if dock_status_entity:
            if not dock_status or dock_status.lower() in {"unknown", "unavailable"}:
                return False
            if (
                dock_status.lower() in _BUSY_DOCK_STATES
                and not (degraded_ready and dock_status.lower() == "pause")
            ):
                return False

        battery_entity = self.config.get(CONF_BATTERY_ENTITY)
        if battery_entity:
            battery = parse_float(self._state(battery_entity))
            if battery is None or battery < float(self.config.get(CONF_MIN_BATTERY)):
                return False
        return True

    def _active_run_allows_error(self, error: str | None) -> bool:
        """Return whether the active vacuum run may tolerate this exact warning."""
        return bool(self.active_run and run_allows_error(self.active_run, error))

    def _run_waiting_for_start_ack(self, run: ActiveRun) -> bool:
        """Return whether a published segment still lacks start evidence."""
        return bool(
            run.phase != RUN_PHASE_CANCEL_PENDING
            and not run.observed_cleaning
            and not run.observed_segment_cleaning
            and not run.start_confirmed_at
            and (
                run.command_published
                or run.command_publish_requested_at
            )
        )

    def _active_run_allows_current_blocker(self) -> bool:
        """Return whether degraded vacuuming allows the current blocker."""
        blocker = self._current_blocker()
        return bool(
            self.active_run
            and self.active_run.vacuum_only
            and self.active_run.allowed_error_fingerprint
            and blocker
            and blocker.vacuum_only_safe
            and blocker.code == self.active_run.allowed_error_fingerprint
        )

    def _configured_error_is_available(self) -> bool:
        """Return whether a configured error sensor has a definitive state."""
        error_entity = self.config.get(CONF_ERROR_ENTITY)
        if not error_entity:
            return True
        error = normalize_state(self._state(error_entity))
        return error is not None and error.lower() not in {"unknown", "unavailable"}

    def _configured_error_is_clear(self) -> bool:
        """Return whether the configured error sensor is available and clear."""
        return self._configured_error_is_available() and is_error_clear(self.error_state)

    def _configured_status_is_available(self) -> bool:
        """Return whether a configured status sensor has a definitive state."""
        status_entity = self.config.get(CONF_STATUS_FLAG_ENTITY)
        if not status_entity:
            return True
        status = self._status_flag()
        return status not in {None, "unknown", "unavailable"}

    def _resource_state(self) -> ResourceState:
        """Return current Valetudo resource state."""
        mop_state = normalize_state(self._state(self.config.get(CONF_MOP_ATTACHMENT_ENTITY)))
        if mop_state in _UNKNOWN_OR_CLEAR_STATES:
            mop_attached = None
        else:
            mop_attached = mop_state.lower() not in {"off", "false", "detached", "missing", "not_attached"}

        return ResourceState(
            error=self.error_state,
            fresh_water=self._state(self.config.get(CONF_FRESH_WATER_ENTITY)),
            dirty_water=self._state(self.config.get(CONF_DIRTY_WATER_ENTITY)),
            detergent=self._state(self.config.get(CONF_DETERGENT_ENTITY)),
            dustbag=self._state(self.config.get(CONF_DUSTBAG_ENTITY)),
            mop_attached=mop_attached,
        )

    def _manual_tracking_allowed(self) -> bool:
        """Return whether manual run tracking is enabled."""
        if self.session and self.session.native_resume_guard_latched:
            return False
        if not self.config.get(CONF_MANUAL_TRACKING):
            return False
        if self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY) is None:
            return False
        return not self.paused or bool(self.config.get(CONF_TRACK_MANUAL_WHEN_PAUSED))

    def _all_people_away(self) -> bool:
        """Return True when every tracked person is away from home."""
        for entity_id in self.people_entities:
            if not self._person_is_away(entity_id):
                return False
        return True

    def _person_is_away(self, entity_id: str) -> bool:
        """Return whether a tracked person is away from home."""
        state = normalize_state(self._state(entity_id))
        normalized = state.lower() if state else None
        return normalized not in _UNKNOWN_PERSON_STATES and normalized != "home"

    def _any_tracked_person_home(self) -> bool:
        """Return whether any tracked person is explicitly home."""
        for entity_id in self.people_entities:
            state = normalize_state(self._state(entity_id))
            if state is not None and state.lower() == "home":
                return True
        return False

    def _latest_person_away_since(self) -> str:
        """Return the latest last-changed time among away tracked people."""
        latest_away_since: datetime | None = None
        for entity_id in self.people_entities:
            state = self.hass.states.get(entity_id)
            if state is None or not self._person_is_away(entity_id):
                continue
            if latest_away_since is None or state.last_changed > latest_away_since:
                latest_away_since = state.last_changed
        return (latest_away_since or dt_util.utcnow()).isoformat()

    def _status_flag(self) -> str | None:
        """Return the normalized Valetudo status flag."""
        status_flag = normalize_state(self._state(self.config.get(CONF_STATUS_FLAG_ENTITY)))
        return status_flag.lower() if status_flag else None

    def _room_id_from_estimated(self, estimated_value: Any) -> str | None:
        """Map estimated segment sensor state to a configured room id."""
        normalized = normalize_state(estimated_value)
        if not normalized:
            return None
        if normalized in self.room_by_id:
            return normalized
        if normalized in self.room_by_segment:
            return self.room_by_segment[normalized].room_id
        room = self.room_by_name.get(normalized.lower())
        return room.room_id if room else None

    def _current_auto_clean_day(self) -> str:
        """Return the Home Assistant local date used for daily auto-clean limits."""
        return dt_util.now().date().isoformat()

    def _state(self, entity_id: str | None) -> str | None:
        """Return a Home Assistant state string."""
        if not entity_id:
            return None
        state = self.hass.states.get(entity_id)
        return state.state if state else None

    def _entity_changed_after(
        self,
        entity_id: str | None,
        timestamp: str | None,
    ) -> bool:
        """Return whether an entity state changed after a persisted timestamp."""
        if not entity_id or not timestamp:
            return False
        state = self.hass.states.get(entity_id)
        changed_after = parse_datetime(timestamp)
        return bool(
            state
            and changed_after
            and state.last_changed > changed_after
        )

    def _configured_sensor_entities(self) -> list[str | None]:
        """Return entities the coordinator should listen to."""
        return [
            self.vacuum_entity,
            self.config.get(CONF_STATUS_FLAG_ENTITY),
            self.config.get(CONF_DOCK_STATUS_ENTITY),
            self.config.get(CONF_ERROR_ENTITY),
            self.config.get(CONF_BATTERY_ENTITY),
            self.config.get(CONF_CURRENT_AREA_ENTITY),
            self.config.get(CONF_CURRENT_TIME_ENTITY),
            self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY),
            self.config.get(CONF_FRESH_WATER_ENTITY),
            self.config.get(CONF_DIRTY_WATER_ENTITY),
            self.config.get(CONF_DETERGENT_ENTITY),
            self.config.get(CONF_DUSTBAG_ENTITY),
            self.config.get(CONF_MOP_ATTACHMENT_ENTITY),
            self.config.get(CONF_MODE_ENTITY),
        ]

    def _resource_sensor_entities(self) -> set[str]:
        """Return configured resource entities that can unblock dispatch."""
        return {
            entity_id
            for entity_id in (
                self.config.get(CONF_FRESH_WATER_ENTITY),
                self.config.get(CONF_DIRTY_WATER_ENTITY),
                self.config.get(CONF_DETERGENT_ENTITY),
                self.config.get(CONF_DUSTBAG_ENTITY),
                self.config.get(CONF_MOP_ATTACHMENT_ENTITY),
            )
            if entity_id
        }

    def _resource_sample_is_blocking(
        self,
        entity_id: str,
        value: str | None,
    ) -> bool:
        """Return whether one raw dock component sample is adverse."""
        normalized = (normalize_state(value) or "").lower()
        if entity_id == self.config.get(CONF_FRESH_WATER_ENTITY):
            return normalized in {"empty", "missing", "unknown", "unavailable"}
        if entity_id == self.config.get(CONF_DIRTY_WATER_ENTITY):
            return normalized in {"full", "missing", "unknown", "unavailable"}
        if entity_id == self.config.get(CONF_DETERGENT_ENTITY):
            return normalized in {"empty", "missing", "unknown", "unavailable"}
        if entity_id == self.config.get(CONF_DUSTBAG_ENTITY):
            return normalized in {"full", "missing", "unknown", "unavailable"}
        return False

    async def _async_resource_fault_confirmed(
        self,
        entity_id: str,
        now: datetime,
    ) -> bool:
        """Debounce active-run component faults while blocking new dispatches."""
        candidates = getattr(self, "_resource_candidates", None)
        if candidates is None:
            self._resource_candidates = {}
            candidates = self._resource_candidates
        value = normalize_state(self._state(entity_id))
        if not self._resource_sample_is_blocking(entity_id, value):
            if entity_id in candidates:
                candidates.pop(entity_id, None)
                await self._async_save_store()
            self._schedule_resource_settle_timer()
            return True
        settle_seconds = int(
            self.config.get(
                CONF_RESOURCE_SETTLE,
                DEFAULT_RESOURCE_SETTLE,
            )
        )
        if settle_seconds <= 0:
            candidates.pop(entity_id, None)
            return True
        candidate = candidates.get(entity_id)
        if (
            candidate
            and candidate.get("state") == value
            and (deadline := parse_datetime(candidate.get("deadline")))
            and now >= deadline
        ):
            candidates.pop(entity_id, None)
            await self._async_save_store()
            self._schedule_resource_settle_timer()
            return True
        if not candidate or candidate.get("state") != value:
            deadline = now + timedelta(seconds=settle_seconds)
            candidates[entity_id] = {
                "state": value or "",
                "observed_at": now.isoformat(),
                "deadline": deadline.isoformat(),
            }
            await self._async_save_store()
            self._notify_listeners()
        self._schedule_resource_settle_timer()
        return False

    def _schedule_resource_settle_timer(self) -> None:
        """Schedule confirmation of the earliest component-fault candidate."""
        candidates = getattr(self, "_resource_candidates", {})
        deadlines = [
            parsed
            for candidate in candidates.values()
            if (parsed := parse_datetime(candidate.get("deadline"))) is not None
        ]
        if not deadlines:
            if getattr(self, "_resource_settle_cancel", None) is not None:
                self._resource_settle_cancel()
                self._resource_settle_cancel = None
            self._resource_settle_deadline = None
            return
        deadline = min(deadlines)
        existing = getattr(self, "_resource_settle_deadline", None)
        if (
            getattr(self, "_resource_settle_cancel", None) is not None
            and existing is not None
            and existing <= deadline
        ):
            return
        if getattr(self, "_resource_settle_cancel", None) is not None:
            self._resource_settle_cancel()
        self._resource_settle_deadline = deadline

        def timer_finished(_now: datetime) -> None:
            self._resource_settle_cancel = None
            self._resource_settle_deadline = None
            schedule_hass_task(
                self.hass,
                self._async_confirm_resource_faults_serialized(),
            )

        remaining = max(0.0, (deadline - dt_util.utcnow()).total_seconds())
        self._resource_settle_cancel = async_call_later(
            self.hass,
            remaining,
            timer_finished,
        )

    async def _async_confirm_resource_faults_serialized(self) -> None:
        """Confirm persistent component faults without racing entity events."""
        async with self._event_lock:
            now = dt_util.utcnow()
            due_entities = [
                entity_id
                for entity_id, candidate in getattr(
                    self,
                    "_resource_candidates",
                    {},
                ).items()
                if (
                    (deadline := parse_datetime(candidate.get("deadline")))
                    is not None
                    and now >= deadline
                    and normalize_state(self._state(entity_id))
                    == candidate.get("state")
                )
            ]
            stale_entities = [
                entity_id
                for entity_id, candidate in getattr(
                    self,
                    "_resource_candidates",
                    {},
                ).items()
                if normalize_state(self._state(entity_id))
                != candidate.get("state")
            ]
            for entity_id in due_entities:
                self._resource_candidates.pop(entity_id, None)
            for entity_id in stale_entities:
                self._resource_candidates.pop(entity_id, None)
            if due_entities or stale_entities:
                await self._async_save_store()
            if due_entities:
                blocker = classify_blocker(self._resource_state())
                if self.active_run and blocker:
                    await self._async_handle_active_run_error(blocker.reason)
                elif self.session and self.session.active:
                    await self._async_maybe_start_next_room()
            self._schedule_resource_settle_timer()

    async def _async_load_store(self) -> None:
        """Load persisted pause state and room ledgers."""
        stored = await self._store.async_load()
        if not isinstance(stored, dict):
            return
        try:
            stored_data_version = int(
                stored.get("coordinator_data_version", 0) or 0
            )
        except (TypeError, ValueError):
            stored_data_version = 0
        legacy_pre_v03_data = stored_data_version < COORDINATOR_DATA_VERSION

        self.paused = bool(stored.get("paused", False))
        self.pause_reason = stored.get("pause_reason")
        self.away_since = stored.get("away_since")
        self.session = SessionState.from_dict(stored.get("session"))
        self.settings_snapshot = AutoCleanSettingsSnapshot.from_dict(
            stored.get("settings_snapshot")
        )
        migrated_recoverable_terminal = False
        if (
            legacy_pre_v03_data
            and self.session
            and not self.session.active
            and not self.session.cancelled
            and (
                self.session.terminal_reason
                in {"blocked", "mop_resource_deferred"}
                or (
                    self.session.terminal_reason == "needs_help"
                    and not is_unrecoverable_error(
                        self.session.terminal_message
                    )
                )
            )
        ):
            migrated_recoverable_terminal = True
            migrated_reason = (
                self.session.terminal_message
                or "Recoverable v0.2.0 session restored"
            )
            self.session.active = True
            self.session.terminal_reason = None
            self.session.terminal_message = None
            self.session.terminal_cause = None
            self.session.needs_help = False
            self.session.notification_sent = False
            self.session.settings_prepared = self.settings_snapshot is not None
            self.session.recovery_notification_fingerprint = None
            self.session.recovery_notification_attempts = 0
            self.session.recovery_notification_sent = False
            self.session.last_recovery_notification_fingerprint = None
            self.session.last_recovery_notification_at = None
            self.session.recovery_announcement_sent = False
            self.session.enter_recovery(
                code="migration.v0_2_recoverable_session",
                disposition=BLOCKER_RECOVERABLE,
                reason=migrated_reason,
                phase="waiting_for_retry",
                next_retry_at=None,
                waiting_for_physical_fix=True,
                operator_action=(
                    "Clear the reported condition; the stored queue will resume."
                ),
            )
        self.active_run = ActiveRun.from_dict(stored.get("active_run"))
        self._active_run_restored = self.active_run is not None
        self.manual_run = ActiveRun.from_dict(stored.get("manual_run"))
        self.retained_task_guard = RetainedTaskGuard.from_dict(
            stored.get("retained_task_guard")
        )
        stored_resource_candidates = stored.get("resource_candidates")
        if isinstance(stored_resource_candidates, dict):
            self._resource_candidates = {
                str(entity_id): dict(candidate)
                for entity_id, candidate in stored_resource_candidates.items()
                if isinstance(candidate, dict)
            }
        if (
            self.session
            and self.settings_snapshot is not None
            and not migrated_recoverable_terminal
        ):
            self.session.settings_prepared = True
        stored_outcomes = stored.get("while_away_outcomes", [])
        if isinstance(stored_outcomes, list):
            self.while_away_outcomes = [
                outcome
                for outcome in (
                    WhileAwayOutcome.from_dict(item)
                    for item in stored_outcomes
                )
                if outcome is not None
            ]
        stored_sequence = stored.get("while_away_outcome_sequence", 0)
        try:
            persisted_sequence = int(stored_sequence)
        except (TypeError, ValueError):
            persisted_sequence = 0
        self._while_away_outcome_sequence = max(
            persisted_sequence,
            max(
                (
                    outcome.sequence or 0
                    for outcome in self.while_away_outcomes
                ),
                default=0,
            ),
        )
        stored_rooms = stored.get("rooms", {})
        if isinstance(stored_rooms, dict):
            for room in self.rooms:
                self.ledgers[room.room_id] = RoomLedger.from_dict(stored_rooms.get(room.room_id))
        stored_disabled_room_ids = stored.get("disabled_room_ids", [])
        if isinstance(stored_disabled_room_ids, list):
            configured_room_ids = set(self.room_by_id)
            self.disabled_room_ids = {
                room_id
                for room_id in (str(item) for item in stored_disabled_room_ids)
                if room_id in configured_room_ids
            }
        if legacy_pre_v03_data:
            await self._async_save_store()

    async def _async_save_store(self) -> None:
        """Persist pause state and room ledgers."""
        await self._store.async_save(
            {
                "coordinator_data_version": COORDINATOR_DATA_VERSION,
                "paused": self.paused,
                "pause_reason": self.pause_reason,
                "away_since": self.away_since,
                "session": self.session.to_dict() if self.session else None,
                "active_run": self.active_run.to_dict() if self.active_run else None,
                "manual_run": self.manual_run.to_dict() if self.manual_run else None,
                "retained_task_guard": (
                    self.retained_task_guard.to_dict()
                    if self.retained_task_guard
                    else None
                ),
                "resource_candidates": getattr(
                    self,
                    "_resource_candidates",
                    {},
                ),
                "settings_snapshot": self.settings_snapshot.to_dict() if self.settings_snapshot else None,
                "while_away_outcomes": [outcome.to_dict() for outcome in self.while_away_outcomes],
                "while_away_outcome_sequence": self._while_away_outcome_sequence,
                "disabled_room_ids": sorted(self.disabled_room_ids),
                "rooms": {room_id: ledger.to_dict() for room_id, ledger in self.ledgers.items()},
            }
        )

    def _require_room(self, room_id: str) -> None:
        """Raise if the room id is not configured."""
        if room_id not in self.room_by_id:
            raise ValueError(f"Unknown room_id: {room_id}")

    @callback
    def _notify_listeners(self) -> None:
        """Notify Home Assistant entities."""
        for update_callback in list(self._listeners):
            update_callback()


def _slugify(value: str) -> str:
    """Return a stable id fragment."""
    slug = re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")
    return slug or DOMAIN
