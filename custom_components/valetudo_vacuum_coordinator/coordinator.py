"""Coordinator for away-only Valetudo room cleaning."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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
    CONF_TRACK_MANUAL_WHEN_PAUSED,
    CONF_WATER_ENTITY,
    CONF_WATER_MOP_OPTION,
    DEFAULT_DOCK_SETTLE,
    DEFAULT_BLOCKED_SESSION_TIMEOUT,
    DEFAULT_DISPATCH_START_TIMEOUT,
    DEFAULT_MIN_BATTERY,
    DEFAULT_NATIVE_RESUME_ENABLED,
    DEFAULT_NATIVE_RESUME_TIMEOUT,
    DOMAIN,
    SERVICE_DOCK_ACTION,
    STATE_DEFERRED,
    STATE_DEGRADED,
    STATE_ERROR,
    STATE_IDLE,
    STATE_PAUSED,
    STATE_RUNNING,
    STATE_WAITING,
    STORE_KEY,
    STORE_VERSION,
)
from .logic import (
    ActiveRun,
    AutoCleanSettingsSnapshot,
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
    RoomConfig,
    RoomLedger,
    SessionState,
    WhileAwayOutcome,
    build_auto_clean_summary,
    build_while_away_messages,
    clean_water_empty_reason,
    cleaning_block_reason,
    evaluate_run_success,
    error_contains_any,
    allowed_error_fingerprint,
    is_clean_water_empty_error,
    is_low_battery_error,
    is_recoverable_navigation_error,
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
    room_auto_cleaned_on,
    room_sort_key,
    run_allows_error,
    schedule_hass_task,
    select_next_room,
    utcnow_iso,
)

_LOGGER = logging.getLogger(__name__)

_READY_VACUUM_STATES = {"docked", "idle"}
_AT_DOCK_VACUUM_STATES = {"docked", "idle", "charging"}
_BUSY_DOCK_STATES = {"cleaning", "emptying", "pause"}
_UNKNOWN_OR_CLEAR_STATES = {None, "", "unknown", "unavailable", "none"}
_UNKNOWN_PERSON_STATES = {None, "", "unknown", "unavailable"}
_RESTORE_RECONCILE_DELAY_SECONDS = 15
_TERMINAL_CLEANUP_RETRY_SECONDS = 30
_MAX_TERMINAL_CLEANUP_RETRIES = 3


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
        self.settings_snapshot: AutoCleanSettingsSnapshot | None = None
        self.while_away_outcomes: list[WhileAwayOutcome] = []
        self.last_error: str | None = None

        self._listeners: list[Callable[[], None]] = []
        self._unsubscribers: list[Callable[[], None]] = []
        self._away_timer_cancel: Callable[[], None] | None = None
        self._next_day_timer_cancel: Callable[[], None] | None = None
        self._terminal_cleanup_retry_cancel: Callable[[], None] | None = None
        self._dock_settle_cancel: Callable[[], None] | None = None
        self._native_resume_timeout_cancel: Callable[[], None] | None = None
        self._dispatch_start_timeout_cancel: Callable[[], None] | None = None
        self._blocked_session_watchdog_cancel: Callable[[], None] | None = None
        self._blocked_watchdog_expiring = False
        self._terminal_cleanup_retry_attempts = 0
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
                or is_clean_water_empty_error(self.error_state)
                or is_low_battery_error(self.error_state)
            )
        ):
            return STATE_DEGRADED
        if not is_error_clear(self.error_state):
            return STATE_ERROR
        if self.session and self.session.active:
            return STATE_WAITING
        return STATE_IDLE

    @property
    def auto_cleaning(self) -> bool:
        """Return whether an away auto-clean session is active or pending summary."""
        if not self.session:
            return False
        if self.session.notification_sent and self.settings_snapshot is None:
            return False
        return bool(self.session.active or self.active_run or self.session.terminal_reason)

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
                self.active_run
                and self.active_run.phase in NATIVE_RESUME_PENDING_PHASES
            )
        )

    @property
    def native_resume_attributes(self) -> dict[str, Any]:
        """Return recovery details shared by coordinator entities."""
        run = self.active_run
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
            "resume_nudge_enabled": bool(
                self.config.get(CONF_RESUME_NUDGE_ENABLED, False)
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
        auto_clean_day = self._current_auto_clean_day()
        pending: list[RoomConfig] = []
        pending_ids: set[str] = set()

        if self.session and self.session.pending_recovery_room_id:
            room = self.room_by_id.get(self.session.pending_recovery_room_id)
            if room and self._room_auto_clean_enabled(room):
                pending.append(room)
                pending_ids.add(room.room_id)

        priority_retry_room_ids = (
            self.session.priority_retry_room_ids if self.session else []
        )
        for room_id in priority_retry_room_ids:
            room = self.room_by_id.get(room_id)
            if room and self._room_auto_clean_enabled(room) and room.room_id not in pending_ids:
                pending.append(room)
                pending_ids.add(room.room_id)

        pending.extend(
            room
            for room in self.rooms
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
            if room and self._room_auto_clean_enabled(room) and room.room_id not in pending_ids:
                pending.append(room)
                pending_ids.add(room.room_id)
        return pending

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
        self.session = SessionState(session_id=utcnow_iso(), started_at=utcnow_iso())
        _LOGGER.info("Starting Valetudo away-cleaning session %s (%s)", self.session.session_id, reason)
        await self._async_prepare_auto_clean_settings()
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

        if self.session:
            self.session.cancelled = True
            self.session.active = False
            self.session.active_room_id = None
            self.session.pending_recovery_room_id = None
            self.session.pending_recovery_reason = None
            self.session.pending_recovery_priority = False
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
        elif had_active_session:
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
        """Execute one persisted destructive cancellation for an active run."""
        run = self.active_run
        if not run or run.phase != RUN_PHASE_CANCEL_PENDING:
            return True

        reason = run.cancel_reason or "Cancelled"
        if not run.cancel_stop_attempted:
            try:
                await self.hass.services.async_call(
                    "vacuum",
                    "stop",
                    {ATTR_ENTITY_ID: self.vacuum_entity},
                    blocking=True,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception(
                    "Failed to stop %s while cancelling its active run",
                    self.vacuum_entity,
                )
                if run.cancel_continue_session and self.session:
                    self.session.active = True
                    self.session.terminal_reason = None
                    self.session.terminal_message = None
                    self.session.needs_help = False
                    self.session.native_resume_guard_latched = True
                else:
                    self._set_needs_help_state(
                        f"Vacuum command may still be active after cancellation failed: {err}"
                    )
                await self._async_save_store()
                self._notify_listeners()
                return False
            run.cancel_stop_attempted = True
            await self._async_save_store()

        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if vacuum_state != "docked":
            try:
                await self.hass.services.async_call(
                    "vacuum",
                    "return_to_base",
                    {ATTR_ENTITY_ID: self.vacuum_entity},
                    blocking=True,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception(
                    "Failed to return %s after cancellation",
                    self.vacuum_entity,
                )
                if run.cancel_continue_session and self.session:
                    self.session.active = True
                    self.session.terminal_reason = None
                    self.session.terminal_message = None
                    self.session.needs_help = False
                    self.session.native_resume_guard_latched = True
                else:
                    self._set_needs_help_state(
                        f"Vacuum stopped but could not return to base after cancellation: {err}"
                    )
                await self._async_save_store()
                self._notify_listeners()
                return False

        if run.room_id:
            ledger = self.ledgers.setdefault(run.room_id, RoomLedger())
            mark_failure(ledger, utcnow_iso(), reason)
            if self.session:
                if run.fallback_vacuum:
                    self.session.mark_fallback_failed(run.room_id, reason)
                else:
                    self.session.mark_failed(run.room_id, reason)
        if self.session:
            self.session.native_resume_guard_latched = False
        self._clear_active_run()
        return True

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
            normalize_state(self._state(self.vacuum_entity)) != "docked"
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
        if observations_changed:
            await self._async_save_store()
            self._notify_listeners()

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
                if self._active_run_allows_error(new_state.state):
                    await self._async_reconcile_active_run(now)
                else:
                    await self._async_handle_active_run_error(new_state.state)
            else:
                recovery_room_id, recovery_reason = self._recovering_room_for_error()
                if recovery_room_id and not (
                    is_low_battery_error(new_state.state)
                    and is_low_battery_error(recovery_reason)
                ):
                    await self._async_handle_pending_recovery_error(new_state.state)
                elif (
                    self.session
                    and self.session.active
                    and self.session.degraded_reason
                    and not is_low_battery_error(new_state.state)
                    and not is_clean_water_empty_error(new_state.state)
                ):
                    await self._async_mark_needs_help(new_state.state)
                elif self.session and self.session.active:
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
            if (
                entity_id == self.config.get(CONF_FRESH_WATER_ENTITY)
                and self.active_run
                and clean_water_empty_reason(self._resource_state())
                and not self.active_run.fallback_vacuum
                and (
                    (room := self.active_room) is not None
                    and room.mop_required
                )
            ):
                await self._async_handle_active_run_error(
                    clean_water_empty_reason(self._resource_state())
                    or "fresh water is empty"
                )
                return
            await self._async_maybe_start_next_room()
            await self._async_maybe_send_auto_clean_summary()
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
            or self.session.terminal_reason
            not in {"mop_resource_deferred", "blocked"}
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
            if self._active_run_allows_error(self.error_state):
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
                    run.dispatch_deadline = None
                    self._cancel_dispatch_start_timeout()
                    changed = True
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
                        if not run.post_suspend_segment_observed:
                            run.post_suspend_segment_observed = True
                            changed = True
                        if not run.observed_segment_cleaning:
                            run.observed_segment_cleaning = True
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
                if self._manual_tracking_allowed():
                    self._start_manual_run(now)
                elif cleared:
                    self._notify_listeners()
                if cleared:
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
                await self._async_finish_manual_run(now)
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
                run.dispatch_deadline = None
                self._cancel_dispatch_start_timeout()
                changed = True
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
                    if not run.post_suspend_segment_observed:
                        run.post_suspend_segment_observed = True
                        changed = True
                    if not run.observed_segment_cleaning:
                        run.observed_segment_cleaning = True
                        changed = True
                changed = self._mark_active_run_resumed(run) or changed

        if entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY):
            if normalized_state == "segment":
                if not run.observed_segment_cleaning:
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

        if (
            entity_id == self.config.get(CONF_DOCK_STATUS_ENTITY)
            and normalized_state in _BUSY_DOCK_STATES
            and normalize_state(self._state(self.vacuum_entity)) != "cleaning"
            and not self._active_run_allows_error(self.error_state)
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
        return changed

    def _mark_active_run_resumed(self, run: ActiveRun) -> bool:
        """Adopt a native cleaning+segment resume for the same active run."""
        if (
            run.phase not in {
                RUN_PHASE_SUSPENDED,
                RUN_PHASE_DOCK_INTERRUPT,
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

        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        status_flag = self._status_flag()
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        dock_status = dock_status.lower() if dock_status else None
        allowed_degraded_error = self._active_run_allows_error(self.error_state)
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
                if not run.post_suspend_segment_observed:
                    run.post_suspend_segment_observed = True
                    changed = True
                if not run.observed_segment_cleaning:
                    run.observed_segment_cleaning = True
                    changed = True
            changed = self._mark_active_run_resumed(run) or changed

        if changed:
            await self._async_save_store()
            self._notify_listeners()

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
            self._schedule_native_resume_timeout()

        if vacuum_state in _AT_DOCK_VACUUM_STATES:
            await self._async_reconcile_active_run_at_dock(now)
            return True

        if run.docked_at is not None:
            run.docked_at = None
            self._cancel_dock_settle_timer()
            await self._async_save_store()
        return run.native_resume_pending

    async def _async_reconcile_active_run_at_dock(self, now: datetime) -> None:
        """Wait for a stable dock snapshot before finalizing a retained run."""
        run = self.active_run
        if not run:
            return
        dock_status = normalize_state(
            self._state(self.config.get(CONF_DOCK_STATUS_ENTITY))
        )
        dock_status = dock_status.lower() if dock_status else None
        allowed_degraded_error = self._active_run_allows_error(self.error_state)
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
            self._schedule_native_resume_timeout()
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
        """Terminate a stalled native resume without commanding the robot."""
        run = self.active_run
        if not run or not run.native_resume_pending:
            return
        run.phase = RUN_PHASE_RECOVERY_STALLED
        reason = (
            f"Native resume timed out after "
            f"{int(self.config.get(CONF_NATIVE_RESUME_TIMEOUT, DEFAULT_NATIVE_RESUME_TIMEOUT))}s"
        )
        run.suspend_reason = reason
        await self._async_save_store()
        self._notify_listeners()
        await self._async_terminate_active_run_needs_help(reason)

    async def _async_terminate_active_run_needs_help(self, message: str) -> None:
        """Fail the uncredited room and end the session without robot commands."""
        run = self.active_run
        if not run:
            return
        if run.room_id:
            run.finalize_estimated_room(dt_util.utcnow())
            ledger = self.ledgers.setdefault(run.room_id, RoomLedger())
            mark_failure(ledger, utcnow_iso(), message)
            if self.session:
                if run.fallback_vacuum:
                    self.session.mark_fallback_failed(run.room_id, message)
                else:
                    self.session.mark_failed(run.room_id, message)
                self._record_while_away_outcome(
                    "failed",
                    run.room_id,
                    message,
                )
        if self.session:
            self.session.native_resume_guard_latched = True
        self._set_needs_help_state(message)
        self._clear_active_run()
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    def _clear_active_run(self) -> None:
        """Clear the active run and all timers tied to it."""
        self.active_run = None
        self._active_run_restored = False
        self._restored_dispatch_intent_deadline = None
        if self.session:
            self.session.active_room_id = None
        self._cancel_active_run_timers()

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
                seconds=int(
                    self.config.get(
                        CONF_DISPATCH_START_TIMEOUT,
                        DEFAULT_DISPATCH_START_TIMEOUT,
                    )
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
        """Consume a published token that firmware never began executing."""
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
        cancellation_reason = (
            self.session.degraded_reason
            if self.session and self.session.degraded_reason
            else failure_reason
        )
        await self._async_finish_active_run(
            success_override=False,
            failure_reason=failure_reason,
            continue_session=False,
            send_summary=False,
        )
        try:
            async with asyncio.timeout(15):
                await self._async_return_to_dock_or_stop_resumable(
                    cancellation_reason,
                    command_may_be_pending=True,
                )
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception(
                "Could not cancel timed-out segment dispatch for %s",
                self.vacuum_entity,
            )
            await self._async_mark_needs_help(
                f"Could not cancel timed-out segment dispatch: {err}"
            )
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

    def _cancel_active_run_timers(self) -> None:
        """Cancel all timers owned by the active run."""
        self._cancel_dock_settle_timer()
        self._cancel_native_resume_timeout()
        self._cancel_dispatch_start_timeout()

    async def _async_arm_blocked_session_watchdog(
        self,
        reason: str | None = None,
    ) -> None:
        """Persist and schedule a finite no-progress deadline."""
        if getattr(self, "_blocked_watchdog_expiring", False):
            return
        session = self.session
        if not session or not session.active or self.active_run:
            return
        changed = False
        if reason and session.blocked_reason != reason:
            session.blocked_reason = reason
            changed = True
        now = dt_util.utcnow()
        candidate_deadline = now + timedelta(
            seconds=self._blocked_session_timeout_seconds(reason)
        )
        deadline = parse_datetime(session.blocked_deadline)
        deadline_shortened = False
        if deadline is None or candidate_deadline < deadline:
            deadline = candidate_deadline
            session.blocked_deadline = deadline.isoformat()
            changed = True
            deadline_shortened = True
        if changed:
            await self._async_save_store()
        if (
            deadline_shortened
            and getattr(self, "_blocked_session_watchdog_cancel", None)
            is not None
        ):
            self._blocked_session_watchdog_cancel()
            self._blocked_session_watchdog_cancel = None
        if getattr(self, "_blocked_session_watchdog_cancel", None) is not None:
            return
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
        normalized_reason = (normalize_state(reason) or "").lower()
        battery_entity = self.config.get(CONF_BATTERY_ENTITY)
        battery = parse_float(self._state(battery_entity)) if battery_entity else None
        minimum_battery = float(
            self.config.get(CONF_MIN_BATTERY, DEFAULT_MIN_BATTERY)
        )
        long_recovery_wait = bool(
            is_low_battery_error(reason)
            or "battery" in normalized_reason
            or "charging" in normalized_reason
            or normalize_state(self._state(self.vacuum_entity)) == "charging"
            or self._status_flag() == "resumable"
            or (
                battery is not None
                and battery < minimum_battery
            )
        )
        if long_recovery_wait:
            return int(
                self.config.get(
                    CONF_NATIVE_RESUME_TIMEOUT,
                    DEFAULT_NATIVE_RESUME_TIMEOUT,
                )
            )
        return int(
            self.config.get(
                CONF_BLOCKED_SESSION_TIMEOUT,
                DEFAULT_BLOCKED_SESSION_TIMEOUT,
            )
        )

    async def _async_expire_blocked_session_serialized(self) -> None:
        """Re-evaluate once, then terminate a session that remains blocked."""
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
            reason = (
                self.session.blocked_reason
                or self._current_blocked_reason()
                or "Automatic cleaning could not make progress"
            )
            if self.session.degraded_reason:
                await self._async_terminalize_mop_deferred(
                    reason,
                    cause="blocked_timeout",
                )
            else:
                await self._async_terminalize_blocked(
                    reason,
                    cause="blocked_timeout",
                )

    def _clear_blocked_session_watchdog(self) -> None:
        """Clear the no-progress deadline, reason, and timer."""
        if getattr(self, "_blocked_session_watchdog_cancel", None) is not None:
            self._blocked_session_watchdog_cancel()
            self._blocked_session_watchdog_cancel = None
        if self.session:
            self.session.blocked_deadline = None
            self.session.blocked_reason = None

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
        if self._legacy_low_battery_retry_room_id():
            await self._async_mark_needs_help(
                "A legacy low-battery restart was queued; passive native resume "
                "does not publish a fresh segment fallback"
            )
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

        resources = self._resource_state()
        degraded_reason = clean_water_empty_reason(resources)
        if degraded_reason:
            self._activate_clean_water_degraded(degraded_reason)
        if self.session.degraded_reason:
            await self._async_maybe_start_degraded_room(resources)
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

        selection, skipped = select_next_room(
            self._auto_clean_rooms(),
            self.ledgers,
            set(self.session.attempted_room_ids),
            self._resource_state(),
            False,
            self._current_auto_clean_day(),
            self.session.retry_room_ids,
            self.session.priority_retry_room_ids,
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
            self._clear_blocked_session_watchdog()
            self.session.active = False
            self.session.active_room_id = None
            self.session.pending_recovery_room_id = None
            self.session.pending_recovery_reason = None
            self.session.pending_recovery_priority = False
            self.session.retry_room_ids = []
            self.session.priority_retry_room_ids = []
            self.session.terminal_reason = no_selection_terminal_reason(
                completed_room_ids=self.session.completed_room_ids,
                skipped_room_ids=self.session.skipped_room_ids,
                failed_room_ids=self.session.failed_room_ids,
                current_skipped_count=len(skipped),
            )
            self.session.terminal_message = self._first_session_block_reason()
            await self._async_save_store()
            self._notify_listeners()
            await self._async_maybe_send_auto_clean_summary()
            return

        await self._async_start_room(
            selection.room,
            vacuum_only=selection.vacuum_only,
            fallback_vacuum=selection.fallback_vacuum,
        )

    async def _async_maybe_start_degraded_room(
        self,
        resources: ResourceState,
    ) -> None:
        """Process the finite clean-water-degraded room lanes."""
        session = self.session
        if not session or not session.active or self.active_run:
            return
        if (
            not is_error_clear(resources.error)
            and not is_clean_water_empty_error(resources.error)
            and not is_low_battery_error(resources.error)
        ):
            await self._async_mark_needs_help(
                resources.error or "Unknown error"
            )
            return

        if not await self._async_prepare_degraded_vacuuming():
            await self._async_terminalize_mop_deferred(
                "Could not prepare vacuum-only cleaning while the clean water tank is empty",
                cause="preparation_failed",
            )
            return

        resources = self._resource_state()
        native_room = self._next_degraded_native_room()
        if native_room:
            if not self._vacuum_ready_for_selection(vacuum_only=True):
                await self._async_arm_blocked_session_watchdog(
                    self._current_blocked_reason(resources)
                    or session.degraded_reason
                )
                self._notify_listeners()
                return
            await self._async_start_room(
                native_room,
                vacuum_only=True,
                fallback_vacuum=False,
            )
            return

        current_water_reason = clean_water_empty_reason(resources)
        if current_water_reason is None:
            self._queue_refilled_normal_retries()
            fallback_attempted = set(session.fallback_attempted_room_ids)
            session.retry_room_ids = [
                room_id
                for room_id in session.retry_room_ids
                if room_id not in fallback_attempted
            ]
            session.priority_retry_room_ids = [
                room_id
                for room_id in session.priority_retry_room_ids
                if room_id not in fallback_attempted
            ]
            attempted = set(session.attempted_room_ids)
            attempted.update(fallback_attempted)
            selection, skipped = select_next_room(
                self._auto_clean_rooms(),
                self.ledgers,
                attempted,
                resources,
                False,
                self._current_auto_clean_day(),
                session.retry_room_ids,
                session.priority_retry_room_ids,
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
            if session.deferred_full_clean_room_ids:
                await self._async_terminalize_mop_deferred(
                    session.degraded_reason or "Clean water tank was empty",
                    cause="refilled_after_fallback",
                )
                return
            await self._async_finish_session_no_selection()
            return

        if self.config.get(CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED):
            fallback_room = self._next_degraded_fallback_room()
            if fallback_room:
                if not self._vacuum_ready_for_selection(vacuum_only=True):
                    await self._async_arm_blocked_session_watchdog(
                        self._current_blocked_reason(resources)
                        or current_water_reason
                    )
                    self._notify_listeners()
                    return
                await self._async_start_room(
                    fallback_room,
                    vacuum_only=True,
                    fallback_vacuum=True,
                )
                return

        await self._async_terminalize_mop_deferred(
            current_water_reason,
            cause="queue_exhausted",
        )

    def _activate_clean_water_degraded(self, reason: str) -> None:
        """Latch degraded mode and record every outstanding full mop obligation."""
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

    def _next_degraded_fallback_room(self) -> RoomConfig | None:
        """Return the next incomplete dual-mode room with an unused fallback token."""
        if not self.session:
            return None
        fallback_attempted = set(self.session.fallback_attempted_room_ids)
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
        fallback_attempted = set(self.session.fallback_attempted_room_ids)
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
        if clean_water_empty_reason(self._resource_state()) is None:
            pending_ids = {room.room_id for room in rooms}
            for room_id in self.session.priority_retry_room_ids:
                room = self.room_by_id.get(room_id)
                if (
                    room
                    and self._room_auto_clean_enabled(room)
                    and room_id not in fallback_attempted
                    and room_id not in pending_ids
                ):
                    rooms.append(room)
                    pending_ids.add(room_id)
            rooms.extend(
                room
                for room in ordered
                if room.mop_required
                and room.room_id not in attempted
                and room.room_id not in fallback_attempted
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
                    and room_id not in fallback_attempted
                    and room_id not in pending_ids
                ):
                    rooms.append(room)
                    pending_ids.add(room_id)
        elif self.config.get(CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED):
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
        return rooms

    def _queue_refilled_normal_retries(self) -> None:
        """Queue one normal retry for a water-failed dual room after refill."""
        session = self.session
        if not session:
            return
        fallback_attempted = set(session.fallback_attempted_room_ids)
        for room_id, reason in list(session.failed_room_reasons.items()):
            room = self.room_by_id.get(room_id)
            if (
                room
                and room.mop_required
                and room_id not in fallback_attempted
                and is_clean_water_empty_error(reason)
                and session.can_retry_room(room_id)
            ):
                session.queue_retry(room_id)

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
            if identifier and (
                normalize_state(self._state(self.vacuum_entity)) == "error"
                or (
                    dock_status is not None
                    and dock_status.lower() in _BUSY_DOCK_STATES
                )
            ):
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
        try:
            async with asyncio.timeout(15):
                await self._async_apply_mode(vacuum_only=True)
        except Exception:  # noqa: BLE001
            _LOGGER.warning(
                "Could not select vacuum mode for degraded session %s",
                self.name,
                exc_info=True,
            )
            return False
        session.degraded_preparation_completed = True
        await self._async_save_store()
        return True

    async def _async_finish_session_no_selection(self) -> None:
        """Finish an exhausted ordinary session."""
        if not self.session:
            return
        self._cancel_away_timer()
        self._clear_blocked_session_watchdog()
        self.session.active = False
        self.session.active_room_id = None
        self.session.pending_recovery_room_id = None
        self.session.pending_recovery_reason = None
        self.session.pending_recovery_priority = False
        self.session.retry_room_ids = []
        self.session.priority_retry_room_ids = []
        self.session.terminal_reason = no_selection_terminal_reason(
            completed_room_ids=self.session.completed_room_ids,
            skipped_room_ids=self.session.skipped_room_ids,
            failed_room_ids=self.session.failed_room_ids,
            current_skipped_count=0,
        )
        self.session.terminal_message = self._first_session_block_reason()
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    async def _async_terminalize_mop_deferred(
        self,
        message: str,
        *,
        cause: str,
    ) -> None:
        """End a safely stopped degraded session without leaving actionable work."""
        if not self.session:
            return
        self._cancel_away_timer()
        self._clear_blocked_session_watchdog()
        self.session.active = False
        self.session.active_room_id = None
        self.session.pending_recovery_room_id = None
        self.session.pending_recovery_reason = None
        self.session.pending_recovery_priority = False
        self.session.retry_room_ids = []
        self.session.priority_retry_room_ids = []
        self.session.blocked_deadline = None
        self.session.terminal_reason = "mop_resource_deferred"
        self.session.terminal_message = message
        self.session.terminal_cause = cause
        self.session.needs_help = False
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    async def _async_terminalize_blocked(
        self,
        message: str,
        *,
        cause: str,
    ) -> None:
        """End an ordinary session that remained safely but persistently blocked."""
        if not self.session:
            return
        self._cancel_away_timer()
        self._clear_blocked_session_watchdog()
        self.session.active = False
        self.session.active_room_id = None
        self.session.pending_recovery_room_id = None
        self.session.pending_recovery_reason = None
        self.session.pending_recovery_priority = False
        self.session.retry_room_ids = []
        self.session.priority_retry_room_ids = []
        self.session.terminal_reason = "blocked"
        self.session.terminal_message = message
        self.session.terminal_cause = cause
        self.session.needs_help = False
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    def _current_blocked_reason(
        self,
        resources: ResourceState | None = None,
    ) -> str | None:
        """Return a concise reason the next dispatch is currently refused."""
        resources = resources or self._resource_state()
        general_reason = cleaning_block_reason(resources)
        if general_reason:
            return general_reason
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if vacuum_state not in _READY_VACUUM_STATES:
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
            self._clear_active_run()
            if self.session is session:
                failure_reason = f"Could not dispatch {room.name}: {err}"
                mark_failure(
                    self.ledgers.setdefault(room.room_id, RoomLedger()),
                    utcnow_iso(),
                    failure_reason,
                )
                if fallback_vacuum:
                    session.mark_fallback_failed(room.room_id, failure_reason)
                else:
                    session.mark_failed(room.room_id, failure_reason)
                self._record_while_away_outcome(
                    "failed",
                    room.room_id,
                    failure_reason,
                )
                if not fallback_vacuum:
                    self._set_needs_help_state(failure_reason)
            await self._async_save_store()
            self._notify_listeners()
            if fallback_vacuum and session.active:
                await self._async_maybe_start_next_room()
            else:
                await self._async_maybe_send_auto_clean_summary()
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
            session.discard_retry(room.room_id)
            if session.pending_recovery_room_id == room.room_id:
                session.resolve_recoverable_failure(
                    room.room_id,
                    queue_retry=False,
                )
            session.mark_fallback_attempted(room.room_id)
            self._clear_blocked_session_watchdog()
            return
        if was_retry or was_priority_retry:
            previous_reason = session.failed_room_reasons.get(room.room_id)
            session.mark_retry_started(room.room_id)
            session.clear_room_issue(room.room_id)
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
        if cleaning_block_reason(resources):
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

        if success_override is None:
            if not self.config.get(CONF_STATUS_FLAG_ENTITY):
                run.observed_segment_cleaning = True
            success, reason = evaluate_run_success(
                room,
                run,
                parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
                parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
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
                    self._record_while_away_outcome(
                        "fallback",
                        room.room_id,
                        self.session.deferred_full_clean_reasons.get(
                            room.room_id,
                            self.session.degraded_reason,
                        ),
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
                    self._record_while_away_outcome("cleaned", room.room_id)
        else:
            mark_failure(ledger, utcnow_iso(), reason)
            if self.session:
                if run.fallback_vacuum:
                    self.session.mark_fallback_failed(room.room_id, reason)
                else:
                    self.session.mark_failed(room.room_id, reason)
                self._record_while_away_outcome("failed", room.room_id, reason)
                if wrong_room_failure:
                    self._set_needs_help_state(reason or "Wrong room observed")

        self._clear_active_run()

        await self._async_save_store()
        self._notify_listeners()
        if wrong_room_failure:
            await self._async_maybe_send_auto_clean_summary()
        elif continue_session:
            await self._async_maybe_start_next_room()
        elif send_summary:
            await self._async_maybe_send_auto_clean_summary()

    async def _async_handle_active_run_error(self, error: str) -> None:
        """Finalize an errored active room and decide whether it can recover after docking."""
        if is_low_battery_error(error):
            await self._async_handle_low_battery_active_run_error(error)
            return

        run = self.active_run
        if run and self._active_run_allows_error(error):
            await self._async_reconcile_active_run(dt_util.utcnow())
            return
        changed_allowed_error = bool(
            run
            and run.allowed_error_fingerprint
            and not is_error_clear(error)
        )
        room_id = run.room_id if run else None
        degraded_reason = clean_water_empty_reason(self._resource_state())
        if is_clean_water_empty_error(error) or degraded_reason:
            self._activate_clean_water_degraded(degraded_reason or error)
        if run and run.native_resume_pending:
            continue_session = bool(
                not run.resume_required
                and not changed_allowed_error
                and not self._error_needs_help(error)
            )
            if run.room_id:
                self._record_while_away_outcome(
                    "failed",
                    run.room_id,
                    error,
                )
            if self.session:
                self.session.native_resume_guard_latched = True
                if not continue_session:
                    self.session.active = False
                    self.session.terminal_reason = "needs_help"
                    self.session.terminal_message = error
                    self.session.needs_help = True
            run.cancelled = True
            run.phase = RUN_PHASE_CANCEL_PENDING
            run.cancel_requested_at = utcnow_iso()
            run.cancel_reason = error
            run.cancel_continue_session = continue_session
            self._cancel_active_run_timers()
            await self._async_save_store()
            self._notify_listeners()
            if await self._async_execute_cancel_pending():
                await self._async_save_store()
                self._notify_listeners()
                if continue_session:
                    await self._async_maybe_start_next_room()
                else:
                    await self._async_maybe_send_auto_clean_summary()
            return

        can_recover_after_dock = bool(
            self.session
            and room_id
            and not (run and run.fallback_vacuum)
            and is_recoverable_navigation_error(error)
            and self.session.can_retry_room(room_id)
        )
        if can_recover_after_dock and self.session and room_id:
            self.session.begin_recovering(room_id, error)
        needs_help = changed_allowed_error or self._error_needs_help(error)
        if needs_help:
            self._set_needs_help_state(error)

        await self._async_finish_active_run(
            success_override=False,
            failure_reason=error,
            continue_session=False,
            send_summary=not needs_help,
        )
        try:
            await self._async_return_to_dock_or_stop_resumable(error)
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("Failed to return %s after %s", self.vacuum_entity, error)
            await self._async_mark_needs_help(
                f"Could not return the vacuum after {error}: {err}"
            )
            return

        if needs_help:
            await self._async_maybe_send_auto_clean_summary()
            return
        if not can_recover_after_dock:
            await self._async_maybe_start_next_room()

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
            await self._async_terminate_active_run_needs_help(
                "Native resume is disabled; low-battery task was not restarted"
            )
            return

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
        if is_low_battery_error(reason):
            await self._async_mark_needs_help(
                "A legacy low-battery restart was pending; passive native resume "
                "cannot safely reissue that room"
            )
            return False
        elif (
            not self._vacuum_successfully_docked()
            or not self._configured_error_is_clear()
        ):
            return False

        if room_id not in self.room_by_id:
            self.session.pending_recovery_room_id = None
            self.session.pending_recovery_reason = None
            self.session.pending_recovery_priority = False
            await self._async_save_store()
            self._notify_listeners()
            return True

        should_retry = self.session.can_retry_room(room_id)
        self.session.resolve_recoverable_failure(room_id, queue_retry=should_retry)
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
                "Recovered %s after the final low-battery failure for room %s; "
                "continuing the queue",
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

        self._remove_while_away_failure(room_id, previous_reason)
        self.session.discard_retry(room_id)
        if self.session.pending_recovery_room_id == room_id:
            self.session.resolve_recoverable_failure(room_id, queue_retry=False)

        mark_failure(
            self.ledgers.setdefault(room_id, RoomLedger()),
            utcnow_iso(),
            error,
        )
        self.session.mark_failed(room_id, error)
        self._record_while_away_outcome("failed", room_id, error)

        if is_low_battery_error(error):
            await self._async_mark_needs_help(
                "A legacy low-battery restart was pending; passive native resume "
                "cannot safely reissue that room"
            )
            return

        if (
            is_recoverable_navigation_error(error)
            and self.session.can_retry_room(room_id)
        ):
            self.session.begin_recovering(room_id, error)

        try:
            await self._async_return_to_dock_or_stop_resumable(error)
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("Failed to return %s after %s", self.vacuum_entity, error)
            await self._async_mark_needs_help(
                f"Could not return the vacuum after {error}: {err}"
            )
            return

        if self._error_needs_help(error):
            await self._async_mark_needs_help(error)
            return

        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_start_next_room()

    async def _async_mark_needs_help(self, message: str) -> None:
        """Terminate the active session with a durable needs-help reason."""
        if not self.session:
            return
        self._set_needs_help_state(message)
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    def _set_needs_help_state(self, message: str) -> None:
        """Set terminal needs-help state without introducing an intermediate save."""
        if not self.session:
            return
        self._clear_blocked_session_watchdog()
        self.session.active = False
        self.session.pending_recovery_room_id = None
        self.session.pending_recovery_reason = None
        self.session.pending_recovery_priority = False
        self.session.retry_room_ids = []
        self.session.priority_retry_room_ids = []
        self.session.terminal_reason = "needs_help"
        self.session.terminal_message = message
        self.session.needs_help = True

    async def _async_reconcile_restored_session(self) -> None:
        """Resume or finalize a persisted auto-clean session after Home Assistant restarts."""
        if not self.session:
            return
        if (
            not self.active_run
            and (
                is_low_battery_error(self.session.pending_recovery_reason)
                or self._legacy_low_battery_retry_room_id()
            )
        ):
            await self._async_mark_needs_help(
                "A legacy low-battery restart was pending; v0.1.3 will not "
                "reissue a segment command"
            )
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
            self._configured_error_is_available()
            and not is_error_clear(self.error_state)
            and not self._active_run_allows_error(self.error_state)
        ):
            await self._async_handle_active_run_error(
                self.error_state or "Unknown error"
            )
            return True

        if not self.active_run.command_published:
            room = self.room_by_id.get(self.active_run.room_id or "")
            if self.active_run.observed_cleaning or self.active_run.observed_segment_cleaning:
                self.active_run.command_published = True
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
            and not self._active_run_allows_error(self.error_state)
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
            run.dispatch_deadline = None
            self._cancel_dispatch_start_timeout()
            changed = True

        status_flag = self._status_flag()
        if status_flag == "segment":
            if not run.observed_segment_cleaning:
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
            and not self._active_run_allows_error(self.error_state)
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
        if not self._vacuum_at_safe_terminal_point() and not self.session.needs_help:
            return
        guarded_cleanup = bool(
            self.session.native_resume_guard_latched
            or self.session.native_guard_cancel_pending
        )
        if not guarded_cleanup:
            if self._terminal_cleanup_retry_attempts >= _MAX_TERMINAL_CLEANUP_RETRIES:
                return
            self._terminal_cleanup_retry_attempts += 1

        notification_failed = False
        if not self.session.notification_sent:
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
            )
            if summary:
                try:
                    await self._async_send_notification(summary.title, summary.message)
                except Exception:  # noqa: BLE001
                    _LOGGER.exception("Could not send auto-clean summary for %s", self.name)
                    notification_failed = True
            if not notification_failed:
                self.session.notification_sent = True

        if guarded_cleanup:
            await self._async_save_store()
            self._notify_listeners()
            return

        restoration_failed = False
        try:
            restoration_failed = not await self._async_restore_auto_clean_settings()
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Could not restore auto-clean settings for %s", self.name)
            restoration_failed = True
        await self._async_save_store()
        self._notify_listeners()
        if notification_failed or restoration_failed:
            self._schedule_terminal_cleanup_retry()
        else:
            self._cancel_terminal_cleanup_retry()
            self._terminal_cleanup_retry_attempts = 0

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
        for reasons in (self.session.skipped_room_reasons, self.session.failed_room_reasons):
            for reason in reasons.values():
                return reason
        return None

    def _vacuum_at_safe_terminal_point(self) -> bool:
        """Return whether it is safe to clear auto-cleaning and notify."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        resources = self._resource_state()
        status_entity = self.config.get(CONF_STATUS_FLAG_ENTITY)
        status_flag = self._status_flag()
        status_safe = not status_entity or status_flag not in {
            None,
            "unknown",
            "unavailable",
            "resumable",
        }
        dock_entity = self.config.get(CONF_DOCK_STATUS_ENTITY)
        dock_status = normalize_state(self._state(dock_entity))
        dock_safe = not dock_entity or (
            dock_status is not None
            and dock_status.lower() in {"idle", "pause"}
        )
        resource_blocked = bool(
            self.session
            and (
                self.session.terminal_reason == "mop_resource_deferred"
                or self._error_is_mop_resource(self.error_state)
                or (
                    normalize_state(resources.dustbag) is not None
                    and normalize_state(resources.dustbag).lower()
                    in {"full", "missing", "unknown", "unavailable"}
                )
                or any(
                    mop_block_reason(room, resources)
                    for room in self._auto_clean_rooms()
                    if room.mop_required
                )
            )
        )
        if (
            self.session
            and self.session.terminal_reason in {
                "mop_resource_deferred",
                "blocked",
            }
            and not self.active_run
            and not self.session.native_resume_guard_latched
            and not self.session.native_guard_cancel_pending
            and (
                is_error_clear(self.error_state)
                or self._error_is_mop_resource(self.error_state)
            )
            and vacuum_state in {"error", "docked", "idle"}
            and status_safe
            and dock_safe
            and resource_blocked
        ):
            return True
        if not status_entity:
            return vacuum_state in _READY_VACUUM_STATES
        return (
            vacuum_state in _READY_VACUUM_STATES
            and status_flag not in {None, "unknown", "unavailable", "resumable"}
        )

    def _vacuum_successfully_docked(self) -> bool:
        """Return whether the robot reached the dock and is no longer resumable."""
        if not self.config.get(CONF_STATUS_FLAG_ENTITY):
            return normalize_state(self._state(self.vacuum_entity)) == "docked"
        status_flag = self._status_flag()
        return (
            normalize_state(self._state(self.vacuum_entity))
            == "docked"
            and status_flag not in {None, "unknown", "unavailable", "resumable"}
        )

    def _error_needs_help(self, error: str | None) -> bool:
        """Return whether an error should stop auto-clean and notify for help."""
        normalized = normalize_state(error)
        if not normalized or is_error_clear(normalized):
            return False
        if is_low_battery_error(normalized):
            return False
        lowered = normalized.lower()
        if "dock" in lowered and any(
            keyword in lowered for keyword in ("cannot reach", "cannot arrive", "cannot navigate")
        ):
            return True
        if self._error_is_mop_resource(normalized):
            return False
        if is_recoverable_navigation_error(normalized):
            return False
        return True

    def _error_is_mop_resource(self, error: str | None) -> bool:
        """Return whether an error is a recoverable mop resource issue."""
        return error_contains_any(error, RECOVERABLE_MOP_ERROR_KEYWORDS)

    def _start_manual_run(self, now: datetime) -> None:
        """Begin observing a manual segment run."""
        if self.manual_run:
            return
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
        self._notify_listeners()

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

    def _record_while_away_outcome(
        self,
        kind: str,
        room_id: str,
        reason: str | None = None,
    ) -> None:
        """Record one retained auto-clean outcome for dashboard display."""
        day = self._current_auto_clean_day()
        self._prune_while_away_outcomes_for_day(day)
        self.while_away_outcomes.append(
            WhileAwayOutcome(day=day, room_id=room_id, kind=kind, reason=reason)
        )
        self._schedule_next_day_timer_if_needed()

    def _remove_while_away_failure(self, room_id: str, reason: str | None) -> None:
        """Remove a failed retained outcome after a recoverable failure clears."""
        retained = [
            outcome
            for outcome in self.while_away_outcomes
            if not (
                outcome.kind == "failed"
                and outcome.room_id == room_id
                and (reason is None or outcome.reason == reason)
            )
        ]
        if len(retained) != len(self.while_away_outcomes):
            self.while_away_outcomes = retained
            if not retained:
                self._cancel_next_day_timer()

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
        *,
        command_may_be_pending: bool = False,
    ) -> None:
        """Cancel a moving or resumable Valetudo task."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if command_may_be_pending:
            await self.hass.services.async_call(
                "vacuum",
                "stop",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=True,
            )
            return
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

        if vacuum_state not in _AT_DOCK_VACUUM_STATES:
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
            blocking=False,
        )

    def _vacuum_ready_for_next_room(self) -> bool:
        """Return whether dispatching another segment is safe."""
        return self._vacuum_ready_for_selection(vacuum_only=False)

    def _vacuum_ready_for_selection(self, *, vacuum_only: bool) -> bool:
        """Return whether the current snapshot safely permits this mode."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        resources = self._resource_state()
        degraded_ready = bool(
            vacuum_only
            and self.session
            and self.session.active
            and self.session.degraded_reason
            and self.session.degraded_preparation_completed
            and clean_water_empty_reason(resources)
            and (
                is_error_clear(resources.error)
                or is_clean_water_empty_error(resources.error)
            )
            and vacuum_state in {"error", "docked", "idle"}
        )
        if vacuum_state not in _READY_VACUUM_STATES and not degraded_ready:
            return False
        if self.config.get(CONF_STATUS_FLAG_ENTITY):
            status_flag = self._status_flag()
            if status_flag in {None, "unknown", "unavailable", "resumable"}:
                return False

        if not self._configured_error_is_available():
            return False
        if cleaning_block_reason(resources):
            return False

        dock_status_entity = self.config.get(CONF_DOCK_STATUS_ENTITY)
        dock_status = normalize_state(self._state(dock_status_entity))
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

    def _legacy_low_battery_retry_room_id(self) -> str | None:
        """Return a v0.1.2 low-battery retry that must not be republished."""
        if not self.session:
            return None
        for room_id in self.session.priority_retry_room_ids:
            if is_low_battery_error(
                self.session.failed_room_reasons.get(room_id)
            ):
                return room_id
        return None

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

    async def _async_load_store(self) -> None:
        """Load persisted pause state and room ledgers."""
        stored = await self._store.async_load()
        if not isinstance(stored, dict):
            return

        self.paused = bool(stored.get("paused", False))
        self.pause_reason = stored.get("pause_reason")
        self.away_since = stored.get("away_since")
        self.session = SessionState.from_dict(stored.get("session"))
        self.active_run = ActiveRun.from_dict(stored.get("active_run"))
        self._active_run_restored = self.active_run is not None
        self.manual_run = ActiveRun.from_dict(stored.get("manual_run"))
        self.settings_snapshot = AutoCleanSettingsSnapshot.from_dict(stored.get("settings_snapshot"))
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

    async def _async_save_store(self) -> None:
        """Persist pause state and room ledgers."""
        await self._store.async_save(
            {
                "paused": self.paused,
                "pause_reason": self.pause_reason,
                "away_since": self.away_since,
                "session": self.session.to_dict() if self.session else None,
                "active_run": self.active_run.to_dict() if self.active_run else None,
                "manual_run": self.manual_run.to_dict() if self.manual_run else None,
                "settings_snapshot": self.settings_snapshot.to_dict() if self.settings_snapshot else None,
                "while_away_outcomes": [outcome.to_dict() for outcome in self.while_away_outcomes],
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
