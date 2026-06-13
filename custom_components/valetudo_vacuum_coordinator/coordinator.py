"""Coordinator for away-only Valetudo room cleaning."""

from __future__ import annotations

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
    CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL,
    CONF_CURRENT_AREA_ENTITY,
    CONF_CURRENT_TIME_ENTITY,
    CONF_DETERGENT_ENTITY,
    CONF_DIRTY_WATER_ENTITY,
    CONF_DOCK_STATUS_ENTITY,
    CONF_DUSTBAG_ENTITY,
    CONF_ERROR_ENTITY,
    CONF_ESTIMATED_SEGMENT_ENTITY,
    CONF_FAN_AUTO_CLEAN_OPTION,
    CONF_FAN_ENTITY,
    CONF_FRESH_WATER_ENTITY,
    CONF_MANUAL_TRACKING,
    CONF_MIN_BATTERY,
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
    DOMAIN,
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
    RECOVERABLE_MOP_ERROR_KEYWORDS,
    ResourceState,
    RoomConfig,
    RoomLedger,
    SessionState,
    WhileAwayOutcome,
    build_auto_clean_summary,
    build_while_away_messages,
    evaluate_run_success,
    error_contains_any,
    is_error_clear,
    manual_rooms_to_credit,
    mark_failure,
    mark_success,
    no_selection_terminal_reason,
    normalize_state,
    parse_datetime,
    parse_float,
    room_auto_cleaned_on,
    schedule_hass_task,
    select_next_room,
    utcnow_iso,
)

_LOGGER = logging.getLogger(__name__)

_READY_VACUUM_STATES = {"docked", "idle"}
_BUSY_DOCK_STATES = {"cleaning", "emptying", "pause"}
_UNKNOWN_OR_CLEAR_STATES = {None, "", "unknown", "unavailable", "none"}
_UNKNOWN_PERSON_STATES = {None, "", "unknown", "unavailable"}


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
        await self._async_reconcile_restored_session()
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
        if self.session.notification_sent:
            return False
        return bool(self.session.active or self.active_run or self.session.terminal_reason)

    @property
    def active_room(self) -> RoomConfig | None:
        """Return the currently commanded room, if any."""
        if not self.active_run or not self.active_run.room_id:
            return None
        return self.room_by_id.get(self.active_run.room_id)

    @property
    def pending_rooms(self) -> list[RoomConfig]:
        """Return rooms not yet consumed in the current session."""
        attempted = set(self.session.attempted_room_ids if self.session else [])
        auto_clean_day = self._current_auto_clean_day()
        return [
            room
            for room in self.rooms
            if room.enabled
            and room.room_id not in attempted
            and not room_auto_cleaned_on(
                self.ledgers.get(room.room_id, RoomLedger()), auto_clean_day
            )
        ]

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
        if self.paused:
            _LOGGER.info("Not starting %s because coordinator is paused", self.name)
            return
        if not self._all_people_away():
            _LOGGER.info("Not starting %s because not all tracked people are away", self.name)
            return
        if self.session and self.session.active:
            await self._async_maybe_start_next_room()
            return

        self.session = SessionState(session_id=utcnow_iso(), started_at=utcnow_iso())
        _LOGGER.info("Starting Valetudo away-cleaning session %s (%s)", self.session.session_id, reason)
        await self._async_prepare_auto_clean_settings()
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_start_next_room()

    async def async_cancel_session(self, reason: str) -> None:
        """Cancel the active away session and active run."""
        had_active_session = bool(self.session and self.session.active)
        had_active_run = self.active_run is not None

        if self.session:
            self.session.cancelled = True
            self.session.active = False
            self.session.active_room_id = None
            self.session.terminal_reason = (
                "returned_home" if reason == "Tracked person arrived home" else "cancelled"
            )
            self.session.terminal_message = reason

        if self.active_run:
            self.active_run.cancelled = True
            if self.active_run.room_id:
                ledger = self.ledgers.setdefault(self.active_run.room_id, RoomLedger())
                mark_failure(ledger, utcnow_iso(), reason)
                if self.session:
                    self.session.mark_failed(self.active_run.room_id, reason)
            self.active_run = None

        if had_active_session or had_active_run:
            await self._async_return_to_dock_or_stop_resumable(reason)
        await self._async_save_store()
        self._notify_listeners()
        await self._async_maybe_send_auto_clean_summary()

    async def async_set_paused(self, paused: bool, reason: str | None = None) -> None:
        """Pause or resume automatic cleaning behavior."""
        self.paused = paused
        self.pause_reason = reason if paused else None
        self._cancel_away_timer()
        if paused:
            await self.async_cancel_session(reason or "paused")
        else:
            if self._all_people_away() and self.away_since is None:
                self.away_since = self._latest_person_away_since()
            self._schedule_away_timer_if_needed()
        await self._async_save_store()
        self._notify_listeners()

    async def async_mark_room_cleaned(self, room_id: str, *, mop: bool, vacuum: bool = True) -> None:
        """Manually mark a room as cleaned."""
        self._require_room(room_id)
        ledger = self.ledgers.setdefault(room_id, RoomLedger())
        mark_success(ledger, utcnow_iso(), mop=mop, vacuum=vacuum)
        await self._async_save_store()
        self._notify_listeners()

    async def async_reset_room(self, room_id: str) -> None:
        """Reset one room ledger."""
        self._require_room(room_id)
        self.ledgers[room_id] = RoomLedger()
        await self._async_save_store()
        self._notify_listeners()

    @callback
    def _handle_state_change_event(self, event: Event) -> None:
        """Schedule handling for HA state changes."""
        self.hass.async_create_task(self._async_handle_state_change_event(event))

    async def _async_handle_state_change_event(self, event: Event) -> None:
        """Handle a monitored Home Assistant state change."""
        entity_id = event.data.get("entity_id")
        new_state: State | None = event.data.get("new_state")
        if new_state is None:
            return

        now = dt_util.utcnow()
        if entity_id in self.people_entities:
            await self._async_handle_presence_change()
            return

        self._observe_active_run(entity_id, new_state, now)
        self._observe_manual_run(entity_id, new_state, now)

        if entity_id == self.vacuum_entity:
            await self._async_handle_vacuum_state(new_state.state, now)
            return
        if entity_id == self.config.get(CONF_DOCK_STATUS_ENTITY):
            await self._async_maybe_start_next_room()
            return
        if entity_id == self.config.get(CONF_ERROR_ENTITY) and not is_error_clear(new_state.state):
            self.last_error = new_state.state
            if self.active_run:
                needs_help = self._error_needs_help(new_state.state)
                await self._async_finish_active_run(
                    success_override=False,
                    failure_reason=new_state.state,
                    continue_session=False,
                )
                await self._async_return_to_dock_or_stop_resumable(new_state.state)
                if needs_help and self.session:
                    self.session.active = False
                    self.session.terminal_reason = "needs_help"
                    self.session.terminal_message = new_state.state
                    self.session.needs_help = True
                    await self._async_save_store()
                    await self._async_maybe_send_auto_clean_summary()
                else:
                    await self._async_maybe_start_next_room()
            self._notify_listeners()
            return
        if entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY):
            await self._async_maybe_start_next_room()

    async def _async_handle_presence_change(self) -> None:
        """React to someone leaving or arriving."""
        if self._all_people_away():
            if self.away_since is None:
                self.away_since = self._latest_person_away_since()
                await self._async_save_store()
            self._schedule_away_timer_if_needed()
            return

        self.away_since = None
        self._cancel_away_timer()
        if self.session and self.session.active and self.config.get(CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL):
            await self.async_cancel_session("Tracked person arrived home")
        else:
            await self._async_save_store()

    def _schedule_away_timer_if_needed(self) -> None:
        """Schedule the configured away grace period."""
        if self.paused or not self._all_people_away():
            return
        if self.session and self.session.active:
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

        if normalized_state == "cleaning":
            if self.active_run:
                self.active_run.observed_cleaning = True
            else:
                cleared = self._clear_while_away_after_manual_clean_started()
                if self._manual_tracking_allowed():
                    self._start_manual_run(now)
                elif cleared:
                    self._notify_listeners()
                if cleared:
                    await self._async_save_store()

        if normalized_state == "returning" and self.active_run:
            self.active_run.observed_cleaning = True

        if normalized_state in {"docked", "idle"}:
            if self.active_run and self._status_flag() != "resumable":
                await self._async_finish_active_run()
            elif self.manual_run:
                await self._async_finish_manual_run(now)
            else:
                await self._async_maybe_send_auto_clean_summary()
                await self._async_maybe_start_next_room()

    def _observe_active_run(self, entity_id: str, new_state: State, now: datetime) -> None:
        """Update active commanded run observations."""
        if not self.active_run:
            return
        if entity_id == self.vacuum_entity and new_state.state == "cleaning":
            self.active_run.observed_cleaning = True
        elif entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY) and new_state.state == "segment":
            self.active_run.observed_segment_cleaning = True
        elif entity_id == self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY):
            self.active_run.observe_estimated_room(self._room_id_from_estimated(new_state.state), now)

    def _observe_manual_run(self, entity_id: str, new_state: State, now: datetime) -> None:
        """Update active manual run observations."""
        if not self.manual_run:
            return
        if entity_id == self.vacuum_entity and new_state.state == "cleaning":
            self.manual_run.observed_cleaning = True
        elif entity_id == self.config.get(CONF_STATUS_FLAG_ENTITY) and new_state.state == "segment":
            self.manual_run.observed_segment_cleaning = True
        elif entity_id == self.config.get(CONF_ESTIMATED_SEGMENT_ENTITY):
            self.manual_run.observe_estimated_room(self._room_id_from_estimated(new_state.state), now)

    async def _async_maybe_start_next_room(self) -> None:
        """Start the next room when the session and Valetudo state allow it."""
        if self.paused or not self.session or not self.session.active or self.active_run:
            self._notify_listeners()
            return
        if not self._all_people_away():
            await self.async_cancel_session("Tracked person arrived home")
            return
        if not self._vacuum_ready_for_next_room():
            self._notify_listeners()
            return

        selection, skipped = select_next_room(
            self.rooms,
            self.ledgers,
            set(self.session.attempted_room_ids),
            self._resource_state(),
            bool(self.config.get(CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED)),
            self._current_auto_clean_day(),
        )

        for room, reason in skipped:
            ledger = self.ledgers.setdefault(room.room_id, RoomLedger())
            mark_failure(ledger, utcnow_iso(), f"Skipped: {reason}")
            self.session.mark_skipped(room.room_id, reason)
            self._record_while_away_outcome("skipped", room.room_id, reason)

        if selection is None:
            self.session.active = False
            self.session.active_room_id = None
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

        await self._async_start_room(selection.room, vacuum_only=selection.vacuum_only)

    async def _async_start_room(self, room: RoomConfig, *, vacuum_only: bool) -> None:
        """Command Valetudo to clean one segment."""
        if not self.session:
            return

        self.session.mark_attempted(room.room_id)
        self.session.active_room_id = room.room_id
        self.active_run = ActiveRun(
            room_id=room.room_id,
            segment_id=room.segment_id,
            session_id=self.session.session_id,
            started_at=utcnow_iso(),
            start_area=parse_float(self._state(self.config.get(CONF_CURRENT_AREA_ENTITY))),
            start_time=parse_float(self._state(self.config.get(CONF_CURRENT_TIME_ENTITY))),
            vacuum_only=vacuum_only,
        )
        if self._status_flag() == "segment":
            self.active_run.observed_segment_cleaning = True

        await self._async_save_store()

        try:
            await self._async_apply_mode(vacuum_only=vacuum_only)
            await self.hass.services.async_call(
                "mqtt",
                "publish",
                {
                    "topic": self.segment_command_topic,
                    "payload": json.dumps(
                        {
                            "segment_ids": [room.segment_id],
                            "iterations": int(self.config.get(CONF_AUTO_CLEAN_ITERATIONS, 2)),
                            "customOrder": True,
                        }
                    ),
                },
                blocking=True,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("Failed to start Valetudo segment clean for %s", room.name)
            mark_failure(self.ledgers.setdefault(room.room_id, RoomLedger()), utcnow_iso(), str(err))
            self.session.mark_failed(room.room_id, str(err))
            self._record_while_away_outcome("failed", room.room_id, str(err))
            self.active_run = None
            self.session.active_room_id = None

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
    ) -> None:
        """Finalize the active commanded run."""
        run = self.active_run
        if not run or not run.room_id:
            return

        room = self.room_by_id[run.room_id]
        run.finalize_estimated_room(dt_util.utcnow())
        ledger = self.ledgers.setdefault(room.room_id, RoomLedger())

        if success_override is None:
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

        if success:
            mark_success(
                ledger,
                utcnow_iso(),
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
                self.session.mark_failed(room.room_id, reason)
                self._record_while_away_outcome("failed", room.room_id, reason)

        self.active_run = None
        if self.session:
            self.session.active_room_id = None

        await self._async_save_store()
        self._notify_listeners()
        if continue_session:
            await self._async_maybe_start_next_room()
        else:
            await self._async_maybe_send_auto_clean_summary()

    async def _async_reconcile_restored_session(self) -> None:
        """Resume or finalize a persisted auto-clean session after Home Assistant restarts."""
        if not self.session:
            return
        if self.active_run and self._status_flag() != "resumable":
            vacuum_state = normalize_state(self._state(self.vacuum_entity))
            if vacuum_state in _READY_VACUUM_STATES:
                await self._async_finish_active_run()
                return
        if self.session.active and not self.active_run:
            await self._async_maybe_start_next_room()
            return
        await self._async_maybe_send_auto_clean_summary()

    async def _async_maybe_send_auto_clean_summary(self) -> None:
        """Send the one final auto-clean notification when the session is terminal."""
        if not self.session or self.session.active or self.active_run or self.session.notification_sent:
            return
        if not self.session.terminal_reason:
            return
        if not self._vacuum_at_safe_terminal_point() and not self.session.needs_help:
            return

        summary = build_auto_clean_summary(
            vacuum_name=self.name.replace(" Coordinator", ""),
            completed_room_names=[self.room_by_id[room_id].name for room_id in self.session.completed_room_ids],
            skipped_room_reasons=self._named_reasons(self.session.skipped_room_reasons),
            failed_room_reasons=self._named_reasons(self.session.failed_room_reasons),
            terminal_reason=self.session.terminal_reason,
            terminal_message=self.session.terminal_message,
            needs_help=self.session.needs_help,
            all_rooms_cleaned=self._all_enabled_rooms_completed(),
            total_room_count=len([room for room in self.rooms if room.enabled]),
        )
        self.session.notification_sent = True
        if summary:
            await self._async_send_notification(summary.title, summary.message)
        await self._async_restore_auto_clean_settings()
        await self._async_save_store()
        self._notify_listeners()

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

    async def _async_restore_auto_clean_settings(self) -> None:
        """Restore user settings captured before the auto-clean session."""
        if self.settings_snapshot is None:
            return
        snapshot = self.settings_snapshot
        self.settings_snapshot = None
        await self._async_select_option(self.config.get(CONF_MODE_ENTITY), snapshot.mode)
        await self._async_select_option(self.config.get(CONF_FAN_ENTITY), snapshot.fan)
        await self._async_select_option(self.config.get(CONF_WATER_ENTITY), snapshot.water)
        await self._async_select_option(self.config.get(CONF_PASSES_ENTITY), snapshot.passes)

    async def _async_select_option(self, entity_id: str | None, option: str | None) -> None:
        """Select an option on a select or input_select entity when available."""
        if not entity_id or option in _UNKNOWN_OR_CLEAR_STATES:
            return
        domain = entity_id.split(".", 1)[0]
        if domain not in {"select", "input_select"}:
            return
        try:
            await self.hass.services.async_call(
                domain,
                "select_option",
                {ATTR_ENTITY_ID: entity_id, "option": option},
                blocking=True,
            )
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Could not select %s on %s", option, entity_id, exc_info=True)

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
        await self.hass.services.async_call(domain, service, data, blocking=False)

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
        return all(room.room_id in completed for room in self.rooms if room.enabled)

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
        return vacuum_state in _READY_VACUUM_STATES and self._status_flag() != "resumable"

    def _error_needs_help(self, error: str | None) -> bool:
        """Return whether an error should stop auto-clean and notify for help."""
        normalized = normalize_state(error)
        if not normalized or is_error_clear(normalized):
            return False
        lowered = normalized.lower()
        if "low battery" in lowered:
            return False
        if "dock" in lowered and any(
            keyword in lowered for keyword in ("cannot reach", "cannot arrive", "cannot navigate")
        ):
            return True
        if self._error_is_mop_resource(normalized):
            return False
        return not any(
            keyword in lowered
            for keyword in ("cannot reach", "cannot arrive", "cannot navigate")
        )

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
        if self.session and not self.session.active and not self.active_run:
            self.session = None

    def _clear_while_away_after_manual_clean_started(self) -> bool:
        """Clear retained away outcome details when a manual clean starts."""
        changed = False
        if self.while_away_outcomes:
            self.while_away_outcomes = []
            self._cancel_next_day_timer()
            changed = True
        if self.session and not self.session.active and not self.active_run:
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
            schedule_hass_task(self.hass, self._async_handle_next_day_rollover())

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

    async def _async_return_to_dock_or_stop_resumable(self, reason: str) -> None:
        """Cancel a moving or resumable Valetudo task."""
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if vacuum_state == "error" and self._error_is_mop_resource(reason):
            _LOGGER.info("Stopping %s after dock resource error: %s", self.vacuum_entity, reason)
            await self.hass.services.async_call(
                "vacuum",
                "stop",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=False,
            )
            return

        if vacuum_state == "docked" and self._status_flag() == "resumable":
            await self.hass.services.async_call(
                "vacuum",
                "stop",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=False,
            )
            return

        if vacuum_state not in {"docked", "idle"}:
            _LOGGER.info("Returning %s to dock because %s", self.vacuum_entity, reason)
            await self.hass.services.async_call(
                "vacuum",
                "return_to_base",
                {ATTR_ENTITY_ID: self.vacuum_entity},
                blocking=False,
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
        vacuum_state = normalize_state(self._state(self.vacuum_entity))
        if vacuum_state not in _READY_VACUUM_STATES:
            return False
        if self._status_flag() == "resumable":
            return False

        dock_status = normalize_state(self._state(self.config.get(CONF_DOCK_STATUS_ENTITY)))
        if dock_status and dock_status.lower() in _BUSY_DOCK_STATES:
            return False

        battery = parse_float(self._state(self.config.get(CONF_BATTERY_ENTITY)))
        if battery is not None and battery < float(self.config.get(CONF_MIN_BATTERY)):
            return False
        return True

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
