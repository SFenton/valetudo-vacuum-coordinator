"""Sensors for Valetudo Vacuum Coordinator."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    ATTR_ACTIVE_ROOM,
    ATTR_BLOCKED_REASON,
    ATTR_BLOCKER_CODE,
    ATTR_BLOCKER_DISPOSITION,
    ATTR_BLOCKER_OPERATOR_ACTION,
    ATTR_CANCELLED,
    ATTR_COMPLETED_ROOMS,
    ATTR_DEFERRED_FULL_CLEAN_REASONS,
    ATTR_DEFERRED_FULL_CLEAN_ROOMS,
    ATTR_DEGRADED_AT,
    ATTR_DEGRADED_REASON,
    ATTR_FALLBACK_ATTEMPTED_ROOMS,
    ATTR_FALLBACK_COMPLETED_ROOMS,
    ATTR_FALLBACK_FAILED_REASONS,
    ATTR_FALLBACK_FAILED_ROOMS,
    ATTR_FALLBACK_VACUUM,
    ATTR_FAILED_REASONS,
    ATTR_FAILED_ROOMS,
    ATTR_LAST_FAILED_REASON,
    ATTR_LAST_MOPPED,
    ATTR_LAST_FALLBACK_VACUUMED,
    ATTR_LAST_SUCCESSFUL_CLEAN,
    ATTR_LAST_VACUUMED,
    ATTR_MOP_DEFERRED,
    ATTR_MOP_DEFERRED_REASON,
    ATTR_NAVIGATION_ERROR_RETURN_ENABLED,
    ATTR_NEXT_CANDIDATE_ROOM,
    ATTR_PENDING_ROOMS,
    ATTR_PENDING_RECOVERY_REASON,
    ATTR_PENDING_RECOVERY_ROOM,
    ATTR_PENDING_RECOVERY_POLICY,
    ATTR_PHASE,
    ATTR_SUSPENDED_AT,
    ATTR_SUSPEND_REASON,
    ATTR_RESUME_SOURCE,
    ATTR_INTERRUPTION_COUNT,
    ATTR_NATIVE_RESUME_OBSERVED,
    ATTR_RESUMABLE_LATCHED,
    ATTR_RECOVERY_DEADLINE,
    ATTR_REQUESTED_ITERATIONS,
    ATTR_RAW_ERROR,
    ATTR_ROOM_ID,
    ATTR_SESSION_ID,
    ATTR_SKIPPED_REASONS,
    ATTR_SKIPPED_ROOMS,
    ATTR_NEEDS_HELP,
    ATTR_NEXT_RETRY_AT,
    ATTR_NOTIFICATION_SENT,
    ATTR_PRESERVED_ROOMS,
    ATTR_RECOVERY_PHASE,
    ATTR_RECOVERY_STARTED_AT,
    ATTR_RETRIED_ROOMS,
    ATTR_RETRY_CADENCE_REASON,
    ATTR_RETRY_ROOMS,
    ATTR_SUCCESSFUL_COUNT,
    ATTR_TERMINAL_MESSAGE,
    ATTR_TERMINAL_REASON,
    ATTR_TERMINAL_CAUSE,
    ATTR_UNCERTAIN_REASONS,
    ATTR_UNCERTAIN_ROOMS,
    ATTR_VACUUM_ONLY,
    ATTR_WAITING_FOR_PHYSICAL_FIX,
    ATTR_WHILE_AWAY_CLEANED,
    ATTR_WHILE_AWAY_ISSUES,
    ATTR_WHILE_AWAY_OUTCOMES,
    DOMAIN,
)
from .entity import ValetudoCoordinatorEntity, get_coordinator_from_discovery
from .status import ValetudoVacuumStatusObserver, get_status_observer


async def async_setup_platform(
    hass: HomeAssistant,
    config: dict[str, Any],
    async_add_entities: AddEntitiesCallback,
    discovery_info: dict[str, Any] | None = None,
) -> None:
    """Set up sensors from YAML discovery."""
    if discovery_info and "status_observer_id" in discovery_info:
        observer = get_status_observer(
            hass.data[DOMAIN],
            str(discovery_info["status_observer_id"]),
        )
        async_add_entities([ValetudoVacuumStatusSensor(observer)])
        return

    coordinator = get_coordinator_from_discovery(hass.data, discovery_info)
    entities: list[SensorEntity] = [
        ValetudoSessionStateSensor(coordinator),
        ValetudoCurrentRoomSensor(coordinator),
        ValetudoQueueSensor(coordinator),
    ]
    entities.extend(ValetudoRoomLedgerSensor(coordinator, room.room_id) for room in coordinator.rooms)
    async_add_entities(entities)


class ValetudoVacuumStatusSensor(SensorEntity):
    """Read-only versioned vacuum status contract."""

    _attr_has_entity_name = False
    _attr_icon = "mdi:robot-vacuum"

    def __init__(self, observer: ValetudoVacuumStatusObserver) -> None:
        """Initialize a status observer sensor."""
        self.observer = observer
        self._attr_name = f"{observer.name} Status"
        self._attr_unique_id = f"{observer.observer_id}_vacuum_status"
        self._attr_suggested_object_id = f"{observer.observer_id}_vacuum_status"

    @property
    def native_value(self) -> str:
        """Return the observer's compact primary value."""
        return self.observer.native_value

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return the complete status contract."""
        return self.observer.contract

    @property
    def device_info(self) -> DeviceInfo:
        """Return a distinct status-observer device."""
        return DeviceInfo(
            identifiers={(DOMAIN, f"status_observer_{self.observer.observer_id}")},
            name=self.observer.name,
            manufacturer="Valetudo",
            model="Vacuum Status Observer",
        )

    async def async_added_to_hass(self) -> None:
        """Subscribe entity state writes to observer changes."""
        self.async_on_remove(self.observer.async_add_listener(self._handle_update))

    @callback
    def _handle_update(self) -> None:
        """Write an updated contract state."""
        self.async_write_ha_state()


class ValetudoSessionStateSensor(ValetudoCoordinatorEntity, SensorEntity):
    """High-level coordinator state sensor."""

    _attr_icon = "mdi:robot-vacuum"

    def __init__(self, coordinator) -> None:
        """Initialize the session state sensor."""
        super().__init__(coordinator, "session_state", "Session State")

    @property
    def native_value(self) -> str:
        """Return coordinator state."""
        return self.coordinator.state

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return session details."""
        session = self.coordinator.session
        return {
            ATTR_SESSION_ID: session.session_id if session else None,
            ATTR_CANCELLED: session.cancelled if session else False,
            ATTR_ACTIVE_ROOM: self.coordinator.active_room.name if self.coordinator.active_room else None,
            ATTR_COMPLETED_ROOMS: session.completed_room_ids if session else [],
            ATTR_SKIPPED_ROOMS: session.skipped_room_ids if session else [],
            ATTR_FAILED_ROOMS: session.failed_room_ids if session else [],
            ATTR_SKIPPED_REASONS: session.skipped_room_reasons if session else {},
            ATTR_FAILED_REASONS: session.failed_room_reasons if session else {},
            ATTR_UNCERTAIN_ROOMS: session.uncertain_room_ids if session else [],
            ATTR_UNCERTAIN_REASONS: (
                session.uncertain_room_reasons if session else {}
            ),
            ATTR_FALLBACK_ATTEMPTED_ROOMS: (
                session.fallback_attempted_room_ids if session else []
            ),
            ATTR_FALLBACK_COMPLETED_ROOMS: (
                session.fallback_completed_room_ids if session else []
            ),
            ATTR_FALLBACK_FAILED_ROOMS: (
                session.fallback_failed_room_ids if session else []
            ),
            ATTR_FALLBACK_FAILED_REASONS: (
                session.fallback_failed_room_reasons if session else {}
            ),
            ATTR_DEFERRED_FULL_CLEAN_ROOMS: (
                session.deferred_full_clean_room_ids if session else []
            ),
            ATTR_DEFERRED_FULL_CLEAN_REASONS: (
                session.deferred_full_clean_reasons if session else {}
            ),
            ATTR_DEGRADED_REASON: session.degraded_reason if session else None,
            ATTR_DEGRADED_AT: session.degraded_at if session else None,
            ATTR_BLOCKED_REASON: session.blocked_reason if session else None,
            ATTR_BLOCKER_CODE: session.blocker_code if session else None,
            ATTR_BLOCKER_DISPOSITION: (
                session.blocker_disposition if session else None
            ),
            ATTR_BLOCKER_OPERATOR_ACTION: (
                session.blocker_operator_action if session else None
            ),
            ATTR_RECOVERY_PHASE: session.recovery_phase if session else None,
            ATTR_RECOVERY_STARTED_AT: (
                session.recovery_started_at if session else None
            ),
            ATTR_NEXT_RETRY_AT: session.next_retry_at if session else None,
            ATTR_RETRY_CADENCE_REASON: (
                session.retry_cadence_reason if session else None
            ),
            ATTR_WAITING_FOR_PHYSICAL_FIX: (
                session.waiting_for_physical_fix if session else False
            ),
            ATTR_PENDING_RECOVERY_ROOM: (
                session.pending_recovery_room_id if session else None
            ),
            ATTR_PENDING_RECOVERY_REASON: (
                session.pending_recovery_reason if session else None
            ),
            ATTR_PENDING_RECOVERY_POLICY: (
                session.pending_recovery_policy if session else None
            ),
            ATTR_RETRY_ROOMS: session.retry_room_ids if session else [],
            ATTR_RETRIED_ROOMS: session.retried_room_ids if session else [],
            ATTR_NEXT_CANDIDATE_ROOM: self.coordinator.next_candidate_room_id,
            ATTR_NAVIGATION_ERROR_RETURN_ENABLED: (
                self.coordinator.navigation_error_return_enabled
            ),
            "recovery_notification_attempts": (
                session.recovery_notification_attempts if session else 0
            ),
            "recovery_notification_sent": (
                session.recovery_notification_sent if session else False
            ),
            "last_recovery_notification_at": (
                session.last_recovery_notification_at if session else None
            ),
            ATTR_PRESERVED_ROOMS: self.coordinator.preserved_room_ids,
            ATTR_RAW_ERROR: self.coordinator.error_state,
            ATTR_TERMINAL_REASON: session.terminal_reason if session else None,
            ATTR_TERMINAL_MESSAGE: session.terminal_message if session else None,
            ATTR_TERMINAL_CAUSE: session.terminal_cause if session else None,
            ATTR_NEEDS_HELP: session.needs_help if session else False,
            ATTR_NOTIFICATION_SENT: session.notification_sent if session else False,
            "preflight_complete": (
                session.preflight_complete if session else False
            ),
            "settings_prepared": (
                session.settings_prepared if session else False
            ),
            "degraded_dock_stop_attempts": (
                session.degraded_dock_stop_attempts if session else 0
            ),
            "degraded_dock_stop_requested_at": (
                session.degraded_dock_stop_requested_at if session else None
            ),
            "degraded_dock_stop_acknowledged_at": (
                session.degraded_dock_stop_acknowledged_at
                if session
                else None
            ),
            "degraded_mode_attempts": (
                session.degraded_mode_attempts if session else 0
            ),
            "degraded_mode_acknowledged_at": (
                session.degraded_mode_acknowledged_at if session else None
            ),
            "degraded_mode_next_retry_at": (
                session.degraded_mode_next_retry_at if session else None
            ),
            "degraded_mode_last_error": (
                session.degraded_mode_last_error if session else None
            ),
            "dispatch_failure_counts": (
                session.dispatch_failure_counts if session else {}
            ),
            "dispatch_failure_reasons": (
                session.dispatch_failure_reasons if session else {}
            ),
            "dispatch_retry_not_before": (
                session.dispatch_retry_not_before if session else {}
            ),
            "dispatch_escalated_rooms": (
                session.dispatch_escalated_room_ids if session else []
            ),
            "last_command_recovery": (
                session.last_command_recovery if session else {}
            ),
            ATTR_WHILE_AWAY_CLEANED: self.coordinator.while_away_cleaned_messages,
            ATTR_WHILE_AWAY_ISSUES: self.coordinator.while_away_issue_messages,
            ATTR_WHILE_AWAY_OUTCOMES: self.coordinator.while_away_outcome_contract,
            **self.coordinator.native_resume_attributes,
        }


class ValetudoCurrentRoomSensor(ValetudoCoordinatorEntity, SensorEntity):
    """Current active room sensor."""

    _attr_icon = "mdi:floor-plan"

    def __init__(self, coordinator) -> None:
        """Initialize the current room sensor."""
        super().__init__(coordinator, "current_room", "Current Room")

    @property
    def native_value(self) -> str | None:
        """Return active room name."""
        room = self.coordinator.active_room
        return room.name if room else None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return current room details."""
        run = self.coordinator.active_run
        return {
            ATTR_ROOM_ID: run.room_id if run else None,
            "segment_id": run.segment_id if run else None,
            ATTR_VACUUM_ONLY: run.vacuum_only if run else False,
            ATTR_FALLBACK_VACUUM: run.fallback_vacuum if run else False,
            ATTR_PHASE: run.phase if run else None,
            ATTR_SUSPENDED_AT: run.suspended_at if run else None,
            ATTR_SUSPEND_REASON: run.suspend_reason if run else None,
            ATTR_RESUME_SOURCE: run.resume_source if run else None,
            ATTR_INTERRUPTION_COUNT: run.interruption_count if run else 0,
            ATTR_NATIVE_RESUME_OBSERVED: (
                run.resumed_after_suspend if run else False
            ),
            ATTR_RESUMABLE_LATCHED: run.resumable_latched if run else False,
            ATTR_RECOVERY_DEADLINE: run.recovery_deadline if run else None,
            ATTR_REQUESTED_ITERATIONS: (
                run.requested_iterations if run else None
            ),
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
            "cancel_ack_deadline": run.cancel_ack_deadline if run else None,
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
            "cancel_recover_room": run.cancel_recover_room if run else False,
            "floor_completion_status": (
                run.floor_completion_status if run else None
            ),
            "floor_completion_reason": (
                run.floor_completion_reason if run else None
            ),
        }


class ValetudoQueueSensor(ValetudoCoordinatorEntity, SensorEntity):
    """Pending room count sensor."""

    _attr_icon = "mdi:format-list-checks"

    def __init__(self, coordinator) -> None:
        """Initialize the queue sensor."""
        super().__init__(coordinator, "queue", "Queue")

    @property
    def native_value(self) -> int:
        """Return pending room count."""
        return len(self.coordinator.pending_rooms)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return pending rooms."""
        session = self.coordinator.session
        return {
            ATTR_PENDING_ROOMS: [
                room.room_id for room in self.coordinator.pending_rooms
            ],
            ATTR_PENDING_RECOVERY_ROOM: (
                session.pending_recovery_room_id if session else None
            ),
            ATTR_PENDING_RECOVERY_REASON: (
                session.pending_recovery_reason if session else None
            ),
            ATTR_PENDING_RECOVERY_POLICY: (
                session.pending_recovery_policy if session else None
            ),
            ATTR_RETRY_ROOMS: session.retry_room_ids if session else [],
            ATTR_RETRIED_ROOMS: session.retried_room_ids if session else [],
            ATTR_NEXT_CANDIDATE_ROOM: self.coordinator.next_candidate_room_id,
            ATTR_PRESERVED_ROOMS: self.coordinator.preserved_room_ids,
            ATTR_DEFERRED_FULL_CLEAN_ROOMS: (
                session.deferred_full_clean_room_ids if session else []
            ),
            ATTR_DEFERRED_FULL_CLEAN_REASONS: (
                session.deferred_full_clean_reasons if session else {}
            ),
            ATTR_FALLBACK_COMPLETED_ROOMS: (
                session.fallback_completed_room_ids if session else []
            ),
            ATTR_FALLBACK_FAILED_ROOMS: (
                session.fallback_failed_room_ids if session else []
            ),
            ATTR_FALLBACK_FAILED_REASONS: (
                session.fallback_failed_room_reasons if session else {}
            ),
        }


class ValetudoRoomLedgerSensor(ValetudoCoordinatorEntity, SensorEntity):
    """Per-room last successful clean sensor."""

    _attr_icon = "mdi:broom"

    def __init__(self, coordinator, room_id: str) -> None:
        """Initialize the room ledger sensor."""
        self.room_id = room_id
        room = coordinator.room_by_id[room_id]
        super().__init__(coordinator, f"room_{room_id}", f"{room.name} Last Cleaned")

    @property
    def native_value(self) -> str | None:
        """Return the last successful clean timestamp."""
        return self.coordinator.ledgers[self.room_id].last_successful_clean

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return detailed room ledger state."""
        ledger = self.coordinator.ledgers[self.room_id]
        room = self.coordinator.room_by_id[self.room_id]
        session = self.coordinator.session
        return {
            ATTR_ROOM_ID: self.room_id,
            "room_name": room.name,
            "segment_id": room.segment_id,
            "mop_required": room.mop_required,
            ATTR_LAST_SUCCESSFUL_CLEAN: ledger.last_successful_clean,
            ATTR_LAST_VACUUMED: ledger.last_vacuumed,
            ATTR_LAST_FALLBACK_VACUUMED: ledger.last_fallback_vacuumed,
            ATTR_LAST_MOPPED: ledger.last_mopped,
            ATTR_LAST_FAILED_REASON: ledger.last_failed_reason,
            ATTR_SUCCESSFUL_COUNT: ledger.successful_count,
            ATTR_MOP_DEFERRED: bool(
                session and self.room_id in session.deferred_full_clean_room_ids
            ),
            ATTR_MOP_DEFERRED_REASON: (
                session.deferred_full_clean_reasons.get(self.room_id)
                if session
                else None
            ),
        }
