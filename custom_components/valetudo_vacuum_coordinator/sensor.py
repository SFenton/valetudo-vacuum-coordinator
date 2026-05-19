"""Sensors for Valetudo Vacuum Coordinator."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    ATTR_ACTIVE_ROOM,
    ATTR_CANCELLED,
    ATTR_COMPLETED_ROOMS,
    ATTR_LAST_FAILED_REASON,
    ATTR_LAST_MOPPED,
    ATTR_LAST_SUCCESSFUL_CLEAN,
    ATTR_LAST_VACUUMED,
    ATTR_PENDING_ROOMS,
    ATTR_ROOM_ID,
    ATTR_SESSION_ID,
    ATTR_SKIPPED_ROOMS,
    ATTR_SUCCESSFUL_COUNT,
    ATTR_VACUUM_ONLY,
)
from .entity import ValetudoCoordinatorEntity, get_coordinator_from_discovery


async def async_setup_platform(
    hass: HomeAssistant,
    config: dict[str, Any],
    async_add_entities: AddEntitiesCallback,
    discovery_info: dict[str, Any] | None = None,
) -> None:
    """Set up sensors from YAML discovery."""
    coordinator = get_coordinator_from_discovery(hass.data, discovery_info)
    entities: list[SensorEntity] = [
        ValetudoSessionStateSensor(coordinator),
        ValetudoCurrentRoomSensor(coordinator),
        ValetudoQueueSensor(coordinator),
    ]
    entities.extend(ValetudoRoomLedgerSensor(coordinator, room.room_id) for room in coordinator.rooms)
    async_add_entities(entities)


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
        return {ATTR_PENDING_ROOMS: [room.room_id for room in self.coordinator.pending_rooms]}


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
        return {
            ATTR_ROOM_ID: self.room_id,
            "room_name": room.name,
            "segment_id": room.segment_id,
            "mop_required": room.mop_required,
            ATTR_LAST_SUCCESSFUL_CLEAN: ledger.last_successful_clean,
            ATTR_LAST_VACUUMED: ledger.last_vacuumed,
            ATTR_LAST_MOPPED: ledger.last_mopped,
            ATTR_LAST_FAILED_REASON: ledger.last_failed_reason,
            ATTR_SUCCESSFUL_COUNT: ledger.successful_count,
        }
