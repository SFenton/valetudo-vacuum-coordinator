"""Switches for Valetudo Vacuum Coordinator."""

from __future__ import annotations

import re
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .entity import ValetudoCoordinatorEntity, get_coordinator_from_discovery


async def async_setup_platform(
    hass: HomeAssistant,
    config: dict[str, Any],
    async_add_entities: AddEntitiesCallback,
    discovery_info: dict[str, Any] | None = None,
) -> None:
    """Set up switches from YAML discovery."""
    coordinator = get_coordinator_from_discovery(hass.data, discovery_info)
    async_add_entities([
        ValetudoPauseSwitch(coordinator),
        *(
            ValetudoRoomAutoCleanDisabledSwitch(coordinator, room.room_id)
            for room in coordinator.rooms
        ),
    ])


class ValetudoPauseSwitch(ValetudoCoordinatorEntity, SwitchEntity):
    """Pause switch for away-cleaning behavior."""

    _attr_icon = "mdi:pause-circle"

    def __init__(self, coordinator) -> None:
        """Initialize the pause switch."""
        super().__init__(coordinator, "pause_switch", "Pause")

    @property
    def is_on(self) -> bool:
        """Return whether the coordinator is paused."""
        return self.coordinator.paused

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Pause automatic behavior."""
        await self.coordinator.async_set_paused(True, "pause switch")

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Resume automatic behavior."""
        await self.coordinator.async_set_paused(False, "pause switch")


class ValetudoRoomAutoCleanDisabledSwitch(ValetudoCoordinatorEntity, SwitchEntity):
    """Per-room switch that excludes a room from away auto-clean sessions."""

    _attr_icon = "mdi:robot-vacuum-off"

    def __init__(self, coordinator, room_id: str) -> None:
        """Initialize the room auto-clean disabled switch."""
        self.room_id = room_id
        room = coordinator.room_by_id[room_id]
        super().__init__(
            coordinator,
            f"room_{_slugify(room_id)}_auto_clean_disabled",
            f"{room.name} Auto-Clean Disabled",
        )
        self._attr_suggested_object_id = (
            f"{coordinator.coordinator_id}_{_slugify(room_id)}_auto_clean_disabled"
        )

    @property
    def is_on(self) -> bool:
        """Return whether this room is disabled for away auto-clean sessions."""
        return self.coordinator.is_room_auto_clean_disabled(self.room_id)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return room details for dashboards."""
        room = self.coordinator.room_by_id[self.room_id]
        return {
            "room_id": room.room_id,
            "room_name": room.name,
            "segment_id": room.segment_id,
            "auto_clean_enabled": not self.is_on and room.enabled,
            "yaml_enabled": room.enabled,
        }

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Disable this room for automatic away cleaning."""
        await self.coordinator.async_set_room_auto_clean_disabled(self.room_id, True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Re-enable this room for automatic away cleaning."""
        await self.coordinator.async_set_room_auto_clean_disabled(self.room_id, False)


def _slugify(value: str) -> str:
    """Create a Home Assistant object-id fragment."""
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")
