"""Binary sensors for Valetudo Vacuum Coordinator."""

from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .entity import ValetudoCoordinatorEntity, get_coordinator_from_discovery


async def async_setup_platform(
    hass: HomeAssistant,
    config: dict[str, Any],
    async_add_entities: AddEntitiesCallback,
    discovery_info: dict[str, Any] | None = None,
) -> None:
    """Set up binary sensors from YAML discovery."""
    coordinator = get_coordinator_from_discovery(hass.data, discovery_info)
    async_add_entities([ValetudoPauseBinarySensor(coordinator)])


class ValetudoPauseBinarySensor(ValetudoCoordinatorEntity, BinarySensorEntity):
    """Read-only pause status binary sensor."""

    _attr_icon = "mdi:pause-circle"

    def __init__(self, coordinator) -> None:
        """Initialize the pause binary sensor."""
        super().__init__(coordinator, "paused", "Paused")

    @property
    def is_on(self) -> bool:
        """Return whether away cleaning behavior is paused."""
        return self.coordinator.paused

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return pause details."""
        return {"reason": self.coordinator.pause_reason}
