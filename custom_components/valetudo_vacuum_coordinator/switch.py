"""Switches for Valetudo Vacuum Coordinator."""

from __future__ import annotations

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
    async_add_entities([ValetudoPauseSwitch(coordinator)])


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
