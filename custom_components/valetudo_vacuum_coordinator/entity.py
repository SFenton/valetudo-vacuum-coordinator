"""Shared entity helpers for Valetudo Vacuum Coordinator."""

from __future__ import annotations

from typing import Any

from homeassistant.core import CALLBACK_TYPE, callback
from homeassistant.helpers.entity import DeviceInfo, Entity

from .const import DOMAIN
from .coordinator import ValetudoVacuumCoordinator


class ValetudoCoordinatorEntity(Entity):
    """Base entity bound to a coordinator."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ValetudoVacuumCoordinator,
        translation_key: str,
        name_suffix: str,
    ) -> None:
        """Initialize the entity."""
        self.coordinator = coordinator
        self._attr_translation_key = translation_key
        self._attr_name = name_suffix
        self._attr_unique_id = f"{coordinator.coordinator_id}_{translation_key}"

    @property
    def device_info(self) -> DeviceInfo:
        """Return the coordinator device info."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.coordinator.coordinator_id)},
            name=self.coordinator.name,
            manufacturer="Valetudo",
            model="Away Room Cleaning Coordinator",
        )

    async def async_added_to_hass(self) -> None:
        """Subscribe entity updates to coordinator notifications."""
        self.async_on_remove(self.coordinator.async_add_listener(self._handle_update))

    @callback
    def _handle_update(self) -> None:
        """Write updated entity state."""
        self.async_write_ha_state()


def get_coordinator_from_discovery(
    hass_data: dict[str, Any],
    discovery_info: dict[str, Any] | None,
) -> ValetudoVacuumCoordinator:
    """Resolve coordinator from platform discovery info."""
    if discovery_info is None:
        raise ValueError("discovery_info is required")
    coordinator_id = discovery_info["coordinator_id"]
    return hass_data[DOMAIN][coordinator_id]
