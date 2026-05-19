"""Shared entity helpers for Valetudo Vacuum Coordinator."""

from __future__ import annotations

from typing import Any

from homeassistant.core import CALLBACK_TYPE, callback
from homeassistant.helpers.entity import DeviceInfo, Entity
from homeassistant.helpers import entity_registry as er

from .const import DOMAIN
from .coordinator import ValetudoVacuumCoordinator


class ValetudoCoordinatorEntity(Entity):
    """Base entity bound to a coordinator."""

    _attr_has_entity_name = False

    def __init__(
        self,
        coordinator: ValetudoVacuumCoordinator,
        translation_key: str,
        name_suffix: str,
    ) -> None:
        """Initialize the entity."""
        self.coordinator = coordinator
        self._attr_translation_key = translation_key
        self._attr_name = f"{coordinator.name} {name_suffix}"
        self._attr_unique_id = f"{coordinator.coordinator_id}_{translation_key}"
        self._attr_suggested_object_id = (
            f"{coordinator.coordinator_id}_{_slugify(name_suffix)}"
        )

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
        self._migrate_short_entity_id()

    @callback
    def _handle_update(self) -> None:
        """Write updated entity state."""
        self.async_write_ha_state()

    @callback
    def _migrate_short_entity_id(self) -> None:
        """Rename overly generic entity IDs created by early versions."""
        suggested_object_id = self._attr_suggested_object_id
        if self.entity_id is None or suggested_object_id is None:
            return
        domain, object_id = self.entity_id.split(".", 1)
        if object_id.startswith(self.coordinator.coordinator_id):
            return

        registry = er.async_get(self.hass)
        try:
            registry.async_update_entity(
                self.entity_id,
                new_entity_id=f"{domain}.{suggested_object_id}",
            )
        except ValueError:
            return


def get_coordinator_from_discovery(
    hass_data: dict[str, Any],
    discovery_info: dict[str, Any] | None,
) -> ValetudoVacuumCoordinator:
    """Resolve coordinator from platform discovery info."""
    if discovery_info is None:
        raise ValueError("discovery_info is required")
    coordinator_id = discovery_info["coordinator_id"]
    return hass_data[DOMAIN][coordinator_id]


def _slugify(value: str) -> str:
    """Create a Home Assistant object-id fragment."""
    return "_".join(value.lower().split())
