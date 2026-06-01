"""Valetudo Vacuum Coordinator integration."""

from __future__ import annotations

from collections.abc import Iterable
import logging
from typing import Any

import voluptuous as vol

from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv, discovery
from homeassistant.helpers.typing import ConfigType

from .const import (
    CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED,
    CONF_AUTO_CLEAN_ITERATIONS,
    CONF_AWAY_DELAY,
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
    CONF_IDENTIFIER,
    CONF_MANUAL_TRACKING,
    CONF_MIN_BATTERY,
    CONF_MODE_ENTITY,
    CONF_MODE_MOP_OPTION,
    CONF_MODE_VACUUM_OPTION,
    CONF_MOP_ATTACHMENT_ENTITY,
    CONF_NOTIFICATION_URL,
    CONF_NOTIFY_SERVICE,
    CONF_PEOPLE,
    CONF_PASSES_ENTITY,
    CONF_ROOM_ENABLED,
    CONF_ROOM_ID,
    CONF_ROOM_MIN_AREA,
    CONF_ROOM_MIN_DURATION,
    CONF_ROOM_MIN_ESTIMATED_DWELL,
    CONF_ROOM_MOP_REQUIRED,
    CONF_ROOM_NAME,
    CONF_ROOM_REQUIRE_ESTIMATED_SEGMENT,
    CONF_ROOM_SEGMENT_ID,
    CONF_ROOMS,
    CONF_SEGMENT_COMMAND_TOPIC,
    CONF_STATUS_FLAG_ENTITY,
    CONF_TRACK_MANUAL_WHEN_PAUSED,
    CONF_VACUUM_ENTITY,
    CONF_WATER_ENTITY,
    CONF_WATER_MOP_OPTION,
    DEFAULT_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED,
    DEFAULT_AUTO_CLEAN_ITERATIONS,
    DEFAULT_AWAY_DELAY,
    DEFAULT_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL,
    DEFAULT_FAN_AUTO_CLEAN_OPTION,
    DEFAULT_MANUAL_TRACKING,
    DEFAULT_MIN_BATTERY,
    DEFAULT_MODE_MOP_OPTION,
    DEFAULT_MODE_VACUUM_OPTION,
    DEFAULT_ROOM_MIN_AREA,
    DEFAULT_ROOM_MIN_DURATION,
    DEFAULT_ROOM_MIN_ESTIMATED_DWELL,
    DEFAULT_TRACK_MANUAL_WHEN_PAUSED,
    DEFAULT_WATER_MOP_OPTION,
    DOMAIN,
    PLATFORMS,
    SERVICE_CANCEL_SESSION,
    SERVICE_MARK_ROOM_CLEANED,
    SERVICE_RESET_ROOM,
    SERVICE_SET_PAUSED,
    SERVICE_START_SESSION,
)
from .coordinator import ValetudoVacuumCoordinator
from .logic import RoomConfig

_LOGGER = logging.getLogger(__name__)

ROOM_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ROOM_ID): cv.string,
        vol.Required(CONF_ROOM_NAME): cv.string,
        vol.Required(CONF_ROOM_SEGMENT_ID): cv.string,
        vol.Optional(CONF_ROOM_MOP_REQUIRED, default=False): cv.boolean,
        vol.Optional(CONF_ROOM_ENABLED, default=True): cv.boolean,
        vol.Optional(CONF_ROOM_MIN_DURATION, default=DEFAULT_ROOM_MIN_DURATION): cv.positive_int,
        vol.Optional(CONF_ROOM_MIN_AREA, default=DEFAULT_ROOM_MIN_AREA): vol.Coerce(float),
        vol.Optional(
            CONF_ROOM_MIN_ESTIMATED_DWELL,
            default=DEFAULT_ROOM_MIN_ESTIMATED_DWELL,
        ): cv.positive_int,
        vol.Optional(CONF_ROOM_REQUIRE_ESTIMATED_SEGMENT, default=False): cv.boolean,
    }
)

COORDINATOR_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_NAME, default="Main Floor Vacuum Coordinator"): cv.string,
        vol.Required(CONF_VACUUM_ENTITY): cv.entity_id,
        vol.Required(CONF_PEOPLE): vol.All(cv.ensure_list, [cv.entity_id]),
        vol.Required(CONF_SEGMENT_COMMAND_TOPIC): cv.string,
        vol.Required(CONF_ROOMS): vol.All(cv.ensure_list, [ROOM_SCHEMA]),
        vol.Optional(CONF_IDENTIFIER): cv.string,
        vol.Optional(CONF_AWAY_DELAY, default=DEFAULT_AWAY_DELAY): cv.positive_int,
        vol.Optional(CONF_MIN_BATTERY, default=DEFAULT_MIN_BATTERY): vol.Coerce(float),
        vol.Optional(CONF_STATUS_FLAG_ENTITY): cv.entity_id,
        vol.Optional(CONF_DOCK_STATUS_ENTITY): cv.entity_id,
        vol.Optional(CONF_ERROR_ENTITY): cv.entity_id,
        vol.Optional(CONF_BATTERY_ENTITY): cv.entity_id,
        vol.Optional(CONF_CURRENT_AREA_ENTITY): cv.entity_id,
        vol.Optional(CONF_CURRENT_TIME_ENTITY): cv.entity_id,
        vol.Optional(CONF_ESTIMATED_SEGMENT_ENTITY): cv.entity_id,
        vol.Optional(CONF_AUTO_CLEAN_ITERATIONS, default=DEFAULT_AUTO_CLEAN_ITERATIONS): cv.positive_int,
        vol.Optional(CONF_MODE_ENTITY): cv.entity_id,
        vol.Optional(CONF_MODE_VACUUM_OPTION, default=DEFAULT_MODE_VACUUM_OPTION): cv.string,
        vol.Optional(CONF_MODE_MOP_OPTION, default=DEFAULT_MODE_MOP_OPTION): cv.string,
        vol.Optional(CONF_FAN_ENTITY): cv.entity_id,
        vol.Optional(CONF_FAN_AUTO_CLEAN_OPTION, default=DEFAULT_FAN_AUTO_CLEAN_OPTION): cv.string,
        vol.Optional(CONF_PASSES_ENTITY): cv.entity_id,
        vol.Optional(CONF_WATER_ENTITY): cv.entity_id,
        vol.Optional(CONF_WATER_MOP_OPTION, default=DEFAULT_WATER_MOP_OPTION): cv.string,
        vol.Optional(CONF_MOP_ATTACHMENT_ENTITY): cv.entity_id,
        vol.Optional(CONF_NOTIFY_SERVICE): cv.string,
        vol.Optional(CONF_NOTIFICATION_URL): cv.string,
        vol.Optional(CONF_FRESH_WATER_ENTITY): cv.entity_id,
        vol.Optional(CONF_DIRTY_WATER_ENTITY): cv.entity_id,
        vol.Optional(CONF_DETERGENT_ENTITY): cv.entity_id,
        vol.Optional(CONF_DUSTBAG_ENTITY): cv.entity_id,
        vol.Optional(
            CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED,
            default=DEFAULT_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED,
        ): cv.boolean,
        vol.Optional(
            CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL,
            default=DEFAULT_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL,
        ): cv.boolean,
        vol.Optional(CONF_MANUAL_TRACKING, default=DEFAULT_MANUAL_TRACKING): cv.boolean,
        vol.Optional(
            CONF_TRACK_MANUAL_WHEN_PAUSED,
            default=DEFAULT_TRACK_MANUAL_WHEN_PAUSED,
        ): cv.boolean,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Any(COORDINATOR_SCHEMA, vol.All(cv.ensure_list, [COORDINATOR_SCHEMA])),
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Valetudo Vacuum Coordinator from YAML."""
    raw_configs = config.get(DOMAIN)
    if raw_configs is None:
        return True

    coordinator_configs = _as_list(raw_configs)
    hass.data.setdefault(DOMAIN, {})

    for raw_config in coordinator_configs:
        coordinator = ValetudoVacuumCoordinator(
            hass=hass,
            name=raw_config[CONF_NAME],
            vacuum_entity=raw_config[CONF_VACUUM_ENTITY],
            people_entities=raw_config[CONF_PEOPLE],
            segment_command_topic=raw_config[CONF_SEGMENT_COMMAND_TOPIC],
            rooms=_build_rooms(raw_config[CONF_ROOMS]),
            config=raw_config,
        )
        await coordinator.async_setup()
        hass.data[DOMAIN][coordinator.coordinator_id] = coordinator

        for platform in PLATFORMS:
            hass.async_create_task(
                discovery.async_load_platform(
                    hass,
                    platform,
                    DOMAIN,
                    {"coordinator_id": coordinator.coordinator_id},
                    config,
                )
            )

    _register_services(hass)
    return True


def _as_list(value: Any) -> list[Any]:
    """Return a config value as a list."""
    if isinstance(value, list):
        return value
    return [value]


def _build_rooms(raw_rooms: Iterable[dict[str, Any]]) -> list[RoomConfig]:
    """Build room config objects."""
    return [
        RoomConfig(
            room_id=raw_room[CONF_ROOM_ID],
            name=raw_room[CONF_ROOM_NAME],
            segment_id=raw_room[CONF_ROOM_SEGMENT_ID],
            mop_required=raw_room[CONF_ROOM_MOP_REQUIRED],
            enabled=raw_room[CONF_ROOM_ENABLED],
            min_duration=raw_room[CONF_ROOM_MIN_DURATION],
            min_area=raw_room[CONF_ROOM_MIN_AREA],
            min_estimated_dwell=raw_room[CONF_ROOM_MIN_ESTIMATED_DWELL],
            require_estimated_segment=raw_room[CONF_ROOM_REQUIRE_ESTIMATED_SEGMENT],
        )
        for raw_room in raw_rooms
    ]


def _register_services(hass: HomeAssistant) -> None:
    """Register integration services once."""
    if hass.data.setdefault(DOMAIN, {}).get("_services_registered", False):
        return

    async def async_start_session(call: ServiceCall) -> None:
        coordinator = _get_coordinator(hass, call.data.get("coordinator"))
        await coordinator.async_start_session(reason="service")

    async def async_cancel_session(call: ServiceCall) -> None:
        coordinator = _get_coordinator(hass, call.data.get("coordinator"))
        await coordinator.async_cancel_session(call.data.get("reason", "service"))

    async def async_set_paused(call: ServiceCall) -> None:
        coordinator = _get_coordinator(hass, call.data.get("coordinator"))
        await coordinator.async_set_paused(bool(call.data["paused"]), call.data.get("reason", "service"))

    async def async_mark_room_cleaned(call: ServiceCall) -> None:
        coordinator = _get_coordinator(hass, call.data.get("coordinator"))
        await coordinator.async_mark_room_cleaned(
            str(call.data["room_id"]),
            mop=bool(call.data.get("mop", True)),
            vacuum=bool(call.data.get("vacuum", True)),
        )

    async def async_reset_room(call: ServiceCall) -> None:
        coordinator = _get_coordinator(hass, call.data.get("coordinator"))
        await coordinator.async_reset_room(str(call.data["room_id"]))

    hass.services.async_register(DOMAIN, SERVICE_START_SESSION, async_start_session)
    hass.services.async_register(DOMAIN, SERVICE_CANCEL_SESSION, async_cancel_session)
    hass.services.async_register(DOMAIN, SERVICE_SET_PAUSED, async_set_paused)
    hass.services.async_register(DOMAIN, SERVICE_MARK_ROOM_CLEANED, async_mark_room_cleaned)
    hass.services.async_register(DOMAIN, SERVICE_RESET_ROOM, async_reset_room)
    hass.data[DOMAIN]["_services_registered"] = True


def _get_coordinator(hass: HomeAssistant, coordinator_id: str | None) -> ValetudoVacuumCoordinator:
    """Get a configured coordinator by id or return the only coordinator."""
    coordinators = {
        key: value
        for key, value in hass.data.get(DOMAIN, {}).items()
        if isinstance(value, ValetudoVacuumCoordinator)
    }
    if coordinator_id:
        return coordinators[coordinator_id]
    if len(coordinators) == 1:
        return next(iter(coordinators.values()))
    raise ValueError("coordinator is required when multiple coordinators are configured")
