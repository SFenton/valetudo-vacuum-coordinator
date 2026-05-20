"""Constants for Valetudo Vacuum Coordinator."""

DOMAIN = "valetudo_vacuum_coordinator"
NAME = "Valetudo Vacuum Coordinator"
VERSION = "0.1.0"

PLATFORM_BINARY_SENSOR = "binary_sensor"
PLATFORM_SENSOR = "sensor"
PLATFORM_SWITCH = "switch"
PLATFORMS = [PLATFORM_BINARY_SENSOR, PLATFORM_SENSOR, PLATFORM_SWITCH]

CONF_AWAY_DELAY = "away_delay"
CONF_BATTERY_ENTITY = "battery_entity"
CONF_DIRTY_WATER_ENTITY = "dirty_water_entity"
CONF_DOCK_STATUS_ENTITY = "dock_status_entity"
CONF_DUSTBAG_ENTITY = "dustbag_entity"
CONF_ERROR_ENTITY = "error_entity"
CONF_ESTIMATED_SEGMENT_ENTITY = "estimated_segment_entity"
CONF_FRESH_WATER_ENTITY = "fresh_water_entity"
CONF_IDENTIFIER = "identifier"
CONF_MANUAL_TRACKING = "manual_tracking"
CONF_MIN_BATTERY = "min_battery"
CONF_MODE_ENTITY = "mode_entity"
CONF_MODE_VACUUM_OPTION = "mode_vacuum_option"
CONF_MODE_MOP_OPTION = "mode_mop_option"
CONF_MOP_ATTACHMENT_ENTITY = "mop_attachment_entity"
CONF_DETERGENT_ENTITY = "detergent_entity"
CONF_NOTIFICATION_URL = "notification_url"
CONF_NOTIFY_SERVICE = "notify_service"
CONF_PEOPLE = "people"
CONF_ROOMS = "rooms"
CONF_SEGMENT_COMMAND_TOPIC = "segment_command_topic"
CONF_STATUS_FLAG_ENTITY = "status_flag_entity"
CONF_CURRENT_AREA_ENTITY = "current_area_entity"
CONF_CURRENT_TIME_ENTITY = "current_time_entity"
CONF_VACUUM_ENTITY = "vacuum_entity"
CONF_WATER_ENTITY = "water_entity"
CONF_WATER_MOP_OPTION = "water_mop_option"
CONF_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED = "allow_vacuum_only_when_mop_blocked"
CONF_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL = "cancel_any_away_run_on_arrival"
CONF_TRACK_MANUAL_WHEN_PAUSED = "track_manual_when_paused"

CONF_ROOM_ID = "id"
CONF_ROOM_NAME = "name"
CONF_ROOM_SEGMENT_ID = "segment_id"
CONF_ROOM_MOP_REQUIRED = "mop_required"
CONF_ROOM_ENABLED = "enabled"
CONF_ROOM_MIN_DURATION = "min_duration"
CONF_ROOM_MIN_AREA = "min_area"
CONF_ROOM_MIN_ESTIMATED_DWELL = "min_estimated_dwell"
CONF_ROOM_REQUIRE_ESTIMATED_SEGMENT = "require_estimated_segment"

DEFAULT_AWAY_DELAY = 300
DEFAULT_MIN_BATTERY = 40
DEFAULT_ROOM_MIN_DURATION = 120
DEFAULT_ROOM_MIN_AREA = 0.0
DEFAULT_ROOM_MIN_ESTIMATED_DWELL = 30
DEFAULT_ALLOW_VACUUM_ONLY_WHEN_MOP_BLOCKED = False
DEFAULT_CANCEL_ANY_AWAY_RUN_ON_ARRIVAL = True
DEFAULT_MANUAL_TRACKING = True
DEFAULT_TRACK_MANUAL_WHEN_PAUSED = True
DEFAULT_MODE_VACUUM_OPTION = "vacuum"
DEFAULT_MODE_MOP_OPTION = "vacuum_and_mop"
DEFAULT_WATER_MOP_OPTION = "high"

SERVICE_CANCEL_SESSION = "cancel_session"
SERVICE_MARK_ROOM_CLEANED = "mark_room_cleaned"
SERVICE_RESET_ROOM = "reset_room"
SERVICE_SET_PAUSED = "set_paused"
SERVICE_START_SESSION = "start_session"

ATTR_ACTIVE_ROOM = "active_room"
ATTR_CANCELLED = "cancelled"
ATTR_COMPLETED_ROOMS = "completed_rooms"
ATTR_FAILED_ROOMS = "failed_rooms"
ATTR_FAILED_REASONS = "failed_reasons"
ATTR_LAST_FAILED_REASON = "last_failed_reason"
ATTR_LAST_MOPPED = "last_mopped"
ATTR_LAST_SUCCESSFUL_CLEAN = "last_successful_clean"
ATTR_LAST_VACUUMED = "last_vacuumed"
ATTR_PENDING_ROOMS = "pending_rooms"
ATTR_ROOM_ID = "room_id"
ATTR_SESSION_ID = "session_id"
ATTR_SKIPPED_REASONS = "skipped_reasons"
ATTR_SKIPPED_ROOMS = "skipped_rooms"
ATTR_SUCCESSFUL_COUNT = "successful_count"
ATTR_TERMINAL_REASON = "terminal_reason"
ATTR_TERMINAL_MESSAGE = "terminal_message"
ATTR_NEEDS_HELP = "needs_help"
ATTR_NOTIFICATION_SENT = "notification_sent"
ATTR_VACUUM_ONLY = "vacuum_only"

STORE_VERSION = 1
STORE_KEY = "valetudo_vacuum_coordinator"

STATE_IDLE = "idle"
STATE_PAUSED = "paused"
STATE_RUNNING = "running"
STATE_WAITING = "waiting"
STATE_ERROR = "error"
