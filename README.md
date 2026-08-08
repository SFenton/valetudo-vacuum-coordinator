# Valetudo Vacuum Coordinator

Home Assistant custom integration that coordinates away-only Valetudo room cleaning.

The integration starts the main-floor robot vacuum after configured people have all been away for a grace period, selects the room that has gone longest without a successful clean, observes Valetudo state to decide whether the room really completed, and stops the away session when someone arrives home.

## Installation

Install as a custom integration in either of these ways:

1. Copy `custom_components/valetudo_vacuum_coordinator` into your Home Assistant `config/custom_components/` directory.
2. Or add this GitHub repository to HACS as a custom integration repository, then install it from HACS.

After installation, add YAML configuration, restart Home Assistant, and check the new entities.

## Example Configuration

Adjust the entity IDs, MQTT topic, Valetudo select option names, and segment IDs for your own Home Assistant and Valetudo setup. MQTT topics are case-sensitive; use the exact Valetudo MQTT identifier from the MQTT device registry or Valetudo settings, even when Home Assistant entity IDs are lowercase. If a listed optional entity does not exist in your HA instance, remove that line.

```yaml
valetudo_vacuum_coordinator:
  name: Downstairs Vacuum Coordinator
  vacuum_entity: vacuum.valetudo_robot
  people:
    - person.person_one
    - person.person_two
  away_delay: 300
  min_battery: 55
  native_resume_enabled: true
  native_resume_timeout: 10800
  dock_settle: 60
  resume_nudge_enabled: false
  segment_command_topic: valetudo/robot/MapSegmentationCapability/clean/set
  status_flag_entity: sensor.valetudo_robot_status_flag
  dock_status_entity: sensor.valetudo_robot_dock_status
  error_entity: sensor.valetudo_robot_error
  battery_entity: sensor.valetudo_robot_battery
  current_area_entity: sensor.valetudo_robot_current_statistics_area
  current_time_entity: sensor.valetudo_robot_current_statistics_time
  estimated_segment_entity: sensor.valetudo_robot_estimated_segment
  mode_entity: select.valetudo_robot_mode
  mode_vacuum_option: vacuum
  mode_mop_option: vacuum_and_mop
  fan_entity: select.valetudo_robot_fan
  fan_auto_clean_option: max
  passes_entity: input_select.valetudo_robot_cleaning_passes
  auto_clean_iterations: 2
  water_entity: select.valetudo_robot_water
  water_mop_option: max
  notify_service: notify.household
  notification_url: /sfenton-react-dash/home?path=living-room#robot-vacuum
  fresh_water_entity: sensor.valetudo_robot_freshwater_dock_component
  dirty_water_entity: sensor.valetudo_robot_wastewater_dock_component
  detergent_entity: sensor.valetudo_robot_detergent_dock_component
  dustbag_entity: sensor.valetudo_robot_dustbag_dock_component
  rooms:
    - id: room_one
      name: Room One
      segment_id: "1"
      mop_required: true
      manual_credit_entity: input_boolean.room_one_selected
      min_duration: 120
      min_area: 0
    - id: room_two
      name: Room Two
      segment_id: "2"
      mop_required: false
      min_duration: 120
      min_area: 0
```

See [configuration.example.yaml](configuration.example.yaml) for a fuller generic example.

## Entities

- Pause switch: toggle this on when guests are staying over or when you do not want automatic away cleaning.
- Per-room auto-clean disabled switches: toggle a room on here to exclude it from future away auto-clean sessions without changing manual selected-room cleaning.
- Pause binary sensor: read-only status for dashboards and automation conditions.
- Auto-cleaning binary sensor: read-only status that stays on during away auto-clean sessions and while a final summary is pending.
- Native-resume-pending binary sensor: read-only guard that stays on while a retained Valetudo task is interrupted or suspended. Use it to block manual/startup command loops.
- Session sensors: state, current room, queue summary.
- Per-room sensors: last successful clean timestamp and successful clean count.

## Dock Actions

The integration registers `valetudo_vacuum_coordinator.dock_action` as a
restricted Home Assistant bridge for Valetudo mop-dock cleaning and drying.
It accepts only an alphanumeric Valetudo identifier, `clean` or `dry`, and
`start` or `stop`. The service constructs the matching
`valetudo-<identifier>.local` capability URL; callers cannot supply arbitrary
URLs.

## Auto-Clean Notifications

Set `notify_service` to enable one final summary notification per away auto-clean session. Normal per-room completion and recoverable error notifications should be suppressed while the auto-cleaning binary sensor is on. The integration sends no summary if someone comes home before any room completes.

## Notes

Valetudo's generic Home Assistant vacuum entity is not enough for reliable accounting. This integration can also use the Status Flag, Dock Status, Error, Battery, Current Statistics, Estimated Segment, and optional Dock Component sensors.

Version 0.1.3 uses passive native resume for low-battery and dock/mop-rinse interruptions. The same active room run and session remain retained while the robot returns, docks, charges, or rinses. The coordinator does not call `vacuum.stop`, `vacuum.return_to_base`, `vacuum.start`, or publish a fresh segment as part of recovery. It waits for native `cleaning` plus `status_flag=segment`, accumulates statistics across counter resets, and only then continues accounting for the original run.

Configure `status_flag_entity` for passive native-resume confirmation. A suspended run is never released merely because the flag later becomes `none`; without a segment-status observation, it remains guarded until timeout or explicit cancellation.

`native_resume_timeout` defaults to three hours. Expiry ends the uncredited room as `needs_help` without restarting or otherwise commanding the robot. Setting `native_resume_enabled: false` also makes a low-battery interruption terminate as `needs_help`; it does not restore the old restart behavior. `dock_settle` defaults to 60 seconds so a docked/idle event cannot complete a run before late status or dock-state updates arrive. `min_battery` now applies only between room dispatches and never releases or restarts a suspended native task. `resume_nudge_enabled` is reserved and defaults to `false`; v0.1.3 intentionally implements no `vacuum.start` nudge or automatic fresh-segment fallback.

Person arrival and explicit cancellation remain intentionally destructive: when an active run exists, the coordinator persists cancellation intent, sends one blocking `vacuum.stop`, returns to base only when needed, then clears the run and restores settings.

Manual clean tracking credits rooms from Valetudo estimated-segment dwell. If a room has `manual_credit_entity`, a manual run snapshots selected rooms at start and only credits selected rooms that were also observed long enough. This keeps transit segments from being marked clean when a Home Assistant dashboard launches a selected-room run.

Binary sensors are read-only in Home Assistant, so the pause control is exposed as both a toggleable pause switch and a read-only pause binary sensor.

## Testing

```powershell
scripts/test.ps1
```

The script disables globally installed pytest plugins because this package's tests are pure logic tests and the workstation's `pytest-socket` plugin blocks asyncio's Windows socketpair during plugin setup.
