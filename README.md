# Valetudo Vacuum Coordinator

Home Assistant custom integration that coordinates away-only Valetudo room cleaning.

The integration starts the main-floor robot vacuum after configured people have all been away for a grace period, selects the room that has gone longest without a successful clean, observes Valetudo state to decide whether the room really completed, and stops the away session when someone arrives home.

## Installation

Install as a custom integration in either of these ways:

1. Copy `custom_components/valetudo_vacuum_coordinator` into your Home Assistant `config/custom_components/` directory.
2. Or add this GitHub repository to HACS as a custom integration repository, then install it from HACS.

After installation, add YAML configuration, restart Home Assistant, and check the new entities.

## Example Configuration

Adjust the entity IDs, MQTT topic, Valetudo select option names, and segment IDs for your own Home Assistant and Valetudo setup. If a listed optional entity does not exist in your HA instance, remove that line.

```yaml
valetudo_vacuum_coordinator:
  name: Downstairs Vacuum Coordinator
  vacuum_entity: vacuum.valetudo_robot
  people:
    - person.person_one
    - person.person_two
  away_delay: 300
  segment_command_topic: valetudo/robot/MapSegmentationCapability/clean/set
  status_flag_entity: sensor.valetudo_robot_status_flag
  dock_status_entity: sensor.valetudo_robot_dock_status
  error_entity: sensor.valetudo_robot_error
  current_area_entity: sensor.valetudo_robot_current_statistics_area
  current_time_entity: sensor.valetudo_robot_current_statistics_time
  estimated_segment_entity: sensor.valetudo_robot_estimated_segment
  mode_entity: select.valetudo_robot_mode
  mode_vacuum_option: vacuum
  mode_mop_option: vacuum_and_mop
  water_entity: select.valetudo_robot_water
  water_mop_option: high
  fresh_water_entity: sensor.valetudo_robot_freshwater_dock_component
  dirty_water_entity: sensor.valetudo_robot_wastewater_dock_component
  detergent_entity: sensor.valetudo_robot_detergent_dock_component
  dustbag_entity: sensor.valetudo_robot_dustbag_dock_component
  rooms:
    - id: room_one
      name: Room One
      segment_id: "1"
      mop_required: true
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
- Pause binary sensor: read-only status for dashboards and automation conditions.
- Session sensors: state, current room, queue summary.
- Per-room sensors: last successful clean timestamp and successful clean count.

## Notes

Valetudo's generic Home Assistant vacuum entity is not enough for reliable accounting. This integration also uses the Status Flag, Dock Status, Error, Current Statistics, Estimated Segment, and optional Dock Component sensors.

Binary sensors are read-only in Home Assistant, so the pause control is exposed as both a toggleable pause switch and a read-only pause binary sensor.

## Testing

```powershell
scripts/test.ps1
```

The script disables globally installed pytest plugins because this package's tests are pure logic tests and the workstation's `pytest-socket` plugin blocks asyncio's Windows socketpair during plugin setup.
