# Valetudo Vacuum Coordinator

Home Assistant custom integration that coordinates away-only Valetudo room cleaning
and can expose independent, read-only status contracts for any Valetudo vacuum.

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
  dispatch_start_timeout: 120
  # Recoverable blockers recheck on this cadence; they do not end the session.
  blocked_session_timeout: 300
  # Debounce dock-component faults during an active run.
  resource_settle: 3
  stale_resume_auto_clear: false
  stale_resume_age: 1800
  stale_resume_settle: 60
  stale_resume_clear_timeout: 30
  resume_nudge_enabled: false
  identifier: robot
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
  allow_vacuum_only_when_mop_blocked: true
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

## Status Observers

Status observers are independent from room-cleaning coordinators. They do not
require people, rooms, MQTT command topics, or coordinator sessions, and they
never call Home Assistant services. Each observer reads its configured vacuum,
raw error, status flag, dock status, battery, and optional typed condition
entities, then exposes one sensor such as
`sensor.main_floor_vacuum_status`.

Legacy coordinator-only configuration remains supported unchanged. To configure
coordinators and observers together, use the structured form:

```yaml
valetudo_vacuum_coordinator:
  coordinators:
    - name: Downstairs Vacuum Coordinator
      vacuum_entity: vacuum.valetudo_robot
      people:
        - person.person_one
      segment_command_topic: valetudo/robot/MapSegmentationCapability/clean/set
      rooms:
        - id: room_one
          name: Room One
          segment_id: "1"

  status_observers:
    - id: downstairs
      name: Downstairs Vacuum
      vacuum_entity: vacuum.valetudo_robot
      error_entity: sensor.valetudo_robot_error
      status_flag_entity: sensor.valetudo_robot_status_flag
      dock_status_entity: sensor.valetudo_robot_dock_status
      battery_entity: sensor.valetudo_robot_battery
      outage_confirmation_seconds: 180
      conditions:
        - entity_id: binary_sensor.downstairs_vacuum_native_resume_pending
          code: vacuum.task_resume_pending
          source: coordinator
          active_states:
            - "on"
```

Do not mix legacy coordinator keys such as `vacuum_entity`, `people`,
`segment_command_topic`, or `rooms` beside `status_observers`. Move the
unchanged legacy coordinator under `coordinators`; mixed shapes are rejected
rather than silently dropping coordinator configuration.

See
[configuration.status-observers.example.yaml](configuration.status-observers.example.yaml)
for Main Floor, Music Room, and Theater Room observers.

### Status Contract Version 1

The sensor state is the observed vacuum state, or the availability value when
the primary entity is missing. Its attributes contain:

| Attribute | Contract |
| --- | --- |
| `version` | Status contract version, currently `1`. |
| `issue_code_version` | Version of the pure raw-error-to-semantic-code mapping. |
| `vacuum_entity_id` | Configured primary vacuum entity. |
| `observed_vacuum_state` | Exact current primary state, or `null` when missing. |
| `availability` | `status` is `available`, `unavailable`, `unknown`, or `missing`; `since` is populated only after the configured outage confirmation interval. |
| `current_issue` | Discriminated by `status`: `clear`, `unknown`, or `present`. Present issues include stable `code`, exact `raw`, and `reported_at`. |
| `active_conditions` | Current typed conditions with `code`, `source` (`coordinator`, `automation`, or `derived`), and `since`. A `resumable` status flag automatically contributes `vacuum.task_resume_pending` with source `derived`. |
| `last_issue` | Latest issue latched by a confirmed outage, including `code`, `raw`, `reported_at`, `provenance`, and optional `cleared_at`. |
| `command_policy` | `mode` is `normal`, `restricted`, or `none`; `reason` is a stable code or `null`. |

Primary `unavailable`, `unknown`, and missing states affect the live contract
and command policy immediately. The default 180-second confirmation applies
only to durable outage history. Persistent `unavailable` and `unknown` states
are confirmed; until then, `availability.since` remains `null` and a prior
current issue is not copied to `last_issue`. If the vacuum recovers first,
that pending history is discarded.

`last_issue` remains available after recovery with `cleared_at` populated.
Consumers must hide cleared history from their default current-status surface;
it is retained only for explicit history or diagnostic presentation. A later
outage with no new current issue does not make the cleared issue unresolved
again.

Only the raw error entity's state string determines issue identity. Error
attributes are intentionally ignored because MQTT can update the state and its
attributes in separate events. Every source event is processed, including
same-state attribute events; sensor state writes are suppressed only when the
resulting contract is unchanged.

If the raw error source becomes unreadable before the primary vacuum becomes
nonavailable, the issue immediately leaves `current_issue` but remains an
internal last-readable candidate. It is discarded by a coherent explicit
clear, or copied to `last_issue` only after a confirmed nonavailable epoch.
Queued source snapshots are processed in callback order so rapid MQTT
state/attribute/unavailable bursts cannot erase that candidate.

Stored state contains confirmed outage history only. Startup always rebuilds
the current issue and active conditions from current Home Assistant entities,
so a previously current issue is never restored as current. Version 1 does not
automatically backfill recorder history. The `recorder_backfill` provenance is
accepted only from a future typed Store migration; there is no YAML backfill
field or status-observer service.

## Entities

- Pause switch: toggle this on when guests are staying over or when you do not want automatic away cleaning.
- Per-room auto-clean disabled switches: toggle a room on here to exclude it from future away auto-clean sessions without changing manual selected-room cleaning.
- Pause binary sensor: read-only status for dashboards and automation conditions.
- Auto-cleaning binary sensor: read-only status that stays on during away auto-clean sessions and while a final summary is pending.
- Native-resume-pending binary sensor: read-only guard that stays on while a retained Valetudo task is interrupted or suspended. Use it to block manual/startup command loops.
- Session sensors: state, current room, actionable queue, fallback-vacuumed rooms, deferred full-clean rooms, typed blocker disposition, recovery phase/deadline, preserved rooms, command acknowledgements, and uncertain floor outcomes.
- Per-room sensors: last successful clean, last vacuumed, last fallback-vacuumed, last mopped, and successful clean count.

The session-state sensor also exposes a versioned `while_away_outcomes`
attribute. Version 2 contains an ordered typed event history and one
authoritative current-day projection per room. The projection identifies the
required operation from each room's `mop_required` configuration, the latest
attempt and result, granted credit, and any outstanding operation. Existing
`while_away_cleaned` and `while_away_issues` attributes remain available for
compatibility. A room projection's `status` is the authoritative primary
outcome; `credit` and `outstanding` are orthogonal details. Once full credit is
earned for the current day, the primary status remains completed with no
outstanding operation, while any later anomalous attempt remains in event
history only. Version 2 adds `partial` and `uncertain` attempt results so
threshold-backed floor work is not mislabeled as a generic failure when dock
servicing faults race completion. Requested iterations are tracked from
distinct segment cycles; if Valetudo does not expose enough evidence to prove
all requested passes, the result remains `uncertain` rather than receiving full
credit. Uncertain work is not blindly repeated.
Outcome retention is bounded to the current Home Assistant local day and is
also cleared when manual cleaning starts.

## Automatic Recovery Policy

Version 0.3.0 keeps the same logical away session and queue for every
recoverable blocker. This includes low battery and native recharge, dock
rinse/service, clean-water faults, wastewater, detergent, dustbag conditions,
temporary unavailable/unknown entities, recoverable navigation failures,
dock busy states, and pending-command races. `blocked_session_timeout` is now a
bounded recheck cadence, not a terminal deadline.

Only errors whose text explicitly identifies an unrecoverable/fatal permanent
failure terminalize the session. Unknown firmware errors default to
recoverable waiting so a newly introduced error cannot silently discard the
queue.

When a blocker appears after a segment publish but before cleaning is
confirmed, the coordinator persists cancellation intent before issuing exactly
one blocking `vacuum.stop`. It records service or physical acknowledgement,
does not repeat an uncertain stop after restart, and cannot dispatch another
segment until cancellation is acknowledged. `return_to_base` is used only when
the robot is still moving.

Ignored segment commands use persisted per-room exponential backoff. After
three consecutive publish/start cancellations under the same robot-state
fingerprint, the room remains preserved but no further segment is published
until the vacuum, dock, resource, battery-readiness bucket, or mode state
changes materially.

Clean-water blockers enter degraded mode. All native vacuum-only rooms remain
eligible even when `allow_vacuum_only_when_mop_blocked: false`; that option
controls only fallback vacuuming of rooms whose configured operation requires
mopping. Deferred mop work resumes automatically in the same away session when
clean water recovers. Wastewater, detergent, dustbag, and error-120 conditions
also permit configured native vacuum-only rooms when the robot can safely
depart, while affected mop or dock-service work remains deferred. Mode-service
failures use bounded exponential retry indefinitely rather than becoming a
permanent dead end.

The session sensor exposes:

- `blocker_code`, `blocker_disposition`, `blocker_operator_action`, `recovery_phase`,
  `recovery_started_at`, and `next_retry_at`;
- `waiting_for_physical_fix` and `preserved_rooms`;
- current and last command publish/stop/return attempts and acknowledgements;
- degraded dock-stop/mode attempts and acknowledgements;
- `uncertain_rooms` and `uncertain_reasons`.

Waiting and recovery notifications are deduplicated per blocker. A recovery
notification is sent once when an alerted blocker clears.

## Dock Actions

The integration registers `valetudo_vacuum_coordinator.dock_action` as a
restricted Home Assistant bridge for Valetudo mop-dock cleaning and drying.
It accepts only an alphanumeric Valetudo identifier, `clean` or `dry`, and
`start` or `stop`. The service constructs the matching
`valetudo-<identifier>.local` capability URL; callers cannot supply arbitrary
URLs.

## Auto-Clean Notifications

Set `notify_service` to enable one final summary notification per away auto-clean session. Normal per-room completion and recoverable error notifications should be suppressed while the auto-cleaning binary sensor is on. The integration sends no summary if someone comes home before any room completes.

When clean water remains empty, the final summary distinguishes fully completed
rooms from rooms that were only vacuumed and still need mopping.

## Notes

Valetudo's generic Home Assistant vacuum entity is not enough for reliable accounting. This integration can also use the Status Flag, Dock Status, Error, Battery, Current Statistics, Estimated Segment, and optional Dock Component sensors.

Version 0.3.1 lets a persisted stale or `operator_required` retained-task guard
yield to an active session's safe vacuum-only resource recovery. When the
status flag is clear and the vacuum is at the dock with a recoverable
vacuum-only-safe blocker such as a missing clean-water tank, the coordinator
stops treating the old guard as the primary blocker. It proceeds through the
bounded degraded dock-stop and mode preparation, continues native vacuum-only
rooms, and preserves mop-required rooms for automatic resume after the
resource is restored.

Version 0.3.0 makes auto-resume the default invariant for coordinator-owned
recoverable
conditions. Stored 0.2.0 `blocked`, `mop_resource_deferred`, and non-fatal
`needs_help` sessions migrate back into explicit waiting state with their room
history, deferred work, settings snapshot, retained-task guard, and away
timestamp preserved. The Store version remains compatible and all new fields
have safe defaults. A separate persisted coordinator data schema version (`3`)
gates this legacy revival so a v0.3 terminal session remains terminal on later
restarts. General HACS installations keep
`stale_resume_auto_clear: false`; users who have validated `vacuum.stop` for
their robot can continue to opt in explicitly. A migrated terminal session
clears its old final-notification flag. If no settings snapshot was stored, it
also clears `settings_prepared` so the next command captures fresh settings and
can restore them safely.

Version 0.2.0 added independent status observers and the version 1 read-only
status contract. Existing coordinator-only YAML remains valid. The structured
configuration form can run any number of coordinators and status observers
together, while status observers remain independent from people, room ledgers,
segment commands, and coordinator services.

Version 0.1.8 adds a persisted retained-task guard that records whether a task
is coordinator-owned, manually observed, or unknown. Every away session now
reconciles that guard before changing passes, fan, water, or mode settings.
Coordinator-owned low-battery and dock interruptions keep their existing
passive native-resume behavior. Active manual or external cleaning is observed
without issuing commands, and unknown work is never actively resumed.

An unowned `resumable` task must remain coherently docked and inactive for
`stale_resume_age` before it is classified as stale. Automatic clearing remains
shadow-safe by default with `stale_resume_auto_clear: false`. Explicitly
enabling it after robot-specific validation permits exactly one
persisted `vacuum.stop` publication. The coordinator then waits up to
`stale_resume_clear_timeout` for the status flag to acknowledge the clear task.
If that succeeds while the dock remains stably `pause`, and `identifier` is
configured, the same transaction may publish exactly one persisted
`dock_action(clean, stop)` and then verify `dock_status=idle`. It never calls
`vacuum.start` for unknown work. A missing identifier, failed publication,
uncertain restart window, exhausted command attempt, or missing acknowledgement
becomes `operator_required` without changing cleaning settings. If the robot
later reaches a coherent clear state, the same preflight session continues
instead of scheduling a new same-away session.

`operator_required` preserves the logical away session in an explicit
suspended state. It prevents another away-timer session from racing the
unresolved task, schedules bounded reconciliations, and allows the same queue
to continue immediately after an operator or robot update clears the blocker.
Manual or external cleaning that is still active is only observed. A dormant
manually observed task that remains docked and `resumable` beyond
`stale_resume_age` is eligible for the same bounded, configuration-gated clear
sequence as an unknown stale task.

Final terminal summaries are sent independently of robot state. Restoring the
captured cleaning settings is a separate step that waits only while a
coordinator run, manually tracked run, or actively moving/cleaning robot still
exists; a docked or idle `resumable` flag by itself does not delay restoration.

The session and native-resume sensors expose retained-task owner, phase,
first-observed and last-material-activity timestamps, stale and verification
deadlines, clear attempts and acknowledgement, and any operator-required
reason, including the separate generic-task and dock-clean-stop stages.
Operator-required notifications are independent of final-summary and
settings-restoration safety gates.

Version 0.1.7 adds the typed `while_away_outcomes` contract without replacing
the version 0.1.6 session attributes. Events have stable IDs, persisted
monotonic sequence numbers, operation modes, terminal results, typed reason
codes with raw diagnostics, and structured threshold data where available.
The room projection retains separate result and outstanding reasons so a
fallback vacuum can fail for one reason while the configured full clean
remains deferred for another. Legacy records remain readable and set
`complete: false` until all retained current-day records are typed. Version
0.1.7 is forward-compatible with version 0.1.6 stored data; rolling back may
make compatibility-only deferral records visible to the older summary logic.
Typed events whose room configuration is no longer present remain in history
and cause `complete: false` rather than producing an inferred projection.

Version 0.1.6 handles the exact clean-water-empty dock fault as a finite
degraded session. It cancels the interrupted full run, processes every
configured vacuum-only room first, then—when
`allow_vacuum_only_when_mop_blocked` is enabled—vacuum-cleans each incomplete
dual-mode room at most once. Those fallback rooms update `last_vacuumed` and
`last_fallback_vacuumed`, but not the successful-clean timestamp, daily
auto-clean slot, mop timestamp, successful count, or full completed-room list.
They therefore remain due for a later normal vacuum-and-mop session.

In version 0.1.6 the degraded queue terminalized as
`mop_resource_deferred`, and `blocked_session_timeout` ended persistent waits.
Version 0.3.0 supersedes that behavior: both fields migrate into a live,
bounded waiting state, deferred work resumes on clear, and no new departure or
manual `start_session` call is required.

Fallback vacuuming of mop-required rooms remains limited to positively
identified clean-water faults. Unknown error 120, wastewater, detergent,
dustbag, tray, and other resource conditions may allow native vacuum-only rooms
but do not downgrade mop-required rooms to fallback vacuuming.
`blocked_session_timeout` now schedules the next diagnostic recheck. Battery,
charging, and owned native-resume waits use `native_resume_timeout` before
entering the slower stalled-recovery phase, but remain recoverable. Raw unowned
`resumable` state still uses the retained-task preflight policy.

If `identifier` is configured, degraded preparation sends one bounded
mop-dock-clean stop through the existing restricted dock-action bridge before
selecting vacuum mode. Firmware support for starting a segment while the
clean-water warning remains latched is model-specific; an ignored command is
cancelled with one acknowledged stop and only reconsidered on the bounded
recovery cadence.

Version 0.1.3 uses passive native resume for low-battery and dock/mop-rinse interruptions. The same active room run and session remain retained while the robot returns, docks, charges, or rinses. The coordinator does not call `vacuum.stop`, `vacuum.return_to_base`, `vacuum.start`, or publish a fresh segment as part of recovery. It waits for native `cleaning` plus `status_flag=segment`, accumulates statistics across counter resets, and only then continues accounting for the original run.

Configure `status_flag_entity` for passive native-resume confirmation. A suspended low-battery run remains guarded until the firmware resumes it, the timeout expires, or it is explicitly cancelled. Other retained tasks can finalize when the robot is stably docked, the flag has cleared to `none` after the suspension, the dock and error sensors are clear, and the configured dock-settle window has elapsed. This distinguishes a completed final pass from a real mop-rinse interruption without issuing recovery commands.

`native_resume_timeout` defaults to three hours. In 0.3.0 expiry moves the
preserved run into `recovery_stalled` and schedules another bounded
reconciliation; it no longer terminalizes the room. The legacy
`native_resume_enabled: false` value is retained for configuration
compatibility but no longer permits recoverable low-battery work to be
discarded. `dock_settle` defaults to 60 seconds so a docked/idle event cannot
complete a run before late status or dock-state updates arrive. `min_battery`
applies only between room dispatches. `resume_nudge_enabled` remains reserved
and no `vacuum.start` nudge is issued.

Person arrival and explicit cancellation remain intentionally destructive: when an active run exists, the coordinator persists cancellation intent, sends one blocking `vacuum.stop`, returns to base only when needed, then clears the run and restores settings.

Manual clean tracking credits rooms from Valetudo estimated-segment dwell. If a room has `manual_credit_entity`, a manual run snapshots selected rooms at start and only credits selected rooms that were also observed long enough. This keeps transit segments from being marked clean when a Home Assistant dashboard launches a selected-room run.

Binary sensors are read-only in Home Assistant, so the pause control is exposed as both a toggleable pause switch and a read-only pause binary sensor.

## Testing

```powershell
scripts/test.ps1
```

On Linux:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q custom_components tests
```

The script disables globally installed pytest plugins because this package's tests are pure logic tests and the workstation's `pytest-socket` plugin blocks asyncio's Windows socketpair during plugin setup.
