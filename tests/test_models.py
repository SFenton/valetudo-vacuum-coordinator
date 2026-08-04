"""Tests for pure Valetudo Vacuum Coordinator scheduling logic."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "custom_components" / "valetudo_vacuum_coordinator"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


package = types.ModuleType("valetudo_vacuum_coordinator_test")
package.__path__ = [str(PACKAGE)]
sys.modules[package.__name__] = package
const = load_module(f"{package.__name__}.const", PACKAGE / "const.py")
logic = load_module(f"{package.__name__}.logic", PACKAGE / "logic.py")


def test_pick_next_room_prefers_oldest_success():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]
    ledgers = {
        "room_one": logic.RoomLedger(last_successful_clean="2026-05-17T10:00:00+00:00"),
        "room_two": logic.RoomLedger(last_successful_clean="2026-05-10T10:00:00+00:00"),
    }

    selection, skipped = logic.select_next_room(
        rooms, ledgers, set(), logic.ResourceState(), False
    )

    assert selection is not None
    assert selection.room.room_id == "room_two"
    assert selection.vacuum_only is True
    assert skipped == []


def test_pick_next_room_skips_attempted_rooms():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]

    selection, _skipped = logic.select_next_room(
        rooms, {}, {"room_one"}, logic.ResourceState(), False
    )

    assert selection is not None
    assert selection.room.room_id == "room_two"


def test_pick_next_room_skips_rooms_auto_cleaned_today():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]
    ledgers = {
        "room_one": logic.RoomLedger(
            last_auto_cleaned="2026-06-05T15:00:00+00:00",
            last_auto_cleaned_day="2026-06-05",
        )
    }

    selection, skipped = logic.select_next_room(
        rooms,
        ledgers,
        set(),
        logic.ResourceState(),
        False,
        auto_clean_day="2026-06-05",
    )

    assert selection is not None
    assert selection.room.room_id == "room_two"
    assert skipped == []


def test_pick_next_room_allows_previous_day_auto_clean_in_new_session():
    room = logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1")
    ledgers = {
        "room_one": logic.RoomLedger(
            last_auto_cleaned="2026-06-04T23:30:00+00:00",
            last_auto_cleaned_day="2026-06-04",
        )
    }

    selection, skipped = logic.select_next_room(
        [room],
        ledgers,
        set(),
        logic.ResourceState(),
        False,
        auto_clean_day="2026-06-05",
    )

    assert selection is not None
    assert selection.room.room_id == "room_one"
    assert skipped == []


def test_pick_next_room_cross_midnight_session_keeps_attempted_rooms_consumed():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]
    ledgers = {
        "room_one": logic.RoomLedger(
            last_auto_cleaned="2026-06-04T23:30:00+00:00",
            last_auto_cleaned_day="2026-06-04",
        )
    }

    selection, skipped = logic.select_next_room(
        rooms,
        ledgers,
        {"room_one"},
        logic.ResourceState(),
        False,
        auto_clean_day="2026-06-05",
    )

    assert selection is not None
    assert selection.room.room_id == "room_two"
    assert skipped == []


def test_mop_resource_blocks_mop_room():
    room = logic.RoomConfig(
        room_id="room_one", name="Room One", segment_id="1", mop_required=True
    )
    resources = logic.ResourceState(dirty_water="full")

    reason = logic.mop_block_reason(room, resources)

    assert reason == "dirty water is full"


def test_unknown_error_120_blocks_mop_room_but_allows_vacuum():
    """Dreame X40 error 120 is a recoverable mop-pad mounting issue.

    It must block mop-required rooms while still allowing plain vacuuming, and it
    must not trip the general cleaning block.
    """
    mop_room = logic.RoomConfig(
        room_id="room_one", name="Room One", segment_id="1", mop_required=True
    )
    resources = logic.ResourceState(error="Unknown error 120")

    assert logic.mop_block_reason(mop_room, resources) == "Unknown error 120"
    assert logic.cleaning_block_reason(resources) is None


def test_unknown_error_120_does_not_block_vacuum_only_room():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
    ]

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        set(),
        logic.ResourceState(error="Unknown error 120"),
        allow_vacuum_only_when_mop_blocked=False,
    )

    assert selection is not None
    assert selection.room.room_id == "room_one"
    assert selection.vacuum_only is True
    assert skipped == []


def test_mop_ready_room_uses_mop_mode():
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        mop_required=True,
    )

    selection, skipped = logic.select_next_room(
        [room], {}, set(), logic.ResourceState(), allow_vacuum_only_when_mop_blocked=False
    )

    assert selection is not None
    assert selection.vacuum_only is False
    assert skipped == []


def test_mop_attachment_sensor_off_does_not_preflight_block_mop_room():
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        mop_required=True,
    )

    selection, skipped = logic.select_next_room(
        [room],
        {},
        set(),
        logic.ResourceState(mop_attached=False),
        allow_vacuum_only_when_mop_blocked=False,
    )

    assert selection is not None
    assert selection.room.room_id == "room_one"
    assert selection.vacuum_only is False
    assert skipped == []


def test_mop_block_can_fall_back_to_vacuum_only():
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        mop_required=True,
    )
    resources = logic.ResourceState(fresh_water="empty")

    selection, skipped = logic.select_next_room(
        [room], {}, set(), resources, allow_vacuum_only_when_mop_blocked=True
    )

    assert selection is not None
    assert selection.vacuum_only is True
    assert selection.mop_block_reason == "fresh water is empty"
    assert skipped == []


def test_dustbag_error_blocks_all_pending_rooms():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        set(),
        logic.ResourceState(dustbag="full"),
        allow_vacuum_only_when_mop_blocked=False,
    )

    assert selection is None
    assert [(room.room_id, reason) for room, reason in skipped] == [
        ("room_one", "dustbag is full"),
        ("room_two", "dustbag is full"),
    ]


def test_recoverable_navigation_error_does_not_block_next_room():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        {"room_one"},
        logic.ResourceState(error="Cannot reach target"),
        allow_vacuum_only_when_mop_blocked=False,
    )

    assert selection is not None
    assert selection.room.room_id == "room_two"
    assert skipped == []


def test_unknown_error_95_is_recoverable_navigation_error():
    assert logic.is_recoverable_navigation_error("Unknown error 95") is True
    assert logic.cleaning_block_reason(logic.ResourceState(error="Unknown error 95")) is None


def test_dock_navigation_error_is_not_recoverable_navigation_error():
    assert logic.is_recoverable_navigation_error("Cannot navigate to the dock") is False


def test_retry_rooms_are_selected_after_unattempted_rooms():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        {"room_one"},
        logic.ResourceState(),
        allow_vacuum_only_when_mop_blocked=False,
        retry_room_ids=["room_one"],
    )

    assert selection is not None
    assert selection.room.room_id == "room_two"
    assert skipped == []

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        {"room_one", "room_two"},
        logic.ResourceState(),
        allow_vacuum_only_when_mop_blocked=False,
        retry_room_ids=["room_one"],
    )

    assert selection is not None
    assert selection.room.room_id == "room_one"
    assert skipped == []


def test_priority_retry_rooms_are_selected_before_unattempted_rooms():
    rooms = [
        logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1"),
        logic.RoomConfig(room_id="room_two", name="Room Two", segment_id="2"),
    ]

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        {"room_one"},
        logic.ResourceState(),
        allow_vacuum_only_when_mop_blocked=False,
        priority_retry_room_ids=["room_one"],
    )

    assert selection is not None
    assert selection.room.room_id == "room_one"
    assert skipped == []


def test_session_recovery_queues_retry_without_clearing_failure():
    session = logic.SessionState(session_id="session", started_at=logic.utcnow_iso())
    session.mark_failed("room_one", "Unknown error 95")
    session.begin_recovering("room_one", "Unknown error 95")

    session.resolve_recoverable_failure("room_one")

    assert session.failed_room_ids == ["room_one"]
    assert session.failed_room_reasons == {"room_one": "Unknown error 95"}
    assert session.pending_recovery_room_id is None
    assert session.pending_recovery_reason is None
    assert session.retry_room_ids == ["room_one"]

    session.mark_retry_started("room_one")
    session.clear_room_issue("room_one")

    assert session.can_retry_room("room_one") is False
    assert session.retry_room_ids == []
    assert session.retried_room_ids == ["room_one"]
    assert session.failed_room_ids == []


def test_session_priority_recovery_round_trips_and_queues_first():
    session = logic.SessionState(session_id="session", started_at=logic.utcnow_iso())
    session.mark_failed("room_one", "Low battery")
    session.begin_recovering("room_one", "Low battery", priority=True)

    restored = logic.SessionState.from_dict(session.to_dict())

    assert restored is not None
    assert restored.pending_recovery_room_id == "room_one"
    assert restored.pending_recovery_priority is True

    restored.resolve_recoverable_failure("room_one")

    assert restored.failed_room_ids == ["room_one"]
    assert restored.pending_recovery_room_id is None
    assert restored.pending_recovery_priority is False
    assert restored.priority_retry_room_ids == ["room_one"]

    restored.mark_retry_started("room_one")
    restored.clear_room_issue("room_one")

    assert restored.priority_retry_room_ids == []
    assert restored.retried_room_ids == ["room_one"]
    assert restored.failed_room_ids == []


def test_low_battery_error_is_resumable_interruption():
    assert logic.is_low_battery_error("Low battery") is True
    assert logic.is_low_battery_error("Battery low") is True
    assert logic.is_low_battery_error("No error") is False


def test_intervention_navigation_error_blocks_pending_rooms():
    rooms = [logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1")]

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        set(),
        logic.ResourceState(error="Robot is stuck"),
        allow_vacuum_only_when_mop_blocked=False,
    )

    assert selection is None
    assert [(room.room_id, reason) for room, reason in skipped] == [("room_one", "Robot is stuck")]


def test_dock_navigation_error_blocks_pending_rooms():
    rooms = [logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1")]

    selection, skipped = logic.select_next_room(
        rooms,
        {},
        set(),
        logic.ResourceState(error="Cannot navigate to the dock"),
        allow_vacuum_only_when_mop_blocked=False,
    )

    assert selection is None
    assert [(room.room_id, reason) for room, reason in skipped] == [
        ("room_one", "Cannot navigate to the dock")
    ]


def test_run_success_rejects_resumable_docked_state():
    room = logic.RoomConfig(room_id="room_one", name="Room One", segment_id="1")
    run = logic.ActiveRun(
        room_id="room_one", segment_id="1", session_id="session", started_at=logic.utcnow_iso()
    )
    run.observed_cleaning = True
    run.observed_segment_cleaning = True
    run.cancelled = True

    ok, reason = logic.evaluate_run_success(
        room,
        run,
        end_area=5000,
        end_time=600,
        error="No error",
    )

    assert ok is False
    assert reason == "Run was cancelled"


def test_run_success_requires_thresholds():
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        min_duration=120,
        min_area=1000,
    )
    run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        start_time=10,
        start_area=100,
    )
    run.observed_cleaning = True
    run.observed_segment_cleaning = True

    ok, reason = logic.evaluate_run_success(
        room,
        run,
        end_area=500,
        end_time=200,
        error="No error",
    )

    assert ok is False
    assert reason == "Cleaned area 400.0, below 1000.0 threshold"


def test_run_success_accepts_completed_segment_run():
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        min_duration=120,
        min_area=1000,
    )
    run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        start_time=10,
        start_area=100,
    )
    run.observed_cleaning = True
    run.observed_segment_cleaning = True

    ok, reason = logic.evaluate_run_success(
        room,
        run,
        end_area=1500,
        end_time=200,
        error="No error",
    )

    assert ok is True
    assert reason is None


def test_run_success_accepts_reset_current_statistics():
    room = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="1",
        min_duration=120,
        min_area=1000,
    )
    run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        start_time=900,
        start_area=10000,
    )
    run.observed_cleaning = True
    run.observed_segment_cleaning = True

    ok, reason = logic.evaluate_run_success(
        room,
        run,
        end_area=7750,
        end_time=360,
        error="No error",
    )

    assert ok is True
    assert reason is None


def test_manual_rooms_to_credit_requires_estimated_dwell():
    room_one = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="9",
        min_estimated_dwell=30,
    )
    room_two = logic.RoomConfig(
        room_id="room_two",
        name="Room Two",
        segment_id="1",
        min_estimated_dwell=30,
    )
    run = logic.ActiveRun(
        room_id=None,
        segment_id=None,
        session_id=None,
        started_at=logic.utcnow_iso(),
        manual=True,
    )
    run.estimated_dwell_seconds = {"room_one": 45, "room_two": 10}

    credited = logic.manual_rooms_to_credit([room_one, room_two], run)

    assert [room.room_id for room in credited] == ["room_one"]


def test_manual_rooms_to_credit_respects_selected_room_snapshot():
    room_one = logic.RoomConfig(
        room_id="room_one",
        name="Room One",
        segment_id="9",
        min_estimated_dwell=30,
    )
    room_two = logic.RoomConfig(
        room_id="room_two",
        name="Room Two",
        segment_id="1",
        min_estimated_dwell=30,
    )
    run = logic.ActiveRun(
        room_id=None,
        segment_id=None,
        session_id=None,
        started_at=logic.utcnow_iso(),
        manual=True,
        manual_credit_room_ids=["room_two"],
    )
    run.estimated_dwell_seconds = {"room_one": 45, "room_two": 45}

    credited = logic.manual_rooms_to_credit([room_one, room_two], run)

    assert [room.room_id for room in credited] == ["room_two"]


def test_v012_active_run_migrates_with_passive_resume_defaults():
    pending = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at=logic.utcnow_iso(),
        command_published=False,
        requested_iterations=3,
    )

    restored_pending = logic.ActiveRun.from_dict(pending.to_dict())
    legacy = pending.to_dict()
    for field_name in (
        "command_published",
        "phase",
        "suspended_at",
        "suspend_reason",
        "resumable_latched",
        "resumed_after_suspend",
        "resume_source",
        "docked_at",
        "interruption_count",
        "requested_iterations",
        "recovery_deadline",
        "resume_required",
        "post_suspend_cleaning_observed",
        "post_suspend_segment_observed",
        "accumulated_area",
        "accumulated_time",
        "last_area",
        "last_time",
        "cancel_requested_at",
        "cancel_reason",
        "cancel_stop_attempted",
    ):
        legacy.pop(field_name)
    restored_legacy = logic.ActiveRun.from_dict(legacy)

    assert restored_pending is not None
    assert restored_pending.command_published is False
    assert restored_pending.requested_iterations == 3
    assert restored_legacy is not None
    assert restored_legacy.command_published is True
    assert restored_legacy.phase == logic.RUN_PHASE_DISPATCHING
    assert restored_legacy.suspended_at is None
    assert restored_legacy.resumable_latched is False
    assert restored_legacy.resumed_after_suspend is False
    assert restored_legacy.interruption_count == 0
    assert restored_legacy.requested_iterations == 2
    assert restored_legacy.recovery_deadline is None
    assert restored_legacy.accumulated_area == 0
    assert restored_legacy.accumulated_time == 0


def test_active_run_native_resume_metadata_round_trips():
    run = logic.ActiveRun(
        room_id="room_one",
        segment_id="1",
        session_id="session",
        started_at="2026-08-04T12:00:00+00:00",
        command_published=True,
        phase=logic.RUN_PHASE_SUSPENDED,
        suspended_at="2026-08-04T12:10:00+00:00",
        suspend_reason="Low battery",
        resumable_latched=True,
        resumed_after_suspend=False,
        docked_at="2026-08-04T12:12:00+00:00",
        interruption_count=2,
        requested_iterations=3,
        recovery_deadline="2026-08-04T15:10:00+00:00",
        resume_required=True,
        accumulated_area=12.5,
        accumulated_time=610,
        last_area=12.5,
        last_time=610,
        cancel_continue_session=True,
    )

    restored = logic.ActiveRun.from_dict(run.to_dict())

    assert restored == run


def test_mark_success_updates_attempted_and_counts():
    ledger = logic.RoomLedger()

    logic.mark_success(ledger, "2026-05-19T12:00:00+00:00", mop=True)

    assert ledger.last_attempted == "2026-05-19T12:00:00+00:00"
    assert ledger.last_successful_clean == "2026-05-19T12:00:00+00:00"
    assert ledger.last_vacuumed == "2026-05-19T12:00:00+00:00"
    assert ledger.last_mopped == "2026-05-19T12:00:00+00:00"
    assert ledger.last_auto_cleaned is None
    assert ledger.last_auto_cleaned_day is None
    assert ledger.successful_count == 1


def test_mark_success_records_auto_clean_day_only_for_auto_clean():
    ledger = logic.RoomLedger()

    logic.mark_success(
        ledger,
        "2026-06-06T01:15:00+00:00",
        mop=False,
        auto_clean=True,
        auto_clean_day="2026-06-05",
    )

    assert ledger.last_auto_cleaned == "2026-06-06T01:15:00+00:00"
    assert ledger.last_auto_cleaned_day == "2026-06-05"


def test_while_away_outcome_round_trips():
    outcome = logic.WhileAwayOutcome(
        day="2026-06-05",
        room_id="room_one",
        kind="failed",
        reason="Cannot reach target",
    )

    restored = logic.WhileAwayOutcome.from_dict(outcome.to_dict())

    assert restored == outcome


def test_while_away_messages_filter_to_requested_day():
    outcomes = [
        logic.WhileAwayOutcome(day="2026-06-04", room_id="room_one", kind="cleaned"),
        logic.WhileAwayOutcome(day="2026-06-05", room_id="room_two", kind="cleaned"),
        logic.WhileAwayOutcome(
            day="2026-06-05",
            room_id="room_three",
            kind="skipped",
            reason="Mop attachment is missing",
        ),
        logic.WhileAwayOutcome(
            day="2026-06-04",
            room_id="room_four",
            kind="failed",
            reason="Cannot reach target",
        ),
    ]

    cleaned, issues = logic.build_while_away_messages(
        outcomes,
        {
            "room_one": "Yesterday Room",
            "room_two": "Today Room",
            "room_three": "Today Mop Room",
            "room_four": "Yesterday Failed Room",
        },
        "2026-06-05",
    )

    assert cleaned == ["Cleaned Today Room"]
    assert issues == ["Could not mop Today Mop Room because the mop attachment was not detected"]


def test_while_away_messages_treat_cross_midnight_completion_as_today():
    outcomes = [
        logic.WhileAwayOutcome(day="2026-06-04", room_id="room_one", kind="cleaned"),
        logic.WhileAwayOutcome(day="2026-06-05", room_id="room_two", kind="cleaned"),
    ]

    cleaned, issues = logic.build_while_away_messages(
        outcomes,
        {"room_one": "Before Midnight", "room_two": "After Midnight"},
        "2026-06-05",
    )

    assert cleaned == ["Cleaned After Midnight"]
    assert issues == []


def test_while_away_messages_drop_room_issue_after_later_clean():
    outcomes = [
        logic.WhileAwayOutcome(
            day="2026-06-05",
            room_id="room_one",
            kind="failed",
            reason="Unknown error 95",
        ),
        logic.WhileAwayOutcome(day="2026-06-05", room_id="room_one", kind="cleaned"),
    ]

    cleaned, issues = logic.build_while_away_messages(
        outcomes,
        {"room_one": "Living Room"},
        "2026-06-05",
    )

    assert cleaned == ["Cleaned Living Room"]
    assert issues == []


def test_auto_clean_settings_snapshot_round_trips():
    snapshot = logic.AutoCleanSettingsSnapshot(
        mode="vacuum_then_mop",
        fan="turbo",
        water="medium",
        passes="1",
    )

    restored = logic.AutoCleanSettingsSnapshot.from_dict(snapshot.to_dict())

    assert restored == snapshot


def test_auto_clean_summary_skips_return_home_without_completed_rooms():
    summary = logic.build_auto_clean_summary(
        vacuum_name="Main Floor Vacuum",
        completed_room_names=[],
        skipped_room_reasons={},
        failed_room_reasons={},
        terminal_reason="returned_home",
    )

    assert summary is None


def test_auto_clean_summary_reports_partial_success_and_skips():
    summary = logic.build_auto_clean_summary(
        vacuum_name="Main Floor Vacuum",
        completed_room_names=["Guest Bathroom", "Dining Room", "Hallway"],
        skipped_room_reasons={"Kitchen": "Mop Dock Clean Water Tank empty"},
        failed_room_reasons={"Guest Room": "Cannot reach target"},
        terminal_reason="complete",
    )

    assert summary is not None
    assert summary.title == "Main Floor Vacuum · Auto-Cleaned 3 Rooms"
    assert summary.message == (
        "Main Floor Vacuum cleaned 3 rooms while everyone was away. "
        "While everyone was away, the vacuum ran into some errors."
    )


def test_auto_clean_summary_reports_blocked_before_start():
    summary = logic.build_auto_clean_summary(
        vacuum_name="Main Floor Vacuum",
        completed_room_names=[],
        skipped_room_reasons={},
        failed_room_reasons={},
        terminal_reason="blocked",
        terminal_message="Mop Dock Clean Water Tank empty",
    )

    assert summary is not None
    assert summary.title == "Main Floor Vacuum · Auto-Clean Blocked"
    assert summary.message == "Could not start: the clean water tank is empty."


def test_auto_clean_summary_reports_needs_help():
    summary = logic.build_auto_clean_summary(
        vacuum_name="Main Floor Vacuum",
        completed_room_names=["Guest Bathroom", "Dining Room"],
        skipped_room_reasons={},
        failed_room_reasons={},
        terminal_reason="needs_help",
        terminal_message="Cannot navigate to the dock",
        needs_help=True,
    )

    assert summary is not None
    assert summary.title == "Main Floor Vacuum · Needs Help"
    assert summary.message == (
        "Main Floor Vacuum cleaned 2 rooms while everyone was away. "
        "While everyone was away, the vacuum ran into some errors."
    )


def test_auto_clean_summary_reports_return_home_compactly():
    summary = logic.build_auto_clean_summary(
        vacuum_name="Main Floor Vacuum",
        completed_room_names=["Guest Room"],
        skipped_room_reasons={"Master Bathroom": "Mop attachment is missing"},
        failed_room_reasons={
            "Gym": "Cleaned for 60s, below 120s threshold",
            "Master Bedroom": "Tracked person arrived home",
        },
        terminal_reason="returned_home",
    )

    assert summary is not None
    assert summary.title == "Main Floor Vacuum · Stopped Early"
    assert summary.message == (
        "Main Floor Vacuum cleaned Guest Room while everyone was away. "
        "While everyone was away, the vacuum ran into some errors."
    )


def test_no_selection_terminal_reason_distinguishes_exhausted_from_failed():
    assert (
        logic.no_selection_terminal_reason(
            completed_room_ids=[],
            skipped_room_ids=[],
            failed_room_ids=[],
            current_skipped_count=0,
        )
        == "complete"
    )
    assert (
        logic.no_selection_terminal_reason(
            completed_room_ids=[],
            skipped_room_ids=[],
            failed_room_ids=["room_one"],
            current_skipped_count=0,
        )
        == "blocked"
    )


def test_schedule_hass_task_prefers_thread_safe_create_task():
    class FakeHass:
        def __init__(self):
            self.created = []
            self.async_created = []

        def create_task(self, coroutine):
            self.created.append(coroutine)

        def async_create_task(self, coroutine):
            self.async_created.append(coroutine)

    coroutine = object()
    hass = FakeHass()

    logic.schedule_hass_task(hass, coroutine)

    assert hass.created == [coroutine]
    assert hass.async_created == []


def test_schedule_hass_task_falls_back_to_async_create_task():
    class FakeHass:
        def __init__(self):
            self.async_created = []

        def async_create_task(self, coroutine):
            self.async_created.append(coroutine)

    coroutine = object()
    hass = FakeHass()

    logic.schedule_hass_task(hass, coroutine)

    assert hass.async_created == [coroutine]


def test_while_away_issue_messages_are_detailed_but_compact():
    messages = logic.build_issue_messages(
        {"Master Bathroom": "Mop attachment is missing"},
        {
            "Gym": "Cleaned for 60s, below 120s threshold",
            "Master Bedroom": "Tracked person arrived home",
        },
    )

    assert messages == [
        "Could not mop Master Bathroom because the mop attachment was not detected",
        "Could not clean Gym because it only ran 60s",
    ]


def test_session_state_round_trips_terminal_details():
    session = logic.SessionState(
        session_id="session",
        started_at="2026-05-20T10:00:00+00:00",
        active=False,
        terminal_reason="complete",
        notification_sent=True,
        native_resume_guard_latched=True,
        native_guard_cancel_pending=True,
        native_guard_stop_confirmed=True,
        native_guard_return_confirmed=False,
        native_guard_cancel_reason="test cancel",
    )
    session.mark_completed("room_one")
    session.mark_skipped("room_two", "clean water empty")
    session.mark_failed("room_three", "Cannot reach target")

    restored = logic.SessionState.from_dict(session.to_dict())

    assert restored is not None
    assert restored.completed_room_ids == ["room_one"]
    assert restored.skipped_room_reasons == {"room_two": "clean water empty"}
    assert restored.failed_room_reasons == {"room_three": "Cannot reach target"}
    assert restored.terminal_reason == "complete"
    assert restored.notification_sent is True
    assert restored.native_resume_guard_latched is True
    assert restored.native_guard_cancel_pending is True
    assert restored.native_guard_stop_confirmed is True
    assert restored.native_guard_return_confirmed is False
    assert restored.native_guard_cancel_reason == "test cancel"
