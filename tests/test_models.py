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
    assert selection.vacuum_only is False
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


def test_mop_resource_blocks_mop_room():
    room = logic.RoomConfig(
        room_id="room_one", name="Room One", segment_id="1", mop_required=True
    )
    resources = logic.ResourceState(dirty_water="full")

    reason = logic.mop_block_reason(room, resources)

    assert reason == "dirty water is full"


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


def test_mark_success_updates_attempted_and_counts():
    ledger = logic.RoomLedger()

    logic.mark_success(ledger, "2026-05-19T12:00:00+00:00", mop=True)

    assert ledger.last_attempted == "2026-05-19T12:00:00+00:00"
    assert ledger.last_successful_clean == "2026-05-19T12:00:00+00:00"
    assert ledger.last_vacuumed == "2026-05-19T12:00:00+00:00"
    assert ledger.last_mopped == "2026-05-19T12:00:00+00:00"
    assert ledger.successful_count == 1
