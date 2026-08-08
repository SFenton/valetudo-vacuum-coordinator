"""Tests for Valetudo dock helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DOCK_MODULE = (
    ROOT
    / "custom_components"
    / "valetudo_vacuum_coordinator"
    / "dock.py"
)
SPEC = importlib.util.spec_from_file_location("valetudo_dock_test", DOCK_MODULE)
assert SPEC is not None and SPEC.loader is not None
dock = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dock)


def test_builds_clean_and_dry_urls() -> None:
    """Build only the supported local capability URLs."""
    assert dock.build_dock_action_url("ExaltedSneakyDeer", "clean") == (
        "http://valetudo-exaltedsneakydeer.local/api/v2/robot/capabilities/"
        "MopDockCleanManualTriggerCapability"
    )
    assert dock.build_dock_action_url("PoliteFatherlyKingfisher", "dry") == (
        "http://valetudo-politefatherlykingfisher.local/api/v2/robot/capabilities/"
        "MopDockDryManualTriggerCapability"
    )


@pytest.mark.parametrize(
    ("identifier", "capability"),
    [
        ("../internal", "clean"),
        ("robot.local", "clean"),
        ("ExaltedSneakyDeer", "empty"),
    ],
)
def test_rejects_unconstrained_targets(
    identifier: str,
    capability: str,
) -> None:
    """Reject identifiers and capabilities outside the fixed allowlist."""
    with pytest.raises(ValueError):
        dock.build_dock_action_url(identifier, capability)
