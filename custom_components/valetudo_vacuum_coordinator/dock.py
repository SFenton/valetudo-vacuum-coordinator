"""Safe Valetudo dock action helpers."""

from __future__ import annotations

import re


DOCK_ACTION_CAPABILITIES = {
    "clean": "MopDockCleanManualTriggerCapability",
    "dry": "MopDockDryManualTriggerCapability",
}
DOCK_ACTIONS = {"start", "stop"}
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9]+$")


def build_dock_action_url(identifier: str, capability: str) -> str:
    """Build a constrained local Valetudo capability URL."""
    if not _IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError("identifier must contain only letters and numbers")
    capability_name = DOCK_ACTION_CAPABILITIES.get(capability)
    if capability_name is None:
        raise ValueError(f"unsupported dock capability: {capability}")
    return (
        f"http://valetudo-{identifier.lower()}.local"
        f"/api/v2/robot/capabilities/{capability_name}"
    )
