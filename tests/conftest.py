"""Test configuration for Valetudo Vacuum Coordinator."""

from __future__ import annotations

import asyncio
import sys


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
