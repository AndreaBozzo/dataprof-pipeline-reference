"""Named failure points for recovery tests.

``PIPELINE_REFERENCE_FAULT`` selects one point. Crash points end the process
with ``os._exit`` so no ``finally`` block, context manager or atexit hook runs:
the same state a killed process leaves. The variable is read on every call so
tests can set it per subprocess.
"""

from __future__ import annotations

import os

VARIABLE = "PIPELINE_REFERENCE_FAULT"
CRASH_EXIT = 86


class InjectedFault(RuntimeError):
    """An exception raised on purpose at a named point."""


def active() -> str:
    return os.environ.get(VARIABLE, "")


def crash(point: str) -> None:
    """Kill the process if ``point`` is the selected crash point."""
    if active() == point:
        os._exit(CRASH_EXIT)


def raise_at(point: str) -> None:
    if active() == point:
        raise InjectedFault(f"injected fault at {point}")


def on_record(number: int) -> None:
    """Called for every source record; ``crash_during_load`` dies at record 100."""
    if number == 100:
        crash("crash_during_load")
