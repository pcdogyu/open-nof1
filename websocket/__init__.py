"""
Runtime management for OKX websocket data streams.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from websocket.manager import (
    start_streams as _start_streams,
    stop_streams as _stop_streams,
    update_instruments as _update_instruments,
)


async def start_streams(instruments: Sequence[str]) -> None:
    """Start websocket collectors for the provided instrument universe."""
    await _start_streams(instruments)


async def stop_streams() -> None:
    """Stop all running websocket collectors."""
    await _stop_streams()


def set_instruments(instruments: Iterable[str]) -> None:
    """Update the instrument filter used by websocket streams."""
    _update_instruments(instruments)
