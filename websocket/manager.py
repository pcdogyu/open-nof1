"""
Lifecycle controls for OKX websocket data collectors.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable, Sequence, Set

from websocket.streams import BaseOkxStream, LiquidationStream, OrderbookDepthStream

logger = logging.getLogger(__name__)

_MANAGER_LOCK = asyncio.Lock()
_STREAM_MANAGER: "WebsocketManager | None" = None


class WebsocketManager:
    """Orchestrates lifecycle for all websocket-derived data collectors."""

    def __init__(self, instruments: Iterable[str]) -> None:
        self._instruments: Set[str] = {inst.strip().upper() for inst in instruments if inst and inst.strip()}
        self._streams: list[_BaseStreamAdapter] = []
        self._started = False

    def update_instruments(self, instruments: Iterable[str]) -> None:
        normalized = {inst.strip().upper() for inst in instruments if inst and inst.strip()}
        if not normalized:
            return
        self._instruments = normalized
        for stream in self._streams:
            stream.update_instruments(self._instruments)

    async def start(self) -> None:
        if self._started:
            return
        self._streams = [
            _BaseStreamAdapter(LiquidationStream(self._instruments)),
            _BaseStreamAdapter(OrderbookDepthStream(self._instruments)),
        ]
        for stream in self._streams:
            await stream.start()
        self._started = True
        logger.info("OKX websocket streams started with %d instruments.", len(self._instruments))

    async def stop(self) -> None:
        if not self._started:
            return
        for stream in self._streams:
            await stream.stop()
        self._streams.clear()
        self._started = False
        logger.info("OKX websocket streams stopped.")


class _BaseStreamAdapter:
    """Lightweight adapter layer to provide common interface for manager."""

    def __init__(self, stream: BaseOkxStream) -> None:
        self._stream = stream

    async def start(self) -> None:
        await self._stream.start()

    async def stop(self) -> None:
        await self._stream.stop()

    def update_instruments(self, instruments: Set[str]) -> None:
        self._stream.set_instruments(instruments)


async def start_streams(instruments: Sequence[str]) -> None:
    """Start websocket collectors if not already active."""
    global _STREAM_MANAGER
    async with _MANAGER_LOCK:
        if _STREAM_MANAGER is None:
            _STREAM_MANAGER = WebsocketManager(instruments)
        else:
            _STREAM_MANAGER.update_instruments(instruments)
        await _STREAM_MANAGER.start()


async def stop_streams() -> None:
    """Stop all websocket collectors."""
    global _STREAM_MANAGER
    async with _MANAGER_LOCK:
        if _STREAM_MANAGER is None:
            return
        await _STREAM_MANAGER.stop()
        _STREAM_MANAGER = None


def update_instruments(instruments: Iterable[str]) -> None:
    """Update instrument filter on active streams without restarting."""
    if _STREAM_MANAGER is None:
        return
    _STREAM_MANAGER.update_instruments(instruments)
