"""
Concrete websocket stream consumers for OKX public market data.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Sequence, Set

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

try:  # websockets>=10
    from websockets.client import WebSocketClientProtocol
except ImportError:  # pragma: no cover - compatibility with older versions
    from websockets.legacy.client import WebSocketClientProtocol  # type: ignore

from data_pipeline.influx import InfluxConfig, InfluxWriter

OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"

logger = logging.getLogger(__name__)


class BaseOkxStream:
    """Base functionality shared by specific OKX websocket streams."""

    def __init__(self, instruments: Iterable[str]) -> None:
        self._instruments: Set[str] = {inst.strip().upper() for inst in instruments if inst and inst.strip()}
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._writer: InfluxWriter | None = None
        # Keepalive tuning to reduce noisy restarts when the upstream is quiet.
        self._ping_interval_seconds = 20
        self._idle_timeout_seconds = 60

    def set_instruments(self, instruments: Iterable[str]) -> None:
        updated = {inst.strip().upper() for inst in instruments if inst and inst.strip()}
        if updated:
            self._instruments = updated

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        try:
            config = InfluxConfig.from_env()
            self._writer = InfluxWriter(config)
        except Exception as exc:
            logger.error("Failed to initialise Influx writer for %s: %s", self.__class__.__name__, exc)
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_forever(), name=f"{self.__class__.__name__}-task")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        if self._writer:
            self._writer.close()
            self._writer = None

    async def _run_forever(self) -> None:
        reconnect_delay = 5
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    OKX_PUBLIC_WS,
                    # Handle ping/idle manually to tolerate quiet periods.
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    max_queue=8,
                ) as ws:
                    await self._subscribe(ws)
                    await self._listen(ws)
            except asyncio.CancelledError:
                break
            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning(
                    "%s websocket closed (%s); reconnecting in %ss",
                    self.__class__.__name__,
                    exc,
                    reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)
            except Exception as exc:
                logger.warning("%s encountered an error: %s", self.__class__.__name__, exc)
                await asyncio.sleep(reconnect_delay)

    async def _listen(self, ws: WebSocketClientProtocol) -> None:
        loop = asyncio.get_running_loop()
        last_message_at = loop.time()

        async def _keepalive() -> None:
            nonlocal last_message_at
            while not self._stop_event.is_set():
                await asyncio.sleep(self._ping_interval_seconds)
                if self._stop_event.is_set():
                    break
                try:
                    await ws.ping()
                except Exception:
                    break
                idle = loop.time() - last_message_at
                if idle >= self._idle_timeout_seconds:
                    logger.warning(
                        "%s idle for %.0fs; closing websocket to trigger reconnect",
                        self.__class__.__name__,
                        idle,
                    )
                    try:
                        await ws.close(code=4004, reason="idle timeout")
                    except Exception:
                        pass
                    break

        keepalive_task = asyncio.create_task(_keepalive(), name=f"{self.__class__.__name__}-keepalive")
        try:
            async for message in ws:
                if self._stop_event.is_set():
                    break
                last_message_at = loop.time()
                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    continue
                if "event" in payload:
                    continue  # subscription acknowledgement or error
                await self._handle_payload(payload)
        finally:
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass

    async def _handle_payload(self, payload: dict) -> None:
        raise NotImplementedError

    async def _subscribe(self, ws: WebSocketClientProtocol) -> None:
        raise NotImplementedError

    @property
    def writer(self) -> InfluxWriter:
        if self._writer is None:
            raise RuntimeError("Influx writer not initialised.")
        return self._writer


class LiquidationStream(BaseOkxStream):
    """Streams liquidation order data and persists aggregated metrics."""

    def __init__(self, instruments: Iterable[str]) -> None:
        super().__init__(instruments)

    async def _subscribe(self, ws: WebSocketClientProtocol) -> None:
        message = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "liquidation-orders",
                    "instType": "SWAP",
                }
            ],
        }
        await ws.send(json.dumps(message))
        logger.info("Subscribed to OKX liquidation stream for %s", ", ".join(sorted(self._instruments)))

    async def _handle_payload(self, payload: dict) -> None:
        data = payload.get("data", [])
        if not data:
            return
        grouped: dict[str, _LiquidationAggregate] = {}
        for entry in data:
            inst_id = (entry.get("instId") or "").upper()
            if self._instruments and inst_id not in self._instruments:
                continue
            details = entry.get("details") or []
            if not details:
                continue
            agg = grouped.setdefault(inst_id, _LiquidationAggregate(instrument_id=inst_id))
            for detail in details:
                agg.merge_entry(detail)
        writer = self.writer
        for agg in grouped.values():
            writer.write_liquidation(
                instrument_id=agg.instrument_id,
                timestamp=agg.timestamp or datetime.now(tz=timezone.utc),
                long_qty=agg.long_qty,
                short_qty=agg.short_qty,
                net_qty=agg.net_qty,
                price=agg.last_price,
            )


class OrderbookDepthStream(BaseOkxStream):
    """Streams deep order book updates and persists snapshots."""

    def __init__(self, instruments: Iterable[str]) -> None:
        super().__init__(instruments)

    async def _subscribe(self, ws: WebSocketClientProtocol) -> None:
        args = [
            {
                "channel": "books",
                "instId": instrument,
                "depth": "500",
            }
            for instrument in sorted(self._instruments)
        ]
        if not args:
            return
        await ws.send(json.dumps({"op": "subscribe", "args": args}))
        logger.info("Subscribed to OKX orderbook depth for %d instruments.", len(args))

    async def _handle_payload(self, payload: dict) -> None:
        arg = payload.get("arg", {})
        inst_id = (arg.get("instId") or "").upper()
        if not inst_id or (self._instruments and inst_id not in self._instruments):
            return
        data = payload.get("data") or []
        if not data:
            return
        snapshot = data[0]
        bids = _parse_book_levels(snapshot.get("bids", []), limit=500)
        asks = _parse_book_levels(snapshot.get("asks", []), limit=500)
        if not bids and not asks:
            return
        timestamp = _parse_timestamp_ms(snapshot.get("ts"))
        writer = self.writer
        writer.write_orderbook(
            instrument_id=inst_id,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )


@dataclass
class _LiquidationAggregate:
    instrument_id: str
    timestamp: datetime | None = None
    long_qty: float = 0.0
    short_qty: float = 0.0
    last_price: float | None = None

    def merge_entry(self, entry: dict) -> None:
        try:
            size = float(entry.get("sz") or 0)
        except (TypeError, ValueError):
            size = 0.0
        try:
            price = float(entry.get("bkPx") or entry.get("px") or 0)
        except (TypeError, ValueError):
            price = None
        if price:
            self.last_price = price
        ts_value = entry.get("ts")
        if ts_value:
            ts = _parse_timestamp_ms(ts_value)
            if self.timestamp is None or ts > self.timestamp:
                self.timestamp = ts
        pos_side = (entry.get("posSide") or "").lower()
        side = (entry.get("side") or "").lower()
        if pos_side == "long" or (pos_side == "" and side == "sell"):
            self.long_qty += size
        elif pos_side == "short" or (pos_side == "" and side == "buy"):
            self.short_qty += size

    @property
    def net_qty(self) -> float:
        return self.long_qty - self.short_qty


def _parse_timestamp_ms(value: object) -> datetime:
    try:
        ms = int(value)
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError):
        return datetime.now(tz=timezone.utc)


def _parse_book_levels(levels: Sequence[Sequence[object]], limit: int) -> List[List[float]]:
    parsed: List[List[float]] = []
    for idx, level in enumerate(levels):
        if idx >= limit:
            break
        try:
            price = float(level[0])
            size = float(level[1])
        except (TypeError, ValueError, IndexError):
            continue
        parsed.append([price, size])
    return parsed
