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
        self._idle_timeout_seconds = 20
        self._connect_timeout_seconds = 20

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
        max_reconnect_delay = 45
        consecutive_failures = 0
        max_consecutive_failures = 10
        
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    OKX_PUBLIC_WS,
                    # Handle ping/idle manually to tolerate quiet periods.
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=10,  # Increased from 5 to allow more time for cleanup
                    open_timeout=self._connect_timeout_seconds,
                    max_queue=16,  # Increased from 8 to handle more backpressure
                ) as ws:
                    reconnect_delay = 5  # reset after successful connect
                    consecutive_failures = 0  # reset failure counter
                    logger.info("%s websocket connection established successfully", self.__class__.__name__)
                    await self._subscribe(ws)
                    await self._listen(ws)
            except asyncio.CancelledError:
                logger.info("%s websocket task cancelled gracefully", self.__class__.__name__)
                break
            except asyncio.TimeoutError:
                consecutive_failures += 1
                logger.warning(
                    "%s websocket handshake timed out after %ss; retrying (%d/%d)",
                    self.__class__.__name__,
                    self._connect_timeout_seconds,
                    consecutive_failures,
                    max_consecutive_failures,
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)
            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                consecutive_failures += 1
                # Special handling for connection reset errors
                if "10054" in str(exc) or "connection reset" in str(exc).lower():
                    logger.warning(
                        "%s websocket connection reset by peer (WinError 10054); reconnecting in %ss (%d/%d)",
                        self.__class__.__name__,
                        reconnect_delay,
                        consecutive_failures,
                        max_consecutive_failures,
                    )
                else:
                    logger.warning(
                        "%s websocket closed (%s); reconnecting in %ss (%d/%d)",
                        self.__class__.__name__,
                        exc,
                        reconnect_delay,
                        consecutive_failures,
                        max_consecutive_failures,
                    )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)
            except OSError as exc:
                consecutive_failures += 1
                # Special handling for WinError 10054 and similar connection errors
                if "10054" in str(exc) or "connection reset" in str(exc).lower():
                    logger.warning(
                        "%s encountered connection error: %s; reconnecting in %ss (%d/%d)",
                        self.__class__.__name__,
                        exc,
                        reconnect_delay,
                        consecutive_failures,
                        max_consecutive_failures,
                    )
                else:
                    logger.error(
                        "%s encountered OS error: %s; reconnecting in %ss (%d/%d)",
                        self.__class__.__name__,
                        exc,
                        reconnect_delay,
                        consecutive_failures,
                        max_consecutive_failures,
                    )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)
            except Exception as exc:
                consecutive_failures += 1
                logger.error(
                    "%s encountered unexpected error: %s; reconnecting in %ss (%d/%d)",
                    self.__class__.__name__,
                    exc,
                    reconnect_delay,
                    consecutive_failures,
                    max_consecutive_failures,
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)
                
            # Implement backoff with jitter to avoid thundering herd problem
            if consecutive_failures >= max_consecutive_failures:
                logger.warning(
                    "%s reached maximum consecutive failures (%d); pausing for extended time",
                    self.__class__.__name__,
                    max_consecutive_failures,
                )
                await asyncio.sleep(60)  # Longer pause after too many failures
                consecutive_failures = 0  # Reset counter after extended pause

    async def _listen(self, ws: WebSocketClientProtocol) -> None:
        loop = asyncio.get_running_loop()
        last_message_at = loop.time()
        last_ping_at = loop.time()
        ping_response_received = True
        ping_interval_adjusted = self._ping_interval_seconds
        
        async def _keepalive() -> None:
            nonlocal last_message_at, last_ping_at, ping_response_received, ping_interval_adjusted
            
            while not self._stop_event.is_set():
                await asyncio.sleep(min(ping_interval_adjusted, 20))  # Cap at 20s for safety
                
                if self._stop_event.is_set():
                    break
                
                current_time = loop.time()
                idle = current_time - last_message_at
                time_since_last_ping = current_time - last_ping_at
                
                try:
                    # Dynamic ping interval adjustment based on connection stability
                    if not ping_response_received and time_since_last_ping > ping_interval_adjusted:
                        # If we didn't receive a response to previous ping, reduce interval
                        ping_interval_adjusted = max(10, ping_interval_adjusted - 2)
                        logger.debug(
                            "%s adjusting ping interval to %ss due to missed pong",
                            self.__class__.__name__,
                            ping_interval_adjusted,
                        )
                    
                    # OKX closes connections that do not send data for 30s.
                    # Use standard websocket ping frame instead of custom JSON ping
                    # to comply with OKX API requirements
                    await ws.ping()
                    last_ping_at = current_time
                    ping_response_received = False  # Reset flag, will be set when pong is received
                    
                    if idle >= self._idle_timeout_seconds:
                        logger.debug(
                            "%s idle for %.0fs; ping sent to keep connection alive",
                            self.__class__.__name__,
                            idle,
                        )
                except Exception as exc:
                    logger.warning("%s keepalive ping failed: %s", self.__class__.__name__, exc)
                    break
        
        keepalive_task = asyncio.create_task(_keepalive(), name=f"{self.__class__.__name__}-keepalive")
        
        try:
            async for message in ws:
                if self._stop_event.is_set():
                    break
                
                last_message_at = loop.time()
                
                try:
                    payload = json.loads(message)
                    
                    # Handle pong responses specifically
                    if isinstance(payload, dict) and payload.get("event") == "pong":
                        ping_response_received = True
                        # If we got a pong, we can slightly increase ping interval if needed
                        if ping_interval_adjusted < self._ping_interval_seconds:
                            ping_interval_adjusted = min(self._ping_interval_seconds, ping_interval_adjusted + 1)
                        continue
                    
                    if "event" in payload:
                        # Log subscription events or errors
                        event_type = payload.get("event")
                        if event_type == "error":
                            logger.error("%s received error event: %s", self.__class__.__name__, payload)
                        elif event_type != "subscribe":
                            logger.debug("%s received event: %s", self.__class__.__name__, event_type)
                        continue
                    
                    await self._handle_payload(payload)
                except json.JSONDecodeError as exc:
                    logger.warning("%s failed to decode message: %s", self.__class__.__name__, exc)
                    continue
                except Exception as exc:
                    logger.error("%s error handling message: %s", self.__class__.__name__, exc)
                    # Continue processing other messages even if one fails
                    continue
        finally:
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass
            logger.debug("%s listener task completed", self.__class__.__name__)

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


class MarketDepthStream(BaseOkxStream):
    """Streams multiple market depth channels (bbo-tbt, books5, etc.) and calculates net depth."""

    def __init__(self, instruments: Iterable[str], channels: Iterable[str] = None) -> None:
        super().__init__(instruments)
        # Default channels if none provided
        self._channels = channels or ["bbo-tbt", "books5"]
        # Store latest depth data for net depth calculation
        self._latest_depth_data = {}

    def set_instruments(self, instruments: Iterable[str]) -> None:
        super().set_instruments(instruments)
        # Clear old data when instruments change
        self._latest_depth_data.clear()

    async def _subscribe(self, ws: WebSocketClientProtocol) -> None:
        args = []
        for instrument in sorted(self._instruments):
            for channel in self._channels:
                # For books and books5 channels, we need to specify the channel name only
                # For other channels like books-l2-tbt, use the channel name directly
                if channel.startswith("books") and channel != "books-l2-tbt" and channel != "books50-l2-tbt":
                    arg = {"channel": channel, "instId": instrument}
                    # Add depth parameter for books channel
                    if channel == "books":
                        arg["depth"] = "5"
                    args.append(arg)
                else:
                    args.append({"channel": channel, "instId": instrument})
        
        if not args:
            return
            
        await ws.send(json.dumps({"op": "subscribe", "args": args}))
        logger.info("Subscribed to OKX market depth channels %s for %d instruments.", 
                    ", ".join(self._channels), len(self._instruments))

    async def _handle_payload(self, payload: dict) -> None:
        arg = payload.get("arg", {})
        channel = arg.get("channel")
        inst_id = (arg.get("instId") or "").upper()
        
        if not channel or not inst_id or (self._instruments and inst_id not in self._instruments):
            return
            
        data = payload.get("data") or []
        if not data:
            return
            
        # Process based on channel type
        if channel in ["bbo-tbt", "books5", "books", "books-l2-tbt", "books50-l2-tbt"]:
            await self._process_depth_data(inst_id, channel, data)
        
    async def _process_depth_data(self, inst_id: str, channel: str, data: list) -> None:
        # Store the latest depth data
        if inst_id not in self._latest_depth_data:
            self._latest_depth_data[inst_id] = {}
            
        # Process and store data based on channel
        for entry in data:
            if channel == "bbo-tbt":
                # Store latest BBO (Best Bid Offer) data
                self._latest_depth_data[inst_id]["bbo"] = {
                    "bid_px": float(entry.get("bidPx", 0)),
                    "bid_sz": float(entry.get("bidSz", 0)),
                    "ask_px": float(entry.get("askPx", 0)),
                    "ask_sz": float(entry.get("askSz", 0)),
                    "ts": _parse_timestamp_ms(entry.get("ts"))
                }
            elif channel in ["books5", "books", "books-l2-tbt", "books50-l2-tbt"]:
                # Store order book levels
                limit = 5 if channel == "books5" else 50
                self._latest_depth_data[inst_id][channel] = {
                    "bids": _parse_book_levels(entry.get("bids", []), limit=limit),
                    "asks": _parse_book_levels(entry.get("asks", []), limit=limit),
                    "ts": _parse_timestamp_ms(entry.get("ts"))
                }
        
        # Calculate and store net depth when we have enough data
        await self._calculate_net_depth(inst_id)
    
    async def _calculate_net_depth(self, inst_id: str) -> None:
        # Get the latest order book data
        book_data = None
        for channel in ["books5", "books", "books-l2-tbt", "books50-l2-tbt"]:
            if channel in self._latest_depth_data[inst_id]:
                book_data = self._latest_depth_data[inst_id][channel]
                break
        
        if not book_data:
            return
            
        bids = book_data.get("bids", [])
        asks = book_data.get("asks", [])
        
        if not bids or not asks:
            return
            
        # Calculate cumulative volumes
        cumulative_bids = []
        cumulative_asks = []
        
        total_bid_volume = 0
        for bid in bids:
            total_bid_volume += bid[1]  # bid[1] is size/volume
            cumulative_bids.append([bid[0], total_bid_volume])  # [price, cumulative_volume]
        
        total_ask_volume = 0
        for ask in asks:
            total_ask_volume += ask[1]  # ask[1] is size/volume
            cumulative_asks.append([ask[0], total_ask_volume])  # [price, cumulative_volume]
        
        # Calculate net depth
        net_depth = []
        bid_idx = 0
        ask_idx = 0
        
        while bid_idx < len(cumulative_bids) and ask_idx < len(cumulative_asks):
            bid_price, bid_volume = cumulative_bids[bid_idx]
            ask_price, ask_volume = cumulative_asks[ask_idx]
            
            # Find the price level to calculate net depth
            if bid_price >= ask_price:  # This shouldn't happen in a valid order book
                # Move to next level
                bid_idx += 1
                ask_idx += 1
            else:
                # Calculate price difference
                price_diff = abs(bid_price - ask_price) / ask_price * 100  # Percentage difference
                # Calculate net volume at this level
                net_volume = bid_volume - ask_volume
                net_depth.append([price_diff, net_volume])
                
                # Move to next level with the lower price
                if bid_price < ask_price:
                    bid_idx += 1
                else:
                    ask_idx += 1
        
        # Store net depth data
        if inst_id not in self._latest_depth_data:
            self._latest_depth_data[inst_id] = {}
            
        self._latest_depth_data[inst_id]["net_depth"] = {
            "data": net_depth,
            "ts": book_data.get("ts") or datetime.now(tz=timezone.utc)
        }
        
        # Write to Influx if writer is available
        try:
            writer = self.writer
            writer.write_market_depth(
                instrument_id=inst_id,
                timestamp=book_data.get("ts") or datetime.now(tz=timezone.utc),
                bids=bids,
                asks=asks,
                net_depth=net_depth
            )
        except Exception as e:
            logger.error("Error writing market depth data to Influx: %s", e)
            # Continue processing even if Influx write fails


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
