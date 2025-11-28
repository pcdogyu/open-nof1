"""
Market data collector responsible for fetching candles, order books, and funding rates.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Literal

import httpx

Timeframe = Literal["2m", "5m", "15m", "1H", "4H"]


@dataclass(slots=True)
class InstrumentConfig:
    """Configuration describing which instrument to sample."""

    instrument_id: str
    contract_type: Literal["swap", "futures", "spot"] = "swap"
    funding_symbol: str | None = None  # override for funding endpoints


@dataclass(slots=True)
class MarketCollectorConfig:
    """Collector settings for timeframes, instruments, and limits."""

    base_url: str = "https://www.okx.com"
    instruments: List[InstrumentConfig] = field(default_factory=list)
    timeframes: List[Timeframe] = field(
        default_factory=lambda: ["2m", "5m", "15m", "1H", "4H"]
    )
    candle_limit: int = 120  # number of points per timeframe
    orderbook_depth: int = 50
    trades_limit: int = 200
    request_delay_seconds: float = 0.0  # throttle between OKX REST hits


class MarketCollector:
    """Fetches market data from OKX public REST endpoints."""

    def __init__(self, config: MarketCollectorConfig) -> None:
        self.config = config
        self._client = httpx.AsyncClient(base_url=config.base_url, timeout=10.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_all(self) -> Dict[str, Any]:
        """Fetch data for all instruments/timeframes."""
        results: Dict[str, Any] = {"fetched_at": datetime.now(tz=timezone.utc).isoformat()}
        for instrument in self.config.instruments:
            inst_id = instrument.instrument_id
            candle_tasks = [
                self._fetch_candles(inst_id, tf, self.config.candle_limit)
                for tf in self.config.timeframes
            ]
            candles = await asyncio.gather(*candle_tasks)
            orderbook = await self._fetch_orderbook(inst_id, self.config.orderbook_depth)
            funding = await self._fetch_funding_rate(instrument)
            trades = await self._fetch_trades(inst_id, self.config.trades_limit)
            results[inst_id] = {
                "candles": {tf: data for tf, data in zip(self.config.timeframes, candles)},
                "orderbook": orderbook,
                "funding_rate": funding,
                "trades": trades,
            }
        return results

    async def _fetch_candles(self, inst_id: str, timeframe: Timeframe, limit: int) -> list:
        """
        Retrieve historical candles. For 2m we build from 1m data aggregated in pairs.
        """
        if timeframe == "2m":
            raw = await self._get(
                "/api/v5/market/candles",
                params={"instId": inst_id, "bar": "1m", "limit": limit * 2},
            )
            return self._aggregate_two_minute(raw.get("data", []))

        bar_map = {"5m": "5m", "15m": "15m", "1H": "1H", "4H": "4H"}
        raw = await self._get(
            "/api/v5/market/candles",
            params={"instId": inst_id, "bar": bar_map[timeframe], "limit": limit},
        )
        return raw.get("data", [])

    async def _fetch_orderbook(self, inst_id: str, depth: int) -> dict:
        raw = await self._get(
            "/api/v5/market/books",
            params={"instId": inst_id, "sz": depth},
        )
        data = raw.get("data", [])
        return data[0] if data else {}

    async def _fetch_funding_rate(self, instrument: InstrumentConfig) -> dict:
        symbol = instrument.funding_symbol or instrument.instrument_id
        raw = await self._get(
            "/api/v5/public/funding-rate",
            params={"instId": symbol},
        )
        data = raw.get("data", [])
        return data[0] if data else {}

    async def _fetch_trades(self, inst_id: str, limit: int) -> list:
        raw = await self._get(
            "/api/v5/market/trades",
            params={"instId": inst_id, "limit": limit},
        )
        return raw.get("data", [])

    async def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "0":
            raise RuntimeError(
                f"OKX API error {payload.get('code')}: {payload.get('msg')}"
            )
        delay = max(0.0, float(self.config.request_delay_seconds))
        if delay:
            # OKX REST 接口有严格的速率限制，主动延迟可以减少 429。
            await asyncio.sleep(delay)
        return payload

    @staticmethod
    def _aggregate_two_minute(data: Iterable[List[str]]) -> List[List[str]]:
        """
        Aggregate list of 1m candles (reverse chronological) into 2m intervals.
        OKX returns candles with latest first; we convert to chronological then process.
        """
        chronological = list(reversed(list(data)))
        aggregated: List[List[str]] = []
        for i in range(0, len(chronological), 2):
            block = chronological[i : i + 2]
            if not block:
                continue
            first = block[0]
            open_price = float(first[1])
            close_price = float(block[-1][4])
            high_price = max(float(c[2]) for c in block)
            low_price = min(float(c[3]) for c in block)
            volume = sum(float(c[5]) for c in block)
            ts = block[-1][0]  # end timestamp
            aggregated.append(
                [
                    ts,
                    f"{open_price}",
                    f"{high_price}",
                    f"{low_price}",
                    f"{close_price}",
                    f"{volume}",
                ]
            )
        aggregated = list(reversed(aggregated))
        return aggregated[: len(data) // 2]
