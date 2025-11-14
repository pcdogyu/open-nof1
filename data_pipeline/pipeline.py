"""
Reusable market data pipeline compatible with both CLI and scheduler use cases.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx

from data_pipeline.collector import (
    InstrumentConfig,
    MarketCollector,
    MarketCollectorConfig,
)
from data_pipeline.indicators import IndicatorCalculator
from data_pipeline.influx import InfluxConfig, InfluxWriter
from models.runtime import SignalRuntime
from models.schemas import MarketSnapshot, RiskContext, SignalRequest, SignalResponse


ACTION_TEXT = {
    "buy": ("open long", "开多"),
    "sell": ("open short", "开空"),
    None: ("no trade", "观望"),
}

REASON_TRANSLATIONS = {
    "deterministic fallback based on position ratio and price bias.": "基于仓位比例与价格偏差的确定性兜底逻辑。",
}


@dataclass(slots=True)
class SignalRecord:
    instrument_id: str
    model_id: str
    decision: str
    confidence: float
    reasoning: str
    suggested_order: Dict[str, Any]
    generated_at: datetime


@dataclass(slots=True)
class PipelineCycleResult:
    timestamp: datetime
    signals: List[SignalRecord]
    feature_snapshots: Dict[str, Dict[str, float]]


@dataclass(slots=True)
class PipelineSettings:
    instruments: Sequence[str]
    max_position: float = 5.0
    current_position: float = 1.0
    cash_available: float = 10_000.0
    signal_log_path: Optional[Path] = None


SIGNAL_LOG_MAX_LINES = 1000


class MarketDataPipeline:
    """
    Encapsulates the OKX market data collection, indicator computation, and
    signal generation workflow.
    """

    def __init__(
        self,
        settings: PipelineSettings,
        *,
        collector: MarketCollector | None = None,
        indicator_calc: IndicatorCalculator | None = None,
        influx_writer: InfluxWriter | None = None,
        runtime: SignalRuntime | None = None,
    ) -> None:
        self.settings = settings
        instrument_configs = [
            InstrumentConfig(instrument_id=inst.strip())
            for inst in settings.instruments
            if inst and inst.strip()
        ]
        if not instrument_configs:
            raise ValueError("At least one instrument must be provided to the pipeline.")

        self.collector = collector or MarketCollector(
            MarketCollectorConfig(instruments=instrument_configs)
        )
        self.indicator_calc = indicator_calc or IndicatorCalculator()
        self.influx_writer = influx_writer or InfluxWriter(InfluxConfig.from_env())
        self.runtime = runtime or SignalRuntime()

        if settings.signal_log_path is not None:
            settings.signal_log_path.parent.mkdir(parents=True, exist_ok=True)

    async def run_cycle(
        self,
        *,
        log: Optional[Callable[[str], None]] = None,
    ) -> PipelineCycleResult:
        """
        Execute a single data collection + signal generation cycle.

        Args:
            log: Optional callable used for human-readable logging output.
        """
        timestamp = datetime.now(tz=timezone.utc)
        try:
            market_payload = await self.collector.fetch_all()
        except httpx.HTTPError:
            raise

        feature_snapshots: Dict[str, Dict[str, float]] = {}
        signals: List[SignalRecord] = []
        timestamp_ns = int(timestamp.timestamp() * 1e9)

        for inst_id in self.settings.instruments:
            inst_id = inst_id.strip()
            if not inst_id:
                continue
            inst_data = market_payload.get(inst_id)
            if not inst_data:
                continue

            candles = inst_data["candles"]
            orderbook = inst_data["orderbook"]
            trades = inst_data["trades"]
            funding = inst_data["funding_rate"]

            feature_map: Dict[str, float] = {}
            for timeframe, candle_data in candles.items():
                indicator = self.indicator_calc.compute_candle_indicators(timeframe, candle_data)
                volume_stats = self.indicator_calc.compute_volume_profile(candle_data)
                fields = {
                    "macd": indicator.macd,
                    "macd_signal": indicator.macd_signal,
                    "macd_hist": indicator.macd_hist,
                    "rsi": indicator.rsi,
                    "realized_vol": indicator.realized_volatility,
                    "close_price": indicator.close_price,
                    **volume_stats,
                }
                self.influx_writer.write_indicator_set(
                    measurement="market_indicators",
                    tags={"instrument_id": inst_id, "timeframe": timeframe},
                    fields=fields,
                    timestamp_ns=timestamp_ns,
                )
                if timeframe == "15m":
                    feature_map.update({f"15m_{k}": v for k, v in fields.items() if v is not None})

            latest_series = candles.get("2m") or []
            latest_candle = latest_series[0] if latest_series else None
            if not latest_candle:
                continue
            price = float(latest_candle[4])

            best_bid = None
            best_ask = None
            spread = None
            if orderbook and orderbook.get("bids") and orderbook.get("asks"):
                try:
                    best_bid = float(orderbook["bids"][0][0])
                    best_ask = float(orderbook["asks"][0][0])
                    spread = best_ask - best_bid
                except (TypeError, ValueError, IndexError):
                    best_bid = None
                    best_ask = None
                    spread = None

            imbalance = self.indicator_calc.compute_orderbook_imbalance(orderbook)
            cvd = self.indicator_calc.compute_cvd(trades)
            funding_rate = float(funding.get("fundingRate", 0.0)) if funding else 0.0

            micro_fields = {
                "orderbook_imbalance": imbalance,
                "cvd": cvd,
                "funding_rate": funding_rate,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "last_price": price,
                "mid_price": (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None,
            }
            self.influx_writer.write_indicator_set(
                measurement="market_microstructure",
                tags={"instrument_id": inst_id},
                fields=micro_fields,
                timestamp_ns=timestamp_ns,
            )
            feature_map.update({k: v for k, v in micro_fields.items() if v is not None})
            feature_snapshots[inst_id] = feature_map.copy()

            requests: List[SignalRequest] = []
            for model_id in self.runtime.registry.list():
                request = SignalRequest(
                    model_id=model_id,
                    market=MarketSnapshot(
                        instrument_id=inst_id,
                        price=price,
                        spread=spread,
                        volume_24h=feature_map.get("15m_volume_sum"),
                        metadata=feature_map.copy(),
                    ),
                    risk=RiskContext(
                        max_position=self.settings.max_position,
                        current_position=self.settings.current_position,
                        cash_available=self.settings.cash_available,
                        notes={
                            "funding_rate": funding_rate,
                            "imbalance": imbalance,
                            "cvd": cvd,
                        },
                    ),
                    positions=[],
                    strategy_hint=(
                        "Evaluate whether conditions warrant open_long or open_short entries. "
                        "If no trade is required, return hold/reduce/close with justification."
                    ),
                )
                requests.append(request)

            responses: Iterable[SignalResponse] = []
            if requests:
                responses = await self.runtime.batch_generate(requests)

            for response in responses:
                record = SignalRecord(
                    instrument_id=inst_id,
                    model_id=response.model_id,
                    decision=response.decision,
                    confidence=response.confidence,
                    reasoning=response.reasoning,
                    suggested_order=response.suggested_order,
                    generated_at=response.generated_at,
                )
                signals.append(record)
                if log is not None:
                    side = decision_to_side(record.decision)
                    action_en, action_zh = ACTION_TEXT.get(side, ACTION_TEXT[None])
                    log(
                        f"[{inst_id}] {record.model_id} -> {record.decision} "
                        f"(confidence={record.confidence:.2f}) => {action_en} / {action_zh}"
                    )
                self.influx_writer.write_signal(
                    instrument_id=record.instrument_id,
                    model_id=record.model_id,
                    decision=record.decision,
                    confidence=record.confidence,
                    reasoning=record.reasoning,
                    order=record.suggested_order,
                    generated_at=record.generated_at,
                )
                if self.settings.signal_log_path is not None:
                    _append_signal_log(
                        self.settings.signal_log_path,
                        {
                            "timestamp": record.generated_at.isoformat(),
                            "instrument_id": inst_id,
                            "model_id": record.model_id,
                            "decision": record.decision,
                            "confidence": record.confidence,
                            "reasoning": record.reasoning,
                            "order": record.suggested_order,
                        },
                        max_lines=SIGNAL_LOG_MAX_LINES,
                    )

        return PipelineCycleResult(
            timestamp=timestamp,
            signals=signals,
            feature_snapshots=feature_snapshots,
        )

    async def aclose(self) -> None:
        """Asynchronously release underlying resources."""
        try:
            await self.collector.close()
        finally:
            self.influx_writer.close()
            await self.runtime.shutdown()


def decision_to_side(decision: str) -> str | None:
    decision = (decision or "").lower()
    if decision == "open_long":
        return "buy"
    if decision == "open_short":
        return "sell"
    return None


def translate_reason(reason: str) -> str:
    if not reason:
        return ""
    lowered = reason.strip().lower()
    return REASON_TRANSLATIONS.get(lowered, reason)


def format_order_bilingual(order: dict) -> Tuple[str, str]:
    if not order:
        return ("n/a", "暂无数据")
    side = (order.get("side") or "").lower()
    size = order.get("size", "?")
    price = order.get("price", "?")
    instrument = order.get("instrument_id") or order.get("instId") or "?"
    order_type = order.get("type") or order.get("ordType") or "?"
    side_en = "buy" if side == "buy" else "sell" if side == "sell" else (side or "n/a")
    side_cn = "开多" if side == "buy" else "开空" if side == "sell" else "未知方向"
    en = f"{side_en} {size} @ {price} ({instrument}, type={order_type})"
    cn = f"{side_cn}{size}张，价格{price}（合约{instrument}，类型{order_type}）"
    return en, cn


def _append_signal_log(path: Path, record: dict, *, max_lines: int = 0) -> None:
    with path.open("a", encoding="utf-8") as fp:
        import json

        json.dump(record, fp, ensure_ascii=False)
        fp.write("\n")
    if max_lines > 0:
        _truncate_signal_log(path, max_lines)


def _truncate_signal_log(path: Path, max_lines: int) -> None:
    """Keep only the most recent ``max_lines`` entries in the signal log."""
    count = 0
    window: deque[str] = deque(maxlen=max_lines)
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            window.append(line)
            count += 1
    if count <= max_lines:
        return
    with path.open("w", encoding="utf-8") as fp:
        fp.writelines(window)
