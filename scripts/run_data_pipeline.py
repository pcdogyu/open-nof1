"""
End-to-end data pipeline:
1. Fetch OKX market data for multiple timeframes.
2. Compute indicators (MACD, RSI, volatility, CVD, order book imbalance).
3. Store results into InfluxDB (bucket: nof).
4. Feed the latest snapshot into the LLM runtime to obtain trading signals.

Prerequisites:
    pip install httpx pandas numpy influxdb-client
    export INFLUX_TOKEN=<your-token>

Usage:
    python scripts/run_data_pipeline.py --instrument BTC-USDT-SWAP --instrument ETH-USDT-SWAP
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import httpx

# Ensure repository root is available on the Python path when run as a script.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from config import PIPELINE_POLL_INTERVAL, TRADABLE_INSTRUMENTS
except ImportError:
    TRADABLE_INSTRUMENTS = [
        "XRP-USDT-SWAP",
        "BNB-USDT-SWAP",
        "BTC-USDT-SWAP",
        "ETH-USDT-SWAP",
        "SOL-USDT-SWAP",
        "DOGE-USDT-SWAP",
    ]
    PIPELINE_POLL_INTERVAL = 120

DEFAULT_INSTRUMENTS = tuple(TRADABLE_INSTRUMENTS)
DEFAULT_POLL_INTERVAL = int(PIPELINE_POLL_INTERVAL)

from data_pipeline.collector import (
    InstrumentConfig,
    MarketCollector,
    MarketCollectorConfig,
)
from data_pipeline.indicators import IndicatorCalculator
from data_pipeline.influx import InfluxConfig, InfluxWriter
from models.runtime import SignalRuntime
from models.schemas import MarketSnapshot, RiskContext, SignalRequest


async def async_main(args: argparse.Namespace) -> None:
    poll_interval = max(1, int(args.interval))
    selected_instruments = [
        inst.strip() for inst in (args.instrument or DEFAULT_INSTRUMENTS)
    ]
    instruments = [
        InstrumentConfig(instrument_id=inst_id) for inst_id in selected_instruments
    ]
    collector = MarketCollector(
        MarketCollectorConfig(
            instruments=instruments,
        )
    )
    indicator_calc = IndicatorCalculator()
    influx_writer = InfluxWriter(InfluxConfig.from_env())
    runtime = SignalRuntime()
    signal_log_path = Path(args.signal_log)
    signal_log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        while True:
            cycle_started = datetime.now(timezone.utc)
            print(f"=== Cycle started at {cycle_started.isoformat()} ===")
            try:
                market_payload = await collector.fetch_all()
            except httpx.HTTPError as exc:
                print(f"⚠️  Fetch failed: {exc} / 数据抓取失败，将在 {poll_interval}s 后重试")
                await asyncio.sleep(poll_interval)
                continue
            timestamp_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
            for inst_id in selected_instruments:
                inst_data = market_payload.get(inst_id)
                if not inst_data:
                    continue
                candles = inst_data["candles"]
                orderbook = inst_data["orderbook"]
                trades = inst_data["trades"]
                funding = inst_data["funding_rate"]

                feature_map: Dict[str, float] = {}
                for timeframe, candle_data in candles.items():
                    indicator = indicator_calc.compute_candle_indicators(timeframe, candle_data)
                    volume_stats = indicator_calc.compute_volume_profile(candle_data)
                    measurement = "market_indicators"
                    tags = {"instrument_id": inst_id, "timeframe": timeframe}
                    fields = {
                        "macd": indicator.macd,
                        "macd_signal": indicator.macd_signal,
                        "macd_hist": indicator.macd_hist,
                        "rsi": indicator.rsi,
                        "realized_vol": indicator.realized_volatility,
                        **volume_stats,
                    }
                    influx_writer.write_indicator_set(
                        measurement=measurement,
                        tags=tags,
                        fields=fields,
                        timestamp_ns=timestamp_ns,
                    )
                    if timeframe == "15m":
                        feature_map.update({f"15m_{k}": v for k, v in fields.items() if v is not None})

                imbalance = indicator_calc.compute_orderbook_imbalance(orderbook)
                cvd = indicator_calc.compute_cvd(trades)
                funding_rate = float(funding.get("fundingRate", 0.0)) if funding else 0.0

                extra_fields = {
                    "orderbook_imbalance": imbalance,
                    "cvd": cvd,
                    "funding_rate": funding_rate,
                }
                influx_writer.write_indicator_set(
                    measurement="market_microstructure",
                    tags={"instrument_id": inst_id},
                    fields=extra_fields,
                    timestamp_ns=timestamp_ns,
                )
                feature_map.update({k: v for k, v in extra_fields.items() if v is not None})

                latest_series = candles.get("2m") or []
                latest_candle = latest_series[0] if latest_series else None
                if not latest_candle:
                    continue
                price = float(latest_candle[4])
                spread = None
                if orderbook:
                    best_bid = float(orderbook["bids"][0][0])
                    best_ask = float(orderbook["asks"][0][0])
                    spread = best_ask - best_bid

                requests: List[SignalRequest] = []
                for model_id in runtime.registry.list():
                    request = SignalRequest(
                        model_id=model_id,
                        market=MarketSnapshot(
                            instrument_id=inst_id,
                            price=price,
                            spread=spread,
                            volume_24h=feature_map.get("15m_volume_sum"),
                            metadata=feature_map,
                        ),
                        risk=RiskContext(
                            max_position=args.max_position,
                            current_position=args.current_position,
                            cash_available=args.cash_available,
                            notes={
                                "funding_rate": funding_rate,
                                "imbalance": imbalance,
                                "cvd": cvd,
                            },
                        ),
                        strategy_hint=(
                            "Evaluate whether conditions warrant open_long or open_short entries. "
                            "If no trade is required, return hold/reduce/close with justification."
                        )
                    )
                    requests.append(request)
                responses = await runtime.batch_generate(requests)
                for response in responses:
                    order_side = decision_to_side(response.decision)
                    action_en, action_zh = ACTION_TEXT[order_side]
                    print(
                        f"[{inst_id}] {response.model_id} -> {response.decision} "
                        f"(confidence={response.confidence:.2f}) => {action_en} / {action_zh}"
                    )
                    reason_en = response.reasoning or ""
                    reason_zh = translate_reason(reason_en)
                    print(f"Reason (EN): {reason_en}")
                    print(f"原因 (中文): {reason_zh}")
                    order_en, order_zh = format_order_bilingual(response.suggested_order)
                    print(f"Suggested order (EN): {order_en}")
                    print(f"建议订单 (中文): {order_zh}")
                    print("-" * 60)
                    _append_signal_log(
                        signal_log_path,
                        {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "instrument_id": inst_id,
                            "model_id": response.model_id,
                            "decision": response.decision,
                            "action_en": action_en,
                            "action_zh": action_zh,
                            "confidence": response.confidence,
                            "reason_en": reason_en,
                            "reason_zh": reason_zh,
                            "order": response.suggested_order,
                        },
                    )
            print(f"=== Cycle completed at {datetime.now(timezone.utc).isoformat()} ===")
            await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        print("⚠️  Signal loop cancelled, preparing to shut down...")
    finally:
        await collector.close()
        influx_writer.close()
        await runtime.shutdown()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run market data pipeline and generate signals.")
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instrument",
        default=None,
        help=(
            "OKX instrument id, e.g. BTC-USDT-SWAP (can be provided multiple times). "
            f"Defaults to config.TRADABLE_INSTRUMENTS ({', '.join(DEFAULT_INSTRUMENTS)})."
        ),
    )
    parser.add_argument(
        "--max-position",
        type=float,
        default=5.0,
        help="Risk context: maximum position size (default 5).",
    )
    parser.add_argument(
        "--current-position",
        type=float,
        default=1.0,
        help="Risk context: current exposure (default 1).",
    )
    parser.add_argument(
        "--cash-available",
        type=float,
        default=10000.0,
        help="Risk context: available cash (default 10000).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Polling interval in seconds between signal refreshes (default {DEFAULT_POLL_INTERVAL}).",
    )
    parser.add_argument(
        "--signal-log",
        default="data/logs/ai_signals.jsonl",
        help="Path to append AI signal logs (default data/logs/ai_signals.jsonl).",
    )
    return parser.parse_args()


ACTION_TEXT = {
    "buy": ("open long", "开多"),
    "sell": ("open short", "开空"),
    None: ("no trade", "观望/减仓"),
}

REASON_TRANSLATIONS = {
    "deterministic fallback based on position ratio and price bias.": "基于仓位比例和价格偏差的确定性回退逻辑。",
}

def translate_reason(reason: str) -> str:
    if not reason:
        return ""
    lowered = reason.strip().lower()
    return REASON_TRANSLATIONS.get(lowered, reason)

def format_order_bilingual(order: dict) -> tuple[str, str]:
    if not order:
        return ("n/a", "暂无")
    side = (order.get("side") or "").lower()
    size = order.get("size", "?")
    price = order.get("price", "?")
    instrument = order.get("instrument_id") or order.get("instId") or "?"
    order_type = order.get("type") or order.get("ordType") or "?"
    side_en = "buy" if side == "buy" else "sell" if side == "sell" else (side or "n/a")
    side_cn = "买入" if side == "buy" else "卖出" if side == "sell" else "未知方向"
    en = f"{side_en} {size} @ {price} ({instrument}, type={order_type})"
    cn = f"{side_cn}{size}手，价格{price}，标的{instrument}，类型{order_type}"
    return en, cn

def _append_signal_log(path: Path, record: dict) -> None:
    with path.open("a", encoding="utf-8") as fp:
        json.dump(record, fp, ensure_ascii=False)
        fp.write("\n")

def decision_to_side(decision: str) -> str | None:
    decision = (decision or "").lower()
    if decision == "open_long":
        return "buy"
    if decision == "open_short":
        return "sell"
    return None


def main() -> None:
    args = parse_args()
    if args.instrument is None:
        args.instrument = list(DEFAULT_INSTRUMENTS)
        print("Using default instruments from config.py:", ", ".join(args.instrument))
    else:
        print("Using instruments from CLI:", ", ".join(args.instrument))
    print(f"Polling interval: {args.interval} seconds")
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
