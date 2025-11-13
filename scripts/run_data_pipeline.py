"""
CLI wrapper for the reusable market data pipeline.

This script now delegates all heavy lifting to ``data_pipeline.pipeline`` so the
same workflow can be invoked both manually and from the APScheduler jobs.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure repository root is importable when executed as a script.
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

from data_pipeline.pipeline import (
    ACTION_TEXT,
    MarketDataPipeline,
    PipelineSettings,
    decision_to_side,
    format_order_bilingual,
    translate_reason,
)


def _should_block_buy(decision: str, settings: PipelineSettings) -> tuple[bool, str]:
    """
    Simple risk guard: if we already hold exposure, skip additional buys.

    Returns:
        blocked flag + a human-readable reason.
    """
    side = decision_to_side(decision)
    if side != "buy":
        return False, ""
    current = float(settings.current_position or 0.0)
    max_position = float(settings.max_position or 0.0)
    if current <= 0:
        return False, ""
    if max_position > 0 and current >= max_position:
        return True, "position limit reached"
    return True, "existing position exposure"


async def async_main(args: argparse.Namespace) -> None:
    poll_interval = max(1, int(args.interval))
    selected_instruments = [inst.strip() for inst in (args.instrument or DEFAULT_INSTRUMENTS)]
    settings = PipelineSettings(
        instruments=selected_instruments,
        max_position=args.max_position,
        current_position=args.current_position,
        cash_available=args.cash_available,
        signal_log_path=Path(args.signal_log),
    )
    pipeline = MarketDataPipeline(settings)

    try:
        while True:
            cycle_started = datetime.now(timezone.utc)
            print(f"=== Cycle started at {cycle_started.isoformat()} ===")
            try:
                result = await pipeline.run_cycle(log=print)
            except Exception as exc:  # pragma: no cover - CLI provides quick feedback
                print(f"⚠️  Fetch failed: {exc!r} / 数据获取失败，等待 {poll_interval}s 重试")
                await asyncio.sleep(poll_interval)
                continue

            for record in result.signals:
                order_side = decision_to_side(record.decision)
                blocked, blocked_reason = _should_block_buy(record.decision, settings)
                effective_side = None if blocked else order_side
                action_en, action_zh = ACTION_TEXT.get(effective_side, ACTION_TEXT[None])
                reason_en = record.reasoning or ""
                reason_zh = translate_reason(reason_en)
                order_en, order_zh = format_order_bilingual(record.suggested_order)
                if blocked:
                    order_en = "n/a (blocked by risk guard)"
                    order_zh = "无操作（风险控制拦截）"
                print(
                    f"[{record.instrument_id}] {record.model_id} -> {record.decision} "
                    f"(confidence={record.confidence:.2f}) => {action_en} / {action_zh}"
                )
                if blocked:
                    print(
                        f"Risk guard: skip buy because {blocked_reason}; "
                        f"current_position={settings.current_position}, max_position={settings.max_position}"
                    )
                    print("风控提示：检测到已有持仓，跳过新的买入信号。")
                print(f"Reason (EN): {reason_en}")
                print(f"原因 (中文): {reason_zh}")
                print(f"Suggested order (EN): {order_en}")
                print(f"推荐下单 (中文): {order_zh}")
                print("-" * 60)

            print(f"=== Cycle completed at {datetime.now(timezone.utc).isoformat()} ===")
            await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        print("⚠️  Signal loop cancelled, preparing to shut down...")
    finally:
        await pipeline.aclose()


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
        default=0.0,
        help="Risk context: current exposure (default 0, set >0 when already holding a position).",
    )
    parser.add_argument(
        "--cash-available",
        type=float,
        default=10_000.0,
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
