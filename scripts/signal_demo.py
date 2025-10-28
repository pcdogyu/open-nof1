"""
Quick demo to run DeepSeek and Qwen signal generation locally.

Offline mode requires no API keys and uses deterministic heuristics.
Set `DEEPSEEK_API_KEY` and `QWEN_API_KEY` to hit the real services.
"""

from __future__ import annotations

import asyncio
import json

from models.runtime import SignalRuntime
from models.schemas import MarketSnapshot, RiskContext, SignalRequest


async def main() -> None:
    runtime = SignalRuntime()
    requests = [
        SignalRequest(
            model_id="deepseek-v1",
            market=MarketSnapshot(
                instrument_id="BTC-USDT-SWAP",
                price=34250.0,
                spread=5.0,
            ),
            risk=RiskContext(
                max_position=1.5,
                current_position=0.3,
                cash_available=5000.0,
            ),
            strategy_hint="Swing trading with emphasis on momentum and risk parity.",
        ),
        SignalRequest(
            model_id="qwen-v1",
            market=MarketSnapshot(
                instrument_id="ETH-USDT-SWAP",
                price=1780.0,
                spread=1.2,
            ),
            risk=RiskContext(
                max_position=10.0,
                current_position=8.5,
                cash_available=2000.0,
            ),
            strategy_hint="Reduce exposure if drawdown risk exceeds limits.",
        ),
    ]
    responses = await runtime.batch_generate(requests)
    for response in responses:
        print(f"Model: {response.model_id}")
        print(json.dumps(response.raw_output, indent=2))
        print("=" * 60)
    await runtime.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
