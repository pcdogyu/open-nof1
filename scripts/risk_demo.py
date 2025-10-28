"""
Demonstrates the risk engine validating a limit order and triggering a circuit breaker.
"""

from __future__ import annotations

import json

from risk.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from risk.engine import RiskEngine
from risk.schemas import MarketContext, OrderIntent, PortfolioMetrics


def main() -> None:
    engine = RiskEngine(
        circuit_breaker=CircuitBreaker(
            CircuitBreakerConfig(
                max_drawdown_pct=5.0,
                max_loss_absolute=300.0,
                cooldown_seconds=120,
            )
        )
    )

    market = MarketContext(instrument_id="BTC-USDT-SWAP", last_price=34000, mid_price=34010)
    engine.update_market(market, tolerance_pct=0.02)

    order = OrderIntent(
        portfolio_id="okx_deepseek_demo",
        instrument_id="BTC-USDT-SWAP",
        side="buy",
        order_type="limit",
        size=0.5,
        price=35500,
    )
    portfolio = PortfolioMetrics(
        portfolio_id="okx_deepseek_demo",
        unrealized_pnl_pct=-6.0,
        daily_realized_pnl=-500.0,
        position_notional=12000.0,
    )

    evaluation = engine.evaluate(order, market, portfolio)
    print(json.dumps(
        {
            "approved": evaluation.approved,
            "violations": [
                {"code": v.code, "message": v.message, "details": v.details}
                for v in evaluation.violations
            ],
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
