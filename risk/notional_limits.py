"""
Order-level notional guardrails and portfolio PnL based stop controls.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from risk.schemas import MarketContext, OrderIntent, PortfolioMetrics, RiskEvaluation


def _resolve_reference_price(order: OrderIntent, market: MarketContext) -> Optional[float]:
    """Pick the best available price to estimate order notional."""
    for candidate in (order.price, market.mid_price, market.last_price, market.best_bid, market.best_ask):
        if candidate is None:
            continue
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


@dataclass(slots=True)
class OrderNotionalGuard:
    """Validate order notionals against configured min/max thresholds."""

    min_notional: float = 0.0
    max_notional: float = 0.0

    def validate(self, order: OrderIntent, market: MarketContext, evaluation: RiskEvaluation) -> None:
        if order.size is None or order.size <= 0:
            return
        price = _resolve_reference_price(order, market)
        if price is None:
            return
        notional = order.size * price
        if self.min_notional > 0 and notional < self.min_notional:
            evaluation.add_violation(
                "NOTIONAL_TOO_LOW",
                "Order notional below configured minimum.",
                instrument_id=order.instrument_id,
                notional=notional,
                min_notional=self.min_notional,
                reference_price=price,
            )
        if self.max_notional > 0 and notional > self.max_notional:
            evaluation.add_violation(
                "NOTIONAL_TOO_HIGH",
                "Order notional exceeds configured maximum.",
                instrument_id=order.instrument_id,
                notional=notional,
                max_notional=self.max_notional,
                reference_price=price,
            )


@dataclass(slots=True)
class ProfitLossGuard:
    """Stop gating new trades once portfolio PnL hits configured bounds."""

    take_profit_pct: float = 0.0
    stop_loss_pct: float = 0.0

    def evaluate(self, metrics: PortfolioMetrics, evaluation: RiskEvaluation) -> None:
        try:
            pnl_pct = float(metrics.unrealized_pnl_pct)
        except (TypeError, ValueError):
            return
        if self.take_profit_pct > 0 and pnl_pct >= self.take_profit_pct:
            evaluation.add_violation(
                "TAKE_PROFIT_REACHED",
                "Portfolio PnL reached take-profit threshold; halting new trades.",
                take_profit_pct=self.take_profit_pct,
                unrealized_pnl_pct=pnl_pct,
                portfolio_id=metrics.portfolio_id,
            )
        if self.stop_loss_pct > 0 and pnl_pct <= -abs(self.stop_loss_pct):
            evaluation.add_violation(
                "STOP_LOSS_REACHED",
                "Portfolio PnL breached stop-loss threshold; halting new trades.",
                stop_loss_pct=self.stop_loss_pct,
                unrealized_pnl_pct=pnl_pct,
                portfolio_id=metrics.portfolio_id,
            )
