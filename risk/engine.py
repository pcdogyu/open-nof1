"""
Composable risk engine chaining price and circuit breaker validations.
"""

from __future__ import annotations

from typing import Iterable, List

from risk.circuit_breaker import CircuitBreaker
from risk.notional_limits import OrderNotionalGuard, ProfitLossGuard
from risk.price_limits import PriceLimitValidator
from risk.schemas import MarketContext, OrderIntent, PortfolioMetrics, RiskEvaluation


class RiskEngine:
    """High-level orchestrator for validating orders prior to execution."""

    def __init__(
        self,
        *,
        price_validator: PriceLimitValidator | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        notional_guard: OrderNotionalGuard | None = None,
        pnl_guard: ProfitLossGuard | None = None,
    ) -> None:
        self.price_validator = price_validator or PriceLimitValidator()
        self.circuit_breaker = circuit_breaker
        self.notional_guard = notional_guard
        self.pnl_guard = pnl_guard
        self._hooks: List = []

    def add_hook(self, hook) -> None:
        """Register custom hook callable(evaluation, order, context)."""
        self._hooks.append(hook)

    def evaluate(
        self,
        order: OrderIntent,
        market: MarketContext,
        portfolio: PortfolioMetrics,
    ) -> RiskEvaluation:
        evaluation = RiskEvaluation(approved=True)
        self.price_validator.validate(order, evaluation)
        if self.notional_guard:
            self.notional_guard.validate(order, market, evaluation)
        if self.circuit_breaker:
            self.circuit_breaker.evaluate(portfolio, evaluation)
        if self.pnl_guard:
            self.pnl_guard.evaluate(portfolio, evaluation)
        for hook in self._hooks:
            hook(evaluation, order, market, portfolio)
        return evaluation

    def update_market(self, market: MarketContext, *, tolerance_pct: float | None = None) -> None:
        """Refresh price limits using the latest market snapshot."""
        self.price_validator.update_from_market(market, tolerance_pct)

    def bulk_update(self, markets: Iterable[MarketContext]) -> None:
        for market in markets:
            self.update_market(market)
