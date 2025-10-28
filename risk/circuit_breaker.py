"""
Circuit breaker logic for halting trading after significant losses or drawdowns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict

from risk.schemas import PortfolioMetrics, RiskEvaluation


@dataclass(slots=True)
class CircuitBreakerConfig:
    """Configuration for circuit breaker thresholds."""

    max_drawdown_pct: float
    max_loss_absolute: float
    cooldown_seconds: int = 300


@dataclass(slots=True)
class CircuitBreakerState:
    """Internal state tracked per-portfolio."""

    halted: bool = False
    halted_at: datetime | None = None
    resume_at: datetime | None = None
    last_metrics: PortfolioMetrics | None = None


class CircuitBreaker:
    """Manages halt logic for multiple portfolios."""

    def __init__(self, config: CircuitBreakerConfig) -> None:
        self.config = config
        self._state: Dict[str, CircuitBreakerState] = {}

    def evaluate(self, metrics: PortfolioMetrics, evaluation: RiskEvaluation) -> None:
        state = self._state.setdefault(metrics.portfolio_id, CircuitBreakerState())
        state.last_metrics = metrics

        if state.halted:
            if state.resume_at and datetime.now(timezone.utc) >= state.resume_at:
                state.halted = False
                state.resume_at = None
            else:
                evaluation.add_violation(
                    "CIRCUIT_BREAKER_ACTIVE",
                    "Circuit breaker active; trading suspended for portfolio.",
                    resume_at=state.resume_at.isoformat() if state.resume_at else None,
                )
                return

        if metrics.unrealized_pnl_pct <= -abs(self.config.max_drawdown_pct):
            self._halt(state, reason="drawdown", evaluation=evaluation)
        elif metrics.daily_realized_pnl <= -abs(self.config.max_loss_absolute):
            self._halt(state, reason="loss", evaluation=evaluation)

    def manual_resume(self, portfolio_id: str) -> None:
        state = self._state.get(portfolio_id)
        if not state:
            return
        state.halted = False
        state.resume_at = None

    def is_halted(self, portfolio_id: str) -> bool:
        state = self._state.get(portfolio_id)
        return bool(state and state.halted)

    def _halt(self, state: CircuitBreakerState, *, reason: str, evaluation: RiskEvaluation) -> None:
        state.halted = True
        state.halted_at = datetime.now(timezone.utc)
        state.resume_at = state.halted_at + timedelta(seconds=self.config.cooldown_seconds)
        evaluation.add_violation(
            "CIRCUIT_BREAKER_TRIGGERED",
            f"Circuit breaker triggered due to {reason}.",
            resume_at=state.resume_at.isoformat() if state.resume_at else None,
        )
