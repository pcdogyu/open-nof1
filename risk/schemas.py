"""
Dataclasses and helper structures used by the risk engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional


OrderSide = Literal["buy", "sell"]
OrderType = Literal["limit", "market"]


@dataclass(slots=True)
class OrderIntent:
    """Normalized order request prior to submission."""

    portfolio_id: str
    instrument_id: str
    side: OrderSide
    order_type: OrderType
    size: float
    price: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MarketContext:
    """Relevant market data for risk evaluation."""

    instrument_id: str
    last_price: float
    mid_price: Optional[float] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PortfolioMetrics:
    """Current portfolio metrics consumed by risk controls."""

    portfolio_id: str
    unrealized_pnl_pct: float
    daily_realized_pnl: float
    position_notional: float
    risk_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RiskViolation:
    """Represents a single broken risk rule."""

    code: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RiskEvaluation:
    """Aggregate result of a risk evaluation run."""

    approved: bool
    violations: List[RiskViolation] = field(default_factory=list)

    def add_violation(self, code: str, message: str, **details: Any) -> None:
        self.violations.append(RiskViolation(code=code, message=message, details=details))
        self.approved = False
