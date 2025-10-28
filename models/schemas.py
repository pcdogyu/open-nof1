"""
Shared data structures for model signal generation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional


SignalDecision = Literal["buy", "sell", "hold", "reduce", "close"]


@dataclass(slots=True)
class MarketSnapshot:
    """Slice of market data provided to the model."""

    instrument_id: str
    price: float
    spread: float | None = None
    volume_24h: float | None = None
    timestamp: datetime | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RiskContext:
    """Risk state relevant for decision making."""

    max_position: float
    current_position: float
    cash_available: float
    leverage_limit: Optional[float] = None
    notes: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SignalRequest:
    """Request payload passed to model adapters."""

    model_id: str
    market: MarketSnapshot
    risk: RiskContext
    strategy_hint: str | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SignalResponse:
    """Structured response returned by model adapters."""

    model_id: str
    decision: SignalDecision
    confidence: float
    reasoning: str
    suggested_order: Dict[str, Any]
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    raw_output: Dict[str, Any] = field(default_factory=dict)

