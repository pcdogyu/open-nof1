"""
Structured domain events emitted when portfolio state changes occur.

Downstream consumers (risk, monitoring, dashboards) can subscribe to these
events via the message bus without coupling to implementation details.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol


@dataclass(slots=True)
class AccountEvent:
    """Base event envelope for account domain messages."""

    portfolio_id: str
    model_id: str
    exchange: str
    event_type: Literal["balance", "position", "status"]
    emitted_at: datetime


@dataclass(slots=True)
class BalanceEvent(AccountEvent):
    """Represents an updated balance snapshot for a portfolio."""

    currency: str
    total: float
    available: float


@dataclass(slots=True)
class PositionEvent(AccountEvent):
    """Represents an updated open position snapshot."""

    symbol: str
    size: float
    entry_price: float
    mark_price: float


class EventPublisher(Protocol):
    """Abstraction for publishing events to the shared message bus."""

    def publish(self, event: AccountEvent) -> None:
        """Publish the account event downstream."""
