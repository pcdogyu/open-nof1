"""
Abstract client definitions for centralized exchange integrations.

Concrete adapters (e.g. OKX, Binance) should subclass `ExchangeClient` and
implement the required methods while respecting throttling and error handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol, runtime_checkable


@dataclass(slots=True)
class ExchangeCredentials:
    """Typed container for exchange authentication data."""

    api_key: str
    api_secret: str
    passphrase: str | None = None


@runtime_checkable
class ExchangeClient(Protocol):
    """Protocol describing the surface area for exchange integrations."""

    name: str

    def authenticate(self, credentials: ExchangeCredentials) -> None:
        """Load credentials into the client and perform any handshake actions."""

    def fetch_balances(self) -> dict:
        """Return the latest wallet balances keyed by currency."""

    def fetch_positions(self, symbols: Iterable[str] | None = None) -> list[dict]:
        """Return open positions; limit results to `symbols` when provided."""

    def place_order(self, payload: dict) -> dict:
        """Submit an order to the exchange and return the normalized response."""

    def cancel_order(self, order_id: str, instrument_id: str | None = None) -> dict:
        """Cancel an existing order by identifier (instrument may be required)."""

    def close(self) -> None:
        """Release network resources (WebSocket connections, sessions, etc.)."""
