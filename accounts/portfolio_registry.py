"""
Registry for tracking per-model trading accounts and associated credentials.

This module intentionally provides only the shape of the interfaces that other
components can depend on while implementation details are filled in later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Protocol


@dataclass(slots=True)
class PortfolioRecord:
    """Represents a single model-controlled trading portfolio."""

    portfolio_id: str
    model_id: str
    base_currency: str
    starting_equity: float
    exchange: str
    api_key_id: Optional[str] = None
    notes: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)


class CredentialResolver(Protocol):
    """Interface for retrieving sensitive credentials from secure storage."""

    def get_api_credentials(self, key_id: str) -> Dict[str, str]:
        """Return credentials for the provided key identifier."""


class PortfolioRegistry:
    """In-memory skeleton registry to be replaced by persistent storage later."""

    def __init__(self, credential_resolver: CredentialResolver | None = None) -> None:
        self._records: Dict[str, PortfolioRecord] = {}
        self._credential_resolver = credential_resolver

    def upsert(self, record: PortfolioRecord) -> None:
        """Create or update a portfolio definition."""
        self._records[record.portfolio_id] = record

    def get(self, portfolio_id: str) -> Optional[PortfolioRecord]:
        """Return the portfolio record if it exists."""
        return self._records.get(portfolio_id)

    def list_by_exchange(self, exchange: str) -> Dict[str, PortfolioRecord]:
        """Return all portfolios targeting a specific exchange."""
        return {
            portfolio_id: record
            for portfolio_id, record in self._records.items()
            if record.exchange == exchange
        }

    def list_all(self) -> list[PortfolioRecord]:
        """Return all registered portfolios."""
        return list(self._records.values())

    def find_by_model(self, model_id: str) -> Optional[PortfolioRecord]:
        """Return the first portfolio managed by the given model identifier."""
        for record in self._records.values():
            if record.model_id == model_id:
                return record
        return None

    def resolve_credentials(self, portfolio_id: str) -> Dict[str, str]:
        """
        Fetch exchange credentials using the configured resolver.

        Raises:
            KeyError: if the portfolio or credential information is missing.
            RuntimeError: if a resolver has not been configured.
        """
        record = self.get(portfolio_id)
        if record is None:
            raise KeyError(f"Unknown portfolio_id '{portfolio_id}'")
        if record.api_key_id is None:
            raise KeyError(f"No api_key_id configured for '{portfolio_id}'")
        if self._credential_resolver is None:
            raise RuntimeError("Credential resolver has not been configured")
        return self._credential_resolver.get_api_credentials(record.api_key_id)
