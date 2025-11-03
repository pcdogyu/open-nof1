"""
Application-wide dependency providers for the web service.

The functions declared here are meant to be used with FastAPI's dependency
injection framework while keeping instantiation logic in one place.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable

from accounts.portfolio_registry import PortfolioRecord, PortfolioRegistry
from accounts.repository import AccountRepository, InfluxAccountRepository


@lru_cache(maxsize=1)
def get_portfolio_registry() -> PortfolioRegistry:
    """
    Return a globally shared portfolio registry instance.

    In production the registry should be backed by persistent storage, but
    the skeleton seeds demo records for immediate manual testing.
    """
    registry = PortfolioRegistry()
    _seed_demo_portfolios(registry)
    return registry


def _seed_demo_portfolios(registry: PortfolioRegistry) -> None:
    """Populate the registry with placeholder entries if empty."""
    if registry.list_by_exchange("okx"):
        return
    for record in _demo_records():
        registry.upsert(record)


def _demo_records() -> Iterable[PortfolioRecord]:
    """Yield demo portfolio records for DeepSeek and Qwen models."""
    yield PortfolioRecord(
        portfolio_id="okx_deepseek_demo",
        model_id="deepseek-v1",
        base_currency="USDT",
        starting_equity=10_000.0,
        exchange="okx-paper",
        api_key_id="secrets/okx/deepseek-demo",
        notes="Seed account for DeepSeek model on OKX paper trading.",
    )
    yield PortfolioRecord(
        portfolio_id="okx_qwen_demo",
        model_id="qwen-v1",
        base_currency="USDT",
        starting_equity=10_000.0,
        exchange="okx-paper",
        api_key_id="secrets/okx/qwen-demo",
        notes="Seed account for Qwen model on OKX paper trading.",
    )


@lru_cache(maxsize=1)
def get_account_repository() -> AccountRepository:
    """Provide a shared repository instance for account persistence."""
    return InfluxAccountRepository()
