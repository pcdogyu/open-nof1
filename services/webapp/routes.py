"""
HTTP route handlers for the FastAPI web application.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException

from accounts.portfolio_registry import PortfolioRecord, PortfolioRegistry
from services.webapp.dependencies import get_portfolio_registry


router = APIRouter()


@router.get("/health", summary="Service health probe")
def health_check() -> dict:
    """Return a static payload for uptime checks."""
    return {"status": "ok"}


@router.get(
    "/portfolios",
    response_model=List[dict],
    summary="List all registered portfolios",
)
def list_portfolios(
    registry: PortfolioRegistry = Depends(get_portfolio_registry),
) -> list[dict]:
    """Return all portfolios currently registered in the system."""
    records = registry.list_all()
    return [
        {
            **asdict(record),
            "metrics": _mock_portfolio_metrics(record),
        }
        for record in records
    ]


@router.get(
    "/portfolios/{portfolio_id}",
    response_model=dict,
    summary="Fetch a single portfolio by identifier",
)
def get_portfolio(
    portfolio_id: str,
    registry: PortfolioRegistry = Depends(get_portfolio_registry),
) -> dict:
    """Return a specific portfolio or raise if it does not exist."""
    record: PortfolioRecord | None = registry.get(portfolio_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    return {
        **asdict(record),
        "metrics": _mock_portfolio_metrics(record),
    }


@router.get(
    "/metrics/system",
    summary="High-level system health metrics",
)
def get_system_metrics() -> dict:
    """Return placeholder metrics for the overall trading system."""
    now = datetime.now(tz=timezone.utc)
    return {
        "as_of": now.isoformat(),
        "status": "degraded" if now.minute % 5 == 0 else "operational",
        "active_models": 2,
        "open_positions": 3,
        "daily_realized_pnl": 152.37,
        "daily_unrealized_pnl": -42.15,
        "latency_ms": {
            "signal_processing_avg": 215,
            "order_execution_p95": 380,
        },
        "risk_alerts": 1,
    }


@router.get(
    "/metrics/models",
    summary="Per-model performance snapshots",
)
def get_model_metrics() -> dict:
    """Return mock performance stats for DeepSeek and Qwen models."""
    now = datetime.now(tz=timezone.utc).isoformat()
    recent_trades = _mock_recent_trades()
    return {
        "as_of": now,
        "models": [
            {
                "model_id": "deepseek-v1",
                "portfolio_id": "okx_deepseek_demo",
                "sharpe_ratio": 1.8,
                "max_drawdown_pct": 4.5,
                "win_rate_pct": 58.0,
                "avg_trade_duration_min": 42,
                "exposure_usd": 6800.0,
                "open_positions": 2,
            },
            {
                "model_id": "qwen-v1",
                "portfolio_id": "okx_qwen_demo",
                "sharpe_ratio": 1.2,
                "max_drawdown_pct": 6.1,
                "win_rate_pct": 53.0,
                "avg_trade_duration_min": 35,
                "exposure_usd": 7200.0,
                "open_positions": 1,
            },
        ],
        "recent_trades": recent_trades,
    }


def _mock_portfolio_metrics(record: PortfolioRecord) -> dict:
    """Generate deterministic placeholder metrics for portfolio responses."""
    base_seed = sum(ord(c) for c in record.portfolio_id)
    equity = record.starting_equity + (base_seed % 500)
    return {
        "last_synced": datetime.now(tz=timezone.utc).isoformat(),
        "balance": {
            record.base_currency: {
                "total": equity,
                "available": equity * 0.9,
            }
        },
        "unrealized_pnl": round((base_seed % 200) - 100, 2),
        "realized_pnl": round((base_seed % 150) - 75, 2),
        "open_positions": base_seed % 3,
        "risk_level": "medium",
    }


def _mock_recent_trades() -> list[dict]:
    """Return placeholder recent trade records shared by UI."""
    return [
        {
            "executed_at": "2025-10-28T09:39:10+00:00",
            "model_id": "deepseek-v1",
            "portfolio_id": "okx_deepseek_demo",
            "instrument_id": "BTC-USDT-SWAP",
            "side": "buy",
            "size": 0.25,
            "entry_price": 34050.5,
            "exit_price": 34210.0,
            "pnl": 42.1,
        },
        {
            "executed_at": "2025-10-28T09:33:52+00:00",
            "model_id": "qwen-v1",
            "portfolio_id": "okx_qwen_demo",
            "instrument_id": "ETH-USDT-SWAP",
            "side": "sell",
            "size": 3.0,
            "entry_price": 1785.8,
            "exit_price": 1792.3,
            "pnl": -18.4,
        },
        {
            "executed_at": "2025-10-28T09:27:18+00:00",
            "model_id": "deepseek-v1",
            "portfolio_id": "okx_deepseek_demo",
            "instrument_id": "BTC-USDT-SWAP",
            "side": "sell",
            "size": 0.1,
            "entry_price": 34120.0,
            "exit_price": 33980.5,
            "pnl": 8.6,
        },
    ]
