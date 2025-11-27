"""
Live data helpers for pulling OKX account state on demand.
"""

from __future__ import annotations

from typing import Dict

import httpx

from exchanges.base_client import ExchangeCredentials
from exchanges.okx.paper import OkxPaperClient, OkxClientError
from services.okx.transform import extract_balances, normalize_orders, normalize_positions, normalize_trades


def fetch_account_snapshot(account_meta: dict, *, limit: int = 50) -> dict:
    """
    Fetch balances, positions, orders, and trades for a single OKX account.

    The returned dictionary contains sanitized primitives ready for JSON serialization.
    """
    credentials = ExchangeCredentials(
        api_key=account_meta["api_key"],
        api_secret=account_meta["api_secret"],
        passphrase=account_meta.get("passphrase"),
    )
    account_id = account_meta.get("account_id") or account_meta.get("name", "unknown")
    model_id = account_meta.get("model_id") or account_id
    base_currency = account_meta.get("base_currency", "USDT")

    client = OkxPaperClient()
    try:
        client.authenticate(credentials)
        balances_raw = client.fetch_balances()
        positions_raw = client.fetch_positions()
        orders_raw = client.fetch_open_orders(limit=limit)
        fills_raw = client.fetch_fills(inst_type="SWAP", limit=limit)
    except (OkxClientError, httpx.HTTPError) as exc:
        raise RuntimeError(f"Failed to fetch OKX data for {account_id}: {exc}") from exc
    finally:
        client.close()

    equity, cash_balance, balance_models = extract_balances(
        balances_raw,
        account_id=account_id,
        base_currency=base_currency,
    )
    positions = normalize_positions(account_id, positions_raw)
    orders = normalize_orders(account_id, model_id, orders_raw)
    trades = normalize_trades(account_id, model_id, fills_raw)

    return {
        "account": {
            "account_id": account_id,
            "model_id": model_id,
            "base_currency": base_currency,
            "equity": equity,
            "cash_balance": cash_balance,
            "starting_equity": float(account_meta.get("starting_equity", 0.0)),
            "pnl": equity - float(account_meta.get("starting_equity", 0.0)),
        },
        "balances": [balance.dict() for balance in balance_models],
        "positions": [position.dict() for position in positions],
        "open_orders": [order.dict() for order in orders],
        "recent_trades": [trade.dict() for trade in trades],
    }
