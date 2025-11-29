"""
Live data helpers for pulling OKX account state on demand.
"""

from __future__ import annotations

from typing import Dict

from exchanges.base_client import ExchangeCredentials
from exchanges.okx.paper import OkxPaperClient, OkxClientError
from services.okx.transform import extract_balances, normalize_orders, normalize_positions, normalize_trades


def fetch_account_snapshot(account_meta: dict, *, limit: int = 50, max_retries: int = 3) -> dict:
    """
    Fetch balances, positions, orders, and trades for a single OKX account.

    The returned dictionary contains sanitized primitives ready for JSON serialization.
    
    Args:
        account_meta: Account metadata dictionary
        limit: Maximum number of orders and trades to fetch
        max_retries: Maximum number of retries for connection errors
    """
    import time
    import random
    import logging
    
    logger = logging.getLogger(__name__)
    
    credentials = ExchangeCredentials(
        api_key=account_meta["api_key"],
        api_secret=account_meta["api_secret"],
        passphrase=account_meta.get("passphrase"),
    )
    account_id = account_meta.get("account_id") or account_meta.get("name", "unknown")
    model_id = account_meta.get("model_id") or account_id
    base_currency = account_meta.get("base_currency", "USDT")
    
    retry_count = 0
    base_delay = 1.0  # Initial delay time (seconds)
    max_delay = 10.0  # Maximum delay time (seconds)
    
    while retry_count <= max_retries:
        client = OkxPaperClient()
        try:
            client.authenticate(credentials)
            balances_raw = client.fetch_balances()
            positions_raw = client.fetch_positions()
            orders_raw = client.fetch_open_orders(limit=limit)
            fills_raw = client.fetch_fills(inst_type="SWAP", limit=limit)
            
            # If we successfully got data after a retry, log it
            if retry_count > 0:
                logger.info(f"Successfully fetched data for account {account_id} after {retry_count} retries")
            
            break  # Exit retry loop on success
            
        except (OkxClientError, ConnectionError, TimeoutError) as exc:
            error_message = str(exc)
            # Special handling for connection reset errors
            if "[WinError 10054]" in error_message or "远程主机强迫关闭了一个现有的连接" in error_message:
                logger.warning(f"Connection closed by remote host for account {account_id}: {error_message}")
            else:
                logger.warning(f"Failed to fetch OKX data for {account_id}: {error_message}")
            
            # If max retries reached, raise error
            if retry_count >= max_retries:
                client.close()
                raise RuntimeError(f"Failed to fetch OKX data for {account_id} after {max_retries} retries: {exc}") from exc
            
            # Calculate backoff time with jitter
            retry_count += 1
            delay = min(base_delay * (2 ** (retry_count - 1)) * (0.5 + random.random()), max_delay)
            logger.info(f"Attempting retry {retry_count} for account {account_id}, waiting {delay:.2f} seconds...")
            
            client.close()
            time.sleep(delay)
            
        except Exception as exc:
            client.close()
            raise RuntimeError(f"Unexpected error fetching OKX data for {account_id}: {exc}") from exc
    
    # Make sure we close the client when done

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
