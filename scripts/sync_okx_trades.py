"""
Synchronize OKX paper trading data into InfluxDB.

Usage:
    python scripts/sync_okx_trades.py [--limit 100]

The script authenticates to each account declared in config.OKX_ACCOUNTS,
fetches recent fills, balances, and positions, then writes the results into
InfluxDB via InfluxAccountRepository.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from accounts.models import Account
from accounts.repository import InfluxAccountRepository
from exchanges.base_client import ExchangeCredentials
from exchanges.okx.paper import OkxPaperClient, OkxClientError
from services.okx.transform import (
    extract_balances,
    normalize_orders,
    normalize_positions,
    normalize_trades,
)

try:
    from config import OKX_ACCOUNTS
except ImportError as exc:  # pragma: no cover - configuration must exist
    raise SystemExit("config.OKX_ACCOUNTS is required") from exc

STATE_PATH = Path("data/state/okx_sync_state.json")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync OKX paper trading data into InfluxDB.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of fills to fetch per account.")
    args = parser.parse_args(argv)

    if not OKX_ACCOUNTS:
        print("No OKX accounts configured. Check config.OKX_ACCOUNTS.", file=sys.stderr)
        return 1

    repo = InfluxAccountRepository()
    state = _load_state()

    for key, payload in OKX_ACCOUNTS.items():
        credentials = _build_credentials(payload)
        account_id, model_id = _resolve_account_identifiers(key, payload)
        start_cursor = state.get(account_id)

        client = OkxPaperClient()
        try:
            client.authenticate(credentials)
        except OkxClientError as exc:
            print(f"[{account_id}] failed to authenticate: {exc}", file=sys.stderr)
            continue

        try:
            _sync_account_data(
                client=client,
                repository=repo,
                account_id=account_id,
                model_id=model_id,
                starting_equity=float(payload.get("starting_equity", 10_000.0)),
                base_currency=payload.get("base_currency", "USDT"),
                limit=args.limit,
                start_cursor=start_cursor,
                state=state,
            )
        finally:
            client.close()

    _save_state(state)
    repo.close()
    return 0


def _build_credentials(payload: dict) -> ExchangeCredentials:
    return ExchangeCredentials(
        api_key=payload["api_key"],
        api_secret=payload["api_secret"],
        passphrase=payload.get("passphrase"),
    )


def _resolve_account_identifiers(key: str, payload: dict) -> Tuple[str, str]:
    account_id = payload.get("account_id") or f"okx_{key}"
    model_id = payload.get("model_id") or key
    return account_id, model_id


def _sync_account_data(
    *,
    client: OkxPaperClient,
    repository: InfluxAccountRepository,
    account_id: str,
    model_id: str,
    starting_equity: float,
    base_currency: str,
    limit: int,
    start_cursor: str | None,
    state: Dict[str, str],
) -> None:
    balances = client.fetch_balances()
    equity, cash_balance, balance_records = extract_balances(
        balances,
        account_id=account_id,
        base_currency=base_currency,
    )

    account = Account(
        account_id=account_id,
        model_id=model_id,
        base_currency=base_currency,
        starting_equity=starting_equity,
        cash_balance=cash_balance,
        equity=equity,
        pnl=equity - starting_equity,
        updated_at=datetime.now(tz=timezone.utc),
    )
    repository.upsert_account(account)
    repository.record_equity_point(account)

    for balance in balance_records:
        repository.record_balance(balance)

    positions = client.fetch_positions()
    for position in normalize_positions(account_id, positions):
        repository.record_position(position)

    fills = client.fetch_fills(limit=limit, after=start_cursor)
    new_cursor = start_cursor
    for trade in normalize_trades(account_id, model_id, fills):
        repository.record_trade(trade)
        try:
            trade_id_int = int(trade.trade_id)
            if new_cursor is None:
                new_cursor = str(trade_id_int)
            else:
                try:
                    if int(new_cursor) < trade_id_int:
                        new_cursor = str(trade_id_int)
                except ValueError:
                    new_cursor = str(trade_id_int)
        except ValueError:
            new_cursor = trade.trade_id

    if new_cursor:
        state[account_id] = new_cursor

    open_orders = client.fetch_open_orders(limit=limit)
    for order in normalize_orders(account_id, model_id, open_orders):
        repository.record_order(order)


def _load_state() -> Dict[str, str]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save_state(state: Dict[str, str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
