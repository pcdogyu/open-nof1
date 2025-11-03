"""
Helper functions for transforming OKX API responses into domain models.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Sequence, Tuple

from accounts.models import Balance, Order, Position, Trade


def extract_balances(
    balances: Sequence[dict],
    *,
    account_id: str,
    base_currency: str,
) -> Tuple[float, float, List[Balance]]:
    if not balances:
        return 0.0, 0.0, []
    envelope = balances[0]
    total_equity = float(envelope.get("totalEq", 0.0))
    balance_records: List[Balance] = []
    cash_balance = 0.0
    for detail in envelope.get("details", []):
        currency = detail.get("ccy", "")
        if currency == base_currency:
            cash_balance = float(detail.get("cashBal", 0.0))
        balance_records.append(
            Balance(
                balance_id=f"{account_id}-{currency}",
                account_id=account_id,
                currency=currency,
                total=float(detail.get("eq", detail.get("cashBal", 0.0))),
                available=float(detail.get("availBal", 0.0)),
                frozen=float(detail.get("frozenBal", 0.0)),
                equity=float(detail.get("eq", 0.0)),
                updated_at=_utc_now(),
            )
        )
    return total_equity, cash_balance, balance_records


def normalize_positions(
    account_id: str,
    raw_positions: Iterable[dict],
) -> List[Position]:
    cleaned: List[Position] = []
    for item in raw_positions:
        quantity = float(item.get("pos", 0.0))
        if not quantity:
            continue
        position_id = str(item.get("posId") or f"{account_id}-{item.get('instId')}")
        cleaned.append(
            Position(
                position_id=position_id,
                account_id=account_id,
                instrument_id=item.get("instId", ""),
                side=item.get("posSide") or item.get("side", "long"),
                quantity=abs(quantity),
                entry_price=float(item.get("avgPx", 0.0)),
                mark_price=_optional_float(item.get("markPx")),
                leverage=_optional_float(item.get("lever")),
                unrealized_pnl=_optional_float(item.get("upl")),
                updated_at=_utc_now(),
            )
        )
    return cleaned


def normalize_trades(
    account_id: str,
    model_id: str,
    raw_fills: Iterable[dict],
) -> List[Trade]:
    trades: List[Trade] = []
    for fill in raw_fills or []:
        trade_id = str(fill.get("billId") or fill.get("tradeId") or fill.get("ordId"))
        if not trade_id:
            continue
        executed_at = _parse_timestamp(fill.get("fillTime") or fill.get("ts"))
        trades.append(
            Trade(
                trade_id=trade_id,
                account_id=account_id,
                model_id=model_id,
                instrument_id=fill.get("instId", ""),
                side=fill.get("side", ""),
                quantity=float(fill.get("sz", 0.0)),
                price=float(fill.get("fillPx", 0.0)),
                fee=_optional_float(fill.get("fee")),
                realized_pnl=_optional_float(fill.get("pnl")),
                executed_at=executed_at,
            )
        )
    return trades


def normalize_orders(
    account_id: str,
    model_id: str,
    raw_orders: Iterable[dict],
) -> List[Order]:
    orders: List[Order] = []
    for order in raw_orders or []:
        order_id = str(order.get("ordId") or order.get("orderId") or order.get("clOrdId"))
        if not order_id:
            continue
        created_at = _parse_timestamp(order.get("cTime") or order.get("uTime"))
        updated_at = _parse_timestamp(order.get("uTime") or order.get("cTime"))
        orders.append(
            Order(
                order_id=order_id,
                account_id=account_id,
                model_id=model_id,
                instrument_id=order.get("instId", ""),
                side=order.get("side", ""),
                order_type=order.get("ordType", ""),
                size=float(order.get("sz", 0.0)),
                filled_size=float(order.get("accFillSz", 0.0)),
                price=_optional_float(order.get("px")),
                average_price=_optional_float(order.get("avgPx")),
                state=order.get("state", ""),
                created_at=created_at,
                updated_at=updated_at,
            )
        )
    return orders


def _parse_timestamp(value: str | None) -> datetime:
    if value is None:
        return _utc_now()
    try:
        timestamp_ms = int(value)
    except (TypeError, ValueError):
        return _utc_now()
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
