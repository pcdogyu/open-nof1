"""
Helpers for attaching OKX take-profit/stop-loss parameters to order payloads.
"""

from __future__ import annotations

from typing import Literal, Mapping

OrderSide = Literal["buy", "sell"]


def _format_price(value: float) -> str:
    return f"{value:.8f}".rstrip("0").rstrip(".") or "0"


def _sanitize_price(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return numeric


def _compute_take_profit(entry_price: float, side: OrderSide, pct: float) -> float | None:
    if pct <= 0:
        return None
    factor = pct / 100.0
    if side == "buy":
        return entry_price * (1 + factor)
    return entry_price * (1 - factor)


def _compute_stop_loss(entry_price: float, side: OrderSide, pct: float) -> float | None:
    if pct <= 0:
        return None
    factor = pct / 100.0
    if side == "buy":
        return entry_price * (1 - factor)
    return entry_price * (1 + factor)


def apply_bracket_targets(
    payload: Mapping[str, object],
    *,
    side: str,
    entry_price: float | None,
    take_profit_pct: float,
    stop_loss_pct: float,
) -> dict:
    """
    Return a payload that includes OKX tp/sl parameters derived from the provided risk config.
    """
    normalized_side = side.lower()
    if normalized_side not in {"buy", "sell"}:
        return dict(payload)
    price = _sanitize_price(entry_price)
    if price is None:
        return dict(payload)
    tp_price = _compute_take_profit(price, normalized_side, max(0.0, float(take_profit_pct)))
    sl_price = _compute_stop_loss(price, normalized_side, max(0.0, float(stop_loss_pct)))
    if tp_price is not None and tp_price <= 0:
        tp_price = None
    if sl_price is not None and sl_price <= 0:
        sl_price = None
    if tp_price is None and sl_price is None:
        return dict(payload)

    enriched = dict(payload)
    if tp_price is not None:
        tp_formatted = _format_price(tp_price)
        enriched["tpTriggerPx"] = tp_formatted
        enriched["tpOrdPx"] = tp_formatted
        enriched["tpTriggerPxType"] = "last"
    if sl_price is not None:
        sl_formatted = _format_price(sl_price)
        enriched["slTriggerPx"] = sl_formatted
        # Use market execution once triggered to maximise exit probability.
        enriched["slOrdPx"] = "-1"
        enriched["slTriggerPxType"] = "last"
    return enriched
