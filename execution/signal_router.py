"""
Utilities for converting model signals into executable order intents.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from accounts.models import Account, Position
from accounts.portfolio_registry import PortfolioRecord, PortfolioRegistry
from models.schemas import MarketSnapshot, SignalResponse
from risk.schemas import MarketContext, OrderIntent, PortfolioMetrics

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RoutedOrder:
    """Container describing an order ready for risk evaluation and execution."""

    intent: OrderIntent
    portfolio: PortfolioRecord
    account_meta: Dict[str, str]
    signal: SignalResponse
    market: MarketContext
    portfolio_metrics: PortfolioMetrics


class SignalRouter:
    """
    Convert model signals into normalized order intents tied to a portfolio.

    The router looks up the target portfolio by model id, applies defaults when
    the model omits order attributes, and enriches the response with market and
    portfolio context required by the downstream risk engine.
    """

    def __init__(
        self,
        registry: PortfolioRegistry,
        *,
        okx_accounts: Optional[Dict[str, Dict[str, str]]] = None,
        instrument_fallbacks: Optional[Sequence[str]] = None,
    ) -> None:
        self._registry = registry
        self._okx_accounts = okx_accounts or {}
        self._instrument_fallbacks = list(instrument_fallbacks or [])

    def route(
        self,
        response: SignalResponse,
        *,
        market: MarketSnapshot,
        account: Account,
        positions: Sequence[Position],
        account_meta: Optional[Dict[str, str]] = None,
    ) -> Optional[RoutedOrder]:
        """
        Return a routed order for the supplied signal or None when no trade
        should be executed.
        """
        decision = (response.decision or "hold").lower()
        if decision in {"hold"}:
            return None

        portfolio = self._registry.find_by_model(response.model_id)
        if portfolio is None:
            logger.warning("No portfolio registered for model %s; skipping", response.model_id)
            return None

        meta = account_meta or self._resolve_account_meta(response.model_id)
        if meta is None:
            logger.warning("No account metadata available for model %s; skipping", response.model_id)
            return None

        order_payload = response.suggested_order or {}
        instrument_id = self._extract_instrument(order_payload, market, meta)
        if not instrument_id:
            logger.warning("Signal from %s missing instrument information; skipping", response.model_id)
            return None

        side = self._extract_side(order_payload, decision)
        order_type = (order_payload.get("order_type") or order_payload.get("type") or "market").lower()
        size = self._extract_size(order_payload)
        if size is None or size <= 0:
            logger.warning("Signal from %s produced non-positive size; skipping", response.model_id)
            return None

        price = self._extract_price(order_payload)
        reference_price = market.price or price or 0.0
        if order_type == "limit" and price is None and reference_price > 0:
            price = reference_price

        intent = OrderIntent(
            portfolio_id=portfolio.portfolio_id,
            instrument_id=instrument_id,
            side=side,
            order_type=order_type,
            size=size,
            price=price,
            metadata={
                "model_id": response.model_id,
                "confidence": response.confidence,
                "reasoning": response.reasoning,
                "account_id": meta.get("account_id"),
            },
        )

        market_context = MarketContext(
            instrument_id=instrument_id,
            last_price=reference_price if reference_price > 0 else (price or 0.0),
            mid_price=market.metadata.get("mid_price") if isinstance(market.metadata, dict) else None,
            best_bid=market.metadata.get("best_bid") if isinstance(market.metadata, dict) else None,
            best_ask=market.metadata.get("best_ask") if isinstance(market.metadata, dict) else None,
            metadata={
                **(market.metadata or {}),
                "model_id": response.model_id,
                "decision": response.decision,
            },
        )

        portfolio_metrics = self._derive_portfolio_metrics(portfolio, account, positions, market_context.last_price)

        return RoutedOrder(
            intent=intent,
            portfolio=portfolio,
            account_meta=meta,
            signal=response,
            market=market_context,
            portfolio_metrics=portfolio_metrics,
        )

    def _resolve_account_meta(self, model_id: str) -> Optional[Dict[str, str]]:
        for payload in self._okx_accounts.values():
            if payload.get("model_id") == model_id:
                return payload
        return None

    def _extract_instrument(
        self,
        payload: Dict[str, object],
        market: MarketSnapshot,
        account_meta: Dict[str, str],
    ) -> Optional[str]:
        instrument = payload.get("instrument_id") or payload.get("inst_id")
        if instrument:
            return str(instrument)
        if market.instrument_id:
            return market.instrument_id
        if account_meta.get("default_instrument"):
            return str(account_meta["default_instrument"])
        return self._instrument_fallbacks[0] if self._instrument_fallbacks else None

    @staticmethod
    def _extract_side(payload: Dict[str, object], decision: str) -> str:
        side = (payload.get("side") or payload.get("direction") or "").lower()
        if side in {"buy", "sell"}:
            return side
        if decision in {"open_long", "reduce"}:
            return "buy"
        if decision in {"open_short", "close"}:
            return "sell"
        return "buy"

    @staticmethod
    def _extract_size(payload: Dict[str, object]) -> Optional[float]:
        for key in ("size", "quantity", "qty", "amount"):
            if key in payload:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    return None
        return None

    @staticmethod
    def _extract_price(payload: Dict[str, object]) -> Optional[float]:
        for key in ("price", "px"):
            if key in payload and payload[key] not in (None, ""):
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    return None
        return None

    @staticmethod
    def _derive_portfolio_metrics(
        portfolio: PortfolioRecord,
        account: Account,
        positions: Sequence[Position],
        reference_price: float,
    ) -> PortfolioMetrics:
        notional = 0.0
        for position in positions:
            mark = position.mark_price or reference_price
            if mark:
                notional += abs(position.quantity * mark)

        unrealized_pct = 0.0
        if account.starting_equity > 0:
            unrealized_pct = ((account.equity - account.starting_equity) / account.starting_equity) * 100

        return PortfolioMetrics(
            portfolio_id=portfolio.portfolio_id,
            unrealized_pnl_pct=unrealized_pct,
            daily_realized_pnl=0.0,
            position_notional=notional,
            risk_metadata={
                "account_equity": account.equity,
                "account_cash": account.cash_balance,
            },
        )
