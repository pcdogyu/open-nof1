"""
Price band validator enforcing per-instrument limit and circuit controls.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from risk.schemas import MarketContext, OrderIntent, RiskEvaluation


@dataclass(slots=True)
class PriceBand:
    """Represents allowable price range for an instrument."""

    reference_price: float
    tolerance_pct: float

    @property
    def min_price(self) -> float:
        return self.reference_price * (1 - self.tolerance_pct)

    @property
    def max_price(self) -> float:
        return self.reference_price * (1 + self.tolerance_pct)


class PriceLimitValidator:
    """
    Validates order prices fall within configured price bands.

    Supports per-instrument overrides and dynamic updates triggered by market
    data refreshes.
    """

    def __init__(self, *, default_tolerance_pct: float = 0.02) -> None:
        if default_tolerance_pct <= 0 or default_tolerance_pct > 1:
            raise ValueError("default_tolerance_pct must be within (0, 1]")
        self._bands: Dict[str, PriceBand] = {}
        self._default_tolerance_pct = default_tolerance_pct

    def set_band(self, instrument_id: str, reference_price: float, tolerance_pct: float) -> None:
        """Configure price band for an instrument."""
        if tolerance_pct < 0 or tolerance_pct > 1:
            raise ValueError("tolerance_pct must be within [0, 1]")
        if reference_price <= 0:
            raise ValueError("reference_price must be positive")
        self._bands[instrument_id] = PriceBand(reference_price, tolerance_pct)

    def remove_band(self, instrument_id: str) -> None:
        """Clear band configuration for the instrument."""
        self._bands.pop(instrument_id, None)

    def update_from_market(self, market: MarketContext, tolerance_pct: Optional[float] = None) -> None:
        """
        Refresh the price band using latest market context.

        Args:
            market: Latest market snapshot.
            tolerance_pct: Optional override for tolerance percentage; if not
                provided the existing tolerance (when present) is reused.
        """
        current_band = self._bands.get(market.instrument_id)
        pct = tolerance_pct if tolerance_pct is not None else (
            current_band.tolerance_pct if current_band else self._default_tolerance_pct
        )
        self.set_band(market.instrument_id, market.mid_price or market.last_price, pct)

    def validate(self, order: OrderIntent, evaluation: RiskEvaluation) -> None:
        """Mutates the evaluation with violations when price constraints fail."""
        band = self._bands.get(order.instrument_id)
        if band is None:
            return
        if order.order_type == "market":
            return
        if order.price is None:
            evaluation.add_violation(
                "PRICE_LIMIT_MISSING_PRICE",
                "Limit order requires explicit price.",
                instrument_id=order.instrument_id,
            )
            return
        if not (band.min_price <= order.price <= band.max_price):
            evaluation.add_violation(
                "PRICE_LIMIT_BREACHED",
                "Order price exceeds configured tolerance band.",
                instrument_id=order.instrument_id,
                order_price=order.price,
                min_price=band.min_price,
                max_price=band.max_price,
            )
