"""
Lightweight order validation helpers executed prior to risk evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Set

from risk.schemas import OrderIntent


class OrderValidationError(ValueError):
    """Raised when an order intent fails preliminary validation."""

    def __init__(self, violations: Sequence[str]) -> None:
        super().__init__("; ".join(violations))
        self.violations = list(violations)


@dataclass(slots=True)
class BasicOrderValidator:
    """
    Perform syntactic validation on normalized order intents before they reach
    the risk engine. Checks include positive size, supported order types/sides,
    and optional instrument allowlists.
    """

    min_size: float = 1e-6
    allowed_sides: Set[str] = field(default_factory=lambda: {"buy", "sell"})
    allowed_types: Set[str] = field(default_factory=lambda: {"market", "limit"})
    instrument_allowlist: Optional[Set[str]] = None

    def validate(self, intent: OrderIntent) -> None:
        violations: List[str] = []

        if intent.size is None or intent.size <= 0:
            violations.append("Order size must be greater than zero.")
        elif intent.size < self.min_size:
            violations.append(f"Order size {intent.size} is below minimum threshold {self.min_size}.")

        if intent.side not in self.allowed_sides:
            violations.append(f"Unsupported side '{intent.side}'. Allowed: {sorted(self.allowed_sides)}.")

        if intent.order_type not in self.allowed_types:
            violations.append(f"Unsupported order type '{intent.order_type}'. Allowed: {sorted(self.allowed_types)}.")

        if self.instrument_allowlist is not None and intent.instrument_id not in self.instrument_allowlist:
            violations.append(f"Instrument '{intent.instrument_id}' is not in the execution allowlist.")

        if intent.order_type == "limit" and intent.price is None:
            violations.append("Limit order requires an explicit price.")

        if violations:
            raise OrderValidationError(violations)


def ensure_valid_order(intent: OrderIntent, validators: Iterable[BasicOrderValidator] | None = None) -> None:
    """
    Run the provided validators (or a default BasicOrderValidator) against the
    order intent. Raises OrderValidationError on failure.
    """
    checks = list(validators) if validators is not None else [BasicOrderValidator()]
    for validator in checks:
        validator.validate(intent)
