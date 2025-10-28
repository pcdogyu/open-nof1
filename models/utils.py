"""
Utility helpers used across model adapters.
"""

from __future__ import annotations

import math
from typing import Any, Dict

from models.schemas import SignalRequest


def clamp_confidence(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    """Clamp confidence values within [minimum, maximum]."""
    if math.isnan(value):
        return minimum
    return max(minimum, min(maximum, value))


def deterministic_decision(request: SignalRequest, *, source: str) -> Dict[str, Any]:
    """
    Produce a reproducible pseudo-decision when no remote model is configured.

    This is useful for local development and unit tests.
    """
    base = sum(ord(c) for c in request.market.instrument_id + request.model_id)
    bias = (request.market.price % 100) / 100
    ratio = request.risk.current_position / request.risk.max_position
    if ratio < 0.3 and bias > 0.5:
        decision = "buy"
    elif ratio > 0.8:
        decision = "reduce"
    else:
        decision = "hold"
    confidence = clamp_confidence(0.4 + bias / 2)
    return {
        "provider": source,
        "decision": decision,
        "confidence": confidence,
        "reasoning": (
            "Deterministic fallback based on position ratio and price bias."
        ),
        "order": {
            "instrument_id": request.market.instrument_id,
            "side": "buy" if decision == "buy" else "sell",
            "size": round(request.risk.max_position * 0.1, 4),
            "type": "limit",
            "price": round(request.market.price * (1 - 0.001), 2),
        },
    }
