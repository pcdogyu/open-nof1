"""
Abstract base class for LLM-driven signal adapters.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from models.schemas import SignalRequest, SignalResponse


class BaseModelAdapter(ABC):
    """Common behaviour for trading signal adapters."""

    model_id: str

    def __init__(self, model_id: str, *, temperature: float = 0.2) -> None:
        self.model_id = model_id
        self.temperature = temperature

    async def generate_signal(self, request: SignalRequest) -> SignalResponse:
        """
        Async entry point used by runtime.

        Subclasses override `_build_prompt` and `_invoke_model` to customise
        behaviour.
        """
        prompt = self._build_prompt(request)
        raw_output = await self._invoke_model(prompt, request)
        return self._parse_response(raw_output, request)

    def _build_prompt(self, request: SignalRequest) -> str:
        """Construct prompt string from request (override for custom logic)."""
        return (
            f"Model: {self.model_id}\n"
            f"Instrument: {request.market.instrument_id}\n"
            f"Price: {request.market.price}\n"
            f"Current position: {request.risk.current_position}\n"
            f"Max position: {request.risk.max_position}\n"
            f"Strategy hint: {request.strategy_hint or 'N/A'}\n"
            "Respond with JSON including decision, confidence, reasoning, order.\n"
        )

    @abstractmethod
    async def _invoke_model(self, prompt: str, request: SignalRequest) -> Dict[str, Any]:
        """Call the backing LLM and return raw JSON-compatible output."""

    @abstractmethod
    def _parse_response(
        self, raw_output: Dict[str, Any], request: SignalRequest
    ) -> SignalResponse:
        """Convert the raw output into a structured SignalResponse."""

    async def aclose(self) -> None:
        """Optional hook to release resources in async context."""
        return None
