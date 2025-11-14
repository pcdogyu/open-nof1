"""
Abstract base class for LLM-driven signal adapters.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Sequence

from models.schemas import SignalRequest, SignalResponse
from services.webapp import prompt_templates


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
        positions_text = self._format_positions_for_prompt(request.positions)
        template = prompt_templates.get_prompt_template()
        context = _PromptContext(
            model_id=self.model_id,
            instrument_id=request.market.instrument_id,
            price=request.market.price,
            spread=request.market.spread or "",
            current_position=request.risk.current_position,
            max_position=request.risk.max_position,
            cash_available=request.risk.cash_available,
            positions=positions_text,
            strategy_hint=request.strategy_hint or "N/A",
            risk_notes=self._format_metadata_block(request.risk.notes),
            market_metadata=self._format_metadata_block(request.market.metadata),
        )
        return self._apply_template(template, context)

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

    def _format_positions_for_prompt(self, positions: Sequence[Dict[str, Any]]) -> str:
        """Return a compact, human-readable summary of open positions for prompts."""
        if not positions:
            return "None"
        lines: list[str] = []
        for position in positions[:10]:
            instrument = position.get("instrument_id", "N/A")
            side = str(position.get("side", "n/a")).upper()
            quantity = position.get("quantity", "n/a")
            entry = position.get("entry_price", "n/a")
            mark = position.get("mark_price", "n/a")
            pnl = position.get("unrealized_pnl", "n/a")
            lines.append(
                f"- {instrument} {side} qty={quantity} entry={entry} mark={mark} pnl={pnl}"
            )
        if len(positions) > 10:
            lines.append(f"... and {len(positions) - 10} more positions.")
        return "\n".join(lines)

    def _format_metadata_block(self, metadata: Dict[str, Any] | None) -> str:
        if not metadata:
            return "None"
        lines: list[str] = []
        for key, value in metadata.items():
            lines.append(f"- {key}: {value}")
        return "\n".join(lines)

    def _apply_template(self, template: str, context: "._PromptContext") -> str:
        try:
            return template.format_map(context)
        except Exception:
            return prompt_templates.DEFAULT_PROMPT_TEMPLATE.format_map(context)


class _PromptContext(dict):
    """Gracefully handle missing keys during template formatting."""

    def __missing__(self, key: str) -> str:
        return f"{{{key}}}"
