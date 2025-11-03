"""
DeepSeek model adapter for trading signal generation.

Supports both offline heuristic mode (no API key) and live mode using the
DeepSeek API when `DEEPSEEK_API_KEY` is set.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict

import httpx

from models.adapters.base import BaseModelAdapter
from models.schemas import SignalRequest, SignalResponse
from models.utils import clamp_confidence, deterministic_decision

DEEPSEEK_ENDPOINT = "https://api.deepseek.com/v1/chat/completions"


class DeepSeekAdapter(BaseModelAdapter):
    """Adapter that calls DeepSeek's chat completion endpoint."""

    def __init__(
        self,
        *,
        model: str = "deepseek-trading-001",
        temperature: float = 0.2,
        api_key: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(model_id="deepseek-v1", temperature=temperature)
        self.remote_model = model
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self._client: httpx.AsyncClient | None = None
        self._timeout = timeout

    async def _invoke_model(self, prompt: str, request: SignalRequest) -> Dict[str, Any]:
        if not self.api_key:
            # Offline deterministic mode.
            await asyncio.sleep(0)
            return deterministic_decision(request, source="deepseek-offline")

        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)

        payload = {
            "model": self.remote_model,
            "temperature": self.temperature,
            "messages": [
                {"role": "system", "content": self._system_prompt()},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        response = await self._client.post(
            DEEPSEEK_ENDPOINT, headers=headers, json=payload
        )
        response.raise_for_status()
        data = response.json()
        try:
            content = data["choices"][0]["message"]["content"]
            raw = json.loads(content)
        except (KeyError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"DeepSeek response parse error: {exc}") from exc
        raw["provider"] = "deepseek"
        raw["model"] = self.remote_model
        raw["usage"] = data.get("usage", {})
        return raw

    def _parse_response(
        self, raw_output: Dict[str, Any], request: SignalRequest
    ) -> SignalResponse:
        decision = self._normalize_decision(raw_output.get("decision"))
        confidence = clamp_confidence(raw_output.get("confidence", 0.5))
        reasoning = raw_output.get("reasoning", "N/A")
        suggested_order = raw_output.get("order", {})
        if decision == "open_long":
            suggested_order.setdefault("side", "buy")
        elif decision == "open_short":
            suggested_order.setdefault("side", "sell")
        return SignalResponse(
            model_id=self.model_id,
            decision=decision,
            confidence=confidence,
            reasoning=reasoning,
            suggested_order=suggested_order,
            raw_output=raw_output,
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _system_prompt(self) -> str:
        return (
            "You are DeepSeek, an LLM helping a trading team manage OKX demo "
            "accounts. Respond strictly with JSON containing the keys: "
            "`decision` (one of open_long, open_short, hold, reduce, close), "
            "`confidence` (0-1), `reasoning`, and `order` (including side, size, "
            "price, type). Use open_long for entering long positions and "
            "open_short for entering short positions."
        )

    @staticmethod
    def _normalize_decision(value: Any) -> str:
        if not value:
            return "hold"
        decision = str(value).lower()
        if decision in {"buy", "long", "open_long"}:
            return "open_long"
        if decision in {"sell", "short", "open_short"}:
            return "open_short"
        if decision in {"reduce", "trim"}:
            return "reduce"
        if decision in {"close", "flat", "exit"}:
            return "close"
        return "hold"
