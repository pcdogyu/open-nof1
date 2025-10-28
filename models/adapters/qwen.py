"""
Qwen (Qianwen) model adapter for trading signal generation.

This adapter mirrors the DeepSeek implementation but targets the Qwen API.
When `QWEN_API_KEY` is absent the adapter falls back to deterministic signals.
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

QWEN_ENDPOINT = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"


class QwenAdapter(BaseModelAdapter):
    """Adapter for Qwen/Qianwen trading signal prompts."""

    def __init__(
        self,
        *,
        model: str = "qwen-max",
        temperature: float = 0.2,
        api_key: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(model_id="qwen-v1", temperature=temperature)
        self.remote_model = model
        self.api_key = api_key or os.getenv("QWEN_API_KEY")
        self._client: httpx.AsyncClient | None = None
        self._timeout = timeout

    async def _invoke_model(self, prompt: str, request: SignalRequest) -> Dict[str, Any]:
        if not self.api_key:
            await asyncio.sleep(0)
            return deterministic_decision(request, source="qwen-offline")

        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)

        payload = {
            "model": self.remote_model,
            "input": {
                "messages": [
                    {"role": "system", "content": self._system_prompt()},
                    {"role": "user", "content": prompt},
                ]
            },
            "parameters": {
                "temperature": self.temperature,
                "result_format": "json",
            },
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        response = await self._client.post(QWEN_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        try:
            output = data["output"]["text"]
            raw = json.loads(output)
        except (KeyError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Qwen response parse error: {exc}") from exc
        raw["provider"] = "qwen"
        raw["model"] = self.remote_model
        raw["usage"] = data.get("usage", {})
        return raw

    def _parse_response(
        self, raw_output: Dict[str, Any], request: SignalRequest
    ) -> SignalResponse:
        decision = raw_output.get("decision") or "hold"
        confidence = clamp_confidence(raw_output.get("confidence", 0.5))
        reasoning = raw_output.get("reasoning", "N/A")
        suggested_order = raw_output.get("order", {})
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
            "You are Qwen, supporting quantitative trading decisions. Respond"
            " strictly with JSON keys: decision, confidence, reasoning, order."
        )
