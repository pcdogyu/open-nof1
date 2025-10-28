"""
Asynchronous runtime orchestrating trading signal generation across adapters.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Iterable, Sequence

from models.bootstrap import build_default_registry
from models.registry import AdapterRegistry
from models.schemas import SignalRequest, SignalResponse


class SignalRuntime:
    """Coordinates registered adapters and executes signal requests."""

    def __init__(
        self,
        registry: AdapterRegistry | None = None,
        *,
        auto_bootstrap: bool = True,
    ) -> None:
        if registry is None:
            registry = build_default_registry() if auto_bootstrap else AdapterRegistry()
        self.registry = registry
        self._exit_stack = AsyncExitStack()

    async def __aenter__(self) -> "SignalRuntime":
        await self._exit_stack.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self._exit_stack.__aexit__(exc_type, exc, tb)

    async def generate_signal(self, request: SignalRequest) -> SignalResponse:
        """Generate a single signal asynchronously."""
        adapter = self.registry.get(request.model_id)
        response = await adapter.generate_signal(request)
        return response

    async def batch_generate(
        self, requests: Iterable[SignalRequest]
    ) -> Sequence[SignalResponse]:
        """Execute multiple signal requests concurrently."""
        tasks = [asyncio.create_task(self.generate_signal(req)) for req in requests]
        return await asyncio.gather(*tasks)

    async def shutdown(self) -> None:
        """Gracefully close adapters that require async cleanup."""
        for model_id in list(self.registry.list()):
            adapter = self.registry.get(model_id)
            await adapter.aclose()
        await self._exit_stack.aclose()

    def register_adapter(self, adapter) -> None:
        """Convenience passthrough to the underlying registry."""
        self.registry.register(adapter)


def run_signal(request: SignalRequest, runtime: SignalRuntime) -> SignalResponse:
    """Synchronous helper for simple contexts."""

    async def _runner() -> SignalResponse:
        return await runtime.generate_signal(request)

    return asyncio.run(_runner())
