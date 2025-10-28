"""
Adapter registry that maps model identifiers to concrete adapters.
"""

from __future__ import annotations

from typing import Dict, Iterable

from models.adapters.base import BaseModelAdapter


class AdapterRegistry:
    """Holds registered adapters keyed by model_id."""

    def __init__(self) -> None:
        self._adapters: Dict[str, BaseModelAdapter] = {}

    def register(self, adapter: BaseModelAdapter, *, overwrite: bool = False) -> None:
        """Register an adapter instance under its declared model_id."""
        key = adapter.model_id
        if not overwrite and key in self._adapters:
            raise KeyError(f"Adapter already registered for model_id '{key}'")
        self._adapters[key] = adapter

    def get(self, model_id: str) -> BaseModelAdapter:
        """Return adapter for given model id."""
        try:
            return self._adapters[model_id]
        except KeyError as exc:
            raise KeyError(f"No adapter registered for model_id '{model_id}'") from exc

    def list(self) -> Iterable[str]:
        """Return registered model identifiers."""
        return self._adapters.keys()
