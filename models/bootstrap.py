"""
Helper utilities to bootstrap adapter registry with default models.
"""

from __future__ import annotations

from models.adapters.deepseek import DeepSeekAdapter
from models.adapters.qwen import QwenAdapter
from models.registry import AdapterRegistry


def build_default_registry() -> AdapterRegistry:
    """Return registry pre-populated with DeepSeek and Qwen adapters."""
    registry = AdapterRegistry()
    registry.register(DeepSeekAdapter())
    registry.register(QwenAdapter())
    return registry
