"""
Utilities for loading, saving, and formatting AI prompt templates.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

PROMPT_TEMPLATE_PATH: Final = Path("data/state/prompt_template.txt")
DEFAULT_PROMPT_TEMPLATE: Final = (
    "Model: {model_id}\n"
    "Instrument: {instrument_id}\n"
    "Price: {price}\n"
    "Spread: {spread}\n"
    "Current position: {current_position}\n"
    "Max position: {max_position}\n"
    "Cash available: {cash_available}\n"
    "Open positions:\n"
    "{positions}\n"
    "Risk notes:\n"
    "{risk_notes}\n"
    "Strategy hint: {strategy_hint}\n"
    "Respond with structured JSON describing decision, confidence, reasoning, and order parameters.\n"
)


def get_prompt_template() -> str:
    """Return the persisted prompt template text or the built-in default."""
    try:
        return PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return DEFAULT_PROMPT_TEMPLATE


def save_prompt_template(value: str) -> str:
    """Persist a new template string and return the sanitized value."""
    sanitized = value.strip() or DEFAULT_PROMPT_TEMPLATE
    PROMPT_TEMPLATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROMPT_TEMPLATE_PATH.write_text(sanitized, encoding="utf-8")
    return sanitized
