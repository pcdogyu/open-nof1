"""
Simple JSON-backed key-value storage for runtime settings.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Any, Dict

logger = logging.getLogger(__name__)

SETTINGS_FILE = Path("data/settings_store.json")
_STORE_LOCK = Lock()


def _read_store() -> Dict[str, Any]:
    if not SETTINGS_FILE.exists():
        return {}
    try:
        raw = SETTINGS_FILE.read_text(encoding="utf-8")
    except OSError as exc:
        logger.debug("Unable to read settings store: %s", exc)
        return {}
    if not raw.strip():
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Settings store is corrupted; ignoring contents.")
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _write_store(data: Dict[str, Any]) -> None:
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_path = SETTINGS_FILE.with_suffix(".tmp")
    try:
        temp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(SETTINGS_FILE)
    except OSError as exc:
        logger.error("Failed to write settings store: %s", exc)


def load_namespace(namespace: str) -> Dict[str, Any]:
    """
    Return a shallow copy of the namespace payload stored on disk.
    Missing namespaces return an empty dict.
    """

    with _STORE_LOCK:
        data = _read_store()
        payload = data.get(namespace, {})
        return dict(payload) if isinstance(payload, dict) else {}


def save_namespace(namespace: str, payload: Dict[str, Any]) -> None:
    """
    Persist the namespace payload atomically.
    """

    if not isinstance(payload, dict):
        raise ValueError("payload must be a dictionary.")
    with _STORE_LOCK:
        data = _read_store()
        data[namespace] = payload
        _write_store(data)
