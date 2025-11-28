"""
HTTP route handlers for the FastAPI web application.
"""

from __future__ import annotations

import ast
import json
import logging
import math
import pprint
import base64
import asyncio
import urllib.parse
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Sequence

import httpx
from fastapi import APIRouter, Depends, HTTPException, Form, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, validator

from accounts.models import Account, AccountSnapshot, Balance, Order, Position, Trade
from accounts.portfolio_registry import PortfolioRecord, PortfolioRegistry
from accounts.repository import AccountRepository
from data_pipeline.influx import InfluxConfig, InfluxWriter
from services.analytics.leaderboard import get_leaderboard, refresh_leaderboard_cache
from services.jobs import scheduler as scheduler_module
from exchanges.base_client import ExchangeCredentials
from exchanges.okx.paper import OkxPaperClient, OkxClientError
from services.okx.catalog import (
    load_catalog_cache,
    refresh_okx_instrument_catalog,
    search_instrument_catalog,
)
from services.okx.live import fetch_account_snapshot
from services.okx.brackets import apply_bracket_targets
from services.storage.settings_store import (
    load_namespace as load_settings_namespace,
    save_namespace as save_settings_namespace,
)
from services.webapp import prompt_templates
from services.webapp.dependencies import get_account_repository, get_portfolio_registry
from services.liquidations import waves as wave_detector

try:  # optional during tests
    from influxdb_client import InfluxDBClient
except ImportError:  # pragma: no cover
    InfluxDBClient = None  # type: ignore

try:
    from websocket import set_instruments as set_websocket_instruments
except ImportError:  # pragma: no cover
    def set_websocket_instruments(_: List[str]) -> None:
        logger.debug("websocket instrumentation unavailable; skipping stream update.")

CONFIG_PATH = Path("config.py")
MARKET_DEPTH_ATTACHMENT_DIR = Path("data/attachments/market_depth")
_DEFAULT_LIQUIDATION_INTERVAL = 120
ORDERBOOK_HISTORY_LOOKBACK_HOURS = 4
MAX_HISTORY_POINTS_PER_INSTRUMENT = ORDERBOOK_HISTORY_LOOKBACK_HOURS * 1800  # 4h @ 8 samples/min
_ORDERBOOK_PERSIST_TRACK: dict[str, str] = {}

try:
    from config import (
        MODEL_DEFAULTS,
        AI_INTERACTION_INTERVAL,
        MARKET_SYNC_INTERVAL,
        OKX_ACCOUNTS,
        PIPELINE_POLL_INTERVAL,
        TRADABLE_INSTRUMENTS,
        LIQUIDATION_INSTRUMENT_OVERRIDES,
    )
    try:
        from config import OKX_CACHE_TTL_SECONDS as _OKX_TTL
    except Exception:  # name missing or other
        _OKX_TTL = 600
    try:
        from config import RISK_SETTINGS as _RISK_DEFAULTS  # type: ignore[attr-defined]
    except Exception:
        _RISK_DEFAULTS = {}
    try:
        from config import LIQUIDATION_CHECK_INTERVAL as _LIQ_INTERVAL
    except Exception:
        LIQUIDATION_CHECK_INTERVAL = _DEFAULT_LIQUIDATION_INTERVAL
    else:
        LIQUIDATION_CHECK_INTERVAL = _LIQ_INTERVAL
    _LIQUIDATION_OVERRIDE_DEFAULTS = dict(LIQUIDATION_INSTRUMENT_OVERRIDES or {})
except ImportError:  # pragma: no cover - fallback for test envs
    MODEL_DEFAULTS = {
        "deepseek-v1": {
            "display_name": "DeepSeek 交易模型",
            "provider": "DeepSeek",
            "enabled": True,
            "api_key": "",
        },
        "qwen-v1": {
            "display_name": "Qwen 千问模型",
            "provider": "阿里云通义",
            "enabled": True,
            "api_key": "",
        },
    }
    OKX_ACCOUNTS = {}
    TRADABLE_INSTRUMENTS = [
        "XRP-USDT-SWAP",
        "BNB-USDT-SWAP",
        "BTC-USDT-SWAP",
        "ETH-USDT-SWAP",
        "SOL-USDT-SWAP",
        "DOGE-USDT-SWAP",
    ]
    PIPELINE_POLL_INTERVAL = 120
    MARKET_SYNC_INTERVAL = 60
    AI_INTERACTION_INTERVAL = 300
    LIQUIDATION_CHECK_INTERVAL = _DEFAULT_LIQUIDATION_INTERVAL
    _OKX_TTL = 600
    _RISK_DEFAULTS = {}
    _LIQUIDATION_OVERRIDE_DEFAULTS = {}
router = APIRouter()
_OKX_CACHE_TTL_SECONDS = int(_OKX_TTL)


def _format_okx_error(exc: OkxClientError) -> str:
    """Return a verbose OKX error string including nested data details."""
    payload = getattr(exc, "payload", None) or {}
    code = payload.get("code")
    msg = payload.get("msg") or payload.get("sMsg")
    detail = str(exc)
    extra_bits: list[str] = []
    if code or msg:
        extra_bits.append(f"code={code}, msg={msg}")
    data_entries = payload.get("data") or []
    formatted_entries: list[str] = []
    for entry in data_entries:
        if isinstance(entry, dict):
            entry_code = entry.get("sCode") or entry.get("code")
            entry_msg = entry.get("sMsg") or entry.get("msg")
            if entry_code or entry_msg:
                parts: list[str] = []
                if entry_code:
                    parts.append(str(entry_code))
                if entry_msg:
                    parts.append(str(entry_msg))
                formatted_entries.append(": ".join(parts))
            else:
                formatted_entries.append(str(entry))
        else:
            formatted_entries.append(str(entry))
    if formatted_entries:
        extra_bits.append("details=" + " | ".join(formatted_entries))
    if extra_bits:
        detail = f"{detail} ({'; '.join(extra_bits)})"
    return detail


SIGNAL_LOG_PATH = Path("data/logs/ai_signals.jsonl")
logger = logging.getLogger(__name__)
_MODEL_REGISTRY_LOCK = Lock()
_MODEL_REGISTRY: dict[str, dict] = {
    model_id: {
        **meta,
        "model_id": model_id,
        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
    }
    for model_id, meta in MODEL_DEFAULTS.items()
}


def _get_enabled_model_ids() -> set[str]:
    """Return a set of model IDs that are currently enabled."""
    with _MODEL_REGISTRY_LOCK:
        return {
            model_id
            for model_id, record in _MODEL_REGISTRY.items()
            if record.get("enabled")
        }
_PIPELINE_SETTINGS_LOCK = Lock()
_PIPELINE_SETTINGS: dict[str, object] = {
    "tradable_instruments": list(TRADABLE_INSTRUMENTS),
    "poll_interval": int(PIPELINE_POLL_INTERVAL),
    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    "liquidation_overrides": {},
}
_pipeline_store = load_settings_namespace("pipeline")
if _pipeline_store:
    instruments_override_raw = _pipeline_store.get("tradable_instruments") or []
    instruments_override = [
        str(inst).upper()
        for inst in instruments_override_raw
        if str(inst).strip()
    ]
    if instruments_override:
        _PIPELINE_SETTINGS["tradable_instruments"] = instruments_override
        TRADABLE_INSTRUMENTS = instruments_override  # type: ignore[assignment]
    poll_override = _pipeline_store.get("poll_interval")
    try:
        poll_value = int(poll_override)
    except (TypeError, ValueError):
        poll_value = None
    if poll_value and poll_value > 0:
        _PIPELINE_SETTINGS["poll_interval"] = poll_value
        PIPELINE_POLL_INTERVAL = poll_value  # type: ignore[assignment]
    overrides_payload = _pipeline_store.get("liquidation_overrides")
    if isinstance(overrides_payload, dict):
        _PIPELINE_SETTINGS["liquidation_overrides"] = overrides_payload
    updated_at_override = _pipeline_store.get("updated_at")
    if isinstance(updated_at_override, str) and updated_at_override.strip():
        _PIPELINE_SETTINGS["updated_at"] = updated_at_override

_SCHEDULER_SETTINGS_LOCK = Lock()
_SCHEDULER_SETTINGS: dict[str, object] = {
    "market_interval": int(MARKET_SYNC_INTERVAL),
    "ai_interval": int(AI_INTERACTION_INTERVAL),
    "liquidation_interval": int(LIQUIDATION_CHECK_INTERVAL),
    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
}
_scheduler_store = load_settings_namespace("scheduler")
if _scheduler_store:
    for key in ("market_interval", "ai_interval", "liquidation_interval"):
        try:
            value = int(_scheduler_store.get(key))
        except (TypeError, ValueError):
            value = None
        if value and value > 0:
            _SCHEDULER_SETTINGS[key] = value
            if key == "market_interval":
                MARKET_SYNC_INTERVAL = value  # type: ignore[assignment]
            elif key == "ai_interval":
                AI_INTERACTION_INTERVAL = value  # type: ignore[assignment]
            elif key == "liquidation_interval":
                LIQUIDATION_CHECK_INTERVAL = value  # type: ignore[assignment]
    updated_at_override = _scheduler_store.get("updated_at")
    if isinstance(updated_at_override, str) and updated_at_override.strip():
        _SCHEDULER_SETTINGS["updated_at"] = updated_at_override

# Cache for boot-time orderbook priming so first requests can avoid hitting upstreams.
_ORDERBOOK_WARM_CACHE: dict | None = None
_OKX_INSTRUMENT_MAP: dict[str, dict] | None = None

_DEFAULT_RISK_SETTINGS = {
    "price_tolerance_pct": 0.02,
    "max_drawdown_pct": 8.0,
    "max_loss_absolute": 1500.0,
    "cooldown_seconds": 600,
    "min_notional_usd": 50.0,
    "max_order_notional_usd": 0.0,
    "max_position": 0.0,
    "take_profit_pct": 0.0,
    "stop_loss_pct": 0.0,
    "default_leverage": 2,
    "max_leverage": 125,
    "pyramid_max_orders": 5,
    "pyramid_reentry_pct": 2.0,
    "liquidation_notional_threshold": 50_000.0,
    "liquidation_same_direction_count": 4,
    "liquidation_opposite_count": 3,
    "liquidation_silence_seconds": 300,
    "max_capital_pct_per_instrument": 0.1,
}


def _get_okx_instrument_meta(inst_id: str) -> dict | None:
    """Return cached OKX instrument metadata such as lot/min size."""
    global _OKX_INSTRUMENT_MAP
    if not inst_id:
        return None
    if _OKX_INSTRUMENT_MAP is None:
        try:
            _OKX_INSTRUMENT_MAP = {
                str(entry.get("instId", "")).upper(): entry
                for entry in load_catalog_cache()
                if entry.get("instId")
            }
        except Exception:
            _OKX_INSTRUMENT_MAP = {}
    return _OKX_INSTRUMENT_MAP.get(str(inst_id).upper())


def _try_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sanitize_risk_config(raw: Optional[Dict[str, object]]) -> dict[str, float | int]:
    """Normalize risk configuration values and enforce bounds."""
    config = dict(_DEFAULT_RISK_SETTINGS)
    if isinstance(raw, dict):
        for key in (
            "price_tolerance_pct",
            "max_drawdown_pct",
            "max_loss_absolute",
            "cooldown_seconds",
            "min_notional_usd",
            "max_order_notional_usd",
            "max_position",
            "take_profit_pct",
            "stop_loss_pct",
            "default_leverage",
            "max_leverage",
            "pyramid_max_orders",
            "pyramid_reentry_pct",
            "liquidation_notional_threshold",
            "liquidation_same_direction_count",
            "liquidation_opposite_count",
            "liquidation_silence_seconds",
            "max_capital_pct_per_instrument",
        ):
            if key in raw:
                config[key] = raw[key]  # type: ignore[assignment]
    price = float(config["price_tolerance_pct"])
    drawdown = float(config["max_drawdown_pct"])
    max_loss = float(config["max_loss_absolute"])
    cooldown = int(config["cooldown_seconds"])
    min_notional = float(config["min_notional_usd"])
    take_profit = float(config["take_profit_pct"])
    stop_loss = float(config["stop_loss_pct"])
    default_leverage = int(config.get("default_leverage", 1))
    max_leverage = int(config.get("max_leverage", 125))
    pyramid_reentry = float(config.get("pyramid_reentry_pct", 0.0))
    liquidation_threshold = float(config.get("liquidation_notional_threshold", 50_000.0))
    same_direction_count = int(config.get("liquidation_same_direction_count", 4))
    opposite_count = int(config.get("liquidation_opposite_count", 3))
    silence_seconds = float(config.get("liquidation_silence_seconds", 300.0))
    max_capital_pct = float(config.get("max_capital_pct_per_instrument", 0.0))
    price = max(0.001, min(0.5, price))
    drawdown = max(0.1, min(95.0, drawdown))
    max_loss = max(1.0, max_loss)
    cooldown = max(10, min(86400, cooldown))
    max_order_notional = float(config.get("max_order_notional_usd", 0.0))
    max_order_notional = max(0.0, min(1_000_000.0, max_order_notional))
    min_notional = max(0.0, min_notional)
    max_position = float(config.get("max_position", 0.0))
    max_position = max(0.0, max_position)
    take_profit = max(0.0, min(500.0, take_profit))
    stop_loss = max(0.0, min(95.0, stop_loss))
    default_leverage = max(1, min(125, default_leverage))
    max_leverage = max(1, min(125, max_leverage))
    if max_leverage < default_leverage:
        max_leverage = default_leverage
    pyramid_max = int(config.get("pyramid_max_orders", 0))
    pyramid_max = max(0, min(100, pyramid_max))
    pyramid_reentry = max(0.0, min(50.0, pyramid_reentry))
    liquidation_threshold = max(0.0, liquidation_threshold)
    same_direction_count = max(1, min(20, same_direction_count))
    opposite_count = max(0, min(20, opposite_count))
    silence_seconds = max(0.0, min(300.0, silence_seconds))
    max_capital_pct = max(0.0, min(1.0, max_capital_pct))
    return {
        "price_tolerance_pct": price,
        "max_drawdown_pct": drawdown,
        "max_loss_absolute": max_loss,
        "cooldown_seconds": cooldown,
        "min_notional_usd": min_notional,
        "max_order_notional_usd": max_order_notional,
        "max_position": max_position,
        "take_profit_pct": take_profit,
        "stop_loss_pct": stop_loss,
        "default_leverage": default_leverage,
        "max_leverage": max_leverage,
        "pyramid_max_orders": pyramid_max,
        "pyramid_reentry_pct": pyramid_reentry,
        "liquidation_notional_threshold": liquidation_threshold,
        "liquidation_same_direction_count": same_direction_count,
        "liquidation_opposite_count": opposite_count,
        "liquidation_silence_seconds": silence_seconds,
        "max_capital_pct_per_instrument": max_capital_pct,
    }


_RISK_SETTINGS_LOCK = Lock()
_RISK_SETTINGS: dict[str, object] = {
    **_sanitize_risk_config(_RISK_DEFAULTS),
    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
}
_risk_store = load_settings_namespace("risk")
if _risk_store:
    sanitized = _sanitize_risk_config(_risk_store)
    _RISK_SETTINGS.update(sanitized)
    updated_at_override = _risk_store.get("updated_at")
    if isinstance(updated_at_override, str) and updated_at_override.strip():
        _RISK_SETTINGS["updated_at"] = updated_at_override

_ORDERBOOK_WRITER: InfluxWriter | None = None
OKX_REST_BASE = "https://www.okx.com"


class InstrumentOverridePayload(BaseModel):
    same_direction: Optional[int] = Field(None, ge=1, le=50)
    opposite_direction: Optional[int] = Field(None, ge=0, le=50)
    notional_threshold: Optional[float] = Field(None, ge=0)
    silence_seconds: Optional[int] = Field(None, ge=0, le=3600)


class PipelineSettingsPayload(BaseModel):
    """Request payload for updating pipeline configuration."""

    tradable_instruments: List[str] = Field(
        ..., description="List of OKX instrument identifiers (e.g. BTC-USDT-SWAP)."
    )
    poll_interval: int = Field(
        ..., ge=30, le=3600, description="Polling interval for the data pipeline in seconds."
    )
    liquidation_overrides: Optional[Dict[str, InstrumentOverridePayload]] = Field(
        default=None,
        description="Optional per-instrument liquidation thresholds.",
    )

    @validator("tradable_instruments")
    def _ensure_instruments(cls, value: List[str]) -> List[str]:
        sanitized = [inst.strip() for inst in value if inst and inst.strip()]
        if not sanitized:
            raise ValueError("At least one instrument must be provided.")
        return sanitized


class WaveOrderRequest(BaseModel):
    """Request payload for triggering automated OKX orders from wave signals."""

    instrument_id: str = Field(..., description="OKX instrument identifier, e.g. BTC-USDT-SWAP.")
    size: float = Field(1.0, gt=0, description="Order size (contracts).")
    side: str = Field("buy", description="Order direction: buy or sell.")
    account_id: Optional[str] = Field(
        default=None,
        description="Optional OKX account identifier override. Defaults to the first configured account.",
    )
    event_price: Optional[float] = Field(default=None, description="Reference price when the wave signal fired.")
    order_mode: Optional[str] = Field(default="size", description="Client-selected automation mode (size/notional).")

    @validator("instrument_id")
    def _normalize_instrument(cls, value: str) -> str:
        normalized = (value or "").strip().upper()
        if not normalized:
            raise ValueError("instrument_id is required.")
        return normalized

    @validator("side")
    def _validate_side(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"buy", "sell"}:
            raise ValueError("side must be either 'buy' or 'sell'.")
        return normalized

class LiquidationMapBinPayload(BaseModel):
    price: float = Field(..., description="Price bucket anchor.")
    notional: float = Field(..., ge=0.0, description="Notional within the bucket.")
    cumulative: float = Field(..., ge=0.0, description="Cumulative notional up to this bucket.")
    side: str = Field(..., description="Bid or ask side.")
    size: float | None = Field(None, ge=0.0, description="Raw size inside the bucket.")

    @validator("side")
    def _normalize_side(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized in {"bid", "buy", "bids"}:
            return "bid"
        if normalized in {"ask", "sell", "asks"}:
            return "ask"
        raise ValueError("side must be bid/buy or ask/sell")


class LiquidationMapRecordPayload(BaseModel):
    instrument: str = Field(..., min_length=1, description="Instrument identifier.")
    timestamp: datetime | None = Field(
        None, description="Timestamp corresponding to the depth snapshot."
    )
    latest_price: float | None = Field(
        None, description="Latest price used to center the buckets."
    )
    bin_size: float = Field(1.0, gt=0.0, description="Bucket size in quote currency.")
    bins: List[LiquidationMapBinPayload] = Field(..., min_items=1)

    @validator("instrument")
    def _normalize_instrument(cls, value: str) -> str:
        inst = (value or "").strip().upper()
        if not inst:
            raise ValueError("instrument is required")
        return inst


def get_pipeline_settings() -> dict:
    """Return current pipeline settings snapshot."""
    with _PIPELINE_SETTINGS_LOCK:
        return {
            "tradable_instruments": list(_PIPELINE_SETTINGS["tradable_instruments"]),
            "poll_interval": int(_PIPELINE_SETTINGS["poll_interval"]),
            "updated_at": _PIPELINE_SETTINGS["updated_at"],
            "liquidation_overrides": _clone_override_map(_PIPELINE_SETTINGS.get("liquidation_overrides")),  # type: ignore[arg-type]
        }


def update_pipeline_settings(
    tradable_instruments: List[str],
    poll_interval: int,
    liquidation_overrides: Optional[Dict[str, Dict[str, object]]] = None,
) -> dict:
    """Update pipeline settings and persist them to config.py."""
    normalized = _normalize_instrument_list(tradable_instruments)
    if not normalized:
        raise HTTPException(status_code=400, detail="At least one instrument is required.")
    poll = int(poll_interval)
    if poll < 30 or poll > 3600:
        raise HTTPException(status_code=400, detail="Polling interval must be between 30 and 3600 seconds.")

    global TRADABLE_INSTRUMENTS, PIPELINE_POLL_INTERVAL
    base_overrides = liquidation_overrides
    with _PIPELINE_SETTINGS_LOCK:
        if base_overrides is None:
            base_overrides = _PIPELINE_SETTINGS.get("liquidation_overrides")  # type: ignore[assignment]
    sanitized_overrides = _sanitize_liquidation_overrides(base_overrides, normalized)
    with _PIPELINE_SETTINGS_LOCK:
        _PIPELINE_SETTINGS["tradable_instruments"] = normalized
        _PIPELINE_SETTINGS["poll_interval"] = poll
        _PIPELINE_SETTINGS["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        _PIPELINE_SETTINGS["liquidation_overrides"] = sanitized_overrides
        TRADABLE_INSTRUMENTS = normalized
        PIPELINE_POLL_INTERVAL = poll
        _persist_pipeline_settings(normalized, poll, sanitized_overrides)
    try:
        scheduler_module.refresh_pipeline(normalized)
    except Exception as exc:  # pragma: no cover - scheduler optional
        logger.debug("Unable to refresh scheduler pipeline: %s", exc)
    try:
        scheduler_module.update_liquidation_overrides(sanitized_overrides)
    except Exception as exc:
        logger.debug("Unable to refresh liquidation overrides: %s", exc)
    try:
        set_websocket_instruments(normalized)
    except Exception as exc:  # pragma: no cover - websocket optional
        logger.debug("Unable to update websocket instruments: %s", exc)
    return {
        "tradable_instruments": list(normalized),
        "poll_interval": poll,
        "updated_at": _PIPELINE_SETTINGS["updated_at"],
        "liquidation_overrides": _clone_override_map(sanitized_overrides),
    }


def get_instrument_catalog(limit: int = 200) -> list[dict]:
    """Return a sanitized snapshot of the cached OKX instrument catalog."""
    entries = search_instrument_catalog(None, limit=limit)
    return [_instrument_entry_to_payload(entry) for entry in entries]


def get_scheduler_settings() -> dict:
    """Return current scheduler interval configuration."""
    with _SCHEDULER_SETTINGS_LOCK:
        return {
            "market_interval": int(_SCHEDULER_SETTINGS["market_interval"]),
            "ai_interval": int(_SCHEDULER_SETTINGS["ai_interval"]),
            "liquidation_interval": int(
                _SCHEDULER_SETTINGS.get("liquidation_interval", LIQUIDATION_CHECK_INTERVAL)
            ),
            "updated_at": _SCHEDULER_SETTINGS["updated_at"],
            "execution_log": scheduler_module.get_execution_log(limit=25),
        }


def update_scheduler_settings(market_interval: int, ai_interval: int, liquidation_interval: int) -> dict:
    """Update scheduler intervals and persist configuration."""
    market = int(market_interval)
    ai = int(ai_interval)
    liquidation = int(liquidation_interval)
    if market < 30 or market > 3600:
        raise HTTPException(status_code=400, detail="市场行情抽取频率需在 30-3600 秒之间。")
    if ai < 60 or ai > 7200:
        raise HTTPException(status_code=400, detail="AI 交互频率需在 60-7200 秒之间。")
    if liquidation < 30 or liquidation > 3600:
        raise HTTPException(status_code=400, detail="爆仓订单流检查频率需在 30-3600 秒之间。")

    with _SCHEDULER_SETTINGS_LOCK:
        _SCHEDULER_SETTINGS["market_interval"] = market
        _SCHEDULER_SETTINGS["ai_interval"] = ai
        _SCHEDULER_SETTINGS["liquidation_interval"] = liquidation
        _SCHEDULER_SETTINGS["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        _persist_scheduler_settings(market, ai, liquidation)

    try:
        scheduler_module.update_task_intervals(
            market_interval=market,
            ai_interval=ai,
            liquidation_interval=liquidation,
        )
    except Exception as exc:  # pragma: no cover - scheduler optional
        logger.debug("Failed to notify scheduler of interval change: %s", exc)

    return get_scheduler_settings()


def get_order_debug_status(limit: int = 25) -> list[dict]:
    """Return the latest order pipeline debug entries."""
    return scheduler_module.get_order_debug_log(limit=limit)


def get_risk_settings() -> dict:
    """Return the currently active risk configuration."""
    with _RISK_SETTINGS_LOCK:
        return {
            "price_tolerance_pct": float(_RISK_SETTINGS["price_tolerance_pct"]),
            "max_drawdown_pct": float(_RISK_SETTINGS["max_drawdown_pct"]),
            "max_loss_absolute": float(_RISK_SETTINGS["max_loss_absolute"]),
            "cooldown_seconds": int(_RISK_SETTINGS["cooldown_seconds"]),
        "min_notional_usd": float(_RISK_SETTINGS.get("min_notional_usd", 0.0)),
        "max_order_notional_usd": float(_RISK_SETTINGS.get("max_order_notional_usd", 0.0)),
        "max_position": float(_RISK_SETTINGS.get("max_position", 0.0)),
        "max_capital_pct_per_instrument": float(_RISK_SETTINGS.get("max_capital_pct_per_instrument", 0.0)),
        "take_profit_pct": float(_RISK_SETTINGS.get("take_profit_pct", 0.0)),
            "stop_loss_pct": float(_RISK_SETTINGS.get("stop_loss_pct", 0.0)),
            "position_take_profit_pct": float(_RISK_SETTINGS.get("position_take_profit_pct", 5.0)),
            "position_stop_loss_pct": float(_RISK_SETTINGS.get("position_stop_loss_pct", 3.0)),
            "default_leverage": int(_RISK_SETTINGS.get("default_leverage", 1)),
            "max_leverage": int(_RISK_SETTINGS.get("max_leverage", 125)),
            "pyramid_max_orders": int(_RISK_SETTINGS.get("pyramid_max_orders", 0)),
            "pyramid_reentry_pct": float(_RISK_SETTINGS.get("pyramid_reentry_pct", 0.0)),
            "liquidation_notional_threshold": float(_RISK_SETTINGS.get("liquidation_notional_threshold", 0.0)),
            "liquidation_same_direction_count": int(_RISK_SETTINGS.get("liquidation_same_direction_count", 4)),
            "liquidation_opposite_count": int(_RISK_SETTINGS.get("liquidation_opposite_count", 3)),
            "liquidation_silence_seconds": int(_RISK_SETTINGS.get("liquidation_silence_seconds", 300)),
            "updated_at": _RISK_SETTINGS["updated_at"],
        }


def update_risk_settings(
    *,
    price_tolerance_pct: float,
    max_drawdown_pct: float,
    max_loss_absolute: float,
    cooldown_seconds: int,
    min_notional_usd: float,
    max_order_notional_usd: float,
    max_position: float,
    take_profit_pct: float,
    stop_loss_pct: float,
    position_take_profit_pct: float,
    position_stop_loss_pct: float,
    default_leverage: int,
    max_leverage: int,
    pyramid_max_orders: int,
    pyramid_reentry_pct: float,
    liquidation_notional_threshold: float,
    liquidation_same_direction_count: int,
    liquidation_opposite_count: int,
    liquidation_silence_seconds: int,
    max_capital_pct_per_instrument: float,
) -> dict:
    """Update risk parameters and persist them to config.py."""
    try:
        price = float(price_tolerance_pct)
        drawdown = float(max_drawdown_pct)
        max_loss = float(max_loss_absolute)
        min_notional = float(min_notional_usd)
        max_order_notional = float(max_order_notional_usd)
        max_position_val = float(max_position)
        take_profit = float(take_profit_pct)
        stop_loss = float(stop_loss_pct)
        position_take_profit = float(position_take_profit_pct)
        position_stop_loss = float(position_stop_loss_pct)
        default_leverage_val = int(default_leverage)
        max_leverage_val = int(max_leverage)
        pyramid_max_val = int(pyramid_max_orders)
        pyramid_reentry_val = float(pyramid_reentry_pct)
        liquidation_threshold_val = float(liquidation_notional_threshold)
        same_direction_val = int(liquidation_same_direction_count)
        opposite_count_val = int(liquidation_opposite_count)
        silence_seconds_val = int(liquidation_silence_seconds)
        max_capital_pct_percent = float(max_capital_pct_per_instrument)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="数值需要为数字。")
    try:
        cooldown = int(cooldown_seconds)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="等待时间必须为整数。")

    if price <= 0 or price > 0.5:
        raise HTTPException(status_code=400, detail="价格偏移阈值需在 0.1% - 50% 之间。")
    if drawdown <= 0 or drawdown > 95:
        raise HTTPException(status_code=400, detail="回撤阈值需在 0 - 95% 之间。")
    if max_loss <= 0:
        raise HTTPException(status_code=400, detail="最大亏损需为正数")
    if cooldown < 10 or cooldown > 86400:
        raise HTTPException(status_code=400, detail="等待时间需在 10 秒 - 24 小时之间。")
    if min_notional < 0:
        raise HTTPException(status_code=400, detail="最小下单金额不能为负数。")
    if max_order_notional < 0 or max_order_notional > 1_000_000:
        raise HTTPException(status_code=400, detail="Maximum order amount must be between 0 and 1000000 USDT.")
    if max_position_val < 0:
        raise HTTPException(status_code=400, detail="最大持仓不得为负数。")
    if take_profit < 0 or take_profit > 500:
        raise HTTPException(status_code=400, detail="止盈范围 0-500% 之间。")
    if stop_loss < 0 or stop_loss > 95:
        raise HTTPException(status_code=400, detail="止损范围 0-95% 之间。")
    if position_take_profit < 0 or position_take_profit > 500:
        raise HTTPException(status_code=400, detail="单笔止盈范围 0-500% 之间。")
    if position_stop_loss < 0 or position_stop_loss > 95:
        raise HTTPException(status_code=400, detail="单笔止损范围 0-95% 之间。")
    if default_leverage_val < 1 or default_leverage_val > 125:
        raise HTTPException(status_code=400, detail="默认杠杆需在 1-125 之间。")
    if max_leverage_val < 1 or max_leverage_val > 125:
        raise HTTPException(status_code=400, detail="最大杠杆需在 1-125 之间。")
    if max_leverage_val < default_leverage_val:
        raise HTTPException(status_code=400, detail="最大杠杆需大于等于默认杠杆。")
    if pyramid_max_val < 0 or pyramid_max_val > 100:
        raise HTTPException(status_code=400, detail="单方向金字塔上限应在 0-100 之间。")
    if pyramid_reentry_val < 0 or pyramid_reentry_val > 50:
        raise HTTPException(status_code=400, detail="�������۸�ƫ����Ӧ�� 0-50% ֮�䡣")
    if liquidation_threshold_val < 0:
        raise HTTPException(status_code=400, detail="爆仓金额阈值必须为非负数。")
    if same_direction_val < 1 or same_direction_val > 20:
        raise HTTPException(status_code=400, detail="同向爆仓笔数需在 1-20 之间。")
    if opposite_count_val < 0 or opposite_count_val > 20:
        raise HTTPException(status_code=400, detail="逆向爆仓笔数需在 0-20 之间。")
    if silence_seconds_val < 0 or silence_seconds_val > 300:
        raise HTTPException(status_code=400, detail="冷静时长需在 0-300 秒之间。")

    if max_capital_pct_percent < 0 or max_capital_pct_percent > 100:
        raise HTTPException(status_code=400, detail="单币对最大占用比例应在 0-100% 之间")
    max_capital_pct = max(0.0, min(1.0, max_capital_pct_percent / 100.0))
    normalized = {
        "price_tolerance_pct": price,
        "max_drawdown_pct": drawdown,
        "max_loss_absolute": max_loss,
        "cooldown_seconds": cooldown,
        "min_notional_usd": min_notional,
        "max_order_notional_usd": max_order_notional,
        "max_position": max_position_val,
        "take_profit_pct": take_profit,
        "stop_loss_pct": stop_loss,
        "position_take_profit_pct": position_take_profit,
        "position_stop_loss_pct": position_stop_loss,
        "default_leverage": default_leverage_val,
        "max_leverage": max_leverage_val,
        "pyramid_max_orders": pyramid_max_val,
        "pyramid_reentry_pct": pyramid_reentry_val,
        "liquidation_notional_threshold": liquidation_threshold_val,
        "liquidation_same_direction_count": same_direction_val,
        "liquidation_opposite_count": opposite_count_val,
        "liquidation_silence_seconds": silence_seconds_val,
        "max_capital_pct_per_instrument": max_capital_pct,
    }
    with _RISK_SETTINGS_LOCK:
        for key, value in normalized.items():
            _RISK_SETTINGS[key] = value
        _RISK_SETTINGS["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        _persist_risk_settings(normalized)

    try:
        scheduler_module.update_risk_configuration(**normalized)
    except Exception as exc:  # pragma: no cover - scheduler optional
        logger.debug("Failed to notify scheduler of risk config change: %s", exc)

    return get_risk_settings()


@router.post(
    "/api/scheduler/test-market",
    summary="Trigger the market data job once for testing",
)
async def trigger_market_job_api() -> dict:
    try:
        result = await scheduler_module.run_market_job_once()
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Manual market job failed: %s", exc)
        raise HTTPException(status_code=500, detail="触发行情测试失败")
    return {
        "status": result.get("status", "unknown"),
        "detail": result.get("detail") or "",
        "executed_at": datetime.now(tz=timezone.utc).isoformat(),
    }


@router.post(
    "/api/scheduler/test-ai",
    summary="Trigger the AI execution job once for testing",
)
async def trigger_ai_job_api() -> dict:
    try:
        result = await scheduler_module.run_ai_job_once()
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Manual AI job failed: %s", exc)
        raise HTTPException(status_code=500, detail="触发 AI 测试失败")
    return {
        "status": result.get("status", "unknown"),
        "detail": result.get("detail") or "",
        "executed_at": datetime.now(tz=timezone.utc).isoformat(),
    }


@router.post(
    "/api/scheduler/test-liquidation",
    summary="Trigger the liquidation order flow scan once for testing",
)
async def trigger_liquidation_job_api() -> dict:
    try:
        result = await scheduler_module.run_liquidation_job_once()
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Manual liquidation scan failed: %s", exc)
        raise HTTPException(status_code=500, detail="触发爆仓检查失败")
    return {
        "status": result.get("status", "unknown"),
        "detail": result.get("detail") or "",
        "executed_at": datetime.now(tz=timezone.utc).isoformat(),
    }


async def refresh_instrument_catalog() -> dict:
    """Fetch the latest instrument catalog from OKX and persist caches."""
    return await refresh_okx_instrument_catalog()


def _normalize_instrument_list(instruments: List[str]) -> List[str]:
    """Strip whitespace, uppercase instrument ids, and de-duplicate in order."""
    seen: set[str] = set()
    normalized: List[str] = []
    for value in instruments:
        inst_id = (value or "").strip().upper()
        if not inst_id or inst_id in seen:
            continue
        seen.add(inst_id)
        normalized.append(inst_id)
    return normalized


def _sanitize_liquidation_overrides(
    overrides: Optional[Dict[str, Dict[str, object]]],
    instruments: Sequence[str],
) -> dict[str, dict]:
    allowed = {inst.strip().upper() for inst in instruments if inst and inst.strip()}
    sanitized: dict[str, dict] = {}
    if not overrides:
        return sanitized

    def _parse_int_value(value: object, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            number = int(float(str(value)))
        except (TypeError, ValueError):
            return None
        if minimum is not None:
            number = max(minimum, number)
        if maximum is not None:
            number = min(maximum, number)
        return number

    def _parse_float_value(value: object, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if minimum is not None:
            number = max(minimum, number)
        if maximum is not None:
            number = min(maximum, number)
        return number

    for key, raw in overrides.items():
        symbol = str(key or "").strip().upper()
        if not symbol or (allowed and symbol not in allowed):
            continue
        payload = raw or {}
        entry: dict[str, object] = {}
        same_val = _parse_int_value(
            payload.get("same_direction")
            or payload.get("same_direction_count")
            or payload.get("same"),
            minimum=1,
            maximum=50,
        )
        opposite_val = _parse_int_value(
            payload.get("opposite_direction")
            or payload.get("opposite_direction_count")
            or payload.get("opposite"),
            minimum=0,
            maximum=50,
        )
        notional_val = _parse_float_value(
            payload.get("notional_threshold")
            or payload.get("liquidation_notional_threshold")
            or payload.get("notional"),
            minimum=0.0,
        )
        silence_val = _parse_float_value(
            payload.get("silence_seconds")
            or payload.get("liquidation_silence_seconds"),
            minimum=0.0,
            maximum=3600.0,
        )
        if same_val is not None:
            entry["same_direction"] = same_val
        if opposite_val is not None:
            entry["opposite_direction"] = opposite_val
        if notional_val is not None:
            entry["notional_threshold"] = notional_val
        if silence_val is not None:
            entry["silence_seconds"] = silence_val
        if entry:
            sanitized[symbol] = entry
    return sanitized


def _clone_override_map(data: Optional[Dict[str, Dict[str, object]]]) -> dict[str, dict]:
    if not data:
        return {}
    return {symbol: dict(values) for symbol, values in data.items()}


_PIPELINE_SETTINGS["liquidation_overrides"] = _sanitize_liquidation_overrides(
    _LIQUIDATION_OVERRIDE_DEFAULTS,
    _PIPELINE_SETTINGS["tradable_instruments"],
)


@router.get("/health", summary="Service health probe")
def health_check() -> dict:
    """Return a static payload for uptime checks."""
    return {"status": "ok"}


@router.get(
    "/portfolios",
    response_model=List[dict],
    summary="List all registered portfolios",
)
def list_portfolios(
    registry: PortfolioRegistry = Depends(get_portfolio_registry),
) -> list[dict]:
    """Return all portfolios currently registered in the system."""
    records = registry.list_all()
    return [
        {
            **asdict(record),
            "metrics": _mock_portfolio_metrics(record),
        }
        for record in records
    ]


@router.get(
    "/portfolios/{portfolio_id}",
    response_model=dict,
    summary="Fetch a single portfolio by identifier",
)
def get_portfolio(
    portfolio_id: str,
    registry: PortfolioRegistry = Depends(get_portfolio_registry),
) -> dict:
    """Return a specific portfolio or raise if it does not exist."""
    record: PortfolioRecord | None = registry.get(portfolio_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    return {
        **asdict(record),
        "metrics": _mock_portfolio_metrics(record),
    }


@router.get(
    "/metrics/system",
    summary="High-level system health metrics",
)
def get_system_metrics() -> dict:
    """Return placeholder metrics for the overall trading system."""
    now = datetime.now(tz=timezone.utc)
    return {
        "as_of": now.isoformat(),
        "status": "degraded" if now.minute % 5 == 0 else "operational",
        "active_models": 2,
        "open_positions": 3,
        "daily_realized_pnl": 152.37,
        "daily_unrealized_pnl": -42.15,
        "latency_ms": {
            "signal_processing_avg": 215,
            "order_execution_p95": 380,
        },
        "risk_alerts": 1,
    }


@router.get(
    "/metrics/models",
    summary="Per-model performance snapshots",
)
def get_model_metrics() -> dict:
    return _collect_model_metrics()


def _collect_model_metrics(repository: AccountRepository | None = None) -> dict:
    """
    Return live performance metrics sourced from the account repository.

    When historical data is insufficient for a metric we return None so the UI can
    display a placeholder instead of stale demo values.
    """
    repo = repository or get_account_repository()
    now = datetime.now(tz=timezone.utc).isoformat()
    try:
        accounts = repo.list_accounts()
    except Exception as exc:  # pragma: no cover - repository failures
        logger.exception("Failed to load accounts for model metrics: %s", exc)
        return {
            "as_of": now,
            "models": [],
            "recent_trades": [],
            "recent_ai_signals": _load_recent_ai_signals(),
        }

    model_rows: list[dict] = []
    aggregated_trades: list[Trade] = []
    for account in accounts:
        try:
            positions = repo.list_positions(account.account_id)
        except Exception:
            logger.warning("Unable to list positions for account %s", account.account_id)
            positions = []
        try:
            trades = repo.list_trades(account.account_id, limit=200)
        except Exception:
            logger.warning("Unable to list trades for account %s", account.account_id)
            trades = []
        try:
            equity_curve = repo.get_equity_curve(account.account_id, limit=200)
        except Exception:
            logger.warning("Unable to fetch equity curve for account %s", account.account_id)
            equity_curve = []

        aggregated_trades.extend(trades)
        model_rows.append(
            _summarize_account_performance(
                account=account,
                positions=positions,
                trades=trades,
                equity_curve=equity_curve,
            )
        )

    recent_trades: list[dict] = []
    for trade in sorted(aggregated_trades, key=lambda t: t.executed_at, reverse=True)[:200]:
        payload = _trade_to_dict(trade)
        quantity = payload.get("quantity")
        if payload.get("size") is None and quantity is not None:
            payload["size"] = quantity
        price = payload.get("price")
        if payload.get("entry_price") is None and price is not None:
            payload["entry_price"] = price
        if payload.get("exit_price") is None and price is not None:
            payload["exit_price"] = price
        if payload.get("pnl") is None and payload.get("realized_pnl") is not None:
            payload["pnl"] = payload.get("realized_pnl")
        recent_trades.append(payload)

    # Fallback/live merge: include latest trades from OKX paper trading accounts
    live_trades: list[dict] = []
    for _, meta in OKX_ACCOUNTS.items():
        try:
            live = fetch_account_snapshot(meta)
        except Exception:
            continue
        for item in live.get("recent_trades", []) or []:
            # Ensure required keys exist; keep as dict already normalized by OKX layer
            live_trades.append(dict(item))

    def _parse_time(value: object) -> str:
        # use string compare after ensuring isoformat strings; fallback empty
        try:
            return str(value)
        except Exception:
            return ""

    combined_trades = recent_trades + live_trades
    combined_trades = sorted(
        combined_trades,
        key=lambda d: _parse_time(d.get("executed_at") or d.get("timestamp")),
        reverse=True,
    )[:20]

    enabled_models = _get_enabled_model_ids()
    pipeline_settings = get_pipeline_settings()
    enabled_instruments = pipeline_settings.get("tradable_instruments", [])
    recent_ai_signals = _load_recent_ai_signals(limit=20, enabled_model_ids=enabled_models)
    return {
        "as_of": now,
        "models": model_rows,
        "recent_trades": combined_trades,
        "recent_ai_signals": recent_ai_signals,
        "enabled_instruments": enabled_instruments,
    }


@router.get(
    "/api/accounts",
    response_model=List[Account],
    summary="List accounts with latest equity snapshots",
)
def list_accounts_api(
    repository: AccountRepository = Depends(get_account_repository),
) -> List[Account]:
    return repository.list_accounts()


@router.get(
    "/api/accounts/{account_id}",
    response_model=AccountSnapshot,
    summary="Retrieve a single account snapshot including positions and trades",
)
def get_account_snapshot_api(
    account_id: str,
    repository: AccountRepository = Depends(get_account_repository),
) -> AccountSnapshot:
    snapshot = repository.get_snapshot(account_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return snapshot


@router.get(
    "/api/okx/summary",
    summary="Aggregate OKX paper trading metrics",
)
def okx_summary_api(
    repository: AccountRepository = Depends(get_account_repository),
) -> dict:
    return get_okx_summary(repository)


@router.post(
    "/api/okx/wave-order",
    summary="Submit a market order when liquidation wave signals trigger.",
)
def okx_wave_order_api(payload: WaveOrderRequest) -> dict:
    symbol = payload.instrument_id
    side = payload.side.lower()
    account_id = payload.account_id or _default_okx_account_id()
    order_mode = (payload.order_mode or "size").strip().lower()
    if order_mode not in {"size", "notional"}:
        order_mode = "size"
    try:
        requested_size = float(payload.size)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="size 参数必须为数字。")
    if requested_size <= 0:
        raise HTTPException(status_code=400, detail="下单数量必须大于 0。")

    risk = get_risk_settings()
    pyramid_cap = int(risk.get("pyramid_max_orders", 0))
    reentry_pct = float(risk.get("pyramid_reentry_pct", 0.0))
    event_price = _try_float(payload.event_price)

    open_orders: Sequence[dict] = []
    positions: Sequence[dict] = []
    try:
        summary = get_okx_summary()
    except Exception as exc:  # pragma: no cover - repository optional
        logger.debug("Unable to fetch OKX summary for wave order: %s", exc)
        summary = {}
    for entry in summary.get("accounts", []):
        account_meta = entry.get("account") or {}
        if account_meta.get("account_id") == account_id:
            open_orders = entry.get("open_orders") or []
            positions = entry.get("positions") or []
            break
    if pyramid_cap > 0:
        active_same_side = sum(
            1
            for order in open_orders
            if (order.get("instrument_id") or "").upper() == symbol
            and (order.get("side") or "").lower() == side
        )
        if active_same_side >= pyramid_cap:
            raise HTTPException(
                status_code=400,
                detail=f"金字塔同向订单已达 {pyramid_cap} 笔，阻止继续加仓。",
            )

    matching_positions: list[dict] = []
    matching_positions: list[dict] = []
    if event_price and event_price > 0 and reentry_pct > 0 and positions:
        for pos in positions:
            if (pos.get("instrument_id") or "").upper() != symbol:
                continue
            side_text = (pos.get("side") or "").lower()
            if side == "buy" and side_text not in {"buy", "long"}:
                continue
            if side == "sell" and side_text not in {"sell", "short"}:
                continue
            matching_positions.append(pos)
    if event_price and matching_positions:
        for pos in matching_positions:
            entry_price = _try_float(pos.get("entry_price"))
            if entry_price and entry_price > 0:
                deviation = abs((entry_price - event_price) / entry_price) * 100.0
                if deviation < reentry_pct:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"{symbol} ����ֲ� {entry_price:.4f} ���źż� "
                            f"{event_price:.4f} ƫ�� {deviation:.2f}% < {reentry_pct:.2f}%�������Զ��Ӳ֡�"
                        ),
                    )

    instrument_meta = _get_okx_instrument_meta(symbol) or {}
    lot_size = _try_float(instrument_meta.get("lotSz")) or 0.0
    min_size = _try_float(instrument_meta.get("minSz")) or 0.0
    max_size = _try_float(instrument_meta.get("maxSz")) or 0.0
    contract_value = _try_float(instrument_meta.get("ctVal")) or 0.0
    inst_type = (instrument_meta.get("instType") or "").upper()
    uses_contract_units = contract_value > 0 and inst_type in {"SWAP", "FUTURES"}
    base_per_contract = contract_value if contract_value > 0 else 1.0

    try:
        ticker = _fetch_public_ticker(symbol)
    except Exception as exc:  # pragma: no cover - network errors
        logger.debug("Failed to fetch ticker for %s: %s", symbol, exc)
        ticker = None
    price_hint = _extract_ticker_price(ticker) if ticker else None
    if (price_hint is None or price_hint <= 0) and event_price and event_price > 0:
        price_hint = event_price

    adjusted_size = max(requested_size, 0.0)
    notional_requested = None
    if order_mode == "notional":
        notional_requested = adjusted_size
        if not price_hint or price_hint <= 0:
            raise HTTPException(status_code=400, detail="无法获取有效行情价格，无法根据金额下单")
        adjusted_size = notional_requested / price_hint
        if uses_contract_units and base_per_contract > 0:
            adjusted_size = adjusted_size / base_per_contract

    if lot_size > 0:
        steps = max(1, math.ceil(adjusted_size / lot_size))
        adjusted_size = steps * lot_size
    if min_size > 0 and adjusted_size < min_size:
        adjusted_size = min_size
    if max_size > 0 and adjusted_size > max_size:
        adjusted_size = max_size

    min_notional = float(risk.get("min_notional_usd", 0.0))
    max_notional = float(risk.get("max_order_notional_usd", 0.0))
    if price_hint and min_notional > 0:
        min_contracts = min_notional / price_hint
        if uses_contract_units and base_per_contract > 0:
            min_contracts = min_contracts / base_per_contract
        if min_contracts > adjusted_size:
            adjusted_size = min_contracts
    if price_hint and max_notional > 0:
        max_contracts = max_notional / price_hint
        if uses_contract_units and base_per_contract > 0:
            max_contracts = max_contracts / base_per_contract
        if max_contracts > 0 and adjusted_size > max_contracts:
            adjusted_size = max_contracts

    if adjusted_size <= 0:
        raise HTTPException(status_code=400, detail="调整后的下单数量无效")

    rounded_size = round(adjusted_size, 8)
    executed_base_size = rounded_size * base_per_contract if uses_contract_units else rounded_size
    result = place_manual_okx_order(
        account_id=account_id,
        instrument_id=symbol,
        side=side,
        order_type="market",
        size=rounded_size,
        price=None,
        margin_mode=None,
    )
    if price_hint:
        notional_estimate = executed_base_size * price_hint
    elif order_mode == "notional" and notional_requested is not None:
        notional_estimate = notional_requested
    else:
        notional_estimate = None
    return {
        "status": result.get("status"),
        "order_id": result.get("order_id"),
        "instrument_id": symbol,
        "side": side,
        "size": rounded_size,
        "base_size": executed_base_size,
        "reference_price": price_hint,
        "notional_estimate": notional_estimate,
        "order_mode": order_mode,
        "risk_constraints": {
            "min_notional_usd": min_notional,
            "max_order_notional_usd": max_notional,
            "pyramid_max_orders": pyramid_cap,
        },
    }


def get_model_catalog() -> list[dict]:
    """Return current model registry configuration."""
    with _MODEL_REGISTRY_LOCK:
        return [dict(info) for info in _MODEL_REGISTRY.values()]


def update_model_config(model_id: str, *, enabled: bool, api_key: str | None) -> dict:
    """Update an individual model configuration entry."""
    with _MODEL_REGISTRY_LOCK:
        record = _MODEL_REGISTRY.get(model_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Model not found")
        record["enabled"] = enabled
        if api_key is not None:
            record["api_key"] = api_key
        record["last_updated"] = datetime.now(tz=timezone.utc).isoformat()
        _persist_model_defaults()
        return dict(record)


def get_prompt_template_text() -> str:
    """Fetch the current AI prompt template."""
    return prompt_templates.get_prompt_template()


def update_prompt_template_text(new_text: str) -> str:
    """Persist a new AI prompt template."""
    return prompt_templates.save_prompt_template(new_text)


@router.get(
    "/api/settings/pipeline",
    summary="Retrieve current pipeline configuration (interval + instruments)",
)
def pipeline_settings_api() -> dict:
    return get_pipeline_settings()


@router.post(
    "/api/settings/pipeline",
    summary="Update pipeline configuration (interval + instruments)",
)
def update_pipeline_settings_api(payload: PipelineSettingsPayload) -> dict:
    overrides_payload: Optional[Dict[str, Dict[str, object]]] = None
    if payload.liquidation_overrides:
        overrides_payload = {
            (key or "").strip().upper(): value.dict(exclude_none=True)
            for key, value in payload.liquidation_overrides.items()
        }
    return update_pipeline_settings(payload.tradable_instruments, payload.poll_interval, overrides_payload)


@router.get(
    "/api/settings/instruments/search",
    summary="Search cached OKX instruments for quick selection",
)
def search_instruments_api(q: Optional[str] = None, limit: int = 50, quote: Optional[str] = None) -> dict:
    limit = max(1, min(limit, 200))
    matches = search_instrument_catalog(q or None, limit=limit, quote=(quote or None))
    return {
        "query": (q or "").strip(),
        "quote": (quote or "").strip().upper(),
        "results": [_instrument_entry_to_payload(item) for item in matches],
        "limit": limit,
        "total_cached": len(load_catalog_cache()),
    }


@router.post(
    "/api/settings/instruments/refresh",
    summary="Refresh the OKX instrument catalog cache (and persist to InfluxDB)",
)
async def refresh_instruments_api() -> dict:
    result = await refresh_instrument_catalog()
    return {
        "count": result["count"],
        "wrote_influx": result["wrote_influx"],
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }


@router.get(
    "/api/streams/liquidations/latest",
    summary="Fetch recent liquidation aggregates from InfluxDB",
)
def latest_liquidations_api(limit: int = 50, instrument: Optional[str] = None) -> dict:
    limit = max(1, min(limit, 200))
    return get_liquidation_snapshot(limit=limit, instrument=instrument)


@router.get(
    "/api/streams/orderbook/latest",
    summary="Fetch recent order book depth snapshots from InfluxDB",
)
def latest_orderbook_api(
    limit: int = 50,
    instrument: Optional[str] = None,
    fresh: bool = False,
) -> dict:
    limit = max(1, min(limit, 500))
    return get_orderbook_snapshot(
        levels=limit,
        instrument=instrument,
        force_live=fresh,
    )


@router.post(
    "/api/liquidation-map/bins",
    summary="Persist aggregated liquidation map buckets into InfluxDB",
)
def record_liquidation_map_bins(payload: LiquidationMapRecordPayload) -> dict:
    writer = _get_orderbook_writer()
    if writer is None:
        raise HTTPException(status_code=503, detail="Influx writer unavailable.")
    timestamp = payload.timestamp or datetime.now(tz=timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    base_ns = int(timestamp.timestamp() * 1e9)
    inst_id = payload.instrument
    bin_size = float(payload.bin_size)
    recorded = 0
    for idx, bucket in enumerate(payload.bins):
        bucket_start = math.floor(bucket.price / bin_size) * bin_size
        fields = {
            "price": float(bucket.price),
            "bucket_start": float(bucket_start),
            "notional": float(bucket.notional),
            "cumulative": float(bucket.cumulative),
        }
        if bucket.size is not None:
            fields["size"] = float(bucket.size)
        tags = {
            "instrument_id": inst_id,
            "side": bucket.side,
            "price_bucket": f"{bucket_start:.4f}",
            "bin_size": f"{bin_size:.4f}",
        }
        try:
            writer.write_indicator_set(
                measurement="liquidation_map_bins",
                tags=tags,
                fields=fields,
                timestamp_ns=base_ns + idx,
            )
            recorded += 1
        except Exception as exc:  # pragma: no cover - write failures logged only
            logger.debug("Failed to persist liquidation map bin: %s", exc)
    return {
        "instrument": inst_id,
        "bins_recorded": recorded,
        "timestamp": timestamp.isoformat(),
    }


@router.get(
    "/api/liquidation-map/history",
    summary="Fetch aggregated liquidation map bins from InfluxDB",
)
def liquidation_map_history_api(
    instrument: Optional[str] = None,
    hours: int = 24,
    limit: int = 8000,
) -> dict:
    hours = max(1, min(hours, 168))
    limit = max(100, min(limit, 20000))
    snapshot = get_liquidation_map_history_snapshot(
        instrument=instrument,
        lookback_hours=hours,
        limit=limit,
    )
    return snapshot


def get_okx_summary(
    repository: AccountRepository | None = None,
    *,
    force_refresh: bool = False,
    ttl_seconds: int | None = None,
) -> dict:
    """Return OKX account snapshots, leaderboard, and equity curves.

    Optimization: prefer cached Influx data; only refresh from OKX if cache is
    older than 10 minutes, then persist refreshed data back to Influx.
    """
    repo = repository or get_account_repository()
    repository_failed = False
    try:
        accounts = repo.list_accounts()
    except Exception as exc:  # repository backend unavailable (e.g., Influx not running)
        logger.exception("Failed to load accounts for OKX summary: %s", exc)
        accounts = []
        repository_failed = True

    now = datetime.now(tz=timezone.utc)
    ttl = int(ttl_seconds) if ttl_seconds is not None else _OKX_CACHE_TTL_SECONDS

    # Build cached payloads from repository
    account_payloads: list[dict] = []
    account_map: Dict[str, dict] = {}
    accounts_by_id: Dict[str, Account] = {a.account_id: a for a in accounts}
    sync_errors: list[dict] = []
    if repository_failed:
        sync_errors.append(
            {
                "account_id": "repository",
                "message": "Account repository unavailable; showing live data only if configured.",
            }
        )
    for account in accounts:
        positions = repo.list_positions(account.account_id)
        trades = repo.list_trades(account.account_id, limit=50)
        curve = repo.get_equity_curve(account.account_id, limit=50)
        balances = repo.list_balances(account.account_id)
        orders = repo.list_orders(account.account_id, limit=50)
        payload = {
            "account": account.dict(),
            "balances": [balance.dict() for balance in balances],
            "positions": [position.dict() for position in positions],
            "recent_trades": [_trade_to_dict(trade) for trade in trades],
            "open_orders": [_order_to_dict(order) for order in orders],
            "equity_curve": curve,
        }
        account_payloads.append(payload)
        account_map[account.account_id] = payload

    # Merge or refresh live data conditionally per account (10-minute TTL)
    for key, meta in OKX_ACCOUNTS.items():
        account_id = meta.get("account_id") or key
        cached = accounts_by_id.get(account_id)
        fresh_enough = False
        if not force_refresh and cached is not None:
            try:
                age = (now - cached.updated_at).total_seconds()
                fresh_enough = age <= ttl
            except Exception:
                fresh_enough = False

        # Always attempt to refresh live state so positions/挂单 stay current.
        try:
            live = fetch_account_snapshot(meta)
        except Exception as exc:
            # If we have a fresh-enough cached snapshot, keep it as a fallback.
            if not fresh_enough:
                sync_errors.append(
                    {
                        "account_id": account_id,
                        "message": str(exc),
                    }
                )
            continue

        # Persist refreshed snapshot back to repository when available
        if not repository_failed:
            try:
                _persist_live_snapshot(repo, live, existing=cached, timestamp=now)
            except Exception as exc:
                sync_errors.append(
                    {
                        "account_id": account_id,
                        "message": f"Failed to persist refreshed data: {exc}",
                    }
                )

        # Update response payload with freshest data
        live_account = live.get("account", {})
        target = account_map.get(account_id)
        if target is None:
            target = {
                "account": live_account,
                "balances": live.get("balances", []),
                "positions": live.get("positions", []),
                "recent_trades": live.get("recent_trades", []),
                "open_orders": live.get("open_orders", []),
                "equity_curve": [],
            }
            account_payloads.append(target)
            account_map[account_id] = target
        else:
            target["account"].update(live_account)
            target["balances"] = live.get("balances", target.get("balances", []))
            target["positions"] = live.get("positions", target.get("positions", []))
            target["recent_trades"] = live.get("recent_trades", target.get("recent_trades", []))
            target["open_orders"] = live.get("open_orders", target.get("open_orders", []))

    # Leaderboard may also rely on repository; guard to avoid 500s in demo setups
    try:
        leaderboard = get_leaderboard(repo)
    except Exception:
        leaderboard = {"leaders": []}
    if not leaderboard.get("leaders"):
        leaderboard = refresh_leaderboard_cache(repo)
    return {
        "as_of": now.isoformat(),
        "accounts": account_payloads,
        "leaderboard": leaderboard,
        "sync_errors": sync_errors,
    }


def _default_okx_account_id() -> str:
    for key, meta in OKX_ACCOUNTS.items():
        return meta.get("account_id") or key
    raise HTTPException(status_code=400, detail="???????? OKX ???")


def _resolve_okx_account_meta(account_identifier: str | None) -> tuple[str, dict]:
    normalized = (account_identifier or "").strip()
    if not normalized:
        normalized = _default_okx_account_id()
    for key, meta in OKX_ACCOUNTS.items():
        account_id = meta.get("account_id") or key
        if normalized in {account_id, key}:
            return account_id, meta
    raise HTTPException(status_code=400, detail=f"??????? {account_identifier}")


def _choose_margin_mode(instrument_id: str, meta: dict, override: str | None) -> str:
    mode = (override or meta.get("margin_mode") or "").lower()
    if mode in {"cash", "cross", "isolated"}:
        return mode
    symbol = (instrument_id or "").upper()
    if symbol.endswith("-SWAP") or "FUTURE" in symbol:
        return "cross"
    return "cash"


def _resolve_leverage(meta: dict, override: float | str | None = None) -> float:
    risk = get_risk_settings()
    default_leverage = max(1.0, float(risk.get("default_leverage", 2)))
    max_leverage = max(default_leverage, float(risk.get("max_leverage", default_leverage)))

    candidate = _try_float(override)
    if candidate is None:
        candidate = _try_float(meta.get("default_leverage"))
    if candidate is None or candidate <= 0:
        candidate = default_leverage
    return max(1.0, min(max_leverage, candidate))


def _extract_ticker_price(ticker: dict) -> float | None:
    for key in ("last", "lastPx", "last_price", "px"):
        value = ticker.get(key)
        if value in (None, ""):
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if numeric > 0:
            return numeric
    return None


def _fetch_public_ticker(inst_id: str) -> dict | None:
    symbol = (inst_id or "").strip().upper()
    if not symbol:
        return None
    try:
        response = httpx.get(
            f"{OKX_REST_BASE}/api/v5/market/ticker",
            params={"instId": symbol},
            timeout=5.0,
        )
        payload = response.json()
    except Exception as exc:  # pragma: no cover - network path
        logger.debug("Failed to fetch ticker for %s: %s", symbol, exc)
        return None
    if payload.get("code") not in {"0", 0}:
        return None
    data = payload.get("data") or []
    return data[0] if data else None


def place_manual_okx_order(
    *,
    account_id: str,
    instrument_id: str,
    side: str,
    order_type: str,
    size: float,
    price: float | None,
    margin_mode: str | None = None,
) -> dict:
    """Submit a manual order to the OKX demo endpoint."""
    account_key, meta = _resolve_okx_account_meta(account_id)
    for field in ("api_key", "api_secret"):
        if not meta.get(field):
            raise HTTPException(status_code=400, detail=f"账户 {account_key} 缺少 {field}")
    normalized_side = side.lower()
    normalized_type = order_type.lower()
    if normalized_side not in {"buy", "sell"}:
        raise HTTPException(status_code=400, detail="方向必须为 buy 或 sell")
    if normalized_type not in {"limit", "market"}:
        raise HTTPException(status_code=400, detail="订单类型必须为 limit 或 market")

    risk_config = get_risk_settings()
    take_profit_pct = float(risk_config.get("take_profit_pct", 0.0))
    stop_loss_pct = float(risk_config.get("stop_loss_pct", 0.0))
    entry_price_hint: float | None = None

    margin_mode_value = _choose_margin_mode(instrument_id, meta, margin_mode)
    leverage_value = _resolve_leverage(meta)

    payload: dict[str, object] = {
        "instrument_id": instrument_id.upper(),
        "side": normalized_side,
        "order_type": normalized_type,
        "size": str(size),
        "margin_mode": margin_mode_value,
        "leverage": leverage_value,
    }
    if normalized_type == "limit":
        if price is None:
            raise HTTPException(status_code=400, detail="限价单需要价格")
        payload["price"] = str(price)
        try:
            entry_price_hint = float(price)
        except (TypeError, ValueError):
            entry_price_hint = None

    credentials = ExchangeCredentials(
        api_key=meta["api_key"],
        api_secret=meta["api_secret"],
        passphrase=meta.get("passphrase"),
    )
    client = OkxPaperClient()
    try:
        client.authenticate(credentials)
        try:
            client.set_leverage(
                instrument_id=payload["instrument_id"],
                leverage=leverage_value,
                margin_mode=margin_mode_value,
                position_side=(payload.get("pos_side") if isinstance(payload.get("pos_side"), str) else None),
            )
        except OkxClientError as exc:
            raise HTTPException(status_code=400, detail=f"设置杠杆失败: {exc}") from exc
        if entry_price_hint is None:
            try:
                ticker = client.fetch_ticker(payload["instrument_id"])
                entry_price_hint = _extract_ticker_price(ticker)
            except Exception:
                entry_price_hint = None
        payload = apply_bracket_targets(
            payload,
            side=normalized_side,
            entry_price=entry_price_hint,
            take_profit_pct=take_profit_pct,
            stop_loss_pct=stop_loss_pct,
        )

        response = client.place_order(payload)
    except OkxClientError as exc:
        raise HTTPException(status_code=400, detail=_format_okx_error(exc))
    finally:
        client.close()

    return {
        "status": response.get("status"),
        "order_id": response.get("order_id"),
        "client_order_id": response.get("client_order_id"),
        "raw": response.get("raw"),
    }


def scale_okx_position(
    *,
    account_id: str,
    instrument_id: str,
    position_side: str,
    quantity: float,
    margin_mode: str | None = None,
) -> dict:
    """Increase an existing position by sending a market order."""
    try:
        normalized_qty = float(quantity)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="补仓数量必须为数字。")
    if normalized_qty <= 0:
        raise HTTPException(status_code=400, detail="补仓数量需要大于 0。")

    side_lower = (position_side or "").lower()
    if side_lower in {"long", "buy"}:
        open_side = "buy"
    elif side_lower in {"short", "sell"}:
        open_side = "sell"
    else:
        raise HTTPException(status_code=400, detail=f"未知的持仓方向 {position_side}")

    account_key, meta = _resolve_okx_account_meta(account_id)
    for field in ("api_key", "api_secret"):
        if not meta.get(field):
            raise HTTPException(status_code=400, detail=f"账户 {account_key} 缺少 {field}")

    symbol = (instrument_id or "").upper()
    instrument_meta = _get_okx_instrument_meta(symbol)
    lot_size = _try_float(instrument_meta.get("lotSz")) if instrument_meta else None
    min_size = _try_float(instrument_meta.get("minSz")) if instrument_meta else None
    max_size = _try_float(instrument_meta.get("maxSz")) if instrument_meta else None
    adjusted_qty = normalized_qty
    if lot_size and lot_size > 0:
        multiples = math.ceil(adjusted_qty / lot_size)
        adjusted_qty = max(lot_size, multiples * lot_size)
    if min_size and min_size > 0 and adjusted_qty < min_size:
        adjusted_qty = min_size
    if max_size and max_size > 0 and adjusted_qty > max_size:
        adjusted_qty = max_size
    if adjusted_qty <= 0:
        raise HTTPException(status_code=400, detail="补仓数量未达到最小下单限制。")

    credentials = ExchangeCredentials(
        api_key=meta["api_key"],
        api_secret=meta["api_secret"],
        passphrase=meta.get("passphrase"),
    )
    client = OkxPaperClient()
    instrument_meta = _get_okx_instrument_meta(symbol) or {}
    quote_currency = (
        instrument_meta.get("settleCurrency")
        or instrument_meta.get("ctValCcy")
        or instrument_meta.get("quoteCcy")
    )
    if not quote_currency:
        parts = symbol.split("-")
        if len(parts) >= 2:
            quote_currency = parts[1]
    try:
        client.authenticate(credentials)
        margin_mode_value = _choose_margin_mode(symbol, meta, margin_mode)
        pos_side_payload: str | None = None
        try:
            live_positions = client.fetch_positions(symbols=[symbol])
        except Exception:
            live_positions = []
        for entry in live_positions or []:
            inst_id = (entry.get("instId") or "").upper()
            if inst_id != symbol:
                continue
            detected_side = (entry.get("posSide") or entry.get("side") or "").strip().lower()
            if detected_side == "net":
                pos_side_payload = None
            elif detected_side:
                pos_side_payload = detected_side.upper()
            raw_mode = (entry.get("mgnMode") or entry.get("marginMode") or "").strip()
            if raw_mode:
                margin_mode_value = raw_mode.lower()
            break

        size_text = f"{adjusted_qty:.8f}".rstrip("0").rstrip(".")
        if not size_text:
            size_text = "0"
        response = client.place_order(
            {
                "instrument_id": symbol,
                "side": open_side,
                "order_type": "market",
                "size": size_text,
                "margin_mode": margin_mode_value,
                "pos_side": pos_side_payload,
                "leverage": _resolve_leverage(meta),
            }
        )
        try:
            get_okx_summary(force_refresh=True, ttl_seconds=0)
        except Exception as refresh_exc:  # pragma: no cover
            logger.debug("Post-scale summary refresh failed: %s", refresh_exc)
    except OkxClientError as exc:
        raise HTTPException(status_code=400, detail=_format_okx_error(exc))
    finally:
        client.close()
    return {
        "status": response.get("status"),
        "order_id": response.get("order_id"),
        "client_order_id": response.get("client_order_id"),
        "raw": response.get("raw"),
    }


def close_okx_position(
    *,
    account_id: str,
    instrument_id: str,
    position_side: str,
    quantity: float,
    margin_mode: str | None = None,
) -> dict:
    """Submit a market order to close an existing position."""
    account_key, meta = _resolve_okx_account_meta(account_id)
    for field in ("api_key", "api_secret"):
        if not meta.get(field):
            raise HTTPException(status_code=400, detail=f"?? {account_key} ?? {field}")

    try:
        normalized_qty = float(quantity)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="平仓数量必须大于 0")
    if normalized_qty <= 0:
        raise HTTPException(status_code=400, detail="平仓数量必须大于 0")

    side_lower = (position_side or "").lower()
    if side_lower in {"long", "buy"}:
        close_side = "sell"
        pos_side = "LONG"
    elif side_lower in {"short", "sell"}:
        close_side = "buy"
        pos_side = "SHORT"
    else:
        raise HTTPException(status_code=400, detail=f"??????? {position_side}")

    credentials = ExchangeCredentials(
        api_key=meta["api_key"],
        api_secret=meta["api_secret"],
        passphrase=meta.get("passphrase"),
    )
    client = OkxPaperClient()
    symbol = instrument_id.upper()
    instrument_meta = _get_okx_instrument_meta(symbol) or {}
    quote_currency = (
        instrument_meta.get("settleCurrency")
        or instrument_meta.get("ctValCcy")
        or instrument_meta.get("quoteCcy")
    )
    if not quote_currency:
        parts = symbol.split("-")
        if len(parts) >= 2:
            quote_currency = parts[1]
    position_size: float | None = None
    try:
        client.authenticate(credentials)
        margin_mode_value = _choose_margin_mode(symbol, meta, margin_mode)
        pos_side_payload: str | None = pos_side
        detected_side: str | None = None
        detected_margin_mode: str | None = None
        requested_entry_side = "long" if close_side == "sell" else "short"
        selected_entry: dict | None = None
        fallback_entry: dict | None = None
        fallback_side: str | None = None
        fallback_mode: str | None = None
        try:
            live_positions = client.fetch_positions(symbols=[symbol])
        except Exception:
            live_positions = []
        net_position = False
        for entry in live_positions or []:
            inst_id = (entry.get("instId") or "").upper()
            if inst_id != symbol:
                continue
            try:
                pos_qty = float(entry.get("pos", 0))
            except (TypeError, ValueError):
                pos_qty = 0.0
            if pos_qty == 0:
                continue
            entry_side = (entry.get("posSide") or entry.get("side") or "").strip().lower()
            if not entry_side:
                entry_side = "long" if pos_qty > 0 else "short"
            raw_mode = (entry.get("mgnMode") or entry.get("marginMode") or "").strip()
            normalized_mode = raw_mode.lower() if raw_mode else None
            if entry_side == "net":
                selected_entry = entry
                detected_side = entry_side
                detected_margin_mode = normalized_mode
                break
            if entry_side == requested_entry_side:
                selected_entry = entry
                detected_side = entry_side
                detected_margin_mode = normalized_mode
                break
            if fallback_entry is None:
                fallback_entry = entry
                fallback_side = entry_side
                fallback_mode = normalized_mode
        if selected_entry is None and fallback_entry is not None:
            selected_entry = fallback_entry
            detected_side = fallback_side
            detected_margin_mode = fallback_mode
        entry = selected_entry
        if detected_margin_mode:
            margin_mode_value = detected_margin_mode
        if detected_side and entry:
            if detected_side == "net":
                pos_side_payload = None
                net_position = True
            else:
                pos_side_payload = detected_side.upper()
            try:
                position_size = abs(float(entry.get("pos", 0.0)))
            except Exception:
                position_size = None

        if position_size is None or position_size <= 0:
            raise HTTPException(status_code=400, detail=f"{symbol} 当前无可平仓位")

        response = client.close_position(
            instrument_id=symbol,
            margin_mode=margin_mode_value,
            pos_side=pos_side_payload,
            ccy=quote_currency if margin_mode_value == "cross" else None,
        )

        try:
            get_okx_summary(force_refresh=True, ttl_seconds=0)
        except Exception as refresh_exc:  # pragma: no cover - cache refresh best-effort
            logger.debug("Post-close summary refresh failed: %s", refresh_exc)
    except OkxClientError as exc:
        raise HTTPException(status_code=400, detail=_format_okx_error(exc))
    finally:
        client.close()

    return {
        "status": response.get("status"),
        "order_id": response.get("order_id"),
        "client_order_id": response.get("client_order_id"),
        "closed_side": close_side,
        "message": f"平仓 {symbol} 成功",
        "raw": response.get("raw"),
    }






@router.post("/okx/close-all-positions", include_in_schema=False)
def okx_close_all_positions(
    account_id: str = Form(...),
) -> RedirectResponse:
    try:
        summary = get_okx_summary(force_refresh=True)
        for bundle in summary.get("accounts", []):
            if bundle.get("account", {}).get("account_id") == account_id:
                for position in bundle.get("positions", []):
                    close_okx_position(
                        account_id=account_id,
                        instrument_id=position.get("instrument_id"),
                        position_side=position.get("side"),
                        quantity=position.get("quantity"),
                        margin_mode=position.get("margin_mode"),
                    )
        # Refresh once more so repository + cache pick up the flattened positions.
        get_okx_summary(force_refresh=True, ttl_seconds=0)
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=success&detail=All positions closed",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=error&detail={urllib.parse.quote_plus(str(exc))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )


def cancel_okx_order(
    *,
    account_id: str,
    instrument_id: str,
    order_id: str,
) -> dict:
    """Cancel an open OKX order."""
    account_key, meta = _resolve_okx_account_meta(account_id)
    for field in ("api_key", "api_secret"):
        if not meta.get(field):
            raise HTTPException(status_code=400, detail=f"账户 {account_key} 缺少 {field}")
    symbol = (instrument_id or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="合约ID不能为空")
    normalized_order_id = str(order_id or "").strip()
    if not normalized_order_id:
        raise HTTPException(status_code=400, detail="订单ID不能为空")

    credentials = ExchangeCredentials(
        api_key=meta["api_key"],
        api_secret=meta["api_secret"],
        passphrase=meta.get("passphrase"),
    )
    client = OkxPaperClient()
    try:
        client.authenticate(credentials)
        response = client.cancel_order(order_id=normalized_order_id, instrument_id=symbol)
    except OkxClientError as exc:
        raise HTTPException(status_code=400, detail=_format_okx_error(exc))
    finally:
        client.close()

    return {
        "status": response.get("status"),
        "order_id": response.get("order_id"),
        "instrument_id": symbol,
        "raw": response.get("raw"),
    }


def get_liquidation_snapshot(limit: int = 50, instrument: Optional[str] = None) -> dict:
    """Return recent liquidation aggregates for dashboard/API consumption."""
    instrument_filter = (instrument or "").strip().upper()
    sanitized_limit = max(1, limit)
    lookback = "60m"
    query_limit = sanitized_limit
    if instrument_filter:
        lookback = "24h"
        query_limit = max(sanitized_limit * 4, 120)
    records = _query_influx_measurement(
        measurement="okx_liquidations",
        limit=query_limit,
        instrument=instrument_filter or None,
        lookback=lookback,
    )
    if not records:
        records = _fetch_live_liquidations(instrument=instrument_filter or None, limit=query_limit)
    cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=120)
    items: list[dict] = []
    fallback_items: list[dict] = []
    grouped_records: dict[str, list[dict]] = {}
    for row in records:
        inst_id = (row.get("instrument_id") or "").upper()
        timestamp = row.get("_time")
        ts = timestamp
        if isinstance(timestamp, str):
            try:
                ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                ts = None
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        net_qty = _coerce_float(row.get("net_qty"))
        last_price = _coerce_float(row.get("last_price"))
        notional_value = _coerce_float(row.get("notional_value"))
        if notional_value is None and net_qty is not None and last_price is not None:
            notional_value = abs(net_qty) * last_price
        record_payload = {
            "instrument_id": inst_id,
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(timestamp),
            "long_qty": _coerce_float(row.get("long_qty")),
            "short_qty": _coerce_float(row.get("short_qty")),
            "net_qty": net_qty,
            "last_price": last_price,
            "notional_value": notional_value,
        }
        grouped_records.setdefault(inst_id, []).append(record_payload)
        fallback_items.append(record_payload)
        if isinstance(ts, datetime) and ts < cutoff:
            continue
        items.append(record_payload)
    if not items:
        items = fallback_items[:sanitized_limit]
    items.sort(key=lambda entry: entry["timestamp"], reverse=True)
    items = items[:sanitized_limit]
    tracked_instruments = [
        inst.strip().upper()
        for inst in (TRADABLE_INSTRUMENTS or [])
        if isinstance(inst, str) and inst.strip()
    ]
    if instrument_filter and instrument_filter not in tracked_instruments:
        tracked_instruments.append(instrument_filter)
    baseline_overrides = _compute_wave_baselines(grouped_records)
    price_change_map = _compute_wave_price_changes(grouped_records)
    wave_detector.bulk_update(grouped_records, baseline_overrides=baseline_overrides)
    wave_signals = wave_detector.snapshot_signals(tracked_instruments or grouped_records.keys())
    for signal in wave_signals:
        inst_upper = (signal.instrument or "").upper()
        price_change = price_change_map.get(inst_upper)
        if price_change is not None:
            signal.metrics.price_change_pct = price_change
    wave_payloads: list[dict] = [signal.to_payload() for signal in wave_signals]
    wave_summary = _summarize_wave_status(wave_signals)
    if not wave_payloads:
        wave_payloads = _fallback_wave_signals(grouped_records)
        if wave_payloads:
            for payload in wave_payloads:
                inst_upper = (payload.get("instrument") or "").upper()
                price_change = price_change_map.get(inst_upper)
                if price_change is not None:
                    payload.setdefault("metrics", {})["price_change_pct"] = price_change
            wave_summary = "Fallback: using last 30m liquidations"
    return {
        "instrument": instrument_filter,
        "items": items,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        "wave_signals": wave_payloads,
        "wave_summary": wave_summary,
    }


def _summarize_wave_status(signals: Sequence[wave_detector.WaveSignal]) -> str:
    if not signals:
        return "等待数据"
    active = [sig.instrument for sig in signals if sig.status == "波次进行"]
    absorbs = [sig.instrument for sig in signals if sig.signal_code == "bottom_absorb"]
    tops = [sig.instrument for sig in signals if sig.signal_code == "top_signal"]
    reversals = [sig.instrument for sig in signals if sig.signal_code == "short_reversal"]
    warns = [sig.instrument for sig in signals if sig.status in {"警戒", "顶部预警"}]
    if active:
        return f"波次进行中：{', '.join(active)}"
    if reversals:
        return f"空单反转：{', '.join(reversals)}"
    if tops:
        return f"顶部：{', '.join(tops)}"
    if warns:
        return f"警戒：{', '.join(warns)}"
    if absorbs:
        return f"吸收：{', '.join(absorbs)}"
    return f"监控 {len(signals)} 个合约"



def _fallback_wave_signals(grouped_records: dict[str, list[dict]]) -> list[dict]:
    """Build lightweight wave rows from recent liquidation records (30m lookback)."""
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(minutes=30)
    fallbacks: list[dict] = []

    def _parse_ts(raw: Any) -> datetime | None:
        ts = raw
        if isinstance(raw, str):
            try:
                ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
        if isinstance(ts, datetime):
            return ts.replace(tzinfo=ts.tzinfo or timezone.utc)
        return None

    for instrument, records in grouped_records.items():
        recent: list[dict] = []
        for rec in records:
            ts = _parse_ts(rec.get("timestamp"))
            if ts is None or ts < cutoff:
                continue
            recent.append(rec)
        if not recent:
            continue

        total_notional = 0.0
        total_count = 0
        net_qty_sum = 0.0
        buy_notional = 0.0
        sell_notional = 0.0
        for rec in recent:
            qty = _coerce_float(rec.get("net_qty")) or 0.0
            last_price = _coerce_float(rec.get("last_price")) or 0.0
            notional = _coerce_float(rec.get("notional_value"))
            if notional is None:
                notional = abs(qty) * last_price if last_price else 0.0
            total_notional += abs(notional or 0.0)
            net_qty_sum += qty
            total_count += 1
            if qty > 0:
                buy_notional += abs(notional or 0.0)
            elif qty < 0:
                sell_notional += abs(notional or 0.0)

        if not total_count:
            continue

        avg_notional = total_notional / total_count if total_count else 0.0
        density = total_count / 30.0
        detail = f"30m buy:{buy_notional:.2f} sell:{sell_notional:.2f}"
        fallbacks.append(
            {
                "instrument": instrument,
                "wave": "historical",
                "status": "fallback",
                "signal": "Fallback from last 30m liquidations",
                "signal_class": "neutral",
                "signal_code": "",
                "liquidation_side_label": detail,
                "metrics": {
                    "flv": total_notional,
                    "baseline": avg_notional,
                    "price_change_pct": 0.0,
                    "price_drop_pct": 0.0,
                    "le": total_count,
                    "pc": total_notional,
                    "density_per_min": density,
                    "lpi": abs(net_qty_sum),
                },
            }
        )

    return fallbacks


def _parse_record_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(text)
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _extract_record_abs_qty(record: dict) -> float:
    qty = _coerce_float(record.get("net_qty"))
    if qty is not None:
        return abs(qty)
    long_qty = _coerce_float(record.get("long_qty"))
    short_qty = _coerce_float(record.get("short_qty"))
    candidates = [long_qty, short_qty]
    for value in candidates:
        if value:
            return abs(value)
    notional = _coerce_float(record.get("notional_value"))
    price = _coerce_float(record.get("last_price"))
    if notional and price and price > 0:
        return abs(notional / price)
    return 0.0


def _compute_wave_baselines(grouped_records: dict[str, list[dict]]) -> dict[str, float]:
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(hours=4)
    baselines: dict[str, float] = {}
    for instrument, records in grouped_records.items():
        total = 0.0
        count = 0
        for record in records:
            ts = _parse_record_timestamp(record.get("timestamp"))
            if ts is None or ts < cutoff:
                continue
            total += _extract_record_abs_qty(record)
            count += 1
        baselines[instrument.upper()] = total / count if count else 0.0
    return baselines


def _compute_wave_price_changes(grouped_records: dict[str, list[dict]]) -> dict[str, float]:
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(minutes=15)
    price_map: dict[str, float] = {}
    for instrument, records in grouped_records.items():
        latest: tuple[datetime, float] | None = None
        reference: tuple[datetime, float] | None = None
        for record in records:
            ts = _parse_record_timestamp(record.get("timestamp"))
            price = _coerce_float(record.get("last_price"))
            if ts is None or price is None or price <= 0:
                continue
            if latest is None or ts > latest[0]:
                latest = (ts, price)
            if ts <= cutoff:
                if reference is None or ts > reference[0]:
                    reference = (ts, price)
        if latest is None:
            continue
        latest_price = latest[1]
        base_price = reference[1] if reference else latest_price
        if base_price <= 0:
            continue
        change_pct = ((latest_price - base_price) / base_price) * 100.0
        price_map[instrument.upper()] = change_pct
    return price_map


def get_orderbook_snapshot(
    levels: int = 10,
    instrument: Optional[str] = None,
    *,
    allow_live_fallback: bool = True,
    force_live: bool = False,
    stale_after_seconds: int = 5,
) -> dict:
    """Return latest order book snapshots for configured instruments."""
    instrument_filter = (instrument or "").strip().upper()
    fetch_limit = max(1, max(levels, len(TRADABLE_INSTRUMENTS)))
    history_cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=ORDERBOOK_HISTORY_LOOKBACK_HOURS)
    # Keep client payloads small enough for fast initial page loads while covering 4h charts.
    max_history_points = max(300, MAX_HISTORY_POINTS_PER_INSTRUMENT)
    instrument_count = max(1, len(TRADABLE_INSTRUMENTS))
    history_fetch_limit = min(max_history_points * instrument_count, max_history_points * 8)
    micro_lookback_hours = max(1, min(ORDERBOOK_HISTORY_LOOKBACK_HOURS, 2))
    micro_records = _query_influx_measurement(
        measurement="market_microstructure",
        limit=max(history_fetch_limit, 500),
        instrument=instrument_filter or None,
        lookback=f"{micro_lookback_hours}h",
    )
    cvd_map: dict[str, float | None] = {}
    cvd_history_map: dict[str, list[dict]] = {}
    for row in micro_records:
        inst_id = (row.get("instrument_id") or "").upper()
        if not inst_id:
            continue
        timestamp = row.get("_time")
        ts = timestamp
        if isinstance(timestamp, str):
            try:
                ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                ts = None
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        cvd_value = _coerce_float(row.get("cvd"))
        if inst_id not in cvd_map:
            cvd_map[inst_id] = cvd_value
        entry = {
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(timestamp),
            "cvd": cvd_value,
        }
        if isinstance(ts, datetime):
            entry["_ts"] = ts
        history_list = cvd_history_map.setdefault(inst_id, [])
        history_list.append(entry)
        if isinstance(ts, datetime):
            history_list[:] = [
                item
                for item in history_list
                if not isinstance(item.get("_ts"), datetime) or item["_ts"] >= history_cutoff
            ]
        if len(history_list) > max_history_points:
            del history_list[max_history_points:]
    history_records = _query_influx_measurement(
        measurement="okx_orderbook_depth",
        limit=history_fetch_limit,
        instrument=instrument_filter or None,
        lookback=f"{ORDERBOOK_HISTORY_LOOKBACK_HOURS}h",
    )
    snapshot_records = _query_influx_measurement(
        measurement="okx_orderbook_depth",
        limit=fetch_limit * 2,
        instrument=instrument_filter or None,
        lookback=f"{max(2, ORDERBOOK_HISTORY_LOOKBACK_HOURS)}h",
    )
    seen: set[str] = set()
    items: list[dict] = []
    history_map: dict[str, list[dict]] = {}

    def _append_history_entry(row: dict) -> None:
        inst_id = (row.get("instrument_id") or "").upper()
        if not inst_id:
            return
        timestamp = row.get("_time")
        ts = timestamp
        if isinstance(timestamp, str):
            try:
                ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                ts = None
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        net_depth = _coerce_float(row.get("net_depth"))
        if net_depth is None:
            total_bid_qty = _coerce_float(row.get("total_bid_qty"))
            total_ask_qty = _coerce_float(row.get("total_ask_qty"))
            if total_bid_qty is not None and total_ask_qty is not None:
                net_depth = total_bid_qty - total_ask_qty
        hist_entry = {
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(timestamp),
            "net_depth": net_depth,
        }
        if isinstance(ts, datetime):
            hist_entry["_ts"] = ts
        history_list = history_map.setdefault(inst_id, [])
        history_list.append(hist_entry)
        if isinstance(ts, datetime):
            history_list[:] = [
                entry
                for entry in history_list
                if not isinstance(entry.get("_ts"), datetime) or entry["_ts"] >= history_cutoff
            ]
        if len(history_list) > max_history_points:
            del history_list[max_history_points:]

    def _build_history_payload(inst_id: str) -> tuple[list[dict], list[dict], float | None]:
        raw_history = history_map.get(inst_id, [])
        history = [
            {"timestamp": entry["timestamp"], "net_depth": entry.get("net_depth")}
            for entry in reversed(raw_history)
        ]
        cvd_history_raw = cvd_history_map.get(inst_id, [])
        cvd_history = [
            {"timestamp": entry["timestamp"], "cvd": entry.get("cvd")}
            for entry in reversed(cvd_history_raw)
        ]
        return history, cvd_history, cvd_map.get(inst_id)

    for row in history_records:
        _append_history_entry(row)
    # When querying without a specific instrument filter, Influx may return fewer rows
    # than expected due to per-table limits. Backfill missing instruments with direct queries.
    if instrument_filter:
        target_instruments = {instrument_filter}
    else:
        target_instruments = {
            inst.strip().upper()
            for inst in TRADABLE_INSTRUMENTS
            if isinstance(inst, str) and inst.strip()
        }
    for inst_id in target_instruments:
        if not inst_id or history_map.get(inst_id):
            continue
        extra_rows = _query_influx_measurement(
            measurement="okx_orderbook_depth",
            limit=max_history_points,
            instrument=inst_id,
            lookback=f"{ORDERBOOK_HISTORY_LOOKBACK_HOURS}h",
        )
        for row in extra_rows:
            _append_history_entry(row)

    for row in snapshot_records:
        inst_id = (row.get("instrument_id") or "").upper()
        if not inst_id:
            continue
        timestamp = row.get("_time")
        ts = timestamp
        if isinstance(timestamp, str):
            try:
                ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                ts = None
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        bids_raw = row.get("bids_json") or "[]"
        asks_raw = row.get("asks_json") or "[]"
        total_bid_qty = _coerce_float(row.get("total_bid_qty"))
        total_ask_qty = _coerce_float(row.get("total_ask_qty"))
        _append_history_entry(row)
        if instrument_filter and inst_id != instrument_filter:
            continue
        if inst_id in seen:
            continue
        seen.add(inst_id)
        try:
            bids = json.loads(bids_raw)
        except (TypeError, json.JSONDecodeError):
            bids = []
        try:
            asks = json.loads(asks_raw)
        except (TypeError, json.JSONDecodeError):
            asks = []
        history, cvd_history, cvd_value = _build_history_payload(inst_id)
        items.append(
            {
                "instrument_id": inst_id,
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(timestamp),
                "best_bid": _coerce_float(row.get("best_bid")),
                "best_ask": _coerce_float(row.get("best_ask")),
                "spread": _coerce_float(row.get("spread")),
                "total_bid_qty": _coerce_float(row.get("total_bid_qty")),
                "total_ask_qty": _coerce_float(row.get("total_ask_qty")),
                "bids": bids[: max(1, levels)],
                "asks": asks[: max(1, levels)],
                "history": history,
                "cvd": cvd_value,
                "cvd_history": cvd_history,
            }
        )
    now = datetime.now(tz=timezone.utc)
    stale_cutoff = now - timedelta(seconds=max(1, stale_after_seconds))
    live_items: list[dict] = []

    def _is_stale(entry: dict) -> bool:
        ts = _coerce_datetime(entry.get("timestamp"), default=now)
        return ts < stale_cutoff

    needs_live_refresh = force_live or not items or any(_is_stale(entry) for entry in items)
    if allow_live_fallback and needs_live_refresh:
        live_items = _fetch_live_orderbooks(levels=levels, instrument_filter=instrument_filter)
        if live_items:
            items = []
            for entry in live_items:
                inst_id = (entry.get("instrument_id") or "").upper()
                if not inst_id:
                    continue
                history, cvd_history, cvd_value = _build_history_payload(inst_id)
                items.append(
                    {
                        "instrument_id": inst_id,
                        "timestamp": entry.get("timestamp"),
                        "best_bid": entry.get("best_bid"),
                        "best_ask": entry.get("best_ask"),
                        "spread": entry.get("spread"),
                        "total_bid_qty": entry.get("total_bid_qty"),
                        "total_ask_qty": entry.get("total_ask_qty"),
                        "bids": (entry.get("bids") or [])[: max(1, levels)],
                        "asks": (entry.get("asks") or [])[: max(1, levels)],
                        "history": history,
                        "cvd": cvd_value,
                        "cvd_history": cvd_history,
                    }
                )
    if not items and _ORDERBOOK_WARM_CACHE and _ORDERBOOK_WARM_CACHE.get("items"):
        cached_items = _ORDERBOOK_WARM_CACHE.get("items", [])
        if instrument_filter:
            cached_items = [
                entry
                for entry in cached_items
                if (entry.get("instrument_id") or "").upper() == instrument_filter
            ]
        items = cached_items
    if not items and not live_items and allow_live_fallback:
        items = _fetch_live_orderbooks(levels=levels, instrument_filter=instrument_filter)
    if not items and not allow_live_fallback:
        items = []
    if items:
        _persist_orderbook_items(items)
    items.sort(key=lambda entry: entry["instrument_id"])
    return {
        "instrument": instrument_filter,
        "levels": max(1, levels),
        "items": items,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def get_liquidation_map_history_snapshot(
    *,
    instrument: Optional[str],
    lookback_hours: int,
    limit: int,
) -> dict:
    instrument_filter = (instrument or "").strip().upper()
    rows = _query_influx_measurement(
        measurement="liquidation_map_bins",
        limit=limit,
        instrument=instrument_filter or None,
        lookback=f"{max(1, lookback_hours)}h",
    )
    if rows:
        return _build_snapshot_from_bin_rows(
            rows=rows,
            instrument_filter=instrument_filter,
            lookback_hours=lookback_hours,
        )
    return _build_snapshot_from_liquidation_orders(
        instrument_filter=instrument_filter,
        lookback_hours=lookback_hours,
        limit=limit,
    )


def _build_snapshot_from_bin_rows(
    *,
    rows: list[dict],
    instrument_filter: str,
    lookback_hours: int,
) -> dict:
    aggregated: dict[tuple[str, float], dict] = {}
    earliest: datetime | None = None
    latest: datetime | None = None
    bin_size_value: float | None = None
    latest_bucket_price: float | None = None
    latest_ts: datetime | None = None
    for row in rows:
        side = str(row.get("side") or "").lower()
        if side not in {"bid", "ask"}:
            continue
        price_bucket = _coerce_float(row.get("bucket_start") or row.get("price"))
        if price_bucket is None:
            continue
        notional = max(0.0, _coerce_float(row.get("notional")) or 0.0)
        size = max(0.0, _coerce_float(row.get("size")) or 0.0)
        if bin_size_value is None:
            bin_size_value = _coerce_float(row.get("bin_size")) or 1.0
        ts = _coerce_datetime(row.get("_time"), default=datetime.now(tz=timezone.utc))
        if earliest is None or ts < earliest:
            earliest = ts
        if latest is None or ts > latest:
            latest = ts
        if latest_ts is None or ts > latest_ts:
            latest_ts = ts
            latest_bucket_price = _coerce_float(row.get("price"))
        key = (side, price_bucket)
        entry = aggregated.setdefault(
            key,
            {
                "price": price_bucket,
                "side": side,
                "notional": 0.0,
                "size": 0.0,
            },
        )
        entry["notional"] += notional
        entry["size"] += size
    bins = sorted(aggregated.values(), key=lambda item: item["price"])
    cumulative_by_side: dict[str, float] = {"bid": 0.0, "ask": 0.0}
    for bucket in bins:
        cumulative_by_side[bucket["side"]] += bucket["notional"]
        bucket["cumulative"] = cumulative_by_side[bucket["side"]]
    return {
        "instrument": instrument_filter,
        "bin_size": bin_size_value or 1.0,
        "bins": bins,
        "lookback_hours": lookback_hours,
        "range_start": earliest.isoformat() if earliest else None,
        "range_end": latest.isoformat() if latest else None,
        "totals": cumulative_by_side,
        "records": len(rows),
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    if latest_bucket_price is not None:
        payload["latest_price"] = latest_bucket_price
    return payload


def _build_snapshot_from_liquidation_orders(
    *,
    instrument_filter: str,
    lookback_hours: int,
    limit: int,
) -> dict:
    lookback = f"{max(1, lookback_hours)}h"
    records_limit = max(limit, lookback_hours * 400)
    rows = _query_influx_measurement(
        measurement="okx_liquidations",
        limit=records_limit,
        instrument=instrument_filter or None,
        lookback=lookback,
    )
    if not rows:
        rows = _fetch_live_liquidations(
            instrument=instrument_filter or None,
            limit=records_limit,
        )
    (
        bins,
        bin_size_value,
        earliest,
        latest,
        latest_price,
        processed_records,
    ) = _aggregate_liquidation_bins(rows, instrument_filter=instrument_filter)
    cumulative_by_side: dict[str, float] = {"bid": 0.0, "ask": 0.0}
    for bucket in bins:
        cumulative_by_side[bucket["side"]] += bucket["notional"]
        bucket["cumulative"] = cumulative_by_side[bucket["side"]]
    payload: dict[str, object] = {
        "instrument": instrument_filter,
        "bin_size": bin_size_value or 1.0,
        "bins": bins,
        "lookback_hours": lookback_hours,
        "range_start": earliest.isoformat() if earliest else None,
        "range_end": latest.isoformat() if latest else None,
        "totals": cumulative_by_side,
        "records": processed_records,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    if latest_price is not None:
        payload["latest_price"] = latest_price
    return payload


def _aggregate_liquidation_bins(
    rows: Sequence[dict],
    *,
    instrument_filter: str,
) -> tuple[list[dict], float | None, datetime | None, datetime | None, float | None, int]:
    normalized: list[dict] = []
    earliest: datetime | None = None
    latest: datetime | None = None
    latest_price: float | None = None
    latest_ts: datetime | None = None
    price_samples: list[float] = []
    processed_records = 0
    for row in rows:
        inst = (row.get("instrument_id") or row.get("instrument") or "").strip().upper()
        if instrument_filter and inst and inst != instrument_filter:
            continue
        price = _coerce_float(row.get("last_price") or row.get("price"))
        if price is None or price <= 0:
            continue
        net_qty = _coerce_float(row.get("net_qty"))
        long_qty = max(0.0, _coerce_float(row.get("long_qty")) or 0.0)
        short_qty = max(0.0, _coerce_float(row.get("short_qty")) or 0.0)
        notional = max(0.0, _coerce_float(row.get("notional_value")) or 0.0)
        qty_basis = max(abs(net_qty or 0.0), long_qty, short_qty)
        if notional <= 0 and qty_basis > 0 and price > 0:
            notional = qty_basis * price
        if notional <= 0:
            continue
        size_value = qty_basis or (notional / price if price else 0.0)
        if size_value <= 0:
            continue
        side = "ask"
        if net_qty is not None:
            side = "bid" if net_qty < 0 else "ask"
        elif long_qty or short_qty:
            side = "ask" if long_qty >= short_qty else "bid"
        ts = _coerce_datetime(row.get("_time"), default=None)
        ts = ts or datetime.now(tz=timezone.utc)
        if earliest is None or ts < earliest:
            earliest = ts
        if latest is None or ts > latest:
            latest = ts
        if latest_ts is None or ts > latest_ts:
            latest_ts = ts
            latest_price = price
        price_samples.append(price)
        normalized.append(
            {
                "side": side,
                "price": price,
                "notional": notional,
                "size": size_value,
            }
        )
        processed_records += 1
    if not normalized:
        return [], None, earliest, latest, latest_price, processed_records
    bin_size_value = _estimate_liquidation_bin_size(price_samples)
    aggregated: dict[tuple[str, float], dict] = {}
    for entry in normalized:
        bucket = math.floor(entry["price"] / bin_size_value) * bin_size_value
        key = (entry["side"], bucket)
        bucket_entry = aggregated.setdefault(
            key,
            {"price": bucket, "side": entry["side"], "notional": 0.0, "size": 0.0},
        )
        bucket_entry["notional"] += entry["notional"]
        bucket_entry["size"] += entry["size"]
    bins = sorted(aggregated.values(), key=lambda item: item["price"])
    return bins, bin_size_value, earliest, latest, latest_price, processed_records


def _estimate_liquidation_bin_size(prices: Sequence[float]) -> float:
    if not prices:
        return 1.0
    min_price = min(prices)
    max_price = max(prices)
    span = max(max_price - min_price, max_price * 0.001, 1e-6)
    target_bins = 160
    raw_step = span / target_bins
    if raw_step <= 0:
        raw_step = max_price * 0.001 or 1.0
    magnitude = 10 ** math.floor(math.log10(raw_step))
    normalized = raw_step / magnitude
    if normalized < 1.5:
        step = 1 * magnitude
    elif normalized < 3:
        step = 2 * magnitude
    elif normalized < 7:
        step = 5 * magnitude
    else:
        step = 10 * magnitude
    # ensure tiny-priced assets still get sub-unit resolution
    if max_price < 5:
        step = min(step, 0.01)
    if max_price < 1:
        step = min(step, 0.001)
    return max(step, 1e-6)


def prime_orderbook_cache(levels: int = 400, instrument: Optional[str] = None) -> dict:
    """
    Populate a warm cache from local Influx data without hitting external sources.

    Used at service startup so first requests render immediately; live data will
    be fetched asynchronously afterwards.
    """
    global _ORDERBOOK_WARM_CACHE
    snapshot = get_orderbook_snapshot(
        levels=levels,
        instrument=instrument,
        allow_live_fallback=False,
    )
    _ORDERBOOK_WARM_CACHE = snapshot
    return snapshot


async def refresh_orderbook_live_async(levels: int = 400, instrument: Optional[str] = None) -> None:
    """
    Fetch fresh orderbook data from upstream in a background task.

    This runs the synchronous OKX call in a thread so the caller isn't blocked.
    """
    loop = asyncio.get_running_loop()
    instrument_filter = (instrument or "").strip().upper() or None
    await loop.run_in_executor(
        None,
        lambda: _fetch_live_orderbooks(levels=levels, instrument_filter=instrument_filter),
    )


def get_market_depth_attachments(limit: int = 6) -> list[dict]:
    """Return recent market depth screenshot attachments as data URLs."""
    directory = MARKET_DEPTH_ATTACHMENT_DIR
    if not directory.exists():
        return []
    allowed = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
    entries: list[dict] = []
    try:
        files = sorted(
            [path for path in directory.iterdir() if path.is_file()],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return []
    for path in files:
        if path.suffix.lower() not in allowed:
            continue
        try:
            data = path.read_bytes()
            stat = path.stat()
        except OSError:
            continue
        mime = _guess_mime_from_suffix(path.suffix.lower())
        encoded = base64.b64encode(data).decode("ascii")
        entries.append(
            {
                "filename": path.name,
                "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "data_url": f"data:{mime};base64,{encoded}",
            }
        )
        if len(entries) >= max(1, limit):
            break
    return entries


def _persist_live_snapshot(
    repo: AccountRepository,
    live: dict,
    *,
    existing: Account | None,
    timestamp: datetime,
) -> None:
    """Persist a live OKX snapshot bundle into the repository."""
    acc = live.get("account", {}) or {}
    created_at = existing.created_at if existing is not None else timestamp
    account_model = Account(
        account_id=str(acc.get("account_id")),
        model_id=str(acc.get("model_id")),
        base_currency=str(acc.get("base_currency", "USD")),
        starting_equity=float(acc.get("starting_equity", 0.0)),
        cash_balance=float(acc.get("cash_balance", 0.0)),
        equity=float(acc.get("equity", 0.0)),
        pnl=float(acc.get("pnl", 0.0)),
        created_at=created_at,
        updated_at=timestamp,
    )
    repo.upsert_account(account_model)
    try:
        repo.record_equity_point(account_model)
    except Exception:  # best-effort, non-fatal
        pass

    # Persist balances
    for b in live.get("balances", []) or []:
        try:
            balance = Balance(
                balance_id=str(b.get("balance_id") or f"{account_model.account_id}-{b.get('currency','')}"),
                account_id=str(b.get("account_id", account_model.account_id)),
                currency=str(b.get("currency", "")),
                total=float(b.get("total", 0.0)),
                available=float(b.get("available", 0.0)),
                frozen=float(b.get("frozen", 0.0)),
                equity=float(b.get("equity", 0.0)),
                updated_at=timestamp,
            )
            repo.record_balance(balance)
        except Exception:
            continue

    # Persist positions
    for p in live.get("positions", []) or []:
        try:
            position = Position(
                position_id=str(p.get("position_id")),
                account_id=str(p.get("account_id", account_model.account_id)),
                instrument_id=str(p.get("instrument_id", "")),
                side=str(p.get("side", "")),
                quantity=float(p.get("quantity", 0.0)),
                entry_price=float(p.get("entry_price", 0.0)),
                mark_price=p.get("mark_price"),
                leverage=p.get("leverage"),
                unrealized_pnl=p.get("unrealized_pnl"),
                notional_value=p.get("notional_value"),
                initial_margin=p.get("initial_margin"),
                updated_at=timestamp,
            )
            repo.record_position(position)
        except Exception:
            continue

    # Persist open orders
    for o in live.get("open_orders", []) or []:
        try:
            order = Order(
                order_id=str(o.get("order_id")),
                account_id=str(o.get("account_id", account_model.account_id)),
                model_id=str(o.get("model_id", account_model.model_id)),
                instrument_id=str(o.get("instrument_id", "")),
                side=str(o.get("side", "")),
                order_type=str(o.get("order_type", "")),
                size=float(o.get("size", 0.0)),
                filled_size=float(o.get("filled_size", 0.0)),
                price=o.get("price"),
                average_price=o.get("average_price"),
                state=str(o.get("state", "live")),
                created_at=timestamp,
                updated_at=timestamp,
            )
            repo.record_order(order)
        except Exception:
            continue

    # Persist recent trades
    for t in live.get("recent_trades", []) or []:
        try:
            close_price_value = t.get("close_price")
            if close_price_value in (None, ""):
                close_price = None
            else:
                try:
                    close_price = float(close_price_value)
                except (TypeError, ValueError):
                    close_price = None
            executed_raw = (
                t.get("executed_at")
                or t.get("executedAt")
                or t.get("timestamp")
                or t.get("ts")
            )
            executed_at = _coerce_datetime(executed_raw, default=timestamp)
            trade = Trade(
                trade_id=str(t.get("trade_id")),
                account_id=str(t.get("account_id", account_model.account_id)),
                model_id=str(t.get("model_id", account_model.model_id)),
                instrument_id=str(t.get("instrument_id", "")),
                side=str(t.get("side", "")),
                quantity=float(t.get("quantity", 0.0)),
                price=float(t.get("price", 0.0)),
                close_price=close_price,
                fee=t.get("fee"),
                realized_pnl=t.get("realized_pnl"),
                executed_at=executed_at,
            )
            repo.record_trade(trade)
        except Exception:
            continue


def _trade_to_dict(trade: Trade) -> dict:
    payload = trade.dict()
    if isinstance(payload.get("executed_at"), datetime):
        payload["executed_at"] = payload["executed_at"].isoformat()
    return payload


def _order_to_dict(order: Order) -> dict:
    payload = order.dict()
    if isinstance(payload.get("created_at"), datetime):
        payload["created_at"] = payload["created_at"].isoformat()
    if isinstance(payload.get("updated_at"), datetime):
        payload["updated_at"] = payload["updated_at"].isoformat()
    return payload


def _summarize_account_performance(
    *,
    account: Account,
    positions: List[Position],
    trades: List[Trade],
    equity_curve: List[dict],
) -> dict:
    exposure = _compute_notional_exposure(positions)
    sharpe_ratio = _compute_sharpe_ratio(equity_curve)
    max_drawdown_pct = _compute_max_drawdown_pct(equity_curve)
    win_rate_pct = _compute_win_rate_pct(trades)
    avg_trade_duration_min = _compute_avg_trade_interval_minutes(trades)

    return {
        "model_id": account.model_id,
        "portfolio_id": account.account_id,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown_pct": max_drawdown_pct,
        "win_rate_pct": win_rate_pct,
        "avg_trade_duration_min": avg_trade_duration_min,
        "exposure_usd": exposure,
        "open_positions": len(positions),
    }


def _compute_notional_exposure(positions: List[Position]) -> float:
    exposure = 0.0
    for position in positions:
        notional = position.notional_value
        if notional is None or notional <= 0:
            price = position.mark_price if position.mark_price is not None else position.entry_price
            notional = abs(position.quantity * price)
        exposure += abs(notional)
    return round(exposure, 2)


def _compute_sharpe_ratio(equity_curve: List[dict]) -> Optional[float]:
    equities: List[float] = []
    for point in equity_curve:
        try:
            equities.append(float(point.get("equity", 0.0)))
        except (TypeError, ValueError):
            continue
    if len(equities) < 2:
        return None
    returns: List[float] = []
    for previous, current in zip(equities, equities[1:]):
        if previous <= 0:
            continue
        returns.append((current - previous) / previous)
    if not returns:
        return None
    mean_return = sum(returns) / len(returns)
    variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
    if variance <= 0:
        return None
    sharpe = mean_return / math.sqrt(variance) * math.sqrt(len(returns))
    return round(sharpe, 2)


def _compute_max_drawdown_pct(equity_curve: List[dict]) -> Optional[float]:
    peak = None
    max_drawdown = 0.0
    for point in equity_curve:
        try:
            equity = float(point.get("equity", 0.0))
        except (TypeError, ValueError):
            continue
        if equity <= 0:
            continue
        if peak is None or equity > peak:
            peak = equity
        drawdown = ((peak - equity) / peak) if peak else 0.0
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    if peak is None:
        return None
    return round(max_drawdown * 100, 2)


def _compute_win_rate_pct(trades: List[Trade]) -> Optional[float]:
    wins = 0
    losses = 0
    for trade in trades:
        pnl = trade.realized_pnl
        if pnl is None:
            continue
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
    total = wins + losses
    if total == 0:
        return None
    return round((wins / total) * 100, 2)


def _compute_avg_trade_interval_minutes(trades: List[Trade]) -> Optional[float]:
    if len(trades) < 2:
        return None
    ordered = sorted(trades, key=lambda trade: trade.executed_at)
    intervals: List[float] = []
    for prev, current in zip(ordered, ordered[1:]):
        delta = current.executed_at - prev.executed_at
        intervals.append(delta.total_seconds() / 60.0)
    if not intervals:
        return None
    return round(sum(intervals) / len(intervals), 2)


def _mock_portfolio_metrics(record: PortfolioRecord) -> dict:
    """Generate deterministic placeholder metrics for portfolio responses."""
    base_seed = sum(ord(c) for c in record.portfolio_id)
    equity = record.starting_equity + (base_seed % 500)
    return {
        "last_synced": datetime.now(tz=timezone.utc).isoformat(),
        "balance": {
            record.base_currency: {
                "total": equity,
                "available": equity * 0.9,
            }
        },
        "unrealized_pnl": round((base_seed % 200) - 100, 2),
        "realized_pnl": round((base_seed % 150) - 75, 2),
        "open_positions": base_seed % 3,
        "risk_level": "medium",
    }


def _instrument_entry_to_payload(entry: dict) -> dict:
    """Return a compact instrument record suitable for API/HTML clients."""
    return {
        "inst_id": entry.get("instId"),
        "alias": entry.get("alias"),
        "base_currency": entry.get("baseCcy"),
        "quote_currency": entry.get("quoteCcy"),
        "settle_currency": entry.get("settleCcy"),
        "category": entry.get("category"),
        "state": entry.get("state"),
    }


def _query_influx_measurement(
    *,
    measurement: str,
    limit: int,
    instrument: Optional[str],
    lookback: str,
) -> list[dict]:
    if InfluxDBClient is None:
        return []
    try:
        config = InfluxConfig.from_env()
    except Exception as exc:
        logger.debug("Influx configuration unavailable for %s: %s", measurement, exc)
        return []
    if not config.token:
        return []
    instrument_filter = ""
    if instrument:
        instrument_filter = f'  |> filter(fn: (r) => r["instrument_id"] == "{instrument}")\n'
    flux = f"""
from(bucket: "{config.bucket}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
{instrument_filter}  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {max(1, limit)})
"""
    try:
        with InfluxDBClient(url=config.url, token=config.token, org=config.org) as client:
            tables = client.query_api().query(flux)
    except Exception as exc:
        logger.debug("Flux query failed for %s: %s", measurement, exc)
        return []
    records: list[dict] = []
    for table in tables:
        for record in getattr(table, "records", []):
            records.append(dict(record.values))
    return records


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_live_liquidations(*, instrument: str | None, limit: int) -> list[dict]:
    """Fallback to OKX REST when InfluxDB has no liquidation records."""
    instruments: Sequence[str]
    if instrument:
        instruments = [instrument]
    else:
        instruments = [
            inst.strip().upper()
            for inst in TRADABLE_INSTRUMENTS
            if isinstance(inst, str) and inst.strip()
        ]
    if not instruments:
        return []
    rows: list[dict] = []
    per_instrument_limit = max(5, min(limit, 100))
    try:
        with httpx.Client(base_url=OKX_REST_BASE, timeout=6.0) as client:
            for inst in instruments:
                inst_family = _derive_inst_family(inst)
                if not inst_family:
                    continue
                try:
                    response = client.get(
                        "/api/v5/public/liquidation-orders",
                        params={
                            "instType": "SWAP",
                            "instFamily": inst_family,
                            "state": "filled",
                            "limit": str(per_instrument_limit),
                        },
                    )
                    payload = response.json()
                except Exception as exc:
                    logger.debug("Live liquidation request failed for %s: %s", inst, exc)
                    continue
                if payload.get("code") not in {"0", 0}:
                    continue
                data = payload.get("data") or []
                for entry in data:
                    if not isinstance(entry, dict) or "$ref" in entry:
                        continue
                    details = entry.get("details") or []
                    for detail in details:
                        record = _convert_live_liquidation_detail(inst, detail)
                        if record:
                            rows.append(record)
    except Exception as exc:
        logger.debug("Live liquidation fallback failed: %s", exc)
    rows.sort(key=lambda row: row.get("_time") or "", reverse=True)
    return rows[:limit]


def _convert_live_liquidation_detail(instrument: str, detail: dict) -> dict | None:
    try:
        size = abs(float(detail.get("sz") or 0.0))
    except (TypeError, ValueError):
        size = 0.0
    if size <= 0:
        return None
    pos_side = (detail.get("posSide") or "").lower()
    side = (detail.get("side") or "").lower()
    long_qty = 0.0
    short_qty = 0.0
    if pos_side == "long" or (not pos_side and side == "sell"):
        long_qty = size
    elif pos_side == "short" or (not pos_side and side == "buy"):
        short_qty = size
    else:
        return None
    try:
        price = float(detail.get("bkPx") or detail.get("px") or 0.0)
    except (TypeError, ValueError):
        price = 0.0
    net_qty = long_qty - short_qty
    timestamp_value = detail.get("ts") or detail.get("time")
    timestamp = _coerce_timestamp_ms(timestamp_value)
    notional = None
    if price and net_qty:
        notional = abs(net_qty) * price
    return {
        "instrument_id": instrument,
        "_time": timestamp.isoformat() if timestamp else datetime.now(tz=timezone.utc).isoformat(),
        "long_qty": long_qty or None,
        "short_qty": short_qty or None,
        "net_qty": net_qty or None,
        "last_price": price or None,
        "notional_value": notional,
    }


def _coerce_timestamp_ms(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        ms = int(value)
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _derive_inst_family(inst_id: str) -> str | None:
    parts = (inst_id or "").upper().split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return None


def _fetch_live_orderbooks(*, levels: int, instrument_filter: str | None) -> list[dict]:
    instruments: Sequence[str]
    if instrument_filter:
        instruments = [instrument_filter]
    else:
        instruments = TRADABLE_INSTRUMENTS
    if not instruments:
        return []
    depth = max(1, min(500, levels if instrument_filter else max(levels, 50)))
    writer = _get_orderbook_writer()
    items: list[dict] = []
    try:
        with httpx.Client(base_url=OKX_REST_BASE, timeout=6.0) as client:
            for inst in instruments:
                inst_id = (inst or "").strip().upper()
                if not inst_id:
                    continue
                response = client.get(
                    "/api/v5/market/books",
                    params={"instId": inst_id, "sz": depth},
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("code") != "0":
                    continue
                data = payload.get("data") or []
                if not data:
                    continue
                snapshot = data[0]
                bids = _sanitize_book_levels(snapshot.get("bids") or [], depth)
                asks = _sanitize_book_levels(snapshot.get("asks") or [], depth)
                if not bids and not asks:
                    continue
                timestamp = _parse_okx_timestamp(snapshot.get("ts"))
                if writer is not None:
                    try:
                        writer.write_orderbook(
                            instrument_id=inst_id,
                            timestamp=timestamp,
                            bids=bids,
                            asks=asks,
                        )
                    except Exception as exc:  # pragma: no cover - persistence best-effort
                        logger.debug("Failed to persist orderbook snapshot for %s: %s", inst_id, exc)
                best_bid = bids[0][0] if bids else None
                best_ask = asks[0][0] if asks else None
                spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
                items.append(
                    {
                        "instrument_id": inst_id,
                        "timestamp": timestamp.isoformat(),
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "spread": spread,
                        "total_bid_qty": sum(level[1] for level in bids) if bids else None,
                        "total_ask_qty": sum(level[1] for level in asks) if asks else None,
                        "bids": bids[: max(1, levels)],
                        "asks": asks[: max(1, levels)],
                    }
                )
                if instrument_filter:
                    break
    except Exception as exc:
        logger.debug("Live orderbook fetch failed: %s", exc)
    return items


def _sanitize_book_levels(levels: Sequence[Sequence[object]], depth: int) -> list[list[float]]:
    sanitized: list[list[float]] = []
    max_depth = max(1, min(depth, 500))
    for idx, level in enumerate(levels):
        if idx >= max_depth:
            break
        try:
            price = float(level[0])
            size = float(level[1])
        except (TypeError, ValueError, IndexError):
            continue
        sanitized.append([price, size])
    return sanitized


def _parse_okx_timestamp(value: object) -> datetime:
    try:
        millis = int(value)
        return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError):
        return datetime.now(tz=timezone.utc)


def _coerce_datetime(value: object, *, default: datetime) -> datetime:
    """
    Convert an incoming timestamp to timezone-aware UTC datetime.
    Falls back to the provided default when parsing fails.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value is None:
        return default
    if isinstance(value, (int, float)):
        epoch = float(value)
        if epoch > 1e14:
            epoch = epoch / 1e9
        elif epoch > 1e11:
            epoch = epoch / 1e3
        try:
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        except (OverflowError, OSError):
            return default
    text = str(value).strip()
    if not text:
        return default
    if text.isdigit():
        try:
            epoch = float(text)
            if epoch > 1e14:
                epoch = epoch / 1e9
            elif epoch > 1e11:
                epoch = epoch / 1e3
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        except Exception:
            pass
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return default


def _persist_orderbook_items(items: Sequence[dict]) -> None:
    """
    Persist served orderbook data into the local InfluxDB cache so newly opened
    dashboards can render historic charts immediately.
    """
    writer = _get_orderbook_writer()
    if writer is None:
        return
    default_ts = datetime.now(tz=timezone.utc)
    for entry in items:
        inst_id = (entry.get("instrument_id") or "").upper()
        if not inst_id:
            continue
        bids = entry.get("bids") or []
        asks = entry.get("asks") or []
        if not bids and not asks:
            continue
        timestamp = _coerce_datetime(entry.get("timestamp"), default=default_ts)
        ts_key = timestamp.isoformat()
        if _ORDERBOOK_PERSIST_TRACK.get(inst_id) == ts_key:
            continue
        try:
            writer.write_orderbook(
                instrument_id=inst_id,
                timestamp=timestamp,
                bids=bids,
                asks=asks,
            )
        except Exception as exc:  # pragma: no cover - best effort cache
            logger.debug("Failed to persist orderbook payload for %s: %s", inst_id, exc)
            continue
        _ORDERBOOK_PERSIST_TRACK[inst_id] = ts_key


def _get_orderbook_writer() -> InfluxWriter | None:
    global _ORDERBOOK_WRITER
    if _ORDERBOOK_WRITER is not None:
        return _ORDERBOOK_WRITER
    if InfluxWriter is None:  # pragma: no cover - safety when dependency missing
        return None
    try:
        config = InfluxConfig.from_env()
    except Exception as exc:
        logger.debug("Unable to load Influx configuration for orderbook cache: %s", exc)
        return None
    if not config.token:
        logger.debug("Influx token missing; orderbook cache persistence disabled.")
        return None
    try:
        _ORDERBOOK_WRITER = InfluxWriter(config)
    except Exception as exc:
        logger.debug("Failed to initialise Influx writer for orderbooks: %s", exc)
        return None
    return _ORDERBOOK_WRITER


def _load_recent_ai_signals(limit: int = 10, *, enabled_model_ids: set[str] | None = None) -> list[dict]:
    if not SIGNAL_LOG_PATH.exists():
        return []
    try:
        lines = SIGNAL_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    records: list[dict] = []
    for line in reversed(lines[-limit:]):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
            model_id = (payload.get("model_id") or "").strip()
            if enabled_model_ids is not None and model_id and model_id not in enabled_model_ids:
                continue
            records.append(payload)
        except json.JSONDecodeError:
            continue
    return records


def _persist_model_defaults() -> None:
    """Persist current model registry to config.py for durability."""
    sanitized: Dict[str, dict] = {}
    for model_id, record in _MODEL_REGISTRY.items():
        sanitized[model_id] = {
            key: record.get(key)
            for key in ("display_name", "provider", "enabled", "api_key")
        }
    _replace_config_assignment("MODEL_DEFAULTS", sanitized)
    try:
        import config as config_module  # type: ignore

        config_module.MODEL_DEFAULTS = sanitized  # type: ignore[attr-defined]
    except ImportError:
        pass


def _persist_scheduler_settings(market_interval: int, ai_interval: int, liquidation_interval: int) -> None:
    """Persist scheduler intervals to the JSON settings store."""
    payload = {
        "market_interval": int(market_interval),
        "ai_interval": int(ai_interval),
        "liquidation_interval": int(liquidation_interval),
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    save_settings_namespace("scheduler", payload)


def _persist_pipeline_settings(
    tradable_instruments: List[str],
    poll_interval: int,
    overrides: Dict[str, Dict[str, object]],
) -> None:
    """Persist pipeline tradable instruments and poll interval to the JSON settings store."""
    payload = {
        "tradable_instruments": list(tradable_instruments),
        "poll_interval": int(poll_interval),
        "liquidation_overrides": overrides,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    save_settings_namespace("pipeline", payload)


def _persist_risk_settings(settings: Dict[str, object]) -> None:
    """Persist risk configuration settings to the JSON settings store."""
    payload = dict(settings)
    payload["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
    save_settings_namespace("risk", payload)


def _replace_config_assignment(var_name: str, value: object) -> None:
    """Replace a top-level assignment in config.py with the provided value."""
    if not CONFIG_PATH.exists():
        return
    source = CONFIG_PATH.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return

    assign_node: ast.Assign | None = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    assign_node = node
                    break
        if assign_node is not None:
            break

    if assign_node is None or not hasattr(assign_node, "lineno") or not hasattr(assign_node, "end_lineno"):
        return

    start = assign_node.lineno - 1
    end = assign_node.end_lineno
    lines = source.splitlines()
    if isinstance(value, (dict, list, tuple)):
        formatted_value = pprint.pformat(value, indent=4, sort_dicts=False, width=100)
    else:
        formatted_value = repr(value)
    formatted = f"{var_name} = {formatted_value}"
    replacement_lines = formatted.splitlines()
    lines[start:end] = replacement_lines
    CONFIG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _guess_mime_from_suffix(suffix: str) -> str:
    lookup = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
    }
    return lookup.get(suffix.lower(), "image/png")
