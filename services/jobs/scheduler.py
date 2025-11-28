"""
Background scheduler for analytics refresh and automated signal execution.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections import deque
from datetime import datetime, timezone, timedelta, date, time
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

try:
    from apscheduler.jobstores.base import JobLookupError
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError:  # pragma: no cover
    JobLookupError = None  # type: ignore
    AsyncIOScheduler = None  # type: ignore
    IntervalTrigger = None  # type: ignore

from accounts.models import Account, Balance, Order as OrderModel, Position, Trade
from accounts.repository import AccountRepository
from execution import RoutedOrder, SignalRouter
from exchanges.base_client import ExchangeCredentials
from exchanges.okx.paper import OkxClientError, OkxPaperClient
from models.runtime import SignalRuntime
from models.schemas import MarketSnapshot, RiskContext, SignalRequest, SignalResponse
from risk.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from risk.engine import RiskEngine
from risk.order_validation import BasicOrderValidator, OrderValidationError, ensure_valid_order
from risk.notional_limits import (
    InstrumentExposureGuard,
    OrderNotionalGuard,
    ProfitLossGuard,
)
from risk.price_limits import PriceLimitValidator
from risk.schemas import MarketContext, OrderIntent
from services.okx.catalog import load_catalog_cache
from services.analytics.leaderboard import refresh_leaderboard_cache
from services.okx.transform import (
    extract_balances,
    normalize_orders,
    normalize_positions,
    normalize_trades,
)
from services.okx.brackets import apply_bracket_targets
from services.storage.settings_store import load_namespace as load_settings_namespace
from services.webapp.dependencies import (
    get_account_repository,
    get_portfolio_registry,
)
from services.liquidations import waves as wave_detector
from data_pipeline.pipeline import (
    MarketDataPipeline,
    PipelineSettings,
    SIGNAL_LOG_MAX_LINES,
    _append_signal_log,
)

try:  # pragma: no cover - optional dependency
    from data_pipeline.influx import InfluxConfig, InfluxWriter
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.flux_table import FluxRecord
except ImportError:  # pragma: no cover
    InfluxConfig = None  # type: ignore
    InfluxWriter = None  # type: ignore
    InfluxDBClient = None  # type: ignore
    FluxRecord = None  # type: ignore

logger = logging.getLogger(__name__)

_DEFAULT_LIQUIDATION_INTERVAL = 120
_MIN_LIQUIDATION_NOTIONAL_USD = 10_000.0
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_AUTO_CLOSE_WINDOW_SECONDS = 45 * 60  # allow a 45-minute window to trigger daily flattening
_AUTO_CLOSE_LOCK = Lock()
_LAST_AUTO_CLOSE_SIGNATURE: Optional[tuple[date, str]] = None

try:  # pragma: no cover - config is optional for tests
    from config import (
        AI_INTERACTION_INTERVAL,
        MARKET_SYNC_INTERVAL,
        OKX_ACCOUNTS,
        TRADABLE_INSTRUMENTS,
        LIQUIDATION_INSTRUMENT_OVERRIDES,
    )
    try:
        from config import LIQUIDATION_CHECK_INTERVAL as _LIQ_INTERVAL
    except Exception:
        LIQUIDATION_CHECK_INTERVAL = _DEFAULT_LIQUIDATION_INTERVAL
    else:
        LIQUIDATION_CHECK_INTERVAL = _LIQ_INTERVAL
    try:
        from config import RISK_SETTINGS as _RISK_DEFAULTS  # type: ignore[attr-defined]
    except Exception:
        _RISK_DEFAULTS = {}
    try:
        _LIQUIDATION_OVERRIDE_DEFAULTS = dict(LIQUIDATION_INSTRUMENT_OVERRIDES or {})
    except Exception:
        _LIQUIDATION_OVERRIDE_DEFAULTS = {}
except ImportError:  # pragma: no cover
    OKX_ACCOUNTS = {}
    TRADABLE_INSTRUMENTS: Sequence[str] = ()
    MARKET_SYNC_INTERVAL = 60
    AI_INTERACTION_INTERVAL = 300
    LIQUIDATION_CHECK_INTERVAL = _DEFAULT_LIQUIDATION_INTERVAL
    _RISK_DEFAULTS = {}
    _LIQUIDATION_OVERRIDE_DEFAULTS = {}

_pipeline_store = load_settings_namespace("pipeline")
if _pipeline_store:
    instruments_override_raw = _pipeline_store.get("tradable_instruments") or []
    instruments_override = [
        str(inst).upper()
        for inst in instruments_override_raw
        if str(inst).strip()
    ]
    if instruments_override:
        TRADABLE_INSTRUMENTS = tuple(instruments_override)  # type: ignore[assignment]

_scheduler_store = load_settings_namespace("scheduler")
if _scheduler_store:
    try:
        MARKET_SYNC_INTERVAL = int(_scheduler_store.get("market_interval") or MARKET_SYNC_INTERVAL)
    except (TypeError, ValueError):
        pass
    try:
        AI_INTERACTION_INTERVAL = int(_scheduler_store.get("ai_interval") or AI_INTERACTION_INTERVAL)
    except (TypeError, ValueError):
        pass
    try:
        LIQUIDATION_CHECK_INTERVAL = int(
            _scheduler_store.get("liquidation_interval") or LIQUIDATION_CHECK_INTERVAL
        )
    except (TypeError, ValueError):
        pass

_risk_store = load_settings_namespace("risk")
if _risk_store:
    try:
        _RISK_DEFAULTS = dict(_risk_store)
    except Exception:
        pass

_SCHEDULER: Optional["AsyncIOScheduler"] = None
_FEATURE_CLIENT: Optional["InfluxDBClient"] = None
_FEATURE_CONFIG: Optional["InfluxConfig"] = None
_OKX_INSTRUMENT_MAP: Optional[Dict[str, dict]] = None

_ANALYTICS_JOB_ID = "refresh_leaderboard_and_equity_curve"
_MARKET_JOB_ID = "market_data_sync"
_EXECUTION_JOB_ID = "execute_model_signals"
_LIQUIDATION_JOB_ID = "scan_liquidation_orderflow"


def _sanitize_interval(value: int, minimum: int, maximum: int = 3600) -> int:
    return int(max(minimum, min(maximum, value)))


def _normalize_auto_close_time(value: object) -> str:
    """Normalize HH:MM formatted strings for the Asia/Shanghai auto-close trigger."""
    if value in (None, ""):
        return ""
    text = str(value).strip()
    parts = text.split(":", 1)
    if len(parts) != 2:
        return ""
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (TypeError, ValueError):
        return ""
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return ""
    return f"{hour:02d}:{minute:02d}"


_DEFAULT_RISK_CONFIG = {
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
    "position_take_profit_pct": 5.0,
    "position_stop_loss_pct": 3.0,
    "pyramid_max_orders": 5,
    "pyramid_reentry_pct": 2.0,
    "liquidation_notional_threshold": 50_000.0,
    "liquidation_same_direction_count": 4,
    "liquidation_opposite_count": 3,
    "liquidation_silence_seconds": 300,
    "max_capital_pct_per_instrument": 0.1,
    "auto_close_time_shanghai": "05:50",
}


def _sanitize_risk_config(raw: Optional[Dict[str, Any]]) -> Dict[str, float | int | str]:
    config = dict(_DEFAULT_RISK_CONFIG)
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
            "position_take_profit_pct",
            "position_stop_loss_pct",
            "pyramid_max_orders",
            "pyramid_reentry_pct",
            "liquidation_notional_threshold",
            "liquidation_same_direction_count",
            "liquidation_opposite_count",
            "liquidation_silence_seconds",
            "max_capital_pct_per_instrument",
            "auto_close_time_shanghai",
        ):
            if key in raw:
                config[key] = raw[key]  # type: ignore[assignment]
    price = float(config["price_tolerance_pct"])
    drawdown = float(config["max_drawdown_pct"])
    max_loss = float(config["max_loss_absolute"])
    cooldown = int(config["cooldown_seconds"])
    min_notional = float(config["min_notional_usd"])
    max_position = float(config.get("max_position", 0.0))
    default_leverage = float(config.get("default_leverage", 2.0))
    max_leverage = float(config.get("max_leverage", max(default_leverage, 1.0)))
    max_capital_pct = float(config.get("max_capital_pct_per_instrument", 0.0))
    max_capital_pct = max(0.0, min(1.0, max_capital_pct))
    config["max_capital_pct_per_instrument"] = max_capital_pct
    pyramid_max = int(config.get("pyramid_max_orders", 0))
    take_profit = float(config.get("take_profit_pct", 0.0))
    stop_loss = float(config.get("stop_loss_pct", 0.0))
    position_take_profit = float(config.get("position_take_profit_pct", 5.0))
    position_stop_loss = float(config.get("position_stop_loss_pct", 3.0))
    reentry_pct = float(config.get("pyramid_reentry_pct", 0.0))
    liquidation_notional = float(config.get("liquidation_notional_threshold", 50_000.0))
    same_direction_count = int(config.get("liquidation_same_direction_count", 4))
    opposite_direction_count = int(config.get("liquidation_opposite_count", 3))
    silence_seconds = float(config.get("liquidation_silence_seconds", 300))
    price = max(0.001, min(0.5, price))
    drawdown = max(0.1, min(95.0, drawdown))
    max_loss = max(1.0, max_loss)
    cooldown = max(10, min(86400, cooldown))
    max_order_notional = float(config.get("max_order_notional_usd", 0.0))
    max_order_notional = max(0.0, min(1_000_000.0, max_order_notional))
    min_notional = max(0.0, min_notional)
    max_position = max(0.0, max_position)
    default_leverage = max(1.0, min(125.0, default_leverage))
    max_leverage = max(default_leverage, min(125.0, max_leverage))
    take_profit = max(0.0, min(500.0, take_profit))
    stop_loss = max(0.0, min(95.0, stop_loss))
    position_take_profit = max(0.0, min(500.0, position_take_profit))
    position_stop_loss = max(0.0, min(95.0, position_stop_loss))
    reentry_pct = max(0.0, min(50.0, reentry_pct))
    liquidation_notional = max(0.0, liquidation_notional)
    pyramid_max = max(0, min(100, pyramid_max))
    same_direction_count = max(1, min(20, same_direction_count))
    opposite_direction_count = max(0, min(20, opposite_direction_count))
    silence_seconds = max(0.0, min(300.0, silence_seconds))
    auto_close_label = _normalize_auto_close_time(config.get("auto_close_time_shanghai"))
    return {
        "price_tolerance_pct": price,
        "max_drawdown_pct": drawdown,
        "max_loss_absolute": max_loss,
        "cooldown_seconds": cooldown,
        "min_notional_usd": min_notional,
        "max_order_notional_usd": max_order_notional,
        "max_position": max_position,
        "default_leverage": default_leverage,
        "max_leverage": max_leverage,
        "take_profit_pct": take_profit,
        "stop_loss_pct": stop_loss,
        "position_take_profit_pct": position_take_profit,
        "position_stop_loss_pct": position_stop_loss,
        "pyramid_max_orders": pyramid_max,
        "pyramid_reentry_pct": reentry_pct,
        "liquidation_notional_threshold": liquidation_notional,
        "liquidation_same_direction_count": same_direction_count,
        "liquidation_opposite_count": opposite_direction_count,
        "liquidation_silence_seconds": silence_seconds,
        "auto_close_time_shanghai": auto_close_label,
    }


def _parse_auto_close_label(label: str) -> Optional[time]:
    normalized = _normalize_auto_close_time(label)
    if not normalized:
        return None
    hour_str, minute_str = normalized.split(":")
    try:
        return time(hour=int(hour_str), minute=int(minute_str))
    except (TypeError, ValueError):
        return None


def _should_trigger_auto_close(now_utc: datetime) -> Optional[tuple[date, str]]:
    with _RISK_CONFIG_LOCK:
        label = _normalize_auto_close_time(_RISK_CONFIG.get("auto_close_time_shanghai"))
    if not label:
        return None
    schedule_time = _parse_auto_close_label(label)
    if schedule_time is None:
        return None
    local_now = now_utc.astimezone(SHANGHAI_TZ)
    local_date = local_now.date()
    target_dt = datetime.combine(local_date, schedule_time, tzinfo=SHANGHAI_TZ)
    if local_now < target_dt:
        return None
    if (local_now - target_dt).total_seconds() > _AUTO_CLOSE_WINDOW_SECONDS:
        return None
    with _AUTO_CLOSE_LOCK:
        if _LAST_AUTO_CLOSE_SIGNATURE == (local_date, label):
            return None
    return local_date, label


def _auto_close_all_accounts() -> dict:
    summary = {
        "accounts_considered": 0,
        "accounts_with_positions": 0,
        "positions_closed": 0,
        "errors": [],
    }
    if not OKX_ACCOUNTS:
        return summary
    for key, meta in OKX_ACCOUNTS.items():
        if not meta.get("enabled", True):
            continue
        summary["accounts_considered"] += 1
        closed, had_positions, errors = _auto_close_account_positions(key, meta)
        if had_positions:
            summary["accounts_with_positions"] += 1
        summary["positions_closed"] += closed
        summary["errors"].extend(errors)
    return summary


def _auto_close_account_positions(account_key: str, meta: dict) -> tuple[int, bool, list[str]]:
    account_id = meta.get("account_id") or account_key
    missing = [field for field in ("api_key", "api_secret") if not meta.get(field)]
    if missing:
        return 0, False, [f"{account_id}:缺少 {'/'.join(missing)}"]
    credentials = ExchangeCredentials(
        api_key=meta["api_key"],
        api_secret=meta["api_secret"],
        passphrase=meta.get("passphrase"),
    )
    client = OkxPaperClient()
    closed = 0
    had_positions = False
    errors: list[str] = []
    try:
        client.authenticate(credentials)
        try:
            positions = client.fetch_positions()
        except Exception as exc:
            errors.append(f"{account_id}:获取持仓失败 {exc}")
            return 0, False, errors
        for entry in positions or []:
            inst_id = (entry.get("instId") or entry.get("instrument_id") or "").upper()
            if not inst_id:
                continue
            qty = _try_float(entry.get("pos") or entry.get("availPos") or entry.get("posCcy"))
            if qty is None or abs(qty) <= 0:
                continue
            had_positions = True
            pos_side_payload = _resolve_auto_close_pos_side(entry, qty)
            margin_mode = _resolve_auto_close_margin_mode(inst_id, entry, meta)
            currency = _resolve_auto_close_currency(entry, inst_id)
            try:
                client.close_position(
                    instrument_id=inst_id,
                    margin_mode=margin_mode,
                    pos_side=pos_side_payload,
                    ccy=currency if margin_mode == "cross" else None,
                )
                closed += 1
            except OkxClientError as exc:
                errors.append(f"{account_id}:{inst_id}:{exc}")
    except OkxClientError as exc:
        errors.append(f"{account_id}:认证失败 {exc}")
    except Exception as exc:
        errors.append(f"{account_id}:自动平仓异常 {exc}")
    finally:
        try:
            client.close()
        except Exception:
            pass
    return closed, had_positions, errors


def _resolve_auto_close_margin_mode(inst_id: str, position_entry: dict, meta: dict) -> str:
    raw_mode = (
        (position_entry.get("mgnMode") or position_entry.get("marginMode") or meta.get("margin_mode") or "")
        .strip()
        .lower()
    )
    if raw_mode in {"cash", "cross", "isolated"}:
        return raw_mode
    symbol = (inst_id or "").upper()
    if symbol.endswith("-SWAP") or "FUTURE" in symbol:
        return "cross"
    return "cash"


def _resolve_auto_close_currency(entry: dict, inst_id: str) -> Optional[str]:
    currency = entry.get("ccy") or entry.get("marginCurrency") or entry.get("posCcy")
    if currency:
        return str(currency).upper()
    meta = _get_okx_instrument_meta(inst_id) or {}
    for key in ("settleCurrency", "ctValCcy", "quoteCcy"):
        currency = meta.get(key)
        if currency:
            return str(currency).upper()
    parts = (inst_id or "").split("-")
    if len(parts) >= 2:
        return parts[1].upper()
    return None


def _resolve_auto_close_pos_side(entry: dict, qty: float) -> Optional[str]:
    side = (entry.get("posSide") or entry.get("side") or "").strip().lower()
    if side in {"long", "short"}:
        return side
    if side == "net":
        return None
    if qty > 0:
        return "long"
    if qty < 0:
        return "short"
    return None




_MARKET_INTERVAL = _sanitize_interval(int(MARKET_SYNC_INTERVAL), 30)
_AI_INTERVAL = _sanitize_interval(int(AI_INTERACTION_INTERVAL), 60)
_LIQUIDATION_INTERVAL = _sanitize_interval(int(LIQUIDATION_CHECK_INTERVAL), 30)
AI_SIGNAL_LOG_PATH = Path("data/logs/ai_signals.jsonl")
AI_SIGNAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_PIPELINE_SETTINGS = PipelineSettings(
    instruments=list(TRADABLE_INSTRUMENTS) if TRADABLE_INSTRUMENTS else ["BTC-USDT-SWAP"],
    signal_log_path=AI_SIGNAL_LOG_PATH,
)
_LIQUIDATION_OVERRIDE_LOCK = Lock()
_LIQUIDATION_OVERRIDES: Dict[str, Dict[str, float | int]] = {}


def _sanitize_liquidation_override_map(
    raw: Optional[Dict[str, Dict[str, object]]]
) -> Dict[str, Dict[str, float | int]]:
    sanitized: Dict[str, Dict[str, float | int]] = {}
    if not isinstance(raw, dict):
        return sanitized

    def _parse_int(value: object, minimum: Optional[int] = None, maximum: Optional[int] = None) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            number = int(float(value))
        except (TypeError, ValueError):
            return None
        if minimum is not None:
            number = max(minimum, number)
        if maximum is not None:
            number = min(maximum, number)
        return number

    def _parse_float(value: object, minimum: Optional[float] = None, maximum: Optional[float] = None) -> Optional[float]:
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

    for key, payload in raw.items():
        symbol = str(key or "").strip().upper()
        if not symbol:
            continue
        entry_raw = payload or {}
        entry: Dict[str, float | int] = {}
        same_val = _parse_int(
            entry_raw.get("same_direction") or entry_raw.get("same_direction_count"),
            minimum=1,
            maximum=50,
        )
        opp_val = _parse_int(
            entry_raw.get("opposite_direction") or entry_raw.get("opposite_direction_count"),
            minimum=0,
            maximum=50,
        )
        notional_val = _parse_float(
            entry_raw.get("notional_threshold") or entry_raw.get("liquidation_notional_threshold"),
            minimum=0.0,
        )
        silence_val = _parse_float(
            entry_raw.get("silence_seconds") or entry_raw.get("liquidation_silence_seconds"),
            minimum=0.0,
            maximum=3600.0,
        )
        if same_val is not None:
            entry["same_direction"] = same_val
        if opp_val is not None:
            entry["opposite_direction"] = opp_val
        if notional_val is not None:
            entry["notional_threshold"] = notional_val
        if silence_val is not None:
            entry["silence_seconds"] = silence_val
        if entry:
            sanitized[symbol] = entry
    return sanitized


with _LIQUIDATION_OVERRIDE_LOCK:
    _LIQUIDATION_OVERRIDES = _sanitize_liquidation_override_map(_LIQUIDATION_OVERRIDE_DEFAULTS)
_PIPELINE: Optional[MarketDataPipeline] = None
_RISK_CONFIG_LOCK = Lock()
_RISK_CONFIG = _sanitize_risk_config(_RISK_DEFAULTS)
_EXECUTION_LOG = deque(maxlen=100)
_ORDER_DEBUG_LOG = deque(maxlen=50)
_ORDER_DEBUG_WRITER: Optional["InfluxWriter"] = None
_EXEC_LOG_WRITER: Optional["InfluxWriter"] = None
_WAVE_AUTOMATION_CURSOR: Dict[str, Dict[Tuple[str, str], int]] = {}

_LIQUIDATION_REVERSAL_WATCH: Dict[str, Dict[str, object]] = {}
_LIQUIDATION_MIN_EVENT_AGE_SECONDS = 0.0
_LIQUIDATION_MAX_EVENT_LOOKBACK = 15 * 60
_LIQUIDATION_MIN_POSITION_HOLD_SECONDS = 15 * 60


def _get_pipeline() -> MarketDataPipeline:
    global _PIPELINE
    if _PIPELINE is None:
        _PIPELINE = MarketDataPipeline(_PIPELINE_SETTINGS)
    return _PIPELINE


def _close_pipeline() -> None:
    global _PIPELINE
    if _PIPELINE is None:
        return
    pipeline = _PIPELINE
    _PIPELINE = None
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(pipeline.aclose())
        else:  # pragma: no cover - executed mainly in CLI contexts
            loop.run_until_complete(pipeline.aclose())
    except RuntimeError:
        asyncio.run(pipeline.aclose())


def _append_execution_log(
    job: str,
    status: str,
    detail: str | None = None,
    duration_seconds: float | None = None,
) -> None:
    now = datetime.now(tz=timezone.utc)
    detail_text = "" if detail is None else str(detail)
    entry = {
        "timestamp": now.isoformat(),
        "job": job,
        "status": status,
        "detail": detail_text,
        "duration_seconds": duration_seconds,
    }
    _EXECUTION_LOG.appendleft(entry)
    writer = _get_execution_log_writer()
    if writer is not None:
        try:
            timestamp_ns = int(now.timestamp() * 1e9)
            writer.write_indicator_set(
                measurement="scheduler_execution_log",
                tags={"job": job},
                fields={
                    "status": status,
                    "detail": detail_text,
                    "duration_seconds": float(duration_seconds) if duration_seconds is not None else None,
                },
                timestamp_ns=timestamp_ns,
            )
        except Exception:
            pass


def get_risk_configuration() -> Dict[str, float | int | str]:
    """Return a snapshot of the current risk configuration."""
    with _RISK_CONFIG_LOCK:
        return dict(_RISK_CONFIG)


def update_risk_configuration(
    *,
    price_tolerance_pct: float,
    max_drawdown_pct: float,
    max_loss_absolute: float,
    cooldown_seconds: int,
    min_notional_usd: float,
    max_order_notional_usd: float = 0.0,
    max_position: float = 0.0,
    take_profit_pct: float = 0.0,
    stop_loss_pct: float = 0.0,
    position_take_profit_pct: float = 5.0,
    position_stop_loss_pct: float = 3.0,
    pyramid_max_orders: int = 0,
    pyramid_reentry_pct: float = 0.0,
    liquidation_notional_threshold: float = 50_000.0,
    liquidation_same_direction_count: int = 4,
    liquidation_opposite_count: int = 3,
    liquidation_silence_seconds: float = 300.0,
    max_capital_pct_per_instrument: float = 0.1,
    auto_close_time_shanghai: str = "",
) -> None:
    global _LAST_AUTO_CLOSE_SIGNATURE
    """Refresh the in-memory risk configuration."""
    normalized = _sanitize_risk_config(
        {
            "price_tolerance_pct": price_tolerance_pct,
            "max_drawdown_pct": max_drawdown_pct,
            "max_loss_absolute": max_loss_absolute,
            "cooldown_seconds": cooldown_seconds,
            "min_notional_usd": min_notional_usd,
            "max_order_notional_usd": max_order_notional_usd,
            "max_position": max_position,
            "take_profit_pct": take_profit_pct,
            "stop_loss_pct": stop_loss_pct,
            "position_take_profit_pct": position_take_profit_pct,
            "position_stop_loss_pct": position_stop_loss_pct,
            "pyramid_max_orders": pyramid_max_orders,
            "pyramid_reentry_pct": pyramid_reentry_pct,
            "liquidation_notional_threshold": liquidation_notional_threshold,
            "liquidation_same_direction_count": liquidation_same_direction_count,
            "liquidation_opposite_count": liquidation_opposite_count,
            "liquidation_silence_seconds": liquidation_silence_seconds,
            "max_capital_pct_per_instrument": max_capital_pct_per_instrument,
            "auto_close_time_shanghai": auto_close_time_shanghai,
        }
    )
    with _RISK_CONFIG_LOCK:
        previous_label = _normalize_auto_close_time(_RISK_CONFIG.get("auto_close_time_shanghai"))
        _RISK_CONFIG.update(normalized)
    if normalized.get("auto_close_time_shanghai") != previous_label:
        with _AUTO_CLOSE_LOCK:
            _LAST_AUTO_CLOSE_SIGNATURE = None


def _build_risk_engine() -> RiskEngine:
    with _RISK_CONFIG_LOCK:
        config = dict(_RISK_CONFIG)
    price_validator = PriceLimitValidator(default_tolerance_pct=config["price_tolerance_pct"])
    circuit_breaker = CircuitBreaker(
        CircuitBreakerConfig(
            max_drawdown_pct=config["max_drawdown_pct"],
            max_loss_absolute=config["max_loss_absolute"],
            cooldown_seconds=int(config["cooldown_seconds"]),
        )
    )
    notional_guard = OrderNotionalGuard(
        min_notional=float(config.get("min_notional_usd", 0.0)),
        max_notional=float(config.get("max_order_notional_usd", 0.0)),
    )
    exposure_guard = InstrumentExposureGuard(
        max_capital_pct=float(config.get("max_capital_pct_per_instrument", 0.0)),
    )
    pnl_guard = ProfitLossGuard(
        take_profit_pct=float(config.get("take_profit_pct", 0.0)),
        stop_loss_pct=float(config.get("stop_loss_pct", 0.0)),
    )
    return RiskEngine(
        price_validator=price_validator,
        circuit_breaker=circuit_breaker,
        notional_guard=notional_guard,
        pnl_guard=pnl_guard,
        exposure_guard=exposure_guard,
    )


def _prime_price_band(risk_engine: RiskEngine, market: Optional[MarketContext]) -> None:
    if market is None:
        return
    with _RISK_CONFIG_LOCK:
        tolerance = float(_RISK_CONFIG["price_tolerance_pct"])
    try:
        risk_engine.update_market(market, tolerance_pct=tolerance)
    except Exception as exc:
        logger.debug("Failed to update price band for %s: %s", market.instrument_id, exc)


def get_execution_log(limit: int = 20) -> List[dict]:
    """Return a snapshot of recent scheduler job executions."""
    return list(_EXECUTION_LOG)[:limit]


def get_order_debug_log(limit: int = 20) -> List[dict]:
    """Return recent order-stage debug entries."""
    entries = list(_ORDER_DEBUG_LOG)[:limit]
    if len(entries) < limit:
        entries.extend(_hydrate_order_debug_from_influx(limit - len(entries)))
    return entries


def _record_order_debug_entry(entry: Dict[str, Any]) -> None:
    payload = dict(entry)
    payload.setdefault("timestamp", datetime.now(tz=timezone.utc).isoformat())
    _ORDER_DEBUG_LOG.appendleft(payload)
    writer = _get_order_debug_writer()
    if writer:
        try:
            timestamp_ns = int(datetime.fromisoformat(payload["timestamp"]).timestamp() * 1e9)
        except Exception:
            timestamp_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
        try:
            writer.write_indicator_set(
                measurement="order_debug_log",
                tags={
                    "account_id": str(payload.get("account_id") or payload.get("account_key") or "-"),
                    "model_id": str(payload.get("model_id") or "-"),
                },
                fields={
                    "signal_generated": int(bool(payload.get("signal_generated"))),
                    "decision_actionable": int(bool(payload.get("decision_actionable"))),
                    "order_ready": int(bool(payload.get("order_ready"))),
                    "risk_passed": int(bool(payload.get("risk_passed"))),
                    "submitted": int(bool(payload.get("submitted"))),
                    "notes": str(payload.get("notes") or ""),
                    "instrument": str(payload.get("instrument") or ""),
                    "decision": str(payload.get("decision") or ""),
                    "confidence": float(payload.get("confidence") or 0.0),
                    "fallback_used": int(bool(payload.get("fallback_used"))),
                },
                timestamp_ns=timestamp_ns,
            )
        except Exception:
            logger.debug("Failed to persist order debug entry to Influx.", exc_info=True)


def _hydrate_order_debug_from_influx(limit: int) -> List[dict]:
    if limit <= 0 or InfluxDBClient is None or InfluxConfig is None:
        return []
    try:
        cfg = InfluxConfig.from_env()
    except Exception:
        return []
    if not getattr(cfg, "token", None):
        return []
    flux = f"""
from(bucket: "{cfg.bucket}")
  |> range(start: -7d)
  |> filter(fn: (r) => r["_measurement"] == "order_debug_log")
  |> pivot(rowKey:["_time","account_id","model_id"], columnKey:["_field"], valueColumn:"_value")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit})
"""
    results: list[dict] = []
    try:
        with InfluxDBClient(url=cfg.url, token=cfg.token, org=cfg.org) as client:
            tables = client.query_api().query(flux)
    except Exception:
        return []
    for table in tables:
        for record in getattr(table, "records", []):
            values = getattr(record, "values", {})
            entry = {
                "timestamp": str(values.get("_time") or ""),
                "account_id": values.get("account_id"),
                "model_id": values.get("model_id"),
                "instrument": values.get("instrument"),
                "decision": values.get("decision"),
                "confidence": values.get("confidence"),
                "signal_generated": bool(values.get("signal_generated")),
                "decision_actionable": bool(values.get("decision_actionable")),
                "order_ready": bool(values.get("order_ready")),
                "risk_passed": bool(values.get("risk_passed")),
                "submitted": bool(values.get("submitted")),
                "fallback_used": bool(values.get("fallback_used")),
                "notes": values.get("notes"),
            }
            results.append(entry)
    return results


def _get_execution_log_writer() -> Optional["InfluxWriter"]:
    global _EXEC_LOG_WRITER
    if InfluxWriter is None:
        return None
    if _EXEC_LOG_WRITER is not None:
        return _EXEC_LOG_WRITER
    try:
        cfg = InfluxConfig.from_env()
    except Exception:
        return None
    if not getattr(cfg, "token", None):
        return None
    try:
        _EXEC_LOG_WRITER = InfluxWriter(cfg)
    except Exception:
        _EXEC_LOG_WRITER = None
    return _EXEC_LOG_WRITER


def _get_order_debug_writer() -> Optional["InfluxWriter"]:
    global _ORDER_DEBUG_WRITER
    if InfluxWriter is None:
        return None
    if _ORDER_DEBUG_WRITER is not None:
        return _ORDER_DEBUG_WRITER
    try:
        cfg = InfluxConfig.from_env()
    except Exception:
        return None
    if not getattr(cfg, "token", None):
        return None
    try:
        _ORDER_DEBUG_WRITER = InfluxWriter(cfg)
    except Exception:
        _ORDER_DEBUG_WRITER = None
    return _ORDER_DEBUG_WRITER


def _hydrate_execution_log_from_influx(limit: int = 20) -> None:
    if InfluxDBClient is None or InfluxConfig is None:
        return
    def _coerce_float(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    try:
        cfg = InfluxConfig.from_env()
    except Exception:
        return
    if not getattr(cfg, "token", None):
        return
    flux = f"""
from(bucket: "{cfg.bucket}")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "scheduler_execution_log")
  |> pivot(rowKey:["_time","job"], columnKey:["_field"], valueColumn:"_value")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit})
"""
    try:
        with InfluxDBClient(url=cfg.url, token=cfg.token, org=cfg.org) as client:
            tables = client.query_api().query(flux)
    except Exception:
        return
    entries: list[dict] = []
    for table in tables:
        for record in getattr(table, "records", []):
            values = getattr(record, "values", {})
            ts = values.get("_time")
            timestamp = (
                ts.isoformat()
                if isinstance(ts, datetime)
                else str(ts)
            )
            entries.append(
                {
                    "timestamp": timestamp,
                    "job": values.get("job", ""),
                    "status": values.get("status", ""),
                    "detail": values.get("detail", ""),
                    "duration_seconds": _coerce_float(values.get("duration_seconds")),
                }
            )
    if not entries:
        return
    entries.sort(key=lambda item: item.get("timestamp"), reverse=True)
    _EXECUTION_LOG.clear()
    _EXECUTION_LOG.extend(entries)


_hydrate_execution_log_from_influx()

def refresh_pipeline(instruments: Sequence[str]) -> None:
    """
    Re-create the shared MarketDataPipeline with a new instrument list.
    """
    global _PIPELINE_SETTINGS
    normalized = [inst.strip().upper() for inst in instruments if inst and inst.strip()]
    if not normalized:
        normalized = ["BTC-USDT-SWAP"]
    _PIPELINE_SETTINGS = PipelineSettings(
        instruments=normalized,
        signal_log_path=_PIPELINE_SETTINGS.signal_log_path,
        max_position=_PIPELINE_SETTINGS.max_position,
        current_position=_PIPELINE_SETTINGS.current_position,
        cash_available=_PIPELINE_SETTINGS.cash_available,
    )
    _close_pipeline()


def update_liquidation_overrides(overrides: Dict[str, Dict[str, object]]) -> None:
    """Update per-instrument liquidation thresholds."""
    sanitized = _sanitize_liquidation_override_map(overrides)
    with _LIQUIDATION_OVERRIDE_LOCK:
        global _LIQUIDATION_OVERRIDES
        _LIQUIDATION_OVERRIDES = sanitized


def start_scheduler() -> None:
    """Start the APScheduler background jobs if the dependency is available."""
    global _SCHEDULER
    if AsyncIOScheduler is None:
        logger.warning("APScheduler not installed; background jobs disabled.")
        return
    if _SCHEDULER is not None:
        return

    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.add_job(
        _refresh_metrics_job,
        trigger=IntervalTrigger(seconds=60),
        id=_ANALYTICS_JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        _market_data_ingestion_job,
        trigger=IntervalTrigger(seconds=_MARKET_INTERVAL),
        id=_MARKET_JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        _execute_model_workflow_job,
        trigger=IntervalTrigger(seconds=_AI_INTERVAL),
        id=_EXECUTION_JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        _scan_liquidation_flow_job,
        trigger=IntervalTrigger(seconds=_LIQUIDATION_INTERVAL),
        id=_LIQUIDATION_JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    _SCHEDULER = scheduler
    logger.info(
        "Background scheduler started with analytics, market, execution, and liquidation jobs."
    )


def update_task_intervals(
    *,
    market_interval: Optional[int] = None,
    ai_interval: Optional[int] = None,
    liquidation_interval: Optional[int] = None,
) -> None:
    """Update scheduled job intervals at runtime."""
    global _MARKET_INTERVAL, _AI_INTERVAL, _LIQUIDATION_INTERVAL
    scheduler = _SCHEDULER
    if scheduler is None or IntervalTrigger is None:
        if market_interval is not None:
            _MARKET_INTERVAL = _sanitize_interval(market_interval, 30)
        if ai_interval is not None:
            _AI_INTERVAL = _sanitize_interval(ai_interval, 60)
        if liquidation_interval is not None:
            _LIQUIDATION_INTERVAL = _sanitize_interval(liquidation_interval, 30)
        return

    if market_interval is not None:
        _MARKET_INTERVAL = _sanitize_interval(market_interval, 30)
        trigger = IntervalTrigger(seconds=_MARKET_INTERVAL)
        try:
            scheduler.reschedule_job(_MARKET_JOB_ID, trigger=trigger)
        except Exception:
            scheduler.add_job(
                _market_data_ingestion_job,
                trigger=trigger,
                id=_MARKET_JOB_ID,
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        logger.info("Updated market data sync interval to %ds", _MARKET_INTERVAL)

    if ai_interval is not None:
        _AI_INTERVAL = _sanitize_interval(ai_interval, 60)
        trigger = IntervalTrigger(seconds=_AI_INTERVAL)
        try:
            scheduler.reschedule_job(_EXECUTION_JOB_ID, trigger=trigger)
        except Exception:
            scheduler.add_job(
                _execute_model_workflow_job,
                trigger=trigger,
                id=_EXECUTION_JOB_ID,
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        logger.info("Updated AI execution interval to %ds", _AI_INTERVAL)

    if liquidation_interval is not None:
        _LIQUIDATION_INTERVAL = _sanitize_interval(liquidation_interval, 30)
        trigger = IntervalTrigger(seconds=_LIQUIDATION_INTERVAL)
        try:
            scheduler.reschedule_job(_LIQUIDATION_JOB_ID, trigger=trigger)
        except Exception:
            scheduler.add_job(
                _scan_liquidation_flow_job,
                trigger=trigger,
                id=_LIQUIDATION_JOB_ID,
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        logger.info("Updated liquidation scan interval to %ds", _LIQUIDATION_INTERVAL)


async def run_market_job_once() -> Dict[str, str]:
    """Manually trigger the market data ingestion job."""
    return await _market_data_ingestion_job()


async def run_ai_job_once() -> Dict[str, str]:
    """Manually trigger the AI execution workflow job."""
    return await _execute_model_workflow_job()


async def run_liquidation_job_once() -> Dict[str, str]:
    """Manually trigger the liquidation order flow scan job."""
    return await _scan_liquidation_flow_job()

def shutdown_scheduler() -> None:
    """Stop the scheduler when the application shuts down."""
    global _SCHEDULER, _FEATURE_CLIENT, _FEATURE_CONFIG, _EXEC_LOG_WRITER
    if _SCHEDULER is not None:
        _SCHEDULER.shutdown(wait=False)
        _SCHEDULER = None
    _close_pipeline()
    if _FEATURE_CLIENT is not None:
        try:
            _FEATURE_CLIENT.close()
        except Exception:  # pragma: no cover - defensive
            pass
        _FEATURE_CLIENT = None
    _FEATURE_CONFIG = None
    if _EXEC_LOG_WRITER is not None:
        try:
            _EXEC_LOG_WRITER.close()
        except Exception:
            pass
        _EXEC_LOG_WRITER = None


def _refresh_metrics_job() -> None:
    """Refresh leaderboard cache and append equity points for each account."""
    repository = get_account_repository()
    _refresh_leaderboard(repository)
    _record_equity_points(repository)


async def _market_data_ingestion_job() -> Dict[str, str]:
    if not TRADABLE_INSTRUMENTS:
        logger.debug("No tradable instruments configured; skipping market ingestion job.")
        detail = "未配置可交易合约"
        _append_execution_log("market", "skipped", detail)
        return {"status": "skipped", "detail": detail}
    pipeline = _get_pipeline()
    started = datetime.now(tz=timezone.utc)
    try:
        result = await pipeline.run_cycle(
            log=lambda message: logger.info("Market pipeline: %s", message),
        )
        logger.info(
            "Market pipeline completed (%d instruments, %d signals).",
            len(result.feature_snapshots),
            len(result.signals),
        )
        detail = f"{len(result.feature_snapshots)} 条币对，{len(result.signals)} 条信号"
        duration = (datetime.now(tz=timezone.utc) - started).total_seconds()
        _append_execution_log("market", "success", detail, duration_seconds=duration)
        return {"status": "success", "detail": detail}
    except Exception as exc:
        logger.warning("Market ingestion job failed: %s", exc)
        message = str(exc)
        duration = (datetime.now(tz=timezone.utc) - started).total_seconds()
        _append_execution_log("market", "error", message, duration_seconds=duration)
        return {"status": "error", "detail": message}




async def _scan_liquidation_flow_job() -> Dict[str, str]:
    '''Scan recent liquidation flow features from InfluxDB.'''
    instruments = [
        inst.strip().upper()
        for inst in (TRADABLE_INSTRUMENTS or [])
        if inst and inst.strip()
    ]
    if not instruments:
        detail = "未配置可巡检的合约"
        _append_execution_log("liquidation", "skipped", detail)
        return {"status": "skipped", "detail": detail}

    query_api, config = _get_feature_query_api()
    if query_api is None or config is None:
        detail = "爆仓订单流数据源未配置"
        _append_execution_log("liquidation", "skipped", detail)
        return {"status": "skipped", "detail": detail}

    started = datetime.now(tz=timezone.utc)
    try:
        highlights: list[str] = []
        records: list[tuple[str, float, str, Optional[float]]] = []
        now = datetime.now(tz=timezone.utc)
        for instrument in instruments:
            features = _load_market_features(instrument)
            net_qty = features.get("liq_net_qty")
            if not isinstance(net_qty, (int, float)) or net_qty == 0:
                continue
            liq_notional = _try_float(features.get("liq_notional"))
            if liq_notional is None:
                price_hint = _try_float(features.get("liq_last_price")) or _try_float(features.get("last_price"))
                if price_hint is not None:
                    liq_notional = abs(float(net_qty)) * price_hint
            if liq_notional is None or liq_notional < _MIN_LIQUIDATION_NOTIONAL_USD:
                continue
            ts_value = features.get("liq_timestamp")
            ts = None
            if isinstance(ts_value, (int, float)):
                ts = _coerce_timestamp(ts_value)
            age_label = ""
            if isinstance(ts, datetime):
                age = max(0, int((now - ts).total_seconds()))
                age_label = f"@{age}s"
            highlights.append(f"{instrument}:{net_qty:+.2f}{age_label}")
            records.append((instrument, float(net_qty), age_label, liq_notional))
        if highlights:
            preview = " | ".join(highlights[:4])
            if len(highlights) > 4:
                preview += " | ..."
            suggestions = []
            for inst, qty, label, notional_value in records[:3]:
                direction_hint = "建议逢低加多" if qty > 0 else "建议逢高做空"
                notional_hint = f"，约 {notional_value:,.0f} USDT" if notional_value else ""
                suggestions.append(f"{direction_hint} {inst}，净爆仓 {qty:+.2f}{label}{notional_hint}")
            advice = " | ".join(suggestions)
            detail = (
                f"扫描 {len(instruments)} 个合约，检测到 {len(highlights)} 条爆仓簇：{preview}"
                f"{' | ' + advice if advice else ''}"
            )
        else:
            detail = f"扫描 {len(instruments)} 个合约，暂无新的爆仓信号。"
        duration = (datetime.now(tz=timezone.utc) - started).total_seconds()
        _append_execution_log("liquidation", "success", detail, duration_seconds=duration)
        return {"status": "success", "detail": detail}
    except Exception as exc:
        duration = (datetime.now(tz=timezone.utc) - started).total_seconds()
        message = str(exc)
        logger.warning("Liquidation scan job failed: %s", exc)
        _append_execution_log("liquidation", "error", message, duration_seconds=duration)
        return {"status": "error", "detail": message}


def _wave_watchlist() -> List[str]:
    return [
        inst.strip().upper()
        for inst in (TRADABLE_INSTRUMENTS or [])
        if isinstance(inst, str) and inst.strip()
    ]


def _refresh_wave_signals_sync(instruments: Sequence[str], *, lookback: str = "120m", limit: int = 120) -> None:
    if not instruments:
        return
    query_api, cfg = _get_feature_query_api()
    if query_api is None or cfg is None:
        return
    for instrument in instruments:
        symbol = instrument.strip().upper()
        if not symbol:
            continue
        flux = f"""
from(bucket: "{cfg.bucket}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r["_measurement"] == "okx_liquidations")
  |> filter(fn: (r) => r["instrument_id"] == "{symbol}")
  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: {limit})
"""
        try:
            tables = query_api.query(flux)
        except Exception as exc:
            logger.debug("Wave detector query failed for %s: %s", symbol, exc)
            wave_detector.update_wave(symbol, [])
            continue
        rows: List[dict] = []
        for table in tables:
            for record in getattr(table, "records", []):
                values = getattr(record, "values", {})
                timestamp_value = values.get("_time")
                if isinstance(timestamp_value, datetime):
                    ts = timestamp_value.isoformat()
                else:
                    ts = str(timestamp_value)
                rows.append(
                    {
                        "instrument_id": symbol,
                        "timestamp": ts,
                        "long_qty": values.get("long_qty"),
                        "short_qty": values.get("short_qty"),
                        "net_qty": values.get("net_qty"),
                        "last_price": values.get("last_price"),
                        "notional_value": values.get("notional_value"),
                    }
                )
        wave_detector.update_wave(symbol, rows)


def _passes_wave_reentry(
    *,
    event_price: Optional[float],
    event_direction: str,
    event_instrument: str,
    positions: Sequence[Position],
    reentry_pct: float,
) -> bool:
    if reentry_pct <= 0 or event_price is None or event_price <= 0:
        return True
    for pos in positions or []:
        if _get_attr(pos, "instrument_id") != event_instrument:
            continue
        entry_price = _try_float(_get_attr(pos, "entry_price"))
        if not entry_price or entry_price <= 0:
            continue
        side = (_get_attr(pos, "side") or "").lower()
        if event_direction == "buy" and side in {"long", "buy"}:
            deviation = ((entry_price - event_price) / entry_price) * 100.0
            if deviation < reentry_pct:
                return False
        elif event_direction == "sell" and side in {"short", "sell"}:
            deviation = ((event_price - entry_price) / entry_price) * 100.0
            if deviation < reentry_pct:
                return False
    return True


async def _maybe_execute_wave_strategy(
    *,
    account: Account,
    positions: Sequence[Position],
    open_orders: Sequence[OrderModel],
    meta: Dict[str, str],
    market_price: float,
) -> Optional[Tuple[SignalRequest, SignalResponse]]:
    watchlist = _wave_watchlist()
    if not watchlist:
        return None
    relevant = set(inst.upper() for inst in _iter_liquidation_watch_instruments(meta))
    if not relevant:
        return None
    events = wave_detector.iter_events(signal_codes=("bottom_absorb", "top_signal", "short_reversal"))
    candidates = [event for event in events if event.instrument in relevant]
    if not candidates:
        return None
    with _RISK_CONFIG_LOCK:
        reentry_pct = float(_RISK_CONFIG.get("pyramid_reentry_pct", 0.0))
        pyramid_cap = int(_RISK_CONFIG.get("pyramid_max_orders", 0))
    cursor = _WAVE_AUTOMATION_CURSOR.setdefault(account.account_id, {})
    candidates.sort(key=lambda evt: evt.detected_at or datetime.min)
    for event in candidates:
        if event.event_id is None:
            continue
        key = (event.instrument, event.signal_code)
        if cursor.get(key, 0) >= event.event_id:
            continue
        direction = event.direction
        if direction not in {"buy", "sell"}:
            cursor[key] = event.event_id
            continue
        if pyramid_cap > 0 and _count_active_orders(open_orders, event.instrument, direction) >= pyramid_cap:
            cursor[key] = event.event_id
            continue
        if not _passes_wave_reentry(
            event_price=event.price,
            event_direction=direction,
            event_instrument=event.instrument,
            positions=positions,
            reentry_pct=reentry_pct,
        ):
            cursor[key] = event.event_id
            continue

        request = _build_signal_request(
            model_id=account.model_id,
            meta=meta,
            account=account,
            positions=positions,
            market_price=market_price,
            instrument_override=event.instrument,
        )
        if request is None:
            cursor[key] = event.event_id
            continue
        side = "long" if direction == "buy" else "short"
        size = _estimate_reversal_order_size(request, side)
        if size <= 0:
            cursor[key] = event.event_id
            continue
        suggested_order = {
            "instrument_id": event.instrument,
            "side": direction,
            "order_type": "market",
            "size": round(size, 6),
            "reference_price": event.price or request.market.price,
        }
        reasoning = (
            f"{event.instrument} {event.signal_text} (LE={event.metrics.le or '-'}, "
            f"PC={event.metrics.pc or '-'}) -> 执行{'买入' if direction == 'buy' else '卖出'} {size}."
        )
        response = SignalResponse(
            model_id=request.model_id,
            decision="open_long" if direction == "buy" else "open_short",
            confidence=0.78,
            reasoning=reasoning,
            suggested_order=suggested_order,
            raw_output={
                "automation": "liquidation_wave",
                "wave_signal_code": event.signal_code,
                "wave_event_id": event.event_id,
                "wave_detected_at": event.detected_at.isoformat(),
            },
        )
        cursor[key] = event.event_id
        return request, response
    return None


async def _maybe_run_auto_close(now_utc: datetime) -> None:
    global _LAST_AUTO_CLOSE_SIGNATURE
    trigger = _should_trigger_auto_close(now_utc)
    if not trigger:
        return
    local_date, label = trigger
    if not OKX_ACCOUNTS:
        detail = f"{label} 自动平仓：未配置 OKX 账户"
        _append_execution_log("auto_close", "skipped", detail)
        with _AUTO_CLOSE_LOCK:
            _LAST_AUTO_CLOSE_SIGNATURE = (local_date, label)
        return
    started = datetime.now(tz=timezone.utc)
    try:
        summary = await asyncio.to_thread(_auto_close_all_accounts)
    except Exception as exc:
        detail = f"{label} 自动平仓失败: {exc}"
        _append_execution_log("auto_close", "error", detail)
        with _AUTO_CLOSE_LOCK:
            _LAST_AUTO_CLOSE_SIGNATURE = (local_date, label)
        return
    duration = (datetime.now(tz=timezone.utc) - started).total_seconds()
    accounts_considered = int(summary.get("accounts_considered", 0))
    positions_closed = int(summary.get("positions_closed", 0))
    accounts_with_positions = int(summary.get("accounts_with_positions", 0))
    errors = summary.get("errors") or []
    status = "success"
    if accounts_considered == 0:
        status = "skipped"
        detail = f"{label} 自动平仓：未配置可用 OKX 账户"
    else:
        detail = (
            f"{label} 自动平仓：检测 {accounts_considered} 个账户，"
            f"{accounts_with_positions} 个账户存在持仓"
        )
        if positions_closed:
            detail += f"，触发 {positions_closed} 次 close-position"
        else:
            detail += "，无可平仓仓位"
        if errors:
            status = "warning"
            preview = " | ".join(str(err) for err in errors[:3])
            if len(errors) > 3:
                preview += " | ..."
            detail += f"；异常：{preview}"
        elif accounts_with_positions == 0:
            status = "skipped"
    _append_execution_log("auto_close", status, detail, duration_seconds=duration)
    with _AUTO_CLOSE_LOCK:
        _LAST_AUTO_CLOSE_SIGNATURE = (local_date, label)


async def _execute_model_workflow_job() -> Dict[str, str]:
    """
    Drive the signal �� routing �� risk �� execution loop for configured accounts.
    """
    now_utc = datetime.now(tz=timezone.utc)
    await _maybe_run_auto_close(now_utc)
    if not OKX_ACCOUNTS:
        logger.debug("No OKX accounts configured; skipping execution job.")
        detail = "未配置 OKX 账户"
        _append_execution_log("ai", "skipped", detail)
        return {"status": "skipped", "detail": detail}

    repository = get_account_repository()
    registry = get_portfolio_registry()
    router = SignalRouter(
        registry,
        okx_accounts=OKX_ACCOUNTS,
        instrument_fallbacks=TRADABLE_INSTRUMENTS,
    )
    risk_engine = _build_risk_engine()
    validator = BasicOrderValidator(
        instrument_allowlist=set(TRADABLE_INSTRUMENTS) if TRADABLE_INSTRUMENTS else None
    )
    processed_accounts = 0
    submitted_orders = 0
    errors = 0
    signal_summaries: list[str] = []

    started = datetime.now(tz=timezone.utc)
    try:
        watchlist = _wave_watchlist()
        if watchlist:
            await asyncio.to_thread(_refresh_wave_signals_sync, watchlist)
        async with SignalRuntime() as runtime:
            for account_key, meta in OKX_ACCOUNTS.items():
                if not meta.get("enabled", True):
                    logger.debug("Account %s disabled via config; skipping", account_key)
                    continue
                model_id = meta.get("model_id")
                if not model_id:
                    logger.warning("OKX account %s missing model_id; skipping", account_key)
                    continue
                processed_accounts += 1
                try:
                    order_submitted, signal_summary = await _process_account_signal(
                        account_key=account_key,
                        meta=meta,
                        runtime=runtime,
                        router=router,
                        validator=validator,
                        risk_engine=risk_engine,
                        repository=repository,
                    )
                    if signal_summary:
                        signal_summaries.append(signal_summary)
                    if order_submitted:
                        submitted_orders += 1
                except OrderValidationError as exc:
                    errors += 1
                    logger.info("Order validation failed for %s: %s", account_key, exc)
                except Exception as exc:  # pragma: no cover - defensive
                    errors += 1
                    logger.exception("Execution pipeline failed for %s: %s", account_key, exc)
        status = "success" if errors == 0 else "warning"
        detail = f"处理 {processed_accounts} 个账户，提交 {submitted_orders} 个订单，错误 {errors} 次"
        if signal_summaries:
            preview = " | ".join(signal_summaries[:5])
            if len(signal_summaries) > 5:
                preview += " | ..."
            detail = f"{detail}；信号详情：{preview}"
        summary_entry = {
            "account_key": "scheduler",
            "account_id": "-",
            "model_id": "-",
            "instrument": "-",
            "account_enabled": processed_accounts > 0,
            "signal_generated": processed_accounts > 0,
            "decision_actionable": submitted_orders > 0,
            "order_ready": submitted_orders > 0,
            "risk_passed": submitted_orders > 0 and errors == 0,
            "submitted": submitted_orders > 0,
            "notes": detail,
        }
        _record_order_debug_entry(summary_entry)
        duration = (datetime.now(tz=timezone.utc) - started).total_seconds()
        _append_execution_log("ai", status, detail, duration_seconds=duration)
        return {"status": status, "detail": detail}
    except Exception as exc:
        logger.exception("Execution job fatal error: %s", exc)
        message = str(exc)
        duration = (datetime.now(tz=timezone.utc) - started).total_seconds()
        _append_execution_log("ai", "error", message, duration_seconds=duration)
        return {"status": "error", "detail": message}

async def _process_account_signal(
    *,
    account_key: str,
    meta: Dict[str, str],
    runtime: SignalRuntime,
    router: SignalRouter,
    validator: BasicOrderValidator,
    risk_engine: RiskEngine,
    repository: AccountRepository,
) -> Tuple[bool, Optional[str]]:
    snapshot = await asyncio.to_thread(_collect_okx_snapshot, account_key, meta)
    if snapshot is None:
        _record_order_debug_entry(
            {
                "account_key": account_key,
                "account_id": meta.get("account_id") or f"okx_{account_key}",
                "model_id": meta.get("model_id") or account_key,
                "account_enabled": bool(meta.get("enabled", True)),
                "signal_generated": False,
                "decision_actionable": False,
                "order_ready": False,
                "risk_passed": False,
                "submitted": False,
                "notes": "无法获取账户快照",
            }
        )
        return False, None
    account, balances, positions, trades, open_orders, market_price = snapshot

    _persist_snapshot(
        repository,
        account=account,
        balances=balances,
        positions=positions,
        trades=trades,
        open_orders=open_orders,
    )

    debug_entry: Dict[str, Any] = {
        "account_key": account_key,
        "account_id": account.account_id,
        "model_id": account.model_id,
        "instrument": None,
        "account_enabled": bool(meta.get("enabled", True)),
        "signal_generated": False,
        "decision_actionable": False,
        "order_ready": False,
        "risk_passed": False,
        "submitted": False,
        "decision": None,
        "confidence": None,
        "fallback_used": False,
        "notes": "",
    }

    automation_signal = await _maybe_execute_wave_strategy(
        account=account,
        positions=positions,
        open_orders=open_orders,
        meta=meta,
        market_price=market_price,
    )
    request: Optional[SignalRequest] = None
    response: Optional[SignalResponse] = None
    if automation_signal:
        request, response = automation_signal
        debug_entry["decision_actionable"] = True
        debug_entry["automation"] = "liquidation_wave"
        debug_entry["notes"] = response.reasoning
    else:
        manual_signal = await _maybe_generate_liquidation_reversal(
            account=account,
            positions=positions,
            meta=meta,
            runtime=runtime,
        )
        if manual_signal:
            request, response = manual_signal
            debug_entry["decision_actionable"] = True
            debug_entry["automation"] = "liquidation_reversal"
            debug_entry["notes"] = "?????????? 60 ?????????"
        else:
            request = _build_signal_request(
                model_id=account.model_id,
                meta=meta,
                account=account,
                positions=positions,
                market_price=market_price,
            )

            if request is None:
                logger.debug("Unable to build signal request for %s; skipping", account_key)
                debug_entry["notes"] = "??????????????????"
                _record_order_debug_entry(debug_entry)
                return False, None
            liquidation_alert = _annotate_liquidation_hint(request)
            if liquidation_alert:
                debug_entry["liquidation_hint"] = liquidation_alert
            try:
                response = await runtime.generate_signal(request)
            except Exception as exc:
                logger.exception("Model invocation failed for %s: %s", account_key, exc)
                debug_entry["notes"] = f"??????: {exc}"
                _record_order_debug_entry(debug_entry)
                return False, None
    if response is None:
        logger.debug("No signal generated for %s after liquidation scan.", account_key)
        debug_entry["notes"] = "无法生成信号"
        _record_order_debug_entry(debug_entry)
        return False, None
    if not debug_entry.get("automation"):
        reentry_response = _maybe_force_pyramid_reentry(
            request=request,
            positions=positions,
            open_orders=open_orders,
        )
        if reentry_response is not None:
            response = reentry_response
            debug_entry["automation"] = "pyramid_reentry"
            debug_entry["notes"] = reentry_response.reasoning
            debug_entry["decision_actionable"] = True


    debug_entry["decision_actionable"] = (response.decision or "").lower() not in {"hold"}
    routed = router.route(
        response,
        market=request.market,
        account=account,
        positions=positions,
        account_meta=meta,
    )
    if debug_entry.get("automation") == "liquidation_reversal":
        debug_entry["automation_summary"] = _format_liquidation_debug_summary(request, response, routed)
    if not routed and not _has_open_positions(positions):
        fallback = await _select_fallback_trade(
            runtime=runtime,
            router=router,
            model_id=account.model_id,
            meta=meta,
            account=account,
            positions=positions,
            market_price=market_price,
        )
        if fallback:
            request, response, routed = fallback
            debug_entry["fallback_used"] = True
            debug_entry["decision_actionable"] = True
            logger.info(
                "Fallback trade selected for %s using %s (confidence %.2f)",
                account_key,
                routed.intent.instrument_id,
                response.confidence,
            )
    signal_summary = _summarize_ai_signal(response, request, routed)
    debug_entry["signal_summary"] = signal_summary
    _record_ai_signal_entry(response, request, routed)
    if not routed:
        logger.debug("Router did not produce executable order for %s", account_key)
        debug_entry["instrument"] = request.market.instrument_id
        debug_entry["notes"] = "模型指令为 hold 或缺少订单信息"
        _record_order_debug_entry(debug_entry)
        return False, signal_summary
    debug_entry["order_ready"] = True
    debug_entry["instrument"] = routed.intent.instrument_id or request.market.instrument_id

    action_tag = _classify_order_action(routed.intent, positions)
    if action_tag:
        debug_entry["order_action"] = action_tag
        if isinstance(routed.intent.metadata, dict):
            routed.intent.metadata["order_action"] = action_tag
    _assign_okx_position_side(routed.intent, positions, meta)

    with _RISK_CONFIG_LOCK:
        pyramid_cap = int(_RISK_CONFIG.get("pyramid_max_orders", 0))
    if pyramid_cap > 0:
        active_count = _count_active_orders(open_orders, routed.intent.instrument_id, routed.intent.side)
        if active_count >= pyramid_cap:
            debug_entry["notes"] = f"金字塔同向订单达到 {pyramid_cap} 笔，已跳过下单"
            debug_entry["pyramid_blocked"] = True
            _record_order_debug_entry(debug_entry)
            return False, signal_summary

    with _RISK_CONFIG_LOCK:
        min_notional = float(_RISK_CONFIG.get("min_notional_usd", 0.0))
    if min_notional > 0:
        price_ref = (
            routed.intent.price
            or routed.market.mid_price
            or routed.market.last_price
            or routed.market.best_bid
            or routed.market.best_ask
        )
        try:
            notional = abs(float(routed.intent.size) * float(price_ref))
        except Exception:
            notional = 0.0
        if notional <= 0:
            debug_entry["notes"] = "缺少价格，无法验证最小名义金额"
            _record_order_debug_entry(debug_entry)
            if signal_summary:
                signal_summary = f"{signal_summary} | {debug_entry['notes']}"
            else:
                signal_summary = debug_entry["notes"]
            return False, signal_summary
        if notional < min_notional:
            if price_ref and price_ref > 0:
                size_value = _try_float(routed.intent.size) or 0.0
                direction = 1.0 if size_value >= 0 else -1.0
                adjusted_size = (min_notional / price_ref) * (direction or 1.0)
                routed.intent.size = round(adjusted_size, 8)
                debug_entry["min_notional_adjustment"] = f"Raised to {min_notional:.2f} USDT minimum"
            else:
                debug_entry["notes"] = "无法满足最小下单金额，缺少价格参考"
                _record_order_debug_entry(debug_entry)
                if signal_summary:
                    signal_summary = f"{signal_summary} | {debug_entry['notes']}"
                else:
                    signal_summary = debug_entry["notes"]
                return False, signal_summary

    size_note = _align_size_to_lot(routed.intent)
    if size_note:
        previous = debug_entry.get("size_adjusted")
        debug_entry["size_adjusted"] = f"{previous}; {size_note}" if previous else size_note

    try:
        ensure_valid_order(routed.intent, [validator])
    except OrderValidationError as exc:
        debug_entry["notes"] = f"下单校验失败: {exc}"
        _record_order_debug_entry(debug_entry)
        raise

    signal_summary = _summarize_ai_signal(response, request, routed)
    leverage_value = _resolve_leverage(meta, routed.intent.metadata.get("leverage"))
    routed.intent.metadata["leverage"] = leverage_value
    _prime_price_band(risk_engine, routed.market)
    evaluation = risk_engine.evaluate(routed.intent, routed.market, routed.portfolio_metrics)
    if not evaluation.approved:
        logger.info(
            "Risk engine rejected order for %s: %s",
            account_key,
            [violation.message for violation in evaluation.violations],
        )
        debug_entry["risk_passed"] = False
        debug_entry["notes"] = "; ".join(violation.message for violation in evaluation.violations)
        _record_order_debug_entry(debug_entry)
        return False, signal_summary
    debug_entry["risk_passed"] = True

    entry_price = _try_float(routed.intent.price)
    if not entry_price:
        entry_price = _try_float(getattr(routed.market, "last_price", None))
    if not entry_price:
        entry_price = _try_float((routed.market.metadata or {}).get("mid_price") if routed.market and routed.market.metadata else None)
    execution_result, placement_error = await asyncio.to_thread(
        _place_order,
        meta,
        routed.intent,
        entry_price,
    )
    if execution_result is None:
        debug_entry["notes"] = f"交易所下单失败: {placement_error or '未知错误'}"
        debug_entry["placement_error"] = placement_error
        _record_order_debug_entry(debug_entry)
        return False, signal_summary

    _record_submitted_order(repository, routed, execution_result)
    debug_entry["submitted"] = True
    debug_entry["notes"] = f"order_id={execution_result.get('order_id')}"
    _record_order_debug_entry(debug_entry)
    logger.info(
        "Submitted order %s for %s (%s %s @ %s)",
        execution_result.get("order_id"),
        account_key,
        routed.intent.side,
        routed.intent.size,
        routed.intent.price or "MKT",
    )
    return True, signal_summary


def _summarize_ai_signal(
    response: SignalResponse,
    request: SignalRequest,
    routed: Optional[RoutedOrder],
) -> str:
    instrument = getattr(request.market, "instrument_id", None) or (
        response.suggested_order or {}
    ).get("instrument_id") or "-"
    decision = response.decision
    confidence = f"{response.confidence:.2f}"
    order_desc = ""
    if routed and routed.intent:
        intent = routed.intent
        intent_instrument = intent.instrument_id or instrument
        instrument = intent_instrument or instrument
        order_desc = f"{intent.side} {intent.size} {intent_instrument}"
    else:
        suggested = response.suggested_order or {}
        side = suggested.get("side")
        size = suggested.get("size")
        instrument = suggested.get("instrument_id", instrument)
        if side or size:
            size_text = "-" if size in (None, "") else str(size)
            order_desc = f"{side or '-'} {size_text}"
    reason = (response.reasoning or "").strip().replace("\n", " ")
    if reason:
        reason = f" 理由:{reason}"
    return f"{instrument}:{response.model_id}={decision}({confidence}){(' -> ' + order_desc) if order_desc else ''}{reason}"


def _format_liquidation_debug_summary(
    request: SignalRequest,
    response: SignalResponse,
    routed: Optional[RoutedOrder],
) -> str:
    instrument = getattr(request.market, "instrument_id", None) or "-"
    meta = request.metadata or {}
    direction = meta.get("liquidation_direction") or (
        "long" if str((response.suggested_order or {}).get("side", "buy")).lower() == "buy" else "short"
    )
    notional = meta.get("liquidation_notional")
    try:
        notional_value = abs(float(notional))
        notional_text = f"{notional_value:,.0f} USDT"
    except (TypeError, ValueError):
        notional_text = str(notional) if notional is not None else "--"
    if routed and routed.intent:
        side = routed.intent.side
        size = routed.intent.size
    else:
        suggested = response.suggested_order or {}
        side = suggested.get("side", "-")
        size = suggested.get("size", "-")
    size_text = "-" if size in (None, "") else str(size)
    side_label = {"buy": "买入", "sell": "卖出"}.get(str(side).lower(), str(side or "-"))
    direction_label = "空单爆仓" if direction == "short" else "多单爆仓"
    guidance = "建议逢高防守，谨慎追多" if direction == "short" else "建议逢低承接，关注反弹"
    order_hint = f"{side_label} {size_text}".strip()
    if size_text in ("-", ""):
        order_hint = side_label
    return (
        f"爆仓巡检：检测到 1 条信号，建议{order_hint}；"
        f"{instrument} 净出现{direction_label}，名义金额约 {notional_text}，{guidance}。"
    )



def _record_ai_signal_entry(
    response: SignalResponse,
    request: SignalRequest,
    routed: Optional[RoutedOrder],
) -> None:
    if AI_SIGNAL_LOG_PATH is None:
        return
    order_payload: Dict[str, Any] = {}
    instrument = getattr(request.market, "instrument_id", None)
    if routed and routed.intent:
        intent = routed.intent
        instrument = intent.instrument_id or instrument
        order_payload = {
            "instrument_id": intent.instrument_id,
            "side": intent.side,
            "size": intent.size,
            "type": intent.order_type,
            "price": intent.price,
        }
    elif response.suggested_order:
        order_payload = dict(response.suggested_order)
        instrument = order_payload.get("instrument_id") or instrument
    record = {
        "timestamp": response.generated_at.isoformat(),
        "instrument_id": instrument or "-",
        "model_id": response.model_id,
        "decision": response.decision,
        "confidence": response.confidence,
        "reasoning": response.reasoning,
        "order": order_payload,
    }
    try:
        _append_signal_log(AI_SIGNAL_LOG_PATH, record, max_lines=SIGNAL_LOG_MAX_LINES)
    except Exception:
        logger.debug("Failed to append AI signal log.", exc_info=True)


def _collect_okx_snapshot(
    account_key: str,
    meta: Dict[str, str],
) -> Optional[Tuple[Account, List[Balance], List[Position], List[Trade], List[OrderModel], float]]:
    account_id = meta.get("account_id") or f"okx_{account_key}"
    model_id = meta.get("model_id", account_key)
    market_price: float = 0.0
    credentials = ExchangeCredentials(
        api_key=meta["api_key"],
        api_secret=meta["api_secret"],
        passphrase=meta.get("passphrase"),
    )

    client = OkxPaperClient()
    try:
        client.authenticate(credentials)
    except OkxClientError as exc:
        logger.error("[%s] OKX authentication failed: %s", account_key, exc)
        return None

    try:
        balances_raw = client.fetch_balances()
        equity, cash_balance, balance_models = extract_balances(
            balances_raw,
            account_id=account_id,
            base_currency=meta.get("base_currency", "USDT"),
        )
        positions = normalize_positions(account_id, client.fetch_positions())
        fills = client.fetch_fills(inst_type="SWAP", limit=100)
        trades = normalize_trades(
            account_id=account_id,
            model_id=model_id,
            raw_fills=fills,
        )
        open_orders = normalize_orders(
            account_id=account_id,
            model_id=model_id,
            raw_orders=client.fetch_open_orders(limit=100),
        )
        price_hint_instrument = (
            meta.get("default_instrument")
            or meta.get("instrument_id")
            or _infer_instrument_from_positions(positions)
            or (TRADABLE_INSTRUMENTS[0] if TRADABLE_INSTRUMENTS else None)
        )
        market_price = _infer_market_price(positions, trades, meta)
        if price_hint_instrument and (not market_price or market_price <= 0):
            try:
                ticker = client.fetch_ticker(price_hint_instrument)
                ticker_price = _try_float(
                    ticker.get("last")
                    or ticker.get("lastPx")
                    or ticker.get("last_price")
                    or ticker.get("px")
                )
                if ticker_price and ticker_price > 0:
                    market_price = ticker_price
            except Exception as exc:
                logger.debug("Failed to fetch price hint for %s: %s", price_hint_instrument, exc)
    finally:
        client.close()

    starting_equity = float(meta.get("starting_equity", 10_000.0))
    account = Account(
        account_id=account_id,
        model_id=model_id,
        base_currency=meta.get("base_currency", "USDT"),
        starting_equity=starting_equity,
        cash_balance=cash_balance,
        equity=equity,
        pnl=equity - starting_equity,
        updated_at=datetime.now(tz=timezone.utc),
    )

    return account, balance_models, positions, trades, open_orders, market_price


def _persist_snapshot(
    repository: AccountRepository,
    *,
    account: Account,
    balances: Iterable[Balance],
    positions: Iterable[Position],
    trades: Iterable[Trade],
    open_orders: Iterable[OrderModel],
) -> None:
    repository.upsert_account(account)
    repository.record_equity_point(account)
    for balance in balances:
        repository.record_balance(balance)
    for position in positions:
        repository.record_position(position)
    for trade in trades:
        repository.record_trade(trade)
    for order in open_orders:
        repository.record_order(order)


def _build_signal_request(
    *,
    model_id: str,
    meta: Dict[str, str],
    account: Account,
    positions: Sequence[Position],
    market_price: float,
    instrument_override: Optional[str] = None,
) -> Optional[SignalRequest]:
    instrument = instrument_override or (
        meta.get("default_instrument")
        or meta.get("instrument_id")
        or _infer_instrument_from_positions(positions)
        or (TRADABLE_INSTRUMENTS[0] if TRADABLE_INSTRUMENTS else None)
    )
    if not instrument:
        return None

    features = _load_market_features(instrument)
    feature_price = features.get("last_price") or features.get("mid_price")
    reference_price = feature_price if feature_price and feature_price > 0 else (market_price if market_price > 0 else 1.0)
    spread = features.get("spread")

    current_position = sum(
        position.quantity if position.instrument_id == instrument else 0.0 for position in positions
    )
    with _RISK_CONFIG_LOCK:
        risk_max_position = float(_RISK_CONFIG.get("max_position", 0.0))
        risk_position_tp = float(_RISK_CONFIG.get("position_take_profit_pct", 5.0))
        risk_position_sl = float(_RISK_CONFIG.get("position_stop_loss_pct", 3.0))
    try:
        meta_max_position = float(meta.get("max_position")) if "max_position" in meta else 0.0
    except (TypeError, ValueError):
        meta_max_position = 0.0
    max_position_base = max(0.01, account.starting_equity / reference_price * 0.1)
    max_position = meta_max_position or risk_max_position or max_position_base

    risk_context = RiskContext(
        max_position=max_position,
        current_position=current_position,
        cash_available=account.cash_balance,
        notes={
            "account_id": account.account_id,
            "equity": account.equity,
            "position_take_profit_pct": risk_position_tp,
            "position_stop_loss_pct": risk_position_sl,
        },
    )
    for key in ("funding_rate", "orderbook_imbalance", "cvd"):
        if key in features:
            risk_context.notes[key] = features[key]
    if features.get("liq_net_qty") is not None:
        risk_context.notes["liquidation_net_qty"] = features["liq_net_qty"]
    if features.get("orderbook_net_depth") is not None:
        risk_context.notes["orderbook_net_depth"] = features["orderbook_net_depth"]
    if features.get("15m_close_price") is not None:
        risk_context.notes["recent_close_price"] = features["15m_close_price"]
    if features.get("15m_volume_sum") is not None:
        risk_context.notes["recent_volume_sum"] = features["15m_volume_sum"]

    market_snapshot = MarketSnapshot(
        instrument_id=instrument,
        price=reference_price,
        spread=spread,
        volume_24h=features.get("15m_volume_sum"),
        metadata={
            "source": "scheduler",
            "account_id": account.account_id,
            "ai_cycle_seconds": _AI_INTERVAL,
            **{k: v for k, v in features.items() if v is not None},
        },
    )

    return SignalRequest(
        model_id=model_id,
        market=market_snapshot,
        risk=risk_context,
        positions=_summarize_positions_for_ai(positions),
        strategy_hint=meta.get("strategy_hint"),
        metadata={
            "account_id": account.account_id,
            "ai_cycle_seconds": _AI_INTERVAL,
        },
    )


def _annotate_liquidation_hint(request: SignalRequest) -> Optional[str]:
    """
    Flag large liquidation events in the request so the model can reason on them
    instead of pushing an immediate directional order.
    """
    meta = request.market.metadata or {}
    instrument = (request.market.instrument_id or "").upper()
    base = None
    if instrument.startswith("ETH"):
        base = "ETH"
    elif instrument.startswith("BTC"):
        base = "BTC"
    if base is None:
        return None

    try:
        long_qty = float(meta.get("liq_long_qty"))
    except (TypeError, ValueError):
        long_qty = None
    try:
        short_qty = float(meta.get("liq_short_qty"))
    except (TypeError, ValueError):
        short_qty = None

    threshold = 5.0
    reasoning: Optional[str] = None
    alert: Optional[str] = None

    if long_qty is not None and long_qty > threshold:
        reasoning = f"{base} 多单爆仓 {long_qty:.2f} > {threshold}"
        alert = f"{reasoning}，请评估是否跟随或加仓"
    elif short_qty is not None and short_qty > threshold:
        reasoning = f"{base} 空单爆仓 {short_qty:.2f} > {threshold}"
        alert = f"{reasoning}，请评估是否减仓或对冲"

    if not alert:
        return None

    request.market.metadata["liquidation_alert"] = alert
    request.market.metadata["liq_long_qty"] = long_qty
    request.market.metadata["liq_short_qty"] = short_qty
    request.risk.notes["liquidation_alert"] = alert
    request.metadata["liquidation_alert"] = alert
    return alert


async def _maybe_generate_liquidation_reversal(
    *,
    account: Account,
    positions: Sequence[Position],
    meta: Dict[str, str],
    runtime: SignalRuntime,
) -> Optional[Tuple[SignalRequest, SignalResponse]]:
    """
    Scan liquidation flow extremes and let the AI runtime evaluate whether to
    increase, trim, or close exposure before placing an automated trade.
    """
    detected_at = datetime.now(tz=timezone.utc)
    for instrument in _iter_liquidation_watch_instruments(meta):
        features = _load_market_features(instrument)
        decision = _evaluate_liquidation_reversal_state(instrument, features, detected_at)
        if not decision:
            continue
        direction, notional_qty, event_time, notional_usd = decision
        request = _build_signal_request(
            model_id=account.model_id,
            meta=meta,
            account=account,
            positions=positions,
            market_price=features.get("liq_last_price")
            or features.get("last_price")
            or features.get("mid_price")
            or 0.0,
            instrument_override=instrument,
        )
        if request is None:
            continue
        _attach_liquidation_metadata(
            request=request,
            direction=direction,
            notional_qty=notional_qty,
            notional_usd=notional_usd,
            event_time=event_time,
            detected_at=detected_at,
        )
        response = _finalize_liquidation_response(
            request=request,
            response=None,
            direction=direction,
            notional_qty=notional_qty,
            notional_usd=notional_usd,
            event_time=event_time,
            detected_at=detected_at,
        )
        if response is None:
            continue
        return request, response
    return None


def _maybe_force_pyramid_reentry(
    *,
    request: SignalRequest,
    positions: Sequence[Position],
    open_orders: Sequence[OrderModel],
) -> Optional[SignalResponse]:
    with _RISK_CONFIG_LOCK:
        deviation_pct = float(_RISK_CONFIG.get("pyramid_reentry_pct", 0.0))
        pyramid_cap = int(_RISK_CONFIG.get("pyramid_max_orders", 0))
        max_position = float(_RISK_CONFIG.get("max_position", 0.0))
    if deviation_pct <= 0:
        return None
    instrument = request.market.instrument_id
    if not instrument:
        return None
    candidate: Optional[Position] = None
    for pos in positions or []:
        if _get_attr(pos, "instrument_id") == instrument:
            candidate = pos
            break
    if candidate is None:
        return None
    side = (_get_attr(candidate, "side") or "").lower()
    qty = _try_float(_get_attr(candidate, "quantity"))
    entry_price = _try_float(_get_attr(candidate, "entry_price"))
    if not qty or not entry_price:
        return None
    if side not in {"long", "buy", "short", "sell"}:
        return None
    direction = "buy" if side in {"long", "buy"} else "sell"
    if pyramid_cap > 0 and _count_active_orders(open_orders, instrument, direction) >= pyramid_cap:
        return None
    metadata = request.market.metadata or {}
    close_price = _try_float(
        metadata.get("recent_close_price")
        or metadata.get("15m_close_price")
        or metadata.get("close_price")
        or metadata.get("last_price")
    )
    if close_price is None:
        close_price = _try_float(request.market.price)
    if close_price is None or close_price <= 0:
        return None
    threshold = entry_price * (1 - deviation_pct / 100.0) if direction == "buy" else entry_price * (1 + deviation_pct / 100.0)
    if direction == "buy":
        if close_price > threshold:
            return None
        observed = ((entry_price - close_price) / entry_price) * 100.0
    else:
        if close_price < threshold:
            return None
        observed = ((close_price - entry_price) / entry_price) * 100.0
    net_position = _compute_net_position(instrument, positions)
    remaining_capacity = None
    if max_position > 0:
        remaining_capacity = max(0.0, max_position - abs(net_position))
        if remaining_capacity <= 1e-9:
            return None
    order_size = abs(qty)
    if remaining_capacity is not None:
        order_size = min(order_size, remaining_capacity)
    if order_size <= 0:
        return None
    reasoning = (
        f"{instrument} 价格相对入场价偏移 {observed:.2f}% 超过 {deviation_pct:.2f}% 阈值，触发金字塔加仓。"
    )
    suggested_order = {
        "instrument_id": instrument,
        "side": direction,
        "size": order_size,
        "order_type": "market",
    }
    raw_output = {
        "automation": "pyramid_reentry",
        "pyramid_reentry_pct": deviation_pct,
        "observed_deviation_pct": observed,
        "entry_price": entry_price,
        "close_price": close_price,
    }
    return SignalResponse(
        model_id=request.model_id,
        decision="open_long" if direction == "buy" else "open_short",
        confidence=0.64,
        reasoning=reasoning,
        suggested_order=suggested_order,
        raw_output=raw_output,
    )


def _iter_liquidation_watch_instruments(meta: Dict[str, str]) -> Iterable[str]:
    preferred = [
        meta.get("default_instrument"),
        meta.get("instrument_id"),
    ]
    preferred.extend(TRADABLE_INSTRUMENTS or [])
    seen: set[str] = set()
    for inst in preferred:
        if not inst:
            continue
        symbol = str(inst).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        yield symbol


def _attach_liquidation_metadata(
    *,
    request: SignalRequest,
    direction: str,
    notional_qty: float,
    notional_usd: float,
    event_time: datetime,
    detected_at: datetime,
) -> None:
    request.strategy_hint = "liquidation_reversal"
    metadata = request.metadata
    metadata["automation"] = "liquidation_reversal"
    metadata["liquidation_direction"] = direction
    metadata["liquidation_notional_qty"] = notional_qty
    metadata["liquidation_notional"] = notional_usd
    metadata["liquidation_event_time"] = event_time.isoformat()
    metadata["liquidation_detected_at"] = detected_at.isoformat()
    request.market.metadata["liquidation_direction"] = direction
    request.market.metadata["liquidation_signal"] = True
    request.market.metadata["liquidation_event_time"] = event_time.isoformat()
    request.risk.notes["liquidation_direction"] = direction
    request.risk.notes["liquidation_notional"] = notional_usd


def _finalize_liquidation_response(
    *,
    request: SignalRequest,
    response: Optional[SignalResponse],
    direction: str,
    notional_qty: float,
    notional_usd: float,
    event_time: datetime,
    detected_at: datetime,
) -> Optional[SignalResponse]:
    instrument = request.market.instrument_id
    side = "buy" if direction == "long" else "sell"
    base_response = response
    suggested = dict(base_response.suggested_order) if base_response and base_response.suggested_order else {}
    if not suggested.get("instrument_id"):
        suggested["instrument_id"] = instrument
    if not suggested.get("side"):
        suggested["side"] = side
    size = None
    try:
        size = float(suggested.get("size", 0.0))
    except (TypeError, ValueError):
        size = None
    if size is None or size <= 0:
        size = _estimate_reversal_order_size(request, direction)
    if size <= 0:
        return None
    suggested["size"] = size
    suggested.setdefault("order_type", "market")

    reasoning = (
        f"{instrument} liquidation {'long' if direction == 'long' else 'short'} quantity {notional_qty:.2f} "
        f"(approx {notional_usd:,.0f} USDT) triggered {int((detected_at - event_time).total_seconds())}s ago; "
        f"executing {suggested['side']} {size}."
    )

    forced_decision = "open_long" if direction == "long" else "open_short"
    if base_response is None:
        base_response = SignalResponse(
            model_id=request.model_id,
            decision=forced_decision,
            confidence=0.72,
            reasoning=reasoning,
            suggested_order=suggested,
            raw_output={},
        )
    else:
        base_response.decision = forced_decision
        base_response.confidence = base_response.confidence or 0.72
        base_response.reasoning = base_response.reasoning or reasoning
        base_response.suggested_order = suggested

    raw_output = base_response.raw_output or {}
    raw_output["automation"] = "liquidation_reversal"
    raw_output["liquidation_direction"] = direction
    raw_output["liquidation_notional_qty"] = notional_qty
    raw_output["liquidation_notional"] = notional_usd
    raw_output["liquidation_event_time"] = event_time.isoformat()
    raw_output["liquidation_detected_at"] = detected_at.isoformat()
    base_response.raw_output = raw_output
    return base_response

def _resolve_liquidation_parameters(symbol: str) -> tuple[float, int, int, float]:
    with _RISK_CONFIG_LOCK:
        liq_threshold = float(_RISK_CONFIG.get("liquidation_notional_threshold", 50_000.0))
        same_required = int(_RISK_CONFIG.get("liquidation_same_direction_count", 4))
        confirmation_required = int(_RISK_CONFIG.get("liquidation_opposite_count", 3))
        silence_required = float(_RISK_CONFIG.get("liquidation_silence_seconds", 300.0))
    with _LIQUIDATION_OVERRIDE_LOCK:
        override = _LIQUIDATION_OVERRIDES.get(symbol.upper())
    if override:
        if "notional_threshold" in override:
            liq_threshold = max(0.0, float(override["notional_threshold"]))
        if "same_direction" in override:
            same_required = max(1, int(override["same_direction"]))
        if "opposite_direction" in override:
            confirmation_required = max(0, int(override["opposite_direction"]))
        if "silence_seconds" in override:
            silence_required = max(0.0, float(override["silence_seconds"]))
    return liq_threshold, same_required, confirmation_required, silence_required


def _evaluate_liquidation_reversal_state(
    instrument: str,
    features: Dict[str, float],
    now: datetime,
) -> Optional[Tuple[str, float, datetime, float]]:
    net_qty = features.get("liq_net_qty")
    timestamp_value = features.get("liq_timestamp")
    if net_qty is None or timestamp_value is None or net_qty == 0:
        return None
    abs_qty = abs(net_qty)
    if abs_qty <= 0:
        return None
    notional = _try_float(features.get("liq_notional"))
    if notional is None:
        price_hint = _try_float(features.get("liq_last_price")) or _try_float(features.get("last_price"))
        if price_hint is not None:
            notional = abs(net_qty) * price_hint
    symbol = instrument.upper()
    liq_threshold, same_required, confirmation_required, silence_required = _resolve_liquidation_parameters(symbol)
    zero_confirmation = confirmation_required <= 0
    confirmation_required = max(0, confirmation_required)
    liq_threshold = max(_MIN_LIQUIDATION_NOTIONAL_USD, liq_threshold)
    if liq_threshold <= 0:
        return None
    event_time = _coerce_timestamp(timestamp_value)
    if event_time is None:
        return None
    silence = (now - event_time).total_seconds()
    if silence < _LIQUIDATION_MIN_EVENT_AGE_SECONDS or silence > _LIQUIDATION_MAX_EVENT_LOOKBACK:
        return None
    state = _LIQUIDATION_REVERSAL_WATCH.setdefault(
        symbol,
        {
            "history": deque(),
            "candidate_direction": None,
            "last_same_time": None,
            "opposite_count": 0,
            "last_large_event": None,
            "silence_armed": False,
            "post_silence_same_count": 0,
            "hold_until": None,
        },
    )
    hold_until = state.get("hold_until")
    if isinstance(hold_until, datetime):
        if hold_until > now:
            return None
        state["hold_until"] = None
    history: deque = state["history"]

    def _finalize_reversal(direction_sign: int) -> Tuple[str, float, datetime, float]:
        state["candidate_direction"] = None
        state["last_same_time"] = None
        state["opposite_count"] = 0
        state["silence_armed"] = False
        state["post_silence_same_count"] = 0
        state["last_large_event"] = event_time
        if _LIQUIDATION_MIN_POSITION_HOLD_SECONDS > 0:
            state["hold_until"] = now + timedelta(seconds=_LIQUIDATION_MIN_POSITION_HOLD_SECONDS)
        else:
            state["hold_until"] = None
        history.clear()
        return ("long" if direction_sign > 0 else "short", abs_qty, event_time, notional_value or abs_qty)
    cutoff = event_time - timedelta(minutes=15)
    while history and history[0][0] < cutoff:
        history.popleft()

    sign = 1 if net_qty > 0 else -1
    candidate_dir = state.get("candidate_direction")
    eligible_same = notional is not None and notional >= liq_threshold
    notional_value = float(notional) if notional is not None else 0.0

    previous_large = state.get("last_large_event")
    if eligible_same:
        if isinstance(previous_large, datetime):
            gap = (event_time - previous_large).total_seconds()
            if candidate_dir is not None and not state.get("silence_armed") and gap >= silence_required:
                state["silence_armed"] = True
                state["post_silence_same_count"] = 0
        state["last_large_event"] = event_time
    silence_armed = bool(state.get("silence_armed"))
    post_silence_same = int(state.get("post_silence_same_count") or 0)

    if eligible_same:
        history.append((event_time, sign))
        same_count = sum(1 for ts, sg in history if sg == sign)
        if same_count >= same_required and (candidate_dir is None or candidate_dir == sign):
            if zero_confirmation:
                return _finalize_reversal(sign)
            state["candidate_direction"] = sign
            state["last_same_time"] = event_time
            state["opposite_count"] = 0
            state["silence_armed"] = False
            state["post_silence_same_count"] = 0
            return None
        if candidate_dir == sign:
            state["last_same_time"] = event_time
            state["opposite_count"] = 0
            if silence_armed:
                post_silence_same += 1
                state["post_silence_same_count"] = post_silence_same
                if post_silence_same >= confirmation_required:
                    return _finalize_reversal(sign)
            return None
    elif candidate_dir is None:
        return None

    candidate_dir = state.get("candidate_direction")
    if candidate_dir is None:
        return None
    if sign == candidate_dir and not eligible_same:
        state["opposite_count"] = 0
        return None
    if sign != -candidate_dir:
        state["opposite_count"] = 0
        return None

    last_same = state.get("last_same_time")
    if not isinstance(last_same, datetime):
        return None
    silence = (event_time - last_same).total_seconds()
    if silence < silence_required:
        state["opposite_count"] = 0
        return None

    if zero_confirmation:
        return _finalize_reversal(sign)

    state["opposite_count"] = int(state.get("opposite_count") or 0) + 1
    if state["opposite_count"] < confirmation_required:
        return None

    return _finalize_reversal(sign)


def _estimate_reversal_order_size(request: SignalRequest, direction: str) -> float:
    with _RISK_CONFIG_LOCK:
        min_notional = float(_RISK_CONFIG.get("min_notional_usd", 0.0))
    price_hint = _try_float(getattr(request.market, "price", None))
    if (price_hint is None or price_hint <= 0) and request.market.metadata:
        price_hint = _try_float(request.market.metadata.get("last_price"))

    max_position = float(max(request.risk.max_position or 0.0, 0.0))
    current = float(request.risk.current_position or 0.0)
    if direction == "long":
        used = max(0.0, current)
    else:
        used = max(0.0, -current)
    capacity = max(0.0, max_position - used)

    notional_target = 0.0
    if min_notional > 0 and price_hint and price_hint > 0:
        notional_target = max(0.0, min_notional / price_hint)

    if max_position <= 0:
        return round(notional_target, 6) if notional_target > 0 else 0.0
    if capacity <= 0:
        return 0.0

    clip = max(max_position * 0.25, 0.001)
    target = notional_target if notional_target > 0 else clip
    return round(min(capacity, target), 6)


def _coerce_timestamp(value: float | int | None) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


async def _select_fallback_trade(
    *,
    runtime: SignalRuntime,
    router: SignalRouter,
    model_id: str,
    meta: Dict[str, str],
    account: Account,
    positions: Sequence[Position],
    market_price: float,
) -> Optional[Tuple[SignalRequest, SignalResponse, RoutedOrder]]:
    """Evaluate all tradable instruments and return the highest-confidence routed order."""
    if not TRADABLE_INSTRUMENTS:
        return None
    candidates: list[tuple[float, SignalRequest, SignalResponse, RoutedOrder]] = []
    seen: set[str] = set()
    for instrument in TRADABLE_INSTRUMENTS:
        inst = (instrument or "").strip()
        if not inst or inst in seen:
            continue
        seen.add(inst)
        request = _build_signal_request(
            model_id=model_id,
            meta=meta,
            account=account,
            positions=positions,
            market_price=market_price,
            instrument_override=inst,
        )
        if request is None:
            continue
        try:
            response = await runtime.generate_signal(request)
        except Exception as exc:  # pragma: no cover - adapter failures handled per instrument
            logger.debug("Fallback signal generation failed for %s: %s", inst, exc)
            continue
        routed = router.route(
            response,
            market=request.market,
            account=account,
            positions=positions,
            account_meta=meta,
        )
        if not routed:
            continue
        candidates.append((float(response.confidence or 0.0), request, response, routed))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, best_request, best_response, best_routed = candidates[0]
    fallback_meta = best_routed.intent.metadata
    fallback_meta["fallback_selected"] = True
    if best_routed.intent.instrument_id in TRADABLE_INSTRUMENTS:
        fallback_meta["fallback_rank"] = TRADABLE_INSTRUMENTS.index(best_routed.intent.instrument_id) + 1
    return best_request, best_response, best_routed


def _summarize_positions_for_ai(positions: Sequence[Position]) -> List[Dict[str, Any]]:
    """Convert full position models into lightweight dicts for model prompts."""
    summaries: List[Dict[str, Any]] = []
    for position in positions:
        summaries.append(
            {
                "instrument_id": position.instrument_id,
                "side": position.side,
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "mark_price": position.mark_price,
                "unrealized_pnl": position.unrealized_pnl,
                "leverage": position.leverage,
                "updated_at": position.updated_at.isoformat(),
            }
        )
    return summaries


def _has_open_positions(positions: Sequence[Position]) -> bool:
    """Return True when the account currently holds any non-zero quantity."""
    for position in positions:
        try:
            qty = float(position.quantity)
        except (TypeError, ValueError):
            continue
        if abs(qty) > 1e-8:
            return True
    return False


def _choose_margin_mode(instrument_id: str, meta: Dict[str, str]) -> str:
    """
    Derive the correct OKX tdMode for an order.

    OKX perpetual/futures contracts reject "cash" (spot) margin mode with
    a generic "All operations failed" error. Default to cross margin for
    derivative symbols when no explicit override is provided.
    """
    override = str(meta.get("margin_mode") or "").lower()
    if override in {"cash", "cross", "isolated"}:
        return override
    symbol = (instrument_id or "").upper()
    if symbol.endswith("-SWAP") or "FUTURE" in symbol:
        return "cross"
    return "cash"


def _resolve_leverage(meta: Dict[str, str], override: object = None) -> float:
    candidate = _try_float(override)
    if candidate is None:
        candidate = _try_float(meta.get("default_leverage"))
    with _RISK_CONFIG_LOCK:
        default_leverage = float(_RISK_CONFIG.get("default_leverage", 2.0))
        max_leverage = float(_RISK_CONFIG.get("max_leverage", max(default_leverage, 1.0)))
    if candidate is None or candidate <= 0:
        candidate = default_leverage
    return max(1.0, min(max_leverage, candidate))


def _summarize_okx_error(payload: Optional[dict]) -> str:
    """
    Render the OKX error payload into a compact string for debugging.
    """
    if not payload or not isinstance(payload, dict):
        return ""
    details: list[str] = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            details.append(str(item))
            continue
        code = item.get("sCode") or item.get("code")
        msg = item.get("sMsg") or item.get("msg")
        tag = item.get("tag")
        snippet = ""
        if code and msg:
            snippet = f"{code}: {msg}"
        elif code:
            snippet = str(code)
        elif msg:
            snippet = str(msg)
        if tag:
            snippet = f"{snippet} (tag={tag})" if snippet else f"(tag={tag})"
        if not snippet:
            snippet = str(item)
        details.append(snippet)
    return "; ".join(details)


def _place_order(meta: Dict[str, str], intent: OrderIntent, entry_price: float | None = None) -> tuple[Optional[Dict[str, object]], Optional[str]]:
    credentials = ExchangeCredentials(
        api_key=meta["api_key"],
        api_secret=meta["api_secret"],
        passphrase=meta.get("passphrase"),
    )
    account_id = intent.metadata.get("account_id") if isinstance(intent.metadata, dict) else None
    client = OkxPaperClient()
    try:
        client.authenticate(credentials)
    except OkxClientError as exc:
        logger.error("[%s] Unable to authenticate for order placement: %s", account_id, exc)
        return None, str(exc)

    try:
        margin_mode = _choose_margin_mode(intent.instrument_id, meta)
        payload: Dict[str, object] = {
            "instrument_id": intent.instrument_id,
            "side": intent.side,
            "order_type": intent.order_type,
            "size": intent.size,
            "margin_mode": margin_mode,
        }
        meta_payload = intent.metadata if isinstance(intent.metadata, dict) else {}
        leverage_value = _resolve_leverage(meta, meta_payload.get("leverage"))
        payload["leverage"] = leverage_value
        if intent.price:
            payload["price"] = intent.price
        client_order_id = meta_payload.get("client_order_id")
        if client_order_id:
            payload["client_order_id"] = client_order_id
        pos_side = meta_payload.get("pos_side")
        if pos_side:
            payload["pos_side"] = str(pos_side).upper()
        if meta_payload.get("reduce_only"):
            payload["reduce_only"] = True
        with _RISK_CONFIG_LOCK:
            tp_pct = float(_RISK_CONFIG.get("take_profit_pct", 0.0))
            sl_pct = float(_RISK_CONFIG.get("stop_loss_pct", 0.0))
        reference_price = entry_price
        if reference_price is None:
            reference_price = _try_float(intent.price)
        if reference_price is None:
            reference_price = _try_float(meta_payload.get("reference_price"))
        payload = apply_bracket_targets(
            payload,
            side=intent.side,
            entry_price=reference_price,
            take_profit_pct=tp_pct,
            stop_loss_pct=sl_pct,
        )
        raw_response = None
        try:
            try:
                client.set_leverage(
                    instrument_id=payload["instrument_id"],
                    leverage=leverage_value,
                    margin_mode=margin_mode,
                    position_side=str(payload.get("pos_side")).upper() if payload.get("pos_side") else None,
                )
            except OkxClientError as exc:
                logger.error(
                    "[%s] Failed to set leverage %.2f for %s: %s",
                    account_id,
                    leverage_value,
                    payload["instrument_id"],
                    exc,
                )
                raise
            result = client.place_order(payload)
            raw_response = result.get("raw")
            return result, None
        except OkxClientError as exc:
            raw_payload = getattr(exc, "payload", None)
            raw_response = raw_payload or getattr(exc, "args", [None])[0]
            detail = str(exc)
            if raw_payload:
                code = raw_payload.get("code")
                msg = raw_payload.get("msg") or raw_payload.get("sMsg")
                parts = [f"code={code}", f"msg={msg}"]
                extra = _summarize_okx_error(raw_payload)
                if extra:
                    parts.append(f"details={extra}")
                detail = f"{detail} ({'; '.join(parts)})"
            raise OkxClientError(detail, payload=raw_payload)
    except OkxClientError as exc:
        logger.error("[%s] Order placement failed: %s", account_id, exc)
        return None, f"{exc}"
    except Exception as exc:
        logger.exception("[%s] Unexpected error during order placement", account_id)
        return None, str(exc)
    finally:
        if raw_response:
            logger.debug("[%s] OKX raw response: %s", account_id, raw_response)
        client.close()


def _record_submitted_order(
    repository: AccountRepository,
    routed: RoutedOrder,
    execution_result: Dict[str, object],
) -> None:
    order_id = str(execution_result.get("order_id"))
    account_id = routed.intent.metadata.get("account_id") or routed.portfolio.portfolio_id
    order = OrderModel(
        order_id=order_id,
        account_id=account_id,
        model_id=routed.signal.model_id,
        instrument_id=routed.intent.instrument_id,
        side=routed.intent.side,
        order_type=routed.intent.order_type,
        size=routed.intent.size,
        filled_size=0.0,
        price=routed.intent.price,
        average_price=None,
        state=str(execution_result.get("status", "submitted")),
        created_at=datetime.now(tz=timezone.utc),
        updated_at=datetime.now(tz=timezone.utc),
    )
    repository.record_order(order)


def _load_market_features(instrument_id: str) -> Dict[str, float]:
    query_api, config = _get_feature_query_api()
    if not query_api or not config:
        return {}
    try:
        micro_query = f"""
from(bucket: "{config.bucket}")
  |> range(start: -30m)
  |> filter(fn: (r) => r["_measurement"] == "market_microstructure")
  |> filter(fn: (r) => r["instrument_id"] == "{instrument_id}")
  |> last()
"""
        indicator_query = f"""
from(bucket: "{config.bucket}")
  |> range(start: -2h)
  |> filter(fn: (r) => r["_measurement"] == "market_indicators")
  |> filter(fn: (r) => r["instrument_id"] == "{instrument_id}")
  |> filter(fn: (r) => r["timeframe"] == "15m")
  |> last()
"""
        liquidation_query = f"""
from(bucket: "{config.bucket}")
  |> range(start: -30m)
  |> filter(fn: (r) => r["_measurement"] == "okx_liquidations")
  |> filter(fn: (r) => r["instrument_id"] == "{instrument_id}")
  |> last()
"""
        orderbook_query = f"""
from(bucket: "{config.bucket}")
  |> range(start: -10m)
  |> filter(fn: (r) => r["_measurement"] == "okx_orderbook_depth")
  |> filter(fn: (r) => r["instrument_id"] == "{instrument_id}")
  |> last()
"""
        features = _flux_tables_to_dict(query_api.query(micro_query))
        indicator_vals = _flux_tables_to_dict(query_api.query(indicator_query))
        for key, value in indicator_vals.items():
            features[f"15m_{key}"] = value
        liquidation_tables = query_api.query(liquidation_query)
        features.update(_flux_tables_to_dict(liquidation_tables, prefix="liq_"))
        liq_timestamp = _extract_latest_timestamp(liquidation_tables)
        if liq_timestamp:
            features["liq_timestamp"] = liq_timestamp.timestamp()
        features.update(_flux_tables_to_dict(query_api.query(orderbook_query), prefix="orderbook_"))

        bid_depth = features.get("orderbook_total_bid_qty")
        ask_depth = features.get("orderbook_total_ask_qty")
        if features.get("orderbook_net_depth") is None and bid_depth is not None and ask_depth is not None:
            features["orderbook_net_depth"] = bid_depth - ask_depth

        net_liq = features.get("liq_net_qty")
        liq_price = features.get("liq_last_price")
        if net_liq is not None and liq_price is not None:
            features.setdefault("liq_notional", abs(net_liq) * liq_price)
        return features
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Failed to load market features for %s: %s", instrument_id, exc)
        return {}


def _get_feature_query_api() -> Tuple[Optional[object], Optional["InfluxConfig"]]:
    global _FEATURE_CLIENT, _FEATURE_CONFIG
    if InfluxDBClient is None or InfluxConfig is None:
        return None, None
    if _FEATURE_CLIENT is None or _FEATURE_CONFIG is None:
        try:
            cfg = InfluxConfig.from_env()
        except Exception as exc:  # pragma: no cover
            logger.debug("Unable to load Influx configuration for market features: %s", exc)
            return None, None
        if not cfg.token:
            logger.debug("Influx token not provided; market features unavailable.")
            return None, None
        _FEATURE_CLIENT = InfluxDBClient(url=cfg.url, token=cfg.token, org=cfg.org)
        _FEATURE_CONFIG = cfg
    return _FEATURE_CLIENT.query_api(), _FEATURE_CONFIG


def _extract_latest_timestamp(tables: Iterable[object]) -> Optional[datetime]:
    latest: Optional[datetime] = None
    if not tables:
        return None
    for table in tables:
        records = getattr(table, "records", None)
        if not records:
            continue
        for record in records:
            getter = getattr(record, "get_time", None)
            if not callable(getter):
                continue
            try:
                timestamp = getter()
            except Exception:
                continue
            if isinstance(timestamp, datetime):
                if latest is None or timestamp > latest:
                    latest = timestamp
    return latest


def _flux_tables_to_dict(tables: Iterable[object], *, prefix: str = "") -> Dict[str, float]:
    values: Dict[str, float] = {}
    if tables is None:
        return values
    for table in tables:
        records = getattr(table, "records", None)
        if not records:
            continue
        for record in records:
            field = getattr(record, "get_field", lambda: None)()
            value = getattr(record, "get_value", lambda: None)()
            if not field:
                continue
            numeric = _try_float(value)
            if numeric is not None:
                key = f"{prefix}{field}" if prefix else field
                values[key] = numeric
    return values


def _try_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return None if math.isnan(result) else result
    try:
        result = float(value)
        return None if math.isnan(result) else result
    except (TypeError, ValueError):
        return None


def _infer_market_price(
    positions: Sequence[Position],
    trades: Sequence[Trade],
    meta: Dict[str, str],
) -> float:
    if trades:
        try:
            return float(trades[0].price)
        except (TypeError, ValueError):
            pass
    for position in positions:
        if position.mark_price:
            return float(position.mark_price)
    if "price_hint" in meta:
        try:
            return float(meta["price_hint"])
        except (TypeError, ValueError):
            pass
    return 0.0


def _infer_instrument_from_positions(positions: Sequence[Position]) -> Optional[str]:
    if not positions:
        return None
    return positions[0].instrument_id


def _get_okx_instrument_meta(inst_id: str) -> Optional[dict]:
    """
    Load instrument metadata from the cached OKX catalog once and reuse it for
    order normalization (lot size/min size).
    """
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
        except Exception as exc:  # pragma: no cover - defensive path
            logger.debug("Failed to load OKX instrument catalog cache: %s", exc)
            _OKX_INSTRUMENT_MAP = {}
    return _OKX_INSTRUMENT_MAP.get(str(inst_id).upper())


def _align_size_to_lot(intent: OrderIntent) -> Optional[str]:
    """
    Clamp order size to the nearest valid OKX lot size to avoid 51121 rejections.
    Returns a human-readable note when an adjustment was applied.
    """
    instrument = _get_okx_instrument_meta(intent.instrument_id)
    if not instrument:
        return None

    lot_size = _try_float(instrument.get("lotSz"))
    min_size = _try_float(instrument.get("minSz"))
    if not lot_size or lot_size <= 0:
        return None

    raw_size = float(intent.size or 0.0)
    multiples = math.floor(raw_size / lot_size)
    if multiples <= 0:
        multiples = 1
    adjusted = multiples * lot_size
    if min_size and min_size > 0 and adjusted < min_size:
        adjusted = math.ceil(min_size / lot_size) * lot_size

    adjusted = float(f"{adjusted:.10g}")  # trim float noise for cleaner payloads
    if abs(adjusted - raw_size) <= 1e-9:
        return None

    intent.size = adjusted
    return f"{intent.instrument_id} size adjusted to {adjusted} (lot={lot_size}, min={min_size or lot_size})"


def _get_attr(obj, name: str):
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _count_active_orders(open_orders: Sequence[object], instrument_id: str, side: str) -> int:
    """Count open orders for pyramid cap enforcement."""
    active_states = {"live", "open", "new", "triggered", "partially_filled", "waiting"}
    count = 0
    for order in open_orders or []:
        inst = _get_attr(order, "instrument_id")
        ord_side = (_get_attr(order, "side") or "").lower()
        state = (_get_attr(order, "state") or "").lower()
        if inst != instrument_id or ord_side != side.lower():
            continue
        if state and state not in active_states:
            continue
        count += 1
    return count


def _compute_net_position(instrument_id: str, positions: Sequence[Position]) -> float:
    net_qty = 0.0
    for pos in positions or []:
        if _get_attr(pos, "instrument_id") != instrument_id:
            continue
        qty = _get_attr(pos, "quantity") or 0.0
        try:
            qty = float(qty)
        except (TypeError, ValueError):
            qty = 0.0
        side = (_get_attr(pos, "side") or "").lower()
        net_qty += qty if side in {"long", "buy"} else -qty
    return net_qty


def _instrument_side_profile(instrument_id: str, positions: Sequence[Position]) -> tuple[bool, bool]:
    has_long = False
    has_short = False
    for pos in positions or []:
        if _get_attr(pos, "instrument_id") != instrument_id:
            continue
        side = (_get_attr(pos, "side") or "").lower()
        if side in {"long", "buy"}:
            has_long = True
        elif side in {"short", "sell"}:
            has_short = True
    return has_long, has_short


def _assign_okx_position_side(
    intent: OrderIntent,
    positions: Sequence[Position],
    account_meta: Optional[Dict[str, str]] = None,
) -> None:
    """Populate OKX metadata for accounts that explicitly use hedge mode."""
    if intent is None or not intent.instrument_id:
        return
    net_qty = _compute_net_position(intent.instrument_id, positions)
    side = (intent.side or "buy").lower()
    if abs(net_qty) < 1e-9:
        pos_side = "long" if side == "buy" else "short"
    else:
        pos_side = "long" if net_qty > 0 else "short"
    reduce_only = (pos_side == "long" and side == "sell") or (pos_side == "short" and side == "buy")
    pos_side_api = "LONG" if pos_side == "long" else "SHORT"
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}

    hedge_mode = False
    explicit_mode = str((account_meta or {}).get("position_mode") or "").lower()
    if explicit_mode in {"hedge", "long_short"}:
        hedge_mode = True
    elif explicit_mode in {"net", "oneway"}:
        hedge_mode = False
    else:
        has_long, has_short = _instrument_side_profile(intent.instrument_id, positions)
        hedge_mode = has_long and has_short

    if hedge_mode:
        metadata["pos_side"] = pos_side_api
    else:
        metadata.pop("pos_side", None)
        metadata.pop("posSide", None)

    if reduce_only:
        metadata["reduce_only"] = True
    intent.metadata = metadata


def _classify_order_action(intent: OrderIntent, positions: Sequence[Position]) -> Optional[str]:
    """
    Derive a human-friendly action tag for the order:
    建仓/加仓/减仓/平仓/对冲单.
    """
    if intent is None or not intent.instrument_id:
        return None
    net_qty = _compute_net_position(intent.instrument_id, positions)

    incoming = float(intent.size or 0.0)
    incoming = incoming if intent.side.lower() == "buy" else -incoming

    if net_qty == 0:
        return "建仓"
    if net_qty * incoming > 0:
        return "加仓"
    if abs(incoming) < abs(net_qty):
        return "减仓"
    if abs(incoming) == abs(net_qty):
        return "平仓"
    return "对冲单"


def _refresh_leaderboard(repository: AccountRepository) -> None:
    snapshot = refresh_leaderboard_cache(repository)
    logger.debug("Leaderboard refreshed with %d entries at %s", len(snapshot.get("leaders", [])), snapshot.get("as_of"))


def _record_equity_points(repository: AccountRepository) -> None:
    accounts = repository.list_accounts()
    for account in accounts:
        repository.record_equity_point(account)
    if accounts:
        logger.debug(
            "Recorded equity points for %d accounts at %s",
            len(accounts),
            datetime.now(tz=timezone.utc).isoformat(),
        )
