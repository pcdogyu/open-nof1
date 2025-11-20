"""
Background scheduler for analytics refresh and automated signal execution.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
from risk.notional_limits import OrderNotionalGuard, ProfitLossGuard
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
from services.webapp.dependencies import (
    get_account_repository,
    get_portfolio_registry,
)
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

try:  # pragma: no cover - config is optional for tests
    from config import (
        AI_INTERACTION_INTERVAL,
        MARKET_SYNC_INTERVAL,
        OKX_ACCOUNTS,
        TRADABLE_INSTRUMENTS,
    )
    try:
        from config import RISK_SETTINGS as _RISK_DEFAULTS  # type: ignore[attr-defined]
    except Exception:
        _RISK_DEFAULTS = {}
except ImportError:  # pragma: no cover
    OKX_ACCOUNTS = {}
    TRADABLE_INSTRUMENTS: Sequence[str] = ()
    MARKET_SYNC_INTERVAL = 60
    AI_INTERACTION_INTERVAL = 300
    _RISK_DEFAULTS = {}

_SCHEDULER: Optional["AsyncIOScheduler"] = None
_FEATURE_CLIENT: Optional["InfluxDBClient"] = None
_FEATURE_CONFIG: Optional["InfluxConfig"] = None
_OKX_INSTRUMENT_MAP: Optional[Dict[str, dict]] = None

_ANALYTICS_JOB_ID = "refresh_leaderboard_and_equity_curve"
_MARKET_JOB_ID = "market_data_sync"
_EXECUTION_JOB_ID = "execute_model_signals"


def _sanitize_interval(value: int, minimum: int, maximum: int = 3600) -> int:
    return int(max(minimum, min(maximum, value)))


_DEFAULT_RISK_CONFIG = {
    "price_tolerance_pct": 0.02,
    "max_drawdown_pct": 8.0,
    "max_loss_absolute": 1500.0,
    "cooldown_seconds": 600,
    "min_notional_usd": 50.0,
    "max_order_notional_usd": 0.0,
    "take_profit_pct": 0.0,
    "stop_loss_pct": 0.0,
    "pyramid_max_orders": 100,
}


def _sanitize_risk_config(raw: Optional[Dict[str, Any]]) -> Dict[str, float | int]:
    config = dict(_DEFAULT_RISK_CONFIG)
    if isinstance(raw, dict):
        for key in (
            "price_tolerance_pct",
            "max_drawdown_pct",
            "max_loss_absolute",
            "cooldown_seconds",
            "min_notional_usd",
            "max_order_notional_usd",
            "pyramid_max_orders",
        ):
            if key in raw:
                config[key] = raw[key]  # type: ignore[assignment]
    price = float(config["price_tolerance_pct"])
    drawdown = float(config["max_drawdown_pct"])
    max_loss = float(config["max_loss_absolute"])
    cooldown = int(config["cooldown_seconds"])
    min_notional = float(config["min_notional_usd"])
    pyramid_max = int(config.get("pyramid_max_orders", 0))
    price = max(0.001, min(0.5, price))
    drawdown = max(0.1, min(95.0, drawdown))
    max_loss = max(1.0, max_loss)
    cooldown = max(10, min(86400, cooldown))
    max_order_notional = float(config.get("max_order_notional_usd", 0.0))
    max_order_notional = max(0.0, min(1_000_000.0, max_order_notional))
    min_notional = max(0.0, min_notional)
    pyramid_max = max(0, min(100, pyramid_max))
    return {
        "price_tolerance_pct": price,
        "max_drawdown_pct": drawdown,
        "max_loss_absolute": max_loss,
        "cooldown_seconds": cooldown,
        "min_notional_usd": min_notional,
        "max_order_notional_usd": max_order_notional,
        "pyramid_max_orders": pyramid_max,
    }


_MARKET_INTERVAL = _sanitize_interval(int(MARKET_SYNC_INTERVAL), 30)
_AI_INTERVAL = _sanitize_interval(int(AI_INTERACTION_INTERVAL), 60)
AI_SIGNAL_LOG_PATH = Path("data/logs/ai_signals.jsonl")
AI_SIGNAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_PIPELINE_SETTINGS = PipelineSettings(
    instruments=list(TRADABLE_INSTRUMENTS) if TRADABLE_INSTRUMENTS else ["BTC-USDT-SWAP"],
    signal_log_path=AI_SIGNAL_LOG_PATH,
)
_PIPELINE: Optional[MarketDataPipeline] = None
_RISK_CONFIG_LOCK = Lock()
_RISK_CONFIG = _sanitize_risk_config(_RISK_DEFAULTS)
_EXECUTION_LOG = deque(maxlen=100)
_ORDER_DEBUG_LOG = deque(maxlen=50)
_ORDER_DEBUG_WRITER: Optional["InfluxWriter"] = None
_EXEC_LOG_WRITER: Optional["InfluxWriter"] = None

_LIQUIDATION_REVERSAL_WATCH: Dict[str, Dict[str, object]] = {}
_LIQUIDATION_SILENCE_SECONDS = 60
_LIQUIDATION_ABS_THRESHOLD = 5.0
_LIQUIDATION_MAX_EVENT_LOOKBACK = 15 * 60


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


def get_risk_configuration() -> Dict[str, float | int]:
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
    take_profit_pct: float = 0.0,
    stop_loss_pct: float = 0.0,
    pyramid_max_orders: int = 0,
) -> None:
    """Refresh the in-memory risk configuration."""
    normalized = _sanitize_risk_config(
        {
            "price_tolerance_pct": price_tolerance_pct,
            "max_drawdown_pct": max_drawdown_pct,
            "max_loss_absolute": max_loss_absolute,
            "cooldown_seconds": cooldown_seconds,
        "min_notional_usd": min_notional_usd,
            "max_order_notional_usd": max_order_notional_usd,
            "take_profit_pct": take_profit_pct,
            "stop_loss_pct": stop_loss_pct,
            "pyramid_max_orders": pyramid_max_orders,
        }
    )
    with _RISK_CONFIG_LOCK:
        _RISK_CONFIG.update(normalized)


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
    pnl_guard = ProfitLossGuard(
        take_profit_pct=float(config.get("take_profit_pct", 0.0)),
        stop_loss_pct=float(config.get("stop_loss_pct", 0.0)),
    )
    return RiskEngine(
        price_validator=price_validator,
        circuit_breaker=circuit_breaker,
        notional_guard=notional_guard,
        pnl_guard=pnl_guard,
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
    scheduler.start()
    _SCHEDULER = scheduler
    logger.info("Background scheduler started with analytics and execution jobs.")


def update_task_intervals(*, market_interval: Optional[int] = None, ai_interval: Optional[int] = None) -> None:
    """Update scheduled job intervals at runtime."""
    global _MARKET_INTERVAL, _AI_INTERVAL
    scheduler = _SCHEDULER
    if scheduler is None or IntervalTrigger is None:
        if market_interval is not None:
            _MARKET_INTERVAL = _sanitize_interval(market_interval, 30)
        if ai_interval is not None:
            _AI_INTERVAL = _sanitize_interval(ai_interval, 60)
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


async def run_market_job_once() -> Dict[str, str]:
    """Manually trigger the market data ingestion job."""
    return await _market_data_ingestion_job()


async def run_ai_job_once() -> Dict[str, str]:
    """Manually trigger the AI execution workflow job."""
    return await _execute_model_workflow_job()

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


async def _execute_model_workflow_job() -> Dict[str, str]:
    """
    Drive the signal �� routing �� risk �� execution loop for configured accounts.
    """
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

    manual_signal = _maybe_generate_liquidation_reversal(
        account=account,
        positions=positions,
        meta=meta,
    )
    response: Optional[SignalResponse] = None
    if manual_signal:
        request, response = manual_signal
        debug_entry["decision_actionable"] = True
        debug_entry["automation"] = "liquidation_reversal"
        debug_entry["notes"] = "净爆仓静默 60s 触发自动下单"
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
            debug_entry["notes"] = "缺少可交易合约"
            _record_order_debug_entry(debug_entry)
            return False, None
        liquidation_alert = _annotate_liquidation_hint(request)
        if liquidation_alert:
            debug_entry["liquidation_hint"] = liquidation_alert

    if response is None:
        logger.debug("No signal generated for %s after liquidation scan.", account_key)
        debug_entry["notes"] = "无法生成信号"
        _record_order_debug_entry(debug_entry)
        return False, None

    debug_entry["decision_actionable"] = (response.decision or "").lower() not in {"hold"}
    routed = router.route(
        response,
        market=request.market,
        account=account,
        positions=positions,
        account_meta=meta,
    )
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
    _assign_okx_position_side(routed.intent, positions)

    size_note = _align_size_to_lot(routed.intent)
    if size_note:
        debug_entry["size_adjusted"] = size_note

    with _RISK_CONFIG_LOCK:
        pyramid_cap = int(_RISK_CONFIG.get("pyramid_max_orders", 0))
    if pyramid_cap > 0:
        active_count = _count_active_orders(open_orders, routed.intent.instrument_id, routed.intent.side)
        if active_count >= pyramid_cap:
            debug_entry["notes"] = f"金字塔同向订单已达 {pyramid_cap} 单，跳过下单"
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
            debug_entry["notes"] = "缺少价格无法计算最小名义金额"
            _record_order_debug_entry(debug_entry)
            if signal_summary:
                signal_summary = f"{signal_summary} | {debug_entry['notes']}"
            else:
                signal_summary = debug_entry["notes"]
            return False, signal_summary
        if notional < min_notional:
            debug_entry["notes"] = f"名义金额 {notional:.2f} 小于最小下单金额 {min_notional:.2f}"
            _record_order_debug_entry(debug_entry)
            if signal_summary:
                signal_summary = f"{signal_summary} | {debug_entry['notes']}"
            else:
                signal_summary = debug_entry["notes"]
            return False, signal_summary

    try:
        ensure_valid_order(routed.intent, [validator])
    except OrderValidationError as exc:
        debug_entry["notes"] = f"下单校验失败: {exc}"
        _record_order_debug_entry(debug_entry)
        raise

    signal_summary = _summarize_ai_signal(response, request, routed)
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

    execution_result, placement_error = await asyncio.to_thread(_place_order, meta, routed.intent)
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
    max_position = float(meta.get("max_position", max(0.01, account.starting_equity / reference_price * 0.1)))

    risk_context = RiskContext(
        max_position=max_position,
        current_position=current_position,
        cash_available=account.cash_balance,
        notes={
            "account_id": account.account_id,
            "equity": account.equity,
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


def _maybe_generate_liquidation_reversal(
    *,
    account: Account,
    positions: Sequence[Position],
    meta: Dict[str, str],
) -> Optional[Tuple[SignalRequest, SignalResponse]]:
    """
    Scan all enabled instruments for the exhaustion pattern described on the
    liquidation dashboard and synthesize an order without consulting the LLM.
    """
    now = datetime.now(tz=timezone.utc)
    for instrument in _iter_liquidation_watch_instruments(meta):
        features = _load_market_features(instrument)
        decision = _evaluate_liquidation_reversal_state(instrument, features, now)
        if not decision:
            continue
        direction, notional_qty, event_time = decision
        request = _build_signal_request(
            model_id=account.model_id,
            meta=meta,
            account=account,
            positions=positions,
            market_price=features.get("liq_last_price") or 0.0,
            instrument_override=instrument,
        )
        if request is None:
            continue
        size = _estimate_reversal_order_size(request, direction)
        if size <= 0:
            continue
        side = "buy" if direction == "long" else "sell"
        reasoning = (
            f"{instrument} 净爆仓{('多单' if direction == 'long' else '空单')}数量 {notional_qty:.2f} "
            f"在 {int((now - event_time).total_seconds())} 秒前停止，触发自动{('抄底做多' if direction == 'long' else '逢顶做空')}。"
        )
        response = SignalResponse(
            model_id=account.model_id,
            decision="open_long" if direction == "long" else "open_short",
            confidence=0.72,
            reasoning=reasoning,
            suggested_order={
                "instrument_id": instrument,
                "side": side,
                "size": size,
            },
            raw_output={
                "automation": "liquidation_reversal",
                "instrument_id": instrument,
                "liq_net_qty": notional_qty,
                "liq_event_time": event_time.isoformat(),
            },
        )
        return request, response
    return None


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


def _evaluate_liquidation_reversal_state(
    instrument: str,
    features: Dict[str, float],
    now: datetime,
) -> Optional[Tuple[str, float, datetime]]:
    net_qty = features.get("liq_net_qty")
    timestamp_value = features.get("liq_timestamp")
    if net_qty is None or timestamp_value is None or net_qty == 0:
        return None
    abs_qty = abs(net_qty)
    if abs_qty < _LIQUIDATION_ABS_THRESHOLD:
        return None
    event_time = _coerce_timestamp(timestamp_value)
    if event_time is None:
        return None
    silence = (now - event_time).total_seconds()
    if silence < _LIQUIDATION_SILENCE_SECONDS or silence > _LIQUIDATION_MAX_EVENT_LOOKBACK:
        return None
    symbol = instrument.upper()
    state = _LIQUIDATION_REVERSAL_WATCH.setdefault(
        symbol,
        {"last_seen_time": None, "last_sign": 0, "triggered_event": None},
    )
    last_time = state.get("last_seen_time")
    if not isinstance(last_time, datetime) or event_time > last_time:
        state["last_seen_time"] = event_time
        state["last_sign"] = 1 if net_qty > 0 else -1
        state["triggered_event"] = None
    elif event_time < last_time:
        return None  # ignore stale records
    sign = 1 if net_qty > 0 else -1
    if state.get("last_sign") != sign:
        state["last_sign"] = sign
        state["triggered_event"] = None
    if state.get("triggered_event") == event_time:
        return None
    state["triggered_event"] = event_time
    return ("long" if sign > 0 else "short", abs_qty, event_time)


def _estimate_reversal_order_size(request: SignalRequest, direction: str) -> float:
    max_position = float(max(request.risk.max_position or 0.0, 0.0))
    if max_position <= 0:
        return 0.0
    current = float(request.risk.current_position or 0.0)
    if direction == "long":
        used = max(0.0, current)
    else:
        used = max(0.0, -current)
    capacity = max(0.0, max_position - used)
    if capacity <= 0:
        return 0.0
    clip = max(max_position * 0.25, 0.001)
    return round(min(capacity, clip), 6)


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


def _place_order(meta: Dict[str, str], intent: OrderIntent) -> tuple[Optional[Dict[str, object]], Optional[str]]:
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
        payload = {
            "instrument_id": intent.instrument_id,
            "side": intent.side,
            "order_type": intent.order_type,
            "size": intent.size,
            "margin_mode": _choose_margin_mode(intent.instrument_id, meta),
        }
        if intent.price:
            payload["price"] = intent.price
        meta_payload = intent.metadata if isinstance(intent.metadata, dict) else {}
        client_order_id = meta_payload.get("client_order_id")
        if client_order_id:
            payload["client_order_id"] = client_order_id
        pos_side = meta_payload.get("pos_side")
        if pos_side:
            payload["pos_side"] = pos_side
        if meta_payload.get("reduce_only"):
            payload["reduce_only"] = True
        raw_response = None
        try:
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


def _assign_okx_position_side(intent: OrderIntent, positions: Sequence[Position]) -> None:
    """Populate OKX metadata so hedge mode is disabled."""
    if intent is None or not intent.instrument_id:
        return
    net_qty = _compute_net_position(intent.instrument_id, positions)
    side = (intent.side or "buy").lower()
    if abs(net_qty) < 1e-9:
        pos_side = "long" if side == "buy" else "short"
    else:
        pos_side = "long" if net_qty > 0 else "short"
    reduce_only = (pos_side == "long" and side == "sell") or (pos_side == "short" and side == "buy")
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    metadata["pos_side"] = pos_side
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
