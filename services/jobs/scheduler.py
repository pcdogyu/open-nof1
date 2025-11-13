"""
Background scheduler for analytics refresh and automated signal execution.
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
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
from models.schemas import MarketSnapshot, RiskContext, SignalRequest
from risk.engine import RiskEngine
from risk.order_validation import BasicOrderValidator, OrderValidationError, ensure_valid_order
from risk.schemas import OrderIntent
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
from data_pipeline.pipeline import MarketDataPipeline, PipelineSettings

try:  # pragma: no cover - optional dependency
    from data_pipeline.influx import InfluxConfig
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.flux_table import FluxRecord
except ImportError:  # pragma: no cover
    InfluxConfig = None  # type: ignore
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
except ImportError:  # pragma: no cover
    OKX_ACCOUNTS = {}
    TRADABLE_INSTRUMENTS: Sequence[str] = ()
    MARKET_SYNC_INTERVAL = 60
    AI_INTERACTION_INTERVAL = 300

_SCHEDULER: Optional["AsyncIOScheduler"] = None
_FEATURE_CLIENT: Optional["InfluxDBClient"] = None
_FEATURE_CONFIG: Optional["InfluxConfig"] = None

_ANALYTICS_JOB_ID = "refresh_leaderboard_and_equity_curve"
_MARKET_JOB_ID = "market_data_sync"
_EXECUTION_JOB_ID = "execute_model_signals"


def _sanitize_interval(value: int, minimum: int, maximum: int = 3600) -> int:
    return int(max(minimum, min(maximum, value)))


_MARKET_INTERVAL = _sanitize_interval(int(MARKET_SYNC_INTERVAL), 30)
_AI_INTERVAL = _sanitize_interval(int(AI_INTERACTION_INTERVAL), 60)
_PIPELINE_SETTINGS = PipelineSettings(
    instruments=list(TRADABLE_INSTRUMENTS) if TRADABLE_INSTRUMENTS else ["BTC-USDT-SWAP"],
    signal_log_path=Path("data/logs/ai_signals_scheduler.jsonl"),
)
_PIPELINE: Optional[MarketDataPipeline] = None


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

def shutdown_scheduler() -> None:
    """Stop the scheduler when the application shuts down."""
    global _SCHEDULER, _FEATURE_CLIENT, _FEATURE_CONFIG
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


def _refresh_metrics_job() -> None:
    """Refresh leaderboard cache and append equity points for each account."""
    repository = get_account_repository()
    _refresh_leaderboard(repository)
    _record_equity_points(repository)


async def _market_data_ingestion_job() -> None:
    if not TRADABLE_INSTRUMENTS:
        logger.debug("No tradable instruments configured; skipping market ingestion job.")
        return
    pipeline = _get_pipeline()
    try:
        result = await pipeline.run_cycle(
            log=lambda message: logger.info("Market pipeline: %s", message),
        )
        logger.info(
            "Market pipeline completed (%d instruments, %d signals).",
            len(result.feature_snapshots),
            len(result.signals),
        )
    except Exception as exc:
        logger.warning("Market ingestion job failed: %s", exc)


async def _execute_model_workflow_job() -> None:
    """
    Drive the signal → routing → risk → execution loop for configured accounts.
    """
    if not OKX_ACCOUNTS:
        logger.debug("No OKX accounts configured; skipping execution job.")
        return

    repository = get_account_repository()
    registry = get_portfolio_registry()
    router = SignalRouter(
        registry,
        okx_accounts=OKX_ACCOUNTS,
        instrument_fallbacks=TRADABLE_INSTRUMENTS,
    )
    risk_engine = RiskEngine()
    validator = BasicOrderValidator(
        instrument_allowlist=set(TRADABLE_INSTRUMENTS) if TRADABLE_INSTRUMENTS else None
    )

    async with SignalRuntime() as runtime:
        for account_key, meta in OKX_ACCOUNTS.items():
            if not meta.get("enabled", True):
                logger.debug("Account %s disabled via config; skipping", account_key)
                continue
            model_id = meta.get("model_id")
            if not model_id:
                logger.warning("OKX account %s missing model_id; skipping", account_key)
                continue
            try:
                await _process_account_signal(
                    account_key=account_key,
                    meta=meta,
                    runtime=runtime,
                    router=router,
                    validator=validator,
                    risk_engine=risk_engine,
                    repository=repository,
                )
            except OrderValidationError as exc:
                logger.info("Order validation failed for %s: %s", account_key, exc)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Execution pipeline failed for %s: %s", account_key, exc)


async def _process_account_signal(
    *,
    account_key: str,
    meta: Dict[str, str],
    runtime: SignalRuntime,
    router: SignalRouter,
    validator: BasicOrderValidator,
    risk_engine: RiskEngine,
    repository: AccountRepository,
) -> None:
    snapshot = await asyncio.to_thread(_collect_okx_snapshot, account_key, meta)
    if snapshot is None:
        return
    account, balances, positions, trades, open_orders, market_price = snapshot

    _persist_snapshot(
        repository,
        account=account,
        balances=balances,
        positions=positions,
        trades=trades,
        open_orders=open_orders,
    )

    request = _build_signal_request(
        model_id=account.model_id,
        meta=meta,
        account=account,
        positions=positions,
        market_price=market_price,
    )
    if request is None:
        logger.debug("Unable to build signal request for %s; skipping", account_key)
        return

    response = await runtime.generate_signal(request)
    routed = router.route(
        response,
        market=request.market,
        account=account,
        positions=positions,
        account_meta=meta,
    )
    if not routed:
        logger.debug("Router did not produce executable order for %s", account_key)
        return

    ensure_valid_order(routed.intent, [validator])

    evaluation = risk_engine.evaluate(routed.intent, routed.market, routed.portfolio_metrics)
    if not evaluation.approved:
        logger.info(
            "Risk engine rejected order for %s: %s",
            account_key,
            [violation.message for violation in evaluation.violations],
        )
        return

    execution_result = await asyncio.to_thread(_place_order, meta, routed.intent)
    if execution_result is None:
        return

    _record_submitted_order(repository, routed, execution_result)
    logger.info(
        "Submitted order %s for %s (%s %s @ %s)",
        execution_result.get("order_id"),
        account_key,
        routed.intent.side,
        routed.intent.size,
        routed.intent.price or "MKT",
    )


def _collect_okx_snapshot(
    account_key: str,
    meta: Dict[str, str],
) -> Optional[Tuple[Account, List[Balance], List[Position], List[Trade], List[OrderModel], float]]:
    account_id = meta.get("account_id") or f"okx_{account_key}"
    model_id = meta.get("model_id", account_key)
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
        fills = client.fetch_fills(limit=100)
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

    market_price = _infer_market_price(positions, trades, meta)

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
) -> Optional[SignalRequest]:
    instrument = (
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

    market_snapshot = MarketSnapshot(
        instrument_id=instrument,
        price=reference_price,
        spread=spread,
        volume_24h=features.get("15m_volume_sum"),
        metadata={
            "source": "scheduler",
            "account_id": account.account_id,
            **{k: v for k, v in features.items() if v is not None},
        },
    )

    return SignalRequest(
        model_id=model_id,
        market=market_snapshot,
        risk=risk_context,
        positions=_summarize_positions_for_ai(positions),
        strategy_hint=meta.get("strategy_hint"),
        metadata={"account_id": account.account_id},
    )


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


def _place_order(meta: Dict[str, str], intent: OrderIntent) -> Optional[Dict[str, object]]:
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
        return None

    try:
        payload = {
            "instrument_id": intent.instrument_id,
            "side": intent.side,
            "order_type": intent.order_type,
            "size": intent.size,
        }
        if intent.price:
            payload["price"] = intent.price
        if intent.metadata.get("client_order_id"):
            payload["client_order_id"] = intent.metadata["client_order_id"]
        return client.place_order(payload)
    except OkxClientError as exc:
        logger.error("[%s] Order placement failed: %s", account_id, exc)
        return None
    finally:
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
        features = _flux_tables_to_dict(query_api.query(micro_query))
        indicator_vals = _flux_tables_to_dict(query_api.query(indicator_query))
        for key, value in indicator_vals.items():
            features[f"15m_{key}"] = value
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


def _flux_tables_to_dict(tables: Iterable[object]) -> Dict[str, float]:
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
                values[field] = numeric
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
    return 100.0


def _infer_instrument_from_positions(positions: Sequence[Position]) -> Optional[str]:
    if not positions:
        return None
    return positions[0].instrument_id


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
