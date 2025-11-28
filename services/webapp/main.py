# Trigger reloader
"""
Entrypoint for the open-nof1.ai user-facing web service.
"""

from __future__ import annotations

import asyncio
import html
import logging
import urllib.parse
from datetime import datetime, timedelta, timezone
from string import Template
from typing import Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Form, status, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

from services.jobs.scheduler import shutdown_scheduler, start_scheduler
from services.webapp import routes
from websocket import start_streams as start_ws_streams, stop_streams as stop_ws_streams

try:
    SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
except Exception:  # pragma: no cover - fallback when tzdata missing
    SHANGHAI_TZ = timezone(timedelta(hours=8))


app = FastAPI(
    title="open-nof1.ai",
    description="LLM-driven trading operations control plane",
    version="0.1.0",
)

logger = logging.getLogger(__name__)

app.include_router(routes.router)


@app.on_event("startup")
async def _startup() -> None:
    start_scheduler()
    try:
        settings = routes.get_pipeline_settings()
        instruments = settings.get("tradable_instruments", [])
        await start_ws_streams(instruments)
    except Exception as exc:
        logger.warning("Failed to start websocket streams: %s", exc)
    # Warm orderbook cache from local Influx first, then refresh live data asynchronously.
    try:
        routes.prime_orderbook_cache()
    except Exception as exc:
        logger.debug("Orderbook cache warm-up skipped: %s", exc)
    try:
        asyncio.create_task(routes.refresh_orderbook_live_async())
    except Exception as exc:
        logger.debug("Unable to schedule live orderbook refresh: %s", exc)


@app.on_event("shutdown")
async def _shutdown() -> None:
    shutdown_scheduler()
    try:
        await stop_ws_streams()
    except Exception as exc:
        logger.warning("Failed to stop websocket streams cleanly: %s", exc)


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
def model_dashboard() -> HTMLResponse:
    metrics = routes.get_model_metrics()
    return HTMLResponse(content=_render_dashboard(metrics))


@app.get("/models", include_in_schema=False, response_class=HTMLResponse)
def model_manager() -> HTMLResponse:
    catalog = routes.get_model_catalog()
    return HTMLResponse(content=_render_model_manager(catalog))


@app.get("/okx", include_in_schema=False, response_class=HTMLResponse)
def okx_dashboard(request: Request) -> HTMLResponse:
    refresh_flag = str(request.query_params.get("refresh", "")).lower()
    force_refresh = refresh_flag in ("1", "true", "yes", "y")
    order_status = request.query_params.get("order_status")
    order_detail = request.query_params.get("detail")
    if order_status:
        order_info = {
            "account": request.query_params.get("order_account"),
            "instrument": request.query_params.get("order_instrument"),
            "side": request.query_params.get("order_side"),
            "order_type": request.query_params.get("order_type"),
            "size": request.query_params.get("order_size"),
            "price": request.query_params.get("order_price"),
            "margin_mode": request.query_params.get("order_margin_mode"),
        }
    else:
        order_info = None
    summary = routes.get_okx_summary(force_refresh=force_refresh)
    return HTMLResponse(
        content=_render_okx_dashboard(summary, order_status, order_detail, order_info)
    )


@app.post("/okx/manual-order", include_in_schema=False)
def okx_manual_order(
    account_id: str = Form(...),
    instrument_id: str = Form(...),
    side: str = Form(...),
    order_type: str = Form(...),
    size: float = Form(...),
    price: Optional[float] = Form(None),
    margin_mode: Optional[str] = Form(None),
) -> RedirectResponse:
    def _fmt_decimal(value: float) -> str:
        text = f"{value:.8f}"
        text = text.rstrip("0").rstrip(".")
        return text or "0"

    def _margin_label(value: str | None) -> str:
        normalized = (value or "").strip().lower()
        mapping = {
            "": "自动",
            "cross": "全仓",
            "isolated": "逐仓",
            "cash": "现货",
        }
        return mapping.get(normalized, normalized or "自动")

    normalized_account = account_id.strip()
    normalized_instrument = instrument_id.strip().upper()
    normalized_side = side.strip().lower()
    normalized_type = order_type.strip().lower()
    display_side = "买入" if normalized_side == "buy" else "卖出"
    display_type = "限价" if normalized_type == "limit" else "市价"
    size_text = _fmt_decimal(size)
    price_text = "市价" if price is None else _fmt_decimal(price)
    amount_value = None if price is None else size * price
    amount_text = f"{amount_value:.2f} USD" if amount_value is not None else "市价撮合"
    margin_label = _margin_label(margin_mode)
    info_params = {
        "order_account": normalized_account,
        "order_instrument": normalized_instrument,
        "order_side": display_side,
        "order_type": display_type,
        "order_size": size_text,
        "order_price": price_text,
        "order_margin_mode": margin_label,
    }

    def _build_redirect(status_value: str, detail_value: str) -> RedirectResponse:
        params = {
            "refresh": "1",
            "order_status": status_value,
            "detail": detail_value,
            **info_params,
        }
        query = urllib.parse.urlencode(params)
        return RedirectResponse(
            url=f"/okx?{query}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    try:
        result = routes.place_manual_okx_order(
            account_id=normalized_account,
            instrument_id=normalized_instrument,
            side=side.strip(),
            order_type=order_type.strip(),
            size=size,
            price=price,
            margin_mode=(margin_mode or "").strip() or None,
        )
        order_id_value = result.get("order_id")
        core_detail = (
            f"{display_side}{normalized_instrument} 数量 {size_text} 张，价格 {price_text}，金额 {amount_text}"
        )
        if order_id_value:
            success_detail = f"订单ID {order_id_value} · {core_detail}"
        else:
            success_detail = core_detail
        return _build_redirect("success", success_detail)
    except Exception as exc:
        error_detail = (
            f"{display_side}{normalized_instrument} 数量 {size_text} 张 下单失败：{exc}"
        )
        return _build_redirect("error", error_detail)


@app.post("/okx/close-position", include_in_schema=False)
def okx_close_position(
    account_id: str = Form(...),
    instrument_id: str = Form(...),
    position_side: str = Form(...),
    quantity: float = Form(...),
    margin_mode: Optional[str] = Form(None),
    action_label: str = Form(""),
    reference_price: str = Form(""),
    side_display: str = Form(""),
) -> RedirectResponse:
    def _fmt_decimal(value: float) -> str:
        text = f"{value:.8f}"
        text = text.rstrip("0").rstrip(".")
        return text or "0"

    instrument_display = instrument_id.strip().upper()
    action_text = (action_label or "").strip()
    if not action_text:
        action_text = "平仓"
    side_label = (side_display or "").strip()
    if not side_label:
        normalized = position_side.strip().lower()
        if normalized in {"long", "buy"}:
            side_label = "多单"
        elif normalized in {"short", "sell"}:
            side_label = "空单"
        else:
            side_label = position_side
    qty_text = _fmt_decimal(quantity)
    reference_value: Optional[float]
    try:
        reference_value = float(reference_price) if reference_price else None
    except ValueError:
        reference_value = None
    if reference_value is not None and reference_value <= 0:
        reference_value = None
    if reference_value is not None:
        price_text = _fmt_decimal(reference_value)
        amount_value = reference_value * quantity
        amount_text = f"{amount_value:.2f} USD"
    else:
        price_text = "市价"
        amount_text = "市价撮合"

    def _build_detail(order_id: object | None, note: str | None = None) -> str:
        core = (
            f"{instrument_display} · {side_label} · {action_text} · 数量 {qty_text} 张 · 名义 {amount_text}"
        )
        if reference_value is not None:
            core += f" · 参考价 {price_text}"
        if note:
            core += f" · {note}"
        if order_id:
            return f"订单ID {order_id} · {core}"
        return core

    try:
        result = routes.close_okx_position(
            account_id=account_id.strip(),
            instrument_id=instrument_display,
            position_side=position_side.strip(),
            quantity=quantity,
            margin_mode=(margin_mode or "").strip() or None,
        )
        note_parts: list[str] = []
        status_text = (result.get("status") or "").strip()
        if status_text:
            note_parts.append(f"state={status_text}")
        if result.get("code"):
            note_parts.append(f"code={result.get('code')}")
        raw_message = result.get("message") or (result.get("raw") or {}).get("msg")
        if raw_message:
            note_parts.append(str(raw_message))
        note_text = " ".join(note_parts) if note_parts else None
        detail = _build_detail(result.get("order_id"), note_text)
        detail = _build_detail(result.get("order_id"), note_text)
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=success&detail={urllib.parse.quote_plus(detail)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except Exception as exc:
        error_detail = _build_detail(None, f"错误 {exc}")
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=error&detail={urllib.parse.quote_plus(error_detail)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

@app.post("/okx/scale-position", include_in_schema=False)
def okx_scale_position(
    account_id: str = Form(...),
    instrument_id: str = Form(...),
    position_side: str = Form(...),
    quantity: float = Form(...),
    margin_mode: Optional[str] = Form(None),
) -> RedirectResponse:
    try:
        result = routes.scale_okx_position(
            account_id=account_id.strip(),
            instrument_id=instrument_id.strip(),
            position_side=position_side.strip(),
            quantity=quantity,
            margin_mode=(margin_mode or "").strip() or None,
        )
        detail = result.get("order_id") or ""
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=success&detail={urllib.parse.quote_plus(str(detail))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=error&detail={urllib.parse.quote_plus(str(exc))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )


@app.post("/okx/cancel-order", include_in_schema=False)
def okx_cancel_order(
    account_id: str = Form(...),
    instrument_id: str = Form(...),
    order_id: str = Form(...),
) -> RedirectResponse:
    try:
        result = routes.cancel_okx_order(
            account_id=account_id.strip(),
            instrument_id=instrument_id.strip(),
            order_id=order_id.strip(),
        )
        detail = result.get("order_id") or ""
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=success&detail={urllib.parse.quote_plus(str(detail))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/okx?refresh=1&order_status=error&detail={urllib.parse.quote_plus(str(exc))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )


@app.get("/liquidations", include_in_schema=False, response_class=HTMLResponse)
def liquidation_dashboard(request: Request) -> HTMLResponse:
    instrument = request.query_params.get("instrument")
    snapshot = routes.get_liquidation_snapshot(limit=50, instrument=instrument)
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments", [])
    return HTMLResponse(content=_render_liquidation_page(snapshot, instruments))


@app.get("/orderbook", include_in_schema=False, response_class=HTMLResponse)
def orderbook_dashboard(request: Request) -> HTMLResponse:
    instrument = request.query_params.get("instrument")
    levels_param = request.query_params.get("levels", "400")
    response_format = (request.query_params.get("format") or "").strip().lower()
    try:
        levels = max(1, min(int(levels_param), 400))
    except ValueError:
        levels = 400
    snapshot = routes.get_orderbook_snapshot(levels=levels, instrument=instrument)
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments", [])
    if response_format == "json":
        return JSONResponse(content=snapshot)
    return HTMLResponse(content=_render_orderbook_page(snapshot, instruments, levels))


@app.get("/liquidation-map", include_in_schema=False, response_class=HTMLResponse)
def liquidation_map_dashboard() -> HTMLResponse:
    default_instrument = "ETH-USDT-SWAP"
    snapshot = routes.get_orderbook_snapshot(
        levels=400,
        instrument=default_instrument,
        force_live=True,
    )
    instrument_payload: dict | None = None
    for item in snapshot.get("items", []):
        inst_id = (item.get("instrument_id") or "").upper()
        if inst_id == default_instrument:
            instrument_payload = item
            break
    if instrument_payload is None and snapshot.get("items"):
        instrument_payload = snapshot["items"][0]

    def _to_float(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    last_price = _to_float((instrument_payload or {}).get("last_price"))
    best_bid = _to_float((instrument_payload or {}).get("best_bid"))
    best_ask = _to_float((instrument_payload or {}).get("best_ask"))
    if last_price is None:
        if best_bid is not None and best_ask is not None:
            last_price = (best_bid + best_ask) / 2.0
        elif best_bid is not None:
            last_price = best_bid
        elif best_ask is not None:
            last_price = best_ask
        else:
            bids = (instrument_payload or {}).get("bids") or []
            asks = (instrument_payload or {}).get("asks") or []
            for level in bids + asks:
                if isinstance(level, (list, tuple)) and level:
                    maybe_price = _to_float(level[0])
                    if maybe_price is not None:
                        last_price = maybe_price
                        break
    instrument_timestamp = (instrument_payload or {}).get("timestamp") or snapshot.get("updated_at")
    formatted_timestamp = _format_asia_shanghai(instrument_timestamp)
    last_price_display = _format_number(last_price, digits=2) if last_price is not None else "--"
    display_pair = (default_instrument.replace("-SWAP", "").replace("-", "") or default_instrument).upper()
    return HTMLResponse(
        content=_render_liquidation_map_page(
            instrument=default_instrument,
            display_pair=display_pair,
            last_price=last_price_display,
            updated_at=formatted_timestamp,
        )
    )


@app.get("/settings", include_in_schema=False, response_class=HTMLResponse)
def pipeline_settings_page() -> HTMLResponse:
    settings = routes.get_pipeline_settings()
    catalog = routes.get_instrument_catalog(limit=300)
    risk_settings = routes.get_risk_settings()
    return HTMLResponse(content=_render_settings_page(settings, catalog, risk_settings))


@app.post("/settings/update", include_in_schema=False)
async def submit_pipeline_settings(
    request: Request,
    poll_interval: int = Form(...),
    instruments: str = Form(""),
) -> RedirectResponse:
    instrument_list = _parse_instrument_input(instruments)
    form_data = await request.form()
    overrides = _collect_override_payload(form_data, instrument_list)
    routes.update_pipeline_settings(instrument_list, poll_interval, overrides)
    return RedirectResponse(url="/settings", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/settings/add", include_in_schema=False)
def add_pipeline_instrument(
    new_instrument: str = Form(""),
    poll_interval: int = Form(...),
    current_instruments: str = Form(""),
) -> RedirectResponse:
    if current_instruments.strip():
        instrument_list = _parse_instrument_input(current_instruments)
    else:
        # Fallback to server-side settings so add operations never rely on stale form data.
        snapshot = routes.get_pipeline_settings()
        existing = snapshot.get("tradable_instruments") or []
        instrument_list = [str(item) for item in existing]
    normalized_new = (new_instrument or "").strip()
    if normalized_new:
        instrument_list.append(normalized_new)
    routes.update_pipeline_settings(instrument_list, poll_interval)
    return RedirectResponse(url="/settings", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/orders/debug", include_in_schema=False, response_class=HTMLResponse)
def order_debug_page() -> HTMLResponse:
    entries = routes.get_order_debug_status()
    return HTMLResponse(content=_render_order_debug_page(entries))


@app.get("/risk", include_in_schema=False, response_class=HTMLResponse)
def risk_settings_page() -> HTMLResponse:
    settings = routes.get_risk_settings()
    return HTMLResponse(content=_render_risk_page(settings))


@app.post("/risk/update", include_in_schema=False)
def submit_risk_settings(
    price_tolerance_pct: float = Form(...),
    max_drawdown_pct: float = Form(...),
    max_loss_absolute: float = Form(...),
    cooldown_seconds: int = Form(...),
    min_notional_usd: float = Form(0),
    max_order_notional_usd: float = Form(0),
    max_position: float = Form(0),
    take_profit_pct: float = Form(0),
    stop_loss_pct: float = Form(0),
    position_take_profit_pct: float = Form(5),
    position_stop_loss_pct: float = Form(3),
    default_leverage: int = Form(1),
    max_leverage: int = Form(125),
    pyramid_max_orders: int = Form(5),
    pyramid_reentry_pct: float = Form(2),
    liquidation_notional_threshold: float = Form(50000),
    liquidation_same_direction_count: int = Form(4),
    liquidation_opposite_count: int = Form(3),
    liquidation_silence_seconds: int = Form(300),
) -> RedirectResponse:
    routes.update_risk_settings(
        price_tolerance_pct=price_tolerance_pct / 100.0,
        max_drawdown_pct=max_drawdown_pct,
        max_loss_absolute=max_loss_absolute,
        cooldown_seconds=cooldown_seconds,
        min_notional_usd=min_notional_usd,
        max_order_notional_usd=max_order_notional_usd,
        max_position=max_position,
        take_profit_pct=take_profit_pct,
        stop_loss_pct=stop_loss_pct,
        position_take_profit_pct=position_take_profit_pct,
        position_stop_loss_pct=position_stop_loss_pct,
        default_leverage=default_leverage,
        max_leverage=max_leverage,
        pyramid_max_orders=pyramid_max_orders,
        pyramid_reentry_pct=pyramid_reentry_pct,
        liquidation_notional_threshold=liquidation_notional_threshold,
        liquidation_same_direction_count=liquidation_same_direction_count,
        liquidation_opposite_count=liquidation_opposite_count,
        liquidation_silence_seconds=liquidation_silence_seconds,
    )
    return RedirectResponse(url="/risk", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/settings/refresh-catalog", include_in_schema=False)
async def refresh_instrument_catalog_action() -> RedirectResponse:
    await routes.refresh_instrument_catalog()
    return RedirectResponse(url="/settings", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/scheduler", include_in_schema=False, response_class=HTMLResponse)
def scheduler_settings_page() -> HTMLResponse:
    scheduler_settings = routes.get_scheduler_settings()
    return HTMLResponse(content=_render_scheduler_page(scheduler_settings))


@app.post("/scheduler/update", include_in_schema=False)
def submit_scheduler_settings(
    market_interval: int = Form(...),
    ai_interval: int = Form(...),
    liquidation_interval: int = Form(...),
) -> RedirectResponse:
    routes.update_scheduler_settings(market_interval, ai_interval, liquidation_interval)
    return RedirectResponse(url="/scheduler", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/prompts", include_in_schema=False, response_class=HTMLResponse)
def prompt_template_page(request: Request) -> HTMLResponse:
    template = routes.get_prompt_template_text()
    saved_flag = str(request.query_params.get("saved", "")).lower() in ("1", "true", "yes")
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments") or []
    models = routes.get_model_catalog()
    content = _render_prompt_editor(
        template,
        instruments=instruments,
        models=models,
        saved=saved_flag,
    )
    return HTMLResponse(content=content)


@app.post("/prompts/update", include_in_schema=False)
def update_prompt_template(prompt_template: str = Form(...)) -> RedirectResponse:
    routes.update_prompt_template_text(prompt_template)
    return RedirectResponse(url="/prompts?saved=1", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/models/update", include_in_schema=False)
def update_model_settings(
    model_id: str = Form(...),
    api_key: Optional[str] = Form(""),
    enabled: Optional[str] = Form(None),
    has_existing_key: str = Form("0"),
    mask_length: str = Form("0"),
) -> RedirectResponse:
    normalized_api_key = (api_key or "").strip()
    enabled_flag = enabled not in (None, "", "0", "false", "off", "False")
    try:
        mask_len_int = int(mask_length)
    except ValueError:
        mask_len_int = 0
    keep_existing = (
        has_existing_key == "1"
        and normalized_api_key != ""
        and set(normalized_api_key) == {"*"}
        and len(normalized_api_key) == mask_len_int
    )
    if keep_existing:
        routes.update_model_config(
            model_id=model_id,
            enabled=enabled_flag,
            api_key=None,
        )
    else:
        routes.update_model_config(
            model_id=model_id,
            enabled=enabled_flag,
            api_key=normalized_api_key,
        )
    return RedirectResponse(url="/models", status_code=status.HTTP_303_SEE_OTHER)


def _render_dashboard(metrics: dict) -> str:
    """Render the monitoring dashboard HTML."""
    esc = _escape
    as_of = esc(_format_asia_shanghai(metrics.get("as_of")))

    model_rows = [
        (
            "<tr>"
            f"<td>{esc(row.get('model_id'))}</td>"
            f"<td>{esc(row.get('portfolio_id'))}</td>"
            f"<td>{esc(row.get('sharpe_ratio'))}</td>"
            f"<td>{esc(row.get('max_drawdown_pct'))}</td>"
            f"<td>{esc(row.get('win_rate_pct'))}</td>"
            f"<td>{esc(row.get('avg_trade_duration_min'))}</td>"
            f"<td>{esc(row.get('exposure_usd'))}</td>"
            f"<td>{esc(row.get('open_positions'))}</td>"
            "</tr>"
        )
        for row in metrics.get("models", [])
    ]
    if not model_rows:
        model_rows.append("<tr><td colspan='8'>暂无模型数据</td></tr>")

    trade_rows: list[str] = []
    for trade in metrics.get("recent_trades", []):
        pnl = trade.get("realized_pnl")
        if pnl is None:
            pnl = trade.get("pnl", 0)
        pnl_class = "pnl-positive" if (pnl or 0) >= 0 else "pnl-negative"
        quantity = trade.get("quantity", trade.get("size"))
        price = trade.get("price", trade.get("entry_price"))
        exit_price = (
            trade.get("close_price")
            or trade.get("exit_price")
            or price
        )
        trade_rows.append(
            "<tr>"
            f"<td>{esc(_format_asia_shanghai(trade.get('executed_at')))}</td>"
            f"<td>{esc(trade.get('model_id'))}</td>"
            f"<td>{esc(trade.get('account_id') or trade.get('portfolio_id'))}</td>"
            f"<td>{esc(trade.get('instrument_id'))}</td>"
            f"<td>{esc(trade.get('side'))}</td>"
            f"<td>{esc(quantity)}</td>"
            f"<td>{esc(price)}</td>"
            f"<td>{esc(exit_price)}</td>"
            f"<td class='{pnl_class}'>{esc(pnl)}</td>"
            "</tr>"
        )
    if not trade_rows:
        rows.append("<tr><td colspan='9'>暂无成交记录</td></tr>")
    enabled_instruments = metrics.get("enabled_instruments") or []
    recent_signals = metrics.get("recent_ai_signals", [])
    latest_by_instrument: dict[str, dict] = {}
    instrument_filter_set = {inst for inst in enabled_instruments if inst}
    filter_enabled = bool(instrument_filter_set)
    for signal in recent_signals:
        instrument_id = signal.get("instrument_id")
        if not instrument_id:
            continue
        if filter_enabled and instrument_id not in instrument_filter_set:
            continue
        if instrument_id in latest_by_instrument:
            continue
        latest_by_instrument[instrument_id] = signal

    instruments_to_show = list(enabled_instruments)
    if not instruments_to_show and not filter_enabled and latest_by_instrument:
        instruments_to_show = list(latest_by_instrument.keys())

    signal_rows: list[str] = []
    for instrument in instruments_to_show:
        signal = latest_by_instrument.get(instrument)
        if signal:
            timestamp = _format_asia_shanghai(signal.get("timestamp") or signal.get("generated_at"))
            model_id = signal.get("model_id")
            action_zh = signal.get("action_zh") or "未知"
            action_en = signal.get("action_en") or "Unknown"
            reason = signal.get("reason_zh") or signal.get("reason_en") or ""
            hold_action = _is_hold_action(action_zh, action_en)
            confidence = _format_number(signal.get("confidence"), 2)
            order_display = _format_order(signal.get("order"), hold_action=hold_action)
        else:
            timestamp = "-"
            model_id = "-"
            action_zh = "暂无"
            action_en = "N/A"
            reason = "等待信号"
            hold_action = False
            confidence = "-"
            order_display = _format_order(None)
        signal_rows.append(
            "<tr>"
            f"<td>{esc(timestamp)}</td>"
            f"<td>{esc(model_id)}</td>"
            f"<td>{esc(instrument or '-')}</td>"
            f"<td>{esc(action_zh)} / {esc(action_en)}</td>"
            f"<td>{confidence}</td>"
            f"<td>{esc(reason)}</td>"
            f"<td>{order_display}</td>"
            "</tr>"
        )
    if not signal_rows:
        signal_rows.append("<tr><td colspan='7' class='empty-state'>请在设置中配置交易对以查看 AI 信号。</td></tr>")

    return HTML_TEMPLATE.format(
        as_of=as_of,
        model_rows="\n".join(model_rows),
        trade_rows="\n".join(trade_rows),
        signal_rows="\n".join(signal_rows),
    )


def _render_model_manager(catalog: list[dict]) -> str:
    """Render the model management UI including toggles and masked API keys."""
    esc = _escape
    cards: list[str] = []
    for entry in catalog:
        model_id = esc(entry.get("model_id"))
        display_name = esc(entry.get("display_name", entry.get("model_id")))
        provider = esc(entry.get("provider", "未知来源"))
        last_updated = esc(_format_asia_shanghai(entry.get("last_updated")))
        raw_api_key = entry.get("api_key") or ""
        masked_value = "*" * len(raw_api_key) if raw_api_key else ""
        masked_value_html = esc(masked_value)
        placeholder_text = "请输入模型 API Key" if not raw_api_key else "已配置，修改请重新输入"
        placeholder_html = esc(placeholder_text)
        enabled = bool(entry.get("enabled"))
        checked_attr = "checked" if enabled else ""

        cards.append(
            """
            <section class=\"model-card\">
                <header>
                    <h2>{display_name}</h2>
                    <p class=\"model-meta\">模型ID：{model_id} · 提供方：{provider}</p>
                </header>
                <form method=\"post\" action=\"/models/update\">
                    <input type=\"hidden\" name=\"model_id\" value=\"{model_id}\">
                    <input type=\"hidden\" name=\"has_existing_key\" value=\"{has_key}\">
                    <input type=\"hidden\" name=\"mask_length\" value=\"{mask_length}\">
                    <div class=\"form-row toggle-row\">
                        <label class=\"toggle\">
                            <input type=\"checkbox\" name=\"enabled\" value=\"on\" {checked}>
                            <span>启用模型</span>
                        </label>
                    </div>
                    <div class=\"form-row\">
                        <label class=\"field-label\" for=\"api-{model_id}\">API Key</label>
                        <input id=\"api-{model_id}\" type=\"password\" name=\"api_key\" value=\"{masked_value}\" placeholder=\"{placeholder}\" autocomplete=\"new-password\" oncopy=\"return false;\" oncut=\"return false;\" ondragstart=\"return false;">
                    </div>
                    <div class=\"form-actions\">
                        <button type=\"submit\">保存设置</button>
                    </div>
                </form>
                <p class=\"last-updated\">最近更新：{last_updated}</p>
            </section>
            """.format(
                display_name=display_name,
                model_id=model_id,
                provider=provider,
                checked=checked_attr,
                has_key="1" if raw_api_key else "0",
                mask_length=str(len(raw_api_key)),
                masked_value=masked_value_html,
                placeholder=placeholder_html,
                last_updated=last_updated,
            )
        )

    if not cards:
        cards_html = "<p class='empty-state'>暂无可用深度数据</p>"
    else:
        cards_html = "\n".join(cards)

    return MODEL_MANAGER_TEMPLATE.format(models_html=cards_html)



def _render_order_info_card(info: dict[str, str | None] | None) -> str:
    if not info:
        return ""
    esc = _escape
    rows: list[str] = []
    for label, key in (
        ("账户", "account"),
        ("合约", "instrument"),
        ("方向", "side"),
        ("类型", "order_type"),
        ("数量", "size"),
        ("价格", "price"),
        ("保证金模式", "margin_mode"),
    ):
        value = info.get(key)
        if value:
            rows.append(
                "<div class='order-info-row'>"
                f"<span>{label}</span>"
                f"<strong>{esc(value)}</strong>"
                "</div>"
            )
    if not rows:
        return ""
    return (
        "<section class='order-info-card'>"
        "<h3>下单信息</h3>"
        "<div class='order-info-grid'>"
        f"{''.join(rows)}"
        "</div>"
        "</section>"
    )


def _render_okx_dashboard(
    summary: dict,
    order_status: str | None = None,
    order_detail: str | None = None,
    order_info: dict[str, str | None] | None = None,
) -> str:
    """Render the OKX paper trading overview page."""
    esc = _escape
    as_of = esc(_format_asia_shanghai(summary.get("as_of")))
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments", [])

    error_block = ""
    errors = summary.get("sync_errors", []) or []
    if errors:
        error_items = []
        for item in errors:
            error_items.append(
                "<li>"
                f"账户：{esc(item.get('account_id')) or '-'} · 错误：{esc(item.get('message'))}"
                "</li>"
            )
        error_block = (
            "<section class='error-card'>"
            "<h2>实时同步告警</h2>"
            f"<ul>{''.join(error_items)}</ul>"
            "</section>"
        )

    flash_block = ""
    if order_status:
        kind = "success" if order_status == "success" else "error"
        message_text = esc(order_detail) if order_detail else ("操作成功" if kind == "success" else "操作失败")
        flash_block = (
            f"<div class='flash {kind}'>"
            f"{message_text}"
            "</div>"
        )
    order_info_card = _render_order_info_card(order_info)

    account_sections: list[str] = []
    for bundle in summary.get("accounts", []):
        account = bundle.get("account", {}) or {}
        raw_account_id = account.get("account_id")
        account_id_display = esc(raw_account_id)
        model_id = esc(account.get("model_id"))
        equity = _format_number(account.get("equity"))
        cash_balance = _format_number(account.get("cash_balance"))
        pnl_value = account.get("pnl")
        pnl = _format_number(pnl_value)
        pnl_class = "pnl-positive" if (pnl_value or 0) >= 0 else "pnl-negative"
        starting_equity = _format_number(account.get("starting_equity"))

        balances_html = _render_balances_table(bundle.get("balances", []))
        positions_html = _render_positions_table(bundle.get("positions", []), account_id=raw_account_id)
        trades_html = _render_trades_table(bundle.get("recent_trades", []))
        orders_html = _render_orders_table(bundle.get("open_orders", []), account_id=raw_account_id)
        curve_html = _render_equity_curve(bundle.get("equity_curve", []))
        close_all_html = _render_close_all_button(raw_account_id)

        account_sections.append(
            f"""
            <section class="okx-card">
                <header>
                    <h2>{account_id_display or 'δ�����˻�'}</h2>
                    <table class='dense summary-table'>
                        <tr>
                            <th>模型</th>
                            <th>初始资金</th>
                            <th>当前权益</th>
                            <th>现金余额</th>
                            <th>累计盈亏</th>
                        </tr>
                        <tr>
                            <td>{model_id or '-'}</td>
                            <td>{starting_equity} USD</td>
                            <td>{equity} USD</td>
                            <td>{cash_balance} USD</td>
                            <td><span class="{pnl_class}">{pnl} USD</span></td>
                        </tr>
                    </table>
                </header>
                <div class="panel">
                    <div class="panel-head">
                        <h3>持仓明细</h3>
                        {close_all_html}
                    </div>
                    {positions_html}
                </div>
                <div class="panel">
                    <h3>挂单列表</h3>
                    {orders_html}
                </div>
                <div class="split">
                    <div class="panel">
                        <h3>余额信息</h3>
                        {balances_html}
                    </div>
                    <div class="panel recent-trades-panel">
                        <h3>近期成交</h3>
                        {trades_html}
                    </div>
                </div>
                <div class="panel">
                    <h3>资产曲线</h3>
                    {curve_html}
                </div>
            </section>
            """
        )

    if not account_sections:
        account_sections.append("<p class='empty-state'>暂无 OKX 模拟账户数据。</p>")

    # Build account options for manual order form
    option_items: list[str] = []
    for bundle in summary.get("accounts", []):
        account_id = esc(bundle.get("account", {}).get("account_id") or "")
        if not account_id:
            continue
        model_id = esc(bundle.get("account", {}).get("model_id") or "-")
        option_items.append(f'<option value="{account_id}">{account_id} · 模型 {model_id}</option>')
    options_html = "".join(option_items) if option_items else '<option value="" disabled selected>暂无账户</option>'
    manual_instrument_options = _build_manual_instrument_options(instruments)

    return OKX_TEMPLATE.format(
        as_of=as_of,
        error_block=error_block,
        flash_block=flash_block,
        order_info_card=order_info_card,
        manual_account_options=options_html,
        manual_instrument_options=manual_instrument_options,
        account_sections="\n".join(account_sections),
    )


def _render_liquidation_page(snapshot: dict, instruments: Sequence[str]) -> str:
    esc = _escape
    instrument_options = _build_instrument_options(instruments, snapshot.get("instrument"))
    rows = _render_liquidation_rows(snapshot.get("items") or [])
    return LIQUIDATION_TEMPLATE.format(
        instrument_options=instrument_options,
        updated_at=esc(_format_asia_shanghai(snapshot.get("updated_at"))),
        rows=rows,
    )


def _render_orderbook_page(snapshot: dict, instruments: Sequence[str], levels: int) -> str:
    esc = _escape
    instrument_options = _build_instrument_options(instruments, snapshot.get("instrument"))
    level_options = _build_depth_options(levels)
    items = snapshot.get("items") or []
    cards = _render_orderbook_cards(items, levels)
    item_count = len(items)
    if item_count == 1:
        grid_class = "book-grid single-card"
    elif 0 < item_count <= 2:
        grid_class = "book-grid two-cols"
    else:
        grid_class = "book-grid"
    return ORDERBOOK_TEMPLATE.format(
        instrument_options=instrument_options,
        level_options=level_options,
        updated_at=esc(_format_asia_shanghai(snapshot.get("updated_at"))),
        cards=cards,
        levels=levels,
        book_grid_class=grid_class,
    )


def _render_liquidation_map_page(
    *,
    instrument: str,
    display_pair: str,
    last_price: str,
    updated_at: str,
) -> str:
    esc = _escape
    return LIQUIDATION_MAP_TEMPLATE.substitute(
        instrument=esc(instrument),
        display_pair=esc(display_pair),
        last_price=esc(last_price or "--"),
        updated_at=esc(updated_at or "--"),
    )


def _render_prompt_editor(
    template: str,
    *,
    instruments: Sequence[str] | None = None,
    models: Sequence[dict] | None = None,
    saved: bool = False,
) -> str:
    esc = _escape
    placeholder_entries = [
        ("{model_id}", "当前触发的模型 ID"),
        ("{instrument_id}", "标的/合约 ID"),
        ("{price}", "最新价格"),
        ("{spread}", "盘口价差"),
        ("{current_position}", "目标合约当前仓位"),
        ("{max_position}", "允许的最大仓位"),
        ("{cash_available}", "可用资金"),
        ("{positions}", "格式化的多笔仓位摘要"),
        ("{risk_notes}", "RiskContext 附加备注"),
        ("{market_metadata}", "行情与指标元数据"),
        ("{strategy_hint}", "策略提示或约束"),
    ]
    placeholder_list = "".join(
        f"<li><code>{esc(token)}</code><span>{esc(description)}</span></li>"
        for token, description in placeholder_entries
    )
    notice_block = "<div class='notice success'>提示词模板已保存。</div>" if saved else ""
    normalized_instruments = [
        esc(str(inst).upper())
        for inst in (instruments or [])
        if str(inst).strip()
    ]
    if normalized_instruments:
        chips = "".join(
            f"<span class='instrument-chip'>{inst}</span>"
            for inst in normalized_instruments
        )
        instrument_block = (
            "<div class='instrument-chips'>"
            f"{chips}"
            "</div>"
        )
    else:
        instrument_block = (
            "<div class='instrument-chips empty'>"
            "当前未设置可交易币对，请在“币对配置”页维护“需要交易的币对”列表。"
            "</div>"
        )
    normalized_models: list[dict] = []
    for entry in models or []:
        raw_model_id = str(entry.get("model_id") or entry.get("id") or "未知模型")
        raw_display = entry.get("display_name") or raw_model_id
        raw_provider = entry.get("provider") or "未知提供方"
        model_id = esc(raw_model_id)
        display_name = esc(raw_display)
        provider = esc(raw_provider)
        enabled = bool(entry.get("enabled"))
        normalized_models.append(
            {
                "raw_display": raw_display,
                "model_id": model_id,
                "display_name": display_name,
                "provider": provider,
                "enabled": enabled,
            }
        )
    enabled_models = [record for record in normalized_models if record["enabled"]]
    disabled_models = [record for record in normalized_models if not record["enabled"]]
    if normalized_models:
        cards: list[str] = []
        for record in normalized_models:
            status_text = "已启用" if record["enabled"] else "未启用"
            status_class = "status enabled" if record["enabled"] else "status disabled"
            cards.append(
                (
                    "<article class='model-card {status_class}'>"
                    "<header>"
                    "<h3>{display}</h3>"
                    "<span>{provider}</span>"
                    "</header>"
                    "<p class='status-label'>{status}</p>"
                    "</article>"
                ).format(
                    status_class=status_class,
                    display=record["display_name"],
                    provider=record["provider"],
                    status=status_text,
                )
            )
        model_block = "<div class='model-grid'>{cards}</div>".format(cards="".join(cards))
    else:
        model_block = (
            "<div class='model-grid empty'>"
            "暂无模型配置，请先在“模型管理”页开启可用模型。"
            "</div>"
        )
    if enabled_models:
        enabled_names = "、".join(rec["raw_display"] for rec in enabled_models)
        enabled_hint = f"以下模型已允许联机推理：{enabled_names}"
    else:
        enabled_hint = "当前没有启用的模型，请在“模型管理”页打开至少一个模型。"
    return PROMPT_EDITOR_TEMPLATE.format(
        prompt_text=esc(template),
        notice_block=notice_block,
        placeholder_list=placeholder_list,
        instrument_block=instrument_block,
        model_block=model_block,
        model_hint=esc(enabled_hint),
    )

def _build_risk_form_context(settings: dict) -> dict:
    esc = _escape
    price_pct = float(settings.get("price_tolerance_pct") or 0.02) * 100
    price_pct = max(0.1, min(50.0, price_pct))
    drawdown_pct = float(settings.get("max_drawdown_pct") or 8.0)
    drawdown_pct = max(0.1, min(95.0, drawdown_pct))
    max_loss = float(settings.get("max_loss_absolute") or 1500.0)
    max_loss = max(1.0, max_loss)
    cooldown_seconds = int(settings.get("cooldown_seconds") or 600)
    cooldown_seconds = max(10, min(86400, cooldown_seconds))
    min_notional = max(0.0, float(settings.get("min_notional_usd") or 0.0))
    max_order_notional = max(0.0, min(1_000_000.0, float(settings.get("max_order_notional_usd") or 0.0)))
    max_position_val = max(0.0, float(settings.get("max_position") or 0.0))
    max_capital_pct = max(0.0, min(1.0, float(settings.get("max_capital_pct_per_instrument") or 0.1)))
    pyramid_max_orders = max(0, min(100, int(settings.get("pyramid_max_orders") or 0)))
    take_profit_pct = max(0.0, min(500.0, float(settings.get("take_profit_pct") or settings.get("portfolio_take_profit_pct") or 0.0)))
    stop_loss_pct = max(0.0, min(95.0, float(settings.get("stop_loss_pct") or settings.get("portfolio_stop_loss_pct") or 0.0)))
    position_take_profit_pct = max(0.0, min(500.0, float(settings.get("position_take_profit_pct") or 5.0)))
    position_stop_loss_pct = max(0.0, min(95.0, float(settings.get("position_stop_loss_pct") or 3.0)))
    default_leverage = max(1, min(125, int(settings.get("default_leverage") or 1)))
    max_leverage = max(default_leverage, min(125, int(settings.get("max_leverage") or default_leverage)))
    pyramid_reentry_pct = max(0.0, min(50.0, float(settings.get("pyramid_reentry_pct") or 0.0)))
    liquidation_threshold = max(0.0, float(settings.get("liquidation_notional_threshold") or 0.0))
    liquidation_same_count = max(1, int(settings.get("liquidation_same_direction_count") or 4))
    liquidation_opposite_count = max(0, int(settings.get("liquidation_opposite_count") or 3))
    silence_config = int(settings.get("liquidation_silence_seconds") or 300)
    liquidation_silence_seconds = max(0, min(300, silence_config))

    def _compact(value: float, digits: int = 2) -> str:
        text = f"{value:.{digits}f}"
        text = text.rstrip("0").rstrip(".")
        return text or "0"

    tolerance_text = _compact(price_pct, 2)
    drawdown_text = _compact(drawdown_pct, 2)
    loss_text = _compact(max_loss, 2)
    min_notional_text = _compact(min_notional, 2)
    take_profit_text = _compact(take_profit_pct, 2)
    stop_loss_text = _compact(stop_loss_pct, 2)
    position_take_profit_text = _compact(position_take_profit_pct, 2)
    position_stop_loss_text = _compact(position_stop_loss_pct, 2)
    max_position_text = _compact(max_position_val, 4)
    max_capital_pct_percent = max_capital_pct * 100.0
    max_capital_pct_text = _compact(max_capital_pct_percent, 2)
    default_leverage_text = _compact(default_leverage, 0)
    max_leverage_text = _compact(max_leverage, 0)
    cooldown_minutes = cooldown_seconds / 60
    cooldown_minutes_text = _compact(cooldown_minutes, 1)
    pyramid_cap_text = _compact(pyramid_max_orders, 0)
    max_order_notional_text = _compact(max_order_notional, 2)
    pyramid_reentry_text = _compact(pyramid_reentry_pct, 2)
    liquidation_threshold_text = _compact(liquidation_threshold, 2)
    silence_minutes_text = _compact(liquidation_silence_seconds / 60.0, 1)
    updated_at = esc(_format_asia_shanghai(settings.get("updated_at")))

    return {
        "price_tolerance_pct": esc(f"{price_pct:.2f}"),
        "price_tolerance_display": esc(tolerance_text),
        "max_drawdown_pct": esc(f"{drawdown_pct:.2f}"),
        "drawdown_display": esc(drawdown_text),
        "max_loss_absolute": esc(f"{max_loss:.2f}"),
        "max_loss_display": esc(loss_text),
        "min_notional_usd": esc(f"{min_notional:.2f}"),
        "min_notional_display": esc(min_notional_text),
        "max_order_notional_usd": esc(f"{max_order_notional:.2f}"),
        "max_order_notional_display": esc(max_order_notional_text),
        "max_capital_pct_per_instrument": esc(f"{max_capital_pct_percent:.2f}"),
        "max_capital_pct_display": esc(max_capital_pct_text),
        "max_position": esc(f"{max_position_val:.4f}"),
        "max_position_display": esc(max_position_text),
        "pyramid_reentry_pct": esc(f"{pyramid_reentry_pct:.2f}"),
        "pyramid_reentry_display": esc(pyramid_reentry_text),
        "liquidation_notional_threshold": esc(f"{liquidation_threshold:.2f}"),
        "liquidation_notional_display": esc(liquidation_threshold_text),
        "take_profit_pct": esc(f"{take_profit_pct:.2f}"),
        "stop_loss_pct": esc(f"{stop_loss_pct:.2f}"),
        "take_profit_display": esc(take_profit_text),
        "stop_loss_display": esc(stop_loss_text),
        "position_take_profit_pct": esc(f"{position_take_profit_pct:.2f}"),
        "position_stop_loss_pct": esc(f"{position_stop_loss_pct:.2f}"),
        "position_take_profit_display": esc(position_take_profit_text),
        "position_stop_loss_display": esc(position_stop_loss_text),
        "default_leverage": esc(str(default_leverage)),
        "max_leverage": esc(str(max_leverage)),
        "default_leverage_display": esc(default_leverage_text),
        "max_leverage_display": esc(max_leverage_text),
        "pyramid_max_orders": esc(str(pyramid_max_orders)),
        "pyramid_max_display": esc(pyramid_cap_text),
        "cooldown_seconds": esc(str(cooldown_seconds)),
        "cooldown_minutes": esc(cooldown_minutes_text),
        "liquidation_same_direction_count": esc(str(liquidation_same_count)),
        "liquidation_opposite_count": esc(str(liquidation_opposite_count)),
        "liquidation_silence_seconds": esc(str(liquidation_silence_seconds)),
        "liquidation_silence_minutes": esc(silence_minutes_text),
        "max_position_val": max_position_val,
        "max_order_notional_val": max_order_notional,
        "max_capital_pct_percent": esc(f"{max_capital_pct_percent:.2f}"),
        "updated_at": updated_at,
    }


def _render_settings_page(settings: dict, catalog: Sequence[dict], risk_settings: dict) -> str:
    """Render the pipeline settings management UI."""
    esc = _escape
    poll_interval = int(settings.get("poll_interval") or 120)
    updated_at = esc(_format_asia_shanghai(settings.get("updated_at")))
    instruments = [str(item) for item in settings.get("tradable_instruments", [])]

    chips = [
        f"<span class='instrument-chip'>{esc(inst)}</span>"
        for inst in instruments
    ]
    chips_html = "".join(chips) if chips else "<span class='empty-state'>当前未配置可交易币对</span>"

    instruments_text = "\n".join(instruments)
    current_value_attr = ",".join(instruments)
    overrides_map_raw = settings.get("liquidation_overrides") or {}
    overrides_map = (
        {str(symbol).upper(): dict(values) for symbol, values in overrides_map_raw.items()}
        if isinstance(overrides_map_raw, dict)
        else {}
    )

    usdt_records: list[tuple[str, str]] = []
    for entry in catalog:
        inst_id = entry.get("inst_id")
        if not inst_id:
            continue
        alias = entry.get("alias")
        base = entry.get("base_currency")
        quote = entry.get("quote_currency")
        label_parts = []
        if alias:
            label_parts.append(str(alias))
        if base and quote:
            label_parts.append(f"{base}-{quote}")
        if not label_parts:
            parts = str(inst_id).split("-")
            if len(parts) >= 2:
                label_parts.append(f"{parts[0]}-{parts[1]}")
        label_text = " | ".join(label_parts) if label_parts else inst_id
        inst_upper = str(inst_id).upper()
        quote_upper = (quote or "").upper()
        is_usdt = (
            quote_upper == "USDT"
            or inst_upper.endswith("-USDT")
            or inst_upper.endswith("-USDT-SWAP")
            or "-USDT-" in inst_upper
        )
        is_usd = (
            quote_upper == "USD"
            or inst_upper.endswith("-USD")
            or inst_upper.endswith("-USD-SWAP")
            or "-USD-" in inst_upper
        )
        if is_usd or not is_usdt:
            continue
        display_text = inst_id
        if label_text and label_text != inst_id:
            display_text = f"{inst_id} ({label_text})"
        usdt_records.append((inst_id, display_text))

    usdt_records.sort(key=lambda item: item[0])
    options_html_usdt = "\n".join(
        f'                    <option value="{esc(inst_id)}">{esc(text)}</option>'
        for inst_id, text in usdt_records
    )
    catalog_count = len(catalog)
    if catalog_count:
        catalog_hint = f"已缓存 {catalog_count} 条 OKX 合约信息"
    else:
        catalog_hint = "未发现已缓存的币对，请点击“刷新合约库”按钮获取"

    override_rows: list[str] = []
    if instruments:
        for inst in instruments:
            symbol = inst.strip().upper()
            entry = overrides_map.get(symbol) or overrides_map.get(inst)
            same_val = entry.get("same_direction") if entry else None
            opp_val = entry.get("opposite_direction") if entry else None
            notional_val = entry.get("notional_threshold") if entry else None
            silence_val = entry.get("silence_seconds") if entry else None

            def _format_input_value(value: object, digits: int = 2) -> str:
                if value in (None, ""):
                    return ""
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    return ""
                if numeric.is_integer():
                    return str(int(numeric))
                return f"{numeric:.{digits}f}".rstrip("0").rstrip(".")

            same_attr = esc(str(int(same_val))) if same_val is not None else ""
            opp_attr = esc(str(int(opp_val))) if opp_val is not None else ""
            notional_attr = esc(_format_input_value(notional_val))
            silence_attr = esc(_format_input_value(silence_val, digits=1))
            inst_label = esc(inst)
            inst_name = inst.strip().upper()
            override_rows.append(
                "<tr>"
                f"<td>{inst_label}</td>"
                f"<td><input type=\"number\" min=\"1\" max=\"50\" name=\"override_same_{inst_name}\" value=\"{same_attr}\" placeholder=\"继承\"></td>"
                f"<td><input type=\"number\" min=\"0\" step=\"1\" name=\"override_notional_{inst_name}\" value=\"{notional_attr}\" placeholder=\"继承\"></td>"
                f"<td><input type=\"number\" min=\"0\" max=\"3600\" name=\"override_silence_{inst_name}\" value=\"{silence_attr}\" placeholder=\"继承\"></td>"
                f"<td><input type=\"number\" min=\"0\" max=\"50\" name=\"override_opp_{inst_name}\" value=\"{opp_attr}\" placeholder=\"继承\"></td>"
                "</tr>"
            )
    else:
        override_rows.append("<tr><td colspan=\"5\" class=\"empty-state\">请先添加可交易币对。</td></tr>")
    override_rows_html = "\n".join(override_rows)
    risk_summary_values = _build_risk_form_context(risk_settings)
    risk_summary_block = SETTINGS_RISK_SUMMARY_TEMPLATE.format(
        price_tolerance_display=risk_summary_values["price_tolerance_display"],
        min_notional_display=risk_summary_values["min_notional_display"],
        max_order_notional_display=risk_summary_values["max_order_notional_display"],
        max_capital_pct_display=risk_summary_values["max_capital_pct_display"],
        pyramid_reentry_display=risk_summary_values["pyramid_reentry_display"],
        pyramid_max_display=risk_summary_values["pyramid_max_display"],
        liquidation_silence_minutes=risk_summary_values["liquidation_silence_minutes"],
        liquidation_same_direction_count=risk_summary_values["liquidation_same_direction_count"],
        liquidation_opposite_count=risk_summary_values["liquidation_opposite_count"],
        liquidation_notional_display=risk_summary_values["liquidation_notional_display"],
        default_leverage_display=risk_summary_values["default_leverage_display"],
        max_leverage_display=risk_summary_values["max_leverage_display"],
    )

    return SETTINGS_TEMPLATE.format(
        poll_interval=poll_interval,
        updated_at=updated_at,
        chips_html=chips_html,
        instruments_text=esc(instruments_text),
        current_instruments_value=esc(current_value_attr),
        datalist_options_usdt=options_html_usdt,
        catalog_hint=esc(catalog_hint),
        override_rows=override_rows_html,
        risk_summary=risk_summary_block,
    )


def _render_order_debug_page(entries: Sequence[dict]) -> str:
    esc = _escape

    def _stage(flag: object) -> str:
        status = bool(flag)
        label = "通过" if status else "待满足"
        css = "pass" if status else "pending"
        return f'<span class="stage-pill stage-{css}">{label}</span>'

    rows: list[str] = []
    for entry in entries:
        timestamp = esc(_format_asia_shanghai(entry.get("timestamp")))
        account = esc(entry.get("account_id") or entry.get("account_key") or "--")
        model = esc(entry.get("model_id") or "--")
        instrument = esc(entry.get("instrument") or "--")
        decision = esc(entry.get("decision") or "--")
        confidence = entry.get("confidence")
        if confidence is not None:
            try:
                decision = f"{decision} ({float(confidence):.2f})"
            except Exception:
                decision = f"{decision} (--)"
        if entry.get("fallback_used"):
            decision = f"{decision} · Fallback"
        automation = (entry.get("automation") or "").strip()
        summaries: list[str] = []
        if automation == "liquidation_reversal":
            auto_summary = entry.get("automation_summary") or entry.get("signal_summary")
            if auto_summary:
                summaries.append(str(auto_summary))
        elif entry.get("signal_summary"):
            summaries.append(str(entry.get("signal_summary")))
        if entry.get("liquidation_hint"):
            summaries.append(f"提示：{entry.get('liquidation_hint')}")
        if entry.get("notes"):
            summaries.append(str(entry.get("notes")))
        note_content = "<br>".join(esc(item) for item in summaries if item)
        if not note_content:
            note_content = "--"
        if automation == "liquidation_reversal":
            note_content = f"<span class='automation-tag'>爆仓巡检</span>{note_content}"
        rows.append(
            "<tr>"
            f"<td>{timestamp}</td>"
            f"<td>{account}</td>"
            f"<td>{model}</td>"
            f"<td>{instrument}</td>"
            f"<td class=\"decision\">{decision}</td>"
            f"<td>{_stage(entry.get('account_enabled'))}</td>"
            f"<td>{_stage(entry.get('decision_actionable'))}</td>"
            f"<td>{_stage(entry.get('order_ready'))}</td>"
            f"<td>{_stage(entry.get('risk_passed'))}</td>"
            f"<td>{_stage(entry.get('submitted'))}</td>"
            f"<td>{note_content}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='11' class='empty-state'>暂无订单调试数据</td></tr>")
    return ORDER_DEBUG_TEMPLATE.format(rows="\n".join(rows))


def _render_risk_page(settings: dict) -> str:
    ctx = _build_risk_form_context(settings)
    return RISK_TEMPLATE.format(**ctx)


def _render_balances_table(balances: Sequence[dict] | None) -> str:
    esc = _escape
    rows = []
    for balance in balances or []:
        rows.append(
            "<tr>"
            f"<td>{esc(balance.get('currency'))}</td>"
            f"<td>{_format_number(balance.get('equity'))}</td>"
            f"<td>{_format_number(balance.get('total'))}</td>"
            f"<td>{_format_number(balance.get('available'))}</td>"
            f"<td>{_format_number(balance.get('frozen'))}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='5'>暂无余额信息</td></tr>")
    return (
        "<table class='dense'>"
        "<thead><tr><th>币种</th><th>总权益</th><th>总金额</th><th>可用余额</th><th>冻结金额</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )




def _render_positions_table(positions: Sequence[dict] | None, *, account_id: str | None = None) -> str:
    esc = _escape
    rows: list[str] = []

    def _fmt_quantity(value: float) -> str:
        text = f"{value:.8f}"
        text = text.rstrip("0").rstrip(".")
        return text or "0"

    for pos in positions or []:
        account_value = esc(account_id or pos.get("account_id") or "")
        instrument_value = esc(pos.get("instrument_id"))
        raw_side = str(pos.get("side") or "")
        side_display = "多单" if raw_side.lower() in {"long", "buy"} else "空单" if raw_side.lower() in {"short", "sell"} else raw_side
        side_value = esc(raw_side)
        side_display_escaped = esc(side_display)
        leverage_display = _format_number(pos.get("leverage"))
        try:
            qty = float(pos.get("quantity") or 0.0)
            mark_px = float(pos.get("mark_price") or 0.0)
            entry_px = float(pos.get("entry_price") or 0.0)
            leverage = float(pos.get("leverage") or 0.0)
        except Exception:
            qty = mark_px = entry_px = leverage = 0.0
        total_qty_text = _fmt_quantity(qty)
        price_reference = mark_px if mark_px > 0 else entry_px if entry_px > 0 else None
        margin = None
        raw_margin = pos.get("initial_margin")
        if raw_margin not in (None, ""):
            try:
                margin = float(raw_margin)
            except (TypeError, ValueError):
                margin = None
        if margin is None:
            notional_hint = pos.get("notional_value")
            if notional_hint not in (None, "") and leverage > 0:
                try:
                    margin = float(notional_hint) / leverage
                except (TypeError, ValueError):
                    margin = None
        if margin is None and qty > 0 and mark_px > 0 and leverage > 0:
            margin = (qty * mark_px) / leverage
        frozen_amount = None
        raw_frozen = pos.get("initial_margin")
        if raw_frozen not in (None, ""):
            try:
                frozen_amount = float(raw_frozen)
            except (TypeError, ValueError):
                frozen_amount = None
        if frozen_amount is None:
            frozen_amount = margin
        pnl_pct = None
        if entry_px > 0 and mark_px > 0:
            delta = (mark_px - entry_px)
            if raw_side.lower() in {"short", "sell"}:
                delta = -delta
            pnl_pct = (delta / entry_px) * 100
        pnl_pct_cell = ""
        if pnl_pct is not None:
            pct_value = _format_number(pnl_pct)
            pct_class = "pnl-positive" if pnl_pct >= 0 else "pnl-negative"
            pnl_pct_cell = f"<span class='{pct_class}'>{pct_value}%</span>"

        def _calc_amount(value: float, price: float | None) -> float | None:
            if price is None or value <= 0:
                return None
            return value * price

        def _build_action_tooltip(action_label: str, qty_text: str, amount_value: float | None) -> str:
            amount_text = _format_number(amount_value, digits=2) if amount_value is not None else "--"
            return f"币对 {instrument_value} {action_label} 数量：{qty_text}，金额：{amount_text} USD"

        price_reference_text = ""
        if price_reference and price_reference > 0:
            price_reference_text = _fmt_quantity(price_reference)

        def _close_form(label: str, qty_value: str, tooltip: str, price_hint: str) -> str:
            btn_class = "btn-close"
            if label == "全部平仓":
                btn_class += " btn-close-full"
            return (
                "<form method='post' action='/okx/close-position' class='inline-form'>"
                f"<input type='hidden' name='account_id' value='{account_value}'>"
                f"<input type='hidden' name='instrument_id' value='{instrument_value}'>"
                f"<input type='hidden' name='position_side' value='{side_value}'>"
                f"<input type='hidden' name='quantity' value='{qty_value}'>"
                f"<input type='hidden' name='action_label' value='{esc(label)}'>"
                f"<input type='hidden' name='reference_price' value='{price_hint}'>"
                f"<input type='hidden' name='side_display' value='{side_display_escaped}'>"
                f"<button type='submit' class='{btn_class}' title='{esc(tooltip)}'>{label}</button>"
                "</form>"
            )

        def _scale_form(label: str, qty_value: str, tooltip: str) -> str:
            return (
                "<form method='post' action='/okx/scale-position' class='inline-form'>"
                f"<input type='hidden' name='account_id' value='{account_value}'>"
                f"<input type='hidden' name='instrument_id' value='{instrument_value}'>"
                f"<input type='hidden' name='position_side' value='{side_value}'>"
                f"<input type='hidden' name='quantity' value='{qty_value}'>"
                f"<button type='submit' class='btn-scale' title='{esc(tooltip)}'>{label}</button>"
                "</form>"
            )

        action_pairs: list[str] = []
        if qty > 0:
            close_options = [
                (0.1, "平仓10%"),
                (0.2, "平仓20%"),
                (0.5, "平仓50%"),
            ]
            scale_options = [
                (0.5, "补仓50%"),
                (1.0, "补仓100%"),
                (2.0, "补仓200%"),
            ]
            for idx, (close_portion, close_label) in enumerate(close_options):
                scale_portion, scale_label = scale_options[idx]
                close_qty = max(qty * close_portion, 0.0001)
                scale_qty = max(qty * scale_portion, 0.0001)
                close_qty_text = _fmt_quantity(close_qty)
                scale_qty_text = _fmt_quantity(scale_qty)
                close_amount = _calc_amount(close_qty, price_reference)
                close_tooltip = _build_action_tooltip("平仓", close_qty_text, close_amount)
                scale_tooltip = _build_action_tooltip("补仓", scale_qty_text, _calc_amount(scale_qty, price_reference))
                stack = (
                    "<div class='action-pair'>"
                    f"{_scale_form(scale_label, esc(scale_qty_text), scale_tooltip)}"
                    f"{_close_form(close_label, esc(close_qty_text), close_tooltip, price_reference_text)}"
                    "</div>"
                )
                action_pairs.append(stack)
        action_pairs.append(
            "<div class='action-pair full'>"
            f"{_close_form('全部平仓', esc(total_qty_text), _build_action_tooltip('平仓', total_qty_text, _calc_amount(qty, price_reference)), price_reference_text)}"
            "</div>"
        )
        action_html = (
            "<div class='close-actions' style='display:flex; flex-wrap:wrap; gap:10px; justify-content:flex-end;'>"
            f"{''.join(action_pairs)}"
            "</div>"
        )
        rows.append(
            "<tr>"
            f"<td>{esc(pos.get('position_id'))}</td>"
            f"<td>{instrument_value}</td>"
            f"<td>{side_display_escaped}</td>"
            f"<td>{leverage_display}</td>"
            f"<td>{_format_number(pos.get('quantity'))}</td>"
            f"<td>{_format_number(pos.get('entry_price'))}</td>"
            f"<td>{_format_number(pos.get('last_price') or pos.get('last') or pos.get('mark_price'))}</td>"
            f"<td>{_format_number(margin)}</td>"
            f"<td>{_format_number(frozen_amount)}</td>"
            f"<td>{_format_number(pos.get('unrealized_pnl'))}</td>"
            f"<td>{pnl_pct_cell}</td>"
            f"<td>{esc(_format_asia_shanghai(pos.get('created_at') or pos.get('updated_at')))}</td>"
            f"<td class='action-col'>{action_html}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='13'>当前无持仓</td></tr>")

    table_html = (
        "<table class='dense'>"
        "<thead><tr><th>持仓ID</th><th>交易对</th><th>方向</th><th>杠杆</th><th>持仓量</th><th>开仓均价</th><th>最新价格</th><th>保证金</th><th>资金冻结</th><th>未实现盈亏</th><th>盈亏%</th><th>下单时间</th><th class='action-col'>操作</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )
    return table_html


def _render_close_all_button(account_id: str | None) -> str:
    if not account_id:
        return ""
    esc = _escape
    return (
        "<form method='post' action='/okx/close-all-positions' class='inline-form close-all-form' "
        "onsubmit=\"return confirm('确认平仓该账户的所有仓位？');\">"
        f"<input type='hidden' name='account_id' value='{esc(account_id)}'>"
        "<button type='submit' class='btn-close'>一键平仓全部</button>"
        "</form>"
    )

def _render_trades_table(trades: Sequence[dict] | None) -> str:
    esc = _escape
    rows = []
    for trade in trades or []:
        instrument = (trade.get("instrument_id") or trade.get("instId") or "--").upper()
        timestamp = trade.get("executed_at") or trade.get("timestamp")
        model = trade.get("model_id") or "--"
        account = trade.get("account_id") or trade.get("portfolio_id") or "--"
        side_raw = (trade.get("side") or trade.get("direction") or "").lower()
        side_label = "买入" if side_raw == "buy" else "卖出" if side_raw == "sell" else "--"
        side_class = "trade-buy" if side_raw == "buy" else "trade-sell" if side_raw == "sell" else ""
        quantity = trade.get("quantity") or trade.get("size") or trade.get("sz")
        entry_price = trade.get("entry_price") or trade.get("price")
        exit_price = trade.get("close_price") or trade.get("exit_price") or trade.get("price")
        fee = trade.get("fee") or trade.get("feeCcy") or trade.get("feeUsd")
        realized_pnl = trade.get("realized_pnl") or trade.get("pnl")
        pnl_cell = _format_number(realized_pnl, digits=4) if realized_pnl not in (None, 0) else ""
        rows.append(
            "<tr>"
            f"<td>{esc(_format_asia_shanghai(timestamp))}</td>"
            f"<td>{esc(model)}</td>"
            f"<td>{esc(account)}</td>"
            f"<td>{esc(instrument)}</td>"
            f"<td class='{side_class} trade-side'>{side_label}</td>"
            f"<td>{_format_number(quantity)}</td>"
            f"<td>{_format_number(entry_price)}</td>"
            f"<td>{_format_number(exit_price)}</td>"
            f"<td>{_format_number(fee, digits=4)}</td>"
            f"<td>{pnl_cell}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='10'>暂无成交记录</td></tr>")

    return (
        "<table class='dense'>"
        "<thead>"
        "<tr><th>成交时间</th><th>模型</th><th>投资账户</th><th>合约</th>"
        "<th>方向</th><th>数量</th><th>开仓价</th><th>平仓价</th><th>手续费</th><th>收益(USD)</th></tr>"
        "</thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        "<div class='trade-pagination' id='trade-pagination'></div>"
    )

def _render_orders_table(orders: Sequence[dict] | None, *, account_id: str | None = None) -> str:
    esc = _escape
    rows = []
    for order in orders or []:
        account_value = esc(account_id or order.get("account_id") or "")
        instrument_value = esc(order.get("instrument_id"))
        order_id_value = esc(order.get("order_id"))
        action_html = ""
        if account_value and instrument_value and order_id_value:
            action_html = (
                "<form method='post' action='/okx/cancel-order' class='inline-form'>"
                f"<input type='hidden' name='account_id' value='{account_value}'>"
                f"<input type='hidden' name='instrument_id' value='{instrument_value}'>"
                f"<input type='hidden' name='order_id' value='{order_id_value}'>"
                "<button type='submit' class='btn-cancel'>取消</button>"
                "</form>"
            )
        rows.append(
            "<tr>"
            f"<td>{order_id_value}</td>"
            f"<td>{instrument_value}</td>"
            f"<td>{esc(order.get('order_type'))}</td>"
            f"<td>{esc(order.get('side'))}</td>"
            f"<td>{_format_number(order.get('size'))}</td>"
            f"<td>{_format_number(order.get('filled_size'))}</td>"
            f"<td>{_format_number(order.get('price'))}</td>"
            f"<td>{_format_number(order.get('average_price'))}</td>"
            f"<td>{esc(order.get('state'))}</td>"
            f"<td>{esc(_format_asia_shanghai(order.get('updated_at')))}</td>"
            f"<td>{esc(_format_asia_shanghai(order.get('created_at')))}</td>"
            f"<td>{action_html}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='12'>暂无挂单</td></tr>")
    return (
        "<table class='dense'>"
        "<thead><tr><th>订单ID</th><th>交易对</th><th>委托类型</th><th>方向</th><th>委托数量</th><th>已成交数量</th><th>委托价格</th><th>成交均价</th><th>订单状态</th><th>更新时间</th><th>创建时间</th><th>操作</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )

def _render_equity_curve(points: Sequence[dict] | None) -> str:
    esc = _escape
    items = []
    for point in points or []:
        items.append(
            f"<li><span class='timestamp'>{esc(_format_asia_shanghai(point.get('timestamp')))}</span> · {esc(point.get('equity'))} USD</li>"
        )
    if not items:
        items.append("<li>暂无资产曲线数据</li>")
    return "<ul class='curve-list'>{}</ul>".format("\n".join(items))


def _build_instrument_options(instruments: Sequence[str], selected: Optional[str]) -> str:
    esc = _escape
    selected_norm = (selected or "").strip().upper()
    options = ['<option value="">全部</option>']
    seen: set[str] = set()
    for instrument in instruments:
        inst_upper = str(instrument or "").strip().upper()
        if not inst_upper or inst_upper in seen:
            continue
        seen.add(inst_upper)
        selected_attr = " selected" if inst_upper == selected_norm else ""
        options.append(f'<option value="{esc(inst_upper)}"{selected_attr}>{esc(inst_upper)}</option>')
    if selected_norm and selected_norm not in seen:
        options.insert(
            1,
            f'<option value="{esc(selected_norm)}" selected>{esc(selected_norm)}</option>',
        )
    return "\n".join(options)


def _build_manual_instrument_options(instruments: Sequence[str]) -> str:
    esc = _escape
    seen: set[str] = set()
    if not instruments:
        return '<option value="" disabled selected>暂无可交易合约</option>'
    options = ['<option value="" disabled selected>请选择合约</option>']
    for inst in instruments:
        inst_upper = str(inst or "").strip().upper()
        if not inst_upper or inst_upper in seen:
            continue
        seen.add(inst_upper)
        options.append(f'<option value="{esc(inst_upper)}">{esc(inst_upper)}</option>')
    return "\n".join(options)


def _build_depth_options(selected: int) -> str:
    choices = (5, 10, 20, 50, 400)
    sanitized = max(1, selected)
    rendered: list[str] = []
    for value in choices:
        selected_attr = " selected" if value == sanitized else ""
        rendered.append(f'<option value="{value}"{selected_attr}>{value} 档</option>')
    if sanitized not in choices:
        rendered.append(f'<option value="{sanitized}" selected>{sanitized} 档</option>')
    return "\n".join(rendered)



def _render_liquidation_rows(items: Sequence[dict]) -> str:
    esc = _escape
    rows: list[str] = []
    for item in items:
        timestamp = _format_asia_shanghai(item.get("timestamp"))
        net_qty = item.get("net_qty")
        price = item.get("last_price")
        notional = item.get("notional_value")
        if notional is None and net_qty is not None and price is not None:
            try:
                notional = abs(float(net_qty)) * float(price)
            except (TypeError, ValueError):
                notional = None
        rows.append(
            "<tr>"
            f"<td>{esc(timestamp)}</td>"
            f"<td>{esc(item.get('instrument_id') or '--')}</td>"
            f"<td>{_format_number(item.get('long_qty'))}</td>"
            f"<td>{_format_number(item.get('short_qty'))}</td>"
            f"<td>{_format_number(item.get('net_qty'))}</td>"
            f"<td>{_format_number(item.get('last_price'))}</td>"
            f"<td>{_format_number(notional)}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='7' class='empty-state'>暂无爆仓数据</td></tr>")
    return "\n".join(rows)


def _render_orderbook_cards(items: Sequence[dict], levels: int) -> str:
    _ = items, levels
    return "<p class='empty-state'>加载市场深度...</p>"

    return "\n".join(cards)

    return "\n".join(cards)

    return "\n".join(cards)

    return "\n".join(cards)

    return "\n".join(cards)



def _format_order(order: dict | None, *, hold_action: bool = False) -> str:
    if hold_action:
        return "无"
    if not order:
        return "N/A"
    side = _escape(order.get("side", "?"))
    size = _escape(order.get("size", "?"))
    price = _escape(order.get("price", "?"))
    return f"{side} {size} @ {price}"


def _escape(value: object) -> str:
    if value is None:
        return ""
    return html.escape(str(value))


def _parse_instrument_input(raw: str) -> list[str]:
    """Normalize instrument input from forms (supports comma or newline separated values)."""
    cleaned: list[str] = []
    seen: set[str] = set()
    for fragment in (raw or "").replace(",", "\n").splitlines():
        inst_id = fragment.strip().upper()
        if not inst_id or inst_id in seen:
            continue
        cleaned.append(inst_id)
        seen.add(inst_id)
    return cleaned


def _collect_override_payload(form: Mapping[str, object], instruments: Sequence[str]) -> dict[str, dict]:
    overrides: dict[str, dict] = {}
    if not instruments:
        return overrides

    def _parse_int(value: object, minimum: Optional[int] = None, maximum: Optional[int] = None) -> Optional[int]:
        text = str(value).strip() if isinstance(value, str) else str(value) if value is not None else ""
        if not text:
            return None
        try:
            number = int(float(text))
        except (TypeError, ValueError):
            return None
        if minimum is not None:
            number = max(minimum, number)
        if maximum is not None:
            number = min(maximum, number)
        return number

    def _parse_float(value: object, minimum: Optional[float] = None, maximum: Optional[float] = None) -> Optional[float]:
        text = str(value).strip() if isinstance(value, str) else str(value) if value is not None else ""
        if not text:
            return None
        try:
            number = float(text)
        except (TypeError, ValueError):
            return None
        if minimum is not None:
            number = max(minimum, number)
        if maximum is not None:
            number = min(maximum, number)
        return number

    for inst in instruments:
        symbol = inst.strip().upper()
        if not symbol:
            continue
        same_val = _parse_int(form.get(f"override_same_{inst}"), minimum=1, maximum=50)
        opp_val = _parse_int(form.get(f"override_opp_{inst}"), minimum=0, maximum=50)
        notional_val = _parse_float(form.get(f"override_notional_{inst}"), minimum=0.0)
        silence_val = _parse_float(form.get(f"override_silence_{inst}"), minimum=0.0, maximum=3600.0)
        entry: dict[str, object] = {}
        if same_val is not None:
            entry["same_direction"] = same_val
        if opp_val is not None:
            entry["opposite_direction"] = opp_val
        if notional_val is not None:
            entry["notional_threshold"] = notional_val
        if silence_val is not None:
            entry["silence_seconds"] = silence_val
        if entry:
            overrides[symbol] = entry
    return overrides


def _format_asia_shanghai(value: Optional[str]) -> str:
    if not value:
        return "N/A"
    raw = value
    try:
        # Epoch seconds support
        if isinstance(raw, (int, float)):
            epoch = float(raw)
            # Heuristic: handle ms / ns
            if epoch > 1e14:
                epoch = epoch / 1e9
            elif epoch > 1e11:
                epoch = epoch / 1e3
            dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        else:
            text = str(raw).strip()
            # Numeric string? (seconds/ms/ns)
            if text.isdigit():
                epoch = float(text)
                if epoch > 1e14:
                    epoch = epoch / 1e9
                elif epoch > 1e11:
                    epoch = epoch / 1e3
                dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
            else:
                normalized = text.replace("Z", "+00:00").replace("/", "-")
                try:
                    dt = datetime.fromisoformat(normalized)
                except ValueError:
                    # Fallback: naive "YYYY-MM-DD HH:MM:SS" treated as UTC
                    try:
                        dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    except Exception:
                        return text
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def _is_hold_action(action_zh: str, action_en: str) -> bool:
    zh = (action_zh or "").lower()
    en = (action_en or "").lower()
    return any(keyword in zh for keyword in ("观望", "保持", "等待")) or en in {"hold", "wait", "neutral"}


HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>Dashboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.2rem; }}
        h2 {{ margin-top: 2rem; margin-bottom: 0.5rem; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 1.5rem; background-color: #1e293b; border-radius: 6px; overflow: hidden; }}
        th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid #334155; }}
        th {{ background-color: #0f172a; font-size: 0.9rem; }}
        tr:nth-child(even) {{ background-color: #1e293b; }}
        tr:hover {{ background-color: #334155; }}
        footer {{ margin-top: 2rem; font-size: 0.85rem; color: #94a3b8; }}
        .timestamp {{ color: #38bdf8; }}
        a {{ color: #38bdf8; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .pnl-positive {{ color: #4ade80; }}
        .pnl-negative {{ color: #f87171; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link active">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>


    <h1>模型表现概览</h1>
    <div>数据时间：<span class="timestamp">{as_of}</span></div>
    <table>
        <thead>
            <tr>
                <th>模型</th>
                <th>投资组合</th>
                <th>夏普比率</th>
                <th>最大回撤(%)</th>
                <th>胜率(%)</th>
                <th>平均持仓时长(分钟)</th>
                <th>仓位敞口(USD)</th>
                <th>持仓数量</th>
            </tr>
        </thead>
        <tbody>
            {model_rows}
        </tbody>
    </table>
    <h2>近期成交</h2>
    <table>
        <thead>
            <tr>
                <th>成交时间</th>
                <th>模型</th>
                <th>投资组合</th>
                <th>合约</th>
                <th>方向</th>
                <th>数量</th>
                <th>入场价</th>
                <th>离场价</th>
                <th>盈亏(USD)</th>
            </tr>
        </thead>
        <tbody>
            {trade_rows}
        </tbody>
    </table>
    <h2>最新 AI 信号</h2>
    <table>
        <thead>
            <tr>
                <th>时间</th>
                <th>模型</th>
                <th>合约</th>
                <th>动作</th>
                <th>置信度</th>
                <th>理由</th>
                <th>建议操作</th>
            </tr>
        </thead>
        <tbody>
            {signal_rows}
        </tbody>
    </table>
    <footer>
        数据来源：<a href="/metrics/models">/metrics/models</a> ｜ 健康检查：<a href="/health">/health</a>
    </footer>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""


MODEL_MANAGER_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>模型管理</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
        .models-grid {{ display: grid; grid-template-columns: repeat(3, minmax(280px, 1fr)); gap: 20px; }}
        @media (max-width: 1200px) {{ .models-grid {{ grid-template-columns: repeat(2, minmax(280px, 1fr)); }} }}
        @media (max-width: 768px) {{ .models-grid {{ grid-template-columns: 1fr; }} }}
        .model-card {{ background-color: #1e293b; border-radius: 12px; padding: 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); }}
        .model-meta {{ margin: 0; font-size: 0.85rem; color: #94a3b8; }}
        .form-row {{ display: flex; flex-direction: column; gap: 8px; margin-top: 16px; }}
        .form-row input[type="password"] {{ padding: 10px 12px; border-radius: 8px; border: 1px solid #334155; background-color: #0f172a; color: #e2e8f0; }}
        .form-row input[type="password"]::placeholder {{ color: #64748b; }}
        .form-actions {{ margin-top: 20px; }}
        button {{ background-color: #38bdf8; color: #0f172a; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-weight: bold; }}
        button:hover {{ background-color: #0ea5e9; }}
        .toggle {{ display: inline-flex; align-items: center; gap: 10px; cursor: pointer; }}
        .toggle input {{ width: 18px; height: 18px; }}
        .last-updated {{ margin-top: 18px; font-size: 0.8rem; color: #64748b; }}
        .empty-state {{ font-size: 0.95rem; color: #94a3b8; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link active">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>

    <h1>模型管理</h1>
    <p>在此启用或停用不同的大模型，并配置各自的 API Key。</p>
    <div class="models-grid">
        {models_html}
    </div>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
 </html>
"""


SETTINGS_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>币对设置</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin-top: 1.8rem; margin-bottom: 0.5rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
        .settings-card {{ background-color: #1e293b; border-radius: 12px; padding: 20px 24px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); margin-top: 20px; }}
        .settings-card form {{ display: flex; flex-direction: column; gap: 14px; }}
        label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        input[type="number"], input[type="text"], textarea, select {{ border-radius: 8px; border: 1px solid #334155; background-color: #0f172a; color: #e2e8f0; padding: 10px 12px; font-size: 0.95rem; }}
        textarea {{ min-height: 160px; resize: vertical; font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace; line-height: 1.4; }}
        input[type="text"]::placeholder {{ color: #64748b; }}
        .chip-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 4px; }}
        .instrument-chip {{ background-color: rgba(56, 189, 248, 0.15); border: 1px solid rgba(56, 189, 248, 0.35); padding: 6px 10px; border-radius: 999px; font-size: 0.85rem; }}
        button {{ background-color: #38bdf8; color: #0f172a; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-weight: bold; align-self: flex-start; }}
        button:hover {{ background-color: #0ea5e9; }}
        button.secondary {{ background-color: transparent; border: 1px solid #38bdf8; color: #38bdf8; }}
        button.secondary:hover {{ background-color: rgba(56, 189, 248, 0.15); }}
        .inline-form {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; margin-top: 12px; max-width: 760px; }}
        .inline-form label {{ flex: 0 0 160px; margin-bottom: 0; }}
        .inline-form > div {{ display: inline-flex; align-items: center; gap: 16px; flex: 0 0 auto; }}
        /* Compact 2-column control rows */
        .control-row {{ display: grid; grid-template-columns: 140px 1fr; align-items: start; gap: 12px; max-width: 980px; margin-top: 12px; }}
        form.control-row {{ grid-template-columns: 140px 1fr; }}
        .control-label {{ color: #94a3b8; font-size: 0.9rem; }}
        .control-field {{ display: inline-flex; align-items: center; gap: 16px; }}
        .control-row > div {{ display: inline-flex; align-items: center; gap: 16px; }}
        .control-row:not(form) > div {{ grid-column: 2 / span 2; }}
        .control-row button {{ grid-column: 2; justify-self: start; }}
        .control-row input[type="hidden"] {{ display: none; }}
        .row-2col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; align-items: start; }}
        .pair-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; align-items: start; margin-bottom: 8px; }}
        .pair-block label {{ margin-bottom: 6px; color: #94a3b8; font-size: 0.9rem; }}
        #instrument-select-usdt {{ width: 100%; max-width: 100%; }}
        .hint {{ color: #94a3b8; font-size: 0.85rem; margin: 0 0 12px 0; }}
        .updated {{ color: #94a3b8; font-size: 0.9rem; margin-top: -0.2rem; }}
        .timestamp {{ color: #38bdf8; }}
        .override-table {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; }}
        .override-table th, .override-table td {{ border: 1px solid #334155; padding: 8px 10px; font-size: 0.85rem; text-align: left; }}
        .override-table th {{ background-color: rgba(15, 23, 42, 0.6); }}
        .override-table td input {{ width: 100%; box-sizing: border-box; }}
        .override-table .empty-state {{ text-align: center; padding: 12px 0; color: #94a3b8; }}
        .risk-summary-card {{ background-color: #1e293b; border-radius: 12px; padding: 20px 24px; margin-top: 1.5rem; box-shadow: 0 8px 24px rgba(2, 6, 23, 0.55); display: flex; flex-direction: column; gap: 0.8rem; }}
        .risk-summary-card h3 {{ margin: 0; font-size: 1rem; }}
        .risk-summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }}
        .risk-summary-item {{ background-color: rgba(15, 23, 42, 0.7); border: 1px solid rgba(148, 163, 184, 0.25); border-radius: 12px; padding: 10px 12px; }}
        .risk-summary-item span {{ display: block; font-size: 0.8rem; color: #94a3b8; margin-bottom: 4px; }}
        .risk-summary-item strong {{ font-size: 1rem; color: #e2e8f0; }}
        @media (max-width: 768px) {{
            .inline-form {{ flex-direction: column; align-items: stretch; }}
            .inline-form label {{ width: 100%; }}
            button {{ width: 100%; justify-content: center; }}
            .control-row, form.control-row {{ grid-template-columns: 1fr; }}
            .control-row:not(form) > div {{ grid-column: 1; }}
            .pair-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link active">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>

    <h1>币对与刷新频率</h1>
    <p class="updated">最近保存：<span class="timestamp">{updated_at}</span></p>
    <div class="pair-grid">
        <section class="settings-card">
            <h2>运行参数</h2>
            <form method="post" action="/settings/update">
                <label for="poll-interval">刷新间隔（秒）</label>
                <input id="poll-interval" type="number" name="poll_interval" min="30" max="3600" value="{poll_interval}" required>
                <label for="instrument-list">当前合约列表（每行一个）</label>
                <textarea id="instrument-list" name="instruments" spellcheck="false">{instruments_text}</textarea>
                <div class="chip-row">{chips_html}</div>
                <h3>爆仓阈值覆盖</h3>
                <p class="hint">留空表示继承下方“风险参数”中的全局阈值；每个币对可以独立设置。</p>
                <table class="override-table">
                    <thead>
                        <tr>
                            <th>合约</th>
                            <th>同向爆仓笔数</th>
                            <th>名义金额阈值（USDT）</th>
                            <th>同向冷静期（秒）</th>
                            <th>逆向确认笔数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {override_rows}
                    </tbody>
                </table>
                <button type="submit">保存配置</button>
            </form>
        </section>
        <section class="settings-card">
            <h2>添加合约</h2>
            <p class="hint">{catalog_hint}</p>
            <form method="post" action="/settings/add" class="control-row" id="add-instrument-form">
                <input type="hidden" name="poll_interval" value="{poll_interval}">
                <input type="hidden" name="current_instruments" value="{current_instruments_value}">
                <label for="instrument-select-usdt" class="control-label">USDT</label>
                <div class="control-field">
                    <select id="instrument-select-usdt" name="new_instrument" size="12">
                        <option value="">请选择合约...</option>
    {datalist_options_usdt}
                    </select>
                </div>
                <button type="submit">添加到列表</button>
            </form>
            <form method="post" action="/settings/refresh-catalog" class="inline-form">
                <button type="submit" class="secondary">刷新合约库</button>
            </form>
        </section>
    </div>
    {risk_summary}
    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""


SETTINGS_RISK_SUMMARY_TEMPLATE = r"""
    <section class="risk-summary-card">
        <h3>全局阈值速览</h3>
        <div class="risk-summary-grid">
            <div class="risk-summary-item"><span>价格波动容忍度</span><strong>{price_tolerance_display}%</strong></div>
            <div class="risk-summary-item"><span>最小下单金额</span><strong>{min_notional_display} USDT</strong></div>
            <div class="risk-summary-item"><span>最大下单金额</span><strong>{max_order_notional_display} USDT</strong></div>
            <div class="risk-summary-item"><span>单币对最大资金占比</span><strong>{max_capital_pct_display}%</strong></div>
            <div class="risk-summary-item"><span>金字塔偏离 / 上限</span><strong>{pyramid_reentry_display}% · {pyramid_max_display} 次</strong></div>
            <div class="risk-summary-item"><span>同向冷静期</span><strong>{liquidation_silence_minutes} 分钟</strong></div>
            <div class="risk-summary-item"><span>同向 / 逆向爆仓笔数</span><strong>{liquidation_same_direction_count} / {liquidation_opposite_count}</strong></div>
            <div class="risk-summary-item"><span>爆仓名义阈值</span><strong>{liquidation_notional_display} USDT</strong></div>
            <div class="risk-summary-item"><span>默认 / 最大杠杆</span><strong>{default_leverage_display} / {max_leverage_display}</strong></div>
        </div>
    </section>
"""

ORDER_DEBUG_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>下单调试</title>
    <style>
        body {{ font-family: "Segoe UI", Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; flex-wrap: wrap; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        h1 {{ margin-bottom: 0.2rem; }}
        .hint {{ color: #94a3b8; margin-bottom: 1rem; }}
        table {{ width: 100%; border-collapse: collapse; background-color: rgba(15, 23, 42, 0.7); border-radius: 12px; overflow: hidden; box-shadow: 0 10px 30px rgba(2, 6, 23, 0.75); }}
        thead th {{ text-align: left; padding: 14px 16px; background-color: rgba(30, 41, 59, 0.9); font-size: 0.85rem; border-bottom: 1px solid rgba(148, 163, 184, 0.2); white-space: nowrap; }}
        tbody td {{ padding: 14px 16px; border-bottom: 1px solid rgba(30, 41, 59, 0.8); font-size: 0.9rem; }}
        tbody tr:nth-child(even) {{ background-color: rgba(15, 23, 42, 0.6); }}
        .stage-pill {{ display: inline-flex; align-items: center; justify-content: center; padding: 4px 10px; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; white-space: nowrap; }}
        .stage-pass {{ background-color: rgba(74, 222, 128, 0.15); color: #4ade80; border: 1px solid rgba(74, 222, 128, 0.4); }}
        .stage-pending {{ background-color: rgba(248, 250, 252, 0.05); color: #fbbf24; border: 1px solid rgba(251, 191, 36, 0.4); }}
        .decision {{ font-family: "Fira Code", "Consolas", monospace; font-size: 0.85rem; }}
        .empty-state {{ text-align: center; padding: 60px 0; color: #94a3b8; font-size: 0.95rem; }}
        .automation-tag {{ display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 9999px; background: rgba(59, 130, 246, 0.2); color: #93c5fd; font-size: 0.75rem; font-weight: 600; margin-right: 8px; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link active">下单调试</a>
    </nav>

    <h1>下单调试</h1>
    <p class="hint">依次检查：账户启用 → 模型输出非 hold → 订单字段齐全 → 风险/校验通过 → 提交下单。</p>
    <table>
        <thead>
            <tr>
                <th>时间</th>
                <th>账户</th>
                <th>模型</th>
                <th>合约</th>
                <th>模型信号</th>
                <th>账户启用</th>
                <th>模型非 hold</th>
                <th>订单字段</th>
                <th>风险/校验</th>
                <th>实际下单</th>
                <th>备注</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""

RISK_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>风险控制</title>
    <style>
        body {{ font-family: "Segoe UI", Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; flex-wrap: wrap; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        header h1 {{ margin-bottom: 0.2rem; }}
        header .updated {{ color: #94a3b8; margin: 0 0 0.5rem 0; }}
        header .hint {{ color: #94a3b8; margin: 0; }}
        .risk-card {{ background-color: #1e293b; border-radius: 16px; padding: 24px; box-shadow: 0 8px 24px rgba(2, 6, 23, 0.65); margin-bottom: 24px; }}
        .risk-card h2 {{ margin-top: 0; margin-bottom: 1rem; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; }}
        .summary-item {{ background-color: rgba(15, 23, 42, 0.65); border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 12px; padding: 12px; }}
        .summary-item span {{ display: block; font-size: 0.8rem; color: #94a3b8; margin-bottom: 4px; }}
        .summary-item strong {{ font-size: 1.1rem; color: #e2e8f0; }}
        .summary-item input {{ width: 100%; box-sizing: border-box; border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.4); background-color: rgba(15, 23, 42, 0.9); padding: 10px 12px; color: #e2e8f0; font-size: 0.95rem; margin-top: 4px; }}
        .summary-item input:focus {{ outline: none; border-color: #38bdf8; box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2); }}
        .summary-item small {{ display: block; color: #94a3b8; margin-top: 4px; font-size: 0.75rem; }}
        .redirect-btn {{ display: inline-flex; padding: 10px 18px; border-radius: 10px; background-color: #38bdf8; color: #0f172a; text-decoration: none; font-weight: 600; margin-top: 0.5rem; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">盘口深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link active">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单链路</a>
    </nav>

    <header>
        <h1>风险控制</h1>
        <p class="updated">最近更新：<span class="timestamp">{updated_at}</span></p>
        <p class="hint">风险参数保存后会实时应用到模型与自动交易流程。</p>
    </header>

    <section class="risk-card primary">
        <h2>风险参数</h2>
        <form method="post" action="/risk/update">
            <div class="summary-grid">
                <label class="summary-item">
                    <span>价格波动容忍度（%）</span>
                    <input type="number" name="price_tolerance_pct" min="0.1" max="50" step="0.1" value="{price_tolerance_pct}" required>
                    <small class="hint">允许委托价格偏离 ±{price_tolerance_display}%</small>
                </label>
                <label class="summary-item">
                    <span>最小下单金额（USDT）</span>
                    <input type="number" name="min_notional_usd" min="0" step="0.01" value="{min_notional_usd}" required>
                    <small class="hint">当前 {min_notional_display} USDT</small>
                </label>
                <label class="summary-item">
                    <span>最大下单金额（USDT）</span>
                    <input type="number" name="max_order_notional_usd" min="0" step="0.01" value="{max_order_notional_usd}" required>
                    <small class="hint">上限 {max_order_notional_display} USDT</small>
                </label>
                <label class="summary-item">
                    <span>单币对最大资金占比（%）</span>
                    <input type="number" name="max_capital_pct_per_instrument" min="0" max="100" step="0.1" value="{max_capital_pct_per_instrument}" required>
                    <small class="hint">当前 {max_capital_pct_display}% × 杠杆</small>
                </label>
                <label class="summary-item">
                    <span>最大持仓（合约）</span>
                    <input type="number" name="max_position" min="0" step="0.0001" value="{max_position}" required>
                    <small class="hint">当前 {max_position_display}</small>
                </label>
                <label class="summary-item">
                    <span>价格刷新冷静期（秒）</span>
                    <input type="number" name="cooldown_seconds" min="10" max="86400" step="10" value="{cooldown_seconds}" required>
                    <small class="hint">约 {cooldown_minutes} 分钟</small>
                </label>
                <label class="summary-item">
                    <span>金字塔补仓偏离（%）</span>
                    <input type="number" name="pyramid_reentry_pct" min="0" max="50" step="0.1" value="{pyramid_reentry_pct}" required>
                    <small class="hint">当前 {pyramid_reentry_display}%</small>
                </label>
                <label class="summary-item">
                    <span>金字塔单向上限（单）</span>
                    <input type="number" name="pyramid_max_orders" min="0" max="100" step="1" value="{pyramid_max_orders}" required>
                    <small class="hint">最多 {pyramid_max_display} 次</small>
                </label>
                <label class="summary-item">
                    <span>默认杠杆</span>
                    <input type="number" name="default_leverage" min="1" max="125" step="1" value="{default_leverage}" required>
                    <small class="hint">当前 {default_leverage_display} 倍</small>
                </label>
                <label class="summary-item">
                    <span>最大杠杆</span>
                    <input type="number" name="max_leverage" min="1" max="125" step="1" value="{max_leverage}" required>
                    <small class="hint">当前 {max_leverage_display} 倍</small>
                </label>
                <label class="summary-item">
                    <span>整体止盈阈值（%）</span>
                    <input type="number" name="take_profit_pct" min="0" max="500" step="0.1" value="{take_profit_pct}" required>
                    <small class="hint">当前 {take_profit_display}%</small>
                </label>
                <label class="summary-item">
                    <span>整体止损阈值（%）</span>
                    <input type="number" name="stop_loss_pct" min="0" max="95" step="0.1" value="{stop_loss_pct}" required>
                    <small class="hint">当前 {stop_loss_display}%</small>
                </label>
                <label class="summary-item">
                    <span>单笔止盈（%）</span>
                    <input type="number" name="position_take_profit_pct" min="0" max="500" step="0.1" value="{position_take_profit_pct}" required>
                    <small class="hint">当前 {position_take_profit_display}%</small>
                </label>
                <label class="summary-item">
                    <span>单笔止损（%）</span>
                    <input type="number" name="position_stop_loss_pct" min="0" max="95" step="0.1" value="{position_stop_loss_pct}" required>
                    <small class="hint">当前 {position_stop_loss_display}%</small>
                </label>
                <label class="summary-item">
                    <span>爆仓名义阈值（USDT）</span>
                    <input type="number" name="liquidation_notional_threshold" min="0" step="1" value="{liquidation_notional_threshold}" required>
                    <small class="hint">当前 {liquidation_notional_display} USDT</small>
                </label>
                <label class="summary-item">
                    <span>同向爆仓笔数（全局）</span>
                    <input type="number" name="liquidation_same_direction_count" min="1" max="20" step="1" value="{liquidation_same_direction_count}" required>
                    <small class="hint">当前 {liquidation_same_direction_count} 笔</small>
                </label>
                <label class="summary-item">
                    <span>逆向确认笔数（全局）</span>
                    <input type="number" name="liquidation_opposite_count" min="0" max="20" step="1" value="{liquidation_opposite_count}" required>
                    <small class="hint">当前 {liquidation_opposite_count} 笔</small>
                </label>
                <label class="summary-item">
                    <span>同向冷静期（秒）</span>
                    <input type="number" name="liquidation_silence_seconds" min="0" max="300" step="1" value="{liquidation_silence_seconds}" required>
                    <small class="hint">约 {liquidation_silence_minutes} 分钟</small>
                </label>
            </div>
            <button type="submit">保存风险参数</button>
        </form>
    </section>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""

OKX_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>OKX 模拟交易</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin-top: 2rem; margin-bottom: 0.5rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
        .error-card {{ background-color: #3f1d45; border-radius: 12px; padding: 16px 20px; margin-bottom: 20px; box-shadow: 0 6px 18px rgba(15, 23, 42, 0.45); }}
        .error-card h2 {{ margin: 0 0 10px 0; color: #f87171; }}
        .error-card ul {{ margin: 0; padding-left: 20px; }}
        .error-card li {{ margin: 6px 0; font-size: 0.9rem; }}
        .okx-card {{ background-color: #1e293b; border-radius: 12px; padding: 20px; margin-top: 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); }}
        .okx-card header {{ margin-bottom: 12px; }}
        .okx-card .meta {{ color: #94a3b8; font-size: 0.9rem; margin-top: 4px; }}
        .summary-table {{ margin-top: 12px; }}
        .split {{ display: flex; gap: 20px; flex-wrap: wrap; align-items: flex-start; }}
        .panel {{ flex: 1 1 320px; min-width: 260px; }}
        .split .panel:first-child {{ flex: 0.9 1 320px; }}
        .split .panel:last-child {{ flex: 1.5 1 520px; min-width: 360px; }}
        .recent-trades-panel table {{ table-layout: auto; }}
        .trade-side {{ font-weight: 600; }}
        .trade-buy {{ color: #4ade80; }}
        .trade-sell {{ color: #f87171; }}
        .trade-pagination {{ display: flex; gap: 6px; flex-wrap: wrap; margin-top: 0.35rem; }}
        .trade-page-btn {{ padding: 4px 10px; border-radius: 6px; border: 1px solid rgba(148, 163, 184, 0.4); background: transparent; color: #cbd5f5; cursor: pointer; }}
        .trade-page-btn.active {{ background: #38bdf8; color: #0f172a; border-color: #38bdf8; }}
        table.dense {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; background-color: rgba(15, 23, 42, 0.6); border-radius: 6px; overflow: hidden; }}
        table.dense th, table.dense td {{ padding: 10px 12px; border-bottom: 1px solid #334155; text-align: left; font-size: 0.9rem; }}
        table.dense th.action-col, table.dense td.action-col {{ text-align: center; }}
        ul.curve-list {{ list-style: none; padding: 0; margin: 0.5rem 0 0 0; }}
        ul.curve-list li {{ padding: 6px 0; border-bottom: 1px dashed #334155; font-size: 0.9rem; }}
        .inline-row {{ display: inline-flex; align-items: center; gap: 10px; margin: 0; }}
        .inline-form {{ display: inline-flex; align-items: center; margin: 0; }}
        .btn-refresh {{ font-size: 12px; padding: 4px 8px; border-radius: 6px; border: 1px solid #38bdf8; color: #38bdf8; text-decoration: none; background: transparent; }}
        .btn-refresh:hover {{ background-color: rgba(56, 189, 248, 0.15); }}
        .btn-close {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            padding: 6px 18px;
            border-radius: 999px;
            border: 1px solid rgba(248, 113, 113, 0.6);
            background-image: linear-gradient(135deg, #f87171, #f43f5e);
            color: #fff;
            font-weight: 600;
            font-size: 0.9rem;
            cursor: pointer;
            transition: transform 0.2s ease, box-shadow 0.2s ease, opacity 0.15s ease;
            box-shadow: 0 8px 18px rgba(248, 113, 113, 0.35);
        }}
        .btn-close:hover {{
            transform: translateY(-1px);
            box-shadow: 0 12px 24px rgba(248, 113, 113, 0.45);
            opacity: 0.95;
        }}
        .btn-close:active {{
            transform: translateY(1px);
            box-shadow: 0 5px 12px rgba(248, 113, 113, 0.3);
        }}
        .btn-close-full {{
            padding-top: 18px;
            padding-bottom: 18px;
            background-image: linear-gradient(135deg, #fb923c, #f97316);
            border-color: rgba(251, 146, 60, 0.8);
            box-shadow: 0 10px 24px rgba(251, 146, 60, 0.45);
        }}
        .local-time {{ color: #94a3b8; font-size: 0.9rem; }}
        .flash {{ margin: 12px 0; padding: 12px 16px; border-radius: 10px; font-size: 0.95rem; }}
        .flash.success {{ background-color: rgba(74, 222, 128, 0.12); color: #4ade80; border: 1px solid rgba(74, 222, 128, 0.3); }}
        .flash.error {{ background-color: rgba(248, 113, 113, 0.12); color: #f87171; border: 1px solid rgba(248, 113, 113, 0.3); }}
        .manual-card {{ background-color: #1e293b; border-radius: 12px; padding: 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); margin-top: 1.5rem; }}
        .manual-form {{ display: flex; flex-wrap: wrap; gap: 14px; align-items: flex-end; }}
        .manual-field {{ display: flex; flex-direction: column; gap: 6px; color: #94a3b8; font-size: 0.9rem; min-width: 160px; flex: 0 0 auto; }}
        .manual-field.flex {{ flex: 1 1 220px; }}
        .manual-form input, .manual-form select {{ border-radius: 8px; border: 1px solid #334155; background-color: #0f172a; color: #e2e8f0; padding: 10px; font-size: 0.95rem; }}
        .manual-form button {{ background-color: #38bdf8; color: #0f172a; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-weight: bold; }}
        .manual-form button:hover {{ background-color: #0ea5e9; }}
        .order-info-card {{ background-color: #14233b; border-radius: 14px; padding: 16px 20px; margin-top: 16px; box-shadow: 0 8px 20px rgba(2, 6, 23, 0.6); }}
        .order-info-card h3 {{ margin: 0 0 10px 0; font-size: 1rem; }}
        .order-info-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; }}
        .order-info-row {{ background-color: rgba(15, 23, 42, 0.4); border-radius: 10px; padding: 10px 12px; display: flex; flex-direction: column; gap: 4px; }}
        .order-info-row span {{ color: #94a3b8; font-size: 0.8rem; }}
        .order-info-row strong {{ font-size: 0.95rem; }}
        .manual-action {{ display: flex; align-items: center; }}
        .manual-action button {{ margin: 0; white-space: nowrap; }}
        .panel-head {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 0.5rem; }}
        .hint {{ color: #94a3b8; font-size: 0.85rem; margin: 0; }}
        .btn-cancel {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            padding: 6px 16px;
            border-radius: 999px;
            border: 1px solid rgba(248, 113, 113, 0.6);
            background-image: linear-gradient(135deg, #ef4444, #f97316);
            color: #fff;
            font-weight: 600;
            font-size: 0.95rem;
            cursor: pointer;
            transition: transform 0.2s ease, box-shadow 0.2s ease, opacity 0.15s ease;
            box-shadow: 0 8px 18px rgba(239, 68, 68, 0.4);
        }}
        .btn-cancel:hover {{
            transform: translateY(-1px);
            box-shadow: 0 12px 26px rgba(239, 68, 68, 0.45);
            opacity: 0.95;
        }}
        .btn-cancel:active {{
            transform: translateY(1px);
            box-shadow: 0 5px 12px rgba(239, 68, 68, 0.35);
        }}
        .btn-scale {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            padding: 6px 16px;
            border-radius: 999px;
            border: 1px solid rgba(34, 197, 94, 0.6);
            background-image: linear-gradient(135deg, #22c55e, #16a34a);
            color: #fff;
            font-weight: 600;
            font-size: 0.9rem;
            cursor: pointer;
            transition: transform 0.2s ease, box-shadow 0.2s ease, opacity 0.15s ease;
            box-shadow: 0 8px 18px rgba(34, 197, 94, 0.35);
        }}
        .btn-scale:hover {{
            transform: translateY(-1px);
            box-shadow: 0 12px 24px rgba(34, 197, 94, 0.45);
            opacity: 0.95;
        }}
        .btn-scale:active {{
            transform: translateY(1px);
            box-shadow: 0 5px 12px rgba(34, 197, 94, 0.3);
        }}
        .action-pair {{
            display: flex;
            flex-direction: column;
            gap: 4px;
        }}
        .action-pair.full {{
            justify-content: space-between;
            min-height: 96px;
        }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link active">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>

    <h1>OKX 模拟交易概览</h1>
    <p class="inline-row">数据时间：<span class="timestamp">{as_of}</span><span class="local-time">（本地时间：<span id="local-time">--:--:--</span>）</span><a href="/okx?refresh=1" class="btn-refresh" title="从交易所拉取最新并写入缓存">强制刷新</a></p>
    {flash_block}
    <section class="manual-card">
      <h2>手动下单</h2>
      <form class="manual-form" method="post" action="/okx/manual-order">
        <label class="manual-field">账户
          <select name="account_id" required>
            {manual_account_options}
          </select>
        </label>
        <label class="manual-field">合约
          <select name="instrument_id" required>
            {manual_instrument_options}
          </select>
        </label>
        <label class="manual-field">方向
          <select name="side" required>
            <option value="buy">买入</option>
            <option value="sell">卖出</option>
          </select>
        </label>
        <label class="manual-field">类型
          <select name="order_type" id="order-type" required>
            <option value="limit">限价</option>
            <option value="market">市价</option>
          </select>
        </label>
        <label class="manual-field">数量
          <input type="number" step="any" min="0" name="size" required>
        </label>
        <label class="manual-field flex">价格（限价必填）
          <input type="number" step="any" min="0" name="price" id="price-input" placeholder="市价单可留空">
        </label>
        <label class="manual-field">保证金模式
          <select name="margin_mode">
            <option value="">自动</option>
            <option value="cross">全仓</option>
            <option value="isolated">逐仓</option>
            <option value="cash">现货</option>
          </select>
        </label>
        <div class="manual-action">
          <button type="submit">提交订单</button>
        </div>
      </form>
    </section>
    {order_info_card}
    {error_block}
    {account_sections}
    <script>
        (function() {{
        function pad(n) {{ return n < 10 ? '0' + n : '' + n; }}
        function fmt(d) {{
          var y=d.getFullYear();
          var m=pad(d.getMonth()+1);
          var day=pad(d.getDate());
          var hh=pad(d.getHours());
          var mm=pad(d.getMinutes());
          var ss=pad(d.getSeconds());
          return y + '-' + m + '-' + day + ' ' + hh + ':' + mm + ':' + ss;
        }}
        function tick() {{
          var el = document.getElementById('local-time');
          if (el) {{ el.textContent = fmt(new Date()); }}
        }}
        tick();
        setInterval(tick, 1000);
        var orderType = document.getElementById('order-type');
        var priceInput = document.getElementById('price-input');
        function togglePrice() {{
          if (!orderType || !priceInput) return;
          var isMarket = orderType.value === 'market';
          priceInput.required = !isMarket;
          priceInput.disabled = isMarket;
          if (isMarket) priceInput.value = '';
        }}
        if (orderType && priceInput) {{
          orderType.addEventListener('change', togglePrice);
          togglePrice();
        }}
        (function paginateTrades() {{
          const perPage = 13;
          const table = document.querySelector('.recent-trades-panel table.dense');
          const pagination = document.getElementById('trade-pagination');
          if (!table || !pagination) {{ return; }}
          const rows = Array.from(table.tBodies[0].rows);
          if (!rows.length) {{ pagination.style.display = 'none'; return; }}
          const pageCount = Math.ceil(rows.length / perPage);
          let current = 0;
          const renderPage = (page) => {{
            rows.forEach((row, idx) => {{
              row.style.display = (idx >= page * perPage && idx < (page + 1) * perPage) ? '' : 'none';
            }});
            pagination.innerHTML = '';
            for (let i = 0; i < pageCount; i += 1) {{
              const btn = document.createElement('button');
              btn.type = 'button';
              btn.className = 'trade-page-btn' + (i === page ? ' active' : '');
              btn.textContent = `${{i + 1}}`;
              btn.disabled = i === page;
              btn.addEventListener('click', () => renderPage(i));
              pagination.appendChild(btn);
            }}
          }};
          renderPage(0);
        }})();
      }})();
    </script>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""


SCHEDULER_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>任务调度</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin-top: 1.5rem; margin-bottom: 0.6rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
        .scheduler-layout {{ display: flex; flex-wrap: wrap; gap: 24px; align-items: flex-start; }}
        .card {{ background-color: #1e293b; border-radius: 12px; padding: 22px 26px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); }}
        .primary-card {{ flex: 0 1 360px; min-width: 320px; max-width: 420px; }}
        form {{ display: grid; gap: 18px; }}
        label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        .control-line {{ display: flex; gap: 12px; align-items: center; }}
        input[type="number"] {{ border-radius: 8px; border: 1px solid #334155; background-color: #0f172a; color: #e2e8f0; padding: 10px 12px; font-size: 0.95rem; flex: 1; min-width: 0; }}
        input[type="number"]::placeholder {{ color: #64748b; }}
        button {{ background-color: #38bdf8; color: #0f172a; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-weight: bold; justify-self: flex-start; }}
        button:hover {{ background-color: #0ea5e9; }}
        button:disabled {{ opacity: 0.6; cursor: not-allowed; }}
        .hint {{ color: #94a3b8; font-size: 0.85rem; margin-top: -6px; }}
        .test-status {{ margin-top: 12px; font-size: 0.85rem; color: #94a3b8; }}
        .test-status.success {{ color: #4ade80; }}
        .test-status.error {{ color: #f87171; }}
        .meta {{ margin-top: 20px; font-size: 0.85rem; color: #64748b; }}
        .timestamp {{ color: #38bdf8; }}
        .log-card {{ flex: 1 1 520px; min-width: 420px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th, td {{ padding: 10px 14px; border-bottom: 1px solid rgba(226, 232, 240, 0.08); text-align: left; font-size: 0.9rem; word-break: break-word; }}
        .log-card table {{ table-layout: auto; }}
        .log-card th:nth-child(1), .log-card td:nth-child(1) {{ width: 140px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .log-card th:nth-child(2), .log-card td:nth-child(2) {{ width: 110px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .log-card th:nth-child(3), .log-card td:nth-child(3) {{ width: 100px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        th {{ color: #a5b4fc; font-weight: 600; background-color: rgba(15, 23, 42, 0.6); }}
        tr:nth-child(even) {{ background-color: rgba(15, 23, 42, 0.35); }}
        .status-pill {{ padding: 4px 10px; border-radius: 999px; font-size: 0.8rem; font-weight: 600; }}
        .status-success {{ background: rgba(34, 197, 94, 0.2); color: #4ade80; }}
        .status-warning {{ background: rgba(251, 191, 36, 0.2); color: #facc15; }}
        .status-error {{ background: rgba(248, 113, 113, 0.2); color: #f87171; }}
        .status-skipped {{ background: rgba(148, 163, 184, 0.2); color: #cbd5f5; }}
        .empty-state {{ text-align: center; color: #94a3b8; padding: 14px 0; }}
        .scheduler-execlog {{ width: 100%; border-collapse: collapse; margin-top: 10px; background-color: rgba(15, 23, 42, 0.6); border-radius: 8px; overflow: hidden; }}
        .scheduler-execlog th, .scheduler-execlog td {{ padding: 10px 12px; border-bottom: 1px solid #334155; text-align: left; font-size: 0.9rem; }}
        .scheduler-execlog td:nth-child(4) {{ white-space: normal; }}
        .scheduler-execlog th:nth-child(5), .scheduler-execlog td:nth-child(5) {{ text-align: right; white-space: nowrap; }}
        @media (max-width: 900px) {{
            .scheduler-layout {{ flex-direction: column; }}
            .card {{ width: 100%; padding: 18px; }}
            button {{ width: 100%; }}
        }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link active">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>

    <h1>任务调度控制</h1>
    <p class="hint">设置行情抽取与 AI 交互的周期，变更会实时更新后台 APScheduler 任务。</p>
    <div class="scheduler-layout">
    <section class="card primary-card">
        <form method="post" action="/scheduler/update">
            <label for="market-interval">
                行情抽取间隔（秒）
                <div class="control-line">
                    <input id="market-interval" name="market_interval" type="number" min="30" max="3600" value="{market_interval}" required>
                    <button type="button" class="test-trigger" id="market-test-btn" data-url="/api/scheduler/test-market" data-label="行情测试">行情测试</button>
                </div>
                <span class="hint">用于调用 OKX 行情 API、写入 Influx、生成最新指标。</span>
            </label>
            <label for="ai-interval">
                AI 交互间隔（秒）
                <div class="control-line">
                    <input id="ai-interval" name="ai_interval" type="number" min="60" max="7200" value="{ai_interval}" required>
                    <button type="button" class="test-trigger" id="ai-test-btn" data-url="/api/scheduler/test-ai" data-label="AI 交互测试">AI 交互测试</button>
                </div>
                <span class="hint">触发 LLM 信号 → 风控 → 模拟下单流程的周期。</span>
            </label>
            <label for="liquidation-interval">
                爆仓订单流检查间隔（秒）
                <div class="control-line">
                    <input id="liquidation-interval" name="liquidation_interval" type="number" min="30" max="3600" value="{liquidation_interval}" required>
                    <button type="button" class="test-trigger" id="liquidation-test-btn" data-url="/api/scheduler/test-liquidation" data-label="爆仓流巡检">爆仓流巡检</button>
                </div>
                <span class="hint">扫描 Influx 爆仓流特征，校验自动抄底/摸顶守护逻辑。</span>
            </label>
            <button type="submit">保存调度设置</button>
        </form>
        <div class="meta">
            最近更新：<span class="timestamp">{updated_at}</span>
        </div>
        <p class="hint test-status" id="scheduler-test-status">点击按钮会立即触发行情、AI 或爆仓巡检任务，用于验证调度逻辑。</p>
    </section>
    <section class="card log-card">
        <h2>最近执行记录</h2>
        <table class="scheduler-execlog">
            <thead>
                <tr>
                    <th>时间</th>
                    <th>任务</th>
                    <th>状态</th>
                    <th>描述信息</th>
                    <th>耗时</th>
                </tr>
            </thead>
            <tbody>
                {log_rows}
            </tbody>
        </table>
    </section>
    </div>
    <script>
      (function() {{
        const statusEl = document.getElementById('scheduler-test-status');
        const buttons = Array.from(document.querySelectorAll('.test-trigger')).map((button) => ({{
          button,
          url: button.dataset.url,
          label: button.dataset.label || button.textContent || '任务',
        }}));
        const setStatus = (text, level) => {{
          if (!statusEl) return;
          statusEl.textContent = text;
          statusEl.className = ['hint', 'test-status', level || ''].join(' ').trim();
        }};
        const runTest = (button, meta) => {{
          if (!button) return;
          button.disabled = true;
          setStatus(`正在触发${{meta.label}}...`, '');
          fetch(meta.url, {{ method: 'POST' }})
            .then((res) => {{
              if (!res.ok) throw new Error('请求失败');
              return res.json();
            }})
            .then((data) => {{
              const status = (data.status || '').toLowerCase();
              const detail = data.detail || '无额外信息';
              if (status === 'error') {{
                setStatus(`${{meta.label}}失败：${{detail}}`, 'error');
              }} else {{
                setStatus(`${{meta.label}}完成：${{detail}}`, 'success');
              }}
            }})
            .catch((err) => {{
              setStatus(`${{meta.label}}失败：${{err.message}}`, 'error');
            }})
            .finally(() => {{
              button.disabled = false;
            }});
        }};
        buttons.forEach((meta) => {{
          if (!meta.button || !meta.url) return;
          meta.button.addEventListener('click', () => runTest(meta.button, meta));
        }});
      }})();
    </script>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""

PROMPT_EDITOR_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>提示词模板</title>
    <style>
        body {{ font-family: "Segoe UI", "PingFang SC", Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin-top: 0; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; flex-wrap: wrap; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
        .prompt-layout {{ max-width: 960px; margin: 0 auto; display: grid; gap: 24px; }}
        .card {{ background-color: #1e293b; border-radius: 12px; padding: 24px 28px; box-shadow: 0 15px 45px rgba(15, 23, 42, 0.55); }}
        .card.helper {{ background-color: rgba(15, 23, 42, 0.8); }}
        label {{ display: block; margin-bottom: 8px; color: #94a3b8; font-size: 0.95rem; }}
        textarea {{ width: 100%; min-height: 320px; border-radius: 10px; border: 1px solid #334155; background-color: #0f172a; color: #e2e8f0; padding: 14px; font-size: 0.95rem; line-height: 1.5; resize: vertical; }}
        textarea:focus {{ outline: none; border-color: #38bdf8; box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2); }}
        button {{ background-color: #38bdf8; color: #0f172a; border: none; padding: 10px 20px; border-radius: 10px; cursor: pointer; font-weight: 600; margin-top: 16px; }}
        button:hover {{ background-color: #0ea5e9; }}
        .hint {{ color: #94a3b8; font-size: 0.9rem; margin-top: 8px; }}
        .notice {{ padding: 12px 16px; border-radius: 10px; margin: 12px 0 0; font-size: 0.9rem; }}
        .notice.success {{ background: rgba(16, 185, 129, 0.18); color: #bbf7d0; border: 1px solid rgba(16, 185, 129, 0.4); }}
        .placeholder-list {{ list-style: none; padding-left: 0; margin: 0; display: grid; gap: 10px; }}
        .placeholder-list li {{ background-color: rgba(15, 23, 42, 0.65); border-radius: 8px; padding: 10px 12px; display: flex; gap: 12px; align-items: center; }}
        .model-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-top: 12px; }}
        .model-grid.empty {{ color: #94a3b8; padding: 12px 0; }}
        .model-card {{ border-radius: 12px; padding: 14px; border: 1px solid rgba(148, 163, 184, 0.25); background: rgba(15, 23, 42, 0.65); }}
        .model-card header {{ display: flex; flex-direction: column; gap: 4px; }}
        .model-card h3 {{ margin: 0; font-size: 1rem; }}
        .model-card span {{ color: #94a3b8; font-size: 0.85rem; }}
        .model-card .status-label {{ margin-top: 10px; font-size: 0.85rem; font-weight: 600; }}
        .model-card.status.enabled {{ border-color: rgba(74, 222, 128, 0.5); background: rgba(22, 163, 74, 0.18); }}
        .model-card.status.enabled .status-label {{ color: #4ade80; }}
        .model-card.status.disabled {{ opacity: 0.6; }}
        .model-card.status.disabled .status-label {{ color: #f87171; }}
        .instrument-chips {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0 8px; }}
        .instrument-chips.empty {{ color: #94a3b8; }}
        .instrument-chip {{ background-color: rgba(56, 189, 248, 0.15); color: #bae6fd; padding: 6px 12px; border-radius: 999px; font-size: 0.85rem; border: 1px solid rgba(56, 189, 248, 0.4); }}
        .divider {{ border: none; border-top: 1px solid rgba(148, 163, 184, 0.3); margin: 18px 0; }}
        code {{ background-color: rgba(56, 189, 248, 0.15); padding: 2px 8px; border-radius: 6px; font-family: "JetBrains Mono", "Fira Code", Consolas, monospace; font-size: 0.85rem; color: #38bdf8; }}
        @media (max-width: 720px) {{
            body {{ padding: 18px 14px; }}
            .card {{ padding: 20px; }}
            .prompt-layout {{ gap: 18px; }}
        }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link active">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>

    <div class="prompt-layout">
        <header>
            <h1>提示词模板</h1>
            <p class="hint">模板中可以自由编排说明，让 LLM 在下单前参考实时仓位与风控上下文。使用 <code>{{</code> 与 <code>}}</code> 输出字面花括号。</p>
            {notice_block}
        </header>
        <section class="card">
            <form method="post" action="/prompts/update">
                <label for="prompt-template">当前模板</label>
                <textarea id="prompt-template" name="prompt_template" spellcheck="false">{prompt_text}</textarea>
                <p class="hint">保存后立即生效，新的信号请求都会使用最新提示词。</p>
                <button type="submit">保存模板</button>
            </form>
        </section>
        <section class="card helper">
            <h2>启用模型</h2>
            <p class="hint">{model_hint}</p>
            {model_block}
            <p class="hint">各模型独立思考、互不影响。仅“已启用”的模型会被提示词引用。</p>
            <hr class="divider">
            <h2>默认交易币对</h2>
            {instrument_block}
            <p class="hint">提示词中的 <code>{{model_id}}</code> 会与下方币对组合，按启用的模型 ID 独立替换。</p>
            <p class="hint">数据来源：“币对配置”页中的“需要交易的币对”列表，并会自动保持同步。</p>
            <hr class="divider">
            <h2>可用占位符</h2>
            <ul class="placeholder-list">
                {placeholder_list}
            </ul>
            <p class="hint">缺失的占位符会被自动替换为空字符串，以避免影响模板渲染。</p>
        </section>
    </div>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""


def _render_scheduler_page(settings: dict) -> str:
    esc = _escape
    updated_at = _format_asia_shanghai(settings.get("updated_at"))
    job_labels = {
        "market": "行情采集",
        "ai": "AI 交互",
        "analytics": "指标刷新",
        "liquidation": "爆仓巡检",
    }
    status_labels = {
        "success": "成功",
        "warning": "部分成功",
        "error": "失败",
        "skipped": "跳过",
    }
    log_rows: list[str] = []
    for entry in settings.get("execution_log") or []:
        timestamp = esc(_format_asia_shanghai(entry.get("timestamp")))
        job = esc(job_labels.get(entry.get("job"), entry.get("job") or "--"))
        status_key = str(entry.get("status") or "unknown")
        status_text = esc(status_labels.get(status_key, status_key))
        status_class = f"status-{status_key}"
        detail = esc(entry.get("detail") or "--")
        duration = entry.get("duration_seconds")
        duration_text = f"{float(duration):.2f}s" if duration is not None else "--"
        log_rows.append(
            "<tr>"
            f"<td>{timestamp}</td>"
            f"<td>{job}</td>"
            f"<td><span class=\"status-pill {status_class}\">{status_text}</span></td>"
            f"<td>{detail}</td>"
            f"<td>{duration_text}</td>"
            "</tr>"
        )
    if not log_rows:
        log_rows.append("<tr><td colspan='5' class='empty-state'>暂无执行记录</td></tr>")
    return SCHEDULER_TEMPLATE.format(
        market_interval=esc(settings.get("market_interval")),
        ai_interval=esc(settings.get("ai_interval")),
        liquidation_interval=esc(settings.get("liquidation_interval")),
        updated_at=esc(updated_at),
        log_rows="\n".join(log_rows),
    )


def _format_number(value: object, digits: int = 2) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number:.{digits}f}"


LIQUIDATION_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>爆仓监控</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background: #38bdf8; color: #0f172a; }}
        h1 {{ margin-bottom: 1rem; }}
        .controls {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: center; margin-bottom: 1rem; }}
        .controls label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        select {{ min-width: 200px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; padding: 8px 10px; }}
        input[type="number"] {{ min-width: 160px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; padding: 8px 10px; }}
        table {{ width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 12px; overflow: hidden; }}
        th, td {{ padding: 10px 14px; border-bottom: 1px solid #334155; text-align: left; }}
        th {{ background: rgba(15, 23, 42, 0.6); font-size: 0.9rem; }}
        tr:nth-child(even) {{ background: rgba(30, 41, 59, 0.7); }}
        .timestamp {{ color: #38bdf8; }}
        .empty-state {{ text-align: center; color: #94a3b8; padding: 16px; }}
        .wave-section {{ background: #1e293b; border-radius: 12px; padding: 16px; margin-bottom: 1rem; border: 1px solid #334155; }}
        .wave-notification {{
            margin-bottom: 16px;
            padding: 12px 16px;
            border-radius: 12px;
            border: 1px solid rgba(56, 189, 248, 0.4);
            background: rgba(15, 23, 42, 0.65);
            color: #e2e8f0;
            font-size: 0.9rem;
            display: none;
        }}
        .wave-notification.success {{ border-color: rgba(74, 222, 128, 0.8); color: #bbf7d0; }}
        .wave-notification.error {{ border-color: rgba(248, 113, 113, 0.8); color: #fecaca; }}
        .wave-notification.show {{ display: block; }}
        .wave-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }}
        .wave-header h2 {{ margin: 0; font-size: 1rem; color: #f1f5f9; }}
        .wave-status {{ font-size: 0.85rem; padding: 4px 12px; border-radius: 999px; background: rgba(56, 189, 248, 0.15); color: #38bdf8; }}
        .detection-table {{ width: 100%; border-collapse: collapse; border: 1px solid #334155; border-radius: 12px; overflow: hidden; }}
        .detection-table th, .detection-table td {{ padding: 8px 10px; font-size: 0.85rem; border-bottom: 1px solid #334155; }}
        .detection-table th {{ background: rgba(15, 23, 42, 0.5); font-weight: 600; }}
        .detection-table tr:nth-child(even) {{ background: rgba(30, 41, 59, 0.4); }}
        .wave-signal-strong {{ color: #fbbf24; }}
        .wave-signal-bottom {{ color: #4ade80; }}
        .wave-signal-warn {{ color: #f87171; }}
        .order-btn {{
            padding: 4px 10px;
            border-radius: 999px;
            border: 1px solid #38bdf8;
            background: transparent;
            color: #38bdf8;
            cursor: pointer;
            font-size: 0.78rem;
        }}
        .order-action-group {{
            display: flex;
            gap: 6px;
        }}
        .order-btn[disabled] {{
            opacity: 0.5;
            cursor: not-allowed;
        }}
        .order-btn.active {{
            background: #38bdf8;
            color: #0f172a;
            border-color: #38bdf8;
            font-weight: 700;
        }}
        .order-summary {{
            margin: 1rem 0;
            padding: 14px 18px;
            border-radius: 12px;
            background: rgba(15, 23, 42, 0.7);
            border: 1px solid rgba(56, 189, 248, 0.4);
            display: grid;
            gap: 8px;
            max-width: 680px;
        }}
        .order-summary .summary-header {{
            font-weight: 700;
            color: #38bdf8;
            font-size: 0.95rem;
        }}
        .order-summary .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 10px;
        }}
        .order-summary .summary-row {{
            display: flex;
            flex-direction: column;
            gap: 4px;
        }}
        .order-summary .summary-label {{
            font-size: 0.75rem;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }}
        .order-summary .summary-value {{
            font-size: 1rem;
            font-weight: 600;
            color: #e2e8f0;
        }}
    .payload-panel {{
        background: #0f172a;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 12px 16px;
        margin: 1rem 0;
        font-family: "Fira Code", Consolas, monospace;
        color: #e2e8f0;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.5);
    }}
    .payload-panel h3 {{
        margin: 0 0 6px;
        font-size: 0.9rem;
        color: #38bdf8;
    }}
    .payload-panel pre {{
        margin: 0;
        white-space: pre-wrap;
        word-break: break-word;
        font-size: 0.75rem;
        line-height: 1.4;
        background: rgba(15, 23, 42, 0.4);
        padding: 8px;
        border-radius: 8px;
        border: 1px dashed rgba(56, 189, 248, 0.5);
    }}
    .signal-subtext {{ font-size: 0.75rem; color: #94a3b8; margin-top: 4px; display: block; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link active">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>
    <h1>OKX 爆仓监控</h1>
    <div class="controls">
        <label>选择合约
            <select id="instrument-select">
                {instrument_options}
            </select>
        </label>
        <label>最小爆仓数量
            <input type="number" id="min-size" min="0" step="0.01" placeholder="如 10">
        </label>
        <label>最小爆仓金额（USDT）
            <input type="number" id="min-notional" min="0" step="10" placeholder="如 100000">
        </label>
        <label>Auto Order Size 自动加仓手数
            <input type="number" id="auto-order-size" min="0.001" step="0.001" value="1">
        </label>
        <label>Auto Order Notional 自动下单金额（USDT）
            <input type="number" id="auto-order-notional" min="10" step="10" value="100">
        </label>
        <label>自动下单方式
            <select id="auto-order-mode">
                <option value="size">按手数</option>
                <option value="notional" selected>按金额</option>
            </select>
        </label>
        <span>最后更新：<span class="timestamp" id="liquidations-updated">{updated_at}</span></span>
    </div>
    <div class="payload-panel" id="okx-payload-panel" hidden>
        <h3>OKX Payload</h3>
        <pre id="okx-payload-display">等待买卖操作后显示请求体。</pre>
    </div>
    <div id="wave-notification" class="wave-notification" aria-live="polite"></div>
    <div id="last-order-details" class="order-summary" hidden>
        <div class="summary-header">最近下单</div>
        <div class="summary-grid">
            <div class="summary-row">
                <span class="summary-label">类型</span>
                <span class="summary-value" id="last-order-side">--</span>
            </div>
            <div class="summary-row">
                <span class="summary-label">下单币对</span>
                <span class="summary-value" id="last-order-pair">--</span>
            </div>
            <div class="summary-row">
                <span class="summary-label">下单数量</span>
                <span class="summary-value" id="last-order-qty">--</span>
            </div>
            <div class="summary-row">
                <span class="summary-label">下单金额 (USDT)</span>
                <span class="summary-value" id="last-order-amount">--</span>
            </div>
        </div>
    </div>
    <div class="wave-section">
        <div class="wave-header">
            <h2>Real-Time Liquidation Waves 实时爆仓波次监测</h2>
            <span class="wave-status" id="wave-status-text">等待数据</span>
        </div>
        <table class="detection-table">
            <thead>
                <tr>
                    <th>合约</th>
                    <th>波次</th>
                    <th>状态</th>
                    <th>FLV / 均值</th>
                    <th>最新价格</th>
                    <th>价格涨跌幅</th>
                    <th>爆仓弹性(涨跌1%触发)</th>
                    <th>PC 价格压力(潜在剧烈波动)</th>
                    <th>密度</th>
                    <th>LPI 强平压力指数</th>
                    <th>信号</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody id="wave-body">
                <tr><td colspan="12" class="empty-state">等待数据...</td></tr>
            </tbody>
        </table>
    </div>
    <table>
        <thead>
            <tr>
                <th>时间</th>
                <th>合约</th>
                <th>多单爆仓</th>
                <th>空单爆仓</th>
                <th>净爆仓</th>
                <th>最新价</th>
                <th>爆仓金额</th>
            </tr>
        </thead>
        <tbody id="liquidations-body">
            {rows}
        </tbody>
    </table>
    <script>
      (function() {{
        const select = document.getElementById('instrument-select');
        const minSizeInput = document.getElementById('min-size');
        const minNotionalInput = document.getElementById('min-notional');
        const autoNotionalInput = document.getElementById('auto-order-notional');
        const payloadDisplay = document.getElementById('okx-payload-display');
        const notificationBar = document.getElementById('wave-notification');
        const orderSummaryPanel = document.getElementById('last-order-details');
        const orderSideValue = document.getElementById('last-order-side');
        const orderPairValue = document.getElementById('last-order-pair');
        const orderQtyValue = document.getElementById('last-order-qty');
        const orderAmountValue = document.getElementById('last-order-amount');
        let notificationTimer = null;
        const hideNotification = () => {{
          if (!notificationBar) {{ return; }}
          notificationBar.classList.remove('show', 'success', 'error');
          notificationBar.textContent = '';
        }};
        const showNotification = (message, status = 'info') => {{
          if (!notificationBar) {{ return; }}
          notificationBar.textContent = message;
          notificationBar.classList.remove('success', 'error');
          if (status === 'success') {{
            notificationBar.classList.add('success');
          }} else if (status === 'error') {{
            notificationBar.classList.add('error');
          }}
          notificationBar.classList.add('show');
          if (notificationTimer) {{ clearTimeout(notificationTimer); }}
          notificationTimer = setTimeout(() => {{ hideNotification(); }}, 6000);
        }};
        const waveTableBody = document.getElementById('wave-body');
        const FETCH_LIMIT = 400;
        let latestItems = [];
        let activeSignals = new Map();
        const fmtNumber = (value, digits) => {{
          if (value === null || value === undefined || value === '') {{ return '-'; }}
          const num = Number(value);
          if (!isFinite(num)) {{ return '-'; }}
          return num.toFixed(digits);
        }};
        const updateOrderSummary = ({{ sideLabel, pairLabel, qtyLabel, amountLabel }}) => {{
          if (!orderSummaryPanel) {{ return; }}
          if (orderSideValue) {{ orderSideValue.textContent = sideLabel || '--'; }}
          if (orderPairValue) {{ orderPairValue.textContent = pairLabel || '--'; }}
          if (orderQtyValue) {{ orderQtyValue.textContent = qtyLabel || '--'; }}
          if (orderAmountValue) {{ orderAmountValue.textContent = amountLabel || '--'; }}
          orderSummaryPanel.hidden = false;
        }};
        const fmtTimestamp = (value) => {{
          if (!value) {{ return '--'; }}
          const date = new Date(value);
          if (!isFinite(date)) {{ return value; }}
          return new Intl.DateTimeFormat('zh-CN', {{
            timeZone: 'Asia/Shanghai',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
          }}).format(date);
        }};
        const _toAbsNumber = (value) => {{
          const num = Number(value);
          return Number.isFinite(num) ? Math.abs(num) : 0;
        }};
        const extractQuantity = (item) => {{
          if (!item) {{ return 0; }}
          const netQty = _toAbsNumber(item.net_qty);
          const longQty = _toAbsNumber(item.long_qty);
          const shortQty = _toAbsNumber(item.short_qty);
          return Math.max(netQty, longQty, shortQty);
        }};
        const computeNotional = (item) => {{
          if (!item) {{ return null; }}
          const provided = Number(item.notional_value);
          if (Number.isFinite(provided)) {{
            return provided;
          }}
          const qty = extractQuantity(item);
          const price = Number(item.last_price);
          if (qty > 0 && Number.isFinite(price)) {{
            return qty * price;
          }}
          return null;
        }};
        const describeLeElasticity = (value) => {{
          if (!Number.isFinite(value) || value <= 0) {{
            return '';
          }}
          if (value >= 3) {{
            return '跌幅敏感：单位跌幅即可触发超大规模强平';
          }}
          if (value >= 2) {{
            return '高弹性：同等跌幅触发强平明显偏多';
          }}
          if (value >= 1) {{
            return '偏高：跌得少却爆得多';
          }}
          if (value >= 0.5) {{
            return '中性：跌幅与强平量大致匹配';
          }}
          return '低弹性：需更大跌幅才触发强平';
        }};
        const formatLeElasticity = (value) => {{
          if (!Number.isFinite(value)) {{
            return '-';
          }}
          const baseText = fmtNumber(value, 2);
          if (value === 0) {{
            return baseText;
          }}
          const explanation = describeLeElasticity(value);
          return explanation ? `${{baseText}} (${{explanation}})` : baseText;
        }};
        const describePcPressure = (value) => {{
          if (!Number.isFinite(value) || value <= 0) {{
            return '';
          }}
          if (value >= 1_000_000) {{
            return '压力爆表，谨慎接盘';
          }}
          if (value >= 300_000) {{
            return '高压，关注回撤';
          }}
          if (value >= 100_000) {{
            return '中等压力';
          }}
          return '压力有限';
        }};
        const formatPcPressure = (value) => {{
          if (!Number.isFinite(value)) {{
            return '-';
          }}
          const baseText = fmtNumber(value, 2);
          const label = describePcPressure(value);
          return label ? `${{baseText}} (${{label}})` : baseText;
        }};
        const signalOrderConfig = (signal) => {{
          const code = (signal.signal_code || '').toLowerCase();
          if (code === 'bottom_absorb') {{
            return {{ side: 'buy', label: '买入' }};
          }}
          if (code === 'top_signal' || code === 'short_reversal') {{
            return {{ side: 'sell', label: '卖出' }};
          }}
          return null;
        }};
        const renderOrderAction = (signal) => {{
          const instrument = signal.instrument || '';
          if (!instrument) {{
            return '<span style="opacity:0.6">--</span>';
          }}
          const config = signalOrderConfig(signal);
          const recommended = config?.side || null;
          const buyActive = recommended === 'buy' ? ' active' : '';
          const sellActive = recommended === 'sell' ? ' active' : '';
          return `
            <div class="order-action-group">
              <button type="button" class="order-btn${{buyActive}}" data-instrument="${{instrument}}" data-side="buy">买入</button>
              <button type="button" class="order-btn${{sellActive}}" data-instrument="${{instrument}}" data-side="sell">卖出</button>
            </div>`;
        }};
        const normalizeInstrument = (value) => (value || '').trim().toUpperCase();
        const sortWaveSignals = (signals) => {{
          if (!Array.isArray(signals)) {{ return []; }}
          const priority = {{
            'ETH-USDT-SWAP': 0,
            'ETHUSDT': 0,
            'BTC-USDT-SWAP': 1,
            'BTCUSDT': 1,
          }};
          return [...signals].sort((a, b) => {{
            const instA = normalizeInstrument(a?.instrument);
            const instB = normalizeInstrument(b?.instrument);
            const rankA = Object.prototype.hasOwnProperty.call(priority, instA) ? priority[instA] : 2;
            const rankB = Object.prototype.hasOwnProperty.call(priority, instB) ? priority[instB] : 2;
            if (rankA !== rankB) {{
              return rankA - rankB;
            }}
            return instA.localeCompare(instB);
          }});
        }};
        const renderWaveTable = (signals) => {{
          const body = waveTableBody;
          if (!body) {{ return; }}
          if (!Array.isArray(signals) || !signals.length) {{
            body.innerHTML = '<tr><td colspan="11" class="empty-state">等待数据...</td></tr>';
            return;
          }}
          const rows = sortWaveSignals(signals).map((signal) => {{
            const metrics = signal.metrics || {{}};
            const flvText = `${{fmtNumber(metrics.flv, 2)}} / ${{fmtNumber(metrics.baseline, 2)}}`;
            const priceChange = Number(metrics.price_change_pct);
            const priceText = Number.isFinite(priceChange)
              ? `${{priceChange >= 0 ? '+' : ''}}${{priceChange.toFixed(2)}}%`
              : '-';
            const endPrice = Number(metrics.end_price);
            const latestPriceText = Number.isFinite(endPrice) ? fmtNumber(endPrice, 4) : '-';
            const leValue = Number(metrics.le);
            const leText = formatLeElasticity(leValue);
            const pcValue = Number(metrics.pc);
            const pcText = Number.isFinite(pcValue) ? formatPcPressure(pcValue) : '-';
            const densityText = metrics.density_per_min
              ? `${{fmtNumber(metrics.density_per_min, 2)}}/min`
              : '-';
            const lpiText = metrics.lpi === undefined ? '-' : fmtNumber(metrics.lpi, 2);
            const signalClass = signal.signal_class || '';
            const detailText = signal.liquidation_side_label || '';
            const detailHtml = detailText ? `<span class="signal-subtext">${{detailText}}</span>` : '';
            const signalText = signal.signal || '???';
            return `
              <tr>
                <td>${{signal.instrument || '--'}}</td>
                <td>${{signal.wave || '--'}}</td>
                <td>${{signal.status || '--'}}</td>
                <td>${{flvText}}</td>
                <td>${{latestPriceText}}</td>
                <td>${{priceText}}</td>
                <td>${{leText}}</td>
                <td>${{pcText}}</td>
                <td>${{densityText}}</td>
                <td>${{lpiText}}</td>
                <td class="${{signalClass}}">
                  <div>${{signalText}}</div>
                  ${{detailHtml}}
                </td>
                <td>${{renderOrderAction(signal)}}</td>
              </tr>`;
          }});
          body.innerHTML = rows.join('');
        }};
        const placeAutoOrder = (instrument, side, button) => {{
          if (!instrument || !side) {{
            showNotification('缺少合约信息，无法下单。', 'error');
            return;
          }}
          const notionalValue = Number(autoNotionalInput?.value);
          if (!Number.isFinite(notionalValue) || notionalValue <= 0) {{
            showNotification('请在 Auto Order Notional 输入大于 0 的金额。', 'error');
            return;
          }}
          const originalText = button.textContent;
          button.disabled = true;
          button.textContent = '下单中...';
          const requestBody = {{
            instrument_id: instrument,
            side,
            size: notionalValue,
            order_mode: 'notional',
          }};
          if (payloadDisplay) {{
            payloadDisplay.textContent = JSON.stringify(requestBody, null, 2);
          }}
          fetch('/api/okx/wave-order', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify(requestBody),
          }})
            .then(async (res) => {{
              const data = await res.json().catch(() => ({{}}));
              return [res.ok, data];
            }})
            .then(([ok, data]) => {{
              if (ok) {{
                const est = Number(data.notional_estimate);
                const confirmation = Number.isFinite(est) ? est : notionalValue;
                const actionText = side === 'buy' ? '做多' : '做空';
                const instrumentLabel = (data.instrument_id || instrument || '--').toUpperCase();
                const baseSizeValue = Number(data.base_size);
                const contractSizeValue = Number(data.size);
                const qtyValue = Number.isFinite(baseSizeValue) ? baseSizeValue : contractSizeValue;
                const qtyDigits = Number.isFinite(qtyValue) && Math.abs(qtyValue) < 1 ? 4 : 2;
                const qtyText = Number.isFinite(qtyValue) ? fmtNumber(qtyValue, qtyDigits) : '--';
                const amountText = fmtNumber(confirmation, 2);
                const invalidAmount = amountText === '-' || amountText === '--';
                const amountLabel = invalidAmount ? '--' : `${{amountText}} USDT`;
                updateOrderSummary({{
                  sideLabel: actionText,
                  pairLabel: instrumentLabel,
                  qtyLabel: qtyText,
                  amountLabel,
                }});
                showNotification(`已提交${{instrumentLabel}} ${{actionText}}订单，数量 ${{qtyText}}，预估 $${{amountText}} USDT。`, 'success');
              }} else {{
                showNotification(data.detail || '下单失败，请稍后重试。', 'error');
              }}
            }})
            .catch((err) => {{
              console.error(err);
              showNotification('下单失败，请稍后重试。', 'error');
            }})
            .finally(() => {{
              button.disabled = false;
              button.textContent = originalText;
            }});
        }};
        waveTableBody?.addEventListener('click', (event) => {{
          const target = event.target.closest('.order-btn');
          if (!target || target.disabled) {{
            return;
          }}
          const instrument = target.getAttribute('data-instrument');
          const side = target.getAttribute('data-side');
          placeAutoOrder(instrument, side, target);
        }});
        const applyFilters = (items) => {{
          const minQty = Math.max(0, Number(minSizeInput.value) || 0);
          const minNotional = Math.max(0, Number(minNotionalInput.value) || 0);
          const instrumentFilter = (select.value || '').trim().toUpperCase();
          return items.filter((item) => {{
            const inst = (item.instrument_id || '').trim().toUpperCase();
            if (instrumentFilter && inst !== instrumentFilter) {{
              return false;
            }}
            const qty = extractQuantity(item);
            const notional = computeNotional(item) || 0;
            if (minQty && qty < minQty) {{
              return false;
            }}
            if (minNotional && notional < minNotional) {{
              return false;
            }}
            return true;
          }});
        }};
                const renderRows = () => {{

          const body = document.getElementById('liquidations-body');

          body.innerHTML = '';

          const filtered = applyFilters(latestItems);

          if (!filtered.length) {{

            body.innerHTML = '<tr><td colspan="7" class="empty-state">等待数据...</td></tr>';

            return;

          }}

          filtered.forEach((item) => {{

            const tr = document.createElement('tr');

            const notional = computeNotional(item);

            tr.innerHTML = `

              <td>${{fmtTimestamp(item.timestamp)}}</td>

              <td>${{item.instrument_id || '--'}}</td>

              <td>${{fmtNumber(item.long_qty, 2)}}</td>

              <td>${{fmtNumber(item.short_qty, 2)}}</td>

              <td>${{fmtNumber(item.net_qty, 2)}}</td>

              <td>${{fmtNumber(item.last_price, 4)}}</td>

              <td>${{notional === null ? '-' : fmtNumber(notional, 2)}}</td>

              <td>

                <div style="display:flex;gap:6px;">

                  <button class="order-btn${{activeSignals.get(item.instrument_id || '--') === 'buy' ? ' active' : ''}}" data-side="buy" data-inst="${{item.instrument_id || '--'}}">买入</button>

                  <button class="order-btn${{activeSignals.get(item.instrument_id || '--') === 'sell' ? ' active' : ''}}" data-side="sell" data-inst="${{item.instrument_id || '--'}}">卖出</button>

                </div>

              </td>`;

            body.appendChild(tr);

          }});

        }};
        function refresh() {{
          const params = new URLSearchParams({{ limit: FETCH_LIMIT.toString() }});
          fetch('/api/streams/liquidations/latest?' + params.toString())
            .then((res) => res.json())
            .then((data) => {{
              latestItems = Array.isArray(data.items) ? data.items : [];
              renderRows();
              renderWaveTable(data.wave_signals || []);
              const updated = document.getElementById('liquidations-updated');
              if (updated) {{ updated.textContent = fmtTimestamp(data.updated_at); }}
              const waveStatus = document.getElementById('wave-status-text');
              if (waveStatus) {{
                waveStatus.textContent = data.wave_summary || '????';
              }}
            }})
            .catch((err) => console.error(err));
        }}
        select.addEventListener('change', () => {{
          renderRows();
        }});
        [minSizeInput, minNotionalInput].forEach((input) => {{
          input.addEventListener('input', () => renderRows());
        }});
        refresh();
        setInterval(refresh, 1000);
      }})();
    </script>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""


ORDERBOOK_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>市场深度</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background: #38bdf8; color: #0f172a; }}
        h1 {{ margin-bottom: 1rem; }}
        .controls {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: center; margin-bottom: 1rem; }}
        .controls label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        select {{ min-width: 160px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; padding: 8px 10px; }}
        .timeframe-toggle {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        .timeframe-buttons {{ display: flex; gap: 8px; }}
        .timeframe-buttons button {{ background: rgba(51, 65, 85, 0.6); border: 1px solid rgba(148, 163, 184, 0.35); color: #e2e8f0; padding: 6px 16px; border-radius: 999px; cursor: pointer; font-size: 0.9rem; transition: background 0.2s ease, color 0.2s ease, border-color 0.2s ease; }}
        .timeframe-buttons button.active {{ background: #38bdf8; border-color: #38bdf8; color: #0f172a; }}
        .timeframe-buttons button:hover {{ border-color: rgba(148, 163, 184, 0.8); }}
        .book-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(410px, 1fr)); gap: 12px; column-gap: 20px; align-items: stretch; width: 100%; margin: 0; justify-items: start; justify-content: start; }}
        .book-grid.two-cols {{ grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; column-gap: 16px; width: 100%; margin: 0; justify-items: start; justify-content: start; }}
        .book-grid.single-card {{ grid-template-columns: minmax(0, 1fr); justify-items: stretch; }}
        .book-grid.single-card .orderbook-chart {{ width: 100%; max-width: 100%; }}
        @media (max-width: 1200px) {{ .book-grid.two-cols {{ grid-template-columns: 1fr; column-gap: 12px; }} }}
        .orderbook-chart {{ background: #0f1b2d; border-radius: 16px; padding: 18px 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); border: 1px solid rgba(148, 163, 184, 0.15); display: flex; flex-direction: column; gap: 14px; width: calc(100% - 40px); box-sizing: border-box; }}
        .orderbook-chart.tone-positive {{ border-color: rgba(74, 222, 128, 0.4); }}
        .orderbook-chart.tone-negative {{ border-color: rgba(248, 113, 113, 0.35); }}
        .orderbook-chart.tone-neutral {{ border-color: rgba(59, 130, 246, 0.25); }}
        .chart-header {{ display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; }}
        .chart-header h2 {{ margin: 0; font-size: 1.15rem; }}
        .chart-header small {{ display: block; font-size: 0.8rem; color: #94a3b8; margin-top: 4px; }}
        .chart-prices {{ width: 100%; display: flex; flex-direction: column; gap: 12px; font-size: 1rem; color: #f8fafc; }}
        .chart-price-group {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; width: 100%; }}
        .chart-prices span {{ background: rgba(248, 250, 252, 0.08); padding: 8px 14px; border-radius: 12px; text-align: center; }}
        .chart-prices span.mid {{ background: rgba(59, 130, 246, 0.18); color: #bfdbfe; }}
        .chart-stats {{ display: grid; grid-template-columns: repeat(5, minmax(120px, 1fr)); column-gap: 12px; row-gap: 8px; font-size: 0.9rem; color: #cbd5f5; width: 100%; align-items: stretch; }}
        @media (max-width: 1200px) {{ .chart-stats {{ grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); row-gap: 10px; }} }}
        .chart-stats .stat-pill {{ background: rgba(148, 163, 184, 0.1); padding: 8px 12px; border-radius: 12px; text-align: center; }}
        .chart-stats .stat-accent {{ background: rgba(34, 197, 94, 0.28); border: 1px solid rgba(34, 197, 94, 0.45); color: #dcfce7; font-weight: 600; }}
        .chart-canvas {{ width: 100%; height: 320px; }}
        .chart-insight {{ margin: 0; font-size: 0.9rem; color: #e2e8f0; }}
        .chart-insight.positive {{ color: #4ade80; }}
        .chart-insight.negative {{ color: #f87171; }}
        .chart-insight.neutral {{ color: #94a3b8; }}
        .section-heading {{ margin: 0 0 1rem; font-size: 1.25rem; color: #e2e8f0; }}
        .empty-state {{ color: #94a3b8; padding: 16px; text-align: center; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link active">市场深度</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>
    <h1>OKX 市场深度</h1>
    <div class="controls">
        <label>选择合约
            <select id="orderbook-instrument">
                {instrument_options}
            </select>
        </label>
        <label>分析档位
            <select id="depth-levels">
                {level_options}
            </select>
        </label>
        <div class="timeframe-toggle">
            <span>净深度周期</span>
            <div class="timeframe-buttons">
                <button type="button" data-timeframe="5m">5m</button>
                <button type="button" data-timeframe="15m" class="active">15m</button>
                <button type="button" data-timeframe="1h">1h</button>
                <button type="button" data-timeframe="4h">4h</button>
            </div>
        </div>
        <span>最后更新：<span class="timestamp" id="orderbook-updated">{updated_at}</span></span>
    </div>
    <div id="orderbook-container" class="{book_grid_class}">
        {cards}
    </div>
    <script>
      (function() {{
        const instrumentSelect = document.getElementById('orderbook-instrument');
        const levelSelect = document.getElementById('depth-levels');
        const timeframeButtons = document.querySelectorAll('.timeframe-buttons button');
        const DEFAULT_PAIRS = ['BTC-USDT-SWAP', 'ETH-USDT-SWAP'];
        const TIMEFRAMES_MS = {{
          '5m': 5 * 60 * 1000,
          '15m': 15 * 60 * 1000,
          '1h': 60 * 60 * 1000,
          '4h': 4 * 60 * 60 * 1000,
        }};
        const DEFAULT_TIMEFRAME = '15m';
        let activeTimeframe = DEFAULT_TIMEFRAME;
        const HISTORY_LIMIT = 4800;
        const netDepthHistory = new Map();
        const BASE_GRID_CLASS = 'book-grid';
        const NET_DEPTH_DELAY_MS = 5000;
        const pendingNetDepth = new Map();
        const committedNetDepth = new Map();
        timeframeButtons.forEach((button) => {{
          if (button.classList.contains('active') && button.dataset.timeframe) {{
            activeTimeframe = button.dataset.timeframe;
          }}
        }});

        const formatNumber = (value, digits = 2) => {{
          if (value === null || value === undefined || value === '') {{ return '--'; }}
          const num = Number(value);
          if (!Number.isFinite(num)) {{ return '--'; }}
          return num.toFixed(digits);
        }};
        const formatSignedNumber = (value, digits = 2) => {{
          const formatted = formatNumber(value, digits);
          if (formatted === '--') {{
            return formatted;
          }}
          const num = Number(value);
          if (num > 0) {{
            return `+${{num.toFixed(digits)}}`;
          }}
          if (num < 0) {{
            return num.toFixed(digits);
          }}
          return num.toFixed(digits);
        }};
        const formatTimestamp = (value) => {{
          if (!value) {{ return '--'; }}
          const date = new Date(value);
          if (!isFinite(date)) {{ return value; }}
          return new Intl.DateTimeFormat('zh-CN', {{
            timeZone: 'Asia/Shanghai',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
          }}).format(date);
        }};
        const sumDepth = (entries, depth) => {{
          if (!Array.isArray(entries) || entries.length === 0) {{ return 0; }}
          let total = 0;
          for (let i = 0; i < depth && i < entries.length; i += 1) {{
            const size = Number(entries[i]?.[1]);
            if (Number.isFinite(size)) {{
              total += size;
            }}
          }}
          return total;
        }};
        const describeDepth = (imbalance) => {{
          if (imbalance >= 0.2) {{
            return {{ tone: 'positive', text: '买盘深度明显占优，短线动能偏多。' }};
          }}
          if (imbalance <= -0.2) {{
            return {{ tone: 'negative', text: '卖盘压制增强，谨防价格回落。' }};
          }}
          return {{ tone: 'neutral', text: '买卖力量接近平衡，关注成交突破信号。' }};
        }};
        const appendNetDepth = (symbol, timestamp, value) => {{
          const key = (symbol || '--').toUpperCase();
          const history = netDepthHistory.get(key) || [];
          const point = {{
            time: timestamp ? new Date(timestamp) : new Date(),
            value,
          }};
          history.push(point);
          while (history.length > HISTORY_LIMIT) {{
            history.shift();
          }}
          netDepthHistory.set(key, history);
          return history;
        }};
        const primeHistory = (symbol, series) => {{
          if (!Array.isArray(series) || !series.length) {{
            return;
          }}
          const key = (symbol || '--').toUpperCase();
          if (netDepthHistory.has(key)) {{
            return;
          }}
          const history = [];
          series.forEach((entry) => {{
            const val = Number(entry?.net_depth);
            if (!Number.isFinite(val)) {{
              return;
            }}
            const time = entry?.timestamp ? new Date(entry.timestamp) : new Date();
            history.push({{ time, value: val }});
          }});
          if (!history.length) {{
            return;
          }}
          const trimmed = history.slice(-HISTORY_LIMIT);
          netDepthHistory.set(key, trimmed);
        }};
        const queueNetDepthUpdate = (symbol, timestamp, value) => {{
          if (!symbol) {{
            return;
          }}
          const key = symbol.toUpperCase();
          const now = Date.now();
          const entryTimestamp = timestamp ? new Date(timestamp) : new Date();
          const existing = pendingNetDepth.get(key);
          if (existing && existing.readyAt > now) {{
            existing.value = value;
            existing.timestamp = entryTimestamp;
            pendingNetDepth.set(key, existing);
            return;
          }}
          pendingNetDepth.set(key, {{
            value,
            timestamp: entryTimestamp,
            readyAt: now + NET_DEPTH_DELAY_MS,
          }});
        }};
        const flushNetDepthQueue = () => {{
          const now = Date.now();
          pendingNetDepth.forEach((entry, key) => {{
            if (entry.readyAt <= now) {{
              committedNetDepth.set(key, {{
                value: entry.value,
                timestamp: entry.timestamp,
              }});
              pendingNetDepth.delete(key);
            }}
          }});
        }};
        const getCommittedNetDepth = (symbol) => {{
          if (!symbol) {{
            return undefined;
          }}
          return committedNetDepth.get(symbol.toUpperCase());
        }};
        const normalizeHistoryPoints = (series) => {{
          if (!Array.isArray(series)) {{
            return [];
          }}
          const normalized = [];
          series.forEach((entry) => {{
            if (!entry) {{
              return;
            }}
            const rawValue = entry.value ?? entry.net_depth;
            const value = Number(rawValue);
            if (!Number.isFinite(value)) {{
              return;
            }}
            let time = entry.time instanceof Date ? entry.time : undefined;
            const fallbackStamp = entry.timestamp ?? entry.time;
            if (!(time instanceof Date) || Number.isNaN(time?.getTime?.())) {{
              if (fallbackStamp) {{
                const parsed = new Date(fallbackStamp);
                if (!Number.isNaN(parsed.getTime())) {{
                  time = parsed;
                }}
              }}
            }}
            if (!(time instanceof Date) || Number.isNaN(time.getTime())) {{
              time = new Date();
            }}
            normalized.push({{ time, value }});
          }});
          return normalized;
        }};
        const filterSeriesByTimeframe = (series, timeframeKey) => {{
          const windowMs = TIMEFRAMES_MS[timeframeKey] || TIMEFRAMES_MS[DEFAULT_TIMEFRAME];
          if (!Array.isArray(series) || !series.length || !windowMs) {{
            return Array.isArray(series) ? series : [];
          }}
          const latest = series[series.length - 1].time?.getTime?.();
          if (!Number.isFinite(latest)) {{
            return series;
          }}
          const cutoff = latest - windowMs;
          return series.filter((pt) => {{
            const ts = pt.time?.getTime?.();
            return Number.isFinite(ts) && ts >= cutoff;
          }});
        }};
        const serializeHistory = (series) => {{
          if (!Array.isArray(series)) {{
            return '[]';
          }}
          const payload = series.map((pt) => {{
            const time = pt.time instanceof Date && !Number.isNaN(pt.time.getTime())
              ? pt.time.toISOString()
              : pt.time;
            return {{ value: pt.value, time }};
          }});
          return JSON.stringify(payload);
        }};
        const getStoredHistory = (canvas) => {{
          if (!canvas || !canvas.dataset) {{
            return [];
          }}
          const raw = canvas.dataset.history || '[]';
          try {{
            const parsed = JSON.parse(raw);
            return normalizeHistoryPoints(parsed);
          }} catch (err) {{
            console.error('Failed to parse stored history', err);
            return [];
          }}
        }};
        function repaintCharts() {{
          document.querySelectorAll('.chart-canvas').forEach((canvas) => {{
            const history = getStoredHistory(canvas);
            drawNetDepthChart(canvas, history, activeTimeframe);
          }});
        }}
        const drawNetDepthChart = (canvas, history, timeframeKey = activeTimeframe) => {{
          const yAxisLabel = "净深度（买-卖，张）";
          const parent = canvas.parentElement;
          const width = Math.max((parent ? parent.clientWidth : 320), 420);
          const height = 360;
          const dpr = window.devicePixelRatio || 1;
          canvas.width = width * dpr;
          canvas.height = height * dpr;
          canvas.style.width = width + "px";
          canvas.style.height = height + "px";
          const ctx = canvas.getContext("2d");
          ctx.setTransform(1, 0, 0, 1, 0, 0);
          ctx.scale(dpr, dpr);
          ctx.clearRect(0, 0, width, height);
          const normalized = normalizeHistoryPoints(history);
          const filtered = filterSeriesByTimeframe(normalized, timeframeKey);
          const series = filtered.length ? filtered : normalized;
          if (!series.length) {{
            ctx.fillStyle = "#94a3b8";
            ctx.font = "14px Arial";
            ctx.fillText("暂无历史数据", 20, height / 2);
            return;
          }}
          const values = series.map((pt) => pt.value);
          const maxVal = Math.max(...values);
          const minVal = Math.min(...values);
          const maxAbs = Math.max(Math.abs(maxVal), Math.abs(minVal), 1);
          const padding = maxAbs * 0.1;
          const limit = maxAbs + padding;
          const labelFormatter = (value) => {{
            const digits = Math.abs(value) < 1 ? 4 : 2;
            return formatSignedNumber(value, digits);
          }};
          const ticksPerSide = 3;
          const tickStep = limit / Math.max(1, ticksPerSide);
          const tickValues = [];
          // Force symmetric ticks so everything above the zero line is positive and below is negative.
          for (let i = ticksPerSide; i > 0; i -= 1) {{
            tickValues.push(tickStep * i);
          }}
          tickValues.push(0);
          for (let i = 1; i <= ticksPerSide; i += 1) {{
            tickValues.push(-tickStep * i);
          }}
          ctx.font = "12px Arial";
          const tickLabels = tickValues.map((value) => labelFormatter(value));
          const maxTickLabelWidth = Math.max(...tickLabels.map((text) => ctx.measureText(text).width), 0);
          const left = Math.max(32, Math.ceil(maxTickLabelWidth + 14));
          const right = 24;
          const xPadding = 20;
          const top = 16;
          const bottom = 36;
          const rightEdge = width - right - xPadding;
          const chartWidth = rightEdge - left;
          const chartHeight = height - top - bottom;
          const topVal = limit;
          const bottomVal = -limit;
          const range = topVal - bottomVal || 1;
          const zeroY = top + chartHeight - ((0 - bottomVal) / range) * chartHeight;
          ctx.save();
          const axisLabelX = Math.max(8, left - maxTickLabelWidth - 12);
          ctx.translate(axisLabelX, top + chartHeight / 2);
          ctx.rotate(-Math.PI / 2);
          ctx.fillStyle = "#cbd5e1";
          ctx.textAlign = "center";
          ctx.textBaseline = "middle";
          ctx.fillText(yAxisLabel, 0, 0);
          ctx.restore();
          ctx.strokeStyle = "#475569";
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(left, zeroY);
          ctx.lineTo(rightEdge, zeroY);
          ctx.stroke();
          ctx.strokeStyle = "#38bdf8";
          ctx.lineWidth = 2;
          ctx.beginPath();
          series.forEach((pt, idx) => {{
            const x = left + (chartWidth * idx) / Math.max(1, series.length - 1);
            const y = top + chartHeight - ((pt.value - bottomVal) / range) * chartHeight;
            if (idx === 0) {{
              ctx.moveTo(x, y);
            }} else {{
              ctx.lineTo(x, y);
            }}
          }});
          ctx.stroke();
          ctx.fillStyle = "#94a3b8";
          ctx.font = "12px Arial";
          ctx.textAlign = "center";
          ctx.textBaseline = "top";
          const tickCount = Math.min(series.length, 8);
          for (let i = 0; i < tickCount; i += 1) {{
            const idx = Math.round((series.length - 1) * (i / Math.max(1, tickCount - 1)));
            const pt = series[idx];
            const x = left + (chartWidth * idx) / Math.max(1, series.length - 1);
            const label = pt.time instanceof Date && !Number.isNaN(pt.time.getTime())
              ? `${{String(pt.time.getHours()).padStart(2, "0")}}:${{String(pt.time.getMinutes()).padStart(2, "0")}}:${{String(pt.time.getSeconds()).padStart(2, "0")}}`
              : "";
            ctx.fillText(label, x, height - bottom + 6);
          }}
          ctx.textAlign = "right";
          ctx.textBaseline = "middle";
          const yLabelX = left - 8;
          tickValues.forEach((value, idx) => {{
            const y = top + chartHeight - ((value - bottomVal) / range) * chartHeight;
            const offset = value === 0 ? 0 : value > 0 ? -2 : 2;
            ctx.fillText(tickLabels[idx], yLabelX, y + offset);
          }});
        }};

        function renderBooks(data) {{
          const container = document.getElementById('orderbook-container');
          container.innerHTML = '';
          flushNetDepthQueue();
          const applyGridClass = (count) => {{
            if (count === 1) {{
              container.className = `${{BASE_GRID_CLASS}} single-card`;
            }} else if (count > 1 && count <= 2) {{
              container.className = `${{BASE_GRID_CLASS}} two-cols`;
            }} else {{
              container.className = BASE_GRID_CLASS;
            }}
          }};
          const levels = parseInt(levelSelect.value || '{levels}', 10) || {levels};
          const filterValue = (instrumentSelect.value || '').trim();
          let items = Array.isArray(data.items) ? data.items.slice() : [];
          if (filterValue) {{
            const matchSymbol = filterValue.toUpperCase();
            const match = items.find(
              (entry) => (entry.instrument_id || '').toUpperCase() === matchSymbol,
            );
            items = match ? [match] : [];
          }} else {{
            const prioritized = [];
            DEFAULT_PAIRS.forEach((pair) => {{
              const found = items.find(
                (entry) => (entry.instrument_id || '').toUpperCase() === pair,
              );
              if (found) {{
                prioritized.push(found);
              }}
            }});
            if (prioritized.length) {{
              items = prioritized;
            }}
          }}
          applyGridClass(items.length);
          if (!items.length) {{
            container.innerHTML = '<p class="empty-state">暂无深度数据</p>';
            const updated = document.getElementById('orderbook-updated');
            if (updated) {{ updated.textContent = formatTimestamp(data.updated_at); }}
            return;
          }}
          items.forEach((item) => {{
            const symbol = (item.instrument_id || '--').toUpperCase();
            primeHistory(symbol, item.history);
            const bids = Array.isArray(item.bids) ? item.bids : [];
            const asks = Array.isArray(item.asks) ? item.asks : [];
            const buyDepth = sumDepth(bids, levels);
            const sellDepth = sumDepth(asks, levels);
            const totalDepth = buyDepth + sellDepth;
            const bidShare = totalDepth > 0 ? buyDepth / totalDepth : 0.5;
            const askShare = 1 - bidShare;
            const imbalance = totalDepth > 0 ? (buyDepth - sellDepth) / totalDepth : 0;
            const {{ tone, text: guidance }} = describeDepth(imbalance);
            const spreadText = formatNumber(item.spread, 4);
            const bestBidText = formatNumber(item.best_bid, 4);
            const bestAskText = formatNumber(item.best_ask, 4);
            const buyDepthText = formatNumber(buyDepth, 2);
            const sellDepthText = formatNumber(sellDepth, 2);
            const totalDepthText = formatNumber(totalDepth, 2);
            const bestBidNum = Number(item.best_bid);
            const bestAskNum = Number(item.best_ask);
            let midText = '--';
            if (Number.isFinite(bestBidNum) && Number.isFinite(bestAskNum)) {{
              midText = formatNumber((bestBidNum + bestAskNum) / 2, 4);
            }}
            const netDepthValue = buyDepth - sellDepth;
            queueNetDepthUpdate(symbol, item.timestamp, netDepthValue);
            const committedSnapshot = getCommittedNetDepth(symbol);
            const netDepthText = committedSnapshot ? formatNumber(committedSnapshot.value, 2) : '--';
            const analysis = `买盘占比 ${{Math.round(bidShare * 100)}}%，${{guidance}}（点差 ${{spreadText}}）`;
            const card = document.createElement('section');
            card.className = `orderbook-chart tone-${{tone}}`;
            card.innerHTML = `
              <div class="chart-header">
                <div>
                  <h2>${{item.instrument_id || '--'}}</h2>
                  <small>更新时间：${{formatTimestamp(item.timestamp)}}</small>
                </div>
              </div>
              <div class="chart-prices">
                <div class="chart-price-group primary">
                  <span>买一 ${{bestBidText}}</span>
                  <span>卖一 ${{bestAskText}}</span>
                  <span class="mid">中间价 ${{midText}}</span>
                </div>
              </div>
              <div class="chart-stats">
                <span class="stat-pill">买盘深度${{buyDepthText}}</span>
                <span class="stat-pill">卖盘深度${{sellDepthText}}</span>
                <span class="stat-pill">总深度 ${{totalDepthText}}</span>
                <span class="stat-pill stat-accent">净深度 ${{netDepthText}}</span>
                <span class="stat-pill stat-accent">点差 ${{spreadText}}</span>
              </div>
              <canvas class="chart-canvas"></canvas>
              <p class="chart-insight ${{tone}}">${{analysis}}</p>
            `;
            const canvas = card.querySelector('canvas');
            container.appendChild(card);
            let history = netDepthHistory.get(symbol) || [];
            if (committedSnapshot) {{
              history = appendNetDepth(symbol, committedSnapshot.timestamp, committedSnapshot.value);
            }}
            const normalizedHistory = normalizeHistoryPoints(history);
            canvas.dataset.history = serializeHistory(normalizedHistory);
            requestAnimationFrame(() => drawNetDepthChart(canvas, normalizedHistory, activeTimeframe));
          }});
          const updated = document.getElementById('orderbook-updated');
          if (updated) {{ updated.textContent = formatTimestamp(data.updated_at); }}
        }}

        function refresh() {{
          const params = new URLSearchParams({{ limit: levelSelect.value || '{levels}' }});
          if (instrumentSelect.value) {{
            params.set('instrument', instrumentSelect.value);
          }}
          params.set('levels', levelSelect.value || '{levels}');
          fetch('/api/streams/orderbook/latest?' + params.toString())
            .then((res) => res.json())
            .then(renderBooks)
            .catch((err) => console.error(err));
        }}

        instrumentSelect.addEventListener('change', refresh);
        levelSelect.addEventListener('change', refresh);
        timeframeButtons.forEach((button) => {{
          button.addEventListener('click', () => {{
            const timeframe = button.dataset.timeframe;
            if (!timeframe || timeframe === activeTimeframe) {{
              return;
            }}
            activeTimeframe = timeframe;
            timeframeButtons.forEach((btn) => btn.classList.toggle('active', btn === button));
            repaintCharts();
          }});
        }});
        window.addEventListener('resize', repaintCharts);
        refresh();
        setInterval(refresh, 2000);
      }})();
    </script>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
"""

LIQUIDATION_MAP_TEMPLATE = Template(r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>清算地图</title>
    <style>
        body { font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }
        h1 { margin-bottom: 0.4rem; }
        .hint { color: #94a3b8; margin-bottom: 1.5rem; }
        .top-nav { display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }
        .nav-link { padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }
        .nav-link.active { background-color: #38bdf8; color: #0f172a; }
        .map-card { background: #0f1b2d; border-radius: 16px; padding: 24px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); border: 1px solid rgba(148, 163, 184, 0.18); }
        .map-meta { display: flex; flex-direction: column; gap: 6px; margin-bottom: 1.5rem; font-size: 0.95rem; }
        .map-meta-row { display: flex; flex-wrap: wrap; gap: 18px; align-items: center; }
        .map-meta strong { font-size: 1.05rem; color: #f8fafc; }
        .map-legend { display: flex; flex-wrap: wrap; gap: 16px; font-size: 0.85rem; color: #cbd5f5; align-items: center; }
        .legend-dot { display: inline-flex; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
        .legend-dot.bar { background: #facc15; }
        .legend-price { color: #f8fafc; }
        .legend-axis { color: #94a3b8; font-size: 0.82rem; }
        .timestamp { color: #38bdf8; }
        .map-chart { display: block; margin-bottom: 1.25rem; width: 100%; }
        .map-chart-body { display: flex; align-items: flex-end; gap: 12px; width: 100%; }
        .map-axis-y { display: none; }
        .map-bars { position: relative; width: 100%; padding: 0; border-radius: 14px; background: rgba(8, 22, 48, 0.85); min-height: 320px; overflow: hidden; }
        .map-bars svg { width: 100%; height: 360px; display: block; }
        .map-empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: #94a3b8; font-size: 0.9rem; background: transparent; }
        .map-bars.has-data .map-empty { display: none; }
        .range-controls { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }
        .range-buttons { display: inline-flex; gap: 8px; }
        .range-btn { background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.45); border-radius: 999px; padding: 2px 12px; font-size: 0.8rem; cursor: pointer; }
        .range-btn.active { background: #38bdf8; color: #0f172a; }
        .range-btn:hover { border-color: #7dd3fc; }
        .order-mode-buttons { display: inline-flex; gap: 8px; }
        .order-mode-btn { background: rgba(248, 250, 252, 0.08); color: #e2e8f0; border: 1px solid rgba(148, 163, 184, 0.4); border-radius: 999px; padding: 2px 12px; font-size: 0.8rem; cursor: pointer; }
        .order-mode-btn.active { background: #fbbf24; color: #1f2937; border-color: #fbbf24; }
        .order-mode-btn:hover { border-color: rgba(251, 191, 36, 0.8); }
        .history-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin-top: 1.5rem; }
        .history-table-card { background: #0f1b2d; border-radius: 16px; padding: 20px; border: 1px solid rgba(148, 163, 184, 0.18); box-shadow: 0 8px 20px rgba(15, 23, 42, 0.4); }
        .history-table-card h2 { margin: 0 0 0.75rem; font-size: 1rem; color: #f8fafc; }
        .history-table-card p { margin: 0 0 1rem; color: #94a3b8; font-size: 0.85rem; }
        .history-table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
        .history-table th { text-align: left; font-weight: 600; color: #cbd5f5; padding-bottom: 6px; border-bottom: 1px solid rgba(148, 163, 184, 0.16); }
        .history-table td { padding: 6px 0; border-bottom: 1px solid rgba(148, 163, 184, 0.08); color: #e2e8f0; }
        .history-table td.price { color: #38bdf8; }
        .history-table td.cumulative { color: #facc15; }
        .history-table tbody tr:last-child td { border-bottom: none; }
        .history-table .empty-row td { color: #64748b; font-style: italic; }
        @media (max-width: 768px) {
            .map-column { flex: 0 0 40px; }
            .map-center span { left: -35px; }
            .map-chart { width: 100%; }
        }
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">主页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模块</a>
        <a href="/liquidations" class="nav-link">清算警报</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/liquidation-map" class="nav-link active">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风控设置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>
    <h1>ETHUSDT 清算地图</h1>
    <p class="hint">默认读取 http://localhost:8000/orderbook?levels=400 的市场深度，将订单金额按 1 USDT 区间累加展示。</p>
    <section class="map-card">
        <div class="map-meta">
            <div class="map-meta-row">
                <span>交易对：<strong id="map-symbol">$display_pair</strong></span>
                <span>最新成交价：<strong id="map-last-price">$last_price</strong> USDT</span>
                <span>最后刷新：<span class="timestamp" id="map-updated">$updated_at</span></span>
                <div class="range-controls">
                    <span>统计区间：<strong id="map-range">--</strong></span>
                    <div class="range-buttons">
                        <button type="button" class="range-btn active" data-hours="24">回看24小时</button>
                        <button type="button" class="range-btn" data-hours="48">回看48小时</button>
                    </div>
                    <div class="order-mode-buttons">
                        <button type="button" class="order-mode-btn active" data-mode="all">全量订单</button>
                        <button type="button" class="order-mode-btn" data-mode="open">未清算订单</button>
                    </div>
                </div>
            </div>
            <div class="map-legend">
                <span><i class="legend-dot bar"></i>区间金额</span>
                <span class="legend-price">当前价格<strong id="map-legend-price">--</strong></span>
                <span class="legend-axis" id="map-price-axis">价格轴：--</span>
            </div>
        </div>
        <div class="map-chart" id="liquidation-map-root" data-instrument="$instrument" data-bin-size="1" data-levels="400">
            <div class="map-chart-body">
                <div class="map-axis-y" id="map-amount-axis">
                    <span>--</span>
                    <span>--</span>
                    <span>0</span>
                </div>
                <div class="map-bars" id="liquidation-bars">
                    <svg id="liquidation-chart"></svg>
                    <div class="map-empty" id="map-empty-message">等待市场深度数据...</div>
                </div>
            </div>
        </div>
    </section>

    <section class="history-grid">
        <article class="history-table-card">
            <h2>历史累计买单</h2>
            <p>按 1 USDT 区间聚合，展示 24 小时内的累计买入金额</p>
            <table class="history-table">
                <thead>
                    <tr>
                        <th>价格 (USDT)</th>
                        <th>区间金额 (USDT)</th>
                        <th>累计买入</th>
                    </tr>
                </thead>
                <tbody id="history-buy-body">
                    <tr class="empty-row"><td colspan="3">等待数据...</td></tr>
                </tbody>
            </table>
        </article>
        <article class="history-table-card">
            <h2>历史累计卖单</h2>
            <p>按 1 USDT 区间聚合，展示 24 小时内的累计卖出金额</p>
            <table class="history-table">
                <thead>
                    <tr>
                        <th>价格 (USDT)</th>
                        <th>区间金额 (USDT)</th>
                        <th>累计卖出</th>
                    </tr>
                </thead>
                <tbody id="history-sell-body">
                    <tr class="empty-row"><td colspan="3">等待数据...</td></tr>
                </tbody>
            </table>
        </article>
    </section>
    <script>
      (function() {
        const root = document.getElementById('liquidation-map-root');
        if (!root) { return; }
        const barsContainer = document.getElementById('liquidation-bars');
        const chartSvg = document.getElementById('liquidation-chart');
        const emptyMessage = document.getElementById('map-empty-message');
        const priceAxisEl = document.getElementById('map-price-axis');
        const amountAxisEl = document.getElementById('map-amount-axis');
        const lastPriceEl = document.getElementById('map-last-price');
        const legendPriceEl = document.getElementById('map-legend-price');
        const updatedEl = document.getElementById('map-updated');
        const rangeEl = document.getElementById('map-range');
        const buyTableBody = document.getElementById('history-buy-body');
        const sellTableBody = document.getElementById('history-sell-body');
        const rangeButtons = document.querySelectorAll('.range-btn');
        const orderModeButtons = document.querySelectorAll('.order-mode-btn');
        const instrument = (root.dataset.instrument || 'ETH-USDT-SWAP').toUpperCase();
        const binSize = Number(root.dataset.binSize) || 1;
        const maxLevels = Number(root.dataset.levels) || 400;
        const REFRESH_MS = 4000;
        const currentUrl = new URL(window.location.href);
        const urlHours = Number(currentUrl.searchParams.get('hours'));
        let historyHours = (Number.isFinite(urlHours) && urlHours >= 1 && urlHours <= 168) ? urlHours : 24;
        const urlMode = (currentUrl.searchParams.get('mode') || 'all').toLowerCase();
        let orderMode = urlMode === 'open' ? 'open' : 'all';
        let lastHistoryPayload = null;
        let zoomLevel = 1;
        let panOffset = 0;
        let lastRenderBins = [];
        let lastRenderPrice = null;
        let lastRenderState = null;

        const syncLegendPrice = () => {
          if (!legendPriceEl || !lastPriceEl) { return; }
          const text = (lastPriceEl.textContent || '').trim();
          legendPriceEl.textContent = text || '--';
        };

        syncLegendPrice();

        const formatNumber = (value, digits = 2) => {
          const num = Number(value);
          if (!Number.isFinite(num)) { return '--'; }
          return num.toFixed(digits);
        };

        const formatCurrency = (value) => {
          const num = Number(value);
          if (!Number.isFinite(num)) { return '--'; }
          if (num >= 1_000_000) { return (num / 1_000_000).toFixed(2) + 'M'; }
          if (num >= 1_000) { return (num / 1_000).toFixed(2) + 'K'; }
          return num.toFixed(0);
        };

        const formatPrice = (value) => {
          const num = Number(value);
          if (!Number.isFinite(num)) { return '--'; }
          if (num >= 100) { return num.toFixed(0); }
          return num.toFixed(2);
        };

        const formatTimestamp = (value) => {
          if (!value) { return '--'; }
          const date = new Date(value);
          if (!isFinite(date)) { return value; }
          return date.toLocaleString('zh-CN', { hour12: false });
        };


        const normalizeLevels = (entries) => {
          if (!Array.isArray(entries)) { return []; }
          return entries.map((entry) => {
            let price;
            let size;
            if (Array.isArray(entry)) {
              price = Number(entry[0]);
              size = Number(entry[1]);
            } else if (entry && typeof entry === 'object') {
              price = Number(entry.price ?? entry[0]);
              size = Number(entry.size ?? entry.qty ?? entry[1]);
            }
            if (!Number.isFinite(price) || !Number.isFinite(size)) {
              return null;
            }
            const absoluteSize = Math.abs(size);
            return { price, size: absoluteSize, notional: Math.abs(price * absoluteSize) };
          }).filter(Boolean);
        };

        const groupBins = (levels) => {
          const buckets = new Map();
          levels.forEach((level) => {
            const bucketKey = Math.floor(level.price / binSize) * binSize;
            if (!Number.isFinite(bucketKey)) { return; }
            const existing = buckets.get(bucketKey) || { price: bucketKey, notional: 0, size: 0 };
            existing.notional += level.notional;
            existing.size += level.size;
            buckets.set(bucketKey, existing);
          });
          return Array.from(buckets.values());
        };

        const accumulateSide = (bins, side) => {
          const comparator = side === 'bid' ? (a, b) => b.price - a.price : (a, b) => a.price - b.price;
          const filtered = bins.filter((bin) => Number.isFinite(bin?.price)).sort(comparator);
          let cumulative = 0;
          const withCumulative = filtered.map((bin) => {
            cumulative += bin.notional;
            return { ...bin, cumulative, side };
          });
          return withCumulative.sort((a, b) => a.price - b.price);
        };

        const deriveLatestPrice = (entry) => {
          const candidates = [entry?.last_price, entry?.best_bid, entry?.best_ask];
          for (const candidate of candidates) {
            const num = Number(candidate);
            if (Number.isFinite(num) && num > 0) { return num; }
          }
          const firstBid = Array.isArray(entry?.bids) && entry.bids.length ? Number(entry.bids[0][0]) : NaN;
          const firstAsk = Array.isArray(entry?.asks) && entry.asks.length ? Number(entry.asks[0][0]) : NaN;
          if (Number.isFinite(firstBid) && Number.isFinite(firstAsk)) { return (firstBid + firstAsk) / 2; }
          if (Number.isFinite(firstBid)) { return firstBid; }
          if (Number.isFinite(firstAsk)) { return firstAsk; }
          return NaN;
        };

        const aggregateHistoryBuckets = (bins, side) => {
          if (!Array.isArray(bins)) { return []; }
          const normalizedSide = side === 'ask' ? 'ask' : 'bid';
          const grouped = new Map();
          bins.forEach((bin) => {
            const recordSide = String(bin?.side || '').toLowerCase();
            if (recordSide !== normalizedSide) { return; }
            const price = Number(bin?.price);
            if (!Number.isFinite(price)) { return; }
            const bucket = Math.floor(price);
            const notional = Math.max(0, Number(bin?.notional) || 0);
            const size = Math.max(0, Number(bin?.size) || 0);
            const existing = grouped.get(bucket) || { price: bucket, interval: 0, size: 0 };
            existing.interval += notional;
            existing.size += size;
            grouped.set(bucket, existing);
          });
          const sorted = Array.from(grouped.values()).sort((a, b) => (
            normalizedSide === 'bid' ? b.price - a.price : a.price - b.price
          ));
          let cumulative = 0;
          return sorted.map((entry) => {
            cumulative += entry.interval;
            return { ...entry, cumulative };
          });
        };

        const renderHistoryTableBody = (entries, target) => {
          if (!target) { return; }
          if (!entries.length) {
            target.innerHTML = '<tr class="empty-row"><td colspan="3">--</td></tr>';
            return;
          }
          const maxRows = 15;
          const rows = entries.slice(0, maxRows).map((entry) => (
            '<tr>' +
            `<td class="price">$${formatPrice(entry.price)}</td>` +
            `<td>$${formatCurrency(entry.interval)}</td>` +
            `<td class="cumulative">$${formatCurrency(entry.cumulative)}</td>` +
            '</tr>'
          ));
          target.innerHTML = rows.join('');
        };

        const updateHistoryTablesUI = (bins) => {
          if (!buyTableBody || !sellTableBody) { return; }
          const buyEntries = aggregateHistoryBuckets(bins, 'bid');
          const sellEntries = aggregateHistoryBuckets(bins, 'ask');
          renderHistoryTableBody(buyEntries, buyTableBody);
          renderHistoryTableBody(sellEntries, sellTableBody);
        };

        const showEmpty = (message) => {
          if (barsContainer) { barsContainer.classList.remove('has-data'); }
          if (chartSvg) { chartSvg.innerHTML = ''; }
          if (emptyMessage) { emptyMessage.textContent = message; }
          syncLegendPrice();
        };

        const updateAmountAxis = (maxValue) => {
          if (!amountAxisEl) { return; }
          const safeMax = Math.max(Number(maxValue) || 0, 1);
          amountAxisEl.innerHTML = '';
          const ticks = 3;
          for (let i = 0; i < ticks; i += 1) {
            const ratio = i / (ticks - 1);
            const value = safeMax * (1 - ratio);
            const span = document.createElement('span');
            span.textContent = i === ticks - 1 ? '0' : formatCurrency(value);
            amountAxisEl.appendChild(span);
          }
        };

        const updatePriceAxis = (bins, latestPrice) => {
          if (!priceAxisEl) { return; }
          if (!Array.isArray(bins) || !bins.length) {
            priceAxisEl.textContent = '价格轴：--';
            return;
          }
          const prices = bins.map((bin) => Number(bin.price)).filter((value) => Number.isFinite(value));
          if (!prices.length) {
            priceAxisEl.textContent = '价格轴：--';
            return;
          }
          const minPrice = Math.min(...prices);
          const maxPrice = Math.max(...prices);
          let mid = Number(latestPrice);
          if (!Number.isFinite(mid)) {
            mid = (minPrice + maxPrice) / 2;
          }
          priceAxisEl.innerHTML =
            '价格轴：<strong>' + formatPrice(minPrice) + '</strong> ← <strong>' +
            formatPrice(mid) + '</strong> → <strong>' + formatPrice(maxPrice) + '</strong>';
        };

        const renderBars = (bins, latestPrice) => {
          if (!chartSvg || !barsContainer) { return; }
          lastRenderBins = Array.isArray(bins) ? bins.slice() : [];
          lastRenderPrice = Number.isFinite(latestPrice) ? latestPrice : null;
          if (!Array.isArray(bins) || !bins.length) {
            showEmpty('暂无深度数据');
            updatePriceAxis([], latestPrice);
            updateAmountAxis(null);
            return;
          }
          barsContainer.classList.add('has-data');
          if (emptyMessage) { emptyMessage.textContent = ''; }
          const rect = chartSvg.getBoundingClientRect();
          const width = Math.max(rect.width || (chartSvg.parentElement ? chartSvg.parentElement.clientWidth : 800) || 800, 800);
          const height = Math.max(rect.height || 360, 360);
          chartSvg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
          chartSvg.innerHTML = '';
          const padding = { left: 90, right: 40, top: 24, bottom: 64 };
          const chartWidth = width - padding.left - padding.right;
          const chartHeight = height - padding.top - padding.bottom;
          const prices = bins.map((bin) => Number(bin.price)).filter((value) => Number.isFinite(value));
          const minPrice = Math.min(...prices);
          const maxPrice = Math.max(...prices);
          const priceRangeFull = Math.max(maxPrice - minPrice, 1);
          const centerPrice = Number.isFinite(latestPrice) ? latestPrice : (minPrice + maxPrice) / 2;
          const visibleRange = priceRangeFull / Math.max(zoomLevel, 1);
          const halfRange = visibleRange / 2;
          const panShiftRange = priceRangeFull / 2;
          let targetCenter = centerPrice + panOffset * panShiftRange;
          if (!Number.isFinite(targetCenter)) {
            targetCenter = (minPrice + maxPrice) / 2;
          }
          const minCenter = minPrice + halfRange;
          const maxCenter = maxPrice - halfRange;
          if (minCenter >= maxCenter) {
            targetCenter = (minPrice + maxPrice) / 2;
          } else {
            targetCenter = Math.min(Math.max(targetCenter, minCenter), maxCenter);
          }
          const visibleMin = Math.max(minPrice, targetCenter - halfRange);
          const visibleMax = Math.min(maxPrice, targetCenter + halfRange);
          const priceRange = Math.max(visibleMax - visibleMin, 1);
          const bids = bins.filter((bin) => (bin.side || '').toLowerCase() === 'bid');
          const asks = bins.filter((bin) => (bin.side || '').toLowerCase() === 'ask');
          const visibleBins = bins.filter((bin) => {
            const price = Number(bin?.price);
            return Number.isFinite(price) && price >= visibleMin && price <= visibleMax;
          });
          const candidateBins = visibleBins.length ? visibleBins : bins;
          const maxNotional = Math.max(...candidateBins.map((bin) => Number(bin.notional) || 0), 0);
          const yMax = Math.max(maxNotional * 1.05, 1);
          updateAmountAxis(yMax);
          updatePriceAxis(bins, latestPrice);
          let markerPrice = Number.isFinite(latestPrice) ? latestPrice : NaN;
          if (!Number.isFinite(markerPrice) && legendPriceEl) {
            const parsedLegend = Number(legendPriceEl.textContent);
            if (Number.isFinite(parsedLegend)) {
              markerPrice = parsedLegend;
            }
          }
          if (!Number.isFinite(markerPrice)) {
            markerPrice = clampedCenter;
          }
          // keep legend synced separately with last price
          const scaleX = (price) => padding.left + ((price - visibleMin) / priceRange) * chartWidth;
          const barWidth = Math.max(2, (chartWidth / Math.max(bins.length, 1)) * 0.6);
          const scaleBarHeight = (value) => (value / yMax) * chartHeight;
          const scaleLineY = (value) => padding.top + chartHeight - (value / yMax) * chartHeight;
          const svgNS = 'http://www.w3.org/2000/svg';
          const background = document.createElementNS(svgNS, 'rect');
          background.setAttribute('x', padding.left);
          background.setAttribute('y', padding.top);
          background.setAttribute('width', chartWidth);
          background.setAttribute('height', chartHeight);
          background.setAttribute('fill', 'rgba(15,23,42,0.35)');
          chartSvg.appendChild(background);
          const drawBars = (collection, color) => {
            collection.forEach((bin) => {
              const price = Number(bin.price);
              if (!Number.isFinite(price)) { return; }
              const x = scaleX(price) - barWidth / 2;
              const value = Number(bin.notional) || 0;
              const h = Math.max(1, scaleBarHeight(value));
              const y = padding.top + chartHeight - h;
              const rectEl = document.createElementNS(svgNS, 'rect');
              rectEl.setAttribute('x', x);
              rectEl.setAttribute('y', y);
              rectEl.setAttribute('width', barWidth);
              rectEl.setAttribute('height', h);
              rectEl.setAttribute('fill', color);
              rectEl.setAttribute('opacity', '0.8');
              rectEl.setAttribute('rx', '3');
              const title = document.createElementNS(svgNS, 'title');
              title.textContent = '价格：' + formatPrice(price) + ' ｜ 区间金额：' + formatCurrency(value) + ' USD';
              rectEl.appendChild(title);
              chartSvg.appendChild(rectEl);
            });
          };
          drawBars(asks, '#fbbf24');
          drawBars(bids, '#38bdf8');
          if (Number.isFinite(latestPrice)) {
            const latestX = scaleX(latestPrice);
            const leftArea = document.createElementNS(svgNS, 'rect');
            leftArea.setAttribute('x', padding.left);
            leftArea.setAttribute('y', padding.top);
            leftArea.setAttribute('width', Math.max(0, latestX - padding.left));
            leftArea.setAttribute('height', chartHeight);
            leftArea.setAttribute('fill', 'rgba(248, 113, 113, 0.08)');
            chartSvg.appendChild(leftArea);
            const rightArea = document.createElementNS(svgNS, 'rect');
            rightArea.setAttribute('x', latestX);
            rightArea.setAttribute('y', padding.top);
            rightArea.setAttribute('width', Math.max(0, padding.left + chartWidth - latestX));
            rightArea.setAttribute('height', chartHeight);
            rightArea.setAttribute('fill', 'rgba(34, 197, 94, 0.08)');
            chartSvg.appendChild(rightArea);
          }
          const leftArea = document.createElementNS(svgNS, 'rect');
          const markerXRaw = scaleX(markerPrice);
          const markerX = Math.min(padding.left + chartWidth, Math.max(padding.left, markerXRaw));
          leftArea.setAttribute('x', padding.left);
          leftArea.setAttribute('y', padding.top);
          leftArea.setAttribute('width', Math.max(0, markerX - padding.left));
          leftArea.setAttribute('height', chartHeight);
          leftArea.setAttribute('fill', 'rgba(34, 197, 94, 0.2)');
          chartSvg.appendChild(leftArea);
          const rightArea = document.createElementNS(svgNS, 'rect');
          rightArea.setAttribute('x', markerX);
          rightArea.setAttribute('y', padding.top);
          rightArea.setAttribute('width', Math.max(0, padding.left + chartWidth - markerX));
          rightArea.setAttribute('height', chartHeight);
          rightArea.setAttribute('fill', 'rgba(248, 113, 113, 0.18)');
          chartSvg.appendChild(rightArea);
          if (Number.isFinite(markerPrice)) {
            const latestX = markerX;
            const marker = document.createElementNS(svgNS, 'line');
            marker.setAttribute('x1', latestX);
            marker.setAttribute('x2', latestX);
            marker.setAttribute('y1', padding.top);
            marker.setAttribute('y2', padding.top + chartHeight);
            marker.setAttribute('stroke', '#f8fafc');
            marker.setAttribute('stroke-dasharray', '4 4');
            marker.setAttribute('opacity', '0.8');
            chartSvg.appendChild(marker);
          }
          const yTickCount = 5;
          for (let i = 0; i < yTickCount; i += 1) {
            const ratio = i / (yTickCount - 1);
            const value = yMax * ratio;
            const y = padding.top + chartHeight - ratio * chartHeight;
            const line = document.createElementNS(svgNS, 'line');
            line.setAttribute('x1', padding.left);
            line.setAttribute('x2', padding.left + chartWidth);
            line.setAttribute('y1', y);
            line.setAttribute('y2', y);
            line.setAttribute('stroke', 'rgba(148, 163, 184, 0.18)');
            line.setAttribute('stroke-dasharray', '4 4');
            chartSvg.appendChild(line);
            const textEl = document.createElementNS(svgNS, 'text');
            textEl.setAttribute('x', padding.left - 8);
            textEl.setAttribute('y', y + 4);
            textEl.setAttribute('fill', '#94a3b8');
            textEl.setAttribute('font-size', '12');
            textEl.setAttribute('text-anchor', 'end');
            textEl.textContent = formatCurrency(value);
            chartSvg.appendChild(textEl);
          }
          const xTickCount = 10;
          for (let i = 0; i < xTickCount; i += 1) {
            const ratio = i / (xTickCount - 1);
            const price = visibleMin + priceRange * ratio;
            const x = padding.left + chartWidth * ratio;
            const textEl = document.createElementNS(svgNS, 'text');
            textEl.setAttribute('x', x);
            textEl.setAttribute('y', height - 10);
            textEl.setAttribute('fill', '#94a3b8');
            textEl.setAttribute('font-size', '12');
            textEl.setAttribute('text-anchor', 'middle');
            textEl.textContent = formatPrice(price);
            chartSvg.appendChild(textEl);
          }
          lastRenderState = {
            minPrice,
            maxPrice,
            visibleRange,
            priceRangeFull,
            chartWidth,
          };
        };

        const renderHistoryTables = (historyPayload) => {
          const bins = Array.isArray(historyPayload?.bins) ? historyPayload.bins : [];
          if (bins.length) {
            lastHistoryPayload = historyPayload;
          }
          if (rangeEl) {
            const start = historyPayload?.range_start ? formatTimestamp(historyPayload.range_start) : '--';
            const end = historyPayload?.range_end ? formatTimestamp(historyPayload.range_end) : '--';
            rangeEl.textContent = (start && end) ? (start + ' ~ ' + end) : '--';
          }
          const latestPrice = Number(historyPayload?.latest_price);
          const filteredBins = applyOrderFilter(bins, latestPrice);
          if (!filteredBins.length) {
            updateHistoryTablesUI([]);
            showEmpty(orderMode === 'open' ? '暂无未清算订单' : '最近24小时无数据');
            return;
          }
          updateHistoryTablesUI(filteredBins);
          renderBars(filteredBins, latestPrice);;
        };

        const persistBins = (bins, latestPrice, sourceTimestamp) => {
          if (!bins.length) { return; }
          const payload = {
            instrument,
            latest_price: Number.isFinite(latestPrice) ? latestPrice : null,
            timestamp: sourceTimestamp || new Date().toISOString(),
            bin_size: binSize,
            bins: bins.map((bin) => ({
              price: bin.price,
              notional: bin.notional,
              cumulative: bin.cumulative,
              size: Number.isFinite(bin.size) ? bin.size : null,
              side: bin.side,
            })),
          };
          fetch('/api/liquidation-map/bins', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          }).catch((error) => console.error('Persist liquidation map bins failed', error));
        };

        const render = (payload) => {
          const items = Array.isArray(payload?.items) ? payload.items : [];
          const instrumentItem = items.find((entry) => (entry?.instrument_id || '').toUpperCase() === instrument) || items[0];
          if (!instrumentItem) {
            showEmpty('暂无深度数据');
            if (lastHistoryPayload) {
              renderHistoryTables(lastHistoryPayload);
            }
            return;
          }
          const latestPrice = deriveLatestPrice(instrumentItem);
          if (Number.isFinite(latestPrice)) {
            lastPriceEl.textContent = formatNumber(latestPrice, 2);
            syncLegendPrice();
          }
          updatedEl.textContent = formatTimestamp(instrumentItem.timestamp || payload.updated_at);
          const bids = normalizeLevels(instrumentItem.bids);
          const asks = normalizeLevels(instrumentItem.asks);
          if (!bids.length && !asks.length) {
            showEmpty('暂无深度数据');
            if (lastHistoryPayload) {
              renderHistoryTables(lastHistoryPayload);
            }
            return;
          }
          const bidBins = accumulateSide(groupBins(bids), 'bid');
          const askBins = accumulateSide(groupBins(asks), 'ask');
          const allBins = [...bidBins, ...askBins].sort((a, b) => a.price - b.price);
          if (!lastHistoryPayload || !(lastHistoryPayload.bins || []).length) {
            renderBars(allBins, latestPrice);
          }
          persistBins(allBins, latestPrice, instrumentItem.timestamp || payload.updated_at);
        };

        const fetchDepth = () => {
          const url = new URL('/api/streams/orderbook/latest', window.location.origin);
          url.searchParams.set('instrument', instrument);
          url.searchParams.set('limit', String(maxLevels));
          url.searchParams.set('fresh', '1');
          return fetch(url.toString(), { headers: { 'Accept': 'application/json' } }).then((res) => {
            if (!res.ok) { throw new Error('无法获取市场深度'); }
            return res.json();
          });
        };

        const fetchHistory = () => {
          const url = new URL('/api/liquidation-map/history', window.location.origin);
          url.searchParams.set('instrument', instrument);
          url.searchParams.set('hours', String(historyHours));
          return fetch(url.toString(), { headers: { 'Accept': 'application/json' } })
            .then((res) => {
              if (!res.ok) { throw new Error('无法获取历史下单数据'); }
              return res.json();
            });
        };

        const syncRangeButtons = () => {
          if (!rangeButtons.length) { return; }
          rangeButtons.forEach((btn) => {
            const btnHours = Number(btn.dataset.hours) || 24;
            btn.classList.toggle('active', btnHours === historyHours);
          });
        };

        const syncOrderModeButtons = () => {
          if (!orderModeButtons.length) { return; }
          orderModeButtons.forEach((btn) => {
            const btnMode = (btn.dataset.mode || 'all').toLowerCase();
            btn.classList.toggle('active', btnMode === orderMode);
          });
        };

        const rebuildCumulativeTotals = (bins) => {
          if (!Array.isArray(bins)) { return []; }
          const totals = { bid: 0, ask: 0 };
          return bins.map((bin) => {
            const side = String(bin?.side || '').toLowerCase();
            if (side !== 'bid' && side !== 'ask') {
              return { ...bin };
            }
            const notional = Math.max(0, Number(bin?.notional) || 0);
            totals[side] += notional;
            return { ...bin, cumulative: totals[side] };
          });
        };

        const applyOrderFilter = (bins, latestPrice) => {
          if (!Array.isArray(bins)) { return []; }
          if (orderMode !== 'open' || !Number.isFinite(latestPrice)) {
            return bins.slice();
          }
          const filtered = bins.filter((bin) => {
            const price = Number(bin?.price);
            if (!Number.isFinite(price)) { return false; }
            const side = String(bin?.side || '').toLowerCase();
            if (side === 'bid') {
              return price <= latestPrice + 1e-9;
            }
            if (side === 'ask') {
              return price >= latestPrice - 1e-9;
            }
            return true;
          });
          return rebuildCumulativeTotals(filtered);
        };

        const refresh = () => {
          fetchHistory()
            .then((history) => renderHistoryTables(history))
            .catch((error) => {
              console.error(error);
              if (lastHistoryPayload) {
                renderHistoryTables(lastHistoryPayload);
              }
            })
            .finally(() => {
              fetchDepth()
                .then((payload) => render(payload))
                .catch((error) => {
                  console.error(error);
                  showEmpty('深度获取失败');
                });
            });
        };

        if (rangeButtons.length) {
          rangeButtons.forEach((button) => {
            button.addEventListener('click', () => {
              const hours = Number(button.dataset.hours) || 24;
              if (historyHours === hours) {
                return;
              }
              historyHours = hours;
              syncRangeButtons();
              try {
                const url = new URL(window.location.href);
                url.searchParams.set('hours', String(historyHours));
                url.searchParams.set('mode', orderMode);
                window.location.href = url.toString();
              } catch (err) {
                console.warn('Failed to update URL, reloading fallback', err);
                window.location.reload();
              }
            });
          });
        }

        if (orderModeButtons.length) {
          orderModeButtons.forEach((button) => {
            button.addEventListener('click', () => {
              const mode = (button.dataset.mode || 'all').toLowerCase();
              if (orderMode === mode) {
                return;
              }
              orderMode = mode === 'open' ? 'open' : 'all';
              syncOrderModeButtons();
              try {
                const url = new URL(window.location.href);
                url.searchParams.set('mode', orderMode);
                url.searchParams.set('hours', String(historyHours));
                window.location.href = url.toString();
              } catch (err) {
                console.warn('Failed to update URL, reloading fallback', err);
                window.location.reload();
              }
            });
          });
        }

        syncRangeButtons();
        syncOrderModeButtons();

        refresh();
        setInterval(refresh, REFRESH_MS);

        if (chartSvg) {
          chartSvg.addEventListener('wheel', (event) => {
            event.preventDefault();
            const direction = event.deltaY > 0 ? 1 : -1;
            const factor = direction > 0 ? 0.9 : 1.1;
            zoomLevel = Math.min(5, Math.max(1, zoomLevel * factor));
            renderBars(lastRenderBins, lastRenderPrice);
          });
          let isDragging = false;
          let startX = 0;
          chartSvg.addEventListener('mousedown', (event) => {
            isDragging = true;
            startX = event.clientX;
          });
          window.addEventListener('mouseup', () => { isDragging = false; });
          window.addEventListener('mousemove', (event) => {
            if (!isDragging) { return; }
            const state = lastRenderState;
            if (!state) { return; }
            const rectMove = chartSvg.getBoundingClientRect();
            const widthMove = state.chartWidth || rectMove.width || 1;
            const deltaX = event.clientX - startX;
            startX = event.clientX;
            const normalizedShift = deltaX / Math.max(widthMove, 1);
            const rangeRatio = state.priceRangeFull > 0 ? (state.visibleRange / state.priceRangeFull) : 0;
            panOffset = Math.max(-1, Math.min(1, panOffset - normalizedShift * rangeRatio * 2));
            renderBars(lastRenderBins, lastRenderPrice);
          });
          let resizeTimer = null;
          window.addEventListener('resize', () => {
            if (resizeTimer) {
              clearTimeout(resizeTimer);
            }
            resizeTimer = setTimeout(() => {
              renderBars(lastRenderBins, lastRenderPrice);
            }, 150);
          });
        }
      })();
    </script>

    <p style="margin-top: 1.5rem; font-size: 0.85rem; color: #94a3b8; text-align: center;">Code by Yuhao@jiansutech.com at 2025.11</p>
</body>
</html>
""")
