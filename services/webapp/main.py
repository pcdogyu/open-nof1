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
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Form, status, Request
from fastapi.responses import HTMLResponse, RedirectResponse

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
    summary = routes.get_okx_summary(force_refresh=force_refresh)
    return HTMLResponse(content=_render_okx_dashboard(summary, order_status, order_detail))


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
    try:
        result = routes.place_manual_okx_order(
            account_id=account_id.strip(),
            instrument_id=instrument_id.strip(),
            side=side.strip(),
            order_type=order_type.strip(),
            size=size,
            price=price,
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


@app.post("/okx/close-position", include_in_schema=False)
def okx_close_position(
    account_id: str = Form(...),
    instrument_id: str = Form(...),
    position_side: str = Form(...),
    quantity: float = Form(...),
    margin_mode: Optional[str] = Form(None),
) -> RedirectResponse:
    try:
        result = routes.close_okx_position(
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
    try:
        levels = max(1, min(int(levels_param), 400))
    except ValueError:
        levels = 400
    snapshot = routes.get_orderbook_snapshot(levels=levels, instrument=instrument)
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments", [])
    return HTMLResponse(content=_render_orderbook_page(snapshot, instruments, levels))


@app.get("/settings", include_in_schema=False, response_class=HTMLResponse)
def pipeline_settings_page() -> HTMLResponse:
    settings = routes.get_pipeline_settings()
    catalog = routes.get_instrument_catalog(limit=300)
    return HTMLResponse(content=_render_settings_page(settings, catalog))


@app.post("/settings/update", include_in_schema=False)
def submit_pipeline_settings(
    poll_interval: int = Form(...),
    instruments: str = Form(""),
) -> RedirectResponse:
    instrument_list = _parse_instrument_input(instruments)
    routes.update_pipeline_settings(instrument_list, poll_interval)
    return RedirectResponse(url="/settings", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/settings/add", include_in_schema=False)
def add_pipeline_instrument(
    new_instrument: str = Form(""),
    poll_interval: int = Form(...),
    current_instruments: str = Form(""),
) -> RedirectResponse:
    instrument_list = _parse_instrument_input(current_instruments)
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
    take_profit_pct: float = Form(0),
    stop_loss_pct: float = Form(0),
    default_leverage: int = Form(1),
    max_leverage: int = Form(125),
    pyramid_max_orders: int = Form(100),
) -> RedirectResponse:
    routes.update_risk_settings(
        price_tolerance_pct=price_tolerance_pct / 100.0,
        max_drawdown_pct=max_drawdown_pct,
        max_loss_absolute=max_loss_absolute,
        cooldown_seconds=cooldown_seconds,
        min_notional_usd=min_notional_usd,
        max_order_notional_usd=max_order_notional_usd,
        take_profit_pct=take_profit_pct,
        stop_loss_pct=stop_loss_pct,
        default_leverage=default_leverage,
        max_leverage=max_leverage,
        pyramid_max_orders=pyramid_max_orders,
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
) -> RedirectResponse:
    routes.update_scheduler_settings(market_interval, ai_interval)
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



def _render_okx_dashboard(summary: dict, order_status: str | None = None, order_detail: str | None = None) -> str:
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
        flash_block = (
            f"<div class='flash {kind}'>"
            f"{'下单成功' if kind == 'success' else '下单失败'}"
            f"{'：' + esc(order_detail) if order_detail else ''}"
            "</div>"
        )

    account_sections: list[str] = []
    for bundle in summary.get("accounts", []):
        account = bundle.get("account", {}) or {}
        account_id = esc(account.get("account_id"))
        model_id = esc(account.get("model_id"))
        equity = _format_number(account.get("equity"))
        cash_balance = _format_number(account.get("cash_balance"))
        pnl_value = account.get("pnl")
        pnl = _format_number(pnl_value)
        pnl_class = "pnl-positive" if (pnl_value or 0) >= 0 else "pnl-negative"
        starting_equity = _format_number(account.get("starting_equity"))

        balances_html = _render_balances_table(bundle.get("balances", []))
        positions_html = _render_positions_table(bundle.get("positions", []), account_id=account_id)
        trades_html = _render_trades_table(bundle.get("recent_trades", []))
        orders_html = _render_orders_table(bundle.get("open_orders", []), account_id=account_id)
        curve_html = _render_equity_curve(bundle.get("equity_curve", []))

        account_sections.append(
            f"""
            <section class=\"okx-card\">
                <header>
                    <h2>{account_id or '未命名账户'}</h2>
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
                            <td><span class=\"{pnl_class}\">{pnl} USD</span></td>
                        </tr>
                    </table>
                </header>
                <div class="panel">
                    <h3>持仓明细 <form method='post' action='/okx/close-all-positions' class='inline-form' style='margin-left: 10px;'><input type='hidden' name='account_id' value='{account_id}'><button type='submit' class='btn-close' onclick="return confirm('确认平仓该账户的所有仓位？');">一键平仓全部</button></form></h3>
                    {positions_html}
                </div>
                <div class=\"panel\">
                    <h3>挂单列表</h3>
                    {orders_html}
                </div>
                <div class=\"split\">
                    <div class=\"panel\">
                        <h3>余额信息</h3>
                        {balances_html}
                    </div>
                    <div class=\"panel recent-trades-panel\">
                        <h3>近期成交</h3>
                        {trades_html}
                    </div>
                </div>
                <div class=\"panel\">
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
    grid_class = "book-grid two-cols" if items and len(items) <= 2 else "book-grid"
    return ORDERBOOK_TEMPLATE.format(
        instrument_options=instrument_options,
        level_options=level_options,
        updated_at=esc(_format_asia_shanghai(snapshot.get("updated_at"))),
        cards=cards,
        levels=levels,
        book_grid_class=grid_class,
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

def _render_settings_page(settings: dict, catalog: Sequence[dict]) -> str:
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
        catalog_hint = f"已缓存 {catalog_count} 个 OKX 交易对信息"
    else:
        catalog_hint = "未发现已缓存的币对，请点击\"刷新币对库\" 按钮获取"

    return SETTINGS_TEMPLATE.format(
        poll_interval=poll_interval,
        updated_at=updated_at,
        chips_html=chips_html,
        instruments_text=esc(instruments_text),
        current_instruments_value=esc(current_value_attr),
        datalist_options_usdt=options_html_usdt,
        catalog_hint=esc(catalog_hint),
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
        notes = entry.get("notes") or ""
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
            f"<td>{esc(notes) if notes else '--'}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='11' class='empty-state'>暂无订单调试数据</td></tr>")
    return ORDER_DEBUG_TEMPLATE.format(rows="\n".join(rows))


def _render_risk_page(settings: dict) -> str:
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
    pyramid_max_orders = max(0, min(100, int(settings.get("pyramid_max_orders") or 0)))
    take_profit_pct = max(0.0, min(500.0, float(settings.get("take_profit_pct") or 0.0)))
    stop_loss_pct = max(0.0, min(95.0, float(settings.get("stop_loss_pct") or 0.0)))
    default_leverage = max(1, min(125, int(settings.get("default_leverage") or 1)))
    max_leverage = max(default_leverage, min(125, int(settings.get("max_leverage") or default_leverage)))

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
    default_leverage_text = _compact(default_leverage, 0)
    max_leverage_text = _compact(max_leverage, 0)
    cooldown_minutes = cooldown_seconds / 60
    cooldown_minutes_text = _compact(cooldown_minutes, 1)
    pyramid_cap_text = _compact(pyramid_max_orders, 0)
    max_order_notional_text = _compact(max_order_notional, 2)
    updated_at = esc(_format_asia_shanghai(settings.get("updated_at")))

    return RISK_TEMPLATE.format(
        price_tolerance_pct=esc(f"{price_pct:.2f}"),
        price_tolerance_display=esc(tolerance_text),
        max_drawdown_pct=esc(f"{drawdown_pct:.2f}"),
        drawdown_display=esc(drawdown_text),
        max_loss_absolute=esc(f"{max_loss:.2f}"),
        max_loss_display=esc(loss_text),
        min_notional_usd=esc(f"{min_notional:.2f}"),
        min_notional_display=esc(min_notional_text),
        max_order_notional_usd=esc(f"{max_order_notional:.2f}"),
        max_order_notional_display=esc(max_order_notional_text),
        take_profit_pct=esc(f"{take_profit_pct:.2f}"),
        stop_loss_pct=esc(f"{stop_loss_pct:.2f}"),
        take_profit_display=esc(take_profit_text),
        stop_loss_display=esc(stop_loss_text),
        default_leverage=esc(str(default_leverage)),
        max_leverage=esc(str(max_leverage)),
        default_leverage_display=esc(default_leverage_text),
        max_leverage_display=esc(max_leverage_text),
        pyramid_max_orders=esc(str(pyramid_max_orders)),
        pyramid_max_display=esc(pyramid_cap_text),
        cooldown_seconds=esc(str(cooldown_seconds)),
        cooldown_minutes=esc(cooldown_minutes_text),
        updated_at=updated_at,
    )


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
    for pos in positions or []:
        account_value = esc(account_id or pos.get("account_id") or "")
        instrument_value = esc(pos.get("instrument_id"))
        raw_side = str(pos.get("side") or "")
        side_display = "多单" if raw_side.lower() in {"long", "buy"} else "空单" if raw_side.lower() in {"short", "sell"} else raw_side
        side_value = esc(raw_side)
        side_display_escaped = esc(side_display)
        quantity_value = esc(str(pos.get("quantity") if pos.get("quantity") is not None else ""))
        leverage_display = _format_number(pos.get("leverage"))
        try:
            qty = float(pos.get("quantity") or 0.0)
            mark_px = float(pos.get("mark_price") or 0.0)
            entry_px = float(pos.get("entry_price") or 0.0)
            leverage = float(pos.get("leverage") or 0.0)
        except Exception:
            qty = mark_px = entry_px = leverage = 0.0
        margin = None
        if qty > 0 and mark_px > 0 and leverage > 0:
            margin = (qty * mark_px) / leverage
        pnl_pct = None
        if entry_px > 0 and mark_px > 0:
            delta = (mark_px - entry_px)
            if raw_side.lower() in {"short", "sell"}:
                delta = -delta
            pnl_pct = (delta / entry_px) * 100
        action_html = (
            "<form method='post' action='/okx/close-position' class='inline-form'>"
            f"<input type='hidden' name='account_id' value='{account_value}'>"
            f"<input type='hidden' name='instrument_id' value='{instrument_value}'>"
            f"<input type='hidden' name='position_side' value='{side_value}'>"
            f"<input type='hidden' name='quantity' value='{quantity_value}'>"
            "<button type='submit' class='btn-close' onclick=\"return confirm('确认平仓该仓位？');\">平仓</button>"
            "</form>"
        )
        rows.append(
            "<tr>"
            f"<td>{esc(pos.get('position_id'))}</td>"
            f"<td>{instrument_value}</td>"
            f"<td>{side_display_escaped}</td>"
            f"<td>{leverage_display}</td>"
            f"<td>{_format_number(pos.get('quantity'))}</td>"
            f"<td>{_format_number(pos.get('entry_price'))}</td>"
            f"<td>{_format_number(pos.get('mark_price'))}</td>"
            f"<td>{_format_number(pos.get('last_price') or pos.get('last') or pos.get('mark_price'))}</td>"
            f"<td>{_format_number(margin)}</td>"
            f"<td>{_format_number(pos.get('unrealized_pnl'))}</td>"
            f"<td>{(_format_number(pnl_pct) + '%') if pnl_pct is not None else ''}</td>"
            f"<td>{esc(_format_asia_shanghai(pos.get('created_at') or pos.get('updated_at')))}</td>"
            f"<td>{action_html}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='13'>当前无持仓</td></tr>")

    return (
        "<table class='dense'>"
        "<thead><tr><th>持仓ID</th><th>交易对</th><th>方向</th><th>杠杆</th><th>持仓量</th><th>开仓均价</th><th>标记价格</th><th>最新价格</th><th>保证金</th><th>未实现盈亏</th><th>盈亏%</th><th>下单时间</th><th>操作</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )
def _render_trades_table(trades: Sequence[dict] | None) -> str:
    esc = _escape
    valid_symbols = {"ETH-USDT-SWAP", "BTC-USDT-SWAP"}
    rows = []
    for trade in trades or []:
        instrument = (trade.get("instrument_id") or "").upper()
        if instrument not in valid_symbols:
            continue
        entry_price = trade.get("entry_price") or trade.get("price")
        exit_price = (
            trade.get("close_price")
            or trade.get("exit_price")
            or trade.get("price")
        )
        fee = trade.get("fee") or trade.get("feeCcy") or trade.get("feeUsd")
        realized_pnl = trade.get("realized_pnl")
        # 仅在平仓侧显示盈亏；开仓侧为空。非零才显示数值。
        pnl_cell = _format_number(realized_pnl, digits=4) if realized_pnl not in (None, 0) else ""
        rows.append(
            "<tr>"
            f"<td>{esc(_format_asia_shanghai(trade.get('executed_at')))}</td>"
            f"<td>{esc(trade.get('model_id'))}</td>"
            f"<td>{esc(trade.get('account_id') or trade.get('portfolio_id'))}</td>"
            f"<td>{esc(instrument)}</td>"
            f"<td>{esc(trade.get('side'))}</td>"
            f"<td>{_format_number(trade.get('quantity'))}</td>"
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
        "<tr><th>成交时间</th><th>模型</th><th>投资组合</th><th>合约</th>"
        "<th>方向</th><th>数量</th><th>入场价</th><th>离场价</th><th>手续费</th><th>盈亏(USD)</th></tr>"
        "</thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
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
</body>
</html>
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
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
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
        .risk-grid {{ display: flex; flex-wrap: wrap; gap: 24px; }}
        .risk-card {{ flex: 1 1 320px; background-color: #1e293b; border-radius: 16px; padding: 24px; box-shadow: 0 8px 24px rgba(2, 6, 23, 0.65); }}
        .risk-card.secondary {{ background-color: rgba(15, 23, 42, 0.8); border: 1px solid rgba(59, 130, 246, 0.2); }}
        .risk-card h2 {{ margin-top: 0; margin-bottom: 1rem; }}
        form label {{ display: flex; flex-direction: column; gap: 6px; font-weight: 600; margin-top: 1rem; }}
        form label:first-of-type {{ margin-top: 0; }}
        input[type="number"] {{ border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.4); background-color: rgba(15, 23, 42, 0.9); padding: 10px 12px; color: #e2e8f0; font-size: 1rem; }}
        input[type="number"]:focus {{ outline: none; border-color: #38bdf8; box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2); }}
        button {{ margin-top: 1.5rem; width: 100%; padding: 12px 0; border: none; border-radius: 10px; font-size: 1rem; background-color: #38bdf8; color: #0f172a; cursor: pointer; font-weight: 600; }}
        button:hover {{ background-color: #0ea5e9; }}
        .hint {{ color: #94a3b8; font-size: 0.9rem; margin-top: 0.3rem; }}
        .timestamp {{ color: #38bdf8; }}
        .rule-list {{ list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 12px; }}
        .rule-list li {{ padding: 12px; border-radius: 10px; background-color: rgba(30, 41, 59, 0.8); border: 1px solid rgba(56, 189, 248, 0.18); font-size: 0.95rem; }}
        @media (max-width: 768px) {{
            .risk-card {{ flex: 1 1 100%; }}
        }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">盘口深度</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">管道设置</a>
        <a href="/risk" class="nav-link active">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单链路</a>
    </nav>

    <header>
        <h1>风险控制面板</h1>
        <p class="updated">最近更新：<span class="timestamp">{updated_at}</span></p>
        <p class="hint">更新后将同步到 config.py 并通知后台任务</p>
        <p class="hint">数据馈送：盘口深度、爆仓流数据将注入模型特征辅助人工/AI决策，已增加价格和成交量推送给大模型。</p>
    </header>

    <div class="risk-grid">
        <section class="risk-card primary">
            <h2>风险参数</h2>
            <form method="post" action="/risk/update">
                <label for="price-tolerance">价格波动容忍度（%）
                    <input id="price-tolerance" type="number" name="price_tolerance_pct" min="0.1" max="50" step="0.1" value="{price_tolerance_pct}" required>
                    <span class="hint">剧烈波动时参考价 ±<span id="hint-price-value">{price_tolerance_display}</span>% 内。</span>
                </label>
                <label for="min-notional">最小下单金额（USDT）
                    <input id="min-notional" type="number" name="min_notional_usd" min="0" step="0.01" value="{min_notional_usd}" required>
                    <span class="hint">当前门槛：<span id="hint-min-notional">{min_notional_display}</span> USDT，0 表示关闭。</span>
                </label>
                <label for="max-order-notional">最大下单金额 (USDT)
                    <input id="max-order-notional" type="number" name="max_order_notional_usd" min="0" step="0.01" value="{max_order_notional_usd}" required>
                    <span class="hint">0 表示不限制；当前 <span id="hint-max-order-notional">{max_order_notional_display}</span> USDT。</span>
                </label>
                <label for="pyramid-max">金字塔单向上限（单币对）
                    <input id="pyramid-max" type="number" name="pyramid_max_orders" min="0" max="100" step="1" value="{pyramid_max_orders}" required>
                    <span class="hint">同向累计订单最多 <span id="hint-pyramid-value">{pyramid_max_display}</span> 笔，0 表示不限制。</span>
                </label>
                <label for="max-drawdown">最大回撤（%）
                    <input id="max-drawdown" type="number" name="max_drawdown_pct" min="0.1" max="95" step="0.1" value="{max_drawdown_pct}" required>
                </label>
                <label for="loss-limit">累计亏损上限（USDT）
                    <input id="loss-limit" type="number" name="max_loss_absolute" min="1" step="1" value="{max_loss_absolute}" required>
                </label>
                <label for="default-leverage">默认杠杆（倍）
                    <input id="default-leverage" type="number" name="default_leverage" min="1" max="125" step="1" value="{default_leverage}" required>
                    <span class="hint">建仓时默认杠杆，范围 1-125。</span>
                </label>
                <label for="max-leverage">最大杠杆（倍）
                    <input id="max-leverage" type="number" name="max_leverage" min="1" max="125" step="1" value="{max_leverage}" required>
                    <span class="hint">不得低于默认杠杆，范围 1-125。</span>
                </label>
                <label for="take-profit">止盈阈值（%）
                    <input id="take-profit" type="number" name="take_profit_pct" min="0" max="500" step="0.1" value="{take_profit_pct}" required>
                    <span class="hint">未实现收益达到阈值时停止开仓，0 表示关闭。</span>
                </label>
                <label for="stop-loss">止损阈值（%）
                    <input id="stop-loss" type="number" name="stop_loss_pct" min="0" max="95" step="0.1" value="{stop_loss_pct}" required>
                    <span class="hint">未实现亏损达到 -阈值 时停止开仓，0 表示关闭。</span>
                </label>
                <label for="cooldown-seconds">冷静时间（秒）
                    <input id="cooldown-seconds" type="number" name="cooldown_seconds" min="10" max="86400" step="10" value="{cooldown_seconds}" required>
                    <span class="hint">相当于暂停 <span id="hint-cooldown-value">{cooldown_minutes}</span> 分钟后再恢复（最短 10 秒，提高下单频率）</span>
                </label>
                <button type="submit">保存策略参数</button>
            </form>
        </section>
        <section class="risk-card secondary">
            <h2>规则说明</h2>
            <ul class="rule-list">
                <li>价格限制：委托价格需落在参考价 ±<span id="rule-price-value">{price_tolerance_display}</span>% 内。</li>
                <li>最小名义：下单金额须 ≥ <span id="rule-min-notional-value">{min_notional_display}</span> USDT，0 表示不限制。</li>
                <li>金字塔：同币种同方向最多 <span id="rule-pyramid-value">{pyramid_max_display}</span> 笔（0 表示关闭限制）。</li>
                <li>回撤保护：账户回撤超过 <span id="rule-drawdown-value">{drawdown_display}</span>% 时进入冷静期。</li>
                <li>亏损限制：累计亏损超出 <span id="rule-loss-value">{max_loss_display}</span> USDT 时停止交易。</li>
                <li>杠杆限制：默认 <span id="rule-default-lev">{default_leverage_display}</span>x，最大 <span id="rule-max-lev">{max_leverage_display}</span>x。</li>
                <li>止盈止损：未实现盈亏 ≥ <span id="rule-take-profit-value">{take_profit_display}</span>% 或 ≤ -<span id="rule-stop-loss-value">{stop_loss_display}</span>% 时停止开仓。</li>
                <li>等待时长：冷静后等待 <span id="rule-cooldown-value">{cooldown_minutes}</span> 分钟或手动恢复。</li>
                <li>数据馈送：盘口深度、爆仓流数据将注入模型特征辅助人工/AI决策。</li>
            </ul>
            <p class="hint">保存后写入 config.py 并通知调度任务</p>
        </section>
    </div>
    <script>
      (function() {{{{
        const priceInput = document.getElementById('price-tolerance');
        const drawdownInput = document.getElementById('drawdown-limit');
        const lossInput = document.getElementById('loss-limit');
        const cooldownInput = document.getElementById('cooldown-seconds');
        const minNotionalInput = document.getElementById('min-notional');
        const pyramidInput = document.getElementById('pyramid-max');
        const takeProfitInput = document.getElementById('take-profit');
        const stopLossInput = document.getElementById('stop-loss');
        const defaultLevInput = document.getElementById('default-leverage');
        const maxLevInput = document.getElementById('max-leverage');
        if (!priceInput || !drawdownInput || !lossInput || !cooldownInput || !minNotionalInput || !pyramidInput || !takeProfitInput || !stopLossInput || !defaultLevInput || !maxLevInput) {{
          return;
        }}}}
        const formatCompact = (value, digits = 2) => {{{{
          const num = Number(value);
          if (!isFinite(num)) {{{{
            return '--';
          }}}}
          const fixed = num.toFixed(digits);
          return fixed.replace(/\.?0+$/, '') || '0';
        }}}};
        const updateRules = () => {{
          const priceValue = Number(priceInput.value) || 0;
          const drawdownValue = Number(drawdownInput.value) || 0;
          const lossValue = Number(lossInput.value) || 0;
          const cooldownSeconds = Number(cooldownInput.value) || 0;
          const cooldownMinutes = cooldownSeconds / 60;
          const minNotional = Number(minNotionalInput.value) || 0;
          const pyramidCap = Number(pyramidInput.value) || 0;
          const takeProfit = Number(takeProfitInput.value) || 0;
          const stopLoss = Number(stopLossInput.value) || 0;
          const defaultLev = Number(defaultLevInput.value) || 0;
          const maxLev = Number(maxLevInput.value) || 0;
          const priceText = formatCompact(priceValue, 2);
          const drawdownText = formatCompact(drawdownValue, 2);
          const lossText = formatCompact(lossValue, 2);
          const cooldownText = formatCompact(cooldownMinutes, 1);
          const minNotionalText = formatCompact(minNotional, 2);
          const pyramidCapText = formatCompact(pyramidCap, 0);
          const takeProfitText = formatCompact(takeProfit, 2);
          const stopLossText = formatCompact(stopLoss, 2);
          const defaultLevText = formatCompact(defaultLev, 0);
          const maxLevText = formatCompact(maxLev, 0);
          document.getElementById('hint-price-value').textContent = priceText;
          document.getElementById('rule-price-value').textContent = priceText;
          document.getElementById('rule-drawdown-value').textContent = drawdownText;
          document.getElementById('rule-loss-value').textContent = lossText;
          document.getElementById('hint-cooldown-value').textContent = cooldownText;
          document.getElementById('rule-cooldown-value').textContent = cooldownText;
          document.getElementById('hint-min-notional').textContent = minNotionalText;
          document.getElementById('rule-min-notional-value').textContent = minNotionalText;
          document.getElementById('hint-pyramid-value').textContent = pyramidCapText;
          document.getElementById('rule-pyramid-value').textContent = pyramidCapText;
          document.getElementById('rule-take-profit-value').textContent = takeProfitText;
          document.getElementById('rule-stop-loss-value').textContent = stopLossText;
          document.getElementById('rule-default-lev').textContent = defaultLevText;
          document.getElementById('rule-max-lev').textContent = maxLevText;
        }};
        ['input', 'change'].forEach((eventName) => {{{{
          [priceInput, drawdownInput, lossInput, cooldownInput, minNotionalInput, pyramidInput, takeProfitInput, stopLossInput, defaultLevInput, maxLevInput].forEach((input) => {{{{
            input.addEventListener(eventName, updateRules);
          }}}});
        }}}});
        updateRules();
      }}}})();
    </script>
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
        table.dense {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; background-color: rgba(15, 23, 42, 0.6); border-radius: 6px; overflow: hidden; }}
        table.dense th, table.dense td {{ padding: 10px 12px; border-bottom: 1px solid #334155; text-align: left; font-size: 0.9rem; }}
        ul.curve-list {{ list-style: none; padding: 0; margin: 0.5rem 0 0 0; }}
        ul.curve-list li {{ padding: 6px 0; border-bottom: 1px dashed #334155; font-size: 0.9rem; }}
        .inline-row {{ display: inline-flex; align-items: center; gap: 10px; margin: 0; }}
        .inline-form {{ display: inline-flex; align-items: center; margin: 0; }}
        .btn-refresh {{ font-size: 12px; padding: 4px 8px; border-radius: 6px; border: 1px solid #38bdf8; color: #38bdf8; text-decoration: none; background: transparent; }}
        .btn-refresh:hover {{ background-color: rgba(56, 189, 248, 0.15); }}
        .btn-close {{ background-color: #f87171; color: #0f172a; border: none; padding: 6px 12px; border-radius: 8px; cursor: pointer; font-weight: 600; }}
        .btn-close:hover {{ background-color: #ef4444; }}
        .local-time {{ color: #94a3b8; font-size: 0.9rem; }}
        .flash {{ margin: 12px 0; padding: 12px 16px; border-radius: 10px; font-size: 0.95rem; }}
        .flash.success {{ background-color: rgba(74, 222, 128, 0.12); color: #4ade80; border: 1px solid rgba(74, 222, 128, 0.3); }}
        .flash.error {{ background-color: rgba(248, 113, 113, 0.12); color: #f87171; border: 1px solid rgba(248, 113, 113, 0.3); }}
        .manual-card {{ background-color: #1e293b; border-radius: 12px; padding: 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); }}
        .manual-form {{ display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }}
        .manual-form label {{ display: flex; flex-direction: column; gap: 6px; color: #94a3b8; font-size: 0.9rem; }}
        .manual-form input, .manual-form select {{ border-radius: 8px; border: 1px solid #334155; background-color: #0f172a; color: #e2e8f0; padding: 10px; font-size: 0.95rem; }}
        .manual-form .actions {{ grid-column: 1 / -1; display: flex; gap: 10px; align-items: center; }}
        .manual-form button {{ background-color: #38bdf8; color: #0f172a; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-weight: bold; }}
        .manual-form button:hover {{ background-color: #0ea5e9; }}
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
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link active">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
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
        <label>账户
          <select name="account_id" required>
            {manual_account_options}
          </select>
        </label>
        <label>合约
          <select name="instrument_id" required>
            {manual_instrument_options}
          </select>
        </label>
        <label>方向
          <select name="side" required>
            <option value="buy">买入</option>
            <option value="sell">卖出</option>
          </select>
        </label>
        <label>类型
          <select name="order_type" id="order-type" required>
            <option value="limit">限价</option>
            <option value="market">市价</option>
          </select>
        </label>
        <label>数量
          <input type="number" step="any" min="0" name="size" required>
        </label>
        <label>价格（限价必填）
          <input type="number" step="any" min="0" name="price" id="price-input" placeholder="市价单可留空">
        </label>
        <label>保证金模式
          <select name="margin_mode">
            <option value="">自动</option>
            <option value="cross">全仓</option>
            <option value="isolated">逐仓</option>
            <option value="cash">现货</option>
          </select>
        </label>
        <div class="actions">
          <button type="submit">提交订单</button>
          <p class="hint">使用 OKX 模拟接口下单，自动补充账户 API 密钥。</p>
        </div>
      </form>
    </section>
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
      }})();
    </script>
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
        .log-card table {{ table-layout: fixed; }}
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
            <button type="submit">保存调度设置</button>
        </form>
        <div class="meta">
            最近更新：<span class="timestamp">{updated_at}</span>
        </div>
        <p class="hint test-status" id="scheduler-test-status">点击按钮会立即触发行情或 AI 任务，可用于验证调度逻辑。</p>
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
        <a href="/prompts" class="nav-link active">提示词</a>
        <a href="/settings" class="nav-link">币对配置</a>
        <a href="/risk" class="nav-link">风险控制</a>
        <a href="/scheduler" class="nav-link">调度器</a>
        <a href="/orders/debug" class="nav-link">下单调试</a>
    </nav>

    <div class="prompt-layout">
        <header>
            <h1>提示词模板</h1>
            <p class="hint">模板中可以自由编排说明，让 LLM 在下单前参考实时仓位与风控上下文。使用 <code>{{{{</code> 与 <code>}}}}</code> 输出字面花括号。</p>
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
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link active">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
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
        <span>最后更新：<span class="timestamp" id="liquidations-updated">{updated_at}</span></span>
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
        const DEFAULT_LIMIT = 50;
        let latestItems = [];
        const fmtNumber = (value, digits) => {{
          if (value === null || value === undefined || value === '') {{ return '-'; }}
          const num = Number(value);
          if (!isFinite(num)) {{ return '-'; }}
          return num.toFixed(digits);
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
        const computeNotional = (item) => {{
          if (!item) {{ return null; }}
          const provided = Number(item.notional_value);
          if (Number.isFinite(provided)) {{
            return provided;
          }}
          const qty = Math.abs(Number(item.net_qty));
          const price = Number(item.last_price);
          if (Number.isFinite(qty) && Number.isFinite(price)) {{
            return qty * price;
          }}
          return null;
        }};
        const applyFilters = (items) => {{
          const minQty = Math.max(0, Number(minSizeInput.value) || 0);
          const minNotional = Math.max(0, Number(minNotionalInput.value) || 0);
          return items.filter((item) => {{
            const qty = Math.abs(Number(item.net_qty)) || 0;
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
            body.innerHTML = '<tr><td colspan="7" class="empty-state">暂无爆仓数据</td></tr>';
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
              <td>${{notional === null ? '-' : fmtNumber(notional, 2)}}</td>`;
            body.appendChild(tr);
          }});
        }};
        function refresh() {{
          const params = new URLSearchParams({{ limit: DEFAULT_LIMIT.toString() }});
          if (select.value) {{ params.set('instrument', select.value); }}
          fetch('/api/streams/liquidations/latest?' + params.toString())
            .then((res) => res.json())
            .then((data) => {{
              latestItems = Array.isArray(data.items) ? data.items : [];
              renderRows();
              const updated = document.getElementById('liquidations-updated');
              if (updated) {{ updated.textContent = fmtTimestamp(data.updated_at); }}
            }})
            .catch((err) => console.error(err));
        }}
        select.addEventListener('change', refresh);
        [minSizeInput, minNotionalInput].forEach((input) => {{
          input.addEventListener('input', () => renderRows());
        }});
        refresh();
        setInterval(refresh, 3000);
      }})();
    </script>
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
        @media (max-width: 1200px) {{ .book-grid.two-cols {{ grid-template-columns: 1fr; column-gap: 12px; }} }}
        .orderbook-chart {{ background: #0f1b2d; border-radius: 16px; padding: 18px 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); border: 1px solid rgba(148, 163, 184, 0.15); display: flex; flex-direction: column; gap: 14px; width: calc(100% - 40px); }}
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
            if (count > 0 && count <= 2) {{
              container.className = `${{BASE_GRID_CLASS}} two-cols`;
            }} else {{
              container.className = BASE_GRID_CLASS;
            }}
          }};
          const levels = parseInt(levelSelect.value || '{levels}', 10) || {levels};
          const filterValue = (instrumentSelect.value || '').trim();
          let items = Array.isArray(data.items) ? data.items.slice() : [];
          if (!filterValue) {{
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
                <span class="stat-pill">买盘深度（前${{levels}}档）${{buyDepthText}}</span>
                <span class="stat-pill">卖盘深度（前${{levels}}档）${{sellDepthText}}</span>
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
        setInterval(refresh, 3000);
      }})();
    </script>
</body>
</html>
"""
