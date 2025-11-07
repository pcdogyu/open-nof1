# Trigger reloader
"""
Entrypoint for the open-nof1.ai user-facing web service.
"""

from __future__ import annotations

import html
import logging
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
    summary = routes.get_okx_summary(force_refresh=force_refresh)
    return HTMLResponse(content=_render_okx_dashboard(summary))


@app.get("/liquidations", include_in_schema=False, response_class=HTMLResponse)
def liquidation_dashboard(request: Request) -> HTMLResponse:
    instrument = request.query_params.get("instrument")
    snapshot = routes.get_liquidation_snapshot(limit=30, instrument=instrument)
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments", [])
    return HTMLResponse(content=_render_liquidation_page(snapshot, instruments))


@app.get("/orderbook", include_in_schema=False, response_class=HTMLResponse)
def orderbook_dashboard(request: Request) -> HTMLResponse:
    instrument = request.query_params.get("instrument")
    levels_param = request.query_params.get("levels", "50")
    try:
        levels = max(1, min(int(levels_param), 400))
    except ValueError:
        levels = 10
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
        exit_price = trade.get("exit_price")
        if exit_price is None:
            exit_price = price
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
        trade_rows.append("<tr><td colspan='9'>暂无成交记录</td></tr>")
    signal_rows: list[str] = []
    for signal in metrics.get("recent_ai_signals", []):
        action_zh = signal.get("action_zh") or "未知"
        action_en = signal.get("action_en") or "Unknown"
        reason = signal.get("reason_zh") or signal.get("reason_en") or ""
        hold_action = _is_hold_action(action_zh, action_en)
        signal_rows.append(
            "<tr>"
            f"<td>{esc(_format_asia_shanghai(signal.get('timestamp')))}</td>"
            f"<td>{esc(signal.get('model_id'))}</td>"
            f"<td>{esc(signal.get('instrument_id'))}</td>"
            f"<td>{esc(action_zh)} / {esc(action_en)}</td>"
            f"<td>{_format_number(signal.get('confidence'), 2)}</td>"
            f"<td>{esc(reason)}</td>"
            f"<td>{_format_order(signal.get('order'), hold_action=hold_action)}</td>"
            "</tr>"
        )
    if not signal_rows:
        signal_rows.append("<tr><td colspan='7'>暂无 AI 信号</td></tr>")

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

        return "<p class='empty-state'>暂无可用深度数据</p>"

    return "\n".join(cards)



def _render_okx_dashboard(summary: dict) -> str:
    """Render the OKX paper trading overview page."""
    esc = _escape
    as_of = esc(_format_asia_shanghai(summary.get("as_of")))

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
        positions_html = _render_positions_table(bundle.get("positions", []))
        trades_html = _render_trades_table(bundle.get("recent_trades", []))
        orders_html = _render_orders_table(bundle.get("open_orders", []))
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
                <div class=\"panel\">
                    <h3>持仓明细</h3>
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
                    <div class=\"panel\">
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

    return OKX_TEMPLATE.format(
        as_of=as_of,
        error_block=error_block,
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
    cards = _render_orderbook_cards(snapshot.get("items") or [], levels)
    return ORDERBOOK_TEMPLATE.format(
        instrument_options=instrument_options,
        level_options=level_options,
        updated_at=esc(_format_asia_shanghai(snapshot.get("updated_at"))),
        cards=cards,
        levels=levels,
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


def _render_positions_table(positions: Sequence[dict] | None) -> str:
    esc = _escape
    rows = []
    for pos in positions or []:
        rows.append(
            "<tr>"
            f"<td>{esc(pos.get('position_id'))}</td>"
            f"<td>{esc(pos.get('instrument_id'))}</td>"
            f"<td>{esc(pos.get('side'))}</td>"
            f"<td>{_format_number(pos.get('quantity'))}</td>"
            f"<td>{_format_number(pos.get('entry_price'))}</td>"
            f"<td>{_format_number(pos.get('mark_price'))}</td>"
            f"<td>{_format_number(pos.get('unrealized_pnl'))}</td>"
            f"<td>{esc(_format_asia_shanghai(pos.get('updated_at')))}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='8'>暂无持仓</td></tr>")

    return (
        "<table class='dense'>"
        "<thead><tr><th>持仓ID</th><th>交易对</th><th>方向</th><th>持仓量</th><th>开仓均价</th><th>标记价格</th><th>未实现盈亏</th><th>更新时间</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _render_trades_table(trades: Sequence[dict] | None) -> str:
    esc = _escape
    rows = []
    for trade in trades or []:
        rows.append(
            "<tr>"
            f"<td>{esc(_format_asia_shanghai(trade.get('executed_at')))}</td>"
            f"<td>{esc(trade.get('model_id'))}</td>"
            f"<td>{esc(trade.get('account_id') or trade.get('portfolio_id'))}</td>"
            f"<td>{esc(trade.get('instrument_id'))}</td>"
            f"<td>{esc(trade.get('side'))}</td>"
            f"<td>{_format_number(trade.get('quantity'))}</td>"
            f"<td>{_format_number(trade.get('price'))}</td>"
            f"<td>{_format_number(trade.get('realized_pnl'))}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='9'>暂无成交记录</td></tr>")

    return (
        "<table class='dense'>"
        "<thead><tr><th>成交时间</th><th>模型</th><th>投资组合</th><th>合约</th><th>方向</th><th>数量</th><th>入场价</th><th>离场价</th><th>盈亏(USD)</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _render_orders_table(orders: Sequence[dict] | None) -> str:
    esc = _escape
    rows = []
    for order in orders or []:
        rows.append(
            "<tr>"
            f"<td>{esc(order.get('order_id'))}</td>"
            f"<td>{esc(order.get('instrument_id'))}</td>"
            f"<td>{esc(order.get('order_type'))}</td>"
            f"<td>{esc(order.get('side'))}</td>"
            f"<td>{_format_number(order.get('size'))}</td>"
            f"<td>{_format_number(order.get('filled_size'))}</td>"
            f"<td>{_format_number(order.get('price'))}</td>"
            f"<td>{_format_number(order.get('average_price'))}</td>"
            f"<td>{esc(order.get('state'))}</td>"
            f"<td>{esc(_format_asia_shanghai(order.get('updated_at')))}</td>"
            f"<td>{esc(_format_asia_shanghai(order.get('created_at')))}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='11'>暂无挂单</td></tr>")
    return (
        "<table class='dense'>"
        "<thead><tr><th>订单ID</th><th>交易对</th><th>委托类型</th><th>方向</th><th>委托数量</th><th>已成交数量</th><th>委托价格</th><th>成交均价</th><th>订单状态</th><th>更新时间</th><th>创建时间</th></tr></thead>"
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
        rows.append(
            "<tr>"
            f"<td>{esc(timestamp)}</td>"
            f"<td>{esc(item.get('instrument_id') or '--')}</td>"
            f"<td>{_format_number(item.get('long_qty'))}</td>"
            f"<td>{_format_number(item.get('short_qty'))}</td>"
            f"<td>{_format_number(item.get('net_qty'))}</td>"
            f"<td>{_format_number(item.get('last_price'))}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='6' class='empty-state'>暂无爆仓数据</td></tr>")
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
    <title>open-nof1.ai Dashboard</title>
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
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link active">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/settings" class="nav-link">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
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
    <title>open-nof1.ai 模型管理</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
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
        <a href="/settings" class="nav-link">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
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
    <title>open-nof1.ai 币对设置</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin-top: 1.8rem; margin-bottom: 0.5rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
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
        <a href="/settings" class="nav-link active">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
    </nav>

    <h1>币对与刷新频率</h1>
    <p class="updated">最近保存：<span class="timestamp">{updated_at}</span></p>
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
</body>
</html>
"""


OKX_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>open-nof1.ai OKX 模拟交易</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin-top: 2rem; margin-bottom: 0.5rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .error-card {{ background-color: #3f1d45; border-radius: 12px; padding: 16px 20px; margin-bottom: 20px; box-shadow: 0 6px 18px rgba(15, 23, 42, 0.45); }}
        .error-card h2 {{ margin: 0 0 10px 0; color: #f87171; }}
        .error-card ul {{ margin: 0; padding-left: 20px; }}
        .error-card li {{ margin: 6px 0; font-size: 0.9rem; }}
        .okx-card {{ background-color: #1e293b; border-radius: 12px; padding: 20px; margin-top: 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); }}
        .okx-card header {{ margin-bottom: 12px; }}
        .okx-card .meta {{ color: #94a3b8; font-size: 0.9rem; margin-top: 4px; }}
        .summary-table {{ margin-top: 12px; }}
        .split {{ display: flex; gap: 20px; flex-wrap: wrap; }}
        .panel {{ flex: 1 1 320px; }}
        table.dense {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; background-color: rgba(15, 23, 42, 0.6); border-radius: 6px; overflow: hidden; }}
        table.dense th, table.dense td {{ padding: 10px 12px; border-bottom: 1px solid #334155; text-align: left; font-size: 0.9rem; }}
        ul.curve-list {{ list-style: none; padding: 0; margin: 0.5rem 0 0 0; }}
        ul.curve-list li {{ padding: 6px 0; border-bottom: 1px dashed #334155; font-size: 0.9rem; }}
        .inline-row {{ display: inline-flex; align-items: center; gap: 10px; margin: 0; }}
        .btn-refresh {{ font-size: 12px; padding: 4px 8px; border-radius: 6px; border: 1px solid #38bdf8; color: #38bdf8; text-decoration: none; background: transparent; }}
        .btn-refresh:hover {{ background-color: rgba(56, 189, 248, 0.15); }}
        .local-time {{ color: #94a3b8; font-size: 0.9rem; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/orderbook" class="nav-link">市场深度</a>
        <a href="/settings" class="nav-link active">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
    </nav>

    <h1>OKX 模拟交易概览</h1>
    <p class="inline-row">数据时间：<span class="timestamp">{as_of}</span><span class="local-time">（本地时间：<span id="local-time">--:--:--</span>）</span><a href="/okx?refresh=1" class="btn-refresh" title="从交易所拉取最新并写入缓存">强制刷新</a></p>
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
    <title>open-nof1.ai 任务调度</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin-top: 1.5rem; margin-bottom: 0.6rem; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background-color: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background-color: #38bdf8; color: #0f172a; }}
        .card {{ background-color: #1e293b; border-radius: 12px; padding: 22px 26px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); max-width: 620px; }}
        form {{ display: grid; gap: 18px; }}
        label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        input[type="number"] {{ border-radius: 8px; border: 1px solid #334155; background-color: #0f172a; color: #e2e8f0; padding: 10px 12px; font-size: 0.95rem; }}
        input[type="number"]::placeholder {{ color: #64748b; }}
        button {{ background-color: #38bdf8; color: #0f172a; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-weight: bold; justify-self: flex-start; }}
        button:hover {{ background-color: #0ea5e9; }}
        .hint {{ color: #94a3b8; font-size: 0.85rem; margin-top: -6px; }}
        .meta {{ margin-top: 20px; font-size: 0.85rem; color: #64748b; }}
        .timestamp {{ color: #38bdf8; }}
        @media (max-width: 600px) {{
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
        <a href="/settings" class="nav-link">策略配置</a>
        <a href="/scheduler" class="nav-link active">调度器</a>
    </nav>

    <h1>任务调度控制</h1>
    <p class="hint">设置行情抽取与 AI 交互的周期，变更会实时更新后台 APScheduler 任务。</p>
    <section class="card">
        <form method="post" action="/scheduler/update">
            <label for="market-interval">
                行情抽取间隔（秒）
                <input id="market-interval" name="market_interval" type="number" min="30" max="3600" value="{market_interval}" required>
                <span class="hint">用于调用 OKX 行情 API、写入 Influx、生成最新指标。</span>
            </label>
            <label for="ai-interval">
                AI 交互间隔（秒）
                <input id="ai-interval" name="ai_interval" type="number" min="60" max="7200" value="{ai_interval}" required>
                <span class="hint">触发 LLM 信号 → 风控 → 模拟下单流程的周期。</span>
            </label>
            <button type="submit">保存调度设置</button>
        </form>
        <div class="meta">
            最近更新：<span class="timestamp">{updated_at}</span>
        </div>
    </section>
</body>
</html>
"""


def _render_scheduler_page(settings: dict) -> str:
    esc = _escape
    updated_at = _format_asia_shanghai(settings.get("updated_at"))
    return SCHEDULER_TEMPLATE.format(
        market_interval=esc(settings.get("market_interval")),
        ai_interval=esc(settings.get("ai_interval")),
        updated_at=esc(updated_at),
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
    <title>open-nof1.ai 爆仓监控</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background: #38bdf8; color: #0f172a; }}
        h1 {{ margin-bottom: 1rem; }}
        .controls {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: center; margin-bottom: 1rem; }}
        .controls label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        select {{ min-width: 200px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; padding: 8px 10px; }}
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
        <a href="/settings" class="nav-link">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
    </nav>
    <h1>OKX 爆仓监控</h1>
    <div class="controls">
        <label>选择合约
            <select id="instrument-select">
                {instrument_options}
            </select>
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
            </tr>
        </thead>
        <tbody id="liquidations-body">
            {rows}
        </tbody>
    </table>
    <script>
      (function() {{
        const select = document.getElementById('instrument-select');
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
        function refresh() {{
          const params = new URLSearchParams({{ limit: '30' }});
          if (select.value) {{ params.set('instrument', select.value); }}
          fetch('/api/streams/liquidations/latest?' + params.toString())
            .then((res) => res.json())
            .then((data) => {{
              const body = document.getElementById('liquidations-body');
              body.innerHTML = '';
              (data.items || []).forEach((item) => {{
                const tr = document.createElement('tr');
                tr.innerHTML = `
                  <td>${{fmtTimestamp(item.timestamp)}}</td>
                  <td>${{item.instrument_id || '--'}}</td>
                  <td>${{fmtNumber(item.long_qty, 2)}}</td>
                  <td>${{fmtNumber(item.short_qty, 2)}}</td>
                  <td>${{fmtNumber(item.net_qty, 2)}}</td>
                  <td>${{fmtNumber(item.last_price, 4)}}</td>`;
                body.appendChild(tr);
              }});
              if ((data.items || []).length === 0) {{
                body.innerHTML = '<tr><td colspan="6" class="empty-state">暂无爆仓数据</td></tr>';
              }}
              const updated = document.getElementById('liquidations-updated');
              if (updated) {{ updated.textContent = fmtTimestamp(data.updated_at); }}
            }})
            .catch((err) => console.error(err));
        }}
        select.addEventListener('change', refresh);
        refresh();
        setInterval(refresh, 5000);
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
    <title>open-nof1.ai 市场深度</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background: #38bdf8; color: #0f172a; }}
        h1 {{ margin-bottom: 1rem; }}
        .controls {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: center; margin-bottom: 1rem; }}
        .controls label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        select {{ min-width: 160px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; padding: 8px 10px; }}
        .book-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(520px, 1fr)); gap: 20px; }}
        .orderbook-chart {{ background: #0f1b2d; border-radius: 16px; padding: 18px 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); border: 1px solid rgba(148, 163, 184, 0.15); display: flex; flex-direction: column; gap: 14px; }}
        .orderbook-chart.tone-positive {{ border-color: rgba(74, 222, 128, 0.4); }}
        .orderbook-chart.tone-negative {{ border-color: rgba(248, 113, 113, 0.35); }}
        .orderbook-chart.tone-neutral {{ border-color: rgba(59, 130, 246, 0.25); }}
        .chart-header {{ display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; }}
        .chart-header h2 {{ margin: 0; font-size: 1.15rem; }}
        .chart-header small {{ display: block; font-size: 0.8rem; color: #94a3b8; margin-top: 4px; }}
        .chart-meta {{ display: flex; gap: 12px; flex-wrap: wrap; font-size: 0.9rem; color: #cbd5f5; }}
        .chart-stats {{ display: flex; gap: 16px; flex-wrap: wrap; font-size: 0.9rem; color: #cbd5f5; }}
        .chart-stats span {{ background: rgba(148, 163, 184, 0.1); padding: 4px 10px; border-radius: 999px; }}
        .chart-canvas {{ width: 100%; height: 220px; }}
        .chart-insight {{ margin: 0; font-size: 0.9rem; color: #e2e8f0; }}
        .chart-insight.positive {{ color: #4ade80; }}
        .chart-insight.negative {{ color: #f87171; }}
        .chart-insight.neutral {{ color: #94a3b8; }}
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
        <a href="/settings" class="nav-link">参数设置</a>
        <a href="/scheduler" class="nav-link">任务队列</a>
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
        <span>最后更新：<span class="timestamp" id="orderbook-updated">{updated_at}</span></span>
    <div id="orderbook-container" class="book-grid">
        {cards}
    </div>
    <script>
      (function() {{
        const instrumentSelect = document.getElementById('orderbook-instrument');
        const levelSelect = document.getElementById('depth-levels');

        const formatNumber = (value, digits = 2) => {{
          if (value === null || value === undefined || value === '') {{ return '--'; }}
          const num = Number(value);
          if (!Number.isFinite(num)) {{ return '--'; }}
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
        const buildSeries = (entries, depth) => {{
          const series = [];
          let cumulative = 0;
          for (let i = 0; i < depth; i += 1) {{
            const size = Number(entries[i]?.[1]) || 0;
            cumulative += size;
            series.push({{ level: i + 1, value: cumulative }});
          }}
          return series;
        }};
        const drawStrengthChart = (canvas, bids, asks) => {{
          const parent = canvas.parentElement;
          const width = Math.max((parent ? parent.clientWidth : 320), 280);
          const height = 220;
          const dpr = window.devicePixelRatio || 1;
          canvas.width = width * dpr;
          canvas.height = height * dpr;
          canvas.style.width = width + 'px';
          canvas.style.height = height + 'px';
          const ctx = canvas.getContext('2d');
          ctx.setTransform(1, 0, 0, 1, 0, 0);
          ctx.scale(dpr, dpr);
          ctx.clearRect(0, 0, width, height);
          const values = [...bids, ...asks].map((pt) => pt.value);
          const maxValue = Math.max(...values, 0);
          if (!maxValue) {{
            ctx.fillStyle = '#94a3b8';
            ctx.font = '14px Arial';
            ctx.fillText('深度不足', 20, height / 2);
            return;
          }}
          const left = 46;
          const right = 16;
          const top = 18;
          const bottom = 30;
          const chartWidth = width - left - right;
          const chartHeight = height - top - bottom;
          ctx.strokeStyle = 'rgba(148, 163, 184, 0.2)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(left, top);
          ctx.lineTo(left, top + chartHeight);
          ctx.lineTo(width - right, top + chartHeight);
          ctx.stroke();
          const drawSeries = (series, color) => {{
            if (!series.length) {{ return; }}
            ctx.strokeStyle = color;
            ctx.lineWidth = 2;
            ctx.beginPath();
            series.forEach((pt, idx) => {{
              const ratio = series.length > 1 ? idx / (series.length - 1) : 0;
              const x = left + chartWidth * ratio;
              const y = top + chartHeight - (pt.value / maxValue) * chartHeight;
              if (idx === 0) {{
                ctx.moveTo(x, y);
              }} else {{
                ctx.lineTo(x, y);
              }}
            }});
            ctx.stroke();
          }};
          drawSeries(bids, '#4ade80');
          drawSeries(asks, '#f87171');
          ctx.font = '12px Arial';
          ctx.fillStyle = '#4ade80';
          ctx.fillText('买盘', left, top - 4);
          ctx.fillStyle = '#f87171';
          ctx.fillText('卖盘', left + 48, top - 4);
          const tickCount = Math.min(5, bids.length);
          ctx.fillStyle = '#94a3b8';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'top';
          for (let i = 0; i < tickCount; i += 1) {{
            const idx = Math.round((bids.length - 1) * (i / (tickCount - 1 || 1)));
            const x = left + chartWidth * (idx / Math.max(1, bids.length - 1));
            ctx.fillText(`${{idx + 1}}档`, x, height - bottom + 6);
          }}
        }};

        function renderBooks(data) {{
          const container = document.getElementById('orderbook-container');
          container.innerHTML = '';
          const levels = parseInt(levelSelect.value || '{levels}', 10) || {levels};
          (data.items || []).forEach((item) => {{
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
            const analysis = `买盘占比 ${{Math.round(bidShare * 100)}}%，${{guidance}}（点差 ${{spreadText}}）`;
            const card = document.createElement('section');
            card.className = `orderbook-chart tone-${{tone}}`;
            card.innerHTML = `
              <div class="chart-header">
                <div>
                  <h2>${{item.instrument_id || '--'}}</h2>
                  <small>更新时间：${{formatTimestamp(item.timestamp)}}</small>
                </div>
                <div class="chart-meta">
                  <span>买一 ${{bestBidText}}</span>
                  <span>卖一 ${{bestAskText}}</span>
                  <span>点差 ${{spreadText}}</span>
                  <span>中间价 ${{midText}}</span>
                </div>
              </div>
              <div class="chart-stats">
                <span>买盘深度（前${{levels}}档）${{buyDepthText}}</span>
                <span>卖盘深度（前${{levels}}档）${{sellDepthText}}</span>
                <span>总深度 ${{totalDepthText}}</span>
                <span>净深度 ${{totalDepth > 0 ? formatNumber(buyDepth - sellDepth, 2) : '--'}}</span>
              </div>
              <canvas class="chart-canvas"></canvas>
              <p class="chart-insight ${{tone}}">${{analysis}}</p>
            `;
            const canvas = card.querySelector('canvas');
            const buySeries = buildSeries(bids, levels);
            const sellSeries = buildSeries(asks, levels);
            canvas.dataset.buy = JSON.stringify(buySeries);
            canvas.dataset.sell = JSON.stringify(sellSeries);
            drawStrengthChart(canvas, buySeries, sellSeries);
            container.appendChild(card);
          }});
          if ((data.items || []).length === 0) {{
            container.innerHTML = '<p class="empty-state">暂无深度数据</p>';
          }}
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
        window.addEventListener('resize', () => {{
          document.querySelectorAll('.chart-canvas').forEach((canvas) => {{
            try {{
              const history = JSON.parse(canvas.dataset.history || '[]');
              drawNetDepthChart(canvas, history);
            }} catch (err) {{
              console.error(err);
            }}
          }});
        }});
        refresh();
        setInterval(refresh, 5000);
      }})();
    </script>
</body>
</html>
"""




