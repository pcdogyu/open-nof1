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
    levels_param = request.query_params.get("levels", "10")
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
        model_rows.append("<tr><td colspan='8'>鏆傛棤妯″瀷鏁版嵁</td></tr>")

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
        trade_rows.append("<tr><td colspan='9'>鏆傛棤鎴愪氦璁板綍</td></tr>")
    signal_rows: list[str] = []
    for signal in metrics.get("recent_ai_signals", []):
        action_zh = signal.get("action_zh") or "鏈煡"
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
        signal_rows.append("<tr><td colspan='7'>鏆傛棤 AI 淇″彿</td></tr>")

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
        provider = esc(entry.get("provider", "鏈煡鏉ユ簮"))
        last_updated = esc(_format_asia_shanghai(entry.get("last_updated")))
        raw_api_key = entry.get("api_key") or ""
        masked_value = "*" * len(raw_api_key) if raw_api_key else ""
        masked_value_html = esc(masked_value)
        placeholder_text = "璇疯緭鍏ユā鍨?API Key" if not raw_api_key else "宸查厤缃紝淇敼璇烽噸鏂拌緭鍏?
        placeholder_html = esc(placeholder_text)
        enabled = bool(entry.get("enabled"))
        checked_attr = "checked" if enabled else ""

        cards.append(
            """
            <section class=\"model-card\">
                <header>
                    <h2>{display_name}</h2>
                    <p class=\"model-meta\">妯″瀷ID锛歿model_id} 路 鎻愪緵鏂癸細{provider}</p>
                </header>
                <form method=\"post\" action=\"/models/update\">
                    <input type=\"hidden\" name=\"model_id\" value=\"{model_id}\">
                    <input type=\"hidden\" name=\"has_existing_key\" value=\"{has_key}\">
                    <input type=\"hidden\" name=\"mask_length\" value=\"{mask_length}\">
                    <div class=\"form-row toggle-row\">
                        <label class=\"toggle\">
                            <input type=\"checkbox\" name=\"enabled\" value=\"on\" {checked}>
                            <span>鍚敤妯″瀷</span>
                        </label>
                    </div>
                    <div class=\"form-row\">
                        <label class=\"field-label\" for=\"api-{model_id}\">API Key</label>
                        <input id=\"api-{model_id}\" type=\"password\" name=\"api_key\" value=\"{masked_value}\" placeholder=\"{placeholder}\" autocomplete=\"new-password\" oncopy=\"return false;\" oncut=\"return false;\" ondragstart=\"return false;">
                    </div>
                    <div class=\"form-actions\">
                        <button type=\"submit\">淇濆瓨璁剧疆</button>
                    </div>
                </form>
                <p class=\"last-updated\">鏈€杩戞洿鏂帮細{last_updated}</p>
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

        return "<p class='empty-state'>鏆傛棤鍙敤娣卞害鏁版嵁</p>"

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
                f"璐︽埛锛歿esc(item.get('account_id')) or '-'} 路 閿欒锛歿esc(item.get('message'))}"
                "</li>"
            )
        error_block = (
            "<section class='error-card'>"
            "<h2>瀹炴椂鍚屾鍛婅</h2>"
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
                    <h2>{account_id or '鏈懡鍚嶈处鎴?}</h2>
                    <table class='dense summary-table'>
                        <tr>
                            <th>妯″瀷</th>
                            <th>鍒濆璧勯噾</th>
                            <th>褰撳墠鏉冪泭</th>
                            <th>鐜伴噾浣欓</th>
                            <th>绱鐩堜簭</th>
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
                    <h3>鎸佷粨鏄庣粏</h3>
                    {positions_html}
                </div>
                <div class=\"panel\">
                    <h3>鎸傚崟鍒楄〃</h3>
                    {orders_html}
                </div>
                <div class=\"split\">
                    <div class=\"panel\">
                        <h3>浣欓淇℃伅</h3>
                        {balances_html}
                    </div>
                    <div class=\"panel\">
                        <h3>杩戞湡鎴愪氦</h3>
                        {trades_html}
                    </div>
                </div>
                <div class=\"panel\">
                    <h3>璧勪骇鏇茬嚎</h3>
                    {curve_html}
                </div>
            </section>
            """
        )

    if not account_sections:
        account_sections.append("<p class='empty-state'>鏆傛棤 OKX 妯℃嫙璐︽埛鏁版嵁銆?/p>")

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
    chips_html = "".join(chips) if chips else "<span class='empty-state'>褰撳墠鏈厤缃彲浜ゆ槗甯佸</span>"

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
        catalog_hint = f"宸茬紦瀛?{catalog_count} 涓?OKX 浜ゆ槗瀵逛俊鎭?
    else:
        catalog_hint = "鏈彂鐜板凡缂撳瓨鐨勫竵瀵癸紝璇风偣鍑籠"鍒锋柊甯佸搴揬" 鎸夐挳鑾峰彇"

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
        rows.append("<tr><td colspan='5'>鏆傛棤浣欓淇℃伅</td></tr>")
    return (
        "<table class='dense'>"
        "<thead><tr><th>甯佺</th><th>鎬绘潈鐩?/th><th>鎬婚噾棰?/th><th>鍙敤浣欓</th><th>鍐荤粨閲戦</th></tr></thead>"
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
        rows.append("<tr><td colspan='8'>鏆傛棤鎸佷粨</td></tr>")

    return (
        "<table class='dense'>"
        "<thead><tr><th>鎸佷粨ID</th><th>浜ゆ槗瀵?/th><th>鏂瑰悜</th><th>鎸佷粨閲?/th><th>寮€浠撳潎浠?/th><th>鏍囪浠锋牸</th><th>鏈疄鐜扮泩浜?/th><th>鏇存柊鏃堕棿</th></tr></thead>"
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
        rows.append("<tr><td colspan='9'>鏆傛棤鎴愪氦璁板綍</td></tr>")

    return (
        "<table class='dense'>"
        "<thead><tr><th>鎴愪氦鏃堕棿</th><th>妯″瀷</th><th>鎶曡祫缁勫悎</th><th>鍚堢害</th><th>鏂瑰悜</th><th>鏁伴噺</th><th>鍏ュ満浠?/th><th>绂诲満浠?/th><th>鐩堜簭(USD)</th></tr></thead>"
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
        rows.append("<tr><td colspan='11'>鏆傛棤鎸傚崟</td></tr>")
    return (
        "<table class='dense'>"
        "<thead><tr><th>璁㈠崟ID</th><th>浜ゆ槗瀵?/th><th>濮旀墭绫诲瀷</th><th>鏂瑰悜</th><th>濮旀墭鏁伴噺</th><th>宸叉垚浜ゆ暟閲?/th><th>濮旀墭浠锋牸</th><th>鎴愪氦鍧囦环</th><th>璁㈠崟鐘舵€?/th><th>鏇存柊鏃堕棿</th><th>鍒涘缓鏃堕棿</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _render_equity_curve(points: Sequence[dict] | None) -> str:
    esc = _escape
    items = []
    for point in points or []:
        items.append(
            f"<li><span class='timestamp'>{esc(_format_asia_shanghai(point.get('timestamp')))}</span> 路 {esc(point.get('equity'))} USD</li>"
        )
    if not items:
        items.append("<li>鏆傛棤璧勪骇鏇茬嚎鏁版嵁</li>")
    return "<ul class='curve-list'>{}</ul>".format("\n".join(items))


def _build_instrument_options(instruments: Sequence[str], selected: Optional[str]) -> str:
    esc = _escape
    selected_norm = (selected or "").strip().upper()
    options = ['<option value="">鍏ㄩ儴</option>']
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
        rendered.append(f'<option value="{value}"{selected_attr}>{value} 妗?/option>')
    if sanitized not in choices:
        rendered.append(f'<option value="{sanitized}" selected>{sanitized} 妗?/option>')
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
        rows.append("<tr><td colspan='6' class='empty-state'>鏆傛棤鐖嗕粨鏁版嵁</td></tr>")
    return "\n".join(rows)


def _render_orderbook_cards(items: Sequence[dict], levels: int) -> str:
    esc = _escape
    level_count = max(1, levels)
    cards: list[str] = []

    def _as_float(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _depth_sum(levels_data: Sequence[Sequence[object]]) -> float:
        total = 0.0
        for entry in levels_data[:level_count]:
            try:
                size = float(entry[1])
            except (TypeError, ValueError, IndexError):
                continue
            total += size
        return total

    def _depth_insight(imbalance: float) -> tuple[str, str]:
        if imbalance >= 0.2:
            return "positive", "涔扮洏娣卞害鏄庢樉鍗犱紭锛岀煭绾垮姩鑳藉亸澶氥€?
        if imbalance <= -0.2:
            return "negative", "鍗栫洏鍘嬪埗澧炲己锛岃皑闃蹭环鏍煎洖钀姐€?
        return "neutral", "涔板崠鍔涢噺鎺ヨ繎骞宠　锛屽叧娉ㄦ垚浜ょ獊鐮翠俊鍙枫€?

    for item in items:
        instrument = esc(item.get("instrument_id") or "--")
        timestamp = esc(_format_asia_shanghai(item.get("timestamp")))
        best_bid = _format_number(item.get("best_bid"), digits=4)
        best_ask = _format_number(item.get("best_ask"), digits=4)
        spread = _format_number(item.get("spread"), digits=4)
        bids = item.get("bids") or []
        asks = item.get("asks") or []

        bid_depth = _depth_sum(bids)
        ask_depth = _depth_sum(asks)
        total_depth = bid_depth + ask_depth
        bid_share = bid_depth / total_depth if total_depth > 0 else 0.5
        ask_share = 1.0 - bid_share if total_depth > 0 else 0.5
        imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0.0
        net_depth = bid_depth - ask_depth

        bid_depth_fmt = _format_number(bid_depth, digits=2)
        ask_depth_fmt = _format_number(ask_depth, digits=2)
        total_depth_fmt = _format_number(total_depth, digits=2)
        net_depth_fmt = _format_number(net_depth, digits=2)
        imbalance_pct = f"{imbalance * 100:.1f}%" if total_depth > 0 else "--"
        bid_share_pct = f"{bid_share * 100:.0f}%"
        ask_share_pct = f"{ask_share * 100:.0f}%"

        best_bid_val = _as_float(item.get("best_bid"))
        best_ask_val = _as_float(item.get("best_ask"))
        mid_price_val = None
        if best_bid_val is not None and best_ask_val is not None:
            mid_price_val = (best_bid_val + best_ask_val) / 2
        mid_price = _format_number(mid_price_val, digits=4)

        tone, insight = _depth_insight(imbalance)
        analysis_text = f"涔扮洏鍗犳瘮 {bid_share_pct}锛寋insight}锛堢偣宸?{spread or '--'}锛?
        analysis = esc(analysis_text)

        cards.append(
            f"""
            <section class="orderbook-card tone-{tone}">
                <header>
                    <div class="title-row">
                        <h2>{instrument}</h2>
                        <span class="timestamp">鏇存柊鏃堕棿锛歿timestamp}</span>
                    </div>
                    <div class="quote-meta">
                        <span>涔颁竴 {best_bid}</span>
                        <span>鍗栦竴 {best_ask}</span>
                        <span>鐐瑰樊 {spread}</span>
                        <span>涓棿浠?{mid_price}</span>
                    </div>
                </header>
                <div class="metric-grid">
                    <div>
                        <span class="label">涔扮洏娣卞害锛堝墠{level_count}妗ｏ級</span>
                        <span class="value accent">{bid_depth_fmt}</span>
                    </div>
                    <div>
                        <span class="label">鍗栫洏娣卞害锛堝墠{level_count}妗ｏ級</span>
                        <span class="value">{ask_depth_fmt}</span>
                    </div>
                    <div>
                        <span class="label">鍑€娣卞害</span>
                        <span class="value">{net_depth_fmt}</span>
                    </div>
                    <div>
                        <span class="label">娣卞害鍗犳瘮</span>
                        <span class="value">{imbalance_pct}</span>
                    </div>
                    <div>
                        <span class="label">鎬绘繁搴?/span>
                        <span class="value">{total_depth_fmt}</span>
                    </div>
                </div>
                <div class="depth-bars">
                    <div class="bar bid" style="width: {bid_share * 100:.1f}%;">
                        涔版柟 {bid_share_pct}
                    </div>
                    <div class="bar ask" style="width: {ask_share * 100:.1f}%;">
                        鍗栨柟 {ask_share_pct}
                    </div>
                </div>
                <p class="insight-text {tone}">{analysis}</p>
            </section>
            """
        )

    if not cards:
        return "<p class='empty-state'>鏆傛棤鍙敤娣卞害鏁版嵁</p>"

    return "\n".join(cards)

    return "\n".join(cards)

    return "\n".join(cards)

    return "\n".join(cards)

    return "\n".join(cards)

    return "\n".join(cards)



def _format_order(order: dict | None, *, hold_action: bool = False) -> str:
    if hold_action:
        return "鏃?
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
    return any(keyword in zh for keyword in ("瑙傛湜", "淇濇寔", "绛夊緟")) or en in {"hold", "wait", "neutral"}


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
        <a href="/" class="nav-link active">棣栭〉</a>
        <a href="/models" class="nav-link">妯″瀷绠＄悊</a>
        <a href="/okx" class="nav-link">OKX 妯℃嫙</a>
        <a href="/liquidations" class="nav-link">鐖嗕粨鐩戞帶</a>
        <a href="/orderbook" class="nav-link">甯傚満娣卞害</a>
        <a href="/settings" class="nav-link">绛栫暐閰嶇疆</a>
        <a href="/scheduler" class="nav-link">璋冨害鍣?/a>
    </nav>


    <h1>妯″瀷琛ㄧ幇姒傝</h1>
    <div>鏁版嵁鏃堕棿锛?span class="timestamp">{as_of}</span></div>
    <table>
        <thead>
            <tr>
                <th>妯″瀷</th>
                <th>鎶曡祫缁勫悎</th>
                <th>澶忔櫘姣旂巼</th>
                <th>鏈€澶у洖鎾?%)</th>
                <th>鑳滅巼(%)</th>
                <th>骞冲潎鎸佷粨鏃堕暱(鍒嗛挓)</th>
                <th>浠撲綅鏁炲彛(USD)</th>
                <th>鎸佷粨鏁伴噺</th>
            </tr>
        </thead>
        <tbody>
            {model_rows}
        </tbody>
    </table>
    <h2>杩戞湡鎴愪氦</h2>
    <table>
        <thead>
            <tr>
                <th>鎴愪氦鏃堕棿</th>
                <th>妯″瀷</th>
                <th>鎶曡祫缁勫悎</th>
                <th>鍚堢害</th>
                <th>鏂瑰悜</th>
                <th>鏁伴噺</th>
                <th>鍏ュ満浠?/th>
                <th>绂诲満浠?/th>
                <th>鐩堜簭(USD)</th>
            </tr>
        </thead>
        <tbody>
            {trade_rows}
        </tbody>
    </table>
    <h2>鏈€鏂?AI 淇″彿</h2>
    <table>
        <thead>
            <tr>
                <th>鏃堕棿</th>
                <th>妯″瀷</th>
                <th>鍚堢害</th>
                <th>鍔ㄤ綔</th>
                <th>缃俊搴?/th>
                <th>鐞嗙敱</th>
                <th>寤鸿鎿嶄綔</th>
            </tr>
        </thead>
        <tbody>
            {signal_rows}
        </tbody>
    </table>
    <footer>
        鏁版嵁鏉ユ簮锛?a href="/metrics/models">/metrics/models</a> 锝?鍋ュ悍妫€鏌ワ細<a href="/health">/health</a>
    </footer>
</body>
</html>
"""


MODEL_MANAGER_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>open-nof1.ai 妯″瀷绠＄悊</title>
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
        <a href="/" class="nav-link">棣栭〉</a>
        <a href="/models" class="nav-link active">妯″瀷绠＄悊</a>
        <a href="/okx" class="nav-link">OKX 妯℃嫙</a>
        <a href="/liquidations" class="nav-link">鐖嗕粨鐩戞帶</a>
        <a href="/orderbook" class="nav-link">甯傚満娣卞害</a>
        <a href="/settings" class="nav-link">绛栫暐閰嶇疆</a>
        <a href="/scheduler" class="nav-link">璋冨害鍣?/a>
    </nav>

    <h1>妯″瀷绠＄悊</h1>
    <p>鍦ㄦ鍚敤鎴栧仠鐢ㄤ笉鍚岀殑澶фā鍨嬶紝骞堕厤缃悇鑷殑 API Key銆?/p>
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
    <title>open-nof1.ai 甯佸璁剧疆</title>
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
        <a href="/" class="nav-link">棣栭〉</a>
        <a href="/models" class="nav-link">妯″瀷绠＄悊</a>
        <a href="/okx" class="nav-link active">OKX 妯℃嫙</a>
        <a href="/liquidations" class="nav-link">鐖嗕粨鐩戞帶</a>
        <a href="/orderbook" class="nav-link">甯傚満娣卞害</a>
        <a href="/settings" class="nav-link">绛栫暐閰嶇疆</a>
        <a href="/scheduler" class="nav-link">璋冨害鍣?/a>
    </nav>

    <h1>甯佸涓庡埛鏂伴鐜?/h1>
    <p class="updated">鏈€杩戜繚瀛橈細<span class="timestamp">{updated_at}</span></p>
    <section class="settings-card">
        <h2>杩愯鍙傛暟</h2>
        <form method="post" action="/settings/update">
            <label for="poll-interval">鍒锋柊闂撮殧锛堢锛?/label>
            <input id="poll-interval" type="number" name="poll_interval" min="30" max="3600" value="{poll_interval}" required>
            <label for="instrument-list">褰撳墠鍚堢害鍒楄〃锛堟瘡琛屼竴涓級</label>
            <textarea id="instrument-list" name="instruments" spellcheck="false">{instruments_text}</textarea>
            <div class="chip-row">{chips_html}</div>
            <button type="submit">淇濆瓨閰嶇疆</button>
        </form>
    </section>
    <section class="settings-card">
        <h2>娣诲姞鍚堢害</h2>
        <p class="hint">{catalog_hint}</p>
        <form method="post" action="/settings/add" class="control-row" id="add-instrument-form">
            <input type="hidden" name="poll_interval" value="{poll_interval}">
            <input type="hidden" name="current_instruments" value="{current_instruments_value}">
            <label for="instrument-select-usdt" class="control-label">USDT</label>
            <div class="control-field">
                <select id="instrument-select-usdt" name="new_instrument" size="12">
                    <option value="">璇烽€夋嫨鍚堢害...</option>
{datalist_options_usdt}
                </select>
            </div>
            <button type="submit">娣诲姞鍒板垪琛?/button>
        </form>
        <form method="post" action="/settings/refresh-catalog" class="inline-form">
            <button type="submit" class="secondary">鍒锋柊鍚堢害搴?/button>
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
    <title>open-nof1.ai OKX 妯℃嫙浜ゆ槗</title>
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
        <a href="/" class="nav-link">棣栭〉</a>
        <a href="/models" class="nav-link">妯″瀷绠＄悊</a>
        <a href="/okx" class="nav-link">OKX 妯℃嫙</a>
        <a href="/liquidations" class="nav-link">鐖嗕粨鐩戞帶</a>
        <a href="/orderbook" class="nav-link">甯傚満娣卞害</a>
        <a href="/settings" class="nav-link active">绛栫暐閰嶇疆</a>
        <a href="/scheduler" class="nav-link">璋冨害鍣?/a>
    </nav>

    <h1>OKX 妯℃嫙浜ゆ槗姒傝</h1>
    <p class="inline-row">鏁版嵁鏃堕棿锛?span class="timestamp">{as_of}</span><span class="local-time">锛堟湰鍦版椂闂达細<span id="local-time">--:--:--</span>锛?/span><a href="/okx?refresh=1" class="btn-refresh" title="浠庝氦鏄撴墍鎷夊彇鏈€鏂板苟鍐欏叆缂撳瓨">寮哄埗鍒锋柊</a></p>
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
    <title>open-nof1.ai 浠诲姟璋冨害</title>
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
        <a href="/" class="nav-link">棣栭〉</a>
        <a href="/models" class="nav-link">妯″瀷绠＄悊</a>
        <a href="/okx" class="nav-link">OKX 妯℃嫙</a>
        <a href="/liquidations" class="nav-link">鐖嗕粨鐩戞帶</a>
        <a href="/orderbook" class="nav-link">甯傚満娣卞害</a>
        <a href="/settings" class="nav-link">绛栫暐閰嶇疆</a>
        <a href="/scheduler" class="nav-link active">璋冨害鍣?/a>
    </nav>

    <h1>浠诲姟璋冨害鎺у埗</h1>
    <p class="hint">璁剧疆琛屾儏鎶藉彇涓?AI 浜や簰鐨勫懆鏈燂紝鍙樻洿浼氬疄鏃舵洿鏂板悗鍙?APScheduler 浠诲姟銆?/p>
    <section class="card">
        <form method="post" action="/scheduler/update">
            <label for="market-interval">
                琛屾儏鎶藉彇闂撮殧锛堢锛?                <input id="market-interval" name="market_interval" type="number" min="30" max="3600" value="{market_interval}" required>
                <span class="hint">鐢ㄤ簬璋冪敤 OKX 琛屾儏 API銆佸啓鍏?Influx銆佺敓鎴愭渶鏂版寚鏍囥€?/span>
            </label>
            <label for="ai-interval">
                AI 浜や簰闂撮殧锛堢锛?                <input id="ai-interval" name="ai_interval" type="number" min="60" max="7200" value="{ai_interval}" required>
                <span class="hint">瑙﹀彂 LLM 淇″彿 鈫?椋庢帶 鈫?妯℃嫙涓嬪崟娴佺▼鐨勫懆鏈熴€?/span>
            </label>
            <button type="submit">淇濆瓨璋冨害璁剧疆</button>
        </form>
        <div class="meta">
            鏈€杩戞洿鏂帮細<span class="timestamp">{updated_at}</span>
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
    <title>open-nof1.ai 鐖嗕粨鐩戞帶</title>
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
        <a href="/" class="nav-link">棣栭〉</a>
        <a href="/models" class="nav-link">妯″瀷绠＄悊</a>
        <a href="/okx" class="nav-link">OKX 妯℃嫙</a>
        <a href="/liquidations" class="nav-link active">鐖嗕粨鐩戞帶</a>
        <a href="/orderbook" class="nav-link">甯傚満娣卞害</a>
        <a href="/settings" class="nav-link">绛栫暐閰嶇疆</a>
        <a href="/scheduler" class="nav-link">璋冨害鍣?/a>
    </nav>
    <h1>OKX 鐖嗕粨鐩戞帶</h1>
    <div class="controls">
        <label>閫夋嫨鍚堢害
            <select id="instrument-select">
                {instrument_options}
            </select>
        </label>
        <span>鏈€鍚庢洿鏂帮細<span class="timestamp" id="liquidations-updated">{updated_at}</span></span>
    </div>
    <table>
        <thead>
            <tr>
                <th>鏃堕棿</th>
                <th>鍚堢害</th>
                <th>澶氬崟鐖嗕粨</th>
                <th>绌哄崟鐖嗕粨</th>
                <th>鍑€鐖嗕粨</th>
                <th>鏈€鏂颁环</th>
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
                body.innerHTML = '<tr><td colspan="6" class="empty-state">鏆傛棤鐖嗕粨鏁版嵁</td></tr>';
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
    <title>open-nof1.ai 甯傚満娣卞害</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background: #38bdf8; color: #0f172a; }}
        h1 {{ margin-bottom: 1rem; }}
        .controls {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: center; margin-bottom: 1rem; }}
        .controls label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        select {{ min-width: 160px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; padding: 8px 10px; }}
        .book-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 20px; }}
        .orderbook-card {{ background: #1e293b; border-radius: 14px; padding: 18px 20px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45); display: flex; flex-direction: column; gap: 14px; border: 1px solid rgba(148, 163, 184, 0.15); }}
        .orderbook-card.tone-positive {{ border-color: rgba(74, 222, 128, 0.4); }}
        .orderbook-card.tone-negative {{ border-color: rgba(248, 113, 113, 0.35); }}
        .orderbook-card.tone-neutral {{ border-color: rgba(59, 130, 246, 0.25); }}
        .title-row {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }}
        .title-row h2 {{ margin: 0; font-size: 1.15rem; }}
        .title-row .timestamp {{ font-size: 0.85rem; color: #94a3b8; }}
        .quote-meta {{ display: flex; flex-wrap: wrap; gap: 12px; font-size: 0.9rem; color: #cbd5f5; }}
        .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }}
        .metric-grid .label {{ font-size: 0.8rem; color: #94a3b8; }}
        .metric-grid .value {{ font-size: 1.2rem; font-weight: 600; }}
        .metric-grid .value.accent {{ color: #34d399; }}
        .depth-bars {{ display: flex; width: 100%; height: 34px; border-radius: 20px; overflow: hidden; border: 1px solid #1f2937; background: rgba(15, 23, 42, 0.8); }}
        .depth-bars .bar {{ display: flex; align-items: center; justify-content: center; font-weight: 600; font-size: 0.85rem; color: #0f172a; }}
        .depth-bars .bid {{ background: rgba(34, 197, 94, 0.85); }}
        .depth-bars .ask {{ background: rgba(239, 68, 68, 0.85); }}
        .insight-text {{ margin: 0; font-size: 0.9rem; color: #e2e8f0; }}
        .insight-text.positive {{ color: #4ade80; }}
        .insight-text.negative {{ color: #f87171; }}
        .insight-text.neutral {{ color: #94a3b8; }}
        .empty-state {{ color: #94a3b8; padding: 16px; text-align: center; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">棣栭〉</a>
        <a href="/models" class="nav-link">妯″瀷绠＄悊</a>
        <a href="/okx" class="nav-link">OKX 妯℃嫙</a>
        <a href="/liquidations" class="nav-link">鐖嗕粨鐩戞帶</a>
        <a href="/orderbook" class="nav-link active">甯傚満娣卞害</a>
        <a href="/settings" class="nav-link">鍙傛暟璁剧疆</a>
        <a href="/scheduler" class="nav-link">浠诲姟闃熷垪</a>
    </nav>
    <h1>OKX 甯傚満娣卞害</h1>
    <div class="controls">
        <label>閫夋嫨鍚堢害
            <select id="orderbook-instrument">
                {instrument_options}
            </select>
        </label>
        <label>鍒嗘瀽妗ｄ綅
            <select id="depth-levels">
                {level_options}
            </select>
        </label>
        <span>鏈€鍚庢洿鏂帮細<span class="timestamp" id="orderbook-updated">{updated_at}</span></span>
    </div>
    <section class="net-depth-panel">
        <div class="panel-header">
            <div>
                <h2>鍑€娣卞害鍒嗗竷</h2>
                <p class="panel-subtitle" id="net-depth-hint">鍩轰簬鍓峽levels}妗ｅ噣娣卞害</p>
            </div>
        </div>
        <canvas id="net-depth-chart"></canvas>
        <p id="net-depth-analysis" class="insight-summary">鏆傛棤鏁版嵁</p>
    </section>
    <div id="orderbook-container" class="book-grid">
        {cards}
    </div>
    <script>
      (function() {{
        const instrumentSelect = document.getElementById('orderbook-instrument');
        const levelSelect = document.getElementById('depth-levels');
        const netDepthCanvas = document.getElementById('net-depth-chart');
        const netDepthAnalysis = document.getElementById('net-depth-analysis');
        const netDepthHint = document.getElementById('net-depth-hint');
        let latestChartPoints = [];
        let latestChartLevels = {levels};

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
            return {{ tone: 'positive', text: '涔扮洏娣卞害鏄庢樉鍗犱紭锛岀煭绾垮姩鑳藉亸澶氥€? }};
          }}
          if (imbalance <= -0.2) {{
            return {{ tone: 'negative', text: '鍗栫洏鍘嬪埗澧炲己锛岃皑闃蹭环鏍煎洖钀姐€? }};
          }}
          return {{ tone: 'neutral', text: '涔板崠鍔涢噺鎺ヨ繎骞宠　锛屽叧娉ㄦ垚浜ょ獊鐮翠俊鍙枫€? }};
        }};

        function renderNetDepthOverview(points, depthLevels) {{
          if (!netDepthCanvas) {{ return; }}
          const parentWidth = netDepthCanvas.parentElement ? netDepthCanvas.parentElement.clientWidth - 16 : 900;
          const height = 260;
          const dpr = window.devicePixelRatio || 1;
          const width = Math.max(parentWidth, 320);
          netDepthCanvas.width = width * dpr;
          netDepthCanvas.height = height * dpr;
          netDepthCanvas.style.width = width + 'px';
          netDepthCanvas.style.height = height + 'px';
          const ctx = netDepthCanvas.getContext('2d');
          ctx.setTransform(1, 0, 0, 1, 0, 0);
          ctx.scale(dpr, dpr);
          ctx.clearRect(0, 0, width, height);
          if (netDepthHint) {{
            netDepthHint.textContent = `鍩轰簬鍓?{{depthLevels}}妗ｅ噣娣卞害`;
          }}
          if (!points.length) {{
            ctx.fillStyle = '#94a3b8';
            ctx.font = '14px Arial';
            ctx.fillText('鏆傛棤鍑€娣卞害鏁版嵁', 20, height / 2);
            if (netDepthAnalysis) {{
              netDepthAnalysis.textContent = '鏆傛棤鏁版嵁';
            }}
            return;
          }}
          const values = points.map((pt) => pt.value);
          const maxVal = Math.max(...values, 0);
          const minVal = Math.min(...values, 0);
          const range = maxVal - minVal || 1;
          const left = 50;
          const right = 20;
          const top = 20;
          const bottom = 40;
          const chartWidth = width - left - right;
          const chartHeight = height - top - bottom;
          const zeroY = top + chartHeight - ((0 - minVal) / range) * chartHeight;

          ctx.strokeStyle = '#475569';
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(left, zeroY);
          ctx.lineTo(width - right, zeroY);
          ctx.stroke();

          const step = points.length > 1 ? chartWidth / (points.length - 1) : 0;
          ctx.strokeStyle = '#38bdf8';
          ctx.lineWidth = 2;
          ctx.beginPath();
          points.forEach((pt, idx) => {{
            const x = left + step * idx;
            const y = top + (maxVal - pt.value) / range * chartHeight;
            if (idx === 0) {{
              ctx.moveTo(x, y);
            }} else {{
              ctx.lineTo(x, y);
            }}
          }});
          ctx.stroke();

          ctx.font = '11px Arial';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          points.forEach((pt, idx) => {{
            const x = left + step * idx;
            const y = top + (maxVal - pt.value) / range * chartHeight;
            ctx.fillStyle = pt.value >= 0 ? '#4ade80' : '#f87171';
            ctx.beginPath();
            ctx.arc(x, y, 4, 0, Math.PI * 2);
            ctx.fill();
            ctx.fillStyle = '#94a3b8';
            ctx.fillText(pt.label, x, height - 12);
          }});

          if (netDepthAnalysis) {{
            const strongestBid = [...points].filter((pt) => pt.value > 0).sort((a, b) => b.value - a.value)[0];
            const strongestAsk = [...points].filter((pt) => pt.value < 0).sort((a, b) => a.value - b.value)[0];
            const totalNet = values.reduce((sum, val) => sum + val, 0);
            let analysis;
            if (strongestBid && strongestAsk) {{
              analysis = `涔扮洏鏈€寮猴細${{strongestBid.label}}锛?{{formatNumber(strongestBid.value, 2)}}锛夛紝鍗栧帇鏈€澶э細${{strongestAsk.label}}锛?{{formatNumber(strongestAsk.value, 2)}}锛夛紝鍑€娣卞害鍚堣 ${{formatNumber(totalNet, 2)}}銆俙;
            }} else if (strongestBid) {{
              analysis = `鏁翠綋鍋忓锛?{{strongestBid.label}} 棰嗗厛锛?{{formatNumber(strongestBid.value, 2)}}锛夛紝鍑€娣卞害鍚堣 ${{formatNumber(totalNet, 2)}}銆俙;
            }} else if (strongestAsk) {{
              analysis = `鏁翠綋鍋忕┖锛?{{strongestAsk.label}} 鍗栧帇鏈€寮猴紙${{formatNumber(strongestAsk.value, 2)}}锛夛紝鍑€娣卞害鍚堣 ${{formatNumber(totalNet, 2)}}銆俙;
            }} else {{
              analysis = '鍑€娣卞害鎺ヨ繎闆讹紝涔板崠鍔涢噺鏆傛椂鍧囪　銆?;
            }}
            netDepthAnalysis.textContent = analysis;
          }}
        }}

        function renderBooks(data) {{
          const container = document.getElementById('orderbook-container');
          container.innerHTML = '';
          const levels = parseInt(levelSelect.value || '{levels}', 10) || {levels};
          const chartPoints = [];
          (data.items || []).forEach((item) => {{
            const bids = Array.isArray(item.bids) ? item.bids : [];
            const asks = Array.isArray(item.asks) ? item.asks : [];
            const bidDepth = sumDepth(bids, levels);
            const askDepth = sumDepth(asks, levels);
            const totalDepth = bidDepth + askDepth;
            const bidShare = totalDepth > 0 ? bidDepth / totalDepth : 0.5;
            const askShare = 1 - bidShare;
            const imbalance = totalDepth > 0 ? (bidDepth - askDepth) / totalDepth : 0;
            const netDepth = bidDepth - askDepth;
            chartPoints.push({ label: item.instrument_id || '--', value: netDepth });
            const {{ tone, text: guidance }} = describeDepth(imbalance);
            const bidSharePct = Math.round(bidShare * 100);
            const askSharePct = Math.round(askShare * 100);
            const imbalancePct = totalDepth > 0 ? (imbalance * 100).toFixed(1) + '%' : '--';
            const spreadText = formatNumber(item.spread, 4);
            const bestBidText = formatNumber(item.best_bid, 4);
            const bestAskText = formatNumber(item.best_ask, 4);
            const bidDepthText = formatNumber(bidDepth, 2);
            const askDepthText = formatNumber(askDepth, 2);
            const netDepthText = formatNumber(netDepth, 2);
            const totalDepthText = formatNumber(totalDepth, 2);
            const bestBidNum = Number(item.best_bid);
            const bestAskNum = Number(item.best_ask);
            let midText = '--';
            if (Number.isFinite(bestBidNum) && Number.isFinite(bestAskNum)) {{
              midText = formatNumber((bestBidNum + bestAskNum) / 2, 4);
            }}
            const analysis = `涔扮洏鍗犳瘮 ${{bidSharePct}}%锛?{{guidance}}锛堢偣宸?${{spreadText}}锛塦;

            const card = document.createElement('section');
            card.className = `orderbook-card tone-${{tone}}`;
            card.innerHTML = `
              <header>
                <div class="title-row">
                  <h2>${{item.instrument_id || '--'}}</h2>
                  <span class="timestamp">鏇存柊鏃堕棿锛?{{formatTimestamp(item.timestamp)}}</span>
                </div>
                <div class="quote-meta">
                  <span>涔颁竴 ${{bestBidText}}</span>
                  <span>鍗栦竴 ${{bestAskText}}</span>
                  <span>鐐瑰樊 ${{spreadText}}</span>
                  <span>涓棿浠?${{midText}}</span>
                </div>
              </header>
              <div class="metric-grid">
                <div>
                  <span class="label">涔扮洏娣卞害锛堝墠${{levels}}妗ｏ級</span>
                  <span class="value accent">${{bidDepthText}}</span>
                </div>
                <div>
                  <span class="label">鍗栫洏娣卞害锛堝墠${{levels}}妗ｏ級</span>
                  <span class="value">${{askDepthText}}</span>
                </div>
                <div>
                  <span class="label">鍑€娣卞害</span>
                  <span class="value">${{netDepthText}}</span>
                </div>
                <div>
                  <span class="label">娣卞害鍗犳瘮</span>
                  <span class="value">${{imbalancePct}}</span>
                </div>
                <div>
                  <span class="label">鎬绘繁搴?/span>
                  <span class="value">${{totalDepthText}}</span>
                </div>
              </div>
              <div class="depth-bars">
                <div class="bar bid" style="width: ${{(bidShare * 100).toFixed(1)}}%;">
                  涔版柟 ${{bidSharePct}}%
                </div>
                <div class="bar ask" style="width: ${{(askShare * 100).toFixed(1)}}%;">
                  鍗栨柟 ${{askSharePct}}%
                </div>
              </div>
              <p class="insight-text ${{tone}}">${{analysis}}</p>
            `;
            container.appendChild(card);
          }});
          latestChartPoints = chartPoints;
          latestChartLevels = levels;
          renderNetDepthOverview(chartPoints, levels);
          if ((data.items || []).length === 0) {{
            container.innerHTML = '<p class="empty-state">鏆傛棤娣卞害鏁版嵁</p>';
            renderNetDepthOverview([], levels);
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
        refresh();
        setInterval(refresh, 4000);
        window.addEventListener('resize', () => {{
          renderNetDepthOverview(latestChartPoints, latestChartLevels);
        }});
      }})();
    </script>
</body>
</html>
"""




