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
    snapshot = routes.get_liquidation_snapshot(limit=50, instrument=instrument)
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments", [])
    return HTMLResponse(content=_render_liquidation_page(snapshot, instruments))


@app.get("/liquidation-map", include_in_schema=False, response_class=HTMLResponse)
def liquidation_map_dashboard() -> HTMLResponse:
    attachments = routes.get_market_depth_attachments(limit=12)
    settings = routes.get_pipeline_settings()
    instruments = settings.get("tradable_instruments", [])
    return HTMLResponse(content=_render_liquidation_map_page(attachments, instruments))


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
        cards_html = "<p class='empty-state'>暂无可用深度数据</p>"
    else:
        cards_html = "\n".join(cards)

    return MODEL_MANAGER_TEMPLATE.format(models_html=cards_html)



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


def _render_liquidation_map_page(attachments: Sequence[dict], instruments: Sequence[str]) -> str:
    gallery = _render_market_depth_attachments(attachments)
    instrument_options = _build_instrument_options(instruments, None)
    level_options = _build_depth_options(50)
    return LIQUIDATION_MAP_TEMPLATE.format(
        attachment_gallery=gallery,
        instrument_options=instrument_options,
        level_options=level_options,
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
    choices = (20, 50, 100, 200, 400)
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


def _render_market_depth_attachments(attachments: Sequence[dict]) -> str:
    esc = _escape
    if not attachments:
        return "<p class='empty-state attachment-empty'>暂无市场深度截图，可将 PNG/JPG 文件放入 data/attachments/market_depth 目录。</p>"
    cards: list[str] = []
    for entry in attachments:
        data_url = entry.get("data_url") or ""
        filename = esc(entry.get("filename") or "--")
        updated = esc(_format_asia_shanghai(entry.get("updated_at")))
        if not data_url:
            continue
        cards.append(
            "<figure class='attachment-card'>"
            f"<img src=\"{data_url}\" alt=\"{filename}\" />"
            f"<figcaption><strong>{filename}</strong><span>{updated}</span></figcaption>"
            "</figure>"
        )
    return "<div class='attachment-gallery'>{}</div>".format("".join(cards))


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
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link active">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
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
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
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
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link active">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
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
        <a href="/okx" class="nav-link active">OKX 模拟</a>
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">策略配置</a>
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
        .ai-signals-card {{ margin-bottom: 2rem; }}
        .ai-signals-card table {{ margin-top: 0.5rem; }}
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
        .log-card {{ width: 100%; max-width: 620px; margin-top: 26px; }}
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
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
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
    <section class="card log-card">
        <h2>最近执行记录</h2>
        <table>
            <thead>
                <tr>
                    <th>时间</th>
                    <th>任务</th>
                    <th>状态</th>
                    <th>描述信息</th>
                </tr>
            </thead>
            <tbody>
                {log_rows}
            </tbody>
        </table>
    </section>
</body>
</html>
"""

PROMPT_EDITOR_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>open-nof1.ai 提示词模板</title>
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
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link active">提示词</a>
        <a href="/settings" class="nav-link">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
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
        log_rows.append(
            "<tr>"
            f"<td>{timestamp}</td>"
            f"<td>{job}</td>"
            f"<td><span class=\"status-pill {status_class}\">{status_text}</span></td>"
            f"<td>{detail}</td>"
            "</tr>"
        )
    if not log_rows:
        log_rows.append("<tr><td colspan='4' class='empty-state'>暂无执行记录</td></tr>")
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
        <a href="/liquidation-map" class="nav-link">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
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


LIQUIDATION_MAP_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>open-nof1.ai 清算地图</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        .top-nav {{ display: flex; gap: 16px; margin-bottom: 1.5rem; font-size: 0.95rem; }}
        .nav-link {{ padding: 6px 12px; border-radius: 6px; background: rgba(51, 65, 85, 0.6); color: #e2e8f0; text-decoration: none; }}
        .nav-link.active {{ background: #38bdf8; color: #0f172a; }}
        h1 {{ margin-bottom: 0.5rem; }}
        .intro {{ margin: 0 0 1rem; color: #94a3b8; line-height: 1.6; max-width: 960px; }}
        .controls {{ display: flex; gap: 16px; align-items: flex-end; flex-wrap: wrap; margin-bottom: 1rem; }}
        .controls label {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.9rem; color: #94a3b8; }}
        select {{ min-width: 160px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; padding: 8px 10px; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 1rem; }}
        .stat-card {{ background: rgba(15, 23, 42, 0.6); border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 12px; padding: 12px 16px; display: flex; flex-direction: column; gap: 4px; }}
        .stat-card span.label {{ color: #94a3b8; font-size: 0.85rem; }}
        .stat-card strong {{ font-size: 1.1rem; }}
        .chart-shell {{ background: #0b1526; border-radius: 16px; border: 1px solid rgba(148, 163, 184, 0.25); padding: 16px; margin-bottom: 1.5rem; }}
        .chart-shell header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
        .chart-shell canvas {{ width: 100%; height: 360px; background: rgba(15, 23, 42, 0.75); border-radius: 12px; }}
        .chart-shell small {{ color: #94a3b8; }}
        .attachment-section {{ background: rgba(15, 23, 42, 0.55); border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 18px; padding: 18px 20px; margin-bottom: 1.5rem; }}
        .attachment-section h2 {{ margin: 0 0 0.5rem; font-size: 1.15rem; }}
        .attachment-section p.helper {{ margin: 0 0 1rem; color: #94a3b8; font-size: 0.85rem; }}
        .attachment-gallery {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }}
        .attachment-card {{ margin: 0; background: #0b1526; border-radius: 12px; overflow: hidden; border: 1px solid rgba(148, 163, 184, 0.25); display: flex; flex-direction: column; }}
        .attachment-card img {{ width: 100%; height: auto; display: block; }}
        .attachment-card figcaption {{ padding: 10px 12px; font-size: 0.85rem; color: #e2e8f0; display: flex; flex-direction: column; gap: 4px; }}
        .attachment-card figcaption span {{ color: #94a3b8; font-size: 0.75rem; }}
        .attachment-empty {{ color: #94a3b8; padding: 20px; border: 1px dashed rgba(148, 163, 184, 0.4); border-radius: 12px; background: rgba(15, 23, 42, 0.4); }}
    </style>
</head>
<body>
    <nav class="top-nav">
        <a href="/" class="nav-link">首页</a>
        <a href="/models" class="nav-link">模型管理</a>
        <a href="/okx" class="nav-link">OKX 模拟</a>
        <a href="/liquidations" class="nav-link">爆仓监控</a>
        <a href="/liquidation-map" class="nav-link active">清算地图</a>
        <a href="/prompts" class="nav-link">提示词</a>
        <a href="/settings" class="nav-link">策略配置</a>
        <a href="/scheduler" class="nav-link">调度器</a>
    </nav>
    <h1>OKX 清算地图</h1>
    <p class="intro">
        实时订阅 OKX 市场深度，横轴为价格（由左至右递增），纵轴为数量（由下至上递增），以柱状图展示未成交买卖挂单堆积情况。
    </p>
    <div class="controls">
        <label>选择合约
            <select id="instrument-select">
                {instrument_options}
            </select>
        </label>
        <label>分析档位
            <select id="level-select">
                {level_options}
            </select>
        </label>
        <div>
            <span>最后更新：<strong id="map-updated">--</strong></span>
        </div>
    </div>
    <div class="stats">
        <div class="stat-card">
            <span class="label">买盘深度</span>
            <strong id="stat-bid-depth">--</strong>
        </div>
        <div class="stat-card">
            <span class="label">卖盘深度</span>
            <strong id="stat-ask-depth">--</strong>
        </div>
        <div class="stat-card">
            <span class="label">净深度</span>
            <strong id="stat-net-depth">--</strong>
        </div>
        <div class="stat-card">
            <span class="label">点差</span>
            <strong id="stat-spread">--</strong>
        </div>
    </div>
    <div class="chart-shell">
        <header>
            <strong id="chart-title">深度分布</strong>
            <small>绿色为买盘、红色为卖盘；柱越高表示该价位挂单越多。</small>
        </header>
        <canvas id="depth-canvas" width="1200" height="360"></canvas>
    </div>
    <section class="attachment-section">
        <h2>历史截图附件</h2>
        <p class="helper">可将交易界面截屏（PNG / JPG / WebP / GIF）放入 <code>data/attachments/market_depth</code> 目录以便回顾。</p>
        {attachment_gallery}
    </section>
    <script>
      (function() {{
        const instrumentSelect = document.getElementById('instrument-select');
        const levelSelect = document.getElementById('level-select');
        const updatedLabel = document.getElementById('map-updated');
        const bidDepthEl = document.getElementById('stat-bid-depth');
        const askDepthEl = document.getElementById('stat-ask-depth');
        const netDepthEl = document.getElementById('stat-net-depth');
        const spreadEl = document.getElementById('stat-spread');
        const titleEl = document.getElementById('chart-title');
        const canvas = document.getElementById('depth-canvas');
        const ctx = canvas.getContext('2d');

        const formatNumber = (value, digits = 2) => {{
          if (value === null || value === undefined || value === '') {{ return '--'; }}
          const num = Number(value);
          if (!Number.isFinite(num)) {{ return '--'; }}
          return num.toFixed(digits);
        }};

        const describeImbalance = (imbalance) => {{
          if (imbalance >= 0.2) {{ return '买盘主导，多头占优'; }}
          if (imbalance <= -0.2) {{ return '卖盘主导，空头占优'; }}
          return '买卖相对均衡';
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

        const drawBars = (entries, color, minPrice, range, maxSize, padding, width, height) => {{
          if (!Array.isArray(entries) || entries.length === 0) {{ return; }}
          ctx.fillStyle = color;
          entries.forEach(([price, size]) => {{
            if (!Number.isFinite(price) || !Number.isFinite(size)) {{ return; }}
            const x = padding + ((price - minPrice) / range) * width;
            const barWidth = Math.max(width / (entries.length * 1.6), 2);
            const barHeight = (size / maxSize) * height;
            ctx.fillRect(x - barWidth / 2, padding + height - barHeight, barWidth, barHeight);
          }});
        }};

        const renderDepth = (data) => {{
          const items = Array.isArray(data.items) ? data.items : [];
          const target = items[0];
          if (!target) {{
            bidDepthEl.textContent = askDepthEl.textContent = netDepthEl.textContent = spreadEl.textContent = '--';
            updatedLabel.textContent = '--';
            titleEl.textContent = '深度分布';
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.fillStyle = '#94a3b8';
            ctx.font = '16px Arial';
            ctx.fillText('暂无深度数据', 40, canvas.height / 2);
            return;
          }}
          const bids = Array.isArray(target.bids) ? target.bids.map(([p, q]) => [Number(p), Number(q)]) : [];
          const asks = Array.isArray(target.asks) ? target.asks.map(([p, q]) => [Number(p), Number(q)]) : [];
          const levels = Number(levelSelect.value) || 50;
          const limitedBids = bids.slice(0, levels).filter(([, q]) => Number.isFinite(q));
          const limitedAsks = asks.slice(0, levels).filter(([, q]) => Number.isFinite(q));
          const buyDepth = limitedBids.reduce((sum, [, q]) => sum + (Number.isFinite(q) ? q : 0), 0);
          const sellDepth = limitedAsks.reduce((sum, [, q]) => sum + (Number.isFinite(q) ? q : 0), 0);
          const totalDepth = buyDepth + sellDepth;
          const imbalance = totalDepth > 0 ? (buyDepth - sellDepth) / totalDepth : 0;
          const spread = Number(target.spread);
          const prices = [...limitedBids.map(([p]) => p), ...limitedAsks.map(([p]) => p)];
          const minPrice = Math.min(...prices);
          const maxPrice = Math.max(...prices);
          const maxSize = Math.max(...limitedBids.map(([, q]) => q), ...limitedAsks.map(([, q]) => q), 1);

          bidDepthEl.textContent = formatNumber(buyDepth, 2);
          askDepthEl.textContent = formatNumber(sellDepth, 2);
          netDepthEl.textContent = formatNumber(buyDepth - sellDepth, 2) + ' · ' + describeImbalance(imbalance);
          spreadEl.textContent = formatNumber(spread, 4);
          updatedLabel.textContent = formatTimestamp(target.timestamp);
          titleEl.textContent = (target.instrument_id || '--') + ' 深度分布';

          ctx.clearRect(0, 0, canvas.width, canvas.height);
          ctx.fillStyle = '#0f172a';
          ctx.fillRect(0, 0, canvas.width, canvas.height);
          const padding = 40;
          const width = canvas.width - padding * 2;
          const height = canvas.height - padding * 2;
          const range = Math.max(maxPrice - minPrice, 1e-6);
          drawBars(limitedBids, '#10b981', minPrice, range, maxSize, padding, width, height);
          drawBars(limitedAsks, '#f87171', minPrice, range, maxSize, padding, width, height);

          ctx.strokeStyle = '#475569';
          ctx.beginPath();
          ctx.moveTo(padding, padding + height);
          ctx.lineTo(padding + width, padding + height);
          ctx.stroke();
          ctx.beginPath();
          ctx.moveTo(padding, padding);
          ctx.lineTo(padding, padding + height);
          ctx.stroke();
        }};

        function refresh() {{
          const params = new URLSearchParams({{ limit: levelSelect.value || '50' }});
          if (instrumentSelect.value) {{
            params.set('instrument', instrumentSelect.value);
          }}
          fetch('/api/streams/orderbook/latest?' + params.toString())
            .then((res) => res.json())
            .then(renderDepth)
            .catch((err) => console.error(err));
        }}

        instrumentSelect.addEventListener('change', refresh);
        levelSelect.addEventListener('change', refresh);
        refresh();
        setInterval(refresh, 4000);
      }})();
    </script>
</body>
</html>
"""


