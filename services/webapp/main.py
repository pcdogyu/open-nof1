"""
Entrypoint for the open-nof1.ai user-facing web service.

Run locally with:
    uvicorn services.webapp.main:app --reload
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from services.webapp import routes


app = FastAPI(
    title="open-nof1.ai",
    description="LLM-driven trading operations control plane",
    version="0.1.0",
)

app.include_router(routes.router)


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
def model_dashboard() -> HTMLResponse:
    """Render a lightweight HTML dashboard for model metrics."""
    metrics = routes.get_model_metrics()
    content = _render_model_dashboard(metrics)
    return HTMLResponse(content=content)


def _render_model_dashboard(metrics: dict) -> str:
    def _format_number(value, precision=2):
        if isinstance(value, (int, float)):
            return f"{value:,.{precision}f}"
        return value

    rows = []
    for model in metrics.get("models", []):
        rows.append(
            f"""
            <tr>
                <td>{model.get('model_id')}</td>
                <td>{model.get('portfolio_id')}</td>
                <td>{_format_number(model.get('sharpe_ratio'), 2)}</td>
                <td>{_format_number(model.get('max_drawdown_pct'), 2)}</td>
                <td>{_format_number(model.get('win_rate_pct'), 1)}%</td>
                <td>{model.get('avg_trade_duration_min')} 分钟</td>
                <td>${_format_number(model.get('exposure_usd'), 2)}</td>
                <td>{model.get('open_positions')}</td>
            </tr>
            """
        )
    rows_html = "\n".join(rows) if rows else "<tr><td colspan='8'>No data</td></tr>"
    trade_rows = []
    for trade in metrics.get("recent_trades", []):
        pnl = trade.get("pnl", 0)
        pnl_class = "pnl-positive" if pnl >= 0 else "pnl-negative"
        trade_rows.append(
            f"""
            <tr>
                <td>{trade.get('executed_at')}</td>
                <td>{trade.get('model_id')}</td>
                <td>{trade.get('portfolio_id')}</td>
                <td>{trade.get('instrument_id')}</td>
                <td>{trade.get('side')}</td>
                <td>{_format_number(trade.get('size'), 4)}</td>
                <td>{_format_number(trade.get('entry_price'), 2)}</td>
                <td>{_format_number(trade.get('exit_price'), 2)}</td>
                <td class="{pnl_class}">{_format_number(pnl, 2)}</td>
            </tr>
            """
        )
    trade_rows_html = (
        "\n".join(trade_rows) if trade_rows else "<tr><td colspan='8'>暂无交易</td></tr>"
    )
    as_of = metrics.get("as_of", "N/A")
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>open-nof1.ai Model Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #0f172a;
                color: #e2e8f0;
                margin: 0;
                padding: 20px;
            }}
            h1 {{
                margin-bottom: 0.2rem;
            }}
            h2 {{
                margin-top: 2rem;
                margin-bottom: 0.5rem;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 1.5rem;
                background-color: #1e293b;
                border-radius: 6px;
                overflow: hidden;
            }}
            th, td {{
                padding: 12px 16px;
                text-align: left;
                border-bottom: 1px solid #334155;
            }}
            th {{
                background-color: #0f172a;
                font-size: 0.9rem;
                text-transform: uppercase;
                letter-spacing: 0.05em;
            }}
            tr:nth-child(even) {{
                background-color: #1e293b;
            }}
            tr:hover {{
                background-color: #334155;
            }}
            footer {{
                margin-top: 2rem;
                font-size: 0.85rem;
                color: #94a3b8;
            }}
            .timestamp {{
                color: #38bdf8;
            }}
            a {{
                color: #38bdf8;
                text-decoration: none;
            }}
            a:hover {{
                text-decoration: underline;
            }}
            .pnl-positive {{
                color: #4ade80;
            }}
            .pnl-negative {{
                color: #f87171;
            }}
        </style>
    </head>
    <body>
        <h1>模型表现总览</h1>
        <div>更新时间: <span class="timestamp">{as_of}</span></div>
        <table>
            <thead>
                <tr>
                    <th>模型</th>
                    <th>投资组合</th>
                    <th>夏普比率</th>
                    <th>最大回撤(%)</th>
                    <th>胜率(%)</th>
                    <th>平均持仓时长</th>
                    <th>风险敞口(USD)</th>
                    <th>持仓数量</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
        <h2>最近交易</h2>
        <table>
            <thead>
                <tr>
                    <th>执行时间</th>
                    <th>模型</th>
                    <th>投资组合</th>
                    <th>标的</th>
                    <th>方向</th>
                    <th>数量</th>
                    <th>入场价</th>
                    <th>出场价</th>
                    <th>盈亏(USD)</th>
                </tr>
            </thead>
            <tbody>
                {trade_rows_html}
            </tbody>
        </table>
        <footer>
            数据来源: <a href="/metrics/models">/metrics/models</a> ·
            接口健康: <a href="/health">/health</a>
        </footer>
    </body>
    </html>
    """
