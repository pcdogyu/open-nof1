# open-nof1.ai 架构概览与实施蓝图

本文档基于公开的 open-nof1.ai 设想，继续拓展关键系统组件、服务间交互以及从零到上线的实施步骤，帮助团队快速把握整体工程形态。

---

## 1. 顶层系统分层

| 层级 | 关键职责 | 主要组件 |
| ---- | -------- | -------- |
| 展示层 | 面向观众的可视化、排行榜、交易详情 | `dashboard/web`, `dashboard/api`, 实时推送 (`dashboard/streaming.py`) |
| 赛事编排层 | 赛程管理、参赛模型注册、规则配置 | `competition/registry.py`, `competition/rule_engine.py`, `competition/schedule.py` |
| 策略执行层 | 模型推理、信号路由、交易执行、风控 | `models/*`, `execution/*`, `risk/*`, `monitoring/*` |
| 数据与集成层 | 行情接入、订单接口、数据存储、消息总线 | `exchanges/*`, `data_pipeline/*`, `infra/*` |

---

## 2. 关键模块扩展说明

### 2.1 赛事编排与治理
- `competition/registry.py`：登记参赛模型信息（所有者、版本、授权范围）。
- `competition/rule_engine.py`：解释赛事规则（资产范围、杠杆、交易窗口），并在信号下发前校验合规性。
- `competition/schedule.py`：定义赛事周期（报名、热身、正式赛、复盘），调度数据回放或实盘切换。
- `governance/audit_log.py`：不可篡改的操作审计链路，支持监管与追责。

### 2.2 数据与基础设施
- `infra/config_loader.py`：集中管理交易所密钥、环境变量、模型配额。
- `infra/message_bus.py`：封装 Kafka/NATS/Redis Stream，供行情、订单、告警使用。
- `storage/postgres.py`、`storage/timeseries.py`：持久化账户、交易、K 线与指标。
- `storage/object_store.py`：保存模型生成的日志、解释文本、可视化素材。

### 2.3 策略与推理
- `models/adapters/`：与外部 LLM（OpenAI、Anthropic、自建）或量化引擎对接。
- `models/runtime.py`：统一的异步执行容器，保证推理配额、并发隔离。
- `models/evaluation.py`：实时评估信号质量（命中率、Sharpe、回撤）。

### 2.4 风控与监控
- `risk/exposure_matrix.py`：计算各模型资产、杠杆敞口，支撑跨模型风险汇总。
- `risk/circuit_breaker.py`：触发性风控（如瞬时亏损>5%），暂停信号执行。
- `monitoring/metric_collector.py`：Prometheus/Grafana 指标输出。
- `monitoring/runbooks/`：标准化告警处置流程文档。

### 2.5 观众互动与生态
- `dashboard/web`：Next.js / React 前端，含排行榜、模型主页、交易流水。
- `dashboard/api/graphql.py`：向前端提供统一查询接口，便于聚合不同模型的数据。
- `community/hooks.py`：提供 Webhook/REST 接口给媒体或第三方开发者。
- `community/sponsorship.py`：赛季赞助、奖励发放、NFT 纪念品等衍生功能。

---

## 3. 端到端实施流程（从零到上线）

1. **需求澄清与合规评估**
   - 明确目标交易所（Binance/OKX/Bybit 等）及所在法域的合规要求。
   - 与法务确认公开展示交易数据的法规约束。

2. **基础设施准备**
   - 搭建 Git 仓库、CI/CD（GitHub Actions）、容器化环境（Docker + Compose）。
   - 准备云端运行环境（Kubernetes/Serverless），配置密钥管理（Vault/KMS）。

3. **行情与交易 API 对接**
   - 使用交易所 SDK/REST/WebSocket 接入行情；实现心跳、重连、限速策略。
   - 完成下单接口签名、风控参数（杠杆、保证金）配置，并通过沙箱环境验收。

4. **账户架构与资金隔离**
   - 为每个模型分配子账户或独立 API key，初始化 10k USD 资金。
   - 编写保证金与杠杆控制逻辑，定义每日对账与财务报表生成流程。

5. **模型引擎集成**
   - 实现统一的 `generate_signal()` 协议，包括输入（行情特征、prompt、规则）和输出（方向、数量、理由）。
   - 集成至少三类模型：
     1. 语言模型（LLM）策略：基于 prompt 解析宏观新闻、链上数据。
     2. 经典量化模型：动量、均值回归、趋势跟随。
     3. 风险对冲模型：依据波动率、相关性调整仓位。
   - 通过 `models/runtime.py` 控制调用频率、超时、重试。

6. **信号路由与执行链路**
   - 信号进入 `execution/signal_router.py`，先调用 `risk/order_validation.py`。
   - 合法信号转为标准订单，经 `data_pipeline/order_executor.py` 下单。
   - `execution/trade_logger.py`、`execution/pnl_tracker.py` 更新数据库与缓存。

7. **实时展示与观众交互**
   - `dashboard/api` 暴露 REST/GraphQL API，供前端与第三方使用。
   - 前端实现排行榜、模型详情页、交易日志、盈亏走势图、持仓热力图。
   - 实现延迟不超过 1s 的 WebSocket 推送，观众可实时看到成交与盈亏。

8. **监控与告警运营**
   - 将关键指标（订单成功率、延迟、杠杆利用率）上报到 Prometheus。
   - 设置告警阈值与 Runbook，异常时自动触发暂停或强制平仓。

9. **媒体与社区合作**
   - 提供嵌入式组件给合作媒体（如「動區動趨」），展示实时排行榜。
   - 建立 API 文档与 SDK，允许第三方申请参赛或订阅数据。

10. **赛后复盘与持续迭代**
    - 使用 `analytics/` 模块生成赛季报告，拆解各模型表现。
    - 对高风险模型进行深入分析，优化风控参数或限额。
    - 根据反馈优化产品体验，筹备下一赛季或扩展多链资产。

---

## 4. 后续扩展建议

- **回测与模拟赛**：构建历史行情回放环境，为新模型提供沙盒测试。
- **模型治理**：引入评分机制、可解释性报告，减少黑箱风险。
- **资产多元化**：支持更多交易所与衍生品（期权、永续合约）。
- **合规合作**：与合规合作伙伴共建监管沙箱，确保赛事合法透明。

---

> 本文档旨在提供从架构到实施的参考蓝图，后续可结合实际团队资源与监管要求持续细化。
---

## New Skeleton Modules
- `accounts/` houses portfolio registries and domain events.
- `exchanges/` standardizes exchange client protocols including `exchanges/okx/paper.py`.
- `services/webapp/` exposes the FastAPI web service entrypoint.

## Web Service Quickstart
- Install deps: `pip install fastapi uvicorn`
- Run dev server: `uvicorn services.webapp.main:app --reload --port 8000`
- Browse model dashboard UI: `http://localhost:8000/`
- Raw portfolios (with metrics): `http://localhost:8000/portfolios`
- System metrics: `http://localhost:8000/metrics/system`
- Model metrics: `http://localhost:8000/metrics/models`

## OKX Demo Trading Flow
- Install deps: `pip install httpx`
- Set env vars `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE`
- Place order: `python scripts/okx_demo_trade.py place --inst-id BTC-USDT-SWAP --side buy --type limit --size 0.001 --price 24000`
- Cancel order: `python scripts/okx_demo_trade.py cancel --inst-id BTC-USDT-SWAP --order-id <ordId>`



## Model Signal Runtime
- Install deps: `pip install httpx`
- Default registry (`models/bootstrap.py`) registers DeepSeek and Qwen adapters automatically.
- Runtime entrypoint: `SignalRuntime` (`models/runtime.py`) with async `generate_signal` / `batch_generate` helpers.
- Offline deterministic mode works without API keys; set `DEEPSEEK_API_KEY` and `QWEN_API_KEY` for live inference.
- Try it: `python scripts/signal_demo.py` prints sample signals for both models.


## Risk Controls
- Price bands: configure via `PriceLimitValidator` (`risk/price_limits.py`) and refresh with market data.
- Circuit breaker: `CircuitBreaker` (`risk/circuit_breaker.py`) halts portfolios on drawdown/loss breaches.
- Combined engine: `RiskEngine` (`risk/engine.py`) runs validations sequentially; extend with hooks.
- Demo: `python scripts/risk_demo.py` shows price rejection and circuit halt output.

## Data Pipeline & AI Signals
- Web UI 首页展示最近 AI 信号：访问 `http://localhost:8000/` 可查看中英双语的模型意见与建议订单。
- Install deps: `pip install httpx pandas numpy influxdb-client`
- Provide Influx access: (A) edit `config.py` token/URL or (B) set `INFLUX_TOKEN` env (plus optional `INFLUX_URL`/`INFLUX_BUCKET`)
- Run pipeline: `python scripts/run_data_pipeline.py --instrument BTC-USDT-SWAP --instrument ETH-USDT-SWAP`
- Pipeline steps: fetch multi-timeframe candles, funding, order book, trades → compute MACD/RSI/volatility/CVD/盘口不平衡 → write to InfluxDB → call `SignalRuntime` (DeepSeek/Qwen) for trade signals.
