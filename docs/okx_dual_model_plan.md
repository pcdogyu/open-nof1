# OKX 双模型实盘演练计划（DeepSeek + Qwen）

## 1. 项目概览

- 目标：让 DeepSeek 与 Qwen 两个 LLM 以独立策略账户接入 OKX Demo/Testnet，形成可量化对比的实盘演练。
- 资金：为每个模型划拨等额的虚拟本金（建议 10,000 USDT），确保评估公平。
- 场景：聚焦永续合约（SWAP）交易，优先选择流动性较好的 BTC/ETH。
- 监控：通过控制台和 InfluxDB 指标实时观察盈亏、风控状态、下单延迟等关键数据。
- 扩展：方案在保证稳定性的前提下，为后续接入 Binance、Bybit 等交易所预留接口。

## 2. 架构总览

```
DeepSeek / Qwen (LLM Agents)
          │
          ▼
models/runtime.py ──> execution/signal_router.py ──> risk stack
          │                                          │
          ▼                                          ▼
accounts/portfolio_registry.py             exchanges/okx/paper.py
          │                                          │
          ▼                                          ▼
storage/{timeseries,postgres}.py           execution/{trade_logger,pnl_tracker}.py
          │                                          │
          └─────────────── dashboard/api & dashboard/web ───────────────┘
```

- **模型适配**：`models/adapters/deepseek.py` 与 `models/adapters/qwen.py` 负责 prompt 构造、输出解析。
- **运行时调度**：`models/runtime.py` 管理异步调用、重试、超时与提示词模板。
- **组合管理**：`accounts/portfolio_registry.py` 记录每个模型的 API Key、交易所、基础货币、备注等元数据。
- **信号路由**：`execution/signal_router.py` 将模型输出转换成标准化订单请求，并在进入风控前附加 owner/model 标识。
- **风控层**：`risk/order_validation.py`、`risk/price_limits.py`、`risk/circuit_breaker.py` 完成价格区间、仓位限制、熔断等校验。
- **交易所适配层**：`exchanges/okx/paper.py` 提供签名、限速、错误分类与 REST/WebSocket 调用。
- **监控**：`monitoring/metric_collector.py` 与 `dashboard/web` 展示 per-model KPI、告警状态与资金曲线。

## 3. 工作流

1. 数据管道 (`scripts/run_data_pipeline.py`) 写入多时间尺度行情与指标。
2. 控制台或后台任务调度调用 `models/runtime.SignalRuntime`，生成 DeepSeek/Qwen 的交易指令。
3. 指令经由 `execution/signal_router.py` 统一格式化，进入风控引擎。
4. 风控通过后调用 `exchanges/okx/paper.py` 下单或撤单。
5. 成交与账户信息写回 InfluxDB，并通过控制台展示。
6. APScheduler (`services/jobs/scheduler.py`) 周期性刷新排行榜、记录权益曲线。

## 4. 实施排期（6 周）

### Week 1：基础设施
- 开启 OKX Demo/Testnet 账户，创建并验证 API key/secret/passphrase。
- 搭建密钥存储（Vault/KMS 或加密配置文件），落地 `infra/config_loader.py`。
- 设计 `accounts/schema.sql` / `accounts/repository.py` 所需的 Influx measurement 与索引。

### Week 2-3：模型适配与运行时
- 对接 DeepSeek、Qwen 官方 REST SDK，补齐鉴权、限速、错误处理。
- 在 `models/runtime.py` 中实现多模型注册、权重配置与并发调度。
- 调整 prompt：包含风险上下文（仓位、余额、杠杆限制）与市场快照（盘口、资金费率、波动率）。

### Week 3-4：执行与风控
- 编写 `execution/signal_router.py`，将模型信号映射为合约下单指令。
- 实现 `risk/order_validation.py`：价格带、最小成交量、杠杆上限、禁止做空等规则。
- 配置 per-model 熔断器（`risk/circuit_breaker.py`），支持按净值回撤、亏损阈值触发。

### Week 4-5：OKX 接入与回测
- 在 `exchanges/okx/paper.py` 完成下单、撤单、查询仓位与成交等接口；实现重试与错误分类。
- 开发 `execution/trade_logger.py`，记录所有信号、风控结果与订单生命周期。
- 设计自动化测试（pytest/pytest-asyncio）覆盖风控、交易所适配与模型调用。

### Week 5-6：监控与运维
- 扩展 `services/webapp`，增加 OKX 实时报表、模型绩效对比与管道配置页面。
- 集成 Prometheus/Grafana，输出模型收益、下单延迟、错误率等指标。
- 撰写常见告警 Runbook，保存至 `monitoring/runbooks/okx_dual_model.md`。

### Week 6+：扩展
- 抽象 `exchanges/base_client.py`，统一签名、限速、错误处理接口。
- 设计 Binance、Bybit 连接器，复用已有风控与执行结构。
- 引入历史回放模块（`backtest/replay_engine.py`），用于回测与模型验证。

## 5. 风险与应对

- **密钥安全**：API Key 需启用最小权限并存储在受控环境，禁用代码库明文。
- **资金隔离**：每个模型使用独立账户，保证账目清晰，便于对比。
- **消息幂等**：模型输出通过消息总线传递时使用 idempotency key，防止重复下单。
- **风控失效**：为关键策略设置二级限额，出现异常时自动触发熔断并告警。
- **模型异常**：保留兜底逻辑（如回退到规则策略），并对 prompt/响应进行一致性检查。

## 6. KPI 指标

- 下单到成交的平均时延 < 500 ms（Demo 环境）。
- 单笔滑点控制在 2 bps 以内。
- 每日模型收益报表生成时间 < 收盘后 5 分钟。
- 控制台首屏加载时间 < 1 秒。
- OKX API 出错后自动恢复的重试次数 ≤ 3。

## 7. 后续路线

1. **多交易所路由**：设计统一交易路由器，对接 Binance/Bybit，实现跨所套利策略。
2. **策略治理**：引入策略上线审批流程、规则校验及审计日志。
3. **开放生态**：设计 Webhook/API，让外部团队上传模型信号或订阅赛事数据。
4. **多资产扩展**：支持股票、期权、现货等品种，完善估值模型与风险控制。

> 通过以上步骤，可逐步打通 DeepSeek 与 Qwen 在 OKX Demo 环境的实盘演练链路，为开放式 AI 交易基准奠定基础。
