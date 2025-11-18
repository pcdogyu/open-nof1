<div align="center">

面向 LLM 的量化交易控制平面（社区版）

</div>

## 项目简介

open-nof1.ai 尝试复刻 [nof1.ai](https://nof1.ai/) —— 让多个 AI 模型在真实或模拟市场中对决的公开基准。仓库以 FastAPI 为核心，串联 OKX 纸交易账户、InfluxDB 时序仓储、LLM 信号运行时和风险控制组件，提供一个可以扩展到多模型、多交易所的实验平台。

当前版本聚焦框架搭建，方便团队后续迭代到生产级能力。

## 核心能力一览

- **Web 控制台**（`services/webapp`）：FastAPI + HTML 仪表盘，用于浏览模型绩效、OKX 快照、账户明细以及基础配置。
- **账户仓储**（`accounts/repository.py`）：使用 InfluxDB 存储账户、持仓、订单、交易、权益曲线等时序数据。
- **数据管道**（`data_pipeline`）：拉取多时间尺度行情、资金费率与委托簿，计算指标后写入 Influx，并驱动信号生成。
- **模型运行时**（`models/runtime.py`）：协调 DeepSeek、Qwen 等 LLM 适配器，支持批量信号生成与离线演示。
- **风险控制**（`risk/`）：价格带校验、组合熔断等基础风控模块，可按需扩展。
- **交易所适配**（`exchanges/okx/paper.py`）：封装 OKX Demo/Testnet REST API，支持下单、撤单、资产拉取。
- **辅助脚本**（`scripts/`）：包含数据管道、OKX Demo 下单、风险演示、Influx 清理等运维工具。

## 环境准备

1. **克隆仓库并安装依赖**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   pip install -r requirements.txt
   ```

2. **配置 InfluxDB**
   - 安装 InfluxDB（推荐 2.x），创建组织与 Bucket。
   - 将连接信息写入 `config.py` 或设置环境变量：
     ```
     set INFLUX_URL=http://localhost:8086
     set INFLUX_ORG=your-org
     set INFLUX_BUCKET=your-bucket
     set INFLUX_TOKEN=your-token
     ```

3. **配置 OKX Demo 账户**
   - 登录 OKX Demo/Testnet 获取 API Key。
   - 在 `config.py` 的 `OKX_ACCOUNTS` 中配置 `api_key`、`api_secret`、`passphrase`、`model_id` 等字段，可按模型新增条目。

4. **配置 LLM Provider**
   - 默认提供 DeepSeek 与 Qwen adapter。
   - 在 `config.py` 或环境变量中设置 `DEEPSEEK_API_KEY`、`QWEN_API_KEY`。

> `config.py` 默认被 `.gitignore` 忽略，仅用于本地调试。生产环境请迁移到更安全的密钥管理方案（Vault/KMS 等）。

## 快速开始

### 启动控制台

```bash
python run_server.py
```

浏览器访问 [http://localhost:8000](http://localhost:8000) 查看模型 Dashboard、OKX 摘要、管道配置等页面。若尚无真实数据，页面会展示占位信息。

### 运行数据管道

```bash
python scripts/run_data_pipeline.py --instrument BTC-USDT-SWAP --instrument ETH-USDT-SWAP
```

流程：抓取行情 → 计算指标（MACD、RSI、波动率、资金费率等）→ 写入 Influx → 调用 LLM 生成交易信号。

### OKX Demo 下单示例

```bash
python scripts/okx_demo_trade.py place --inst-id BTC-USDT-SWAP --side buy --type limit --size 0.001 --price 24000
python scripts/okx_demo_trade.py cancel --inst-id BTC-USDT-SWAP --order-id <ordId>
```

### OKX 工具脚本

- 生成签名示例：`python exchanges/okxaccount.py`
- 查询 Demo 余额：`python exchanges/okxaccountbalance.py`

### 风控演示

```bash
python scripts/risk_demo.py
```

展示价格带拒单与熔断触发效果。

## 目录概览

```
accounts/            # 账户、交易、仓位模型与 Influx 仓储
data_pipeline/       # 行情采集与指标计算
exchanges/           # 交易所适配层（当前实现 OKX Demo 客户端）
models/              # LLM adapter、注册表、运行时
risk/                # 风险控制组件
services/webapp/     # FastAPI 控制台（HTML + REST API）
services/analytics/  # 榜单与数据缓存
services/jobs/       # APScheduler 定时任务
services/okx/        # OKX 账户快照、品种目录
scripts/             # 运维与演示脚本
docs/                # 设计文档与实施计划
```

## Roadmap（对齐 nof1.ai）

- 前端升级为 Next.js + GraphQL，并加入实时推送（WebSocket/SSE）。
- 补齐竞赛治理：模型注册、规则引擎、调度系统与审计日志。
- 引入统一配置/密钥管理、Postgres、对象存储等基础设施。
- 实现模型评估指标（Sharpe、Sortino、风险敞口矩阵）与 Prometheus/Grafana 监控。
- 扩展更多交易所（Binance、Bybit）与历史回放/回测引擎。

## 贡献指南

1. Fork 仓库并创建分支。
2. 在虚拟环境中运行必要的 lint/test（若后续引入）。
3. 提交 PR 时，请附上关键功能的使用说明或验证结果。

## 许可证

项目尚未指定正式许可证，默认保留所有权利。若计划商用或二次分发，请先与维护者沟通。

---

欢迎提交 Issue / PR，一起完善开放的 AI Trading Benchmark。
