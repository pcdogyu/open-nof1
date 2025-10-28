# OKX ˫ģ�ͺ�ɳ�谲���滮（DeepSeek + Qwen）

## 1. Ŀ���뼯�ܸ���
- ��ģ��：DeepSeek �� Qwen ƽ̨ LLM ��Ϊ���Կͻ�����վ��Ϊͬ�������Ĳ�Դ；
- �ʽ��ֲ�：ÿ��ģ�ͷ��� 10k USD ����ģ�档����OKX Demo/Testnet ����；
- �����：֧�ָ�ɳ��ɳ���ּ��򺣻ص㡢�����ߡ����ڼ��；
- ����չʾ：���ݳ�ڼ��ɽ���ڱ�ش����Ժ� dashboard/api/web ���߲�ά；
- ����չ��：Ϊ���ڻ����������̳ɱ��Ը��� Binanace、Bybit �ȳ���ϵͳ��׼��；

## 2. ϵͳ����״��
```
DeepSeek/Qwen (LLM Agents)
        │
        ▼
models/runtime.py  ──► execution/signal_router.py ──► risk stack
        │                                              │
        ▼                                              ▼
accounts/portfolio_registry.py                exchanges/okx/paper.py
        │                                              │
        ▼                                              ▼
storage/{timeseries,postgres}.py             execution/{trade_logger,pnl_tracker}.py
        │                                              │
        └─────► dashboard/api ──► dashboard/web ◄─────┘
```
- **ģ��������**：`models/adapters/deepseek.py` �� `models/adapters/qwen.py` ʵ���� prompt ��Ӧ�������еĲ�����
- **�첽����**：`models/runtime.py` ͳһ����ʱ������첽ִ�С�supervision��retry；
- **�˻�����**：`accounts/portfolio_registry.py` ����ģ��������Դ�� API key/passphrase ��Ԥ��������
- **�ź�·��**：`execution/signal_router.py` ���з���ģɳƳ��������仡 owners ���� risk/order_validation.py ����ϵĹ���；
- **OKX ��ԭ**：`exchanges/okx/paper.py` �ṩ REST/WebSocket ����ݣ�ƽ���ɽ�/ɳ���������；
- **���ؼ���**：`monitoring/metric_collector.py` �� `dashboard/web` ǰ�˷�չʾ per-model KPI ��᣻

## 3. ʵʩ��������
1. **���嵥�����**（Week 1）
   - ���깽�� OKX Demo/Testnet �ͻ����󣬴��� API key/secret/passphrase；
   - �� Vault/KMS �� `infra/config_loader.py` ��������；
   - ��ʵ `accounts/schema.sql`（����）���� per-model portfolio ���õĴ�ݴ洢；

2. **ģ�͹�������**（Week 2-3）
   - ʵ������ DeepSeek/Qwen REST SDK �� Python adapter；
   - �� `models/runtime.py` �д��� DeepSeek/Qwen ִ�������ڵĹ����첽����；
   - �� Prompt �ж��� risk context（positions/balance/borrow_limit）��market snapshot（bid/ask/spread）；

3. **�źſ�ܺͷ���**（Week 3-4）
   - �� `execution/signal_router.py` �м�� `owner_model_id` �� OKX �˻����；
   - �� `risk/order_validation.py` ����ģ�ͼ��ɽ���ʽ�ޡ���Խ�ؽ�ֹ；���ø��˽ӿڸ���； 
   - ʵ�� per-model circuit breaker（������ֵ��PnL drawdown��ticker blocklist）；

4. **OKX Demo ʵʱ����**（Week 4-5）
   - ���� `exchanges/okx/paper.py`：REST（/trade/order） WebSocket（positions/balance/ticker）；
   - ����ɽ����־ `execution/trade_logger.py` ������������ PnL ��ڼ���；
   - ʵ�����԰���� (pytest + pytest-asyncio) ��ɳ�谲���；

5. **չʾ��֪ͨ**（Week 5-6）
   - �� `dashboard/api` ���� OKX ģ��ҳĿ；
   - �� Grafana/Prometheus ����ģ�ͻ� KPI��ɳ���ռ��；
   - ���ݲ्ְ runbook（`monitoring/runbooks/okx_dual_model.md`）�����쳣；

6. **���չ��׼��**（Week 6+）
   - �滮 `exchanges/base_client.py` ��ͳһ����（signing, rate-limit, error taxonomy）；
   - ׼���� Binance/Bybit connectors ��ͬ���˻�ʵ���Լ�；
   - ���ȹ����ӳٴ��� replay ����（`backtest/replay_engine.py`）��Լ；

## 4. ��ؼ��о�������
- **���İ�ȫ**：���õĽ��ű鱣�ߣ�key ֻ�ڲ��߿ɼ��������ڵ�；
- **�ʽ���������**：per-model ledger ���� balance��PnL��exposure���� daily reconciliation； 
- **�ֲ������**：DeepSeek/Qwen tasks �� message bus ��ʹ�� idempotency key ��������；
- **�쳣����**：Retry ��˳ʱд��ʱ�谲������ͣ��（PnL drawdown>5%）；
- **������ά**：���ֶ����� prompt σ�ʣ�ͬ�� backtest ��������；

## 5. KPI ��֤����
- �ɽ�ִ�в��ȣ�< 500ms（Demo）
- Spread slippage ��ֹ�� ≤ 2bps
- Per-model daily PnL report readiness < 5min end-of-day
- Web dashboard ʵʱʱ�۶��� 1s
- OKX API error recovery < 3 retry cycles

## 6. ��չ·��
1. **��������**：��е Binance/Bybit connectors -> ͳһ exchange router；
2. **���޸����**：���� risk ����Ϊ policy-as-code（例如 OPA）；
3. **�������ƽ**：API/Webhook ���뽻���ߺ͸�����������；
4. **����ҹ���**：�ؽ� governance workflow，���ֿϵͳ��ο���；

> ��ָ��Ϊ OKX Demo ����汾���ݲ�сģ����(DeepSeek + Qwen) ��һλĿ����ʵʩ����ָ�ꡣ���ڲ�һ��ģ���չ���������ڽ����й淶����ͨ������������
