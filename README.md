# Sys-Trader — Binance USDT-M 永续合约交易系统

事件驱动的 Binance 永续合约自动化交易系统：策略信号 → 风控链 → 执行层 → 对账/告警/面板 全链路闭环。**不含信号/回测引擎**（策略与回测独立运行），本仓库聚焦交易基础设施：风控、执行、运维、可视化。

## 架构总览

```
策略/回测引擎 (独立) ──► 信号 ──► 风控链 ──► OrderManager ──► Binance USDT-M API
                                    │                │
                              (PositionSizer → Leverage → AvailableMargin →    │
                               DrawdownBreaker → DailyLossLimit → Concentration
                               → DailyTradeLimit → MaxStopDistance)            │
                                    │                ▼
                              EventBus (Redis Streams) ◄── User Data Stream / 对账
                                    │
                        ┌───────────┼───────────┐
                        ▼           ▼           ▼
                  StateStore    OpsArchive    Alert/DingTalk
                  (实时面板)    (历史归档)    (告警/每日摘要)
```

- **风控链（8 件套）**：仓位计算、杠杆上限、可用保证金、回撤熔断（15%）、日亏损、集中度、单日交易次数、最大止损距离
- **主动防御**：保证金率自动减仓、回撤分级减仓（12%）、清算价/爆仓距离预警（距清算 <8% 自动减仓）、ADL 队列告警、大额强平监控
- **执行层**：LIMIT 入场（GTC/IOC/PostOnly）、部分成交余量策略、Algo Order 条件单（固定/追踪止损 + 止盈）、幂等下单（clientOrderId）、PENDING 超时撤单、fail-correct 撤单语义
- **对账**：启动对账 + 300s 持续对账（local_only/remote_only/qty_mismatch 三类漂移自愈）、资金费 income 流水 tranId 精确对账、实际手续费率（commissionRate）
- **运维**：双看板（交易 + 运维）、钉钉告警（CRITICAL @人）、每日运营摘要、heartbeat/soak/proxy 三看门狗、kill switch、24h 稳定性测试

## 快速开始

```bash
# 1. 准备环境 (Python 3.11 + Redis)
pip install -r requirements.txt

# 2. 配置 (复制模板, 填入 testnet 密钥)
cp config/.env.example config/.env

# 3. 启动交易系统 (默认 testnet, 24h 限时运行)
python -m shared.runner --hours 24

# 4. 启动面板 (后端 :8000 + 前端 Vite :5173)
python dashboard/server.py
cd dashboard/frontend && npm install && npm run dev

# 5. 运行测试
python -m pytest tests/ -q --basetemp=.pytest-tmp
```

## 关键环境变量（节选）

| 变量 | 默认 | 说明 |
|------|------|------|
| `BINANCE_API_KEY/SECRET` | - | testnet/实盘 API 密钥 |
| `PROXY_HOST/PORT` | `127.0.0.1:7897` | Clash/mihomo 代理 |
| `MAX_LEVERAGE` | `5` | 全局杠杆上限 |
| `MARGIN_DELEVERAGE_THRESHOLD` | `0.8` | 保证金率自动减仓阈值 |
| `LIQ_ALERT_PCT` | `0.08` | 爆仓距离减仓阈值 |
| `FUNDING_ACCOUNTING` | `income` | 资金费记账口径 (income/estimate/off) |
| `PROTECTION_SL_MODE` | `stop` | 止损模式 (stop/trailing) |
| `DINGTALK_WEBHOOK_URL` | - | 钉钉机器人 (关键词 `SysTrader` + 加签) |

完整清单见 `config/.env.example`。

## 安全说明

- 所有密钥（`.env`、`.claude/`、代理凭据）均被 `.gitignore` 排除，**请勿提交任何真实密钥**
- 默认连接 **testnet**（`testnet.binancefuture.com`），连接实盘需显式 `--no-testnet`（慎用）

## 目录结构

```
shared/        runner 主控 / EventBus / 对账 / 资金费 / 心跳
execution/     OrderGateway (REST) / OrderManager (订单生命周期)
risk/          风控链 8 件套
market_data/   行情 feed / 用户数据流 / K线归档 / 深度 / 强平流
portfolio/     持仓/权益/盈亏跟踪
dashboard/     FastAPI 后端 + React/Vite 前端 (交易 + 运维双看板)
monitor/       Alerter / DingTalk / MetricsCollector
tools/         看门狗 / Telegram / 交易日志 / TCA / 稳定性测试
docs/          架构文档 / ERROR_LEDGER 错题本
tests/         全量 pytest (560+ 用例)
```

## 文档

- [架构文档](docs/superpowers/specs/2026-07-04-trading-system-architecture.md)
- [ERROR_LEDGER 错题本](docs/ERROR_LEDGER.md) — 全部踩坑记录与修复（BUG-001~038）
- 项目审计：`docs/2026-08-16-project-audit.md`

> ⚠️ 本项目为个人学习/交易辅助用途，不构成投资建议。合约交易存在爆仓风险，请使用 testnet 充分验证。
