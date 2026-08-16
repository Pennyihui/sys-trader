# Sys_trader — 完整交易系统架构设计

> **日期**: 2026-07-04 | **阶段**: 架构设计 | **基于**: [调研报告](../research/2026-07-04-trading-system-components-research.md)

---

## 1. 项目背景

### 1.1 现状

已有 [`agent_team`](../../workspace_0503/agent_team/) — 4 层多时间框架 BTC 趋势交易系统：

- `Data_pipeline/` — 数据获取、特征计算（批量模式）
- `Outlook/` — 周线宏观预测（斐波那契+艾略特+链上，XGBoost）
- `Status/` — 日线 15 格市场状态矩阵 + 11 策略信号（XGBoost）
- `Signal_Generation/` — 4h RL 环境 + 贝叶斯引擎 + conviction scoring → 交易信号
- `Backtest/` — 回测引擎、指标计算、自适应 barrier
- `shared/` — Meta-Labeling、Triple-Barrier 标签工厂、支撑阻力特征

**限制**：单标的（BTC）、批处理模式（CSV 文件读入）、无实盘能力。

### 1.2 目标

多标的（BTC/ETH/SOL 等主流合约）、低频（0-3 笔/天）、USDT 本位永续合约、全自动风控、Binance 执行、事件驱动架构。

---

## 2. 架构总览

### 2.1 模块清单

| # | 模块 | 目录 | 职责 | 状态 |
|:--|------|------|------|:----:|
| — | Signal Engine | `signal_engine/` | 4 层 RL+贝叶斯信号生成 | 迁移 |
| 1 | Market Data | `market_data/` | WebSocket 订阅、K 线缓存、闭合检测 | 新建 |
| 2 | Scheduler | `scheduler/` | K 线闭合 → 触发对应层信号引擎 | 新建 |
| 3 | Risk Manager | `risk/` | 中间件链：仓位→杠杆→熔断→日亏损→集中度 | 新建 |
| 4 | Execution Engine | `execution/` | Binance 下单/撤单/成交管理 | 新建 |
| 5 | Portfolio Tracker | `portfolio/` | 持仓、保证金、权益、盈亏追踪 | 新建 |
| 6 | Monitor & Alert | `monitor/` | 心跳检测、异常告警、Telegram 通知 | 新建 |
| — | Event Bus | `shared/event_bus.py` | Redis Streams 事件总线 | 新建 |
| — | Persistence | `data/` | SQLite 信号/订单/持仓记录 | 新建 |

### 2.2 架构图

```
                         ┌──────────────────────┐
                         │   Monitor & Alert     │
                         │  (Health / Metrics)   │
                         └──────────┬───────────┘
                                    │
   ┌────────────────────────────────┼────────────────────────────────┐
   │                         Event Bus                                │
   │                      (Redis Streams)                             │
   └────────────────────────────────┼────────────────────────────────┘
          │           │            │            │            │
   ┌──────▼──┐ ┌──────▼──┐  ┌─────▼─────┐ ┌───▼────┐ ┌────▼─────┐
   │ Market  │ │Scheduler│  │  Signal   │ │  Risk  │ │Execution │
   │  Data   │ │         │  │  Engine   │ │Manager │ │  Engine  │
   │(WebSocket)│         │  │ (你现有)  │ │        │ │(Binance) │
   └──────┬──┘ └──────┬──┘  └─────┬─────┘ └───┬────┘ └────┬─────┘
          │           │            │            │            │
   ┌──────▼───────────▼────────────▼────────────▼────────────▼─────┐
   │                     Persistence Layer                          │
   │   SQLite (信号/订单/持仓)  +  Redis (状态/缓存)                 │
   └───────────────────────────────────────────────────────────────┘
```

### 2.3 事件类型

| 事件流 | 生产者 | 消费者 | 说明 |
|--------|--------|--------|------|
| `kline.closed` | Market Data | Scheduler | K 线闭合通知 |
| `features.ready` | Signal Engine | — | 特征计算完成（内部） |
| `signal.generated` | Signal Engine | Risk Manager | 交易信号输出 |
| `signal.approved` | Risk Manager | Execution Engine | 通过风控的信号 |
| `signal.rejected` | Risk Manager | Monitor | 被拒绝的信号 |
| `order.filled` | Execution Engine | Portfolio, Monitor | 订单成交 |
| `position.changed` | Portfolio Tracker | Risk Manager, Monitor | 持仓变更 |
| `alert.*` | Monitor | — | 告警事件 |
| `heartbeat.*` | 各模块 | Monitor | 心跳 |

---

## 3. 模块详细设计

### 3.1 Market Data

**职责**：WebSocket 订阅 → K 线缓存 → 闭合检测。**不做特征计算。**

```
Binance WebSocket ──→ Connection Pool ──→ K-line Buffer ──→ 闭合检测 ──→ kline.closed 事件
```

**连接池**：

- 库：`binance-connector-python`（官方 SDK，`WebsocketMode.POOL`）
- pool_size = min(symbol_count, 5)
- 每连接最多 200 streams，round-robin 分配
- 自动重连

**订阅清单**（每标的）：

| Stream | 用途 |
|--------|------|
| `<symbol>@kline_1w` | Weekly Outlook |
| `<symbol>@kline_1d` | Daily Status |
| `<symbol>@kline_4h` | 4h Signal Generation |
| `<symbol>@markPrice` | 标记价格（合约关键，非 lastPrice） |

**K 线缓存**：

- 缓存最近 N 根 K 线（N = 各层特征所需窗口的最大值）
- 检测闭合：收到新 K 线的第一笔更新 → 上一根已闭合
- 闭合后立即发布 `kline.closed {symbol, timeframe, ohlcv[]}` 事件

**关键设计决策**：

- 只用 markPrice 而非 lastPrice（防价格操纵）
- 不做特征计算——保持模块薄，特征逻辑留在 Signal Engine 各层
- 连接池而非单一 multiplex（单连接故障隔离）

---

### 3.2 Scheduler

**职责**：收到 K 线闭合事件 → 触发对应层的 Signal Engine。**不筛选标的，不做质量判断。**

```
kline.closed (4h) ──→ 触发 Signal Engine 4h 层
kline.closed (1d) ──→ 触发 Signal Engine Daily 层  
kline.closed (1w) ──→ 触发 Signal Engine Weekly 层
```

**触发规则**：

| 时间框架 | 触发频率 | 说明 |
|----------|----------|------|
| Weekly (1w) | 每周一 00:00 UTC | 全标的并行 |
| Daily (1d) | 每日 00:00 UTC | 全标的并行 |
| 4h | 每 4h (00/04/08/12/16/20 UTC) | 哪个标的闭合触发哪个 |

**实现**：
- 标的间无依赖，`ThreadPoolExecutor(max_workers=8)` 并行
- 轻量实现，不需要 Celery/RQ 等任务队列
- K 线闭合时间有 jitter（±几分钟），Scheduler 用事件驱动而非 cron

**Weekly/Daily 特殊处理**：
- 触发一次后缓存结果
- 4h 层拉取最新的 Weekly/Daily 推理结果作为上下文

---

### 3.3 Signal Engine

**职责**：从 `agent_team` 迁移现有信号逻辑，保持内部结构不变，包装事件接口。

**内部结构不变**：

```
Weekly (Outlook)    → weekly_features → Primary+Meta Model → {bullish/bearish/neutral}
Daily (Status)      → daily_features   → Primary+Meta Model → {market_state, direction}
4h (Signal Gen)     → signal_features  → RL Env + Bayesian  → {direction, conviction, attribution}
```

**新增适配**：

- `SignalEngine.run(symbol, timeframe, ohlcv)` — 标准入口
- 特征计算在各层内部完成（复用 `*_features.py`）
- 4h 层从缓存读取 Weekly/Daily 的上下文输出
- 输出 `signal.generated {symbol, direction, conviction, entry_price, stop_loss, attribution, timestamp}`

**元数据要求**：

每个信号携带：
- `direction`: LONG / SHORT
- `conviction`: 0.0-1.0（信念评分）
- `entry_price`: 建议入场价
- `stop_loss`: 建议止损价
- `take_profit`: 建议止盈价
- `attribution`: 信号来源分解（哪个策略贡献了多少）
- `symbol`: 标的
- `timestamp`: 生成时间

---

### 3.4 Risk Manager

**职责**：中间件链模式。每个中间件独立检查，链式传递。通过则放行，不通过则记录拒绝原因。

```
signal.approved ⟵─ [PositionSizer] → [LeverageController] → [DrawdownBreaker] → [DailyLossLimit] → [ConcentrationCheck] ⟶ signal.generated
signal.rejected ⟵（任一步拒绝）
```

#### 3.4.1 PositionSizer

```
输入: signal + account_state
逻辑:
  risk_amount = equity × risk_per_trade (可配，default 1.5%)
  stop_distance = |entry - stop_loss|
  size = floor(risk_amount / stop_distance / contract_value)
  if size > max_position_per_symbol: size = max_position_per_symbol
输出: modified_signal + {position_size, leverage}
```

#### 3.4.2 LeverageController

```
输入: signal + account_state + symbol_volatility
逻辑:
  target_leverage = min(config.max_leverage, floor(volatility_factor / current_volatility))
  if margin_ratio > 0.6 → 拒绝（保证金过高，不开新仓）
输出: modified_signal + {leverage}
```

#### 3.4.3 DrawdownBreaker

```
状态: ACTIVE | TRIGGERED | COOLDOWN

触发条件:
  - 当前回撤 (peak_equity - current_equity) / peak_equity > max_drawdown (default 15%)
  - 连续亏损 >= 3 笔 → 触发 1 小时熔断

ACTIVE → TRIGGERED: 任一条件满足
TRIGGERED → COOLDOWN: 所有持仓平掉后进入冷却
COOLDOWN → ACTIVE: 冷却时间到 (default 2-4h)
```

#### 3.4.4 DailyLossLimit

```
重置: 每日 UTC 00:00
逻辑:
  if daily_realized_pnl / equity <= -daily_loss_limit (default -5%):
    拒绝所有新信号，直到下一个 UTC 00:00
```

#### 3.4.5 ConcentrationCheck

```
约束:
  - 单标的保证金 / 总权益 ≤ 30%
  - 同方向总保证金 / 总权益 ≤ 50% (防止全部多头或空头)
  - 总保证金 / 总权益 ≤ 80% (留 20% 缓冲)
```

---

### 3.5 Execution Engine

**职责**：通过风控的信号 → Binance 订单 → 成交确认 → 通知 Portfolio。

```
signal.approved → OrderManager → OrderGateway → Binance Futures API
                                          ↓
                                    成交 / 失败 / 超时
                                          ↓
                              order.filled / order.failed 事件
```

**两层设计**：

**Order Gateway**（Binance API 通信）：
- 下单：`POST /fapi/v1/order`
- 撤单：`DELETE /fapi/v1/order`
- 查询：`GET /fapi/v1/order`
- 账户：`GET /fapi/v2/account`
- HMAC 签名，API Key 只给期货权限
- testnet 环境先行验证

**Order Manager**（订单生命周期）：

| 场景 | 处理 |
|------|------|
| 限价单提交 | LIMIT 订单挂在信号建议入场价 |
| 止损单 | STOP_MARKET 挂在信号止损价（用 markPrice 触发） |
| 止盈单 | TAKE_PROFIT_MARKET 挂在信号止盈价 |
| 部分成交 | 等待 30s 后取消剩余，按实际成交记录 |
| 网络错误 | 最多 3 次重试，指数退避 (1/2/4s) |
| 限频 (429) | 等待 Retry-After header |
| 订单超时 | 60s 未成交 → 撤单 |

**优先级事件队列**（参考 Bybit 生产级实践）：

```
Priority 0: 成交事件 (fill)     — 最敏感
Priority 1: 订单状态变更        — 次敏感
Priority 2: 持仓更新            — 最低
```

线程实现：`heapq` + `threading.RLock()` + `Condition`。

---

### 3.6 Portfolio Tracker

**职责**：系统在任何时刻都能回答"我现在持有什么、值多少钱"。

**追踪数据**：

| 字段 | 来源 | 更新频率 |
|------|------|----------|
| total_equity | Binance Account API | 60s 定时 + 成交时立即更新 |
| available_balance | Binance Account API | 同上 |
| positions (per symbol) | Execution 成交事件 + 定时对账 | 成交时立即 |
| unrealized_pnl | markPrice × position_size | 60s 定时 |
| total_margin | sum(position.margin) | 成交时立即 |
| margin_ratio | total_margin / total_equity | 实时计算 |
| peak_equity | max(peak_equity, current_equity) | 每次权益更新 |
| daily_pnl | sum(当日已实现盈亏) | 实时 |
| trade_history | 每笔成交记录 | 追加 |

**对账机制**：

每 2 分钟用 Binance Account API 对账：
- 本地持仓 vs Binance 返回持仓
  - 一致 → OK
  - 本地有 / Binance 无 → 可能手动平仓，记录并修正
  - Binance 有 / 本地无 → 漏单，告警

**User Data Stream**：

订阅 Binance User Data Stream 获取实时的账户/订单/持仓推送，作为 REST API 轮询的补充。

---

### 3.7 Monitor & Alert

**职责**：系统心跳监控、异常检测、通知。

**心跳机制**：

每个模块每 10 秒发送 `heartbeat.{module_name}` 事件。
Monitor 检测：超时 60 秒无心跳 → 告警。

**关键指标**：

| 指标 | 阈值 |
|------|------|
| margin_ratio | > 80% → 🔴 |
| 连续亏损次数 | ≥ 5 → 🔴 |
| 回撤 | > 15% → 🔴 |
| 日亏损 | > 3% → 🟡, > 5% → 🔴（风控已拦截） |
| WebSocket 断连 | > 30s 无重连 → 🔴 |
| 订单重试 | > 1 次 → 🟡 |

**通知渠道**：

- Telegram Bot（推荐）：免费，支持分组频道，支持命令交互（`/status`, `/positions`, `/stop`）
- 备用：日志文件、控制台输出

**实现**：

轻量方案——不需要 Prometheus + Grafana：
- Python `logging` → 文件 + StreamHandler
- 自定义 `MetricsCollector` 单例，线程安全
- 定时 (30s) 检查阈值 → 触发 Telegram Bot API

**Telegram 命令支持**：

| 命令 | 功能 |
|------|------|
| `/status` | 当前账户权益、持仓、今日盈亏 |
| `/positions` | 当前所有持仓详情 |
| `/pnl` | 今日/本周/本月盈亏汇总 |
| `/stop` | 紧急停止：立即平掉所有仓位 |
| `/pause` | 暂停新开仓 |
| `/resume` | 恢复新开仓 |

---

### 3.8 Event Bus

**职责**：模块间通信的唯一媒介。

**技术选择**：Redis Streams（轻量，低频场景足够，运维简单）

**Python 接口**：

```python
# 发布
EventBus.publish(stream="kline.closed", data={symbol, timeframe, ohlcv})

# 订阅
EventBus.subscribe(stream="kline.closed", consumer_group, handler)
```

**事件格式**：

```json
{
  "event_id": "uuid",
  "stream": "signal.generated",
  "timestamp": "2026-07-04T08:00:00Z",
  "data": {
    "symbol": "BTCUSDT",
    "direction": "LONG",
    "conviction": 0.72,
    ...
  }
}
```

**消费者组**：
- 每条 stream 一个 consumer group
- 支持 ACK 机制，确保消息被处理

---

## 4. 数据流示例

完整交易链路：

```
1. UTC 08:00 → Market Data 检测到 BTC 4h K 线闭合
2. 发送 kline.closed {BTCUSDT, 4h, ohlcv[...]}

3. Scheduler 收到 → ThreadPool 分配 worker
   → SignalEngine.run("BTCUSDT", "4h", ohlcv)

4. Signal Engine 内部:
   - 读取缓存的 BTC Weekly/Daily 推理结果
   - signal_features.py 计算 4h 特征
   - RL env 推理 + 贝叶斯更新
   - → {LONG, conviction=0.72, entry=62500, sl=61500, tp=65000}

5. 发送 signal.generated

6. Risk Manager 中间件链:
   PositionSizer:     risk 1.5% × $10,000 / $1,000 distance = 0.15 BTC ✅
   LeverageController: 3x ✅
   DrawdownBreaker:   current_dd 3% < 15% ✅
   DailyLossLimit:    today_pnl +$12 ✅
   ConcentrationCheck: BTC margin 25% < 30% ✅
   → 放行

7. 发送 signal.approved

8. Execution Engine 收到:
   OrderManager → POST LIMIT BUY 0.15 BTC @ 62500
                → POST STOP_MARKET SELL 0.15 BTC @ 61500
                → POST TAKE_PROFIT_MARKET SELL 0.15 BTC @ 65000

9. BTC 价格触及 62500 → 成交
   → 发送 order.filled {BTCUSDT, LONG, 0.15, 62500}

10. Portfolio Tracker:
    更新持仓: BTC +0.15, 保证金占用更新, peak_equity 更新

11. Monitor:
    记录成交 🟢
    发送 Telegram: "BTCUSDT LONG 0.15 @ 62500 | SL:61500 TP:65000"
```

---

## 5. 现有代码迁移策略

| 代码 | 去向 | 改动 |
|------|------|------|
| `Outlook/*` | `signal_engine/outlook/` | 不改逻辑，加 `run(symbol, ohlcv)` 入口 |
| `Status/*` | `signal_engine/status/` | 同上 |
| `Signal_Generation/*` | `signal_engine/4h/` | 同上 |
| `shared/*` | `shared/` | 直接复用 |
| `Backtest/*` | `backtest/` | 不变，后续单独升级 |
| `Data_pipeline/*` | `data_pipeline/` | 保留用于历史数据、回测；实盘用 Market Data |
| `train_system.py` | 保留 | 用于离线训练，不用于实盘 |

---

## 6. 技术栈

| 层 | 选择 | 理由 |
|----|------|------|
| 事件总线 | Redis Streams | 低频够用，运维简单 |
| 行情 WebSocket | binance-connector-python | 官方 SDK，连接池 |
| 执行 API | python-binance REST | 成熟，文档丰富 |
| 持久化 | SQLite | 单节点，不需要 Postgres |
| K 线缓存 | Redis | 已在依赖中 |
| 并发 | threading + heapq | 参考 Bybit 生产实践，避免 asyncio 复杂 |
| 通知 | python-telegram-bot | Telegram Bot API 包装 |
| 模型推理 | XGBoost（已有） | 不变 |
| RL 框架 | 现有（Stable-Baselines3 等） | 不变 |

---

## 7. 模块间依赖

```
market_data → 无外部依赖（仅 Redis + Binance WS）
scheduler → event_bus, signal_engine (调用)
signal_engine → event_bus, shared（复用特征/标签/模型逻辑）
risk → event_bus, portfolio（读取账户状态）
execution → event_bus, portfolio（写入成交记录）
portfolio → event_bus
monitor → event_bus（订阅所有模块的心跳和事件）
```

**Signal Engine 不依赖 Redis**：它通过函数调用被 Scheduler 触发，输入输出都是 Python 对象。事件发布在 wrapper 层完成。

---

## 8. 目录结构规划

```
Sys_trader/
├── market_data/           # 模块 1
│   ├── ws_pool.py         #   WebSocket 连接池
│   ├── kline_buffer.py    #   K 线缓存
│   └── config.yaml        #   订阅标的清单
├── scheduler/             # 模块 2
│   ├── scheduler.py       #   主调度器
│   └── timeframe.py       #   时间框架规则
├── signal_engine/         # 模块 0 (迁移)
│   ├── outlook/           #   周线层（从 agent_team/Outlook 迁移）
│   ├── status/            #   日线层（从 agent_team/Status 迁移）
│   ├── 4h/                #   4h 层（从 agent_team/Signal_Generation 迁移）
│   └── engine.py          #   统一入口 run(symbol, timeframe, ohlcv)
├── risk/                  # 模块 3
│   ├── chain.py           #   中间件链调度
│   ├── position_sizer.py
│   ├── leverage_ctrl.py
│   ├── drawdown_breaker.py
│   ├── daily_loss_limit.py
│   └── concentration.py
├── execution/             # 模块 4
│   ├── order_manager.py   #   订单生命周期
│   ├── order_gateway.py   #   Binance API 通信
│   └── event_queue.py     #   优先级事件队列
├── portfolio/             # 模块 5
│   ├── tracker.py         #   持仓/权益追踪
│   └── reconciler.py      #   对账逻辑
├── monitor/               # 模块 6
│   ├── collector.py       #   指标收集
│   ├── alerter.py         #   告警规则
│   └── telegram_bot.py    #   Telegram 通知
├── shared/                # 共享基础设施（从 agent_team/shared 复用+扩展）
│   ├── event_bus.py       #   Redis Streams 接口
│   ├── config_loader.py   #   配置加载
│   ├── label_factory.py   #   Triple-Barrier（复用）
│   ├── meta_model.py      #   MetaLabeler（复用）
│   └── sr_features.py     #   支撑阻力（复用）
├── backtest/              # 保留+升级
├── data_pipeline/         # 保留（离线使用）
├── data/                  # SQLite 数据库
├── models/                # 训练好的模型文件（gitignored）
├── config/
│   ├── symbols.yaml       # 标的清单
│   ├── risk.yaml          # 风控参数
│   └── execution.yaml     # 执行参数
├── logs/                  # 运行日志
└── tests/                 # 测试
```

---

## 9. 配置参数（可调）

### 风控参数 (`config/risk.yaml`)

```yaml
risk:
  risk_per_trade: 0.015        # 每笔风险占比 (1.5%)
  max_leverage: 5              # 最大杠杆
  max_position_per_symbol: 0.30  # 单标的最大保证金占比
  max_same_direction: 0.50     # 同方向最大保证金占比
  max_total_margin: 0.80       # 总保证金上限
  max_drawdown: 0.15           # 回撤熔断线
  daily_loss_limit: 0.05       # 日亏损上限
  consecutive_loss_breaker: 3  # 连续亏损熔断笔数
  cooldown_minutes: 120        # 熔断冷却时间
```

### 标的清单 (`config/symbols.yaml`)

```yaml
symbols:
  primary:                     # 始终在列表中
    - BTCUSDT
    - ETHUSDT
    - SOLUSDT
  secondary:                   # 可选标的
    - BNBUSDT
    - DOGEUSDT
    - AVAXUSDT
    - LINKUSDT
    - ARBUSDT
```

---

## 10. 风险与限制

| 风险 | 缓解措施 |
|------|----------|
| 单节点故障 | Monitor 心跳超时告警；Telegram 远程 `/stop` |
| Binance API 异常 | 重试 + 指数退避；限频处理；testnet 先行 |
| 模型漂移 | Monitor 追踪信号准确率；定期回测评估 |
| 极端行情 | markPrice 触发止损；熔断机制；低杠杆 |
| 系统 bug | testnet 充分测试；paper trading 阶段；渐进上线 |

---

## 11. 构建顺序

```
Phase 1: 基础设施
  1.1 shared/event_bus.py (Redis Streams)
  1.2 目录结构 + 配置文件

Phase 2: 数据通道
  2.1 market_data/ (WebSocket + K线缓存)
  2.2 monitor/ (基础心跳 + 日志)

Phase 3: 信号适配
  3.1 signal_engine/ 迁移 (Outlook/Status/4h)
  3.2 scheduler/ (触发逻辑)

Phase 4: 执行链路
  4.1 execution/ (先在 testnet)
  4.2 portfolio/

Phase 5: 风控
  5.1 risk/ 中间件链

Phase 6: 联调
  6.1 集成测试 (paper trading)
  6.2 testnet 端到端
  6.3 monitor/ 告警完善

Phase 7: 上线
  7.1 实盘资金渐进 (从小额开始)
  7.2 24h 监控观察
```

---

## 12. 实现现状与设计差异（2026-08-16 审计同步）

> 本架构为 2026-07-04 初始设计。落地过程中做了多项工程取舍，以下差异以**代码现状为准**：

| 设计 (本文) | 现状 | 说明 |
|------|------|------|
| 通知渠道 Telegram + 命令交互 | **钉钉 webhook 单向告警** (monitor/dingtalk.py + tools/*_watchdog.py) | Telegram 未实现；告警统一 `[SysTrader]` 关键词前缀 |
| 风控链 5 中间件（含 LeverageController） | 5 中间件 ✓（2026-08-16 补上杠杆检查 risk/leverage.py） | 此前只有 4 件套 |
| 对账周期 2 分钟 | 300 秒 | shared/reconciler.py `_CHECK_INTERVAL=300` |
| 交易频率 低频 4h（0-3 笔/天） | 默认 `scalping_15m`（EMA 交叉测试策略） | 四层 agent_team 信号引擎迁移为**独立待办** |
| 事件总线为"唯一媒介" | 旁路埋点 | feed → runner 直连回调；signal/order/position/heartbeat 流走 EventBus |
| 止损/止盈用普通 STOP/TAKE_PROFIT_MARKET | **Algo Order API** `/fapi/v1/algoOrder` | 条件单端点（testnet/实盘均已验证） |
| scheduler/ 线程池调度 | 未接线（feed 直连 `runner._on_kline_closed`） | 保留为简化取舍 |
| execution/event_queue.py 优先级队列 | 未实现 | YAGNI（低频系统无排队需求） |
| ws_pool.py 连接池 round-robin | 未使用（feed 用 combined stream + 4 冗余连接） | 死代码已删（2026-08-16） |
| shared/label_factory、meta_model、sr_features | 未迁移 | 与四层信号引擎同属独立待办 |
| Telegram /stop /pause /resume 远程命令 | dashboard 控制台命令 + `redis-cli XADD` | kill switch 接线已落地（command 流） |
| Monitor 6 条阈值表循环 | 部分（margin_ratio/drawdown + 心跳看门狗） | 连续亏损/日亏/断连阈值由 tools/heartbeat_watchdog 外部覆盖 |

