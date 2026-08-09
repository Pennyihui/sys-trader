# EventBus 数据链路 + 统一装配 设计

> 日期: 2026-08-09 | 基于架构: docs/superpowers/specs/2026-07-04-trading-system-architecture.md

## 1. 背景与问题

### 1.1 现状（经代码核实）

1. **两条平级装配，互补不重叠**：
   - `shared/runner.py`（生产入口）: MarketDataFeed + OrderGateway + PortfolioTracker + Idempotency + Preflight + Reconciler。**无策略、无风控** —— 跑起来永远不会产生信号。
   - `tools/stability_test.py`（24h 测试入口）: 同样的核心模块 + SignalEngine + 4 个风控中间件 + OrderManager（**实例化但从未调用，死代码**）+ 稳定性保障（stall/连接检查/网络诊断）。**无幂等、无预检、无对账**。
   - 两装配都不含 EventBus / dashboard / monitor / guardian / scheduler。
2. **EventBus 是纸上架构**：`shared/event_bus.py`（Redis Streams）实现完整，但生产代码零调用，Redis 从未部署。
3. **Dashboard 显示空数据**：`dashboard/server.py` 独立进程自己 new 了空的 `MarketDataFeed(symbols=[])` 和 `PortfolioTracker()`（`server.py:96-99`），真实 feed/portfolio 在主系统进程里。
4. **OrderManager 有测试背书无实战**：5 个测试文件覆盖（生命周期/重试/持久化/e2e），但从未在两个装配中被真正调用；其完整路径（LIMIT 入场 + algo 止损/止盈）无真实 testnet 长时间验证。

### 1.2 目标

1. 建立唯一完整装配入口（SystemRunner），消除两条装配的行为分叉。
2. 首次真正启用架构文档规定的 EventBus（Redis Streams）通信骨干，打通主系统 → dashboard 的真实数据链路。
3. dashboard 显示真实数据（持仓/权益/信号/订单/心跳/行情），进程保持独立。

## 2. 架构决策（含理由）

| 决策 | 选择 | 理由 |
|---|---|---|
| 数据链路方式 | **EventBus 事件驱动**（非内嵌/非快照轮询） | 架构文档 2.2/2.3 规定 Event Bus 为模块通信骨干，事件类型表已定义 `position.changed`/`order.filled`/`heartbeat.*` 等 |
| 事件流范围 | **完整事件驱动**（用户拍板） | 按架构文档事件表接线，而非最小快照 |
| 装配 | **统一装配**（用户拍板） | 两装配互补不重叠，行为分叉风险；单一入口是架构健康性的要求 |
| 执行路径 | **OrderManager 完整路径**（用户拍板） | 符合架构文档"执行引擎"职责（提交/重试/持久化/三态），有测试背书；统一装配即其获得实战验证的机会 |
| 本机部署 | **Windows 直跑 + Memurai**（用户拍板） | docker 在 Windows 是 WSL2 VM 常驻 1.5-2GB 内存，本机 24h 测试/调试直跑更省更稳 |
| Redis 持久化 | **关闭**（不配 RDB/AOF） | 事件流是瞬态数据，丢失无损失，不落盘则纯内存零 IO |

## 3. 统一装配 —— SystemRunner 成为唯一入口

### 3.1 迁入内容（自 stability_test.py）

| 部分 | 说明 |
|---|---|
| 策略层 | `SignalEngine(StrategyRegistry.get(strategy))`，策略名参数化（默认 `scalping_15m`）；import `signal_engine.scalping_strategy` 注册 |
| 风控链 | 4 中间件参数原样迁入：PositionSizer(0.015) / DrawdownBreaker(0.15, 3, 120min) / DailyLossLimit(0.05) / ConcentrationCheck(0.30/0.50/0.80) |
| 执行层 | `OrderManager`（execution_mode 可配置，默认 DRY_RUN，testnet 下显式 LIVE） |
| 信号接线 | `feed.on_kline_closed` 回调：15m 过滤 → engine.run → risk_chain.process → OrderManager.execute_signal → 成交后由装配层更新 portfolio（open_position，沿用 stability_test 做法） |
| 数量对齐 | `align_qty_to_step` + `_fetch_step_sizes` 移入 `execution/`（执行职责），从 stability_test 迁移 |
| 稳定性保障 | `_check_stall` / `_check_connections` / `_network_diag` / snapshot / report 迁入 runner |

### 3.2 保留不动

Preflight 预检、Idempotency 幂等、Reconciler 对账、启动时账户权益同步（preflight 缓存）。

### 3.3 参数化

CLI/env：`--strategy`、`--symbols`、`--execution-mode`（dry_run/paper/live）、`--hours`、`--testnet`。

### 3.4 策略层定位

当前 `scalping_15m` 是**临时测试策略**（仅用于验证链路）。策略层基于 IStrategy + StrategyRegistry 可插拔设计，装配不绑定任何具体策略——真实策略（agent_team 的 RL 4 层：Weekly XGBoost / Daily XGBoost / 4h RL+贝叶斯）实现为 IStrategy 后，通过 `--strategy` 注入即可，装配零改动。真实策略迁移是独立待办，不在本期范围。

### 3.5 stability_test.py 转型

保留 `python tools/stability_test.py --hours 24` 用法，内部改为构造 SystemRunner 并复用其报告输出。不再维护第二条装配。

## 4. 执行路径 —— OrderManager

- `OrderManager.execute_signal()`：LIMIT 入场 + algo STOP_MARKET 止损 + algo TAKE_PROFIT_MARKET 止盈，含 trades.db 持久化、重试、三态模式。
- 已确认风险：LIMIT 可能不成交；**实现时先行验证 Algo Order API 在 testnet 的可用性**，随后重新跑 24h 验证。

## 5. EventBus 埋点

### 5.1 事件流

| 事件流 | 埋点位置 | 内容 |
|---|---|---|
| `position.changed` | `PortfolioTracker.open/close_position/update_equity` | 持仓、权益、已实现盈亏 |
| `order.filled` | `OrderManager.submit_*` 成交后 | 订单 id、symbol、方向、状态、均价 |
| `signal.generated` | `SignalEngine.run` 产出 Signal | 策略、方向、symbol、价格、conviction |
| `signal.approved` / `signal.rejected` | `MiddlewareChain.process` 结果处 | 通过/拒绝 + 风控原因 |
| `heartbeat` | runner 新增 `HeartbeatPublisher` 线程（5s 周期读 MetricsCollector）；**本次一并埋点**：各模块关键循环点调用 `MetricsCollector.instance().heartbeat(module)`（feed 消息循环 / reconciler / runner 主循环）——否则 MetricsCollector 为空，dashboard 模块状态全空 | 各模块最后心跳时间 |
| `command`（反向） | dashboard /ws 命令 → publish；SystemRunner 订阅 | `emergency_stop` / `resume` —— kill switch 接线（见测试管道 spec 第 7 节） |

### 5.2 注入与容错

- 构造参数 `event_bus: Optional[EventBus] = None`，None 时静默跳过 —— 现有测试零改动。
- `EventBus.publish` 增加 try/except：Redis 故障仅记日志，不阻塞交易主流程。
- `xadd maxlen=10000` 已有，防内存膨胀。

## 6. Dashboard 进程（独立，PM2 不变）

| 组件 | 改动 |
|---|---|
| `StateStore`（新，dashboard/state_store.py） | 每事件流一个消费线程（`run_consumer` 阻塞循环），维护持仓/权益/信号(≤50条)/订单/心跳副本，线程安全 |
| `DataCollector` | 交易状态改读 StateStore；proxy_pool/network 保持 HTTP 透传 |
| 行情 | 保留 dashboard 自己的 `MarketDataFeed`（symbols 从配置读），订阅 markPrice@1s —— 行情属 Market Data 职责，不走事件流 |
| `server.py create_app` | 构造 StateStore + 启动消费线程 + 真实 symbols feed |
| Redis 不可用 | 页面显示 disconnected 而非崩溃 |

## 7. 部署

### 7.1 Windows 直跑（主路径）

- 安装 **Memurai**（Developer 免费版，单实例），`localhost:6379` 无感，redis-py 直连。
- **关闭持久化**（事件流瞬态，不配 RDB/AOF）。
- 运行方式不变：`python -m shared.runner` / PM2 / `python tools/stability_test.py --hours 24`。
- 新增依赖：仅 Memurai（常驻 ~几十 MB 内存）。

### 7.2 Docker 部署路径（等价部署形态，本机日常不运行）

- `docker-compose.yml` 增加 redis:7-alpine 服务；backend/dashboard/frontend 服务保持。
- `REDIS_URL` 环境变量（默认 `redis://localhost:6379`）；`PROXY_HOST` 环境变量（Windows 直跑 `127.0.0.1`，容器内 `host.docker.internal` 访问宿主机 Clash）。
- 本机不具备 docker 环境（已核实），docker 路径验证以代码层保证 + 后续有环境时冒烟。

## 8. 错误处理

| 故障 | 行为 |
|---|---|
| Redis 不可用（主系统） | publish 失败仅日志，交易不受影响 |
| Redis 不可用（dashboard） | 页面显示 disconnected，不崩溃 |
| 消费线程异常 | `run_consumer` 已有 1s 重试 |
| Algo Order API testnet 不可用 | 实现时先行验证；不可用则止损/止盈降级为仅日志告警（记入订单持久化） |

## 9. 测试计划

1. 现有 195 测试不破坏（注入式埋点保证向后兼容）。
2. 新增：
   - EventBus 集成测试（publish → 消费线程收到）
   - StateStore 单测（喂事件 → 状态更新）
   - test_dashboard.py 适配（mock StateStore）
   - 装配端到端（mock gateway：K线 → 信号 → 风控 → OrderManager → position.changed 事件 → StateStore → WS 推送全链路）
3. 重新跑 24h 稳定性验证（装配已变）。

## 10. 明确不做（YAGNI）

- `kline.closed` / `features.ready` 事件流（dashboard 不需要，Scheduler 未接线）。
- `alert.*` 事件流（已有钉钉执行器，后续再接）。
- LIMIT 入场切换（OrderManager 参数化入口类型，本期默认按 OrderManager 现有行为）。
- 本机 docker 运行（资源开销，见决策表）。
- guardian/scheduler 模块接入（独立待办）。
- 真实策略迁移（agent_team RL 策略实现为 IStrategy，独立待办；本期仅确保装配可插拔）。
