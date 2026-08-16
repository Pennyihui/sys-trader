# 交易工程错题本

> 记录系统开发与运维中踩过的坑，避免重复犯错。
> 每条记录包含：症状、根因、教训、预防。

---

## 一、工程 Bug

### BUG-001: `SignalEngine.run()` 硬编码时间框架白名单，拒绝策略自定义 timeframe

- **日期**: 2026-08-01
- **严重度**: 🔴 高（信号静默丢失）
- **症状**: 稳定性测试跑 9 小时，21 次 K 线闭合检查全部 `sig=0`，但手动验证策略历史数据有 15 个交叉信号。策略从未被执行。
- **根因**: `engine.run()` 开头有 `if timeframe not in ("1w", "1d", "4h"): return None`。`IStrategy` 接口允许策略声明任意 `timeframe`（如 `scalping_15m` 用 "15m"），但执行路径仍假设只有 4 个固定时间框架，传 "15m" 直接返回 None。
- **教训**: **接口与实现必须一致**。扩展接口（IStrategy 自定义 timeframe）后，必须同步检查所有执行路径是否支持新值。任何"白名单"都应从接口定义推导，而非硬编码。
- **预防**: 新增策略时，先验证 `SignalEngine.run(symbol, strategy.timeframe, data)` 能走通；单元测试应覆盖"策略声明任意 timeframe"的场景。
- **修复**: `signal_engine/engine.py` — 策略匹配时优先走策略分析，白名单仅用于无策略时的回退分支。

---

### BUG-002: 数据流订阅清单与策略时间框架脱节

- **日期**: 2026-08-01
- **严重度**: 🔴 高（回调永不触发）
- **症状**: `on_kline_closed` 回调一次都不触发，`closes=0`，但 WebSocket 连接正常、价格在更新。
- **根因**: `MarketDataFeed._build_stream_url()` 只订阅了 `kline_4h/1d/1w/markPrice/aggTrade`，**没有订阅 `kline_1h`（后来还有 `kline_15m`）**。策略改用 1h/15m 后，K 线闭合事件根本收不到。
- **教训**: **策略的时间框架必须体现在数据订阅清单里**。改策略前先确认 feed 订阅了对应 timeframe 的 stream。
- **预防**: `MarketDataFeed.backfill()` 与 `_build_stream_url()` 应共享同一个时间框架清单；新增策略时检查 feed 是否订阅。
- **修复**: `market_data/feed.py` — 订阅清单加入 `kline_1h`、`kline_15m`；`_timeframe_from_interval` 映射补全。

---

### BUG-003: 订单被拒误报为成功（AlgoOrderResponse 默认状态）

- **日期**: 2026-07-27
- **严重度**: 🔴 高（仓位无保护）
- **症状**: 止损/止盈条件单被交易所拒绝，但代码返回 `status="NEW"`，系统以为已挂单。
- **根因**: `place_algo_order` 解析响应时 `result.get("algoStatus", result.get("status", "NEW"))` — 响应缺字段时默认 `"NEW"`（成功态），应默认 `"REJECTED"`。
- **教训**: **外部 API 响应的默认值要偏向"失败"**，宁报错不误报成功。金融系统里"以为有保护实际没有"比"多报一次错"危险得多。
- **修复**: 默认值改为 `"REJECTED"`。

---

### BUG-004: TP2 超卖导致反向开仓

- **日期**: 2026-07-27
- **严重度**: 🔴 高（资金风险）
- **症状**: TP1 平掉 50% 仓位后，TP2 仍按原始仓位全量卖出，可能形成反向持仓。
- **根因**: `PositionState` 未追踪已平仓量（`closed_qty`），TP2 计算用 `pos.quantity`（未更新的原始值）。
- **教训**: **部分平仓后，本地状态必须追踪剩余量**。任何"分批"操作都要显式维护已执行部分。
- **修复**: `PositionState.closed_qty` 字段，`_exec_tp_tier` 用 `pos.quantity - state.closed_qty` 计算剩余。

---

### BUG-005: 条件单丢失重试逻辑

- **日期**: 2026-07-27
- **严重度**: 🟡 中（瞬时故障丢单）
- **症状**: 止损/止盈条件单从 `_place_with_retry`（3 次重试）改成裸调用 `gateway.place_algo_order()`，网络抖动直接丢单。
- **根因**: 重构时只改了调用路径，没同步保留重试。
- **教训**: **重构时行为不变是底线**。改动调用路径后必须检查原重试/错误处理是否保留。
- **修复**: 新增 `_place_algo_with_retry`。

---

### BUG-006: `_on_conn_open/close` 无边界检查（stop 竞态）

- **日期**: 2026-07-27
- **严重度**: 🟡 中（偶发 IndexError）
- **症状**: `stop()` 清空 `_conns` 后，pending 的 close callback 访问 `_conns[conn_id]` 触发 IndexError。
- **根因**: callback 无边界检查。
- **教训**: **回调/异步路径访问共享列表必须防御性检查**（尤其涉及生命周期终止时）。
- **修复**: `if conn_id < len(self._conns)` 守卫。

---

### BUG-007: `state.connected` 被重置（重连竞态）

- **日期**: 2026-07-27
- **严重度**: 🟡 中（误判无备用连接）
- **症状**: 快速重连时 `_on_conn_open` 设置 `connected=True`，随后 `_run_conn` 的 `state.connected = False` 覆盖，主连接切换误判"无可用备用"。
- **根因**: `run_forever()` 返回后的重置语句与 open callback 竞态。
- **教训**: **同一状态只由一个路径写入**。open callback 已负责设置 connected，主循环不应再写。
- **修复**: 删除 `state.connected = False` 重置行。

---

## 二、运维问题

### OPS-001: 多个测试进程重复运行，日志统计互相干扰

- **日期**: 2026-08-01
- **严重度**: 🟡 中（测试结果不可信）
- **症状**: 日志里 `t=325m` 和 `t=536m` 两个实例交替输出，`closes` 统计混乱（9 小时 21 次 vs 预期 108 次），无法判断是系统问题还是进程冲突。
- **根因**: 重启测试时旧进程未杀干净（`taskkill /F /IM pythonw.exe` 在 Git Bash 下因路径转义失败），新进程又启动，形成 4 个并行实例。
- **教训**:
  1. **启动长驻进程前，先确认没有旧实例**（`wmic process where "name='pythonw.exe'" get commandline`）
  2. **后台进程管理要幂等**：启动脚本应先检查 PID 文件/进程列表，重复启动直接拒绝或先杀旧进程
  3. **日志要带进程标识**：多实例时区分不了谁写的
- **预防**: 写一个 `tools/start_stability_test.py` 包装器，启动前自动清理旧实例；日志文件名带上 PID。
- **修复**: PowerShell `Stop-Process -Name pythonw -Force` 清理。

---

### OPS-002: `--hours` 参数不接受小数，快速验证不便

- **日期**: 2026-08-01
- **严重度**: 🟢 低
- **症状**: 想跑 10 分钟验证（`--hours 0.17`）报错 `invalid int value`。
- **根因**: argparse 类型声明为 `int`。
- **教训**: 测试工具的参数要支持小数小时/分钟级验证，不能只有小时粒度。
- **修复**: 改为 `float`。

---

### OPS-003: 稳定性测试用 `timeout 600` 包一层，10 分钟自动被杀

- **日期**: 2026-08-01
- **严重度**: 🟢 低
- **症状**: 试跑 `timeout 600 python tools/stability_test.py`，进程 10 分钟被 SIGTERM 杀掉，日志停在 t=9m。
- **根因**: 用 `timeout` 命令限时，忘记这是长时间测试。
- **教训**: 长时间运行的测试不要用 `timeout` 包；用 `--hours` 参数自己控制时长，或直接后台运行。
- **修复**: 移除 timeout，用 `pythonw` 后台运行。

---

## 三、通用教训

1. **接口扩展必须全链路验证** — 改 `IStrategy` 接口后，`engine.run()`、`feed` 订阅、backfill、测试全部要跟上（BUG-001/002）。
2. **金融系统默认值偏向失败** — API 响应缺字段时默认"拒绝/失败"，不要默认"成功"（BUG-003）。
3. **部分操作必须追踪已执行量** — 分批平仓要有 `closed_qty`（BUG-004）。
4. **重构保留原行为** — 改调用路径时检查重试/错误处理（BUG-005）。
5. **异步回调访问共享状态要防御** — 生命周期边界（stop）是竞态高发区（BUG-006/007）。
6. **长驻进程管理要幂等** — 启动前清理旧实例，日志带标识（OPS-001）。

---

## 四、2026-08-16 全项目审计批次

> 四路并行审查（核心交易链路 / 行情信号监控 / 文档一致性 / 仓库卫生）+ 修复。
> 以下条目均已修复，记录供回归参考。

### BUG-008: 停滞熔断是死代码（get_last_price 永不为 None）

- **严重度**: 🔴 高（熔断防线空转，行情死亡无感知）
- **症状**: `_check_stall` 以 `get_last_price(sym) is None` 判停滞；缓存价收到过一笔 aggTrade 就永不为 None，WS 断连后价格静止**永不触发** STALE 分支。
- **根因**: 用"缓存值是否存在"代替"数据流是否存活"。
- **教训**: 判断新鲜度必须用时间戳，不能用缓存值。
- **修复**: feed 增加 `_last_update_ts`（每 symbol 最后行情消息时间）+ `get_last_update_ts()`；`_check_stall` 按时间戳前进与否判定。

### BUG-009: KlineBuffer 乱序写入破坏序列

- **严重度**: 🔴 高（数据完整性根基）
- **症状**: 备用连接重连窗口补发过期闭合 candle，`add()` 无条件 append，`_latest` 指向旧 candle → 指标/信号基于乱序数据。
- **修复**: `add()` 增加 open_time 单调性保护：过期数据按 open_time 定位替换同窗行，无同窗行则丢弃（返回 False）；feed 对被丢弃的 candle 不触发闭合回调。

### BUG-010: 信号在未闭合 K 线上求值

- **严重度**: 🔴 高（闭合语义 off-by-one）
- **症状**: 闭合回调触发时备用连接已写入新一根 forming candle，`analyze()` 用 `df.iloc[-1]` 在部分数据上出信号。
- **修复**: runner `_on_kline_closed` 过滤 `is_closed` 行；engine.run 对带 `is_closed` 字段的记录做防御性过滤。

### BUG-011: 撤单状态默认 CANCELED（BUG-003 复发）

- **严重度**: 🔴 高（撤单失败误报成功 → 以为撤了实际还挂着）
- **症状**: Binance 错误响应体 `{code, msg}` 无 status 字段，`cancel_order`/`cancel_algo_order` 默认 CANCELED，撤单失败静默。
- **修复**: gateway `_status_or_fail()`：缺 status 且 code 非 0/200 → REJECTED；`_cancel_one_order` 非 CANCELED 一律告警。

### BUG-012: HTTP 5xx / 非 JSON 响应静默丢单

- **严重度**: 🔴 高（瞬时代理故障伪装成业务拒单）
- **症状**: 代理 502/SSL EOF 返回非 JSON body，`_request` 返回 `{}` → `place_order` 状态默认 REJECTED 且 error=None，看起来像业务拒绝、无重试。
- **修复**: 非 JSON → 抛 RequestException；5xx → 抛 HTTPError；均触发外层 @retrier 重试，耗尽后由调用方记 ERROR。

### BUG-013: EventBus 消费失败即 ACK（"重试一次"承诺落空）

- **严重度**: 🟡 中（signal/order 事件至少一次语义被破坏）
- **修复**: `_deliver` 失败后立即重试一次（0.2s），仍失败再 ACK；跨崩溃滞留由 XAUTOCLAIM 兜底。

### BUG-014: paper 模式 SL/TP 永不触发

- **严重度**: 🔴 高（模拟持仓无保护、shadow 统计失真）
- **症状**: 模拟条件单返回 NEW 后全系统无触发机制，止损止盈永远不执行。
- **修复**: PaperTrader 挂起条件单（`_pending_conditionals`）+ `poll_conditionals()` 按 markPrice 触发（STOP/TAKE_PROFIT 语义与 Binance 一致）；OrderManager `poll_paper_conditionals()` 同步 ManagedOrder 状态并发布 order.filled；runner 主循环 PAPER 模式每轮轮询。

### BUG-015: PM2 dashboard 秒退

- **严重度**: 🟡 中（dashboard 后端起不来）
- **症状**: `ecosystem.config.js` 用 `python dashboard/server.py`，但 server.py 无 `__main__` 块 → 进程启动即退出 → PM2 循环重启。
- **修复**: server.py 补 `__main__`（等价 uvicorn.run）；ecosystem 补 max_restarts/exp_backoff。

### 批量加固（同批次）

| 项 | 修复 |
|----|------|
| 杠杆风控缺失（架构 §3.4.2 第 5 中间件） | 新增 `risk/leverage.py`，runner 风控链装配；`Signal.leverage` 随策略下发；`MAX_LEVERAGE` 环境变量（默认 5） |
| tickSize 硬编码 0.10 | OrderManager 按 symbol tick 对齐入场/止损/止盈（exchangeInfo PRICE_FILTER 拉取，内置 BTC 0.10 / ETH 0.01 / SOL 0.001 兜底）；`round_price` 8 位小数防浮点尾差 |
| PortfolioTracker 多线程竞态 | RLock + 发布移出锁；日切重置改 date 比较（跨月同 day 漏重置） |
| Alerter 告警风暴 + 列表无界 | 同 metric 60s 节流；`_alerts` 上限 500；check_thresholds 属性缺失防御 |
| DingTalk markdown 无关键词前缀 | `send_markdown` title 统一 `[SysTrader]` 前缀 |
| Dashboard 事件循环阻塞/无保护 | DataCollector 外部服务状态 TTL 缓存（10s）；广播循环 collect 异常不再杀死任务 |
| OrderManager `_orders` 无限增长 | `_prune_terminal()` 归档终态单（保留活跃 + 最近 500） |
| runner stop 清理不完整 | 补 event_bus.stop / command 线程 join；sys.exit 移入信号处理器 |
| feed 的 5xx 处理、`healthy` 硬编码 BTCUSDT | 见 BUG-012（healthy 保留，属稳定接口语义） |

### OPS-004: config/.env1 真实密钥被 git 跟踪

- **严重度**: 🔴 高（密钥泄露面）
- **修复**: `git rm --cached config/.env1`；.gitignore 改 `config/.env.*`（!config/.env.example）；**Binance 密钥已在历史中，建议轮换**。
- **同批**: `git rm --cached .codegraph/daemon.pid`、`.superpowers/**/server.pid`（运行时文件入库）；删除 `tools/test_api_key.py`（硬编码 sensenova 密钥）及 9 个一次性诊断脚本。

### OPS-005: requirements.txt 缺依赖，CI 必挂

- **症状**: GitHub Actions 装 requirements 后跑 pytest，`import pandas/pydantic/websocket-client` 全部失败。
- **修复**: 补 pandas/pydantic/websocket-client/psutil/pytest；删除全仓库无引用的 `websockets`（代码用的是 websocket-client）。
- **同批**: .dockerignore 补 config/.env*、logs/、models/ 等防密钥进镜像层。

---

## 五、2026-08-16 第二轮（核心链路报告落地）

> 第一轮修复后，核心交易链路审查报告中仍有未落地项，本轮补齐。

### BUG-016: 入场成交从不轮询，未成交即登记持仓（幽灵持仓）

- **严重度**: 🔴 高（系统性）
- **症状**: LIMIT 入场 PENDING 时无条件 `open_position` → ①超时撤单被持仓豁免跳过、入场单永挂；②去重逻辑永久挡掉该 symbol 新信号；③对账把幽灵仓按现价平仓计入盈亏，可能误触发连亏熔断；④SL/TP 在成交前挂出，价格先触 TP 时 reduce_only 无仓可减被拒 → 成交后裸仓。
- **修复**: 成交前不登记持仓；`execute_signal` 仅在入场 FILLED/PARTIALLY_FILLED 时立即挂 SL/TP，PENDING 延后；新增 `OrderManager.sync_entry_fills()`（LIVE 模式每 10s 轮询 `GET /fapi/v1/order`）+ `place_protection()`；runner `_sync_entry_fills` 确认成交后登记持仓并补挂 SL/TP。

### BUG-017: 无幂等键 + 请求超时 < 签名窗口 = 重试双成交

- **严重度**: 🔴 高（资金风险）
- **症状**: requests timeout=10s < recvWindow=15s，代理延迟尖峰时客户端超时抛错，@retrier 用新 timestamp 重发同一订单 → 双成交窗口。
- **修复**: 入场单必带 `newClientOrderId`（重试复用同一 id，交易所去重）；`_recover_by_client_id`：返回 -2010/-2011 时按 origClientOrderId 查回真实订单状态；请求超时放大到 `recvWindow + 5s`。

### BUG-018: emergency_stop 撤掉保护单 → 熔断瞬间持仓裸奔

- **严重度**: 🔴 高
- **修复**: `_cancel_active_orders` 只撤 LIMIT 入场单，SL/TP 保护单保留并告警；RUNBOOK playbook #6 同步。

### BUG-019: 对账把做空持仓永久误报 + 远端持仓永不导入

- **严重度**: 🟡 中（对账失明 + 重启后叠仓风险）
- **症状**: 远端 positionAmt 做空为负、本地 qty 恒正，`abs(qty-local)>0.0001` 恒真 → 每 5 分钟误报；runner 只处理 local_only，remote_only/qty_mismatch 永不修正。
- **修复**: 先比方向再比数量绝对值；remote 数据携带 entryPrice；`_on_reconcile_drift` 三类漂移全处理（local_only 平仓同步 / remote_only 导入持仓 / qty_mismatch 对齐交易所）。

### BUG-020: 回撤熔断无冷却无滞回

- **修复**: 回撤触发与连亏触发统一进 COOLDOWN（原实现回撤一恢复立即开仓）。

### BUG-021: SL/TP 无参数校验 + 主循环裸退 + 成功偏向默认值

- **修复**: `OrderManager.validate_protection` 几何校验（LONG: SL<入场<TP；SHORT 反之），违规拒整单；`_execute_signal` 的 position_size 缺失不再默认 0.001（拒信号）；`run_forever` try/finally 清理（report + stop 兜底）。

---

## 六、2026-08-16 第三轮（P0-P2 功能补全）

> 对照 freqtrade/Hummingbot/NautilusTrader 等开源项目功能集补齐系统能力。

### P0（生产必备）

| 项 | 实现 |
|----|------|
| 交易所杠杆从未设置 (BUG-022: 风控按 3x 算保证金, 实际是账户默认杠杆) | gateway `change_leverage`/`get_position_mode_dual`/`set_margin_type`; runner `_sync_account_config` 启动同步 (杠杆=策略值, ISOLATED, 双向持仓 ERROR) |
| User Data Stream 缺失 (10s 轮询) | `market_data/user_data_stream.py` (listenKey + ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/保活/重连); runner 接线, 推送与轮询双通道 `_register_fills` |
| 手续费/资金费未入盈亏 | PortfolioTracker `fee_rate` (往返 0.1%) 计入已实现盈亏与连亏; FundingRateMonitor 接线 (8h, 钉钉告警) |
| 权益口径 walletBalance 不含未实现 | `_refresh_equity`/preflight/initialize 统一改 `totalWalletBalance` |
| 无全部撤单 | gateway `cancel_all_open_orders` + Telegram/Dashboard `/cancelall` |
| 远程控制缺失 | `tools/telegram_bot.py` (status/positions/pause/resume/stop/forceexit/cancelall); runner `force_exit`(市价平仓+撤保护+本地同步)/`pause`/`setparam`; dashboard Controls 加按钮 |

### P1（重要）

| 项 | 实现 |
|----|------|
| 下单前价格保护 | `MAX_ENTRY_DEVIATION` (默认 0.5%) 偏差超阈值拒绝 |
| 余额层对账 | reconciler `balance_drift` (权益差 > 2 USDT 且 > 2%) |
| postOnly | `POST_ONLY=1` 入场单走 LIMIT_MAKER (maker 费率) |
| 订单持久化从未接线 | runner 注入 TradeDatabase (OrderManager/PaperTrader), 保留策略 `purge_orders/signals` |
| 资金费告警 | 同 P0 接线 |
| 交易日志+绩效统计 | `tools/trade_journal.py` (CSV 导出 + 汇总; 胜率配对为已知限制) |
| 密钥权限自检 | preflight 提现权限告警 + 权益口径统一 |

### P2（增强）

| 项 | 实现 |
|----|------|
| K线历史归档 | `market_data/kline_archive.py` + feed upsert (`KLINE_ARCHIVE=1`) |
| 深度滑点预检 | `market_data/orderbook.py` (`ORDERBOOK_CHECK=1`, 逐档吃单估算 bps) |
| 动态参数 | command 流 `setparam` (risk_per_trade/max_leverage 热更新) |
| 指标导出 | dashboard `GET /metrics` (MetricsCollector.snapshot) |
| TCA 分析 | `tools/tca.py` (成交价 vs 限价滑点 bps, 按 symbol 汇总) |

---

## 七、2026-08-16 第四轮（运维看板 + 交易看板改造）

### BUG-023: `python dashboard/server.py` 直接运行 ModuleNotFoundError

- **症状**: 直接以脚本方式运行 dashboard/server.py（或 PM2 `script: dashboard/server.py`）
  报 `No module named 'dashboard'`——脚本模式 sys.path[0] 是脚本所在目录而非项目根。
  上一轮只补了 `__main__` 入口，未解决 import 路径，PM2 入口仍会崩。
- **修复**: server.py 顶部 `sys.path.insert(0, 项目根)`，`python dashboard/server.py`
  与 `python -m dashboard.server` 等价（RUNBOOK 已注明）。

### 运维看板（OpsDashboard，参考 FreqUI + Grafana 模式）

- `dashboard/ops_archive.py`: heartbeat/command 流 → SQLite (data/ops_history.db,
  7 天保留)，Redis Stream maxlen 只留 ~14h，归档后看板可看 7 天历史
- 后端新 API: /api/ops/summary、/api/ops/history?hours=、/api/ops/commands、/api/ops/soak
  （soak 读 logs/soak_metrics.csv）
- 前端双页签: 交易 / 运维；运维页含 4 张 SVG 时序图（零依赖 charts.tsx）、
  模块心跳秒龄、代理池/网络状态、运维事件时间线
- `tools/soak_watchdog.py` 补 runner PID 自动解析（原缺省监控自身 RSS，曲线无意义）
- 交易看板改造: 加回撤 KPI、保证金率分级配色、中文标签

### BUG-024: `_request` 无 PUT 分支 → listenKey 保活发成 GET

- **症状**: User stream keepalive 每 30 分钟必失败（`PUT /fapi/v1/listenKey`
  实际发的是 GET）→ 频繁换 key 重连。
- **修复**: `_request` 补 PUT 分支（requests.put）。

### BUG-025: 对账把"账户接口失败"误判成"持仓消失"（假平仓风暴）

- **严重度**: 🔴 高（反复假平仓+重导入，污染盈亏统计）
- **症状**: 代理抖动时 `get_account` 返回 error/异常 → reconciler 拿空 positions
  → local_only 假漂移 → 每 5 分钟"平仓"全部持仓，下轮又 remote_only 重导入；
  实测 04:38/04:53 两轮假平仓产生假已实现盈亏 ≈ -3.4 USDT，dashboard 持仓在
  0/3 之间闪动、保证金率显示 0%。
- **根因**: `_fetch_account` 失败返回 `{}`，空 positions 与"交易所无持仓"语义相同。
- **教训**: 对账系统的"拉取失败"必须与"确认为空"区分——无法确认远端状态时
  跳过本轮，绝不能产生任何漂移结论。
- **修复**: `_fetch_account/_fetch_remote` 失败/响应无效返回 None → `reconcile`
  跳过本轮（drift=False、不触发 on_drift、保留本地状态）；startup_reconciler 同修。

### 运维看板 Vite 代理缺口

- **症状**: 运维页全部 fetch 报 `SyntaxError: Unexpected token '<'`（Vite 返回
  SPA HTML 而非后端 JSON）。
- **修复**: vite.config.ts 补 `/api` 与 `/metrics` 代理 → :8000。
- **同批**: 前端界面全面汉化（模块状态修正为真实心跳模块：行情数据/主运行器/
  对账器，其余模块说明在运行器进程内无独立心跳）。

---

## 八、2026-08-16 第五轮（面板二期：交易+运维功能补全）

对照 FreqUI/Hummingbot Dashboard/OctoBot 功能集，用户拍板全做：

| 面板 | 新增 |
|------|------|
| 交易 | 权益曲线（equity_history 归档）、K线蜡烛图+平仓标记（/api/kline）、平仓明细表、绩效统计（胜率/盈亏比/净盈亏/手续费）、危险操作二次确认弹窗、24h 行情条（ticker TTL 60s）、可用余额/已用保证金、信号/订单时间戳、风控参数面板（setparam UI，参数 gauges 经 heartbeat 上报）、浏览器告警通知（保证金率>60%/回撤>10%/订单错误，🔕 授权 + 提示音） |
| 运维 | 告警历史（钉钉发送同步归档到 Redis alert 流）、进程启动/停止历史（lifecycle 流）、WS 连接数趋势、资金费成本曲线、CPU 曲线（soak 加 cpu_pct 列）、时间偏移统计摘要、日志体积卡 |

关键接线: OpsArchive 扩 6 表（equity/trade/alert/lifecycle + heartbeat 新列）;
DingTalkNotifier 发送即归档 alert 流（watchdog 告警全覆盖）; runner 启动/停止
发布 lifecycle、_check_connections 上报 ws gauges、风控参数 gauges;
heartbeat_publisher stats 扩展; soak_watchdog 加 cpu_pct 列（CSV 头变更）。

测试: 后端 138 项通过 (后续全量 492 项, 见第九节); 前端 tsc+vite 通过。注意: 交易进程的 gauge 上报需
runner 重启后生效（不影响交易正确性, 面板显示"—"直到下次重启）。

### BUG-026: dashboard 重启后持仓显示丢失（StateStore 无状态重放）

- **症状**: 每次重启 dashboard 后端, 交易面板持仓数归零、权益卡只剩启动后新事件。
  持仓事件只在开仓/对账漂移时发布, 对账无漂移时面板永远看不到存量持仓。
- **修复**: StateStore.start() 先 `_bootstrap()`——XREVRANGE 重放最近 500 条
  position/signal/order 流事件 (倒序取回按正序处理), 重建持仓/权益/信号/订单状态。
  实测恢复 ETH LONG 0.053 / BTC LONG 0.0045 / SOL SHORT 3.96 + equity 4976.70。
- **教训**: 事件驱动的展示层必须支持启动重放或快照, 否则"重启即失忆"。

### 行情条全市场刷屏 (ticker 过滤失效)

- **症状**: 交易面板行情条显示全交易所 400+ 交易对 (含测试网 meme 币)。
- **根因**: `urlencode` 默认 quote_plus 把 JSON 数组内空格编码为 `+`,
  币安忽略 symbols 参数回退全市场; 响应侧又未过滤。
- **修复**: quote_via=quote 编码 + 响应侧白名单双重过滤, 仅返回 DASHBOARD_SYMBOLS。

---

## 九、2026-08-16 第六轮（五路 subagent 全面审查）

五路并行只读审查（核心链路/行情数据/Dashboard 全栈/运维工具链/测试文档），
全部确认问题已修复：

### BUG-027: 数据库跨线程写 SQLite（check_same_thread=True）

- **症状**: 首次真实成交时 user stream 线程 `_persist_result` 抛 ProgrammingError:
  下单前抛=信号静默丢失, 下单后抛=持仓/保护登记丢失 (5 分钟对账兜底, 期间裸仓)。
- **修复**: TradeDatabase `check_same_thread=False` + 全局锁串行化全部读写。

### BUG-028: 撤单网络失败误标 CANCELED（BUG-003/011 复发）

- **修复**: `_cancel_one_order` 仅 CANCELED/REJECTED(未知订单) 才标 CANCELED;
  ERROR(网络/限流) 保持 PENDING 下一轮重试, 本地状态不与交易所脱节。

### BUG-029: 条件单 (SL/TP) 无任何状态跟踪通道（S1）

- **症状**: 保护单在交易所触发平仓后, 系统要到 5 分钟对账才知道, 且对账平仓
  不撤残余保护单 → 旧 TP 可能误平后续新仓。
- **修复**: gateway.get_open_algo_orders; OrderManager.sync_algo_orders (开放清单
  消失=已触发, 10s 轮询); runner._on_protection_triggered 撤残余+同步平仓;
  对账/force_exit 平仓联动撤保护单。

### BUG-030: 延后挂保护未按实际成交价校验几何（S3）

- **修复**: place_protection 用 avg_price 校验 SL/TP 几何, 冲突时拒挂并告警
  (宁裸仓报警, 不挂反向秒损单)。

### BUG-031: force_exit 不撤 PENDING 入场单（S4）

- **修复**: _force_exit_symbol 撤该 symbol 全部活跃单 (保护+入场),
  防价格回踩成交出用户没要的新仓。

### BUG-032: setparam 无效/绕过熔断（S5）

- **修复**: max_leverage 更新实例属性后重建链 (此前读环境变量=no-op);
  重建链继承 DrawdownBreaker COOLDOWN 状态。

### BUG-033: 行情层修复包

| 项 | 修复 |
|----|------|
| 主备切换窗口闭合 K 线回调永久丢失 | feed._replay_missed_closures (升主后补发最近 2 周期内未通知闭合线) |
| feed 消息解析异常杀连接 (重连风暴) | _on_message 整体 try/except |
| 归档 8× 写放大 | 仅主连接 upsert |
| listenKey 保活空参 + ws_url 竞态 | 显式带 key + 锁内读 |
| funding_monitor 线程静默死亡 | 循环级 try/except + 持仓快照 |
| PaperTrader _fills 无界 | 裁剪 2000 |
| EventBus Redis 宕机日志风暴 | 退避 5s |
| dingtalk alert 归档阻塞 | Redis socket_connect_timeout=2s |
| orderbook 除零 | quantity<=0 防护 |

### 安全加固（第六轮）

- proxy_pool /proxies 凭据接口加 Bearer 鉴权 + 移除 CORS *; network_monitor 同去 CORS
- CONTROLLER_SECRET 改环境变量 PROXY_POOL_API_TOKEN 可覆盖
- Telegram bot fail-closed: 未配 TELEGRAM_CHAT_ID 拒绝启动
- dashboard WS 命令通道支持 DASHBOARD_TOKEN 鉴权 (4401)
- .dockerignore 补 proxy_pool cache/data/logs + network_monitor 产物

### 测试/文档修正

- requirements.txt 补 httpx (TestClient); server.py DASHBOARD_AUTOSTART 惰性装配
  (conftest 关闭), 单测不再真连 Redis/WS; test_runner_assembly 补 OrderGateway
  mock (此前每轮 7 次真实 testnet HTTP); StateStore bootstrap 重放窗口 500→10000
  (BUG-026 复发修复); DataCollector 持仓快照锁; soak_watchdog PID 透传 (此前
  修复未生效); --hours 支持小数; RUNBOOK 编码为 UTF-8 (审查误报)。

新增回归测试: tests/test_round5_fixes.py (13 项)。全量 492 passed。
运行状态: 24h 测试最终重启 PID 18944 (06:40, 全部修复后代码)。

### 核心链路报告补充修复（D1-D5，同轮收口）

| 项 | 修复 |
|----|------|
| D1 双通道重复登记 TOCTOU | `_register_fills` 加 `_fills_lock` 串行化 + 重查持仓 |
| D2 部分成交余量永不补登记/补保护 | 两通道 PARTIALLY_FILLED→FILLED 增量进 newly_filled；`place_protection(qty=)` 按增量补挂；已登记持仓只更新数量 |
| D3 平仓后 symbol 被保护单锁死 ≤30min | 入场去重只认 LIMIT 单（保护单 PENDING 不挡新信号） |
| D4 /cancelall 不联动本地状态 | 撤单后本地订单同步置 CANCELED + 告警 |
| D5 共享状态无统一锁 | tracker 加 `positions_snapshot`/`update_position` 锁内方法，reconciler/runner 对齐调用（OrderManager/_orders 锁上轮已加） |
| 集中度硬编码 3x 杠杆 | 拟开仓保证金改按 signal.leverage 实际值 |
| listenKey 过期不主动断连 | 换 key 后主动 ws.close() 强制重连 |
| stop() 双调用 | `_stopped` 幂等保护 |

最终全量: **507 passed, 0 error**。24h 测试最终运行 PID 26928 (06:54 启动)。

### BUG-034: feed 主备切换死锁 → K线闭合永久冻结 (2026-08-16 实测)

- **严重度**: 🔴 致命 (单次 24h 测试 10 小时 0 信号 0 下单)
- **症状**: ws=3/8、价格在更新, 但 closes=0 持续 10 小时, stalls 累到 1.8 万。
- **根因**: `_try_switch_primary` 在持有非重入 `self._lock` 时调用新增的
  `_replay_missed_closures()` (其内部又获取同一把锁) → 死锁。卡死后所有连接
  的 K线闭合处理 (`_on_kline_message` 的 `with self._lock:`) 全部阻塞,
  价格路径 (markPrice, 不碰锁) 不受影响 → 表面"行情正常"实则信号全停。
- **修复**: 补发逻辑移到锁外调用。
- **教训**: 非重入锁内绝不可调用会再取同锁的函数; 新代码必须过并发审查。

### BUG-035: websocket-client 1.8 无 connect_timeout 参数

- **症状**: 给 run_forever 加 `connect_timeout=20` 后所有 WS 连接线程抛
  `unexpected keyword argument` → ws=0/8。
- **修复**: 改用 `http_proxy_timeout=20` (1.8 的代理连接超时参数)。

### BUG-036: soak_watchdog 检测不到 runner PID (日志轮转丢启动行)

- **症状**: "System running" 行只在启动时写一次, 日志轮转后当前文件里没有,
  detect_runner_pid 返回 None → RSS/CPU 曲线全 0。
- **修复**: runner 启动写 `data/runner.pid` (RUNNER_PID_PATH), soak 优先读
  PID 文件, 回退扫当前+轮转日志。

### 运维: proxy_watchdog 未随重启批次启动

- 前几轮重启只启了 test/soak/heartbeat/dashboard, 漏了 proxy_watchdog →
  代理节点延迟超标时无自动切换, 叠加节点退化 (testnet 8.3s) 拖垮整场测试。
- 现已纳入标准启动批次; 代理池手动刷新 `python tools/proxy_pool/proxy_pool.py --generate`。

---

## 附：如何新增条目

新增错误时按以下模板：

```markdown
### BUG-XXX / OPS-XXX: 标题

- **日期**: YYYY-MM-DD
- **严重度**: 🔴/🟡/🟢 + 一句影响
- **症状**: 现象（可复现的描述）
- **根因**: 深层原因
- **教训**: 可迁移的经验
- **预防**: 防止再次发生的具体措施
- **修复**: 修了什么
```

---

## 六、第六轮风控/运营/执行补强 (2026-08-16)

新增功能 (10 项全部落地, 全量测试 534 passed):

- **风控 #1**: 保证金率自动减仓 (MARGIN_DELEVERAGE_THRESHOLD=0.8, 冷却 120s, 关保证金最大持仓, CRITICAL 告警)
- **风控 #2**: 回撤分级响应 (DRAWDOWN_REDUCE_TIER=0.12 减仓档 + 既有 15% 熔断档, 回撤回落 20% 重新武装)
- **风控 #3**: DailyTradeLimit 单日最大交易次数 (MAX_TRADES_DAY=30, 0=禁用)
- **风控 #4**: MaxStopDistance 最大止损距离 (MAX_STOP_PCT=0.05, 0=禁用)
- **运营 #5**: 每日钉钉运营摘要 (DIGEST_HOUR:MINUTE 起 10 分钟窗口, 按日期去重)
- **财务 #6**: 资金费精确对账 (FUNDING_ACCOUNTING=income → /fapi/v1/income + tranId 游标去重, 首跑播种不补历史)
- **告警 #7**: 钉钉 @人 (DINGTALK_AT_MOBILES, CRITICAL 级 send_at)
- **执行 #8**: IOC 入场单选项 (ENTRY_TIF=GTC/IOC, IOC 与 postOnly 互斥)
- **执行 #9**: 部分成交余量策略 (PARTIAL_FILL_POLICY=wait/cancel, 撤单失败保持 PARTIALLY_FILLED 重试)
- **面板 #10**: 持仓表盈亏平衡价 (LONG: entry·(1+f)/(1-f), SHORT: entry·(1-f)/(1+f))

### DSH-001: PowerShell -replace 重写 Python 源文件破坏 UTF-8

- **日期**: 2026-08-16
- **严重度**: 🟡 中 (测试文件编码损坏, SyntaxError)
- **症状**: \(Get-Content x.py) -replace ... | Set-Content x.py -Encoding utf8\ 后中文注释全部变乱码, 字符串字面量被截断产生 SyntaxError。
- **根因**: PowerShell 5.1 Get-Content 默认编码与 -Encoding utf8 (带 BOM) 往返, 非 ASCII 字符被错误解码/重编码。
- **教训**: 源码文件修改一律用工具的文件编辑能力 (edit/write, 保持 UTF-8 无 BOM), 不要用 shell 文本管道重写源码。
- **预防**: 需批量替换时用 python 脚本显式 encoding='utf-8' 读写。
- **修复**: write 工具完整重写测试文件恢复。

### DSH-002: str-Enum 构造器不自动转换字符串

- **日期**: 2026-08-16
- **严重度**: 🟢 低 (测试构造错误)
- **症状**: ExecutionModeManager("live") 后 mode.value 抛 AttributeError。
- **根因**: str 子类 Enum 构造器不把 str 参数强转为成员, "live" 保持普通 str。
- **教训**: 测试构造枚举时显式 ExecutionMode("live"), 或走 from_env()。
- **修复**: 测试改用 ExecutionModeManager(ExecutionMode("live"))。

---

## 七、第七轮: Binance 合约 API 缺口补强 (2026-08-16)

调研来源: Binance 官方衍生品文档 + JKorf/Binance.Net + nautilus_trader 集成对照表 + GitHub 开源 bot (pkdoddamani/Aditya2458)。8 项全做:

- **#1 实际手续费率**: gateway.get_commission_rate → FEE_RATE=auto 时按 2×taker 计往返费率 (本次实测 0.0008, 替代硬编码 0.001), equity 事件携带 fee_rate 供面板保本价同源
- **#2 清算价/爆仓距离**: gateway.get_position_risks (v3) → 60s 同步 → position.risk 事件 (面板清算价/爆仓距离/ADL 列) + LIQ_ALERT_PCT=0.08 内自动减仓
- **#3 ADL/保证金率**: MARGIN_CALL 升级 CRITICAL+@人; adlQuantile>0 每 symbol 告警一次 (退出队列重新武装)
- **#4 workingType**: PROTECTION_WORKING_TYPE=CONTRACT_PRICE(默认)/MARK_PRICE, algoOrder 透传
- **#5 追踪止损**: PROTECTION_SL_MODE=trailing → TRAILING_STOP_MARKET + callbackRate (0.1-5), PAPER/DRY_RUN 自动退化固定止损; 撤单/触发检测/熔断保留清单全部纳入 TRAILING_STOP_MARKET
- **#6 多资产模式**: get_multi_assets_mode 检测 + CRITICAL 告警; **修复可用余额单位混杂 bug** — 原 sum(各资产 availableBalance) 把 BTC 数量与 USDT 金额直接相加, 改为单资产取 USDT/多资产取 totalMarginBalance
- **#7 大额强平**: ForceOrderStream 独立 WS (隔离故障域, 默认关), 名义价值 >= FORCE_ORDER_ALERT_USDT 时 WARNING + 5min 节流
- **#8 限流余量**: 响应头 X-MBX-USED-WEIGHT-1M → api_weight_used gauge

### BUG-037: 可用余额跨资产单位混杂相加

- **日期**: 2026-08-16
- **严重度**: 🟡 中 (可用保证金口径错误, 多资产账户会显著高估/低估下单能力)
- **症状**: _refresh_equity / 启动初始化把 account v2 各资产的 availableBalance 直接 sum — BTC 的 availableBalance 是 BTC 数量 (如 0.01), 与 USDT 金额 (如 4976) 相加, 数量级完全无意义。
- **根因**: 未区分资产单位; account v2 assets[] 每项 availableBalance 以本资产计价。
- **教训**: 跨币种余额求和前必须先确认计价单位; 合约账户应优先用 totalMarginBalance/totalWalletBalance 这类统一口径字段。
- **预防**: 单资产模式取 USDT 条目; 多资产模式 (fapiMultiAssetsMargin=true) 取 totalMarginBalance。
- **修复**: runner._available_balance(acc, multi_assets) 统一口径 + 启动检测多资产模式。

### BUG-038: 佣金费率硬编码导致盈亏口径漂移

- **日期**: 2026-08-16
- **严重度**: 🟡 中 (0.001 与真实 0.0008 差 25%, VIP/BNB 折扣后偏差更大)
- **症状**: tracker.fee_rate / 保本价 / 手续费累计全用 0.001, 与交易所实际扣费不符。
- **根因**: 从未查询 /fapi/v1/commissionRate。
- **修复**: FEE_RATE=auto 启动时查询, 2×taker 计往返, 失败保留 0.001 (fail-safe)。
