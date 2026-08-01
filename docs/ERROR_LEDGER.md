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
