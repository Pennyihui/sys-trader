# 测试与验证管道 设计

> 日期: 2026-08-09 | 依赖: [2026-08-09-eventbus-dashboard-design.md](./2026-08-09-eventbus-dashboard-design.md)（EventBus 数据链路 + 统一装配，先于本设计实现）

## 1. 背景与目标

统一装配后，需要一套完整的测试与验证管道（**回测引擎除外**，独立待办），覆盖：

- A. 离线模拟（历史 K 线重放跑真实运行时）
- B. 影子交易（实盘信号 vs 模拟成交对齐）
- C. testnet soak 拉长到 7 天 + 明确验收标准
- D. 实盘分级（小规模 → 分级扩容）
- E. Kill switch 接线（dashboard → 主系统）
- F. 可靠性补缺（429 限速审计 / 断连日志 / 看门狗 / 内存监控）

依据业界实践（RustyBT 就绪清单、Matrixtrak 20 点可靠性清单、GeneTrader 影子门槛、polybot shadow mode）。

## 2. 决策

| 决策 | 选择 | 理由 |
|---|---|---|
| 影子模式 | **双实例并行**（LIVE + PAPER 各自独立决策） | 业界主流（polybot/ml4t/MultiCharts 实测）；单信号喂双路径对齐率恒 100% 无意义 |
| 影子验收 | **信号对齐 ≥95% + 逐笔执行质量**（滑点 vs 到达价、填充率） | 低频系统 1 周 0-21 笔，相关性统计（≥50 笔样本才可靠）不适用，明确不做 |
| 范围 | **A-F 全部纳入本期** | 用户拍板"除回测引擎外都完成" |
| 与数据链路 spec 的关系 | 依赖前置 | B/E 走 EventBus 通道，需先行实现 |

## 3. A. 离线模拟（重放）

**新组件 `ReplayFeed`**（tools/，与 replay_runner.py 同目录——测试支撑组件，不进生产模块）：

- `feed.backfill()` 拉取的历史 K 线持久化到 `data/replay/<symbol>_<tf>.json`
- `ReplayFeed` 实现 MarketDataFeed 的接口（`get_last_price`/`get_mark_price`/`on_kline_closed` 触发），按时间戳重放：逐条触发 `on_kline_closed`，驱动**完整装配**（DRY_RUN 模式）
- 新增入口 `tools/replay_runner.py`：`--data` 指定历史数据目录，`--symbols`、`--strategy`
- **验收**：全量重放无异常；信号/风控拒绝/下单统计输出；**起止 RSS 对比判定无内存泄漏**（增长 > 阈值即失败）
- 价值：分钟级跑完数周数据，验证真实运行时逻辑，可重复

## 4. B. 影子交易（Shadow）

### 4.1 架构（双实例并行）

业界主流做法（[polybot](https://polybot.cryptuon.com/guides/shadow-mode/)、[ml4t-live](https://github.com/ml4t/live)、MultiCharts 实测）：两个独立实例并行，各自独立决策。**同一信号喂双路径（对齐率恒 100%）无意义**——双实例的 tick 传递延迟差异导致的小幅信号偏差是预期现象（[MultiCharts 实测](https://multicharts.com/discussion/viewtopic.php?t=49343&sid=b5398b631a54a1f0c5ce2b9ef7e8e123&view=print)）。

```
实例 A: SystemRunner(LIVE，小仓位实盘) ──┐
                                          ├─→ EventBus（signal.generated 事件带 instance 标识）
实例 B: SystemRunner(PAPER，同参数模拟) ──┘
                                              └─→ ShadowMonitor（tools/shadow_monitor.py）
                                                    双实例信号对齐率 / 逐笔滑点(成交价 vs 到达价) / 填充率
                                                    落盘 JSON 报告
```

- 两个 SystemRunner 实例参数相同（`--instance` 标识），仅 execution_mode 不同：A=LIVE（小仓位）、B=PAPER（PaperTrader）
- 同一 EventBus：`signal.generated` 事件带 instance 字段，ShadowMonitor 按 instance 分流比对
- 风控参数完全一致（apples-to-apples）

### 4.2 验收标准

- 信号对齐 ≥ 95%（双实例 tick 延迟偏差是预期的，100% 反而是异常信号）
- 逐笔执行质量：滑点（成交价 vs 到达价）记录 + 填充率统计（TCA 风格）
- 运行 1 周无系统性偏差
- **明确不做**：收益/胜率/PnL 相关性统计（agent66 式 >0.7）——低频系统 1 周 0-21 笔，达不到相关性统计所需的样本量（≥50 笔才可靠）
- **已知局限**（写进文档）：影子交易测不了市场冲击——影子通过 ≠ 可直接满仓，仍需 D 分级

## 5. C. testnet soak 7 天

- `--hours 168` 运行统一装配（testnet + LIVE 执行模式）
- soak 期间监控：每小时 RSS（内存泄漏判定）、错误计数、风控熔断触发次数、对账漂移
- **验收**：7 天无意外错误、无风控熔断触发、对账零漂移、内存曲线平稳（RSS 波动 < 阈值）

## 6. D. 实盘分级

- `PositionSizer.risk_per_trade` 参数化（CLI `--risk-per-trade`，默认 0.015）
- 分级流程（运营 + 参数旋钮）：实盘第一周 0.2% → 稳定 7 天 → 逐级调回设计值；资金暴露同理（10% → 35% → 65% → 100%）
- 每级验收：7 天无重大事故 + 关键指标（胜率/盈亏比/回撤）与 testnet 一致（±20%）
- 产出物：验收标准文档（runbook 附录）

## 7. E. Kill switch 接线

现状：`server.py:41` 收到 `emergency_stop` 命令仅打日志。

设计（EventBus 反向通道）：

```
dashboard /ws 命令
  └─→ publish command.emergency_stop（新事件流）
        └─→ SystemRunner 订阅 → OrderManager 停止下单 + 撤活跃订单
             + 风控链进入熔断态（拒绝新信号）
command.resume → 解除熔断
```

- 熔断态：风控链在 `MiddlewareChain.process` 前检查，熔断期拒绝一切信号
- dashboard Controls 按钮立即生效（前端已发命令，后端接线即可）

## 8. F. 可靠性补缺

| 项 | 现状 | 补缺 |
|---|---|---|
| 429 限速 | `OrderGateway._request` 有 @retrier(3次) | 审计 retrier 对 429 的退避处理；缺则加（退避 + jitter） |
| 断连日志 | feed 断连有日志 | 补 close_code / last_message_ago_ms / uptime_seconds |
| 看门狗 | PM2 autorestart 已有 | 补 RSS/进程监控脚本（`tools/soak_watchdog.py`，每小时记录 RSS + 错误计数，供 soak/实盘期间使用） |
| 监控可视化 | 钉钉告警已有 | 事件进 dashboard 后状态可视化（依赖数据链路 spec） |

## 9. 验收标准汇总

| 阶段 | 验收标准 |
|---|---|
| A 离线模拟 | 全量重放无异常 + RSS 平稳 |
| B 影子交易 | 信号对齐 ≥95% + 逐笔滑点/填充率记录，1 周无系统性偏差 |
| C testnet soak | 7 天无意外错误/熔断/漂移，内存平稳 |
| D 实盘分级 | 每级 7 天稳定 + 指标与 testnet 一致 ±20% |
| E kill switch | 命令触发 3 秒内停单+撤单+熔断生效（测试验证） |
| F 可靠性 | 429 退避有效、断连日志完整、看门狗正常记录 |

## 10. 明确不做（YAGNI）

- 回测引擎（独立待办，含 walk-forward/成本建模）
- 成交级影子比对（滑点模型，后续按需）
- 高开低走的信号陈旧度分析（低频策略无此问题）
- 实盘阶段的实际执行（本设计只到"验收标准 + 参数旋钮"就绪）
