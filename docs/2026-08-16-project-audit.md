# 2026-08-16 项目全面审计与修复记录

> 触发: "查看当前项目信息和记忆，找出缺失与不合理之处并解决"。
> 方式: 读取 agentmemory 项目记忆（C:\Users\Evan\.agentmemory\standalone.json, 6 条记忆）+ 4 路并行审查
> （核心交易链路 / 行情信号监控 / 文档一致性 / 仓库卫生）+ 全部修复 + 回归测试。

## 一、记忆要点（来自 agentmemory）

- 24h 稳定性测试自 08-15 15:17 运行中（PID 26296），截至审计时 t≈9.4h，ws=8/8，closes=102，sig=6，order=4/0，stalls=0；08-16 15:17 结束，结束后 nssm start SystraderService。
- 已修复的历史大坑：kline 闭合 0 次（feed testnet 端点/备用连接写 buffer）、-1021 根因（代理节点延迟尖峰 + w32time 时钟漂移）、代理池健康检查死循环、健康检查 URL 与交易 endpoint 不一致（8ef6e55）。
- 环境: Memurai 4.1.2 (Redis 7.2.5)、Anaconda E:\Anaconda3、Dell Inspiron 7591 仅支持 S0 现代待机（跑长测试不能关屏）、nssm 服务需 UAC 提权、钉钉告警必须 [SysTrader] 前缀。
- 已知待办: 四层信号引擎（agent_team）迁移、clientOrderId 幂等键、回测引擎（YAGNI 明确不做）。

## 二、发现并修复的问题

### 🔴 安全（密钥泄露面）

| 问题 | 处理 |
|------|------|
| `config/.env1` 含真实 Binance 密钥被 git 跟踪 | `git rm --cached` + .gitignore `config/.env.*`；**建议轮换密钥（历史中已泄露）** |
| `.claude/settings.json` 含真实 ANTHROPIC_AUTH_TOKEN (DeepSeek API key) 被跟踪 | `git rm -r --cached .claude` + .gitignore；**建议轮换该 token** |
| `.codegraph/daemon.pid`、`.superpowers/**/server.pid` 运行时文件入库 | `git rm --cached` |
| `tools/test_api_key.py` 硬编码 sensenova 密钥 | 删除 |
| `.dockerignore` 未排除 config/.env*（Docker COPY 会把密钥打进镜像层） | 补全忽略规则 |

### 🔴 正确性 bug（代码）

| Bug | 修复 |
|-----|------|
| 停滞熔断死代码: `get_last_price` 缓存价永不为 None → 熔断永不触发 | feed 增加 `_last_update_ts`/`get_last_update_ts`；`_check_stall` 按"最后消息年龄"判定 |
| KlineBuffer 乱序写入: 重连补发的过期 candle append 到末尾破坏序列 | `add()` 单调性保护（同窗定位替换 / 过期丢弃），返回 bool；feed 丢弃时跳过闭合回调 |
| 信号在未闭合 forming K 线上求值 | runner `_on_kline_closed` 过滤 `is_closed`；engine.run 防御性过滤 |
| 撤单错误响应默认 CANCELED（BUG-003 复发） | gateway `_status_or_fail()` 偏向失败；`_cancel_one_order` 非 CANCELED 一律告警 |
| HTTP 5xx/非 JSON 响应静默丢单（代理故障伪装成业务拒单） | 抛 RequestException/HTTPError 走外层 @retrier |
| EventBus 消费失败立即 ACK（"重试一次"承诺落空） | `_deliver` 失败重试一次再 ACK |
| paper 模式 SL/TP 永不触发（模拟持仓无保护） | PaperTrader 条件单挂起 + `poll_conditionals()` 按 markPrice 触发；OrderManager 同步状态 + 发布 order.filled；runner 主循环轮询 |
| PortfolioTracker 多线程竞态 | RLock；发布移出锁；日切重置改 date 比较（跨月漏重置） |
| PM2 dashboard 秒退（server.py 无 `__main__`） | 补 `__main__` 入口 + ecosystem 补 max_restarts/exp_backoff |
| tickSize 硬编码 0.10（SOL/ETH 精度错、PRICE_FILTER 拒单风险） | exchangeInfo 拉取 tickSize + OrderManager 按 symbol 对齐入场/SL/TP；内置兜底档位 |
| Alerter 告警风暴 + 列表无界 + 属性缺失炸循环 | 同 metric 60s 节流、上限 500、getattr 防御 |
| DingTalk markdown 无 [SysTrader] 前缀（310000 拒绝） | title 统一前缀 |
| Dashboard 事件循环被同步 HTTP 阻塞 / collect 异常杀广播 | TTL 缓存（10s）+ try/except |
| OrderManager `_orders` 无限增长 | `_prune_terminal()` |
| runner stop() 不 join 线程、sys.exit 位置 | 补 join/event_bus.stop，sys.exit 移入信号处理器 |
| feed 每 symbol 无新鲜度追踪（停滞检测依赖） | 见停滞熔断项 |
| 杠杆风控中间件缺失（架构 §3.4.2 第 5 环） | 新增 `risk/leverage.py` + runner 装配 + `Signal.leverage` + `MAX_LEVERAGE` 环境变量 |
| 实盘模式 stepSize 从 testnet 拉取 | `_fetch_exchange_filters` 按 testnet 标志选 base_url + 复用 gateway.proxies |
| `market_data/ws_pool.py` 死代码（订阅清单错误: 缺 15m/1h、markprice 后缀错） | 删除 + 删 test_ws_pool.py |

### 🟡 配置 / 依赖 / 文档

| 问题 | 处理 |
|------|------|
| requirements.txt 缺 pandas/pydantic/websocket-client/psutil，且含全仓库未用的 `websockets` | 重写（CI/Docker 此前必挂） |
| `config/.env.example` 过时（TELEGRAM 变量、缺 PROXY/MAX_LEVERAGE/RECV_WINDOW） | 重写 |
| RUNBOOK 未提 docker 启动方式、dashboard 后端启动方式缺失 | 补全 + 说明容器只跑 dashboard |
| 架构 spec 与现状多处冲突（Telegram vs 钉钉、4 vs 5 中间件、对账周期、低频 vs 15m、事件总线旁路） | 架构文档追加 §12「实现现状与设计差异」表 |
| ERROR_LEDGER 缺本期条目 | 追加 BUG-008~015、OPS-004/005 + 批量加固表 |
| 根目录/tools 一次性脚本 10 个、2 个 CC-Switch zip | 脚本删除；zip 保留（用户工具，gitignore 已覆盖）并提示可自行清理 |

### 明确不做（记录在案）

- 四层信号引擎迁移（agent_team Outlook/Status/4h）— 大工程，独立待办
- clientOrderId 幂等键 — gateway 注释已留待办，低频策略风险可控
- scheduler/event_queue/Telegram bot 接线 — 架构取舍已文档化
- 回测引擎 — 原 spec 明确 YAGNI
- .claude/.opencode 个人 AI 工具配置 — 已 gitignore，文件保留在本地

## 三、第二轮修复（核心链路报告落地）

第一轮后核心交易链路报告中仍有未落地项，第二轮补齐（ERROR_LEDGER BUG-016~021）：

| 问题 | 修复 |
|------|------|
| 入场成交从不轮询、未成交即登记持仓（幽灵仓/叠仓/裸仓链条） | 成交前不登记持仓；PENDING 时 SL/TP 延后；新增 `OrderManager.sync_entry_fills()`（LIVE 每 10s 轮询 `GET /fapi/v1/order`）+ `place_protection()`；runner `_sync_entry_fills` 成交确认后登记持仓并补挂保护 |
| 无幂等键 + timeout(10s) < recvWindow(15s) → 重试双成交 | 入场单必带 `newClientOrderId` 重试复用；-2010/-2011 时按 origClientOrderId 查回真实状态；请求超时放大到 recvWindow+5s |
| emergency_stop 撤掉 SL/TP → 熔断瞬间裸仓 | 只撤入场单，保护单保留 + 告警；RUNBOOK playbook #6 同步 |
| 对账做空永久误报 + remote_only 永不导入（重启后叠仓风险） | 先比方向再比数量绝对值；remote 携带 entryPrice；三类漂移全处理（导入/对齐/平仓同步） |
| 回撤熔断无冷却无滞回 | 与连亏路径统一进 COOLDOWN |
| SL/TP 无几何校验 / position_size 成功偏向默认 / 主循环裸退 | `validate_protection` 拒非法几何；position_size 缺失拒信号；run_forever try/finally 清理 |

**仍留作已知限制（记录在案）**: 权益口径用 walletBalance（不含未实现盈亏）；close_position 未扣手续费（fee_model 未接线）；stop() 正常停机不撤交易所挂单（重启后由启动对账恢复，比"每次停机撤单"更安全）。

## 四、测试结果

- 全量回归: **415 passed**（唯一失败项 test_heartbeat_publisher 的时序脆弱断言已加固，复跑全绿）
- 环境性失败（与代码无关）: `tmp_path` 类测试在本 DSH 沙箱下因临时目录 ACL 无法运行（用户本机不受影响）；`test_integration_end2end` 5 项需真实 testnet WS（基线即失败，记忆中有记录）
- 新增回归测试: `tests/test_audit_fixes_2026_08_16.py`（乱序 K 线/杠杆/模拟条件单/tick 对齐/告警节流/撤单偏向失败/停滞检测）
- 修改文件全部 `py_compile` 通过

## 五、第三轮（P0-P2 功能补全，用户拍板全做）

对照 freqtrade/Hummingbot/NautilusTrader 功能集补齐（详见 ERROR_LEDGER 第六节）：

- **P0**: 交易所杠杆/持仓模式/保证金模式自动同步（修复"风控按 3x 算、实际账户默认杠杆"的资金风险）；User Data Stream 成交/余额推送（market_data/user_data_stream.py）；手续费计入已实现盈亏 + 资金费监控接线钉钉；权益口径改 totalWalletBalance；全部撤单端点；Telegram 远程控制 + dashboard Force Exit/Cancel All 按钮
- **P1**: 下单前价格保护、余额层对账、postOnly(maker 费率)、订单持久化接线(此前 OrderManager(db=None) 从未落库)+保留策略、交易日志导出工具、密钥权限自检
- **P2**: K线归档(data/kline.db)、orderbook 深度滑点预检、command 流动态参数(setparam)、dashboard /metrics、TCA 滑点分析工具

新文件: market_data/{user_data_stream,kline_archive,orderbook}.py、tools/{telegram_bot,trade_journal,tca}.py、tests/test_p0_p2_features_2026_08_16.py
测试: 全量 462 passed（仅 5 个需真实 testnet WS 的环境性 e2e error）
环境变量新增见 config/.env.example 与 RUNBOOK「账户配置自动同步/远程控制」章节。

## 六、用户待办（需要人工）

1. **轮换密钥**: Binance API key（config/.env1 曾入库）、DeepSeek ANTHROPIC_AUTH_TOKEN（.claude/settings.json 曾入库）——git 历史中已泄露，仅删文件不够。
2. 提交本次变更: `git add -A && git commit`（含 git rm --cached 的索引变更与未提交的 .gitignore 修改）。
3. 24h 稳定性测试 08-16 15:17 结束后: `nssm start SystraderService`（记忆中的原计划）。
4. 代码改动在**下一次重启后生效**——当前运行中的 24h 测试进程加载的是旧代码，无需中断。
