# 运维手册

> 交易系统生产运维文档。

---

## 系统架构

```
                   ┌─────────────┐
                   │  Dashboard  │  :5173 (React)
                   │  (FastAPI)  │  :8000 (API)
                   └──────┬──────┘
                          │
┌─────────┐  ┌──────▼──────┐  ┌─────────┐
│  Market  │  │  系统主进程  │  │ 日志    │
│  Data    │◄─┤systrader   ├──► JSON    │
│  WS      │  │runner.py   │  │ 轮转    │
└─────────┘  └──────┬──────┘  └─────────┘
                   │
           ┌───────▼────────┐
           │  OrderGateway   │──► Binance API
           │  (testnet/live) │
           └────────────────┘
```

## 前置: 安装 Memurai

Redis 兼容服务（EventBus/StateStore/Dashboard 依赖），默认 localhost:6379。
安装与配置（含关闭持久化）见 [docs/redis-setup.md](docs/redis-setup.md)。

```bash
redis-cli ping   # → PONG 即就绪
```

## 启动

```bash
# PM2 方式 (推荐)
pm2 start ecosystem.config.js

# 手动方式
python -m shared.runner

# Dashboard
cd dashboard/frontend && npm run dev
```

### Dashboard 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| REDIS_URL | `redis://localhost:6379` | Redis 连接串（backend 启动时读） |
| DASHBOARD_SYMBOLS | `BTCUSDT,ETHUSDT,SOLUSDT` | 行情 feed 订阅交易对（逗号分隔） |
| DASHBOARD_INSTANCE | `live` | 只消费该 instance 的事件流 |

### Dashboard 启动行为（import 副作用）

`uvicorn dashboard.server:app`（或任意 `import dashboard.server`）在模块加载时即执行模块级
`app = create_app()`：自动装配 EventBus（连 Redis）→ StateStore（6 个消费线程：
position/order/signal/heartbeat 流）→ MarketDataFeed（4 条 Binance WS 行情线程），
**启动即开始消费与连接**。Redis 不可用时 StateStore 启动失败被捕获（dashboard 降级运行、
无实时状态），feed 线程仍会启动。

## 查看状态

```bash
pm2 status           # 进程状态
pm2 logs systrader   # 实时日志
pm2 monit            # CPU/内存监控

curl http://localhost:8000/health  # 健康检查
```

## 维护操作

### 正常重启
```bash
pm2 restart systrader
```

### 紧急停止
```bash
pm2 stop systrader              # 停止交易
pm2 delete systrader             # 删除进程
```

### 日志管理
```bash
# 日志位置
logs/
├── systrader.log          # 主日志 (JSON, 轮转)
├── dashboard-error.log    # Dashboard 错误
└── pm2-out.log           # PM2 输出

# 查看最新
tail -f logs/systrader.log
```

## 稳定性测试（soak）

```bash
# testnet 7 天 soak（统一装配）
python tools/stability_test.py --hours 168

# 并行健康监控（每小时 RSS + 错误计数）
python tools/soak_watchdog.py --log logs/systrader.log --out logs/soak_metrics.csv
```

### 验收标准（C 阶段）

- 7 天无意外错误（soak_metrics.csv 错误计数无异常尖峰）
- 无风控熔断触发（日志无 RISK REJECTED 熔断类）
- 对账零漂移（reconciler 无 drift 告警）
- 内存曲线平稳（RSS 波动 < 阈值，无持续增长趋势）

### 实盘分级（D 阶段，验收标准）

| 级 | risk_per_trade | 时长 | 验收 |
|---|---|---|---|
| 1 | 0.002 | 7 天 | 无重大事故 + 指标与 testnet 一致 ±20% |
| 2 | 0.005 | 7 天 | 同上 |
| 3 | 0.010 | 7 天 | 同上 |
| 4 | 0.015（设计值） | 持续 | 同上 |

```bash
python -m shared.runner --risk-per-trade 0.002 --execution-mode live
```

### 影子交易（B 阶段）

双实例运行（live 小仓位 + paper 模拟同参数），ShadowMonitor 比对：
- 验收：信号对齐 ≥95% + 逐笔滑点/填充率记录 + 1 周无系统性偏差
- 工具：python tools/shadow_monitor.py（record API + JSON 报告；实时订阅接线为后续增强）

## 常见问题

| 问题 | 检查 | 解决 |
|------|------|------|
| WebSocket 断连 | `pm2 logs systrader` 看 WS error | 自动重连，等 5s |
| API Key 错误 | 检查 config/.env | 重新配置 |
| 下单被拒 | 检查 key 权限 | 后台开合约权限 |
| 代理断连 | `netstat -ano \| findstr :7897` | 重启 Clash |
| 磁盘满 | `df -h` | 清理 logs/ 旧日志 |

## 告警响应

| 告警 | 操作 |
|------|------|
| margin_ratio > 80% | 立即检查持仓，考虑减仓 |
| 连续 3 笔亏损 | 暂停策略，检查市场条件 |
| WS 超时 60s | 检查网络和代理 |
| 日亏损 > 5% | 系统自动停交易，检查原因 |

## 文件

```
config/.env          # API 密钥 (不提交 git)
data/trades.db       # 交易记录 SQLite
logs/                # 日志文件
models/              # 训练好的模型
```

## 进程守护与代理看门狗（Ops T4）

### nssm 服务化（SystraderService）

`tools/install_systrader_service.bat`（右键"以管理员身份运行"）:
- 服务名 `SystraderService`，命令 `python -m shared.runner --execution-mode live --instance live`
- 工作目录 = 项目根；崩溃自动重启（AppExit Default Restart，5 秒延迟）
- 日志: `logs/systrader-service.log` / `logs/systrader-service.err`（每日轮转）
- 脚本含管理员检查（net session）与安装前二次确认，不会误装

手动命令（nssm 位于 `tools/proxy_pool/nssm.exe`）:

```bat
:: 安装
tools\proxy_pool\nssm.exe install SystraderService python "-m shared.runner --execution-mode live --instance live"
tools\proxy_pool\nssm.exe set SystraderService AppDirectory "D:\Documents\z_python_data_analy\Quent\Sys_trader"
tools\proxy_pool\nssm.exe set SystraderService Start SERVICE_AUTO_START
tools\proxy_pool\nssm.exe set SystraderService AppExit Default Restart
tools\proxy_pool\nssm.exe start SystraderService

:: 停止 / 重启 / 卸载
tools\proxy_pool\nssm.exe stop SystraderService
tools\proxy_pool\nssm.exe restart SystraderService
tools\proxy_pool\nssm.exe remove SystraderService confirm
```

查看日志: `tail -f logs/systrader-service.log`（PowerShell: `Get-Content logs\systrader-service.log -Wait`）。
注意: 服务账户的 PATH 可能不含 python，若 `python` 找不到请用绝对路径重装（如 `C:\Users\Evan\anaconda3\python.exe`）。

### 代理故障切换（tools/proxy_watchdog.py）

Clash 代理（127.0.0.1:7897）延迟波动 6-10s 会拖垮 Binance 签名窗口。
看门狗周期探测 testnet 时间接口，连续超标时调用 proxy_pool 切换节点 + 钉钉告警:

```bash
python tools/proxy_watchdog.py --threshold-ms 5000 --consecutive 3 --interval 30
```

- 探测: GET https://testnet.binancefuture.com/fapi/v1/time 走 7897（12s 超时）
- 判定: 连续 3 次延迟 > 5000ms（或探测失败）→ 切换 + 告警
- 切换: 读 `tools/proxy_pool/proxy_pool.json` → `apply_config(force_reload=True)`
  （全量重写 mihomo.yaml + 热重载，与 proxy_pool 服务健康检查同路径）
- 告警: `DINGTALK_WEBHOOK_URL` 环境变量；缺失时降级为日志
- 去抖: 切换后 300s 冷却（`--cooldown` 可调）

### 常见问题补充

| 问题 | 检查 | 解决 |
|------|------|------|
| 代理延迟持续超标 | `python tools/proxy_watchdog.py` 日志 | 自动切节点；失败则手动 `python tools/proxy_pool/proxy_pool.py --generate` |
| 服务崩溃未重启 | `nssm status SystraderService` | 确认 AppExit Default Restart 已设置；查 logs/systrader-service.err |

## 故障响应 Playbook（Ops T5）

> 每个故障: 症状 / 检测方式 / 影响 / 处理步骤 / 预期恢复时间 / 升级路径。
> 告警通道: heartbeat_watchdog + proxy_watchdog 钉钉告警
> （`DINGTALK_WEBHOOK_URL` 优先，旧名 `DINGTALK_WEBHOOK` 兜底；均未配置时降级日志）。

### 1. 主系统心跳停滞

- **症状**: 钉钉「心跳停滞告警」（heartbeat 事件超过 60s 无更新）；`pm2 status` 进程在但无日志输出
- **检测方式**: `pm2 logs systrader | tail -50` 看最后日志时间；`python tools/heartbeat_watchdog.py --stale-after 60` 手动探测
- **影响**: 进程可能静默挂起 → 无信号、无下单、对账停摆
- **处理步骤**:
  1. `pm2 status systrader` 确认进程存活（已退出则 `pm2 restart systrader`）
  2. `grep -E "ERROR|STALL|NETDIAG" logs/systrader.log | tail -20` 找最后异常
  3. 正常重启: `pm2 restart systrader`（或 `tools\proxy_pool\nssm.exe restart SystraderService`）
  4. 确认恢复: `curl http://localhost:8000/health` + 日志出现 `SNAPSHOT`/heartbeat
- **预期恢复时间**: 重启后 5-30s 心跳恢复
- **升级路径**: 重启 3 次仍复发 → 人工介入查网络/代理/代码；期间禁止手动下单

### 2. 代理高延迟

- **症状**: proxy_watchdog 钉钉告警（连续 3 次探测 > 5000ms）；runner 日志 `NETDIAG ... gateway=OK dns223=OK clash=OPEN`（本地网络正常 → 节点问题）
- **检测方式**: `python tools/proxy_watchdog.py --threshold-ms 5000 --consecutive 3 --interval 30` 日志；`netstat -ano | findstr :7897` 确认 Clash 存活；`curl -x http://127.0.0.1:7897 https://testnet.binancefuture.com/fapi/v1/time` 实测延迟
- **影响**: 签名请求超时 / -1021 时间戳超窗 → 下单失败率升高、撤单延迟
- **处理步骤**:
  1. 等自动切换（proxy_pool 切换节点，300s 冷却）
  2. 未自动切换 → 手动 `python tools/proxy_pool/proxy_pool.py --generate`
  3. 切换后重测 `curl -x ...` 延迟，确认 < 2s
  4. 有超时挂单被撤 → 按下一根 15m K线信号重新评估
- **预期恢复时间**: 自动切换 30s-5min；手动 1-2min
- **升级路径**: 持续超标 > 30min → 人工更换代理线路 / 重启 Clash / 检查本地网络

### 3. K线闭合停滞

- **症状**: 钉钉「K线闭合停滞告警」（kline_closes 超过 15 分钟无增长，由 heartbeat stats 检测）
- **检测方式**: `python tools/heartbeat_watchdog.py --closes-stall-minutes 15`；`grep -iE "ws|stall|reconnect" logs/systrader.log | tail`
- **影响**: 行情中断 → 无新信号；持续停滞还会触发 runner 熔断（见 #6）→ 停单
- **处理步骤**:
  1. 查 feed 日志 `grep -iE "ws|stall|reconnect" logs/systrader.log | tail -20`，确认自动重连是否生效
  2. 未恢复 → `pm2 restart systrader` 重启
  3. 若已触发 stall 熔断 → 先修复行情再按 #6 resume
- **预期恢复时间**: 自动重连 5s；重启 30s；K线恢复后告警自动解除
- **升级路径**: 重启后仍停滞 → 人工检查 Clash 节点（见 #2）/ Binance testnet 服务状态

### 4. 挂单超时自动撤单

- **症状**: runner 日志 `PENDING TIMEOUT ... 自动撤单`（PENDING 订单超 30 分钟未成交，如 LIMIT 入场价未回踩到位）
- **检测方式**: `grep "PENDING TIMEOUT" logs/systrader.log`
- **影响**: 僵尸单被撤（避免长期挂单）；无持仓影响；该信号意图作废
- **处理步骤**:
  1. 确认撤单成功（无 `cancel failed` 告警）
  2. 评估是否重新挂: 信号仍有效 → 等下一根 15m K线闭合自然触发新信号，不手工重挂
  3. 注意: 有持仓的 symbol 的止损/止盈条件单是持仓保护，**不会**被超时撤单
- **预期恢复时间**: 自动完成，无需人工（人工评估 1-5min）
- **升级路径**: 撤单持续 ERROR → 查 API Key 权限/代理 → 人工在交易所面板核对并撤单

### 5. 订单失败率告警

- **症状**: 钉钉「订单失败率告警」（orders_failed / (orders_placed + orders_failed) > 10%）
- **检测方式**: `python tools/heartbeat_watchdog.py --fail-rate-threshold 0.10`；`grep -E "ORDER FAILED|ORDER EXCEPTION" logs/systrader.log | tail -20`
- **影响**: 下单链路异常（API Key 权限 / 余额不足 / 429 限频 / -1021 代理延迟）
- **处理步骤**:
  1. 查最近失败原因（上一步 grep 输出，看 error 字段）
  2. 按错误码处理: 余额不足 → 充值；429 → 等自动退避；-1021 → 走 #2 代理处理
  3. 确认 IdempotencyTracker 无重复单（`data/intents.db` 无堆积）
  4. 无法定位 → 手动熔断停单（dashboard 控制台发 emergency_stop，或见 #6）
- **预期恢复时间**: 自动恢复 1-5min；人工 5-15min
- **升级路径**: 失败率持续 > 10% 且无法定位 → 人工暂停系统排查

### 6. 熔断触发（stall / kill switch）

- **症状**: 日志 `STALL BREAKER ... 触发熔断停单` 或 `EMERGENCY STOP — 停止下单`；后续信号被拒（`Circuit breaker active`）
- **检测方式**: `grep -E "STALL BREAKER|EMERGENCY STOP|Circuit breaker" logs/systrader.log`
- **影响**: 停止新单 + 撤销全部活跃订单（含持仓的 SL/TP 保护单）；**持仓保留但失去保护**
- **处理步骤**:
  1. 确认根因: stall 熔断 → 行情/网络问题（按 #2/#3 处理）；kill switch → 人工触发（确认意图）
  2. 修复根因后，人工确认持仓与余额（`curl http://localhost:8000/health` 或对账日志）
  3. 手动 resume: dashboard 控制台发 resume 命令，或:
     ```bash
     redis-cli XADD systrader:command * payload "{\"event_id\":\"resume-1\",\"stream\":\"command\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%S+00:00)\",\"data\":{\"command\":\"resume\"}}"
     ```
  4. 恢复后观察下一根 15m K线正常出信号、下单
- **预期恢复时间**: 人工确认后 1-5min
- **升级路径**: 熔断后无法确认持仓状态 → 人工登录交易所核对 → 必要时人工撤单/平仓

### Ops T5 新增参数

| 命令 | 参数 | 默认 | 说明 |
|------|------|------|------|
| `python -m shared.runner` | `--stall-strikes` | 3 | 连续停滞判定次数达到后熔断停单（0=只告警不熔断） |
| `python -m shared.runner` | `--pending-timeout-minutes` | 30 | PENDING 订单超时自动撤单阈值，分钟（0=禁用） |
| `python tools/heartbeat_watchdog.py` | `--closes-stall-minutes` | 15 | kline_closes 无增长告警阈值，分钟 |
| `python tools/heartbeat_watchdog.py` | `--fail-rate-threshold` | 0.10 | 订单失败率告警阈值 |
