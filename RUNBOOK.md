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
