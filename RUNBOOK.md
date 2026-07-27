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

## 启动

```bash
# PM2 方式 (推荐)
pm2 start ecosystem.config.js

# 手动方式
python -m shared.runner

# Dashboard
cd dashboard/frontend && npm run dev
```

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
