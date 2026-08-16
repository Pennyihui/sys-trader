# 运维系统设计

> 日期: 2026-07-27

## 模块

| 文件 | 状态 | 职责 |
|------|:----:|------|
| `shared/logging.py` | 已有 | JSON 结构化日志 |
| `shared/runner.py` | 重写 | 启动前校验 + 优雅关闭 |
| `shared/startup_reconciler.py` | 已有 | 启动持仓对账 |
| `shared/reconciler.py` | 新建 | 持续对账循环 (每5min) |
| `shared/idempotency.py` | 新建 | clientOrderId 幂等性 + intent 追踪 |
| `shared/order_guard.py` | 新建 | 启动时检查 pending intent |
| `shared/preflight.py` | 新建 | 启动前校验 (余额/权限/网络/测试) |
| `ecosystem.config.js` | 已有 | PM2 进程管理 |
| `RUNBOOK.md` | 已有 | 运维文档 |

## 启动流程

```
preflight(余额/权限/网络/测试) → order_guard(pending intent 对账)
→ startup_reconciler(持仓对账) → runner(启动模块)
→ reconciler 持续循环(每5min) → SIGTERM → 记录退出状态
```
