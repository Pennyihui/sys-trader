# PositionGuardian — 本地价格监控与动态风控

> 日期: 2026-07-26 | 基于架构设计: docs/superpowers/specs/2026-07-04-trading-system-architecture.md

## 定位

PositionGuardian 在 Algo Order API 条件单（安全网）之上，提供本地策略增强层：

- Algo 单：硬止损/止盈，断网也不丢
- Guardian：跟踪止损、动态距离、部分止盈

## 功能

1. **跟踪止损**：价格上涨时止损跟着上移，锁住利润
2. **动态距离**：基于 ATR 自动调整止损宽度，波动大放宽、波动小收紧
3. **部分止盈**：达到目标价分批平仓（TP1 平 50%，TP2 平剩余）

## 接口

```python
class PositionGuardian:
    def __init__(self, feed, portfolio, gateway, config=None)
    def start()     # 启动后台线程
    def stop()      # 停止
```

## 配置

- trailing_activation_pct: 0.003（涨 0.3% 开始跟踪）
- trailing_step_pct: 0.005（每涨 0.5% 移动一次）
- atr_period: 14
- stop_atr_multiple: 2.0（止损 = ATR × 2）
- tp1_pct: 0.03, tp1_ratio: 0.5
- tp2_pct: 0.06
- check_interval: 1.0s
