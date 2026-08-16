# 策略接口 + 数据库合并 + 重试装饰器

> 日期: 2026-07-31

## 1. 策略接口

参考 Freqtrade IStrategy，定义可插拔策略基类：

- `signal_engine/interface.py` — IStrategy 抽象基类
  - `analyze(df) -> Optional[Signal]` 主入口
  - `populate_indicators(df)` 向量化指标钩子
  - `populate_entry_trend(df)` 入场信号钩子
  - `populate_exit_trend(df)` 出场信号钩子
  - `custom_stoploss()` 动态止损回调
  - `leverage()` 杠杆回调
- `signal_engine/engine.py` — SignalEngine 持有 strategy 实例，可插拔

## 2. 数据库合并

`shared/database.py` 统一管理三个表：
- `trades` (已有)
- `signals` (已有)
- `order_intents` (从 idempotency 迁入)

删除 `shared/idempotency.py` 的独立连接，改为复用 TradeDatabase。

## 3. 重试装饰器

`shared/retry.py`：
- `@retrier(max_retries=3, backoff=1.0)` 指数退避重试
- 应用于 OrderGateway 的 API 调用
