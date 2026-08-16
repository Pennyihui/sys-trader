# 生产级硬化 — 资金费率 + 费用模型 + 配置校验 + 运行模式 + 持久化

> 日期: 2026-08-01

## 1. FundingRateMonitor
- `shared/funding_monitor.py`：每 8h 结算前抓取实盘费率 (`GET /fapi/v1/premiumIndex`，公开接口)
- 计算持仓资金成本，超过阈值推钉钉告警
- 复用 `FundingRateTracker` 做计算

## 2. 滑点/手续费模型
- `shared/fee_model.py`：taker 0.05% / maker 0.02%，滑点百分比可配
- OrderManager 下单时附加到 TradeRecord

## 3. 参数配置校验
- `config/settings.py`：Pydantic BaseSettings 校验所有配置
- 启动时 fail-fast，配置错误直接报错

## 4. 运行模式三态
- `shared/execution_mode.py`：DRY_RUN / PAPER / LIVE
- OrderManager 根据模式路由：dry-run 不成交 / paper 走 PaperTrader / live 走 Binance

## 5. 订单生命周期持久化
- TradeDatabase 增加 orders 表：created → submitted → filled → closed
- 状态机记录每次变更

## 6. PaperTrader 集成
- OrderManager 增加 paper 模式支持，自动调用 PaperTrader
