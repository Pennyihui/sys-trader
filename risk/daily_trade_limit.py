"""单日最大交易次数上限 (2026-08-16 风控补强 #3)。

防止策略在震荡市刷单烧手续费; trade_count_today 由 PortfolioTracker 维护
(日切自动重置)。max_trades=0 表示禁用。
"""

from typing import Any, Dict, Optional

from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class DailyTradeLimit(Middleware):
    def __init__(self, max_trades: int = 30):
        self.max_trades = max_trades

    def process(self, signal: Signal, portfolio: PortfolioTracker,
                modifications: Optional[Dict[str, Any]] = None) -> MiddlewareResult:
        if self.max_trades <= 0:
            return MiddlewareResult(rejected=False, signal=signal)
        if portfolio.trade_count_today >= self.max_trades:
            return MiddlewareResult(
                rejected=True,
                reason=(f"DailyTradeLimit: 今日已开仓 {portfolio.trade_count_today} 次 "
                        f">= 上限 {self.max_trades}"))
        return MiddlewareResult(rejected=False, signal=signal)
