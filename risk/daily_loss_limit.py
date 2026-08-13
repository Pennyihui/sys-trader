from typing import Any, Dict

from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class DailyLossLimit(Middleware):
    def __init__(self, daily_loss_limit: float = 0.05):
        self.daily_loss_limit = daily_loss_limit

    def process(self, signal: Signal, portfolio: PortfolioTracker,
                modifications: Dict[str, Any] = None) -> MiddlewareResult:
        if portfolio.total_equity <= 0:
            return MiddlewareResult(rejected=True, reason="DailyLossLimit: equity <= 0")
        loss_ratio = -portfolio.daily_realized_pnl / portfolio.total_equity
        if loss_ratio >= self.daily_loss_limit:
            return MiddlewareResult(rejected=True, reason=f"DailyLossLimit: daily loss {loss_ratio:.2%} >= {self.daily_loss_limit:.0%}")
        return MiddlewareResult(rejected=False, signal=signal)
