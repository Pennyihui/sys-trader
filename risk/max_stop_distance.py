"""最大止损距离校验 (2026-08-16 风控补强 #4)。

止损太远的信号意味着单笔风险失真 (risk_per_trade 按止损距离算仓位,
距离过大则仓位极小或无意义), 直接拒绝病态信号。
max_stop_pct: 止损距离占入场价的百分比上限, 0=禁用。
"""

from typing import Any, Dict, Optional

from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class MaxStopDistance(Middleware):
    def __init__(self, max_stop_pct: float = 0.05):
        self.max_stop_pct = max_stop_pct

    def process(self, signal: Signal, portfolio: PortfolioTracker,
                modifications: Optional[Dict[str, Any]] = None) -> MiddlewareResult:
        if self.max_stop_pct <= 0 or signal.entry_price <= 0:
            return MiddlewareResult(rejected=False, signal=signal)
        distance = abs(signal.entry_price - signal.stop_loss) / signal.entry_price
        if distance > self.max_stop_pct:
            return MiddlewareResult(
                rejected=True,
                reason=(f"MaxStopDistance: 止损距离 {distance:.2%} "
                        f"> 上限 {self.max_stop_pct:.2%}"))
        return MiddlewareResult(rejected=False, signal=signal)
