"""AvailableMarginCheck — 下单前可用保证金检查（风控链第 3 环, 2026-08-16）。

仓位计算用 total_equity 口径, 但实际下单受 availableBalance (未占用保证金)
约束。本中间件保证本笔所需保证金 ≤ 可用余额 × safety_ratio, 防止保证金
不足被交易所拒绝 (或触发爆仓边缘开仓)。
"""

from typing import Any, Dict, Optional

from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class AvailableMarginCheck(Middleware):
    def __init__(self, safety_ratio: float = 0.9):
        self.safety_ratio = safety_ratio

    def process(self, signal: Signal, portfolio: PortfolioTracker,
                modifications: Optional[Dict[str, Any]] = None) -> MiddlewareResult:
        size = float((modifications or {}).get("position_size", 0.0) or 0.0)
        if size <= 0:
            return MiddlewareResult(rejected=True,
                                    reason="AvailableMarginCheck: position_size missing")
        leverage = float(getattr(signal, "leverage", 3) or 3)
        if leverage <= 0:
            leverage = 3.0
        required = size * signal.entry_price / leverage
        available = portfolio.available_balance
        if available <= 0:
            return MiddlewareResult(
                rejected=True,
                reason=f"AvailableMarginCheck: available balance {available:.2f} <= 0")
        if required > available * self.safety_ratio:
            return MiddlewareResult(
                rejected=True,
                reason=(f"AvailableMarginCheck: required margin {required:.2f} > "
                        f"{self.safety_ratio:.0%} available {available:.2f}"))
        return MiddlewareResult(rejected=False, signal=signal,
                                modifications={"required_margin": round(required, 4)})
