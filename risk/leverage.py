"""LeverageController — 杠杆上限检查（风控链第 2 环）。

设计出处: docs/superpowers/specs/2026-07-04-trading-system-architecture.md §3.4.2。
架构原定 5 中间件（仓位→杠杆→熔断→日亏损→集中度），此前缺失，2026-08-16 审计补上。

信号携带策略声明的杠杆 (Signal.leverage)，超出 max_leverage 即拒绝，
防止策略升级杠杆时绕过全局杠杆上限。
"""

from typing import Any, Dict, Optional

from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class LeverageController(Middleware):
    def __init__(self, max_leverage: int = 5):
        if max_leverage < 1:
            raise ValueError("max_leverage must be >= 1")
        self.max_leverage = max_leverage

    def process(self, signal: Signal, portfolio: PortfolioTracker,
                modifications: Optional[Dict[str, Any]] = None) -> MiddlewareResult:
        leverage = int(getattr(signal, "leverage", 3) or 3)
        if leverage > self.max_leverage:
            return MiddlewareResult(
                rejected=True,
                reason=(
                    f"LeverageController: leverage {leverage}x exceeds "
                    f"max {self.max_leverage}x"
                ),
            )
        return MiddlewareResult(
            rejected=False, signal=signal,
            modifications={"leverage": leverage},
        )
