from typing import Any, Dict

from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class ConcentrationCheck(Middleware):
    def __init__(self, max_per_symbol: float = 0.30, max_same_direction: float = 0.50,
                 max_total_margin: float = 0.80, leverage: int = 3):
        self.max_per_symbol = max_per_symbol
        self.max_same_direction = max_same_direction
        self.max_total_margin = max_total_margin
        self.leverage = leverage  # 与 runner 开仓杠杆一致, 用于估算拟开仓保证金

    def process(self, signal: Signal, portfolio: PortfolioTracker,
                modifications: Dict[str, Any] = None) -> MiddlewareResult:
        if portfolio.total_equity <= 0:
            return MiddlewareResult(rejected=True, reason="ConcentrationCheck: equity <= 0")
        # 2026-08-16: 拟开仓杠杆取信号实际值 (原硬编码 self.leverage=3,
        # 与 _sync_account_config 设置的实际杠杆脱节 → 保证金低估)
        lev = float(getattr(signal, "leverage", self.leverage) or self.leverage or 3)
        if lev <= 0:
            lev = 3.0
        proposed_size = (modifications or {}).get("position_size", 0.0) or 0.0
        proposed_margin = 0.0
        if proposed_size > 0 and signal.entry_price > 0:
            proposed_notional = min(proposed_size * signal.entry_price, 100.0)
            proposed_margin = proposed_notional / lev
        sym_margin = portfolio.margin_for_symbol(signal.symbol) + proposed_margin
        sym_ratio = sym_margin / portfolio.total_equity
        if sym_ratio >= self.max_per_symbol:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: {signal.symbol} margin {sym_ratio:.1%} >= {self.max_per_symbol:.0%}")
        dir_margin = portfolio.margin_same_direction(signal.direction) + proposed_margin
        dir_ratio = dir_margin / portfolio.total_equity
        if dir_ratio >= self.max_same_direction:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: {signal.direction} total margin {dir_ratio:.1%} >= {self.max_same_direction:.0%}")
        total_ratio = (portfolio.total_margin + proposed_margin) / portfolio.total_equity
        if total_ratio >= self.max_total_margin:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: total margin {total_ratio:.1%} >= {self.max_total_margin:.0%}")
        return MiddlewareResult(rejected=False, signal=signal)
