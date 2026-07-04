from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class ConcentrationCheck(Middleware):
    def __init__(self, max_per_symbol: float = 0.30, max_same_direction: float = 0.50, max_total_margin: float = 0.80):
        self.max_per_symbol = max_per_symbol
        self.max_same_direction = max_same_direction
        self.max_total_margin = max_total_margin

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        if portfolio.total_equity <= 0:
            return MiddlewareResult(rejected=True, reason="ConcentrationCheck: equity <= 0")
        sym_margin = portfolio.margin_for_symbol(signal.symbol)
        sym_ratio = sym_margin / portfolio.total_equity
        if sym_ratio >= self.max_per_symbol:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: {signal.symbol} margin {sym_ratio:.1%} >= {self.max_per_symbol:.0%}")
        dir_margin = portfolio.margin_same_direction(signal.direction)
        dir_ratio = dir_margin / portfolio.total_equity
        if dir_ratio >= self.max_same_direction:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: {signal.direction} total margin {dir_ratio:.1%} >= {self.max_same_direction:.0%}")
        total_ratio = portfolio.margin_ratio
        if total_ratio >= self.max_total_margin:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: total margin {total_ratio:.1%} >= {self.max_total_margin:.0%}")
        return MiddlewareResult(rejected=False, signal=signal)
