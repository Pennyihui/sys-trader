from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class PositionSizer(Middleware):
    def __init__(self, risk_per_trade: float = 0.015):
        self.risk_per_trade = risk_per_trade

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        stop_distance = abs(signal.entry_price - signal.stop_loss)
        if stop_distance <= 0:
            return MiddlewareResult(rejected=True, reason="PositionSizer: invalid stop distance (zero or negative)")
        risk_amount = portfolio.total_equity * self.risk_per_trade
        position_size = risk_amount / stop_distance
        if position_size <= 0:
            return MiddlewareResult(rejected=True, reason=f"PositionSizer: calculated size {position_size} <= 0")
        return MiddlewareResult(rejected=False, signal=signal, modifications={"position_size": position_size, "risk_amount": risk_amount})
