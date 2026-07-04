import time
from enum import Enum
from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class BreakerState(str, Enum):
    ACTIVE = "ACTIVE"
    TRIGGERED = "TRIGGERED"
    COOLDOWN = "COOLDOWN"


class DrawdownBreaker(Middleware):
    def __init__(self, max_drawdown: float = 0.15, consecutive_loss_breaker: int = 3, cooldown_minutes: int = 120):
        self.max_drawdown = max_drawdown
        self.consecutive_loss_breaker = consecutive_loss_breaker
        self.cooldown_seconds = cooldown_minutes * 60
        self.state = BreakerState.ACTIVE
        self._triggered_at: float = 0.0

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        if self.state == BreakerState.COOLDOWN:
            if time.time() - self._triggered_at >= self.cooldown_seconds:
                self.state = BreakerState.ACTIVE
            else:
                remaining = int((self.cooldown_seconds - (time.time() - self._triggered_at)) / 60)
                return MiddlewareResult(rejected=True, reason=f"DrawdownBreaker: cooldown, {remaining}min remaining")
        if portfolio.current_drawdown >= self.max_drawdown:
            self.state = BreakerState.TRIGGERED
            return MiddlewareResult(rejected=True, reason=f"DrawdownBreaker: drawdown {portfolio.current_drawdown:.2%} >= {self.max_drawdown:.0%}")
        if portfolio.consecutive_losses >= self.consecutive_loss_breaker:
            self.state = BreakerState.TRIGGERED
            self._triggered_at = time.time()
            self.state = BreakerState.COOLDOWN
            return MiddlewareResult(rejected=True, reason=f"DrawdownBreaker: {portfolio.consecutive_losses} consecutive losses")
        return MiddlewareResult(rejected=False, signal=signal)
