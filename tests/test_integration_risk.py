import pytest
from risk.chain import MiddlewareChain
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker, Position


class TestRiskIntegration:
    def setup_method(self):
        self.tracker = PortfolioTracker(initial_equity=10000.0)
        self.chain = MiddlewareChain()
        self.chain.add(PositionSizer(risk_per_trade=0.015))
        self.chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
        self.chain.add(DailyLossLimit(daily_loss_limit=0.05))
        self.chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))

    def test_signal_passes_full_chain(self):
        signal = Signal(symbol="ETHUSDT", direction="LONG", conviction=0.68, entry_price=3100.0, stop_loss=3000.0, take_profit=3400.0)
        result = self.chain.process(signal, self.tracker)
        assert not result.rejected
        assert result.modifications.get("position_size") is not None

    def test_signal_rejected_when_concentration_exceeded(self):
        self.tracker.open_position(Position(symbol="BTCUSDT", direction="LONG", quantity=1.4, entry_price=62500.0, leverage=3))
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.80, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = self.chain.process(signal, self.tracker)
        assert result.rejected
        assert "concentration" in result.reason.lower() or "Concentration" in result.reason
