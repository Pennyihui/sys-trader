import pytest
from risk.chain import MiddlewareChain, MiddlewareResult
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker, Position


@pytest.mark.integration
class TestRiskChain:
    def setup_method(self):
        self.tracker = PortfolioTracker(initial_equity=10000.0)
        self.chain = MiddlewareChain()

    def test_position_sizer_calculates_correct_size(self):
        sizer = PositionSizer(risk_per_trade=0.015)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = sizer.process(signal, self.tracker)
        assert not result.rejected
        assert result.signal is not None
        assert result.modifications.get("position_size") is not None

    def test_position_sizer_zero_size_for_invalid_stop(self):
        sizer = PositionSizer(risk_per_trade=0.015)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=62500.0, take_profit=65000.0)
        result = sizer.process(signal, self.tracker)
        assert result.rejected

    def test_drawdown_breaker_active_by_default(self):
        breaker = DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = breaker.process(signal, self.tracker)
        assert not result.rejected

    def test_drawdown_breaker_triggers_on_drawdown(self):
        self.tracker.update_equity(10000.0)
        self.tracker.peak_equity = 12000.0
        breaker = DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = breaker.process(signal, self.tracker)
        assert result.rejected

    def test_daily_loss_limit_respects_threshold(self):
        limit = DailyLossLimit(daily_loss_limit=0.05)
        self.tracker.daily_realized_pnl = -600.0
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = limit.process(signal, self.tracker)
        assert result.rejected

    def test_concentration_single_symbol_limit(self):
        self.tracker.open_position(Position(symbol="BTCUSDT", direction="LONG", quantity=0.48, entry_price=62500.0, leverage=3))
        check = ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = check.process(signal, self.tracker)
        assert result.rejected

    def test_chain_processes_all_middleware_in_order(self):
        self.chain.add(PositionSizer(risk_per_trade=0.015))
        self.chain.add(DailyLossLimit(daily_loss_limit=0.05))
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = self.chain.process(signal, self.tracker)
        assert not result.rejected

    def test_chain_stops_at_first_rejection(self):
        self.chain.add(DailyLossLimit(daily_loss_limit=0.05))
        self.chain.add(PositionSizer(risk_per_trade=0.015))
        self.tracker.daily_realized_pnl = -600.0
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = self.chain.process(signal, self.tracker)
        assert result.rejected
        assert "DailyLossLimit" in result.reason
