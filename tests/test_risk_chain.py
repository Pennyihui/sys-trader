import pytest
from unittest.mock import MagicMock
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

    def test_concentration_counts_proposed_margin(self):
        """本次拟开仓保证金计入 per-symbol 判断 (不再只看存量)。"""
        tracker = PortfolioTracker(initial_equity=1000.0)
        # 已有持仓保证金 280 (28%), 拟开仓名义价值按上限 100 USDT 计 → +33.33 保证金
        tracker.open_position(Position(symbol="BTCUSDT", direction="LONG",
                                       quantity=0.01344, entry_price=62500.0, leverage=3))
        check = ConcentrationCheck(max_per_symbol=0.30)
        signal = Signal("BTCUSDT", "LONG", 0.80, 64000.0, 62000.0, 68000.0)
        result = check.process(signal, tracker, {"position_size": 0.1})
        assert result.rejected
        assert "BTCUSDT" in result.reason

    def test_concentration_without_proposed_margin_passes(self):
        """无拟开仓数量时只按存量判断, 28% 不超 30%。"""
        tracker = PortfolioTracker(initial_equity=1000.0)
        tracker.open_position(Position(symbol="BTCUSDT", direction="LONG",
                                       quantity=0.01344, entry_price=62500.0, leverage=3))
        check = ConcentrationCheck(max_per_symbol=0.30)
        signal = Signal("BTCUSDT", "LONG", 0.80, 64000.0, 62000.0, 68000.0)
        result = check.process(signal, tracker, {})
        assert not result.rejected

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


@pytest.mark.unit
def test_publishes_approved_and_rejected():
    sig = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.8,
                 entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0)
    portfolio = PortfolioTracker(initial_equity=10000.0)

    # 空链 → 恰好发布 1 次 signal.approved
    bus1 = MagicMock()
    chain = MiddlewareChain(event_bus=bus1)
    chain.process(sig, portfolio)
    assert bus1.publish.call_count == 1
    stream, payload = bus1.publish.call_args[0]
    assert stream == "signal.approved"
    assert payload["instance"] == "live"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["direction"] == "LONG"
    assert payload["signal_id"] == sig.signal_id
    assert isinstance(payload["modifications"], dict)

    # 拒绝中间件 → 恰好发布 1 次 signal.rejected（互斥：不含 approved）
    class Rejecter:
        def process(self, signal, portfolio):
            return MiddlewareResult(rejected=True, reason="test")

    bus2 = MagicMock()
    chain = MiddlewareChain(event_bus=bus2)
    chain.add(Rejecter())
    chain.process(sig, portfolio)
    assert bus2.publish.call_count == 1
    stream, payload = bus2.publish.call_args[0]
    assert stream == "signal.rejected"
    assert payload["instance"] == "live"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["direction"] == "LONG"
    assert payload["reason"] == "test"
    assert payload["signal_id"] == sig.signal_id
