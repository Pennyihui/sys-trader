import pytest
from unittest.mock import MagicMock
from portfolio.tracker import PortfolioTracker, Position


@pytest.mark.unit
class TestPortfolioTracker:
    def setup_method(self):
        self.tracker = PortfolioTracker(initial_equity=10000.0)

    def test_initial_state(self):
        assert self.tracker.total_equity == 10000.0
        assert self.tracker.available_balance == 10000.0
        assert self.tracker.peak_equity == 10000.0
        assert len(self.tracker.positions) == 0

    def test_open_position_adds_to_tracker(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.15, entry_price=62500.0, leverage=3)
        self.tracker.open_position(pos)
        assert "BTCUSDT" in self.tracker.positions
        assert self.tracker.positions["BTCUSDT"].quantity == 0.15

    def test_close_position_removes_from_tracker(self):
        pos = Position(symbol="ETHUSDT", direction="LONG", quantity=0.5, entry_price=3100.0, leverage=3)
        self.tracker.open_position(pos)
        pnl = self.tracker.close_position("ETHUSDT", 3200.0)
        assert "ETHUSDT" not in self.tracker.positions
        assert pnl > 0

    def test_unrealized_pnl_long_position(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        upnl = self.tracker.unrealized_pnl("BTCUSDT", 62000.0)
        assert upnl == pytest.approx(200.0, rel=0.01)

    def test_unrealized_pnl_short_position(self):
        pos = Position(symbol="BTCUSDT", direction="SHORT", quantity=0.1, entry_price=62000.0, leverage=3)
        self.tracker.open_position(pos)
        upnl = self.tracker.unrealized_pnl("BTCUSDT", 60000.0)
        assert upnl == pytest.approx(200.0, rel=0.01)

    def test_margin_ratio_calculation(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.15, entry_price=62500.0, leverage=3)
        self.tracker.open_position(pos)
        expected_margin = (0.15 * 62500.0) / 3
        assert self.tracker.total_margin == pytest.approx(expected_margin)
        ratio = self.tracker.margin_ratio
        assert ratio == pytest.approx(expected_margin / 10000.0)

    def test_peak_equity_tracks_maximum(self):
        self.tracker.update_equity(11000.0)
        assert self.tracker.peak_equity == 11000.0
        self.tracker.update_equity(10500.0)
        assert self.tracker.peak_equity == 11000.0

    def test_drawdown_calculation(self):
        self.tracker.update_equity(12000.0)
        self.tracker.update_equity(10200.0)
        dd = self.tracker.current_drawdown
        assert dd == pytest.approx(0.15, rel=0.01)

    def test_daily_pnl_tracks_realized(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.15, entry_price=62500.0, leverage=3)
        self.tracker.open_position(pos)
        self.tracker.close_position("BTCUSDT", 63000.0)
        assert self.tracker.daily_realized_pnl > 0

    def test_break_even_does_not_count_as_loss(self):
        """平本 (净盈亏==0) 不计入连亏。用 fee_rate=0 构造纯平本场景;
        默认费率下平本价卖出净盈亏为负 (手续费), 是计入连亏的。"""
        self.tracker.fee_rate = 0.0
        self.tracker.consecutive_losses = 2
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.tracker.close_position("BTCUSDT", 60000.0)
        assert self.tracker.consecutive_losses == 2

    def test_fee_makes_break_even_a_small_loss(self):
        """默认费率下平本价平仓净亏 (手续费), 计入连亏 (2026-08-16 P0-3)。"""
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        pnl = self.tracker.close_position("BTCUSDT", 60000.0)
        assert pnl < 0
        assert self.tracker.consecutive_losses == 1

    def test_loss_increments_consecutive_losses(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.tracker.close_position("BTCUSDT", 59000.0)
        assert self.tracker.consecutive_losses == 1

    def test_daily_reset_before_increment(self):
        """日切重置先于累加: 跨午夜首笔开/平仓不被清零。"""
        from datetime import timedelta
        self.tracker.daily_realized_pnl = 50.0
        self.tracker.trade_count_today = 3
        self.tracker._last_reset_day = self.tracker._last_reset_day - timedelta(days=1)  # 模拟跨日
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        # 跨日重置后当日计数从 1 开始, 旧计数 3 已被清零
        assert self.tracker.trade_count_today == 1
        self.tracker._last_reset_day = self.tracker._last_reset_day - timedelta(days=1)  # 再模拟跨日
        self.tracker.close_position("BTCUSDT", 61000.0)
        # 跨日重置后当日已实现盈亏仅包含本次平仓
        assert self.tracker.daily_realized_pnl > 0
        assert self.tracker.daily_realized_pnl < 300.0


@pytest.mark.unit
def test_publishes_position_changed_on_open():
    bus = MagicMock()
    tracker = PortfolioTracker(initial_equity=1000.0, event_bus=bus)
    tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 64000.0, 3))
    bus.publish.assert_called_once()
    stream, payload = bus.publish.call_args[0]
    assert stream == "position.changed"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["direction"] == "LONG"
    assert payload["instance"] == "live"


@pytest.mark.unit
def test_publishes_custom_instance_tag():
    bus = MagicMock()
    tracker = PortfolioTracker(initial_equity=1000.0, event_bus=bus, instance="paper")
    tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 64000.0, 3))
    stream, payload = bus.publish.call_args[0]
    assert stream == "position.changed"
    assert payload["instance"] == "paper"


@pytest.mark.unit
def test_publishes_metrics_on_equity_and_close():
    bus = MagicMock()
    tracker = PortfolioTracker(initial_equity=10000.0, event_bus=bus)

    tracker.update_equity(10500.0)
    _, payload = bus.publish.call_args_list[0][0]
    assert payload["event"] == "equity"
    assert payload["instance"] == "live"
    assert "margin_ratio" in payload
    assert "daily_pnl" in payload
    assert "drawdown" in payload

    tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 64000.0, 3))
    tracker.close_position("BTCUSDT", 65000.0)
    _, close_payload = bus.publish.call_args_list[-1][0]
    assert close_payload["event"] == "close"
    assert close_payload["instance"] == "live"
    assert "margin_ratio" in close_payload
    assert "daily_pnl" in close_payload
    assert "drawdown" in close_payload


@pytest.mark.unit
def test_no_event_bus_is_silent():
    tracker = PortfolioTracker(initial_equity=1000.0)
    tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 64000.0, 3))  # 不抛异常
