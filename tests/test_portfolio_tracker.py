import pytest
from portfolio.tracker import PortfolioTracker, Position


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
