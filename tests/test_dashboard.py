import pytest
from unittest.mock import MagicMock
from dashboard.data_collector import DataCollector


@pytest.mark.integration
class TestDataCollector:
    def setup_method(self):
        self.feed = MagicMock()
        self.feed.get_last_price.return_value = 64000.0
        self.feed.get_mark_price.return_value = 64000.0
        self.feed.buffer.count.return_value = 5
        self.portfolio = MagicMock()
        self.portfolio.total_equity = 10000.0
        self.portfolio.total_margin = 1200.0
        self.portfolio.margin_ratio = 0.12
        self.portfolio.daily_realized_pnl = 50.0
        self.portfolio.current_drawdown = 0.03
        self.portfolio.positions = {}
        self.collector = DataCollector(feed=self.feed, portfolio=self.portfolio)

    def test_collect_returns_all_fields(self):
        data = self.collector.collect()
        assert "equity" in data
        assert "margin_ratio" in data
        assert "daily_pnl" in data
        assert "positions" in data
        assert "prices" in data

    def test_collect_btc_mark_price(self):
        pos = MagicMock()
        pos.symbol = "BTCUSDT"
        pos.direction = "LONG"
        pos.quantity = 0.1
        pos.entry_price = 63000.0
        self.portfolio.positions = {"BTCUSDT": pos}
        self.portfolio.unrealized_pnl.return_value = 100.0
        data = self.collector.collect()
        assert data["prices"]["BTCUSDT"]["mark"] == 64000.0
        assert data["position_count"] == 1

    def test_empty_positions_returns_empty_list(self):
        data = self.collector.collect()
        assert data["positions"] == []

    def test_drawdown_included(self):
        data = self.collector.collect()
        assert "drawdown" in data
