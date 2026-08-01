"""测试资金费率监控。"""
import pytest
from unittest.mock import MagicMock, patch
from shared.funding_monitor import FundingRateMonitor
from portfolio.tracker import PortfolioTracker, Position


class TestFundingMonitor:
    def setup_method(self):
        self.portfolio = PortfolioTracker()
        self.portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        self.monitor = FundingRateMonitor(self.portfolio, cost_threshold=0.5)

    @patch("shared.funding_monitor.requests.get")
    def test_fetch_rate_success(self, mock_get):
        mock_get.return_value.json.return_value = {"symbol": "BTCUSDT", "lastFundingRate": "0.0001"}
        rate = self.monitor.fetch_rate("BTCUSDT")
        assert rate == pytest.approx(0.0001)

    @patch("shared.funding_monitor.requests.get")
    def test_fetch_rate_error(self, mock_get):
        mock_get.side_effect = Exception("network down")
        assert self.monitor.fetch_rate("BTCUSDT") is None

    @patch("shared.funding_monitor.requests.get")
    def test_check_positions_alerts_on_threshold(self, mock_get):
        mock_get.return_value.json.return_value = {"symbol": "BTCUSDT", "lastFundingRate": "0.001"}
        alerts = []
        self.monitor.on_alert = lambda m: alerts.append(m)
        self.monitor.check_positions()
        assert len(alerts) == 1
        assert "Funding cost" in alerts[0]

    @patch("shared.funding_monitor.requests.get")
    def test_check_positions_no_alert_below_threshold(self, mock_get):
        mock_get.return_value.json.return_value = {"symbol": "BTCUSDT", "lastFundingRate": "0.00001"}
        alerts = []
        self.monitor.on_alert = lambda m: alerts.append(m)
        self.monitor.check_positions()
        assert len(alerts) == 0
