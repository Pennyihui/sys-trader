import pytest
from shared.funding_rate import FundingRateTracker


class TestFundingRateTracker:
    def setup_method(self):
        self.tracker = FundingRateTracker()

    def test_update_and_get(self):
        self.tracker.update("BTCUSDT", 0.0001)
        assert self.tracker.get_rate("BTCUSDT") == 0.0001

    def test_unknown_symbol_returns_none(self):
        assert self.tracker.get_rate("UNKNOWN") is None

    def test_estimate_cost_zero_without_rate(self):
        cost = self.tracker.estimate_cost("BTCUSDT", 10000.0)
        assert cost == 0.0

    def test_estimate_cost_positive_rate(self):
        self.tracker.update("BTCUSDT", 0.0001)
        cost = self.tracker.estimate_cost("BTCUSDT", 10000.0, hours=8)
        assert cost == pytest.approx(1.0, rel=0.01)

    def test_next_funding_time(self):
        nft = self.tracker.next_funding_time()
        now = __import__("time").time()
        assert nft > now
        assert nft - now <= 8 * 3600
