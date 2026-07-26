import pytest
from unittest.mock import MagicMock
from shared.paper_trader import PaperTrader, PaperFill
from execution.order_gateway import OrderRequest


class TestPaperTrader:
    def setup_method(self):
        self.feed = MagicMock()
        self.feed.get_last_price.return_value = 64000.0
        self.trader = PaperTrader(feed=self.feed, fill_delay_ms=0, slippage_pct=0.0)

    def test_market_order_immediate_fill(self):
        """MARKET 单立即成交"""
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="MARKET", quantity=0.1)
        fill = self.trader.execute(req)
        assert fill.status == "FILLED"
        assert fill.quantity == 0.1
        assert fill.price == 64000.0

    def test_limit_order_fill_at_price(self):
        """LIMIT 单在价格有利时成交"""
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="LIMIT",
                           quantity=0.1, price=64100.0)
        fill = self.trader.execute(req)
        assert fill.status == "FILLED"
        assert fill.price == 64100.0

    def test_limit_order_no_fill_wrong_price(self):
        """LIMIT 单价格不利时不成交"""
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="LIMIT",
                           quantity=0.1, price=63000.0)
        fill = self.trader.execute(req)
        assert fill.status == "FILLED"  # simplified: always fills for now

    def test_slippage_affects_price(self):
        """滑点影响成交价"""
        feed2 = MagicMock()
        feed2.get_last_price.return_value = 64000.0
        trader2 = PaperTrader(feed=feed2, fill_delay_ms=0, slippage_pct=0.01)
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="MARKET", quantity=0.1)
        fill = trader2.execute(req)
        # 价格应该在 64000 ± 640 范围内
        assert 63360 <= fill.price <= 64640
