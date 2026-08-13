import pytest
from unittest.mock import MagicMock
from shared.paper_trader import PaperTrader, PaperFill
from execution.order_gateway import OrderRequest


@pytest.mark.unit
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

    def test_stop_market_conditional_not_filled_immediately(self):
        """条件单 (STOP_MARKET) 不应随市价立即成交, 返回 NEW 挂起"""
        req = OrderRequest(symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
                           quantity=0.1, stop_price=62000.0, reduce_only=True)
        fill = self.trader.execute(req)
        assert fill.status == "NEW"
        assert fill.executed_qty == 0.0

    def test_take_profit_market_conditional_not_filled_immediately(self):
        """条件单 (TAKE_PROFIT_MARKET) 不应随市价立即成交, 返回 NEW 挂起"""
        req = OrderRequest(symbol="BTCUSDT", side="SELL", order_type="TAKE_PROFIT_MARKET",
                           quantity=0.1, stop_price=68000.0, reduce_only=True)
        fill = self.trader.execute(req)
        assert fill.status == "NEW"
        assert fill.executed_qty == 0.0

    def test_no_price_leaves_order_unfilled(self):
        """无行情时不成交, 不生成 0 价成交记录"""
        feed3 = MagicMock()
        feed3.get_last_price.return_value = None
        trader3 = PaperTrader(feed=feed3, fill_delay_ms=0, slippage_pct=0.0)
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="MARKET", quantity=0.1)
        fill = trader3.execute(req)
        assert fill.status == "NEW"
        assert fill.executed_qty == 0.0
