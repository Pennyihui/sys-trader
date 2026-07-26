import pytest
from shared.database import TradeDatabase, TradeRecord


@pytest.mark.unit
class TestTradeDatabase:
    def setup_method(self):
        self.db = TradeDatabase(":memory:")

    def test_store_and_retrieve_trade(self):
        trade = TradeRecord(
            symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=0.15, price=64000.0, status="FILLED",
            order_id=12345, order_type_detail="MARKET",
        )
        self.db.store_trade(trade)
        trades = self.db.get_trades()
        assert len(trades) == 1
        assert trades[0].symbol == "BTCUSDT"

    def test_get_trades_empty(self):
        assert len(self.db.get_trades()) == 0

    def test_store_signal(self):
        self.db.store_signal("BTCUSDT", "LONG", 0.72, 64000.0)
        signals = self.db.get_signals(limit=5)
        assert len(signals) == 1
        assert signals[0]["direction"] == "LONG"
