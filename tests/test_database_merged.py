"""测试合并后的数据库（trades + signals + intents）。"""
import pytest
from shared.database import TradeDatabase, TradeRecord


class TestMergedDatabase:
    def setup_method(self):
        self.db = TradeDatabase(":memory:")

    def test_trades_and_intents_coexist(self):
        trade = TradeRecord(symbol="BTCUSDT", side="BUY", order_type="MARKET",
                            quantity=0.1, price=64000.0, status="FILLED")
        self.db.store_trade(trade)
        intent = self.db.create_intent("BTCUSDT", "SELL", "MARKET", 0.1)
        assert len(self.db.get_trades()) == 1
        assert intent["status"] == "PENDING"

    def test_intent_lifecycle(self):
        intent = self.db.create_intent("BTCUSDT", "BUY", "LIMIT", 0.1, 63000.0)
        self.db.update_intent_status(intent["id"], "FILLED", "ex123")
        pending = self.db.get_pending_intents()
        assert len(pending) == 0

    def test_pending_intents_after_restart(self):
        import tempfile, os
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        f.close()
        db1 = TradeDatabase(f.name)
        db1.create_intent("BTCUSDT", "BUY", "MARKET", 0.1)
        db1.close()
        db2 = TradeDatabase(f.name)
        pending = db2.get_pending_intents()
        assert len(pending) == 1
        db2.close()
        os.unlink(f.name)
