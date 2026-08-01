"""测试 IdempotencyTracker — 薄封装 TradeDatabase 的 order_intents 表。"""

import pytest
from shared.idempotency import IdempotencyTracker


class TestIdempotency:
    def setup_method(self):
        self.tracker = IdempotencyTracker(db_path=":memory:")

    def test_create_intent_returns_valid(self):
        intent = self.tracker.create_intent("BTCUSDT", "BUY", "LIMIT", 0.1, 64000.0)
        assert intent["status"] == "PENDING"
        assert intent["client_order_id"].startswith("sys_")

    def test_update_status_changes_state(self):
        intent = self.tracker.create_intent("BTCUSDT", "SELL", "MARKET", 0.1)
        self.tracker.update_status(intent["id"], "FILLED", "ex123")
        pending = self.tracker.get_pending_intents()
        assert len(pending) == 0

    def test_pending_intents_after_restart(self):
        import tempfile, os
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        f.close()
        t1 = IdempotencyTracker(db_path=f.name)
        t1.create_intent("BTCUSDT", "BUY", "MARKET", 0.1)
        t1.close()
        t2 = IdempotencyTracker(db_path=f.name)
        pending = t2.get_pending_intents()
        assert len(pending) == 1
        t2.close()
        os.unlink(f.name)
