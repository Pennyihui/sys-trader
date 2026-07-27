import pytest
from shared.idempotency import IdempotencyTracker, IntentStatus, OrderIntent


class TestIdempotency:
    def setup_method(self):
        self.tracker = IdempotencyTracker(":memory:")

    def test_create_intent_returns_valid(self):
        intent = self.tracker.create_intent("BTCUSDT", "BUY", "LIMIT", 0.1, 64000.0)
        assert intent.status == IntentStatus.PENDING
        assert intent.client_order_id.startswith("sys_")

    def test_update_status_changes_state(self):
        intent = self.tracker.create_intent("BTCUSDT", "SELL", "MARKET", 0.1)
        self.tracker.update_status(intent.id, IntentStatus.FILLED, "ex123")
        pending = self.tracker.get_pending_intents()
        assert len(pending) == 0

    def test_pending_intents_after_restart(self):
        self.tracker.create_intent("BTCUSDT", "BUY", "MARKET", 0.1)
        self.tracker.create_intent("ETHUSDT", "SELL", "LIMIT", 1.0, 3100.0)
        # 模拟重启后读取
        tracker2 = IdempotencyTracker(":memory:")  # 不同实例
        # 用同一个文件才能模拟重启，:memory: 每个实例独立
        # 用文件模式
        import tempfile, os
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        f.close()
        t1 = IdempotencyTracker(f.name)
        t1.create_intent("BTCUSDT", "BUY", "MARKET", 0.1)
        t1.close()
        t2 = IdempotencyTracker(f.name)
        pending = t2.get_pending_intents()
        assert len(pending) == 1
        t2.close()
        os.unlink(f.name)
