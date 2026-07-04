import pytest
import threading
from monitor.collector import MetricsCollector


class TestMetricsCollector:
    def setup_method(self):
        MetricsCollector.reset()
        self.collector = MetricsCollector.instance()

    def test_singleton_returns_same_instance(self):
        c1 = MetricsCollector.instance()
        c2 = MetricsCollector.instance()
        assert c1 is c2

    def test_record_heartbeat_updates_timestamp(self):
        self.collector.heartbeat("market_data")
        last = self.collector.last_heartbeat("market_data")
        assert last is not None
        assert last > 0

    def test_missing_heartbeat_returns_none(self):
        assert self.collector.last_heartbeat("nonexistent") is None

    def test_increment_counter_adds(self):
        self.collector.increment("trades.today")
        self.collector.increment("trades.today")
        assert self.collector.get_counter("trades.today") == 2

    def test_unknown_counter_returns_zero(self):
        assert self.collector.get_counter("unknown.counter") == 0

    def test_set_gauge_stores_value(self):
        self.collector.set_gauge("margin_ratio", 0.45)
        assert self.collector.get_gauge("margin_ratio") == 0.45

    def test_reset_clears_all_metrics(self):
        self.collector.heartbeat("test")
        self.collector.increment("test.counter")
        assert self.collector.last_heartbeat("test") is not None
        MetricsCollector.reset()
        assert MetricsCollector.instance().last_heartbeat("test") is None
        assert MetricsCollector.instance().get_counter("test.counter") == 0

    def test_thread_safety_concurrent_heartbeats(self):
        def send_heartbeats():
            for _ in range(100):
                self.collector.heartbeat("market_data")

        threads = [threading.Thread(target=send_heartbeats) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert self.collector.last_heartbeat("market_data") is not None
