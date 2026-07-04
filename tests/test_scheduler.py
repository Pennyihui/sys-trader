import pytest
import threading
import queue
from scheduler.scheduler import Scheduler


class TestScheduler:
    def setup_method(self):
        self.results = queue.Queue()
        def mock_engine_run(symbol, timeframe, ohlcv):
            self.results.put((symbol, timeframe))
            return None
        self.scheduler = Scheduler(engine_run=mock_engine_run, max_workers=2)

    def test_dispatch_4h_calls_engine(self):
        self.scheduler.dispatch("BTCUSDT", "4h", [{"open": 62000}])
        try:
            symbol, timeframe = self.results.get(timeout=5)
            assert symbol == "BTCUSDT"
            assert timeframe == "4h"
        except queue.Empty:
            pytest.fail("engine_run was not called")

    def test_dispatch_weekly_calls_engine(self):
        self.scheduler.dispatch("ETHUSDT", "1w", [{"open": 3100}])
        try:
            symbol, timeframe = self.results.get(timeout=5)
            assert symbol == "ETHUSDT"
            assert timeframe == "1w"
        except queue.Empty:
            pytest.fail("engine_run was not called")

    def test_dispatch_daily_calls_engine(self):
        self.scheduler.dispatch("SOLUSDT", "1d", [{"open": 150}])
        try:
            symbol, timeframe = self.results.get(timeout=5)
            assert symbol == "SOLUSDT"
            assert timeframe == "1d"
        except queue.Empty:
            pytest.fail("engine_run was not called")

    def test_parallel_dispatch_uses_threadpool(self):
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        for s in symbols:
            self.scheduler.dispatch(s, "4h", [{"open": 100}])
        received = []
        for _ in range(3):
            try:
                received.append(self.results.get(timeout=5))
            except queue.Empty:
                break
        assert len(received) == 3
        symbols_received = {r[0] for r in received}
        assert symbols_received == set(symbols)

    def test_shutdown_waits_for_completion(self):
        self.scheduler.dispatch("BTCUSDT", "4h", [{"open": 62000}])
        self.scheduler.shutdown(wait=True)
        try:
            self.results.get(timeout=1)
        except queue.Empty:
            pytest.fail("Task should have completed before shutdown")
