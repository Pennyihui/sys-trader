"""HeartbeatPublisher 测试。"""

import pytest
from unittest.mock import MagicMock

from monitor.collector import MetricsCollector
from shared.heartbeat_publisher import HeartbeatPublisher


@pytest.fixture(autouse=True)
def _isolate_collector():
    """MetricsCollector 是跨测试文件共享的单例 — 每个测试前后重置隔离。"""
    MetricsCollector.reset()
    yield
    MetricsCollector.reset()


@pytest.mark.unit
def test_publishes_heartbeat_with_module_times():
    bus = MagicMock()
    collector = MetricsCollector.instance()
    collector.heartbeat("market_data")
    publisher = HeartbeatPublisher(bus, interval=0.05)
    publisher._run_once()
    bus.publish.assert_called_once()
    stream, payload = bus.publish.call_args[0]
    assert stream == "heartbeat"
    assert "market_data" in payload["modules"]


@pytest.mark.unit
def test_stop_clears_flag():
    bus = MagicMock()
    publisher = HeartbeatPublisher(bus, interval=0.05)
    publisher.start()
    publisher.stop()
    assert publisher._stop.is_set()
