"""HeartbeatPublisher 测试。"""

import time

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
def test_payload_includes_stats_gauges():
    """payload 携带 stats: kline_closes / orders_placed / orders_failed gauges。"""
    bus = MagicMock()
    collector = MetricsCollector.instance()
    collector.set_gauge("kline_closes", 42)
    collector.set_gauge("orders_placed", 7)
    collector.set_gauge("orders_failed", 1)
    publisher = HeartbeatPublisher(bus, interval=0.05)
    publisher._run_once()
    stream, payload = bus.publish.call_args[0]
    assert stream == "heartbeat"
    assert payload["stats"] == {
        "kline_closes": 42, "orders_placed": 7, "orders_failed": 1,
        "server_time_offset": 0.0,
        "ws_connected": 0.0, "ws_total": 0.0, "funding_cost": 0.0,
        "risk_per_trade": 0.0, "max_leverage": 0.0,
        # 风控补强 (2026-08-16 #3/#4)
        "max_trades_day": 0.0, "max_stop_pct": 0.0,
    }


@pytest.mark.unit
def test_stop_clears_flag():
    bus = MagicMock()
    publisher = HeartbeatPublisher(bus, interval=0.05)
    publisher.start()
    publisher.stop()
    assert publisher._stop.is_set()


@pytest.mark.unit
def test_event_bus_none_silent():
    """event_bus=None (注入模式) 时 _run_once 静默返回, 不抛异常。"""
    publisher = HeartbeatPublisher(None, interval=0.05)
    publisher._run_once()  # 不抛异常即通过


@pytest.mark.unit
def test_run_loop_survives_exception(monkeypatch):
    """_run_once 抛异常时 _run_loop 不退出 (捕获后继续下一轮)。"""
    attempts = []

    def exploding_run_once(self):
        attempts.append(1)
        raise RuntimeError("boom")

    monkeypatch.setattr(HeartbeatPublisher, "_run_once", exploding_run_once)
    publisher = HeartbeatPublisher(MagicMock(), interval=0.01)
    publisher.start()
    # 有界等待: 原实现固定 sleep 0.05s 断言 >=2, 机器负载高时会偶发 1 次 (flaky)
    deadline = time.time() + 2.0
    while len(attempts) < 2 and time.time() < deadline:
        time.sleep(0.02)
    publisher.stop()
    assert len(attempts) >= 2  # 抛异常后循环仍存活


@pytest.mark.unit
def test_start_twice_does_not_spawn_second_thread():
    """start() 双重调用保护: 已运行时不重复启动。"""
    bus = MagicMock()
    publisher = HeartbeatPublisher(bus, interval=0.01)
    publisher.start()
    first_thread = publisher._thread
    publisher.start()
    assert publisher._thread is first_thread
    publisher.stop()
