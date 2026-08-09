"""端到端：埋点 publish → StateStore → DataCollector → collect。

覆盖 Task 5-12 全链路：PositionTracker / SignalEngine 埋点事件
经 StateStore 消费（instance 过滤）后，由 DataCollector.collect()
聚合为 dashboard 数据。

网络隔离：DataCollector.collect() 会调用 _collect_proxy_pool /
_collect_network（真实 HTTP 到 127.0.0.1:8765/8766，各 3s timeout）。
本文件不依赖这两个外部服务——用 fixture 将二者替换为本地桩
（unavailable 分支），保证 unit 级、无网络依赖。
"""

import pytest
from unittest.mock import MagicMock

from shared.event_bus import Event
from dashboard.state_store import StateStore
from dashboard.data_collector import DataCollector


@pytest.fixture
def network_patched_collector(monkeypatch):
    """构造 DataCollector 并打桩 Proxy Pool / Network Monitor HTTP 采集。

    测试环境 127.0.0.1:8765/8766 服务不保证在跑；collect() 若走真实
    HTTP 会各阻塞 3s 且返回 unavailable。这里直接替换为本地桩，
    使测试不依赖外部服务（unit 级）。
    """
    def _build(store, feed):
        collector = DataCollector(state_store=store, feed=feed)
        monkeypatch.setattr(collector, "_collect_proxy_pool", lambda: {"status": "unavailable"})
        monkeypatch.setattr(collector, "_collect_network", lambda: {"status": "unavailable"})
        return collector
    return _build


@pytest.mark.unit
def test_full_chain_position_to_collect(mock_feed, network_patched_collector):
    """position.changed → StateStore → DataCollector.collect 返回持仓。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(Event(stream="position.changed", data={
        "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
        "quantity": 0.1, "entry_price": 63000.0, "instance": "live"}))
    store._handle(Event(stream="position.changed", data={
        "event": "equity", "total_equity": 10000.0, "instance": "live"}))

    collector = network_patched_collector(store, mock_feed)
    data = collector.collect()

    assert data["position_count"] == 1
    assert data["positions"][0]["symbol"] == "BTCUSDT"
    assert data["equity"] == 10000.0
    # 实时 upnl：(mark 64000 - entry 63000) * 0.1 * LONG(1) = 100.0
    assert data["positions"][0]["unrealized_pnl"] == 100.0


@pytest.mark.unit
def test_shadow_instance_filtered_from_collect():
    """影子实例（paper）事件不进 dashboard 状态。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(Event(stream="signal.generated", data={
        "instance": "paper", "symbol": "BTCUSDT", "direction": "LONG"}))
    assert store.signals == []


@pytest.mark.unit
def test_full_signal_chain_to_collect(mock_feed, network_patched_collector):
    """signal.generated → approved → collect 透出决策。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(Event(stream="signal.generated", data={
        "instance": "live", "symbol": "BTCUSDT", "direction": "LONG",
        "conviction": 0.8, "signal_id": "s1"}))
    store._handle(Event(stream="signal.approved", data={
        "instance": "live", "symbol": "BTCUSDT", "direction": "LONG",
        "signal_id": "s1", "modifications": {"position_size": 0.001}}))

    collector = network_patched_collector(store, mock_feed)
    data = collector.collect()

    assert len(data["signals"]) == 2
    assert data["signals"][1]["decision"] == "signal.approved"
