"""端到端：埋点 publish → StateStore → DataCollector → collect。

覆盖 Task 5-12 全链路：PositionTracker / SignalEngine 埋点事件
经 StateStore 消费（instance 过滤）后，由 DataCollector.collect()
聚合为 dashboard 数据。含：真实 tracker 埋点驱动（防 T5 payload 与
T10/T11 消费侧漂移）、order.filled、close 分支、SHORT 方向。

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
from portfolio.tracker import PortfolioTracker, Position


class _CaptureBus:
    """捕获型 EventBus 替身：publish 时记录 (stream, data)，不碰 Redis。"""

    def __init__(self):
        self.events: list = []

    def publish(self, stream: str, data: dict) -> str:
        self.events.append((stream, data))
        return "captured"


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


@pytest.mark.unit
def test_real_tracker_publish_to_collect(mock_feed, network_patched_collector):
    """真实 PortfolioTracker 埋点 payload → StateStore → collect。

    用真实 tracker + 捕获型 bus 驱动：验证 T5 实际发布的 payload 形状
    （open 事件含 symbol/direction/quantity/entry_price/instance）能被
    T10 消费侧与 T11 collect 侧正确消费，防两端 payload 漂移。
    """
    bus = _CaptureBus()
    tracker = PortfolioTracker(initial_equity=10000.0, event_bus=bus, instance="live")
    tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 63000.0, 3))

    assert bus.events, "tracker 应发布 position.changed 事件"
    stream, data = bus.events[0]
    assert stream == "position.changed"
    assert data["instance"] == "live"

    store = StateStore(event_bus=MagicMock(), instance_filter="live")
    store._handle(Event(stream=stream, data=data))

    collector = network_patched_collector(store, mock_feed)
    result = collector.collect()

    assert result["position_count"] == 1
    assert result["positions"][0]["symbol"] == "BTCUSDT"
    assert result["positions"][0]["direction"] == "LONG"
    assert result["positions"][0]["entry_price"] == 63000.0


@pytest.mark.unit
def test_order_filled_to_collect(network_patched_collector):
    """order.filled → collect 透出订单列表。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(Event(stream="order.filled", data={
        "instance": "live", "symbol": "BTCUSDT", "side": "BUY",
        "order_type": "LIMIT", "quantity": 0.1, "price": 63000.0,
        "order_id": "o1"}))

    collector = network_patched_collector(store, MagicMock())
    data = collector.collect()

    assert len(data["orders"]) == 1
    assert data["orders"][0]["order_id"] == "o1"
    assert data["orders"][0]["side"] == "BUY"
    assert data["orders"][0]["symbol"] == "BTCUSDT"


@pytest.mark.unit
def test_position_close_removes_and_updates_equity(network_patched_collector):
    """position.changed close → 持仓移除 + equity/metrics 更新。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(Event(stream="position.changed", data={
        "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
        "quantity": 0.1, "entry_price": 63000.0, "instance": "live"}))
    store._handle(Event(stream="position.changed", data={
        "event": "close", "symbol": "BTCUSDT", "realized_pnl": 100.0,
        "total_equity": 10100.0, "margin_ratio": 0.6, "daily_pnl": 100.0,
        "drawdown": 0.02, "instance": "live"}))

    collector = network_patched_collector(store, MagicMock())
    data = collector.collect()

    assert data["position_count"] == 0
    assert data["positions"] == []
    assert data["equity"] == 10100.0
    assert data["margin_ratio"] == 0.6
    assert data["daily_pnl"] == 100.0


@pytest.mark.unit
def test_short_direction_upnl_negative(network_patched_collector):
    """SHORT 方向：行情反向（mark > entry）时 upnl 为负。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(Event(stream="position.changed", data={
        "event": "open", "symbol": "BTCUSDT", "direction": "SHORT",
        "quantity": 0.1, "entry_price": 64000.0, "instance": "live"}))

    feed = MagicMock()
    feed.get_mark_price.return_value = 65000.0
    feed.get_last_price.return_value = 65000.0
    collector = network_patched_collector(store, feed)
    data = collector.collect()

    # SHORT：(mark 65000 - entry 64000) * 0.1 * (-1) = -100.0
    assert data["positions"][0]["unrealized_pnl"] == -100.0
