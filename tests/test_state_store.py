"""StateStore 测试 — 事件消费线程与状态维护。"""

import time
import pytest
from unittest.mock import MagicMock, patch
from shared.event_bus import Event
from dashboard.state_store import StateStore


@pytest.mark.unit
class TestStateStore:
    def setup_method(self):
        self.bus = MagicMock()
        self.store = StateStore(event_bus=self.bus, instance_filter="live")

    def test_handle_position_changed(self):
        self.store._handle(Event(stream="position.changed", data={
            "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
            "quantity": 0.1, "entry_price": 64000.0, "instance": "live"}))
        assert self.store.positions["BTCUSDT"]["direction"] == "LONG"

    def test_handle_position_changed_filters_instance(self):
        self.store._handle(Event(stream="position.changed", data={
            "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
            "instance": "paper"}))
        assert self.store.positions == {}  # 影子实例被过滤

    def test_handle_signal_generated_filters_instance(self):
        self.store._handle(Event(stream="signal.generated", data={
            "instance": "paper", "symbol": "BTCUSDT", "direction": "LONG"}))
        assert self.store.signals == []  # 影子实例被过滤

        self.store._handle(Event(stream="signal.generated", data={
            "instance": "live", "symbol": "BTCUSDT", "direction": "LONG"}))
        assert len(self.store.signals) == 1

    def test_signals_bounded_to_50(self):
        for i in range(60):
            self.store._handle(Event(stream="signal.generated", data={
                "instance": "live", "symbol": "X", "direction": "LONG", "conviction": 0.5}))
        assert len(self.store.signals) == 50

    def test_handle_signal_approved_filters_instance(self):
        self.store._handle(Event(stream="signal.approved", data={
            "instance": "paper", "symbol": "BTCUSDT", "direction": "LONG"}))
        assert self.store.signals == []  # 影子实例被过滤

        self.store._handle(Event(stream="signal.approved", data={
            "instance": "live", "symbol": "BTCUSDT", "direction": "LONG"}))
        assert len(self.store.signals) == 1
        assert self.store.signals[0]["decision"] == "signal.approved"

    def test_handle_heartbeat(self):
        self.store._handle(Event(stream="heartbeat", data={
            "instance": "live", "modules": {"market_data": 0.2, "runner": 1.0}}))
        assert self.store.heartbeats["market_data"] == 0.2

    def test_equity_snapshot(self):
        self.store._handle(Event(stream="position.changed", data={
            "event": "equity", "total_equity": 12345.0, "instance": "live",
            "margin_ratio": 0.42, "daily_pnl": 120.5, "drawdown": 0.03}))
        assert self.store.equity == 12345.0
        assert self.store.margin_ratio == 0.42
        assert self.store.daily_pnl == 120.5
        assert self.store.drawdown == 0.03

    def test_metrics_default_when_payload_missing(self):
        """旧 payload 无指标字段时保持默认值（向后兼容）。"""
        self.store._handle(Event(stream="position.changed", data={
            "event": "equity", "total_equity": 9999.0, "instance": "live"}))
        assert self.store.equity == 9999.0
        assert self.store.margin_ratio == 1.0
        assert self.store.daily_pnl == 0.0
        assert self.store.drawdown == 0.0

    def test_stop_does_not_kill_shared_bus(self):
        """stop() 只 join 本实例线程，不调用共享 bus.stop()（Task 12 地雷回归）。"""
        self.store.stop()
        self.bus.stop.assert_not_called()
