"""StateStore 测试 — 事件消费线程与状态维护。"""

import time
import pytest
from unittest.mock import MagicMock, patch
from dashboard.state_store import StateStore


@pytest.mark.unit
class TestStateStore:
    def setup_method(self):
        self.bus = MagicMock()
        self.store = StateStore(event_bus=self.bus, instance_filter="live")

    def test_handle_position_changed(self):
        self.store._handle({"stream": "position.changed", "data": {
            "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
            "quantity": 0.1, "entry_price": 64000.0}})
        assert self.store.positions["BTCUSDT"]["direction"] == "LONG"

    def test_handle_signal_generated_filters_instance(self):
        self.store._handle({"stream": "signal.generated", "data": {
            "instance": "paper", "symbol": "BTCUSDT", "direction": "LONG"}})
        assert self.store.signals == []  # 影子实例被过滤

        self.store._handle({"stream": "signal.generated", "data": {
            "instance": "live", "symbol": "BTCUSDT", "direction": "LONG"}})
        assert len(self.store.signals) == 1

    def test_signals_bounded_to_50(self):
        for i in range(60):
            self.store._handle({"stream": "signal.generated", "data": {
                "instance": "live", "symbol": "X", "direction": "LONG", "conviction": 0.5}})
        assert len(self.store.signals) == 50

    def test_handle_signal_approved_filters_instance(self):
        self.store._handle({"stream": "signal.approved", "data": {
            "instance": "paper", "symbol": "BTCUSDT", "direction": "LONG"}})
        assert self.store.signals == []  # 影子实例被过滤

        self.store._handle({"stream": "signal.approved", "data": {
            "instance": "live", "symbol": "BTCUSDT", "direction": "LONG"}})
        assert len(self.store.signals) == 1
        assert self.store.signals[0]["decision"] == "signal.approved"

    def test_handle_heartbeat(self):
        self.store._handle({"stream": "heartbeat", "data": {
            "modules": {"market_data": 0.2, "runner": 1.0}}})
        assert self.store.heartbeats["market_data"] == 0.2

    def test_equity_snapshot(self):
        self.store._handle({"stream": "position.changed", "data": {
            "event": "equity", "total_equity": 12345.0}})
        assert self.store.equity == 12345.0
