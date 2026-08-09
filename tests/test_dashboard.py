import pytest
from unittest.mock import MagicMock
from dashboard.data_collector import DataCollector


@pytest.mark.unit
class TestDataCollector:
    def setup_method(self):
        self.state = MagicMock()
        self.state.positions = {"BTCUSDT": {
            "symbol": "BTCUSDT", "direction": "LONG", "quantity": 0.1,
            "entry_price": 63000.0, "mark_price": 64000.0, "unrealized_pnl": 100.0}}
        self.state.equity = 10000.0
        self.state.margin_ratio = 0.12
        self.state.daily_pnl = 50.0
        self.state.drawdown = 0.03
        self.state.signals = []
        self.state.orders = []
        self.state.heartbeats = {}
        self.feed = MagicMock()
        self.feed.get_last_price.return_value = 64000.0
        self.feed.get_mark_price.return_value = 64000.0
        self.collector = DataCollector(state_store=self.state, feed=self.feed)

    def test_collect_returns_all_fields(self):
        data = self.collector.collect()
        assert "equity" in data
        assert "margin_ratio" in data
        assert "daily_pnl" in data
        assert "positions" in data
        assert "prices" in data

    def test_collect_btc_mark_price(self):
        data = self.collector.collect()
        assert data["prices"]["BTCUSDT"]["mark"] == 64000.0
        assert data["position_count"] == 1
        assert data["positions"][0]["unrealized_pnl"] == 100.0

    def test_unrealized_pnl_computed_from_mark(self):
        """真实 open payload（无 unrealized_pnl 字段）→ collect 时按 mark 实时计算。"""
        self.state.positions = {"BTCUSDT": {
            "symbol": "BTCUSDT", "direction": "LONG", "quantity": 0.1,
            "entry_price": 63000.0, "instance": "live"}}
        self.feed.get_mark_price.return_value = 65000.0
        data = self.collector.collect()
        assert data["positions"][0]["unrealized_pnl"] == 200.0  # (65000-63000)*0.1

    def test_open_event_to_collect_position_count(self):
        """真实 open payload 走 StateStore → collect 出现持仓（含实时 upnl）。"""
        from dashboard.state_store import StateStore
        from shared.event_bus import Event
        store = StateStore(event_bus=MagicMock(), instance_filter="live")
        store._handle(Event(stream="position.changed", data={
            "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
            "quantity": 0.1, "entry_price": 63000.0, "instance": "live"}))
        collector = DataCollector(state_store=store, feed=self.feed)
        data = collector.collect()
        assert data["position_count"] == 1
        assert data["positions"][0]["unrealized_pnl"] == 100.0  # (64000-63000)*0.1

    def test_empty_positions_returns_empty_list(self):
        self.state.positions = {}
        data = self.collector.collect()
        assert data["positions"] == []

    def test_drawdown_included(self):
        data = self.collector.collect()
        assert "drawdown" in data

    def test_signals_orders_heartbeats_included(self):
        data = self.collector.collect()
        assert data["signals"] == []
        assert data["orders"] == []
        assert data["heartbeats"] == {}


@pytest.mark.unit
class TestCreateApp:
    def test_create_app_wires_state_store_and_feed(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1")  # 不可达：StateStore 启动失败不崩溃
        from dashboard.server import create_app
        app = create_app()
        assert app is not None

    def test_websocket_command_publishes(self):
        """dashboard 命令 → command 事件流。"""
        from dashboard.server import handle_ws_command
        bus = MagicMock()
        handle_ws_command(bus, "emergency_stop")
        bus.publish.assert_called_once_with("command", {"command": "emergency_stop"})
