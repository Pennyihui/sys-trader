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

    def test_tickers_filtered_to_whitelist(self, monkeypatch):
        """行情条只返回白名单交易对 (2026-08-16: 修复全市场刷屏)。"""
        monkeypatch.setenv("DASHBOARD_SYMBOLS", "BTCUSDT,ETHUSDT")
        all_market = [
            {"symbol": "BTCUSDT", "lastPrice": "63000", "priceChangePercent": "1.5",
             "highPrice": "64000", "lowPrice": "62000"},
            {"symbol": "ETHUSDT", "lastPrice": "1880", "priceChangePercent": "-0.5",
             "highPrice": "1900", "lowPrice": "1860"},
            {"symbol": "我踏马来了USDT", "lastPrice": "0.01", "priceChangePercent": "4.4",
             "highPrice": "0.02", "lowPrice": "0.01"},
        ]

        class _Resp:
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False
            def read(self):
                import json as _j
                return _j.dumps(all_market).encode()

        class _Opener:
            def open(self, req, timeout):
                return _Resp()

        import dashboard.data_collector as dc
        dc._TICKER_CACHE = []
        dc._TICKER_CACHE_TS = 0.0
        monkeypatch.setattr(dc.urllib.request, "build_opener", lambda handler: _Opener())
        tickers = self.collector._collect_tickers()
        assert [t["symbol"] for t in tickers] == ["BTCUSDT", "ETHUSDT"]


@pytest.mark.unit
class TestCreateApp:
    def test_create_app_wires_state_store_and_feed(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1")  # 不可达：StateStore 启动失败不崩溃
        from dashboard.server import create_app
        app = create_app()
        assert app is not None

    def test_websocket_command_publishes(self):
        """dashboard 命令 → command 事件流 (发布成功返回 True)。"""
        from dashboard.server import handle_ws_command
        bus = MagicMock()
        bus.publish.return_value = "id-1"
        ok = handle_ws_command(bus, "emergency_stop")
        assert ok is True
        bus.publish.assert_called_once_with("command", {"command": "emergency_stop"})

    def test_websocket_command_publish_failure_returns_false(self):
        """publish 失败 (Redis down) / 无总线 → False, WS 端回发 ack 失败。"""
        from dashboard.server import handle_ws_command
        bus = MagicMock()
        bus.publish.return_value = ""  # EventBus.publish 失败返回 ""
        assert handle_ws_command(bus, "emergency_stop") is False
        assert handle_ws_command(None, "emergency_stop") is False

    def test_create_app_with_custom_collector(self, monkeypatch):
        """传入自定义 collector 时不触发自动装配（UnboundLocalError 回归）。"""
        from dashboard.server import create_app

        def _boom(*a, **k):
            raise AssertionError("auto-assembly ran despite custom collector")

        monkeypatch.setattr("shared.config_loader.load_env", _boom)
        collector = MagicMock()
        app = create_app(data_collector=collector)
        assert app is not None

    def test_create_app_strips_and_guards_symbols(self, monkeypatch):
        """DASHBOARD_SYMBOLS 解析：strip 空格 + 过滤空项。"""
        monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1")
        monkeypatch.setenv("DASHBOARD_SYMBOLS", " BTCUSDT, ETHUSDT ,,SOLUSDT,")
        captured = {}

        class FakeFeed:
            def __init__(self, symbols, **kw):
                captured["symbols"] = symbols

            def start(self):
                pass

        monkeypatch.setattr("market_data.feed.MarketDataFeed", FakeFeed)
        from dashboard.server import create_app
        app = create_app()
        assert app is not None
        assert captured["symbols"] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    def test_create_app_feed_uses_proxy_env(self, monkeypatch):
        """PROXY_HOST/PROXY_PORT 环境变量传入 feed 构造 (docker 路径)。"""
        monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1")  # 不可达：StateStore 启动失败不崩溃
        monkeypatch.setenv("PROXY_HOST", "host.docker.internal")
        monkeypatch.setenv("PROXY_PORT", "7890")
        captured = {}

        class FakeFeed:
            def __init__(self, symbols, **kw):
                captured.update(kw)

            def start(self):
                pass

        monkeypatch.setattr("market_data.feed.MarketDataFeed", FakeFeed)
        from dashboard.server import create_app
        app = create_app()
        assert app is not None
        assert captured["proxy_host"] == "host.docker.internal"
        assert captured["proxy_port"] == 7890
