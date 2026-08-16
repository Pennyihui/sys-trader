"""运维看板测试 (2026-08-16): OpsArchive 归档 + /api/ops/* 路由。"""

import json
import time

import pytest
from unittest.mock import MagicMock

from dashboard.ops_archive import OpsArchive
from shared.event_bus import Event


@pytest.fixture
def archive(tmp_path):
    a = OpsArchive(db_path=str(tmp_path / "ops.db"), retention_days=7)
    yield a
    a.close()


def _hb_event(ts: float, closes=10, placed=1, failed=0, offset=500, modules=None):
    return Event(
        stream="heartbeat",
        data={"instance": "live",
              "stats": {"kline_closes": closes, "orders_placed": placed,
                        "orders_failed": failed, "server_time_offset": offset},
              "modules": modules or {"runner": 1.0, "market_data": 2.0}},
        timestamp="2026-08-16T00:00:00+00:00",
    )


@pytest.mark.unit
class TestOpsArchive:
    def test_heartbeat_archived_and_queried(self, archive):
        archive.on_heartbeat(Event(stream="heartbeat", data={
            "instance": "live",
            "stats": {"kline_closes": 42, "orders_placed": 3, "orders_failed": 0,
                      "server_time_offset": 800},
            "modules": {"runner": 1.0},
        }, timestamp="2026-08-16T00:00:00+00:00"))
        latest = archive.latest()
        assert latest is not None
        assert latest["kline_closes"] == 42
        assert latest["server_time_offset"] == 800
        assert latest["modules"]["runner"] == 1.0
        rows = archive.history(hours=24)
        assert len(rows) == 1
        assert rows[0]["kline_closes"] == 42

    def test_command_archived(self, archive):
        archive.on_command(Event(stream="command", data={
            "command": "emergency_stop", "symbol": "", "source": "dashboard",
        }, timestamp="2026-08-16T00:01:00+00:00"))
        cmds = archive.commands()
        assert len(cmds) == 1
        assert cmds[0]["command"] == "emergency_stop"

    def test_prune_removes_old_rows(self, archive):
        old_ts = time.time() - 8 * 86400  # 8 天前
        archive.on_heartbeat(Event(stream="heartbeat", data={
            "instance": "live", "stats": {"kline_closes": 1}, "modules": {},
        }, timestamp="2026-08-16T00:00:00+00:00"))
        # 直接改 ts 模拟旧数据 (event timestamp 解析失败会回退 now, 无法注入旧值)
        archive.conn.execute(
            "UPDATE heartbeat_history SET ts=?", (old_ts,))
        archive.conn.commit()
        archive.prune()
        assert archive.history(hours=24 * 30) == []

    def test_dedupe_same_timestamp(self, archive):
        ev1 = Event(stream="heartbeat", data={"instance": "live",
                                              "stats": {"kline_closes": 1},
                                              "modules": {}},
                    timestamp="2026-08-16T00:00:00+00:00")
        ev2 = Event(stream="heartbeat", data={"instance": "live",
                                              "stats": {"kline_closes": 9},
                                              "modules": {}},
                    timestamp="2026-08-16T00:00:00+00:00")
        archive.on_heartbeat(ev1)
        archive.on_heartbeat(ev2)
        assert archive.latest()["kline_closes"] == 9  # 同 ts 覆盖
        assert len(archive.history(hours=24)) == 1

    def test_equity_and_trade_archived(self, archive):
        """面板二期: equity/close 事件 → 权益曲线 + 平仓明细。"""
        archive.on_position(Event(stream="position.changed", data={
            "event": "equity", "instance": "live", "total_equity": 10000.0,
            "available_balance": 9000.0, "margin_ratio": 0.1,
            "daily_pnl": 5.0, "drawdown": 0.01,
        }, timestamp="2026-08-16T00:00:00+00:00"))
        archive.on_position(Event(stream="position.changed", data={
            "event": "close", "instance": "live", "symbol": "BTCUSDT",
            "direction": "LONG", "entry_price": 60000.0, "exit_price": 61000.0,
            "quantity": 0.1, "gross_pnl": 100.0, "fee": 1.2, "realized_pnl": 98.8,
        }, timestamp="2026-08-16T00:01:00+00:00"))
        eq = archive.equity(hours=24)
        assert len(eq) == 1 and eq[0]["total_equity"] == 10000.0
        trades = archive.trades()
        assert len(trades) == 1
        assert trades[0]["realized_pnl"] == 98.8
        assert trades[0]["direction"] == "LONG"

    def test_alert_and_lifecycle_archived(self, archive):
        archive.on_alert(Event(stream="alert", data={
            "source": "dingtalk", "message": "心跳停滞告警",
        }, timestamp="2026-08-16T00:00:00+00:00"))
        archive.on_lifecycle(Event(stream="lifecycle", data={
            "event": "started", "pid": 1234, "instance": "live",
        }, timestamp="2026-08-16T00:00:10+00:00"))
        alerts = archive.alerts()
        assert len(alerts) == 1 and "心跳停滞" in alerts[0]["message"]
        restarts = archive.restarts()
        assert len(restarts) == 1 and restarts[0]["pid"] == 1234


@pytest.mark.unit
class TestOpsRoutes:
    def _app(self, archive):
        from dashboard.server import create_app
        collector = MagicMock()
        collector.collect.return_value = {}
        # 传自定义 collector 避免自动装配; ops_archive 显式注入
        app = create_app(data_collector=collector, event_bus=MagicMock(),
                         ops_archive=archive)
        return app

    def test_ops_summary_route(self, archive):
        archive.on_heartbeat(Event(stream="heartbeat", data={
            "instance": "live",
            "stats": {"kline_closes": 5, "orders_placed": 1, "orders_failed": 0,
                      "server_time_offset": 300},
            "modules": {"runner": 2.0},
        }, timestamp="2026-08-16T00:00:00+00:00"))
        from fastapi.testclient import TestClient
        client = TestClient(self._app(archive))
        resp = client.get("/api/ops/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["heartbeat"]["kline_closes"] == 5
        assert "uptime_seconds" in body
        assert "proxy_pool" in body and "network" in body

    def test_ops_history_and_commands_routes(self, archive):
        archive.on_heartbeat(Event(stream="heartbeat", data={
            "instance": "live",
            "stats": {"kline_closes": 7, "orders_placed": 2, "orders_failed": 1,
                      "server_time_offset": 100},
            "modules": {},
        }, timestamp="2026-08-16T00:00:00+00:00"))
        archive.on_command(Event(stream="command", data={
            "command": "pause", "symbol": "", "source": "telegram",
        }, timestamp="2026-08-16T00:02:00+00:00"))
        from fastapi.testclient import TestClient
        client = TestClient(self._app(archive))
        hist = client.get("/api/ops/history?hours=24").json()
        assert len(hist["points"]) == 1
        assert hist["points"][0]["orders_failed"] == 1
        cmds = client.get("/api/ops/commands").json()
        assert cmds["commands"][0]["command"] == "pause"

    def test_ops_soak_route_empty_ok(self, archive, monkeypatch, tmp_path):
        """soak CSV 不存在时返回空 rows (隔离真实 logs/ 目录)。"""
        import dashboard.server as server_mod
        monkeypatch.setattr(server_mod, "PROJECT_ROOT", tmp_path)
        from fastapi.testclient import TestClient
        client = TestClient(self._app(archive))
        resp = client.get("/api/ops/soak")
        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert body["total_errors"] == 0

    def test_ops_routes_degrade_without_archive(self):
        from dashboard.server import create_app
        from fastapi.testclient import TestClient
        collector = MagicMock()
        collector.collect.return_value = {}
        app = create_app(data_collector=collector, event_bus=MagicMock(),
                         ops_archive=None)
        client = TestClient(app)
        assert client.get("/api/ops/summary").json()["heartbeat"] is None
        assert client.get("/api/ops/history").json()["points"] == []
        assert client.get("/api/ops/commands").json()["commands"] == []

    def test_ops_new_routes(self, archive):
        """面板二期路由: equity/trades/alerts/restarts/kline。"""
        archive.on_position(Event(stream="position.changed", data={
            "event": "equity", "instance": "live", "total_equity": 9999.0,
            "available_balance": 8000.0, "margin_ratio": 0.2,
            "daily_pnl": 0.0, "drawdown": 0.0,
        }, timestamp="2026-08-16T00:00:00+00:00"))
        from fastapi.testclient import TestClient
        from dashboard.server import create_app
        app = create_app(data_collector=MagicMock(collect=MagicMock(return_value={})),
                         event_bus=MagicMock(), ops_archive=archive)
        client = TestClient(app)
        assert len(client.get("/api/ops/equity?hours=24").json()["points"]) == 1
        assert client.get("/api/ops/trades").json()["trades"] == []
        assert client.get("/api/ops/alerts").json()["alerts"] == []
        assert client.get("/api/ops/restarts").json()["restarts"] == []
        k = client.get("/api/kline?symbol=BTCUSDT&timeframe=15m").json()
        assert k["symbol"] == "BTCUSDT"
