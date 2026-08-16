"""2026-08-16 P0-P2 功能补充的回归测试。

覆盖: 杠杆/账户配置、User Data Stream 解析、手续费计入盈亏、
权益口径、postOnly、价格保护、手动平仓命令、资金费接线、K线归档、深度滑点估算、
Telegram 命令路由、保留策略。
"""

import os
import time

import pytest
from unittest.mock import MagicMock, patch

from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse
from execution.order_manager import OrderManager, OrderState
from market_data.kline_archive import KlineArchive
from market_data.kline_buffer import Kline
from market_data.orderbook import OrderbookDepth
from market_data.user_data_stream import UserDataStream
from portfolio.tracker import PortfolioTracker, Position
from shared.database import TradeDatabase
from shared.execution_mode import ExecutionMode, ExecutionModeManager
from shared.runner import SystemRunner


# ─── P0-1 杠杆/账户配置 ───


@pytest.mark.unit
def test_gateway_change_leverage_posts():
    gw = OrderGateway(testnet=True)
    with patch.object(gw, "_request", return_value={"leverage": 3}) as mock_req:
        assert gw.change_leverage("BTCUSDT", 3) == 3
    args = mock_req.call_args[0]
    assert args[0] == "POST"
    assert "leverage" in args[1]
    assert args[2]["leverage"] == "3"
    assert args[2]["symbol"] == "BTCUSDT"


@pytest.mark.unit
def test_gateway_position_mode_and_cancel_all():
    gw = OrderGateway(testnet=True)
    with patch.object(gw, "_request", return_value={"dualSidePosition": True}):
        assert gw.get_position_mode_dual() is True
    with patch.object(gw, "_request", return_value=[{"orderId": 1}, {"orderId": 2}]):
        assert gw.cancel_all_open_orders("BTCUSDT") == 2
    with patch.object(gw, "_request", return_value={"code": -2011, "msg": "x"}):
        assert gw.cancel_all_open_orders("BTCUSDT") == -1


@pytest.mark.unit
def test_runner_sync_account_config_calls_gateway():
    r = SystemRunner()
    r.gateway = MagicMock()
    r.gateway.get_position_mode_dual.return_value = False
    r.gateway.change_leverage.return_value = 3
    r.engine = MagicMock()
    r.engine.strategy.leverage.return_value = 3
    r._sync_account_config()
    assert r.gateway.change_leverage.call_count == 3  # 3 symbols
    assert r.gateway.set_margin_type.call_count == 3


# ─── P0-2 User Data Stream ───


@pytest.mark.unit
def test_user_stream_routes_order_update():
    got = []
    gw = MagicMock()
    gw.testnet = True
    stream = UserDataStream(gateway=gw, on_order_update=got.append)
    stream._on_message('{"e":"ORDER_TRADE_UPDATE","o":{"i":42,"X":"FILLED","z":"0.1","ap":"64000"}}')
    assert len(got) == 1
    assert got[0]["i"] == 42
    assert got[0]["X"] == "FILLED"


@pytest.mark.unit
def test_user_stream_listen_key_expired_refreshes():
    gw = MagicMock()
    gw.testnet = True
    gw.create_listen_key.return_value = "key2"
    stream = UserDataStream(gateway=gw)
    stream._listen_key = "key1"
    stream._on_message('{"e":"listenKeyExpired"}')
    assert stream._listen_key == "key2"


@pytest.mark.unit
def test_order_manager_on_user_order_update_fills_entry():
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=42, symbol="BTCUSDT", side="BUY", status="NEW",
        executed_qty=0.0, avg_price=0.0)
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    entry = mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    assert entry.state == OrderState.PENDING
    newly = mgr.on_user_order_update({"i": 42, "c": entry.client_order_id,
                                      "X": "FILLED", "z": "0.1", "ap": "63900"})
    assert len(newly) == 1
    assert entry.state == OrderState.FILLED
    assert entry.filled_qty == 0.1


# ─── P0-3 手续费计入盈亏 ───


@pytest.mark.unit
def test_close_position_deducts_fees():
    t = PortfolioTracker(initial_equity=10000.0, fee_rate=0.001)
    t.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
    pnl = t.close_position("BTCUSDT", 61000.0)
    gross = 100.0
    fee = (60000.0 + 61000.0) * 0.1 * 0.001  # 12.1
    assert pnl == pytest.approx(gross - fee)
    assert t.total_fees == pytest.approx(fee)
    assert t.total_equity == pytest.approx(10000.0 + gross - fee)


# ─── P0-4 权益口径 ───


@pytest.mark.unit
def test_refresh_equity_prefers_total_wallet_balance():
    r = SystemRunner()
    r.gateway = MagicMock()
    r.gateway.get_account.return_value = {
        "totalWalletBalance": "9800",
        "assets": [{"walletBalance": "9000", "availableBalance": "8500", "asset": "USDT"}],
    }
    r.portfolio = MagicMock()
    r._refresh_equity()
    r.portfolio.update_equity.assert_called_once_with(
        9800.0, available_balance=8500.0,
        assets=[{"asset": "USDT", "walletBalance": 9000.0}])


# ─── P1-1 价格保护 ───


@pytest.mark.unit
def test_price_deviation_guard_rejects_stale_signal(monkeypatch):
    monkeypatch.setenv("MAX_ENTRY_DEVIATION", "0.005")
    r = SystemRunner()
    r.gateway = MagicMock()
    r.orders = MagicMock()
    r.orders.active_orders = []
    r.portfolio = MagicMock()
    r.portfolio.positions = {}
    r.risk_chain = MagicMock()
    r.risk_chain.process.return_value = MagicMock(
        rejected=False, reason="", modifications={"position_size": 0.01})
    r.step_sizes = {"BTCUSDT": 0.001}
    r.feed = MagicMock()
    r.feed.get_last_price.return_value = 60000.0  # 现价与信号价差 > 0.5%
    sig = MagicMock(symbol="BTCUSDT", direction="LONG", conviction=0.8,
                    entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0)
    r._execute_signal(sig)
    r.orders.execute_signal.assert_not_called()
    assert r.stats["risk_rejected"] == 1


# ─── P0-6 手动平仓命令 ───


@pytest.mark.unit
def test_force_exit_closes_position():
    r = SystemRunner()
    r.portfolio = PortfolioTracker(initial_equity=10000.0, fee_rate=0.001)
    r.portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
    r.feed = MagicMock()
    r.feed.get_last_price.return_value = 60500.0
    r.gateway = MagicMock()
    r.gateway.place_order.return_value = OrderResponse(
        order_id=7, symbol="BTCUSDT", side="SELL", status="FILLED",
        executed_qty=0.1, avg_price=60500.0)
    r.orders = MagicMock()
    r.orders.active_orders = []
    r._force_exit_symbol("BTCUSDT")
    assert "BTCUSDT" not in r.portfolio.positions
    req = r.gateway.place_order.call_args[0][0]
    assert req.side == "SELL"
    assert req.reduce_only is True
    assert req.order_type == "MARKET"


@pytest.mark.unit
def test_handle_command_routes_new_commands():
    r = SystemRunner()
    r.gateway = MagicMock()
    r.portfolio = MagicMock()
    r.portfolio.positions = {}
    r._handle_command({"command": "pause"})
    assert r._circuit_breaker == "paused"
    r._handle_command({"command": "resume"})
    assert r._circuit_breaker is None
    r._handle_command({"command": "cancel_all", "symbol": "BTCUSDT"})
    r.gateway.cancel_all_open_orders.assert_called_once_with("BTCUSDT")


# ─── P1-3 postOnly ───


@pytest.mark.unit
def test_post_only_uses_limit_maker(monkeypatch):
    monkeypatch.setenv("POST_ONLY", "1")
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=1, symbol="BTCUSDT", side="BUY", status="NEW",
        executed_qty=0.0, avg_price=0.0)
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    req = gw.place_order.call_args[0][0]
    assert req.post_only is True


# ─── P2-1 K线归档 ───


@pytest.mark.unit
def test_kline_archive_upserts_closed_only(tmp_path):
    db_path = str(tmp_path / "kline.db")
    arch = KlineArchive(db_path)
    k = Kline("BTCUSDT", "15m", 1000, 1000 + 900_000,
              100, 110, 95, 105, 5.0, is_closed=True)
    arch.upsert(k)
    arch.upsert(Kline("BTCUSDT", "15m", 2000, 2000 + 900_000,
                      105, 115, 100, 110, 6.0, is_closed=False))  # forming 不入库
    assert arch.count("BTCUSDT", "15m") == 1
    # 同窗更新不新增行
    arch.upsert(Kline("BTCUSDT", "15m", 1000, 1000 + 900_000,
                      100, 112, 95, 108, 5.5, is_closed=True))
    assert arch.count("BTCUSDT", "15m") == 1
    arch.close()


# ─── P2-2 深度滑点估算 ───


@pytest.mark.unit
def test_slippage_estimate_walks_levels():
    book = {
        "bids": [[100.0, 1.0], [99.0, 2.0]],
        "asks": [[101.0, 0.5], [102.0, 2.0]],
    }
    # BUY 1.0: 0.5@101 + 0.5@102 → avg 101.5 vs best 101 → 49.5 bps
    bps = OrderbookDepth.estimate_slippage_bps(book, "BUY", 1.0)
    assert bps == pytest.approx(49.5049, rel=0.01)
    # 深度不足以承接 → None
    assert OrderbookDepth.estimate_slippage_bps(book, "BUY", 10.0) is None


# ─── P2-3 动态参数 ───


@pytest.mark.unit
def test_setparam_rebuilds_risk_chain():
    r = SystemRunner()
    r.risk_chain = MagicMock()
    r.portfolio = MagicMock()
    r._apply_param("risk_per_trade", "0.005")
    assert r.risk_per_trade == 0.005
    assert not isinstance(r.risk_chain, MagicMock)  # 已重建
    r._apply_param("bad_key", "1")  # 不抛异常


# ─── P1-6 保留策略 ───


@pytest.mark.unit
def test_db_purge_orders():
    db = TradeDatabase(":memory:")
    oid = db.create_order("BTCUSDT", "BUY", "LIMIT", 0.1, 64000.0)
    db.update_order_status(oid, "FILLED", "1", 0.1, 64000.0)
    # 把 created_at 改成 100 天前
    import datetime as _dt
    old = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=100)).isoformat()
    db.conn.execute("UPDATE orders SET created_at=?", (old,))
    db.conn.commit()
    assert db.purge_orders(days=90) == 1
    assert db.get_orders(limit=10) == []
    db.close()


# ─── Telegram 命令路由 (不触网) ───


@pytest.mark.unit
def test_telegram_handle_routes_commands():
    from tools.telegram_bot import TelegramBot
    bot = TelegramBot.__new__(TelegramBot)
    bot.redis = MagicMock()
    bot.allowed = {"123"}  # fail-closed: 白名单非空且包含本 chat
    bot.token = "t"
    bot.prefix = "systrader"
    sent = []
    bot.send = lambda chat_id, text: sent.append(text)

    # status 无心跳
    bot._latest_event = lambda stream: None
    bot.handle(123, "/status")
    assert any("暂无心跳" in s for s in sent)
    # 命令发布
    bot.handle(123, "/forceexit BTCUSDT")
    bot.handle(123, "/cancelall ALL")
    bot.handle(123, "/stop")
    published = [c[0][0] for c in bot.redis.xadd.call_args_list]
    assert "systrader:command" in published
    assert bot.redis.xadd.call_count == 3


# ─── dashboard handle_ws_command 兼容 dict ───


@pytest.mark.unit
def test_dashboard_handle_ws_command_dict():
    from dashboard.server import handle_ws_command
    bus = MagicMock()
    bus.publish.return_value = "1-0"
    assert handle_ws_command(bus, {"command": "force_exit", "symbol": "BTCUSDT"}) is True
    bus.publish.assert_called_once_with("command", {"command": "force_exit", "symbol": "BTCUSDT"})
    assert handle_ws_command(None, "pause") is False
