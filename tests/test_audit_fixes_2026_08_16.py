"""2026-08-16 审计修复的回归测试 — 乱序 K 线 / 杠杆风控 / 模拟条件单 / tick 对齐 / 告警节流等。"""

import time

import pytest
from unittest.mock import MagicMock

from execution.order_gateway import OrderGateway
from execution.order_manager import OrderManager, round_price
from market_data.kline_buffer import KlineBuffer, Kline
from monitor.alerter import Alerter, AlertLevel
from risk.chain import MiddlewareChain
from risk.leverage import LeverageController
from signal_engine.engine import Signal
from shared.execution_mode import ExecutionModeManager, ExecutionMode
from shared.paper_trader import PaperTrader


# ─── KlineBuffer 乱序保护 ───


def _k(open_time: int, close: float, is_closed: bool = True) -> Kline:
    return Kline(
        symbol="BTCUSDT", timeframe="15m",
        open_time=open_time, close_time=open_time + 900_000,
        open=close - 10, high=close + 10, low=close - 20,
        close=close, volume=1.0, is_closed=is_closed,
    )


@pytest.mark.unit
def test_kline_buffer_drops_out_of_order_candle():
    buf = KlineBuffer()
    assert buf.add(_k(2000, 100.0)) is True
    assert buf.add(_k(3000, 110.0)) is True
    # 过期 candle (open_time 早于最新) → 丢弃
    assert buf.add(_k(1500, 95.0)) is False
    rows = buf.get_klines("BTCUSDT", "15m")
    assert [r.open_time for r in rows] == [2000, 3000]
    assert buf.get_latest("BTCUSDT", "15m").open_time == 3000


@pytest.mark.unit
def test_kline_buffer_updates_matching_window_in_place():
    """乱序同窗更新: 晚到的 closed 更新替换对应行, 不破坏序列。"""
    buf = KlineBuffer()
    buf.add(_k(2000, 100.0))
    buf.add(_k(3000, 110.0, is_closed=False))  # forming
    # 2000 窗口的晚到更新 (closed) → 按 open_time 定位替换
    assert buf.add(_k(2000, 101.0)) is True
    rows = buf.get_klines("BTCUSDT", "15m")
    assert [r.open_time for r in rows] == [2000, 3000]
    assert rows[0].close == 101.0
    assert rows[1].is_closed is False


@pytest.mark.unit
def test_kline_buffer_rejects_gap_without_match():
    buf = KlineBuffer()
    buf.add(_k(2000, 100.0))
    buf.add(_k(3000, 110.0))
    # 1500 无同窗行且早于最新 → 纯过期, 丢弃
    assert buf.add(_k(1500, 90.0)) is False
    assert buf.count("BTCUSDT", "15m") == 2


# ─── LeverageController ───


@pytest.mark.unit
def test_leverage_controller_accepts_within_limit():
    from portfolio.tracker import PortfolioTracker
    chain = MiddlewareChain()
    chain.add(LeverageController(max_leverage=5))
    sig = Signal("BTCUSDT", "LONG", 0.8, 64000.0, 63000.0, 66000.0, leverage=3)
    result = chain.process(sig, PortfolioTracker(initial_equity=10000.0))
    assert result.rejected is False
    assert result.modifications.get("leverage") == 3


@pytest.mark.unit
def test_leverage_controller_rejects_over_limit():
    from portfolio.tracker import PortfolioTracker
    chain = MiddlewareChain()
    chain.add(LeverageController(max_leverage=5))
    sig = Signal("BTCUSDT", "LONG", 0.8, 64000.0, 63000.0, 66000.0, leverage=20)
    result = chain.process(sig, PortfolioTracker(initial_equity=10000.0))
    assert result.rejected is True
    assert "LeverageController" in result.reason


# ─── PaperTrader 条件单触发 ───


def _make_paper(feed=None) -> PaperTrader:
    if feed is None:
        feed = MagicMock()
        feed.get_mark_price.return_value = 64000.0
        feed.get_last_price.return_value = 64000.0
    return PaperTrader(feed=feed, fill_delay_ms=0.0)


@pytest.mark.unit
def test_paper_conditional_hangs_until_trigger():
    from execution.order_gateway import OrderRequest
    feed = MagicMock()
    feed.get_mark_price.return_value = 64000.0
    feed.get_last_price.return_value = 64000.0
    pt = _make_paper(feed)
    fill = pt.execute(OrderRequest(
        symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
        quantity=0.1, stop_price=63000.0, reduce_only=True,
    ))
    assert fill.status == "NEW"
    assert len(pt.pending_conditionals) == 1
    # 未触发: 价格 64000 >= 63000 不是 SELL STOP 触发条件
    pt.poll_conditionals()
    assert pt.filled_conditional_ids == set()


@pytest.mark.unit
def test_paper_stop_market_triggers_on_price_drop():
    from execution.order_gateway import OrderRequest
    feed = MagicMock()
    feed.get_mark_price.return_value = 62900.0  # 跌破 stop
    pt = _make_paper(feed)
    fill = pt.execute(OrderRequest(
        symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
        quantity=0.1, stop_price=63000.0, reduce_only=True,
    ))
    pt.poll_conditionals()
    assert fill.order_id in pt.filled_conditional_ids
    assert len(pt.pending_conditionals) == 0
    got = pt.conditional_fill(fill.order_id)
    assert got.status == "FILLED"
    assert got.executed_qty == 0.1


@pytest.mark.unit
def test_paper_take_profit_triggers_on_price_rally():
    from execution.order_gateway import OrderRequest
    feed = MagicMock()
    feed.get_mark_price.return_value = 66100.0  # 涨过 tp
    pt = _make_paper(feed)
    fill = pt.execute(OrderRequest(
        symbol="BTCUSDT", side="SELL", order_type="TAKE_PROFIT_MARKET",
        quantity=0.1, stop_price=66000.0, reduce_only=True,
    ))
    pt.poll_conditionals()
    assert fill.order_id in pt.filled_conditional_ids
    got = pt.conditional_fill(fill.order_id)
    assert got.avg_price == pytest.approx(66000.0)


# ─── tickSize 对齐 ───


@pytest.mark.unit
def test_round_price_respects_tick_precision():
    # SOL tick=0.001: 不再被固定 2 位小数破坏档位
    assert round_price(75.4326, 0.001) == pytest.approx(75.432)
    assert round_price(63000.05, 0.10) == pytest.approx(63000.0)


@pytest.mark.unit
def test_order_manager_aligns_entry_and_sl_to_tick():
    # SOLUSDT 实际 tickSize=0.01 (2026-08-16 实测 exchangeInfo)
    gw = MagicMock()
    gw.place_order.return_value = MagicMock(status="FILLED", order_id=1,
                                            executed_qty=1.0, avg_price=75.43,
                                            error=None, code=None)
    gw.place_algo_order.return_value = MagicMock(algo_id=7, status="NEW", error=None)
    om = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    orders = om.execute_signal("SOLUSDT", "LONG", 1.0, 75.4326, 74.9876, 76.5432)
    entry, sl, tp = orders
    assert entry.price == pytest.approx(75.43)          # 向下对齐 tick=0.01
    assert sl.price == pytest.approx(74.98)
    assert tp.price == pytest.approx(76.54)


@pytest.mark.unit
def test_order_manager_prunes_terminal_orders():
    gw = MagicMock()
    gw.place_order.return_value = MagicMock(status="REJECTED", order_id=0,
                                            executed_qty=0.0, avg_price=0.0,
                                            error="reject", error_msg=None)
    om = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    for _ in range(1100):
        om.submit_entry("BTCUSDT", "LONG", 0.001, 64000.0, 63000.0, 66000.0)
    assert len(om._orders) <= 1000 + 500


# ─── Alerter 节流 ───


@pytest.mark.unit
def test_alerter_throttles_same_metric():
    sent = []
    a = Alerter(on_alert=sent.append)
    a.fire(AlertLevel.CRITICAL, "margin_ratio", "first")
    a.fire(AlertLevel.CRITICAL, "margin_ratio", "second")  # 60s 内同 metric → 节流
    assert len(sent) == 1
    a.fire(AlertLevel.CRITICAL, "drawdown", "other metric")  # 不同 metric 不节流
    assert len(sent) == 2


@pytest.mark.unit
def test_alerter_tolerates_missing_portfolio_attrs():
    """portfolio 缺属性时 check_thresholds 不炸 (2026-08-16 审计)。"""
    a = Alerter(on_alert=lambda x: None)
    a.check_thresholds(MagicMock(), portfolio=MagicMock(spec=[]))  # 无属性对象


# ─── gateway 撤单 fail-biased ───


@pytest.mark.unit
def test_cancel_order_error_body_is_rejected_not_canceled():
    gw = OrderGateway(testnet=True)
    # 错误响应体 {code, msg} 无 status → REJECTED (而非乐观 CANCELED)
    assert gw._status_or_fail({"code": -2011, "msg": "Unknown order sent."}, "CANCELED") == "REJECTED"
    # 成功响应体带 status → 原样返回
    assert gw._status_or_fail({"orderId": 1, "status": "CANCELED"}, "CANCELED") == "CANCELED"
    # 空响应体 (无 code 无 status) → fallback
    assert gw._status_or_fail({}, "CANCELED") == "CANCELED"


# ─── runner 停滞检测 (feed 时间戳) ───


@pytest.mark.unit
def test_check_stall_uses_update_ts_not_cached_price():
    from shared.runner import STALE_THRESHOLD, SystemRunner
    r = SystemRunner()
    r.feed = MagicMock()
    r.symbols = ["BTCUSDT"]
    r._stall_strikes = {}
    now = time.time()
    # 缓存价存在但消息时间戳停滞 → 判定 stall (旧实现这里会直接放过)
    r.feed.get_last_price.return_value = 64000.0
    r.feed.get_last_update_ts.return_value = now - STALE_THRESHOLD - 1
    r.stall_strikes = 3
    from unittest.mock import patch
    with patch.object(r, "_network_diag"):
        r._check_stall()
    assert r.stats["stalls"] >= 1
