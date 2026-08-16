"""第五轮全面审查修复的回归测试 (2026-08-16 五路 subagent 审查)。

覆盖: S1 条件单触发跟踪 / S2 撤单失败保持 PENDING / S3 保护单几何按成交价校验 /
S4 force_exit 撤 PENDING 入场单 / S5 setparam 真实生效+保留熔断状态 /
数据库跨线程 / feed 切换补发闭合线。
"""

import pytest
from unittest.mock import MagicMock, patch

from execution.order_gateway import OrderGateway, OrderResponse, AlgoOrderResponse
from execution.order_manager import OrderManager, OrderState, ManagedOrder
from portfolio.tracker import PortfolioTracker, Position
from shared.database import TradeDatabase
from shared.execution_mode import ExecutionMode, ExecutionModeManager
from shared.runner import SystemRunner
from signal_engine.engine import Signal
from risk.chain import MiddlewareChain
from risk.drawdown_breaker import DrawdownBreaker, BreakerState


# ─── S2: 撤单失败不再误标 CANCELED ───


@pytest.mark.unit
def test_cancel_network_error_keeps_pending():
    """撤单网络失败 (ERROR) → 订单保持 PENDING, 下一轮重试, 不与交易所脱节。"""
    r = SystemRunner()
    r.gateway = MagicMock()
    r.gateway.cancel_order.return_value = OrderResponse(
        order_id=9, symbol="BTCUSDT", side="BUY", status="ERROR",
        executed_qty=0.0, avg_price=0.0, error="Connection timeout")
    order = ManagedOrder(order_id=9, symbol="BTCUSDT", side="BUY",
                         order_type="LIMIT", quantity=0.01, price=64000.0,
                         state=OrderState.PENDING)
    r._cancel_one_order(order)
    assert order.state == OrderState.PENDING  # 不误标


@pytest.mark.unit
def test_cancel_unknown_order_marks_canceled():
    """未知订单 (REJECTED, -2011) → 交易所已无此单, 标记 CANCELED。"""
    r = SystemRunner()
    r.gateway = MagicMock()
    r.gateway.cancel_order.return_value = OrderResponse(
        order_id=9, symbol="BTCUSDT", side="BUY", status="REJECTED",
        executed_qty=0.0, avg_price=0.0, error="Unknown order sent.")
    order = ManagedOrder(order_id=9, symbol="BTCUSDT", side="BUY",
                         order_type="LIMIT", quantity=0.01, price=64000.0,
                         state=OrderState.PENDING)
    r._cancel_one_order(order)
    assert order.state == OrderState.CANCELED


# ─── S3: 保护单几何按成交价校验 ───


@pytest.mark.unit
def test_place_protection_rejects_bad_geometry_vs_fill():
    """LIMIT 迟到成交价低于原止损 (LONG) → 拒绝挂保护, 不挂秒损单。"""
    gw = MagicMock()
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    order = ManagedOrder(order_id=1, symbol="BTCUSDT", side="BUY",
                         order_type="LIMIT", quantity=0.1, price=64000.0,
                         state=OrderState.FILLED, filled_qty=0.1,
                         avg_price=63000.0,  # 实际成交价低于信号价
                         stop_price=64000.0, take_profit=68000.0)
    result = mgr.place_protection(order)
    assert result == []  # 止损高于成交价 → 拒绝挂单
    gw.place_algo_order.assert_not_called()


@pytest.mark.unit
def test_place_protection_ok_when_geometry_valid():
    gw = MagicMock()
    gw.place_algo_order.return_value = AlgoOrderResponse(
        algo_id=7, symbol="BTCUSDT", side="SELL", status="NEW")
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    order = ManagedOrder(order_id=1, symbol="BTCUSDT", side="BUY",
                         order_type="LIMIT", quantity=0.1, price=64000.0,
                         state=OrderState.FILLED, filled_qty=0.1,
                         avg_price=63900.0, stop_price=62000.0, take_profit=68000.0)
    result = mgr.place_protection(order)
    assert len(result) == 2


# ─── S1: 条件单触发跟踪 ───


@pytest.mark.unit
def test_sync_algo_orders_detects_trigger():
    """保护单 algoId 不在开放清单 → 已触发, 标记 FILLED 并返回 symbol。"""
    gw = MagicMock()
    gw.place_algo_order.return_value = AlgoOrderResponse(
        algo_id=101, symbol="BTCUSDT", side="SELL", status="NEW")
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    mgr.submit_stop_loss("BTCUSDT", "LONG", 0.1, 62000.0)
    gw.get_open_algo_orders.return_value = {999}  # 101 不在开放清单 → 已触发
    triggered = mgr.sync_algo_orders()
    assert triggered == ["BTCUSDT"]
    order = mgr._orders_snapshot()[0]
    assert order.state == OrderState.FILLED


@pytest.mark.unit
def test_sync_algo_orders_query_failure_keeps_state():
    gw = MagicMock()
    gw.place_algo_order.return_value = AlgoOrderResponse(
        algo_id=102, symbol="ETHUSDT", side="SELL", status="NEW")
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    mgr.submit_stop_loss("ETHUSDT", "LONG", 0.1, 1800.0)
    gw.get_open_algo_orders.return_value = None  # 查询失败
    assert mgr.sync_algo_orders() == []
    assert mgr._orders_snapshot()[0].state == OrderState.PENDING  # 保持原状


@pytest.mark.unit
def test_runner_on_protection_triggered_closes_and_cancels():
    """保护单触发 → 撤残余保护单 + 本地平仓。"""
    r = SystemRunner()
    r.portfolio = PortfolioTracker(initial_equity=10000.0)
    r.portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
    r.feed = MagicMock()
    r.feed.get_last_price.return_value = 60500.0
    r.gateway = MagicMock()
    r.gateway.cancel_algo_order.return_value = AlgoOrderResponse(
        algo_id=7, symbol="BTCUSDT", side="SELL", status="CANCELED")
    tp_order = ManagedOrder(order_id=7, symbol="BTCUSDT", side="SELL",
                            order_type="TAKE_PROFIT_MARKET", quantity=0.1,
                            price=66000.0, state=OrderState.PENDING)
    r.orders = MagicMock()
    r.orders.active_orders = [tp_order]
    r._on_protection_triggered("BTCUSDT")
    assert "BTCUSDT" not in r.portfolio.positions
    r.gateway.cancel_algo_order.assert_called_once_with("BTCUSDT", 7)
    assert tp_order.state == OrderState.CANCELED


# ─── S4: force_exit 撤 PENDING 入场单 ───


@pytest.mark.unit
def test_force_exit_cancels_pending_entry():
    """force_exit 同时撤 PENDING 入场单, 防价格回踩成交出用户没要的新仓。"""
    r = SystemRunner()
    r.portfolio = PortfolioTracker(initial_equity=10000.0)
    r.portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
    r.feed = MagicMock()
    r.feed.get_last_price.return_value = 60500.0
    r.gateway = MagicMock()
    r.gateway.cancel_order.return_value = OrderResponse(
        order_id=8, symbol="BTCUSDT", side="BUY", status="CANCELED",
        executed_qty=0.0, avg_price=0.0)
    r.gateway.place_order.return_value = OrderResponse(
        order_id=9, symbol="BTCUSDT", side="SELL", status="FILLED",
        executed_qty=0.1, avg_price=60500.0)
    entry = ManagedOrder(order_id=8, symbol="BTCUSDT", side="BUY",
                         order_type="LIMIT", quantity=0.1, price=59000.0,
                         state=OrderState.PENDING)
    r.orders = MagicMock()
    r.orders.active_orders = [entry]
    r._force_exit_symbol("BTCUSDT")
    r.gateway.cancel_order.assert_called_once_with("BTCUSDT", 8)
    assert entry.state == OrderState.CANCELED
    assert "BTCUSDT" not in r.portfolio.positions


# ─── S5: setparam 真实生效 + 保留熔断状态 ───


@pytest.mark.unit
def test_setparam_max_leverage_actually_applies():
    r = SystemRunner()
    r._apply_param("max_leverage", "10")
    from risk.leverage import LeverageController
    for mw in r.risk_chain._middleware:
        if isinstance(mw, LeverageController):
            assert mw.max_leverage == 10
            return
    pytest.fail("LeverageController 未在风控链中")


@pytest.mark.unit
def test_setparam_preserves_breaker_cooldown():
    r = SystemRunner()
    breaker = DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3,
                              cooldown_minutes=120)
    breaker.state = BreakerState.COOLDOWN
    breaker._triggered_at = 12345.0
    r.risk_chain = MiddlewareChain()
    r.risk_chain.add(breaker)
    r._apply_param("risk_per_trade", "0.01")
    for mw in r.risk_chain._middleware:
        if isinstance(mw, DrawdownBreaker):
            assert mw.state == BreakerState.COOLDOWN  # 熔断状态被继承
            assert mw._triggered_at == 12345.0
            return
    pytest.fail("DrawdownBreaker 未在风控链中")


# ─── 数据库跨线程 ───


@pytest.mark.unit
def test_database_write_from_other_thread():
    """主线程创建, 子线程写 — 不再抛 ProgrammingError (2026-08-16 审计)。"""
    import threading
    db = TradeDatabase(":memory:")
    errors = []

    def worker():
        try:
            oid = db.create_order("BTCUSDT", "BUY", "LIMIT", 0.1, 64000.0)
            db.update_order_status(oid, "FILLED", "1", 0.1, 64000.0)
        except Exception as e:
            errors.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert errors == []
    assert len(db.get_orders(limit=10)) == 1
    db.close()


# ─── feed 切换补发闭合线 ───


@pytest.mark.unit
def test_replay_missed_closures_fires_recent_only():
    from market_data.feed import MarketDataFeed
    from market_data.kline_buffer import Kline
    import time as _t
    fired = []
    feed = MarketDataFeed(symbols=["BTCUSDT"], testnet=True,
                          on_kline_closed=lambda s, tf, ohlcv: fired.append((s, tf)))
    now_ms = int(_t.time() * 1000)
    # 最近闭合 (窗口内) → 应补发
    feed.buffer.add(Kline("BTCUSDT", "15m", now_ms - 500_000,
                          now_ms - 500_000 + 900_000, 100, 110, 95, 105, 1.0,
                          is_closed=True))
    # 很久以前的闭合 (backfill 历史) → 不补发
    feed.buffer.add(Kline("BTCUSDT", "15m", now_ms - 99_900_000,
                          now_ms - 99_900_000 + 900_000, 90, 95, 85, 92, 1.0,
                          is_closed=True))
    feed._replay_missed_closures()
    assert fired == [("BTCUSDT", "15m")]  # 只补发窗口内的一根


# ─── 幂等恢复 _recover_by_client_id (第五路审查盲区) ───


@pytest.mark.unit
def test_recover_by_client_id_returns_real_status():
    """place_order 返回 -2010/-2011 → 按 clientOrderId 查回真实订单状态。"""
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=0, symbol="BTCUSDT", side="BUY", status="REJECTED",
        executed_qty=0.0, avg_price=0.0, error="Order would immediately trigger.",
        code=-2010)
    gw.query_order_by_client_id.return_value = {
        "orderId": 77, "status": "FILLED", "executedQty": "0.1", "avgPrice": "63900",
    }
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    order = mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    assert order.state == OrderState.FILLED
    assert order.order_id == 77
    assert order.filled_qty == 0.1


@pytest.mark.unit
def test_recover_by_client_id_query_failure_keeps_rejected():
    """查询失败 (订单确实不存在) → 保持 REJECTED, 不伪装成功。"""
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=0, symbol="BTCUSDT", side="BUY", status="REJECTED",
        executed_qty=0.0, avg_price=0.0, error="Order would immediately trigger.",
        code=-2010)
    gw.query_order_by_client_id.return_value = None
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    order = mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    assert order.state == OrderState.REJECTED


# ─── setparam 非法值 (第五路审查盲区) ───


@pytest.mark.unit
@pytest.mark.parametrize("key,value", [
    ("risk_per_trade", "0.2"),   # 超上限
    ("risk_per_trade", "-1"),    # 负数
    ("risk_per_trade", "abc"),   # 非数值
    ("max_leverage", "99"),      # 超上限
    ("max_leverage", "0"),       # 低于下限
])
def test_setparam_invalid_values_keep_params(key, value):
    r = SystemRunner()
    old_risk = r.risk_per_trade
    old_lev = r.max_leverage
    old_chain = r.risk_chain
    r._apply_param(key, value)  # 不抛异常
    assert r.risk_per_trade == old_risk
    assert r.max_leverage == old_lev
    assert r.risk_chain is old_chain  # 链未被重建


# ─── force_exit 失败分支 (第五路审查盲区) ───


@pytest.mark.unit
def test_force_exit_no_position_early_return():
    r = SystemRunner()
    r.portfolio = PortfolioTracker(initial_equity=10000.0)
    r.gateway = MagicMock()
    r._force_exit_symbol("BTCUSDT")  # 无持仓 → 早退, 不触达 gateway
    r.gateway.place_order.assert_not_called()


@pytest.mark.unit
def test_force_exit_unfilled_keeps_position():
    """平仓单未成交 (REJECTED) → 持仓保留, 不误平。"""
    r = SystemRunner()
    r.portfolio = PortfolioTracker(initial_equity=10000.0)
    r.portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
    r.feed = MagicMock()
    r.feed.get_last_price.return_value = 60500.0
    r.gateway = MagicMock()
    r.gateway.place_order.return_value = OrderResponse(
        order_id=9, symbol="BTCUSDT", side="SELL", status="REJECTED",
        executed_qty=0.0, avg_price=0.0, error="insufficient balance")
    r.orders = MagicMock()
    r.orders.active_orders = []
    r._force_exit_symbol("BTCUSDT")
    assert "BTCUSDT" in r.portfolio.positions  # 持仓保留


# ─── StateStore bootstrap 幂等 (第五路审查盲区) ───


@pytest.mark.unit
def test_bootstrap_duplicate_open_events_idempotent():
    """重复 open 事件重放后持仓不叠加 (dict 覆盖语义)。"""
    import json
    bus = MagicMock()
    bus._key = lambda s: f"systrader:{s}"
    open1 = json.dumps({"stream": "position.changed", "timestamp": "2026-08-16T05:01:00+00:00",
                        "data": {"event": "open", "symbol": "BTCUSDT", "direction": "LONG",
                                 "quantity": 0.1, "entry_price": 60000.0, "instance": "live"}})
    open2 = json.dumps({"stream": "position.changed", "timestamp": "2026-08-16T05:02:00+00:00",
                        "data": {"event": "open", "symbol": "BTCUSDT", "direction": "LONG",
                                 "quantity": 0.1, "entry_price": 60000.0, "instance": "live"}})
    bus.redis.xrevrange = lambda key, count: (
        [("2-0", {"payload": open2}), ("1-0", {"payload": open1})]
        if key == "systrader:position.changed" else [])
    from dashboard.state_store import StateStore
    store = StateStore(event_bus=bus, instance_filter="live")
    store.start()
    assert len(store.positions) == 1  # 覆盖不叠加


# ─── 资金费记账 / 可用保证金检查 (2026-08-16) ───


@pytest.mark.unit
def test_tracker_add_funding_fee_books_pnl():
    t = PortfolioTracker(initial_equity=10000.0)
    t.add_funding_fee(2.5)
    assert t.total_funding_fees == 2.5
    assert t.daily_realized_pnl == -2.5
    assert t.total_realized_pnl == -2.5
    assert t.total_equity == 10000.0  # 权益不动 (交易所已扣, 以账户刷新为准)


@pytest.mark.unit
def test_available_margin_check_rejects_insufficient():
    from risk.chain import MiddlewareChain
    from risk.available_margin import AvailableMarginCheck
    from risk.position_sizer import PositionSizer
    t = PortfolioTracker(initial_equity=1000.0)
    t.available_balance = 50.0  # 可用保证金只有 50
    chain = MiddlewareChain()
    chain.add(PositionSizer(risk_per_trade=0.5))  # 大仓位 → 所需保证金超可用
    chain.add(AvailableMarginCheck(safety_ratio=0.9))
    sig = Signal("BTCUSDT", "LONG", 0.8, 64000.0, 62000.0, 68000.0, leverage=3)
    result = chain.process(sig, t)
    assert result.rejected is True
    assert "AvailableMarginCheck" in result.reason


@pytest.mark.unit
def test_available_margin_check_passes_sufficient():
    from risk.chain import MiddlewareChain
    from risk.available_margin import AvailableMarginCheck
    from risk.position_sizer import PositionSizer
    t = PortfolioTracker(initial_equity=10000.0)
    t.available_balance = 9000.0
    chain = MiddlewareChain()
    chain.add(PositionSizer(risk_per_trade=0.015))
    chain.add(AvailableMarginCheck(safety_ratio=0.9))
    sig = Signal("BTCUSDT", "LONG", 0.8, 64000.0, 62000.0, 68000.0, leverage=3)
    result = chain.process(sig, t)
    assert result.rejected is False


@pytest.mark.unit
def test_funding_monitor_cost_apply_flag():
    """首个周期不记账 (apply_cost=False), 后续周期记账。"""
    from shared.funding_monitor import FundingRateMonitor
    t = PortfolioTracker(initial_equity=10000.0)
    t.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
    costs = []
    mon = FundingRateMonitor(t, cost_threshold=999, on_cost=lambda s, c: costs.append(c))
    mon.fetch_rate = lambda sym: 0.0001
    mon.tracker.update("BTCUSDT", 0.0001)
    mon.check_positions(apply_cost=False)   # 首个周期: 只告警不记账
    assert costs == []
    mon.check_positions(apply_cost=True)    # 后续周期: 记账
    assert len(costs) == 1 and costs[0] > 0