"""SystemRunner 统一装配测试 — mock gateway，验证完整信号链接线。"""

import time

import pytest
from unittest.mock import MagicMock, patch

from execution.order_manager import ManagedOrder, OrderState
from monitor.collector import MetricsCollector
from shared.execution_mode import ExecutionMode
from shared.runner import STALE_THRESHOLD, SystemRunner


@pytest.fixture(autouse=True)
def _isolate_metrics():
    """MetricsCollector 是跨测试文件共享的单例 — runner gauge 断言前重置隔离。"""
    MetricsCollector.reset()
    yield
    MetricsCollector.reset()


@pytest.fixture
def runner():
    # 2026-08-16 审计: 必须 mock OrderGateway, 否则 initialize() 里的
    # _sync_account_config/_fetch_exchange_filters 每轮打真实 testnet HTTP
    with patch("shared.runner.PreflightChecker") as MockPreflight, \
         patch("shared.runner.PositionReconciler") as MockReconciler, \
         patch("shared.runner.OrderGateway") as MockGW:
        MockPreflight.return_value.run_all.return_value = {
            "assets": [{"walletBalance": "10000"}],
        }
        r = SystemRunner()
        r.gateway = MagicMock()
        r.portfolio = MagicMock()
        r.feed = MagicMock()
        yield r


def _wire_signal_chain(runner):
    """构造 15m 信号 → 风控 → 下单链路 (全 mock)。"""
    runner.engine = MagicMock()
    runner.engine.strategy.timeframe = "15m"
    runner.engine.run.return_value = MagicMock(
        symbol="BTCUSDT", direction="LONG", conviction=0.8,
        entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0,
    )
    runner.risk_chain = MagicMock()
    runner.risk_chain.process.return_value = MagicMock(
        rejected=False, reason="", modifications={"position_size": 0.001},
    )
    runner.orders = MagicMock()
    runner.portfolio = MagicMock()
    runner.step_sizes = {"BTCUSDT": 0.001}
    runner.feed = MagicMock()
    runner.feed.get_last_price.return_value = 64000.0


def _entry_order() -> ManagedOrder:
    return ManagedOrder(
        order_id=1, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.001, price=64000.0, state=OrderState.PENDING,
    )


def _filled_entry_order() -> ManagedOrder:
    """入场单即时成交 FILLED (2026-08-16 起: 只有已成交才登记持仓/挂保护)。"""
    return ManagedOrder(
        order_id=1, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.001, price=64000.0, state=OrderState.FILLED,
        filled_qty=0.001, avg_price=64000.0,
    )


@pytest.mark.unit
def test_initialize_wires_full_assembly(runner):
    """装配后策略/风控/执行层全部就绪。"""
    with patch.object(runner, "_fetch_exchange_filters", return_value=({"BTCUSDT": 0.001}, {"BTCUSDT": 0.10})):
        runner.initialize()
    assert runner.engine is not None
    assert runner.risk_chain is not None
    assert runner.orders is not None
    assert runner.feed.on_kline_closed is not None


@patch("shared.runner.OrderGateway")
@patch("shared.runner.PreflightChecker")
@patch("shared.runner.PositionReconciler")
def test_paper_mode_wires_paper_trader(MockReconciler, MockPreflight, MockGW):
    """PAPER 模式构造 PaperTrader 并传给 OrderManager (否则零下单)。"""
    MockPreflight.return_value.run_all.return_value = {
        "assets": [{"walletBalance": "10000"}],
    }
    r = SystemRunner(execution_mode_name="paper")
    r.gateway = MagicMock()
    r.feed = MagicMock()
    with patch.object(r, "_fetch_exchange_filters", return_value=({}, {})):
        r.initialize()
    assert r.orders.paper_trader is not None
    assert r.orders.execution_mode.mode == ExecutionMode.PAPER
    assert r.orders.paper_trader.feed is r.feed


def test_live_mode_no_paper_trader(runner):
    """LIVE 模式不接 PaperTrader。"""
    with patch.object(runner, "_fetch_exchange_filters", return_value=({}, {})):
        runner.initialize()
    assert runner.orders.paper_trader is None
    assert runner.orders.execution_mode.mode == ExecutionMode.LIVE


@patch("shared.runner.OrderGateway")
@patch("shared.runner.PreflightChecker")
@patch("shared.runner.PositionReconciler")
def test_paper_mode_execute_signal_goes_through_paper_trader(MockReconciler, MockPreflight, MockGW):
    """PAPER 模式下 execute_signal 经 PaperTrader 产生成交, 不触达 gateway。"""
    MockPreflight.return_value.run_all.return_value = {
        "assets": [{"walletBalance": "10000"}],
    }
    r = SystemRunner(execution_mode_name="paper")
    r.feed = MagicMock()
    r.feed.get_last_price.return_value = 64000.0
    with patch.object(r, "_fetch_exchange_filters", return_value=({"BTCUSDT": 0.001}, {"BTCUSDT": 0.10})):
        r.initialize()
    orders = r.orders.execute_signal("BTCUSDT", "LONG", 0.001, 64000.0, 62000.0, 68000.0)
    # 入场 LIMIT 即时成交 FILLED, 条件单挂起 NEW → PENDING
    assert orders[0].state == OrderState.FILLED
    assert all(o.state == OrderState.PENDING for o in orders[1:])
    MockGW.return_value.place_order.assert_not_called()
    MockGW.return_value.place_algo_order.assert_not_called()


@patch("shared.runner.OrderGateway")
@patch("shared.runner.PreflightChecker")
@patch("shared.runner.PositionReconciler")
def test_initialize_wires_reconcile_drift_callback(MockReconciler, MockPreflight, MockGW):
    """对账漂移回调接线: 交易所持仓消失 → close_position 同步本地。"""
    MockPreflight.return_value.run_all.return_value = {
        "assets": [{"walletBalance": "10000"}],
    }
    r = SystemRunner()
    r.gateway = MagicMock()
    r.feed = MagicMock()
    with patch.object(r, "_fetch_exchange_filters", return_value=({}, {})):
        r.initialize()
    call_kwargs = MockReconciler.call_args[1]
    assert callable(call_kwargs.get("on_drift"))


def test_refresh_equity_updates_portfolio(runner):
    """周期权益刷新: totalWalletBalance (含未实现) → portfolio.update_equity (2026-08-16 P0-4)。"""
    runner.gateway.get_account.return_value = {
        "totalWalletBalance": "9500",
        "assets": [{"walletBalance": "9000", "availableBalance": "8500", "asset": "USDT"}],
    }
    runner._refresh_equity()
    kwargs = runner.portfolio.update_equity.call_args[1]
    assert kwargs["available_balance"] == 8500.0
    assert kwargs["assets"] == [{"asset": "USDT", "walletBalance": 9000.0}]
    runner.portfolio.update_equity.assert_called_once_with(9500.0, **kwargs)


def test_refresh_equity_falls_back_to_assets_sum(runner):
    """无 totalWalletBalance 字段时回退各资产 walletBalance 之和。"""
    runner.gateway.get_account.return_value = {
        "assets": [{"walletBalance": "9000", "availableBalance": "8800", "asset": "USDT"}],
    }
    runner._refresh_equity()
    kwargs = runner.portfolio.update_equity.call_args[1]
    assert kwargs["available_balance"] == 8800.0
    runner.portfolio.update_equity.assert_called_once_with(9000.0, **kwargs)


def test_refresh_equity_ignores_failure(runner):
    """权益刷新失败 (返回 error) 时静默跳过, 不抛异常。"""
    runner.gateway.get_account.return_value = {"error": "network down"}
    runner._refresh_equity()  # 不抛异常


def test_reconcile_drift_syncs_closed_position(runner):
    """对账检测到本地持仓消失 → close_position 以现价同步平仓。"""
    runner.portfolio.close_position = MagicMock(return_value=-50.0)
    runner.feed.get_last_price.return_value = 64000.0
    report = MagicMock()
    report.details = {"local_only": ["BTCUSDT"], "remote_only": [], "qty_mismatch": []}
    runner._on_reconcile_drift(report)
    runner.portfolio.close_position.assert_called_once_with("BTCUSDT", 64000.0)


@pytest.mark.unit
def test_on_kline_closed_15m_generates_signal(runner):
    """15m K线闭合 → 信号 → 风控 → 下单全链路 (入场即时成交路径)。"""
    _wire_signal_chain(runner)
    runner.orders.execute_signal.return_value = [_filled_entry_order()]

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.orders.execute_signal.assert_called_once_with(
        "BTCUSDT", "LONG", 0.001, 64000.0, 62000.0, 68000.0,
    )
    runner.portfolio.open_position.assert_called_once()
    assert runner.stats["kline_closes"] == 1
    assert runner.stats["signals"] == 1
    assert runner.stats["orders_placed"] == 1
    assert runner.stats["orders_failed"] == 0


@pytest.mark.unit
def test_pending_entry_does_not_register_position(runner):
    """入场单 PENDING → 不登记持仓 (成交确认后由 _sync_entry_fills 登记, 2026-08-16 审计)。"""
    _wire_signal_chain(runner)
    runner.orders.execute_signal.return_value = [_entry_order()]

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.portfolio.open_position.assert_not_called()
    assert runner.stats["orders_placed"] == 1
    assert runner.stats["orders_failed"] == 0


@pytest.mark.unit
def test_on_kline_closed_ignores_other_timeframes(runner):
    """非策略时间框架的 K线闭合被忽略。"""
    runner.engine = MagicMock()
    runner.engine.strategy.timeframe = "15m"
    runner._on_kline_closed("BTCUSDT", "4h", [MagicMock()])
    runner.engine.run.assert_not_called()


@pytest.mark.unit
def test_risk_rejection_skips_order(runner):
    """风控拒绝 → 不下单, 计入 risk_rejected。"""
    _wire_signal_chain(runner)
    runner.risk_chain.process.return_value = MagicMock(
        rejected=True, reason="drawdown breach", modifications={},
    )

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.orders.execute_signal.assert_not_called()
    assert runner.stats["signals"] == 1
    assert runner.stats["risk_rejected"] == 1
    assert runner.stats["orders_placed"] == 0


@pytest.mark.unit
def test_entry_order_rejected_counts_failed(runner):
    """入场单 REJECTED → 记 orders_failed, 不记持仓。"""
    _wire_signal_chain(runner)
    runner.orders.execute_signal.return_value = [
        ManagedOrder(
            order_id=1, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
            quantity=0.001, price=64000.0, state=OrderState.REJECTED, error="insufficient margin",
        ),
    ]

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.portfolio.open_position.assert_not_called()
    assert runner.stats["orders_failed"] == 1
    assert runner.stats["orders_placed"] == 0


@pytest.mark.unit
def test_existing_position_skips_new_order(runner):
    """已有该 symbol 持仓 → 跳过下单, 避免叠单。"""
    _wire_signal_chain(runner)
    runner.portfolio.positions = {"BTCUSDT": MagicMock()}

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.orders.execute_signal.assert_not_called()
    assert runner.stats["signals"] == 1
    assert runner.stats["orders_placed"] == 0


@pytest.mark.unit
def test_pending_entry_order_skips_new_order(runner):
    """已有 PENDING 入场单 → 跳过下单, 避免叠单。"""
    _wire_signal_chain(runner)
    runner.orders.active_orders = [_entry_order()]

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.orders.execute_signal.assert_not_called()
    assert runner.stats["signals"] == 1
    assert runner.stats["orders_placed"] == 0


@pytest.mark.unit
def test_proxy_host_from_env(monkeypatch):
    """PROXY_HOST/PROXY_PORT 环境变量传入 feed 与 gateway 构造 (docker 路径)。

    容器内 feed 代理必须指向宿主机 Clash (host.docker.internal)，而非容器自身。
    """
    monkeypatch.setenv("PROXY_HOST", "host.docker.internal")
    monkeypatch.setenv("PROXY_PORT", "7890")
    captured = {}

    class FakeFeed:
        def __init__(self, symbols, **kw):
            captured.update(kw)

        def start(self):
            pass

        def backfill(self, limit):
            pass

    with patch("shared.runner.PreflightChecker") as MockPreflight, \
         patch("shared.runner.PositionReconciler") as MockReconciler, \
         patch("shared.runner.MarketDataFeed", FakeFeed):
        MockPreflight.return_value.run_all.return_value = {
            "assets": [{"walletBalance": "10000"}],
        }
        r = SystemRunner()
        with patch.object(r, "_fetch_exchange_filters", return_value=({"BTCUSDT": 0.001}, {"BTCUSDT": 0.10})):
            r.initialize()
        assert captured["proxy_host"] == "host.docker.internal"
        assert captured["proxy_port"] == 7890
        # gateway 未显式传 proxy 时同样读环境变量
        assert r.gateway.proxies["https"] == "http://host.docker.internal:7890"


@pytest.mark.unit
def test_risk_per_trade_parameterized():
    """risk_per_trade 构造参数生效 (实盘分级 D 阶段旋钮 0.002→0.005→0.010→0.015)。"""
    r = SystemRunner(risk_per_trade=0.005)
    assert r.risk_per_trade == 0.005


@pytest.mark.unit
def test_hours_zero_runs_forever():
    r = SystemRunner(hours=0)
    assert r.hours == 0  # 生产模式无限运行


@pytest.mark.unit
def test_hours_positive_bounds_run():
    r = SystemRunner(hours=168)
    assert r.hours == 168  # soak 模式 7 天


# ─── Ops T5: 数据停滞熔断 / PENDING 超时撤单 / stats gauges ───


def _simulate_stall(runner):
    """全部 symbol 数据流停滞: 最后行情消息时间戳已超过 STALE_THRESHOLD。

    停滞语义 (2026-08-16 审计重写): 依据"最后消息年龄"判定——旧实现用
    缓存价 is None 判定, 缓存价收到过一次就永不为 None, 熔断防线形同虚设。
    """
    now = time.time()
    stale = now - STALE_THRESHOLD - 1
    runner.feed.get_last_update_ts.return_value = stale  # 消息时间戳停滞


@pytest.mark.unit
def test_stall_three_strikes_trigger_circuit_breaker(runner):
    """连续 3 次停滞判定 → 熔断停单 (circuit breaker 置位)。"""
    with patch.object(runner, "_network_diag"):
        for _ in range(3):
            _simulate_stall(runner)
            runner._check_stall()
    assert runner._circuit_breaker == "emergency_stop"
    # stalls 按 symbol 计数: 3 symbols × 3 次判定
    assert runner.stats["stalls"] == 3 * len(runner.symbols)


@pytest.mark.unit
def test_stall_less_than_strikes_no_breaker(runner):
    """连续 2 次停滞判定 (默认 3) 不触发熔断。"""
    with patch.object(runner, "_network_diag"):
        for _ in range(2):
            _simulate_stall(runner)
            runner._check_stall()
    assert runner._circuit_breaker is None


@pytest.mark.unit
def test_stall_strikes_parameterized():
    """--stall-strikes 参数生效 (stall_strikes=2 时 2 次即熔断)。"""
    r = SystemRunner(stall_strikes=2)
    assert r.stall_strikes == 2
    r.feed = MagicMock()
    with patch.object(r, "_network_diag"):
        for _ in range(2):
            _simulate_stall(r)
            r._check_stall()
    assert r._circuit_breaker == "emergency_stop"


@pytest.mark.unit
def test_stall_breaker_no_auto_resume_on_recovery(runner):
    """价格恢复后熔断不自动解除 (需手动 resume, 与 kill switch 语义一致)。"""
    with patch.object(runner, "_network_diag"):
        for _ in range(3):
            _simulate_stall(runner)
            runner._check_stall()
    assert runner._circuit_breaker == "emergency_stop"
    runner.feed.get_last_update_ts.return_value = time.time()
    runner._check_stall()
    assert runner._circuit_breaker == "emergency_stop"


@pytest.mark.unit
def test_stall_strikes_reset_on_price_recovery(runner):
    """数据流恢复清零连续停滞计数, 之后重新计数。"""
    with patch.object(runner, "_network_diag"):
        for _ in range(2):
            _simulate_stall(runner)
            runner._check_stall()
    assert runner._circuit_breaker is None
    runner.feed.get_last_update_ts.return_value = time.time()
    runner._check_stall()                          # 恢复 → 计数清零
    with patch.object(runner, "_network_diag"):
        _simulate_stall(runner)
        runner._check_stall()
    assert runner._circuit_breaker is None         # 重新计数, 1 次不足 3


@pytest.mark.unit
def test_pending_timeout_cancels_stale_entry(runner):
    """PENDING 入场单超时 → 自动撤单 (cancel_order)。"""
    runner.orders = MagicMock()
    runner.portfolio.positions = {}
    runner.gateway = MagicMock()
    stale = ManagedOrder(
        order_id=55, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.001, price=64000.0, state=OrderState.PENDING,
        created_at=time.time() - 31 * 60,
    )
    runner.orders.active_orders = [stale]
    runner._check_pending_timeouts()
    runner.gateway.cancel_order.assert_called_once_with("BTCUSDT", 55)
    assert stale.state == OrderState.CANCELED  # 撤单后移出活跃集, 不再重复检测


@pytest.mark.unit
def test_pending_timeout_skips_fresh_order(runner):
    """未超时的 PENDING 订单不撤。"""
    runner.orders = MagicMock()
    runner.portfolio.positions = {}
    runner.gateway = MagicMock()
    fresh = ManagedOrder(
        order_id=56, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.001, price=64000.0, state=OrderState.PENDING,
        created_at=time.time() - 10 * 60,
    )
    runner.orders.active_orders = [fresh]
    runner._check_pending_timeouts()
    runner.gateway.cancel_order.assert_not_called()
    assert fresh.state == OrderState.PENDING


@pytest.mark.unit
def test_pending_timeout_cancels_zombie_algo_order(runner):
    """无持仓时超时条件单 (TAKE_PROFIT_MARKET) 按类型走 cancel_algo_order。"""
    runner.orders = MagicMock()
    runner.portfolio.positions = {}
    runner.gateway = MagicMock()
    zombie = ManagedOrder(
        order_id=303, symbol="ETHUSDT", side="SELL", order_type="TAKE_PROFIT_MARKET",
        quantity=0.01, price=66000.0, state=OrderState.PENDING,
        created_at=time.time() - 45 * 60,
    )
    runner.orders.active_orders = [zombie]
    runner._check_pending_timeouts()
    runner.gateway.cancel_algo_order.assert_called_once_with("ETHUSDT", 303)


@pytest.mark.unit
def test_pending_timeout_keeps_protection_for_open_position(runner):
    """有持仓的 symbol: 止损条件单是持仓保护, 超时不撤。"""
    runner.orders = MagicMock()
    runner.portfolio.positions = {"BTCUSDT": MagicMock()}
    runner.gateway = MagicMock()
    sl = ManagedOrder(
        order_id=202, symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
        quantity=0.001, price=62000.0, state=OrderState.PENDING,
        created_at=time.time() - 60 * 60,
    )
    runner.orders.active_orders = [sl]
    runner._check_pending_timeouts()
    runner.gateway.cancel_algo_order.assert_not_called()
    runner.gateway.cancel_order.assert_not_called()
    assert sl.state == OrderState.PENDING


@pytest.mark.unit
def test_pending_timeout_minutes_parameterized(runner):
    """--pending-timeout-minutes 参数生效。"""
    assert SystemRunner().pending_timeout_minutes == 30
    r = SystemRunner(pending_timeout_minutes=10)
    r.orders = MagicMock()
    r.portfolio = MagicMock()
    r.portfolio.positions = {}
    r.gateway = MagicMock()
    stale = ManagedOrder(
        order_id=77, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.001, price=64000.0, state=OrderState.PENDING,
        created_at=time.time() - 11 * 60,
    )
    r.orders.active_orders = [stale]
    r._check_pending_timeouts()
    r.gateway.cancel_order.assert_called_once_with("BTCUSDT", 77)


@pytest.mark.unit
def test_kline_close_sets_metrics_gauge(runner):
    """_on_kline_closed 注册 kline_closes gauge。"""
    runner.engine = MagicMock()
    runner.engine.strategy.timeframe = "15m"
    runner.engine.run.return_value = None  # 无信号
    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])
    assert MetricsCollector.instance().get_gauge("kline_closes") == 1


@pytest.mark.unit
def test_order_outcome_sets_metrics_gauges(runner):
    """下单成功注册 orders_placed gauge (与 kline_closes)。"""
    _wire_signal_chain(runner)
    runner.orders.execute_signal.return_value = [_entry_order()]
    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])
    m = MetricsCollector.instance()
    assert m.get_gauge("kline_closes") == 1
    assert m.get_gauge("orders_placed") == 1
    assert m.get_gauge("orders_failed") == 0


@pytest.mark.unit
def test_order_failure_sets_metrics_gauge(runner):
    """下单失败注册 orders_failed gauge。"""
    _wire_signal_chain(runner)
    runner.orders.execute_signal.return_value = [
        ManagedOrder(
            order_id=1, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
            quantity=0.001, price=64000.0, state=OrderState.REJECTED, error="insufficient margin",
        ),
    ]
    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])
    m = MetricsCollector.instance()
    assert m.get_gauge("orders_failed") == 1
    assert m.get_gauge("orders_placed") == 0
