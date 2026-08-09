"""SystemRunner 统一装配测试 — mock gateway，验证完整信号链接线。"""

import pytest
from unittest.mock import MagicMock, patch

from execution.order_manager import ManagedOrder, OrderState
from shared.runner import SystemRunner


@pytest.fixture
def runner():
    with patch("shared.runner.PreflightChecker") as MockPreflight, \
         patch("shared.runner.PositionReconciler") as MockReconciler:
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


@pytest.mark.unit
def test_initialize_wires_full_assembly(runner):
    """装配后策略/风控/执行层全部就绪。"""
    with patch.object(runner, "_fetch_step_sizes", return_value={"BTCUSDT": 0.001}):
        runner.initialize()
    assert runner.engine is not None
    assert runner.risk_chain is not None
    assert runner.orders is not None
    assert runner.feed.on_kline_closed is not None


@pytest.mark.unit
def test_on_kline_closed_15m_generates_signal(runner):
    """15m K线闭合 → 信号 → 风控 → 下单全链路 (成功路径)。"""
    _wire_signal_chain(runner)
    runner.orders.execute_signal.return_value = [_entry_order()]

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
        with patch.object(r, "_fetch_step_sizes", return_value={"BTCUSDT": 0.001}):
            r.initialize()
        assert captured["proxy_host"] == "host.docker.internal"
        assert captured["proxy_port"] == 7890
        # gateway 未显式传 proxy 时同样读环境变量
        assert r.gateway.proxies["https"] == "http://host.docker.internal:7890"
