"""SystemRunner 统一装配测试 — mock gateway，验证完整信号链接线。"""

import pytest
from unittest.mock import MagicMock, patch

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
    """15m K线闭合 → 信号 → 风控 → 下单全链路。"""
    runner.engine = MagicMock()
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

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.orders.execute_signal.assert_called_once()
    assert runner.stats["signals"] == 1


@pytest.mark.unit
def test_on_kline_closed_ignores_other_timeframes(runner):
    """非 15m K线闭合被忽略。"""
    runner.engine = MagicMock()
    runner._on_kline_closed("BTCUSDT", "4h", [MagicMock()])
    runner.engine.run.assert_not_called()
