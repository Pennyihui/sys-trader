"""Kill switch 测试 — command 事件 → 熔断。"""

import pytest
from unittest.mock import MagicMock
from execution.order_manager import ManagedOrder, OrderState
from shared.runner import SystemRunner


@pytest.mark.unit
def test_emergency_stop_blocks_orders():
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.risk_chain = MagicMock()
    runner.portfolio = MagicMock()
    runner.feed = MagicMock()
    runner.step_sizes = {}
    runner._handle_command({"command": "emergency_stop"})
    assert runner._circuit_breaker == "emergency_stop"
    runner._execute_signal(MagicMock())
    runner.orders.execute_signal.assert_not_called()


@pytest.mark.unit
def test_resume_clears_breaker():
    runner = SystemRunner()
    runner._handle_command({"command": "emergency_stop"})
    runner._handle_command({"command": "resume"})
    assert runner._circuit_breaker is None


@pytest.mark.unit
def test_kill_switch_blocks_execution_before_risk():
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.portfolio = MagicMock()
    runner.feed = MagicMock()
    runner.step_sizes = {}
    runner._circuit_breaker = "emergency_stop"
    runner._execute_signal(MagicMock())
    runner.orders.execute_signal.assert_not_called()


@pytest.mark.unit
def test_cancel_active_orders_branches_by_type():
    """熔断撤单: 只撤 LIMIT 入场单, 保留 SL/TP 保护单 (2026-08-16 审计)。"""
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.orders.active_orders = [
        ManagedOrder(order_id=101, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
                     quantity=0.01, price=64000.0, state=OrderState.PENDING),
        ManagedOrder(order_id=202, symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
                     quantity=0.01, price=63000.0, state=OrderState.PENDING),
    ]
    runner.gateway = MagicMock()
    runner._cancel_active_orders()
    runner.gateway.cancel_order.assert_called_once_with("BTCUSDT", 101)
    runner.gateway.cancel_algo_order.assert_not_called()  # 保护单保留


@pytest.mark.unit
def test_emergency_stop_calls_cancel():
    """emergency_stop 后撤入场单, 保留持仓保护单 (TP 不撤)。"""
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.orders.active_orders = [
        ManagedOrder(order_id=101, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
                     quantity=0.01, price=64000.0, state=OrderState.PENDING),
        ManagedOrder(order_id=202, symbol="BTCUSDT", side="SELL", order_type="TAKE_PROFIT_MARKET",
                     quantity=0.01, price=66000.0, state=OrderState.PENDING),
    ]
    runner.gateway = MagicMock()
    runner._handle_command({"command": "emergency_stop"})
    runner.gateway.cancel_order.assert_called_once_with("BTCUSDT", 101)
    runner.gateway.cancel_algo_order.assert_not_called()


@pytest.mark.unit
def test_cancel_protection_kept_no_crash():
    """活跃集只有保护单时熔断撤单为空操作, 不抛异常。"""
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.orders.active_orders = [
        ManagedOrder(order_id=202, symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
                     quantity=0.01, price=63000.0, state=OrderState.PENDING),
    ]
    runner.gateway = MagicMock()
    runner._cancel_active_orders()  # 不抛异常即通过
    runner.gateway.cancel_order.assert_not_called()
