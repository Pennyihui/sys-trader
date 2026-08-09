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
    """LIMIT → cancel_order(orderId); 条件单 → cancel_algo_order(algoId)。"""
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
    runner.gateway.cancel_algo_order.assert_called_once_with("BTCUSDT", 202)


@pytest.mark.unit
def test_emergency_stop_calls_cancel():
    """emergency_stop 后实际遍历活跃订单并撤销 (LIMIT + 条件单各一路)。"""
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
    runner.gateway.cancel_algo_order.assert_called_once_with("BTCUSDT", 202)


@pytest.mark.unit
def test_cancel_algo_error_status_warns_not_crash():
    """条件单撤销返回 ERROR → 记录告警, 不抛异常 (返回值不可忽略)。"""
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.orders.active_orders = [
        ManagedOrder(order_id=202, symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
                     quantity=0.01, price=63000.0, state=OrderState.PENDING),
    ]
    runner.gateway = MagicMock()
    runner.gateway.cancel_algo_order.return_value.status = "ERROR"
    runner._cancel_active_orders()  # 不抛异常即通过
