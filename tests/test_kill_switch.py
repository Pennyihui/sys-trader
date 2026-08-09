"""Kill switch 测试 — command 事件 → 熔断。"""

import pytest
from unittest.mock import MagicMock
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
