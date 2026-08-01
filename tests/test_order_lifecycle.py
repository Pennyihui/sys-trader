"""测试订单生命周期持久化。"""
import pytest
from shared.database import TradeDatabase
from execution.order_manager import OrderManager, OrderState
from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse
from shared.paper_trader import PaperTrader
from shared.execution_mode import ExecutionMode, ExecutionModeManager
from unittest.mock import MagicMock, patch


class TestOrderLifecycle:
    def setup_method(self):
        self.db = TradeDatabase(":memory:")
        self.gateway = MagicMock()
        self.gateway.place_order.return_value = OrderResponse(
            order_id=123, symbol="BTCUSDT", side="BUY", status="FILLED",
            executed_qty=0.1, avg_price=60000.0,
        )
        self.manager = OrderManager(gateway=self.gateway, db=self.db)

    def test_create_order_recorded(self):
        order_id = self.db.create_order("BTCUSDT", "BUY", "MARKET", 0.1, 60000.0)
        orders = self.db.get_orders()
        assert len(orders) == 1
        assert orders[0]["status"] == "CREATED"

    def test_order_status_transitions(self):
        order_id = self.db.create_order("BTCUSDT", "BUY", "MARKET", 0.1)
        self.db.update_order_status(order_id, "FILLED", "ex123", 0.1, 60000.0, 3.0)
        orders = self.db.get_orders()
        assert orders[0]["status"] == "FILLED"
        assert orders[0]["exchange_order_id"] == "ex123"

    def test_lookup_by_exchange_id(self):
        order_id = self.db.create_order("BTCUSDT", "BUY", "MARKET", 0.1)
        self.db.update_order_status(order_id, "FILLED", "ex999")
        found = self.db.get_order_by_exchange_id("ex999")
        assert found is not None
        assert found["status"] == "FILLED"

    def test_dry_run_does_not_call_gateway(self):
        mgr = ExecutionModeManager(ExecutionMode.DRY_RUN)
        manager = OrderManager(gateway=self.gateway, db=self.db, execution_mode=mgr)
        order = manager.submit_entry("BTCUSDT", "LONG", 0.1, 60000.0, 59000.0, 62000.0)
        self.gateway.place_order.assert_not_called()

    def test_live_calls_gateway(self):
        mgr = ExecutionModeManager(ExecutionMode.LIVE)
        manager = OrderManager(gateway=self.gateway, db=self.db, execution_mode=mgr)
        order = manager.submit_entry("BTCUSDT", "LONG", 0.1, 60000.0, 59000.0, 62000.0)
        self.gateway.place_order.assert_called()
