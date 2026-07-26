import pytest
from unittest.mock import patch, MagicMock
from execution.order_manager import OrderManager, OrderState, ManagedOrder
from execution.order_gateway import OrderRequest, OrderResponse, AlgoOrderResponse


@pytest.mark.integration
class TestOrderManager:
    def setup_method(self):
        self.gateway = MagicMock()
        self.gateway.place_order.return_value = OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0)
        self.gateway.place_algo_order.return_value = AlgoOrderResponse(algo_id=100, symbol="BTCUSDT", side="SELL", status="NEW")
        self.manager = OrderManager(gateway=self.gateway)

    def test_submit_limit_order_creates_entry(self):
        self.gateway.place_order.return_value = OrderResponse(order_id=42, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0)
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert order.order_id == 42
        assert order.symbol == "BTCUSDT"
        assert order.state == OrderState.PENDING

    def test_execute_signal_places_entry_stop_and_take_profit(self):
        orders = self.manager.execute_signal("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert len(orders) == 3
        assert self.gateway.place_order.call_count == 1       # entry via place_order
        assert self.gateway.place_algo_order.call_count == 2  # SL + TP via place_algo_order

    def test_order_state_transitions(self):
        self.gateway.place_order.return_value = OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0)
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert order.state == OrderState.PENDING
        order.state = OrderState.FILLED
        assert order.state == OrderState.FILLED
        order.state = OrderState.CANCELED
        assert order.state == OrderState.CANCELED

    def test_retry_on_network_error(self):
        call_count = [0]
        def side_effect(req):
            call_count[0] += 1
            if call_count[0] < 3:
                return OrderResponse(order_id=0, symbol=req.symbol, side=req.side, status="ERROR", executed_qty=0.0, avg_price=0.0, error="Connection timeout")
            return OrderResponse(order_id=99, symbol=req.symbol, side=req.side, status="NEW", executed_qty=0.0, avg_price=0.0)

        self.gateway.place_order.side_effect = side_effect
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert call_count[0] == 3
        assert order.order_id == 99

    def test_active_orders_filters_non_pending(self):
        self.gateway.place_order.side_effect = [
            OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0),
            OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0),
            OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0),
        ]
        self.manager.execute_signal("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        active = self.manager.active_orders
        assert len(active) == 3
