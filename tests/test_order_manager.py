import pytest
from unittest.mock import patch, MagicMock
from execution.order_manager import OrderManager, OrderState, ManagedOrder
from execution.order_gateway import OrderRequest, OrderResponse, AlgoOrderResponse
from shared.execution_mode import ExecutionMode, ExecutionModeManager


@pytest.mark.integration
class TestOrderManager:
    def setup_method(self):
        self.gateway = MagicMock()
        self.gateway.place_order.return_value = OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0)
        self.gateway.place_algo_order.return_value = AlgoOrderResponse(algo_id=100, symbol="BTCUSDT", side="SELL", status="NEW")
        # 显式 LIVE：这些用例验证 gateway 调用，OrderManager 默认 DRY_RUN 不触达交易所
        self.manager = OrderManager(gateway=self.gateway, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))

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

    def test_entry_rejected_skips_sl_tp(self):
        """入场单被拒 → 不再下止损/止盈, 只返回入场单。"""
        self.gateway.place_order.return_value = OrderResponse(
            order_id=0, symbol="BTCUSDT", side="BUY", status="REJECTED",
            executed_qty=0.0, avg_price=0.0, error="insufficient margin",
        )
        orders = self.manager.execute_signal("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert len(orders) == 1
        assert orders[0].state == OrderState.REJECTED
        self.gateway.place_algo_order.assert_not_called()

    def test_filled_entry_maps_to_filled_state(self):
        """入场单即时成交 FILLED → OrderState.FILLED (不再是 PENDING)。"""
        self.gateway.place_order.return_value = OrderResponse(
            order_id=42, symbol="BTCUSDT", side="BUY", status="FILLED",
            executed_qty=0.15, avg_price=62500.0,
        )
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert order.state == OrderState.FILLED
        assert order.filled_qty == 0.15
        # 已成交单不进入活跃集 (不再等待成交)
        assert all(o.order_id != 42 for o in self.manager.active_orders)

    def test_partially_filled_maps_to_partially_filled_state(self):
        """PARTIALLY_FILLED → OrderState.PARTIALLY_FILLED (保留在活跃集)。"""
        self.gateway.place_order.return_value = OrderResponse(
            order_id=43, symbol="BTCUSDT", side="BUY", status="PARTIALLY_FILLED",
            executed_qty=0.05, avg_price=62500.0,
        )
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert order.state == OrderState.PARTIALLY_FILLED
        assert any(o.order_id == 43 for o in self.manager.active_orders)


@pytest.mark.unit
def test_publishes_order_filled_after_submit():
    bus = MagicMock()
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=1, symbol="BTCUSDT", side="BUY", status="FILLED",
        executed_qty=0.1, avg_price=64000.0)
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE),
                       event_bus=bus)
    mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    calls = [c[0][0] for c in bus.publish.call_args_list]
    assert "order.filled" in calls


@pytest.mark.unit
def test_publishes_order_filled_payload_has_fields():
    """order.filled 载荷包含 instance/symbol/side/status/quantity/price/order_id。"""
    bus = MagicMock()
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=1, symbol="BTCUSDT", side="BUY", status="FILLED",
        executed_qty=0.1, avg_price=64000.0)
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE),
                       event_bus=bus, instance="paper")
    mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    stream, payload = bus.publish.call_args[0]
    assert stream == "order.filled"
    assert payload["instance"] == "paper"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["side"] == "BUY"
    assert payload["order_type"] == "LIMIT"
    assert payload["status"] == "FILLED"
    assert payload["quantity"] == 0.1
    assert payload["price"] == 64000.0
    assert payload["order_id"] == 1


@pytest.mark.unit
def test_algo_orders_also_publish_order_filled():
    """STOP_MARKET/TAKE_PROFIT_MARKET 走 algo_id 路径，同样发布事件。"""
    bus = MagicMock()
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="FILLED", executed_qty=0.1, avg_price=64000.0)
    gw.place_algo_order.return_value = AlgoOrderResponse(algo_id=100, symbol="BTCUSDT", side="SELL", status="FILLED")
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE),
                       event_bus=bus)
    mgr.execute_signal("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    calls = [c[0][0] for c in bus.publish.call_args_list]
    assert calls.count("order.filled") == 3  # entry + stop_loss + take_profit


@pytest.mark.unit
def test_no_event_bus_is_silent():
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=1, symbol="BTCUSDT", side="BUY", status="FILLED",
        executed_qty=0.1, avg_price=64000.0)
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)  # 不抛异常


@pytest.mark.unit
def test_algo_publish_payload_has_quantity():
    """algo 单（ST/TP）payload 回退到请求侧数量与触发价，order_id 用 algo_id。"""
    bus = MagicMock()
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="FILLED", executed_qty=0.1, avg_price=64000.0)
    gw.place_algo_order.return_value = AlgoOrderResponse(algo_id=100, symbol="BTCUSDT", side="SELL", status="FILLED")
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE),
                       event_bus=bus)
    mgr.submit_stop_loss("BTCUSDT", "LONG", 0.1, 62000.0)
    stream, payload = bus.publish.call_args[0]
    assert stream == "order.filled"
    assert payload["quantity"] == 0.1
    assert payload["price"] == 62000.0  # req.trigger_price = round_price(62000.0)
    assert payload["order_id"] == 100  # algo_id


@pytest.mark.unit
def test_dry_run_does_not_publish():
    """DRY_RUN 无真实成交（status NEW），不发布 order.filled。"""
    bus = MagicMock()
    gw = MagicMock()
    mgr = OrderManager(gateway=gw, event_bus=bus)  # 默认 DRY_RUN
    mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    bus.publish.assert_not_called()
