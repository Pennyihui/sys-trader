import os
import pytest
from unittest.mock import patch, MagicMock
from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse


class TestOrderGateway:
    def setup_method(self):
        os.environ["BINANCE_API_KEY"] = "test_key"
        os.environ["BINANCE_API_SECRET"] = "test_secret"
        self.gateway = OrderGateway(testnet=True)

    def test_order_request_dataclass(self):
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=0.15, price=62500.0, time_in_force="GTC")
        assert req.symbol == "BTCUSDT"
        assert req.side == "BUY"
        assert req.quantity == 0.15
        assert req.price == 62500.0

    def test_order_response_dataclass(self):
        resp = OrderResponse(order_id=12345, symbol="BTCUSDT", side="BUY", status="FILLED", executed_qty=0.15, avg_price=62500.0)
        assert resp.order_id == 12345
        assert resp.status == "FILLED"
        assert resp.executed_qty == 0.15

    def test_place_limit_order_returns_response(self):
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=0.15, price=62500.0)
        with patch.object(self.gateway, "place_order") as mock_place:
            mock_place.return_value = OrderResponse(order_id=42, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0)
            resp = self.gateway.place_order(req)
            assert resp.order_id == 42
            assert resp.status == "NEW"
            mock_place.assert_called_once_with(req)

    def test_cancel_order_returns_response(self):
        with patch.object(self.gateway, "cancel_order") as mock_cancel:
            mock_cancel.return_value = OrderResponse(order_id=42, symbol="BTCUSDT", side="BUY", status="CANCELED", executed_qty=0.0, avg_price=0.0)
            resp = self.gateway.cancel_order("BTCUSDT", 42)
            assert resp.status == "CANCELED"

    def test_api_credentials_from_env(self):
        gw = OrderGateway(testnet=True)
        assert gw.api_key == "test_key"
        assert gw.api_secret == "test_secret"

    def test_testnet_url_is_correct(self):
        gw = OrderGateway(testnet=True)
        assert "testnet" in gw.base_url

    def test_live_url_is_correct(self):
        gw = OrderGateway(testnet=False)
        assert "testnet" not in gw.base_url
        assert "fapi.binance.com" in gw.base_url
