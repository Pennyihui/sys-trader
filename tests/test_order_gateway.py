import os
import pytest
from unittest.mock import patch, MagicMock
from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse


@pytest.mark.integration
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


@pytest.mark.unit
class TestOrderGatewayRequestRetry:
    """429 限流 / -1021 时间戳超窗的业务退避重试（Task 18）。"""

    def setup_method(self):
        os.environ["BINANCE_API_KEY"] = "test_key"
        os.environ["BINANCE_API_SECRET"] = "test_secret"
        self.gw = OrderGateway(testnet=True)
        self.gw.retry_business_backoff = 0.0  # 测试不等待

    @staticmethod
    def _resp(status_code, payload):
        r = MagicMock()
        r.status_code = status_code
        r.json.return_value = payload
        return r

    def test_429_retries_then_succeeds(self):
        responses = [
            self._resp(429, {"code": -1003, "msg": "Too many requests"}),
            self._resp(200, {"orderId": 42, "status": "NEW"}),
        ]
        with patch("execution.order_gateway.requests.post", side_effect=responses) as mock_post:
            result = self.gw._request("POST", "/fapi/v1/order", {})
        assert result["orderId"] == 42
        assert mock_post.call_count == 2

    def test_minus_1021_retries_then_succeeds(self):
        responses = [
            self._resp(200, {"code": -1021,
                             "msg": "Timestamp for this request is outside of the recvWindow."}),
            self._resp(200, {"orderId": 43, "status": "NEW"}),
        ]
        with patch("execution.order_gateway.requests.post", side_effect=responses) as mock_post:
            result = self.gw._request("POST", "/fapi/v1/order", {})
        assert result["orderId"] == 43
        assert mock_post.call_count == 2

    def test_429_gives_up_after_max_retries(self):
        self.gw.retry_business_errors = 3
        responses = [self._resp(429, {"code": -1003, "msg": "Too many requests"})] * 3
        with patch("execution.order_gateway.requests.post", side_effect=responses) as mock_post:
            result = self.gw._request("POST", "/fapi/v1/order", {})
        assert result["code"] == -1003
        assert mock_post.call_count == 3

    def test_non_retryable_business_error_returns_immediately(self):
        responses = [self._resp(200, {"code": -1100, "msg": "Invalid symbol."})]
        with patch("execution.order_gateway.requests.post", side_effect=responses) as mock_post:
            result = self.gw._request("POST", "/fapi/v1/order", {})
        assert result["code"] == -1100
        assert mock_post.call_count == 1

    def test_get_uses_same_retry_path(self):
        responses = [
            self._resp(429, {"code": -1003, "msg": "Too many requests"}),
            self._resp(200, {}),
        ]
        with patch("execution.order_gateway.requests.get", side_effect=responses) as mock_get:
            result = self.gw._request("GET", "/fapi/v2/account", {})
        assert result == {}
        assert mock_get.call_count == 2

    def test_429_with_non_json_body_still_retries(self):
        """代理/CDN 层返回非 JSON 429 页面：body 视为 {}，状态码仍触发重试。"""
        bad = self._resp(429, {"code": -1003, "msg": "Too many requests"})
        bad.json.side_effect = ValueError("no json body")
        bad.text = "<html>Rate limited by CDN</html>"
        ok = self._resp(200, {"orderId": 44, "status": "NEW"})
        with patch("execution.order_gateway.requests.post",
                   side_effect=[bad, ok]) as mock_post:
            result = self.gw._request("POST", "/fapi/v1/order", {})
        assert result["orderId"] == 44
        assert mock_post.call_count == 2

    def test_non_json_200_body_returns_empty_immediately(self):
        """200 但 body 非 JSON（非重试条件）：返回 {}，不再触发外层重试。"""
        bad = self._resp(200, {})
        bad.json.side_effect = ValueError("no json body")
        bad.text = "<html>ok page</html>"
        with patch("execution.order_gateway.requests.post",
                   side_effect=[bad]) as mock_post:
            result = self.gw._request("POST", "/fapi/v1/order", {})
        assert result == {}
        assert mock_post.call_count == 1
