"""OrderGateway — Binance Futures REST API wrapper for order placement."""

import os
import hmac
import hashlib
import time
import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode

import requests

from shared.retry import retrier

logger = logging.getLogger(__name__)


@dataclass
class OrderRequest:
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: str = "GTC"
    reduce_only: bool = False


@dataclass
class OrderResponse:
    order_id: int
    symbol: str
    side: str
    status: str
    executed_qty: float
    avg_price: float
    error: Optional[str] = None


@dataclass
class AlgoOrderRequest:
    """条件单请求（Algo Order API /fapi/v1/algoOrder）"""
    symbol: str
    side: str
    algo_type: str = "CONDITIONAL"
    order_type: str = "STOP_MARKET"  # STOP_MARKET / TAKE_PROFIT_MARKET
    quantity: float = 0.0
    trigger_price: Optional[float] = None
    reduce_only: bool = False


@dataclass
class AlgoOrderResponse:
    algo_id: int
    symbol: str
    side: str
    status: str
    error: Optional[str] = None


class OrderGateway:
    BASE_URL_TESTNET = "https://testnet.binancefuture.com"
    BASE_URL_LIVE = "https://fapi.binance.com"

    def __init__(self, testnet: bool = True):
        self.testnet = testnet
        self.api_key = os.environ.get("BINANCE_API_KEY", "")
        self.api_secret = os.environ.get("BINANCE_API_SECRET", "")
        self.base_url = self.BASE_URL_TESTNET if testnet else self.BASE_URL_LIVE

    def _sign(self, params: dict) -> str:
        query = urlencode(params)
        return hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()

    @retrier(max_retries=3, backoff=1.0, retry_on=(requests.exceptions.RequestException,))
    def _request(self, method: str, endpoint: str, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)
        url = f"{self.base_url}{endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key}
        if method == "POST":
            resp = requests.post(url, headers=headers, data=params, timeout=10)
        elif method == "DELETE":
            resp = requests.delete(url, headers=headers, data=params, timeout=10)
        else:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
        return resp.json()

    def place_order(self, req: OrderRequest) -> OrderResponse:
        params = {
            "symbol": req.symbol,
            "side": req.side,
            "type": req.order_type,
            "quantity": str(req.quantity),
        }
        if req.price is not None:
            params["price"] = str(req.price)
            params["timeInForce"] = req.time_in_force
        if req.stop_price is not None:
            params["stopPrice"] = str(req.stop_price)
        if req.reduce_only:
            params["reduceOnly"] = "true"
        try:
            result = self._request("POST", "/fapi/v1/order", params)
            return OrderResponse(
                order_id=result.get("orderId", 0),
                symbol=result.get("symbol", req.symbol),
                side=result.get("side", req.side),
                status=result.get("status", "REJECTED"),
                executed_qty=float(result.get("executedQty", 0)),
                avg_price=float(result.get("avgPrice", 0)),
                error=result.get("msg"),
            )
        except Exception as e:
            logger.error("Order placement failed: %s", e)
            return OrderResponse(
                order_id=0,
                symbol=req.symbol,
                side=req.side,
                status="ERROR",
                executed_qty=0.0,
                avg_price=0.0,
                error=str(e),
            )

    def place_algo_order(self, req: AlgoOrderRequest) -> AlgoOrderResponse:
        """通过 Algo Order API 下达条件单（止损/止盈）。

        testnet 和实盘均支持（需 /fapi/v1/algoOrder 端点）。
        """
        params = {
            "algoType": "CONDITIONAL",
            "symbol": req.symbol,
            "side": req.side,
            "type": req.order_type,
        }
        if req.quantity > 0:
            params["quantity"] = str(req.quantity)
        if req.trigger_price is not None:
            params["triggerPrice"] = str(req.trigger_price)
        if req.reduce_only:
            params["reduceOnly"] = "true"
        try:
            result = self._request("POST", "/fapi/v1/algoOrder", params)
            return AlgoOrderResponse(
                algo_id=result.get("algoId", 0),
                symbol=result.get("symbol", req.symbol),
                side=result.get("side", req.side),
                status=result.get("algoStatus", result.get("status", "REJECTED")),
                error=result.get("msg"),
            )
        except Exception as e:
            logger.error("Algo order placement failed: %s", e)
            return AlgoOrderResponse(algo_id=0, symbol=req.symbol, side=req.side, status="ERROR", error=str(e))

    def cancel_algo_order(self, symbol: str, algo_id: int) -> AlgoOrderResponse:
        """取消条件单。"""
        try:
            result = self._request("DELETE", "/fapi/v1/algoOrder",
                                   {"symbol": symbol, "algoId": str(algo_id)})
            return AlgoOrderResponse(
                algo_id=result.get("algoId", algo_id),
                symbol=symbol,
                side=result.get("side", ""),
                status=result.get("algoStatus", result.get("status", "CANCELED")),
                error=result.get("msg"),
            )
        except Exception as e:
            logger.error("Algo order cancellation failed: %s", e)
            return AlgoOrderResponse(algo_id=algo_id, symbol=symbol, side="", status="ERROR", error=str(e))

    def cancel_order(self, symbol: str, order_id: int) -> OrderResponse:
        try:
            result = self._request(
                "DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": str(order_id)}
            )
            return OrderResponse(
                order_id=result.get("orderId", order_id),
                symbol=symbol,
                side=result.get("side", ""),
                status=result.get("status", "CANCELED"),
                executed_qty=float(result.get("executedQty", 0)),
                avg_price=float(result.get("avgPrice", 0)),
                error=result.get("msg"),
            )
        except Exception as e:
            logger.error("Order cancellation failed: %s", e)
            return OrderResponse(
                order_id=order_id,
                symbol=symbol,
                side="",
                status="ERROR",
                executed_qty=0.0,
                avg_price=0.0,
                error=str(e),
            )

    def get_account(self) -> dict:
        try:
            return self._request("GET", "/fapi/v2/account", {})
        except Exception as e:
            logger.error("Account fetch failed: %s", e)
            return {"error": str(e)}
