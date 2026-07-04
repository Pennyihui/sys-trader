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
