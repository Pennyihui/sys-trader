"""OrderGateway — Binance Futures REST API wrapper for order placement."""

import os
import hmac
import hashlib
import random
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

    def __init__(self, testnet: bool = True, proxy_host: Optional[str] = None,
                 proxy_port: Optional[int] = None):
        self.testnet = testnet
        self.api_key = os.environ.get("BINANCE_API_KEY", "")
        self.api_secret = os.environ.get("BINANCE_API_SECRET", "")
        self.base_url = self.BASE_URL_TESTNET if testnet else self.BASE_URL_LIVE
        # 代理地址统一读环境变量（缺省 127.0.0.1:7897 本机直跑；Docker 路径由
        # PROXY_HOST=host.docker.internal 指向宿主机 Clash），与 feed.py 一致
        if proxy_host is None:
            proxy_host = os.environ.get("PROXY_HOST", "127.0.0.1")
        if proxy_port is None:
            proxy_port = int(os.environ.get("PROXY_PORT", "7897"))
        # 显式代理（与 feed.py 一致），不依赖 Windows 系统代理设置
        # testnet 与实盘走同一通道，确保测试链路与生产一致
        self.proxies = {
            "http": f"http://{proxy_host}:{proxy_port}",
            "https": f"http://{proxy_host}:{proxy_port}",
        }
        # 业务错误退避（429 限流 / -1021 时间戳超窗）在 _request 内部重试，
        # 网络异常仍由外层 @retrier 处理
        self.retry_business_errors = 3
        self.retry_business_backoff = 1.0
        # 服务器时钟偏移校准（-1021 时间戳超窗的根治）：代理延迟尖峰时
        # 重新签名只是刷新本机时间戳，若本机时钟与服务器偏差仍会超窗；
        # 用 /fapi/v1/time 校准偏移（缓存 60s），签名时叠加
        self._time_offset = 0
        self._last_sync = 0.0

    def _sign(self, params: dict) -> str:
        query = urlencode(params)
        return hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()

    @staticmethod
    def _fmt_qty(qty: float) -> str:
        """格式化数量为固定小数位字符串, 避免极小值走科学计数法。

        Binance 拒绝 "8e-05" 这类指数记法 (STRATEGY_PARAM_INVALID),
        需以纯十进制提交 (如 "0.00008")。format(qty, 'f') 输出固定小数,
        再去除尾部无意义的 0。
        """
        if not qty:
            return "0"
        return format(qty, "f").rstrip("0").rstrip(".")

    def _sync_server_time(self):
        """从 /fapi/v1/time 校准本机与服务器时钟偏移（Binance 官方推荐）。

        -1021（timestamp outside recvWindow）的根治：代理延迟尖峰导致
        本机时间戳超窗时，重试只能刷新本机时间，偏移依旧。校准后签名
        时间戳叠加偏移，超窗概率大幅下降。失败时偏移退化 0（现状）。
        """
        try:
            now = int(time.time() * 1000)
            resp = requests.get(
                f"{self.base_url}/fapi/v1/time", timeout=5, proxies=self.proxies
            )
            server = resp.json().get("serverTime")
            if server:
                self._time_offset = int(server) - now
        except Exception as e:
            logger.warning("Server time sync failed (fallback offset=0): %s", e)
            self._time_offset = 0

    @staticmethod
    def _is_business_retryable(resp, body: dict) -> bool:
        """HTTP 429（限流）或 Binance 业务码 -1021（时间戳超窗，代理延迟
        尖峰导致）值得退避重试；其余业务错误（-1100 等）立即返回。"""
        if resp.status_code == 429:
            return True
        return body.get("code") == -1021

    @retrier(max_retries=3, backoff=1.0, retry_on=(requests.exceptions.RequestException,))
    def _request(self, method: str, endpoint: str, params: dict) -> dict:
        # 幂等性风险：429 重试时若服务器已接受请求但响应丢失，可能重复成交。
        # 暂不引入 clientOrderId 幂等键（较大改动，另立待办）。
        url = f"{self.base_url}{endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key}
        # 60s 缓存内校准一次服务器时钟偏移（签名用），失败退化本机时间
        if time.time() - self._last_sync > 60:
            self._sync_server_time()
            self._last_sync = time.time()
        for attempt in range(self.retry_business_errors):
            # 每次重试都重新签名：时间戳必须刷新，否则 -1021 不会自愈
            params["timestamp"] = int(time.time() * 1000) + self._time_offset
            # 放宽签名窗口：国内代理延迟波动可达 6-10s（默认 5s 窗口会被
            # 拖出窗）。15s 容忍度高且对低频策略安全（可配置覆盖）。
            params["recvWindow"] = int(os.environ.get("RECV_WINDOW", "15000"))
            params["signature"] = self._sign(params)
            if method == "POST":
                resp = requests.post(url, headers=headers, data=params, timeout=10, proxies=self.proxies)
            elif method == "DELETE":
                resp = requests.delete(url, headers=headers, data=params, timeout=10, proxies=self.proxies)
            else:
                resp = requests.get(url, headers=headers, params=params, timeout=10, proxies=self.proxies)
            try:
                body = resp.json()
            except ValueError:
                # 代理/CDN 层可能返回非 JSON 页面：body 视为空，
                # 429 状态码本身仍触发重试
                logger.warning(
                    "Non-JSON response %s %s (http=%d): %.200s",
                    method, endpoint, resp.status_code, resp.text,
                )
                body = {}
            if not self._is_business_retryable(resp, body) or attempt == self.retry_business_errors - 1:
                return body
            # 指数退避 + jitter（±10% 随机偏移），避免多实例同时重试打满限流窗口；
            # backoff=0 时 jitter 也为 0（random * 0），测试可即时重试
            delay = ((2 ** attempt) * self.retry_business_backoff
                     + random.uniform(0, 0.1) * self.retry_business_backoff)
            logger.warning(
                "Business error %s %s (attempt %d/%d, http=%d code=%s): retry in %.1fs",
                method, endpoint, attempt + 1, self.retry_business_errors,
                resp.status_code, body.get("code", "?"), delay,
            )
            if delay > 0:
                time.sleep(delay)

    def place_order(self, req: OrderRequest) -> OrderResponse:
        params = {
            "symbol": req.symbol,
            "side": req.side,
            "type": req.order_type,
            "quantity": self._fmt_qty(req.quantity),
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
            params["quantity"] = self._fmt_qty(req.quantity)
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
