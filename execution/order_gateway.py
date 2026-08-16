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
    # 幂等键: 重试复用同一 id, 服务器去重防双成交 (2026-08-16 审计)
    client_order_id: Optional[str] = None
    # postOnly: LIMIT_MAKER 挂单, 只吃 maker 费率 (0.05% → 0.02%)
    post_only: bool = False


@dataclass
class OrderResponse:
    order_id: int
    symbol: str
    side: str
    status: str
    executed_qty: float
    avg_price: float
    error: Optional[str] = None
    code: Optional[int] = None  # Binance 业务错误码 (幂等恢复判断用)


@dataclass
class AlgoOrderRequest:
    """条件单请求（Algo Order API /fapi/v1/algoOrder）"""
    symbol: str
    side: str
    algo_type: str = "CONDITIONAL"
    order_type: str = "STOP_MARKET"  # STOP_MARKET / TAKE_PROFIT_MARKET / TRAILING_STOP_MARKET
    quantity: float = 0.0
    trigger_price: Optional[float] = None
    reduce_only: bool = False
    # 追踪止损回撤比例 (TRAILING_STOP_MARKET, 如 1.0 = 1%, 2026-08-16 #5)
    callback_rate: Optional[float] = None
    # 触发基准: CONTRACT_PRICE(最新价, 默认) / MARK_PRICE(标记价, 2026-08-16 #4)
    working_type: Optional[str] = None


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
        时间戳叠加偏移，超窗概率大幅下降。

        失败时保留上一次成功的偏移量（而非归零）——代理抖动导致"问时间"
        超时是常态，归零等于丢弃之前的校准结果，反而在抖动期间失去保护。
        timeout 放宽到 15s，容忍代理延迟尖峰。
        """
        try:
            now = int(time.time() * 1000)
            resp = requests.get(
                f"{self.base_url}/fapi/v1/time", timeout=15, proxies=self.proxies
            )
            server = resp.json().get("serverTime")
            if server:
                self._time_offset = int(server) - now
                self._record_offset(self._time_offset)
                logger.info("Server time synced (offset=%dms)", self._time_offset)
                return
            logger.warning("Server time sync: empty serverTime in response")
        except Exception as e:
            logger.warning(
                "Server time sync failed (keep last offset=%dms): %s",
                self._time_offset, e,
            )

    @staticmethod
    def _record_offset(offset_ms: int):
        """把时间偏移追加到 JSONL 时序文件 + 注册 MetricsCollector gauge。

        用于离线画"时间偏移曲线"，分析时钟漂移/代理延迟趋势。
        文件每行一条 {"ts": <epoch_ms>, "offset_ms": <int>}，与交易系统
        真正用于签名的偏移量一一对应。失败静默（记录是观测增强，不阻塞交易）。
        """
        import json as _json
        path = os.environ.get("TIME_OFFSET_LOG", "logs/time_offset.jsonl")
        try:
            rec = _json.dumps({"ts": int(time.time() * 1000), "offset_ms": int(offset_ms)})
            with open(path, "a", encoding="utf-8") as f:
                f.write(rec + "\n")
        except Exception as e:
            logger.debug("记录时间偏移失败: %s", e)
        try:
            from monitor.collector import MetricsCollector
            MetricsCollector.instance().set_gauge("server_time_offset", float(offset_ms))
        except Exception:
            pass

    @staticmethod
    def _is_business_retryable(resp, body) -> bool:
        """HTTP 429（限流）或 Binance 业务码 -1021（时间戳超窗，代理延迟
        尖峰导致）值得退避重试；其余业务错误（-1100 等）立即返回。

        2026-08-16 修复: 部分接口 (如 /fapi/v1/income) 成功响应是 list,
        旧实现 body.get("code") 对 list 直接 AttributeError 崩溃。
        """
        if resp.status_code == 429:
            return True
        if isinstance(body, dict):
            return body.get("code") == -1021
        return False

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
            # 请求超时必须 ≥ recvWindow: 客户端在服务器签名窗口内超时抛错后,
            # @retrier 会用新 timestamp 重发 → 同一订单可能被服务器接受两次。
            # 超时放大到 recvWindow + 5s, 宁可等也不制造"超时+重发"双成交窗口
            # (2026-08-16 审计: 原 timeout=10s < recvWindow=15s)。
            timeout = max(15.0, int(params["recvWindow"]) / 1000.0 + 5.0)
            if method == "POST":
                resp = requests.post(url, headers=headers, data=params, timeout=timeout, proxies=self.proxies)
            elif method == "DELETE":
                resp = requests.delete(url, headers=headers, data=params, timeout=timeout, proxies=self.proxies)
            elif method == "PUT":
                # 2026-08-16 修复: 此前 PUT 落到 else 走 GET, listenKey 保活
                # (PUT /fapi/v1/listenKey) 实际发成 GET → 每 30min 保活必失败
                resp = requests.put(url, headers=headers, data=params, timeout=timeout, proxies=self.proxies)
            else:
                resp = requests.get(url, headers=headers, params=params, timeout=timeout, proxies=self.proxies)
            # 限流余量观测 (2026-08-16 #8): 解析响应头 X-MBX-USED-WEIGHT-1M
            # 注册 gauge → 心跳 → 面板, 429 前提前看到权重占用
            try:
                used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                if used is not None:
                    from monitor.collector import MetricsCollector
                    MetricsCollector.instance().set_gauge("api_weight_used", float(used))
            except Exception:
                pass
            try:
                body = resp.json()
            except ValueError:
                # 代理/CDN 层返回非 JSON 页面 (SSL EOF / 502 等瞬时代理故障):
                # 抛错触发外层 @retrier 重试, 不再静默返回空 body 伪装成业务拒单
                # (2026-08-16 审计: 原实现会把代理故障记成 REJECTED 且无 error 信息)。
                logger.warning(
                    "Non-JSON response %s %s (http=%d): %.200s",
                    method, endpoint, resp.status_code, resp.text,
                )
                raise requests.exceptions.RequestException(
                    f"Non-JSON response (http={resp.status_code})"
                )
            if not self._is_business_retryable(resp, body) or attempt == self.retry_business_errors - 1:
                # HTTP 5xx (网关/代理层错误, 非业务拒绝) 抛错触发外层重试;
                # 业务码错误 (4xx JSON) 按既有语义返回调用方
                if resp.status_code >= 500:
                    msg = body.get("msg", "") if isinstance(body, dict) else ""
                    raise requests.exceptions.HTTPError(f"HTTP {resp.status_code} {msg}")
                return body
            # -1021 (时间戳超窗) 重试前强制重校准服务器时钟: 代理延迟尖峰期间
            # 偏移漂移可达数秒, 60s 缓存的旧偏移会让重试反复 -1021
            # (2026-08-16 实测: 20:34-20:37 代理抖动导致连续预检失败)
            if isinstance(body, dict) and body.get("code") == -1021:
                self._sync_server_time()
                self._last_sync = time.time()
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
            "type": "LIMIT_MAKER" if req.post_only else req.order_type,
            "quantity": self._fmt_qty(req.quantity),
        }
        if req.price is not None and not req.post_only:
            params["price"] = str(req.price)
            params["timeInForce"] = req.time_in_force
        elif req.price is not None and req.post_only:
            params["price"] = str(req.price)  # LIMIT_MAKER 无需 timeInForce
        if req.stop_price is not None:
            params["stopPrice"] = str(req.stop_price)
        if req.reduce_only:
            params["reduceOnly"] = "true"
        # 幂等键: 同一订单重试复用同一 newClientOrderId, 交易所按 id 去重,
        # 网络层"超时+重发"不再产生第二笔成交 (2026-08-16 审计)
        if req.client_order_id:
            params["newClientOrderId"] = req.client_order_id
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
                code=result.get("code"),
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
        if req.callback_rate is not None:
            params["callbackRate"] = str(req.callback_rate)
        if req.working_type:
            params["workingType"] = req.working_type
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

    @staticmethod
    def _status_or_fail(body: dict, fallback: str) -> str:
        """解析响应状态: 缺 status 字段时偏向失败。

        Binance 成功响应都带 status 字段; 错误响应体只有 {code, msg}。
        若按 fallback 直接返回 (如 "CANCELED") 会把撤单失败误报为成功
        (ERROR_LEDGER BUG-003 教训: 默认值偏向失败)。
        """
        raw = body.get("status") or body.get("algoStatus")
        if raw is not None:
            return str(raw)
        code = body.get("code")
        if code is not None and code not in (0, 200):
            return "REJECTED"
        return fallback

    def cancel_algo_order(self, symbol: str, algo_id: int) -> AlgoOrderResponse:
        """取消条件单。"""
        try:
            result = self._request("DELETE", "/fapi/v1/algoOrder",
                                   {"symbol": symbol, "algoId": str(algo_id)})
            return AlgoOrderResponse(
                algo_id=result.get("algoId", algo_id),
                symbol=symbol,
                side=result.get("side", ""),
                status=self._status_or_fail(result, "CANCELED"),
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
                status=self._status_or_fail(result, "CANCELED"),
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

    def get_commission_rate(self, symbol: str) -> Optional[dict]:
        """实际手续费率 (GET /fapi/v1/commissionRate) — 2026-08-16 #1。

        返回 {symbol, makerCommissionRate, takerCommissionRate} (字符串小数,
        含 VIP/BNB 折扣); 失败返回 None (调用方保留默认费率)。
        """
        try:
            result = self._request("GET", "/fapi/v1/commissionRate", {"symbol": symbol})
            if isinstance(result, dict) and "takerCommissionRate" in result:
                return result
            logger.warning("get_commission_rate %s: 非预期响应 %s", symbol, str(result)[:120])
            return None
        except Exception as e:
            logger.warning("get_commission_rate %s failed: %s", symbol, e)
            return None

    def get_position_risks(self) -> Optional[list]:
        """持仓风险明细 (GET /fapi/v3/positionRisk) — 2026-08-16 #2。

        返回全部 symbol 的风险列表 (含 liquidationPrice/adlQuantile/markPrice/
        isolatedMargin/unRealizedProfit); 失败返回 None。
        """
        try:
            result = self._request("GET", "/fapi/v3/positionRisk", {})
            if isinstance(result, list):
                return result
            logger.warning("get_position_risks: 非预期响应 %s", str(result)[:120])
            return None
        except Exception as e:
            logger.warning("get_position_risks failed: %s", e)
            return None

    def get_multi_assets_mode(self) -> Optional[bool]:
        """多资产保证金模式 (GET /fapi/v1/multiAssetsMargin) — 2026-08-16 #6。

        True=多资产模式 (保证金口径变化); False=单资产; None=查询失败。
        """
        try:
            result = self._request("GET", "/fapi/v1/multiAssetsMargin", {})
            if isinstance(result, dict) and "multiAssetsMargin" in result:
                return bool(result["multiAssetsMargin"])
            logger.warning("get_multi_assets_mode: 非预期响应 %s", str(result)[:120])
            return None
        except Exception as e:
            logger.warning("get_multi_assets_mode failed: %s", e)
            return None

    def get_income(self, income_type: str = "FUNDING_FEE",
                   start_time_ms: Optional[int] = None,
                   limit: int = 1000) -> Optional[list]:
        """资金流水查询 (GET /fapi/v1/income) — 精确资金费对账 (2026-08-16 #6)。

        返回 [{symbol, incomeType, income, time, tranId, ...}] 列表;
        None = 端点不可用 (testnet 无权限 -2014 / 网络异常), [] = 可用但无流水。
        tranId 全局递增, 消费侧按 tranId 去重后逐笔记账, 替代估算口径。
        """
        params: dict = {"incomeType": income_type, "limit": str(min(max(int(limit), 1), 1000))}
        if start_time_ms is not None:
            params["startTime"] = str(int(start_time_ms))
        try:
            result = self._request("GET", "/fapi/v1/income", params)
            if isinstance(result, list):
                return result
            logger.warning("get_income: 非预期响应 %s", str(result)[:120])
            return None
        except Exception as e:
            logger.error("get_income failed: %s", e)
            return None

    # ─── 账户配置: 杠杆 / 持仓模式 / 保证金模式 ───

    def change_leverage(self, symbol: str, leverage: int) -> Optional[int]:
        """设置合约杠杆 (POST /fapi/v1/leverage)。返回生效的杠杆, 失败返回 None。

        2026-08-16 审计补缺: 此前全系统从未设置交易所杠杆, 风控按策略 3x
        计算保证金, 实际却是账户默认杠杆 — 集中度/保证金率全部失真。
        """
        try:
            result = self._request(
                "POST", "/fapi/v1/leverage",
                {"symbol": symbol, "leverage": str(int(leverage))},
            )
            if result and result.get("leverage") is not None:
                logger.info("Leverage set %s = %sx", symbol, result["leverage"])
                return int(result["leverage"])
            logger.warning("change_leverage %s failed: %s", symbol, result)
            return None
        except Exception as e:
            logger.error("change_leverage %s exception: %s", symbol, e)
            return None

    def get_position_mode_dual(self) -> Optional[bool]:
        """查询持仓模式: True=双向持仓 (hedge), False=单向。失败返回 None。

        系统按单向持仓设计 (symbol → 单一 Position), 双向模式下方向语义会错乱。
        """
        try:
            result = self._request("GET", "/fapi/v1/positionSide/dual", {})
            return bool(result.get("dualSidePosition", False))
        except Exception as e:
            logger.error("get positionSide/dual failed: %s", e)
            return None

    def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> bool:
        """设置保证金模式 (ISOLATED/CROSSED)。有持仓时切换会被交易所拒绝,
        -4046 = 已是该模式 (视为成功)。失败仅告警不致命 (手动处理)。"""
        try:
            result = self._request(
                "POST", "/fapi/v1/marginType",
                {"symbol": symbol, "marginType": margin_type},
            )
            code = result.get("code") if isinstance(result, dict) else None
            if code in (None, 200, -4046):
                logger.info("Margin type %s = %s", symbol, margin_type)
                return True
            logger.warning("set_margin_type %s → %s failed: %s",
                           symbol, margin_type, result.get("msg", ""))
            return False
        except Exception as e:
            logger.error("set_margin_type %s exception: %s", symbol, e)
            return False

    # ─── 撤单 ───

    def get_open_algo_orders(self, symbol: str) -> Optional[set]:
        """查询该 symbol 当前开放的 Algo 条件单 (GET /fapi/v1/algoOrder)。

        返回开放单的 algoId 集合; 查询失败返回 None (调用方保持原状态)。
        2026-08-16 S1: 保护单 (SL/TP) 触发后从开放清单消失, 本地据此判定
        "已触发平仓" — 此前全系统无任何条件单状态跟踪通道。
        """
        try:
            result = self._request(
                "GET", "/fapi/v1/algoOrder", {"symbol": symbol})
            if isinstance(result, list):
                return {int(r.get("algoId", 0)) for r in result if r.get("algoId")}
            logger.warning("get_open_algo_orders %s: 非预期响应 %s", symbol,
                           str(result)[:120])
            return None
        except Exception as e:
            logger.warning("get_open_algo_orders %s failed: %s", symbol, e)
            return None

    def cancel_all_open_orders(self, symbol: str) -> int:
        """撤销该 symbol 全部挂单 (DELETE /fapi/v1/allOpenOrders)。

        返回交易所确认撤单数, 失败返回 -1。用于人工清场 (Telegram /cancelall),
        会同时撤掉 SL/TP 保护单 — 仅限明确的人工操作路径调用。
        """
        try:
            result = self._request(
                "DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol},
            )
            # 成功响应可能是空数组 [] 或带 code 的错误体
            if isinstance(result, dict) and result.get("code") not in (None, 0, 200):
                logger.warning("cancel_all_open_orders %s failed: %s",
                               symbol, result.get("msg", ""))
                return -1
            n = len(result) if isinstance(result, list) else -1
            logger.warning("CANCEL ALL %s: 已撤 %d 笔挂单", symbol, n)
            return n
        except Exception as e:
            logger.error("cancel_all_open_orders %s exception: %s", symbol, e)
            return -1

    def query_order_status(self, symbol: str, order_id: int) -> Optional[dict]:
        """查询订单状态 (GET /fapi/v1/order)。

        供 PENDING 入场单成交轮询使用; 失败返回 None (调用方保持原状态)。
        """
        try:
            result = self._request(
                "GET", "/fapi/v1/order",
                {"symbol": symbol, "orderId": str(order_id)},
            )
            if result and result.get("status"):
                return result
            return None
        except Exception as e:
            logger.warning("Query order %s/%s failed: %s", symbol, order_id, e)
            return None

    def query_order_by_client_id(self, symbol: str, client_order_id: str) -> Optional[dict]:
        """按 newClientOrderId 查询订单 — 幂等恢复路径: 下单请求超时/响应丢失后,
        服务器可能已接受, 用同一 id 查回真实状态 (避免重复下单)。"""
        try:
            result = self._request(
                "GET", "/fapi/v1/order",
                {"symbol": symbol, "origClientOrderId": client_order_id},
            )
            if result and result.get("status"):
                return result
            return None
        except Exception as e:
            logger.warning("Query order by clientId failed: %s", e)
            return None

    # ─── User Data Stream (listenKey) ───

    def create_listen_key(self) -> Optional[str]:
        """POST /fapi/v1/listenKey — 创建用户数据流, 返回 listenKey。"""
        try:
            result = self._request("POST", "/fapi/v1/listenKey", {})
            key = result.get("listenKey") if isinstance(result, dict) else None
            if key:
                logger.info("User data stream listenKey created")
                return key
            logger.warning("create_listen_key failed: %s", result)
            return None
        except Exception as e:
            logger.error("create_listen_key exception: %s", e)
            return None

    def keepalive_listen_key(self, listen_key: str = "") -> bool:
        """PUT /fapi/v1/listenKey — 保活 (60 分钟有效期, 建议 30 分钟保活)。

        2026-08-16 审计: 显式携带 listenKey 参数 (旧实现空参, 部分环境会被
        判定无效导致每 30min 强制换 key 断线)。
        """
        try:
            params = {"listenKey": listen_key} if listen_key else {}
            result = self._request("PUT", "/fapi/v1/listenKey", params)
            return bool(isinstance(result, dict) and not result.get("code"))
        except Exception as e:
            logger.warning("keepalive_listen_key failed: %s", e)
            return False
