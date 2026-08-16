"""Orderbook 深度查询与滑点估算 (2026-08-16 P2-2)。

下单前拉取限价深度 (REST GET /fapi/v1/depth, limit=20),
按订单数量逐档吃单估算成交均价 → 与最优买/卖价的偏差 (bps)。
超阈值时 runner 拒绝下单 (MAX_SLIPPAGE_BPS)。
"""

import logging
from typing import List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


class OrderbookDepth:
    def __init__(self, testnet: bool = True,
                 proxy_host: str = "127.0.0.1", proxy_port: int = 7897):
        base = ("https://testnet.binancefuture.com" if testnet
                else "https://fapi.binance.com")
        self.api_url = f"{base}/fapi/v1/depth"
        self.proxies = {"http": f"http://{proxy_host}:{proxy_port}",
                        "https": f"http://{proxy_host}:{proxy_port}"}

    def fetch(self, symbol: str, limit: int = 20) -> Optional[dict]:
        """返回 {bids: [[price, qty]...], asks: [[price, qty]...]}, 失败 None。"""
        try:
            resp = requests.get(
                self.api_url,
                params={"symbol": symbol, "limit": limit},
                proxies=self.proxies, timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            if "bids" not in data or "asks" not in data:
                return None
            return {
                "bids": [[float(p), float(q)] for p, q in data["bids"]],
                "asks": [[float(p), float(q)] for p, q in data["asks"]],
            }
        except Exception as e:
            logger.warning("Orderbook fetch %s failed: %s", symbol, e)
            return None

    @staticmethod
    def estimate_slippage_bps(book: dict, side: str, quantity: float) -> Optional[float]:
        """估算下单 quantity 的成交滑点 (bps, 相对最优价)。吃穿深度或无深度 → None。

        BUY 吃 asks, SELL 吃 bids; 均价 vs 最优价的偏差即滑点。
        """
        if quantity <= 0:  # 2026-08-16 审计: 除零防护
            return None
        levels = book["asks"] if side == "BUY" else book["bids"]
        if not levels:
            return None
        best = levels[0][0]
        remaining = quantity
        notional = 0.0
        for price, qty in levels:
            take = min(remaining, qty)
            notional += take * price
            remaining -= take
            if remaining <= 0:
                break
        if remaining > 0:
            return None  # 深度不足以承接该数量
        avg = notional / quantity
        return abs(avg - best) / best * 10_000
