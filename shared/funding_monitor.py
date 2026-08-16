"""资金费率监控 — 每 8 小时抓取实盘费率，计算持仓资金成本。"""

import logging
import threading
import time
from typing import Callable, Dict, Optional

import requests

from shared.funding_rate import FundingRateTracker

logger = logging.getLogger(__name__)

CHECK_INTERVAL = 8 * 3600  # 8h
API_URL_LIVE = "https://fapi.binance.com/fapi/v1/premiumIndex"
API_URL_TESTNET = "https://testnet.binancefuture.com/fapi/v1/premiumIndex"


class FundingRateMonitor:
    """监控实盘资金费率，计算持仓成本，超阈值告警。"""

    def __init__(self, portfolio, cost_threshold: float = 1.0,
                 on_alert: Optional[Callable] = None,
                 price_fn: Optional[Callable[[str], Optional[float]]] = None,
                 on_cost: Optional[Callable[[str, float], None]] = None,
                 testnet: bool = True,
                 proxy_host: str = "127.0.0.1", proxy_port: int = 7897):
        self.portfolio = portfolio
        self.cost_threshold = cost_threshold
        self.on_alert = on_alert or (lambda msg: None)
        # 资金费记账回调 (2026-08-16): 每个 8h 结算周期把持仓资金成本
        # 计入本地已实现盈亏, 不再只是告警
        self.on_cost = on_cost or (lambda symbol, cost: None)
        # 实时价回调 (如 feed.get_last_price), 缺省时回退 entry_price 计价
        self.price_fn = price_fn
        # 2026-08-16 修复: 原实现硬编码实盘 URL 且不走代理 → 国内直连超时。
        # 按运行环境选 testnet/实盘 + 显式代理 (与 feed/gateway 一致)
        self.api_url = API_URL_TESTNET if testnet else API_URL_LIVE
        self.proxies = {"http": f"http://{proxy_host}:{proxy_port}",
                        "https": f"http://{proxy_host}:{proxy_port}"}
        self.tracker = FundingRateTracker()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def fetch_rate(self, symbol: str) -> Optional[float]:
        """从公开 API 获取当前资金费率。"""
        try:
            resp = requests.get(self.api_url, params={"symbol": symbol},
                                timeout=10, proxies=self.proxies)
            resp.raise_for_status()
            rate = float(resp.json().get("lastFundingRate", 0))
            self.tracker.update(symbol, rate)
            return rate
        except Exception as e:
            logger.error("Funding rate fetch failed %s: %s", symbol, e)
            return None

    def check_positions(self, apply_cost: bool = True):
        """计算所有持仓的资金成本，超阈值告警; apply_cost=True 时把估算的
        8h 资金成本计入本地已实现盈亏 (首个周期不记账, 防启动即重复计提)。"""
        # 2026-08-16 审计: 持仓 dict 可能被其他线程写, 遍历前做快照
        try:
            positions = {s: p for s, p in
                         getattr(self.portfolio, "positions", {}).items()}
        except RuntimeError:
            logger.warning("Funding monitor: positions 快照失败 (并发写), 跳过本轮")
            return
        max_cost = 0.0
        for symbol, pos in positions.items():
            rate = self.fetch_rate(symbol)
            if rate is None:
                continue
            live_price = self.price_fn(symbol) if self.price_fn else None
            value = pos.quantity * (live_price if live_price else pos.entry_price)
            cost = self.tracker.estimate_cost(symbol, value, hours=8)
            max_cost = max(max_cost, cost)
            if cost > 0 and apply_cost:
                # 记账: 估算的 8h 资金成本计入本地已实现盈亏
                self.on_cost(symbol, cost)
            if cost >= self.cost_threshold:
                msg = f"Funding cost {symbol}: {cost:.2f} USDT/8h (rate={rate:.4%})"
                logger.warning(msg)
                self.on_alert(msg)
        # gauge → heartbeat → 运维面板资金费卡 (2026-08-16 面板二期)
        try:
            from monitor.collector import MetricsCollector
            MetricsCollector.instance().set_gauge("funding_cost", round(max_cost, 4))
        except Exception:
            pass

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("FundingRateMonitor started (8h cycle)")

    def _run(self):
        # 2026-08-16 审计: 循环级异常兜底 — 此前 check_positions 抛异常会
        # 静默杀死本线程, 资金费监控永久失效且无任何告警
        first = True
        while not self._stop.is_set():
            try:
                self.check_positions(apply_cost=not first)
            except Exception as e:
                logger.error("FundingRateMonitor cycle failed: %s", e)
            first = False
            self._stop.wait(timeout=CHECK_INTERVAL)

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        logger.info("FundingRateMonitor stopped")
