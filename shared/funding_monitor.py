"""资金费率监控 — 每 8 小时抓取实盘费率，计算持仓资金成本。"""

import logging
import threading
import time
from typing import Callable, Dict, Optional

import requests

from shared.funding_rate import FundingRateTracker

logger = logging.getLogger(__name__)

CHECK_INTERVAL = 8 * 3600  # 8h
API_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"


class FundingRateMonitor:
    """监控实盘资金费率，计算持仓成本，超阈值告警。"""

    def __init__(self, portfolio, cost_threshold: float = 1.0,
                 on_alert: Optional[Callable] = None):
        self.portfolio = portfolio
        self.cost_threshold = cost_threshold
        self.on_alert = on_alert or (lambda msg: None)
        self.tracker = FundingRateTracker()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def fetch_rate(self, symbol: str) -> Optional[float]:
        """从实盘公开 API 获取当前资金费率。"""
        try:
            resp = requests.get(API_URL, params={"symbol": symbol}, timeout=10)
            resp.raise_for_status()
            rate = float(resp.json().get("lastFundingRate", 0))
            self.tracker.update(symbol, rate)
            return rate
        except Exception as e:
            logger.error("Funding rate fetch failed %s: %s", symbol, e)
            return None

    def check_positions(self):
        """计算所有持仓的资金成本，超阈值告警。"""
        for symbol, pos in self.portfolio.positions.items():
            rate = self.fetch_rate(symbol)
            if rate is None:
                continue
            value = pos.quantity * pos.entry_price
            cost = self.tracker.estimate_cost(symbol, value, hours=8)
            if cost >= self.cost_threshold:
                msg = f"Funding cost {symbol}: {cost:.2f} USDT/8h (rate={rate:.4%})"
                logger.warning(msg)
                self.on_alert(msg)

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("FundingRateMonitor started (8h cycle)")

    def _run(self):
        self.check_positions()
        while not self._stop.is_set():
            self._stop.wait(timeout=CHECK_INTERVAL)
            self.check_positions()

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        logger.info("FundingRateMonitor stopped")
