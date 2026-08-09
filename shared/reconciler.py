"""持续对账循环 — 运行时定期检查本地持仓 vs 交易所。"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from execution.order_gateway import OrderGateway
from portfolio.tracker import PortfolioTracker

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 300


@dataclass
class ReconcileReport:
    drift: bool
    details: Dict
    timestamp: float


class PositionReconciler:
    """定期对账，发现差异告警但不自动修正。"""

    def __init__(self, gateway: OrderGateway, portfolio: PortfolioTracker,
                 interval: float = _CHECK_INTERVAL,
                 on_drift: Optional[Callable] = None):
        self.gateway = gateway
        self.portfolio = portfolio
        self.interval = interval
        self.on_drift = on_drift or (lambda r: None)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _fetch_remote(self, cached_account: Optional[Dict] = None) -> Dict[str, float]:
        try:
            acc = cached_account or self.gateway.get_account()
            positions = acc.get("positions", [])
            return {
                p["symbol"]: float(p.get("positionAmt", 0))
                for p in positions if abs(float(p.get("positionAmt", 0))) > 0.0001
            }
        except Exception as e:
            logger.error("Reconciler: fetch failed: %s", e)
            return {}

    def reconcile(self, cached_account: Optional[Dict] = None) -> ReconcileReport:
        remote = self._fetch_remote(cached_account)
        local = {s: p.quantity for s, p in self.portfolio.positions.items()}
        diff = {"remote_only": [], "local_only": [], "qty_mismatch": []}
        for sym, qty in remote.items():
            if sym in local:
                if abs(qty - local[sym]) > 0.0001:
                    diff["qty_mismatch"].append({"symbol": sym, "local": local[sym], "remote": qty})
            else:
                diff["remote_only"].append(sym)
        for sym in local:
            if sym not in remote:
                diff["local_only"].append(sym)
        drift = bool(diff["remote_only"] or diff["local_only"] or diff["qty_mismatch"])
        report = ReconcileReport(drift=drift, details=diff, timestamp=time.time())
        if drift:
            logger.warning("Position drift: %s", diff)
            self.on_drift(report)
        return report

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("Reconciler started (interval=%ds)", self.interval)

    def _run(self):
        while not self._stop.is_set():
            try:
                self.reconcile()
            except Exception as e:
                # feed 线程可能在迭代期间写 positions (open_position),
                # 竞态异常不能杀死对账线程, 记日志后继续下一轮
                logger.error("Reconciler: reconcile failed: %s", e)
            self._stop.wait(timeout=self.interval)

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        logger.info("Reconciler stopped")
