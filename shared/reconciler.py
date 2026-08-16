"""持续对账循环 — 运行时定期检查本地持仓 vs 交易所。"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from execution.order_gateway import OrderGateway
from monitor.collector import MetricsCollector
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

    def _fetch_account(self, cached_account: Optional[Dict] = None) -> Optional[Dict]:
        """获取账户快照; 失败或响应无效返回 None (调用方必须跳过本轮对账)。

        2026-08-16 修复: 旧实现失败时返回 {}, 调用方把空 positions 当成
        "交易所持仓全部消失" → local_only 假漂移 → 每 5 分钟假平仓+重导入
        (代理抖动时反复发生, 已实现盈亏/连亏统计被污染)。
        """
        try:
            acc = cached_account or self.gateway.get_account()
        except Exception as e:
            logger.error("Reconciler: account fetch failed: %s", e)
            return None
        if not isinstance(acc, dict) or acc.get("error") or "positions" not in acc:
            logger.error("Reconciler: account response invalid, skip cycle: %.120s",
                         str(acc))
            return None
        return acc

    def _fetch_remote(self, cached_account: Optional[Dict] = None) -> Optional[Dict[str, dict]]:
        acc = self._fetch_account(cached_account)
        if acc is None:
            return None
        result = {}
        for p in acc.get("positions", []):
            amt = float(p.get("positionAmt", 0))
            if abs(amt) > 0.0001:
                result[p["symbol"]] = {
                    "qty": amt,  # 带符号: LONG>0 / SHORT<0
                    "entry": float(p.get("entryPrice", 0) or 0),
                }
        return result

    def reconcile(self, cached_account: Optional[Dict] = None) -> ReconcileReport:
        remote = self._fetch_remote(cached_account)
        if remote is None:
            # 远端状态不可确认: 跳过本轮, 不产生任何漂移结论
            report = ReconcileReport(
                drift=False,
                details={"remote_only": [], "local_only": [], "qty_mismatch": []},
                timestamp=time.time(),
            )
            logger.warning("Reconciler: 账户不可达, 跳过本轮对账 (保留本地状态)")
            return report
        # 快照: feed 线程可能在迭代期间写 positions (open_position), 避免并发改 dict
        local = self.portfolio.positions_snapshot() \
            if hasattr(self.portfolio, "positions_snapshot") \
            else {s: p for s, p in list(self.portfolio.positions.items())}
        diff = {"remote_only": [], "local_only": [], "qty_mismatch": []}
        for sym, rp in remote.items():
            r_qty = float(rp["qty"])
            if sym in local:
                lp = local[sym]
                r_dir = "LONG" if r_qty > 0 else "SHORT"
                # 2026-08-16 审计: 旧实现 abs(qty - local) 对做空恒误报
                # (远端 positionAmt 为负, 本地 qty 恒正)。
                # 先比方向再比数量绝对值。
                if r_dir != lp.direction or abs(abs(r_qty) - lp.quantity) > 0.0001:
                    diff["qty_mismatch"].append({
                        "symbol": sym,
                        "local": lp.quantity,
                        "local_direction": lp.direction,
                        "remote": abs(r_qty),
                        "remote_direction": r_dir,
                    })
            else:
                diff["remote_only"].append(
                    {"symbol": sym, "qty": r_qty, "entry": rp["entry"]}
                )
        for sym in local:
            if sym not in remote:
                diff["local_only"].append(sym)
        # 余额层对账 (P1-2): 交易所权益 (含未实现) vs 本地权益,
        # > 2 USDT 且 > 2% 判定漂移 (本地每 60s 才同步一次, 阈值留噪声余量)
        acc = cached_account if cached_account is not None else self._fetch_account()
        if isinstance(acc, dict) and acc.get("totalWalletBalance"):
            remote_equity = float(acc["totalWalletBalance"])
            local_equity = self.portfolio.total_equity
            if local_equity > 0:
                gap = abs(remote_equity - local_equity)
                if gap > max(2.0, local_equity * 0.02):
                    diff["balance_drift"] = {
                        "remote": round(remote_equity, 2),
                        "local": round(local_equity, 2),
                        "gap": round(gap, 2),
                    }
        drift = bool(diff["remote_only"] or diff["local_only"]
                     or diff["qty_mismatch"] or diff.get("balance_drift"))
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
            # 模块心跳: 每轮对账循环标记 reconciler 存活
            MetricsCollector.instance().heartbeat("reconciler")
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
