"""启动时状态同步 — 与交易所对账，确保本地状态与链上一致。"""

import logging
from typing import Dict, Optional

from execution.order_gateway import OrderGateway
from portfolio.tracker import PortfolioTracker, Position

logger = logging.getLogger(__name__)


class StartupReconciler:
    """启动时检查持仓与交易所是否一致。"""

    def __init__(self, gateway: OrderGateway, portfolio: PortfolioTracker):
        self.gateway = gateway
        self.portfolio = portfolio

    def reconcile(self) -> Dict:
        """获取交易所持仓并与本地比对，返回差异报告。"""
        remote = self._fetch_remote_positions()
        local = {s: p.quantity for s, p in self.portfolio.positions.items()}

        diff = {"remote_only": [], "local_only": [], "matched": []}
        for symbol, qty in remote.items():
            if symbol in local:
                diff["matched"].append(symbol)
            else:
                diff["remote_only"].append(symbol)
        for symbol in local:
            if symbol not in remote:
                diff["local_only"].append(symbol)

        if diff["remote_only"] or diff["local_only"]:
            logger.warning("Position drift detected: %s", diff)
        else:
            logger.info("Position state consistent (%d positions)", len(local))

        return diff

    def _fetch_remote_positions(self) -> Dict[str, float]:
        """从交易所获取当前持仓。"""
        try:
            account = self.gateway.get_account()
            positions = account.get("positions", [])
            result = {}
            for p in positions:
                amt = float(p.get("positionAmt", 0))
                if abs(amt) > 0.0001:
                    result[p["symbol"]] = amt
            return result
        except Exception as e:
            logger.error("Failed to fetch remote positions: %s", e)
            return {}
