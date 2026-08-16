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
        """获取交易所持仓并与本地比对，返回差异报告（存在性 + 数量 + 方向）。

        2026-08-16 审计: 与 PositionReconciler 统一——远端 positionAmt 做空为负,
        先比方向再比数量绝对值, 消除做空恒误报。
        """
        remote = self._fetch_remote_positions()
        if remote is None:
            # 账户不可达: 跳过, 不把"拉取失败"误判成"本地持仓全部多余"
            logger.warning("StartupReconciler: 账户不可达, 跳过启动对账")
            return {"remote_only": [], "local_only": [], "qty_mismatch": [],
                    "matched": [], "skipped": True}
        # 快照: 运行中 positions 可能被 feed 线程修改, 避免并发改 dict
        local = {s: p for s, p in list(self.portfolio.positions.items())}

        diff = {"remote_only": [], "local_only": [], "qty_mismatch": [], "matched": []}
        for symbol, rp in remote.items():
            r_qty = float(rp["qty"])
            if symbol in local:
                lp = local[symbol]
                r_dir = "LONG" if r_qty > 0 else "SHORT"
                if r_dir != lp.direction or abs(abs(r_qty) - lp.quantity) > 0.0001:
                    diff["qty_mismatch"].append(
                        {"symbol": symbol, "local": lp.quantity,
                         "local_direction": lp.direction,
                         "remote": abs(r_qty), "remote_direction": r_dir})
                else:
                    diff["matched"].append(symbol)
            else:
                diff["remote_only"].append(
                    {"symbol": symbol, "qty": r_qty, "entry": rp["entry"]})
        for symbol in local:
            if symbol not in remote:
                diff["local_only"].append(symbol)

        if diff["remote_only"] or diff["local_only"] or diff["qty_mismatch"]:
            logger.warning("Position drift detected: %s", diff)
        else:
            logger.info("Position state consistent (%d positions)", len(local))

        return diff

    def _fetch_remote_positions(self) -> Optional[Dict[str, dict]]:
        """从交易所获取当前持仓 (symbol → {qty: 带符号, entry: 开仓均价})。

        失败/响应无效返回 None (调用方跳过对账), 而非空 dict——
        空 dict 会被误读成"交易所无持仓"。
        """
        try:
            account = self.gateway.get_account()
            if not isinstance(account, dict) or account.get("error") \
                    or "positions" not in account:
                logger.error("StartupReconciler: 账户响应无效: %.120s", str(account))
                return None
            result = {}
            for p in account.get("positions", []):
                amt = float(p.get("positionAmt", 0))
                if abs(amt) > 0.0001:
                    result[p["symbol"]] = {
                        "qty": amt,
                        "entry": float(p.get("entryPrice", 0) or 0),
                    }
            return result
        except Exception as e:
            logger.error("Failed to fetch remote positions: %s", e)
            return None
