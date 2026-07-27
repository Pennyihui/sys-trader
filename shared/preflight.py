"""启动前校验 — 单次获取账户数据，多项检查。"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from execution.order_gateway import OrderGateway

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str = ""


class PreflightChecker:
    def __init__(self, gateway: OrderGateway):
        self.gateway = gateway
        self.results: List[CheckResult] = []
        self._cached_account: Optional[Dict] = None

    def _get_account(self) -> Optional[Dict]:
        if self._cached_account is None:
            try:
                self._cached_account = self.gateway.get_account()
            except Exception as e:
                logger.error("Account fetch failed: %s", e)
                return None
        return self._cached_account

    def run_all(self) -> Optional[Dict]:
        """执行所有检查。成功返回账户数据快照，失败返回 None。"""
        self.results = []
        acc = self._get_account()
        if acc is None:
            for name in ("account_reachable", "can_trade", "balance_sufficient"):
                self.results.append(CheckResult(name, False, "Account API unreachable"))
            return None
        if "canTrade" in acc:
            self.results.append(CheckResult("account_reachable", True, "OK"))
        can_trade = acc.get("canTrade", False)
        self.results.append(CheckResult("can_trade", can_trade,
                                        "enabled" if can_trade else "DISABLED"))
        total = sum(float(a.get("walletBalance", 0)) for a in acc.get("assets", []))
        enough = total > 10
        self.results.append(CheckResult("balance_sufficient", enough, f"{total:.2f} USDT"))
        all_pass = all(r.passed for r in self.results)
        for r in self.results:
            logger.info("[%s] %s: %s", "PASS" if r.passed else "FAIL", r.name, r.message)
        return acc if all_pass else None
