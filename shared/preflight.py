"""启动前校验 — 检查账户余额、API 权限、网络连通性。"""

import logging
import sys
from dataclasses import dataclass, field
from typing import List, Optional

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

    def run_all(self) -> bool:
        self.results = []
        self._check_account()
        self._check_can_trade()
        self._check_balance()
        all_pass = all(r.passed for r in self.results)
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            logger.info("[%s] %s: %s", status, r.name, r.message)
        if not all_pass:
            logger.error("Preflight checks FAILED — aborting startup")
        return all_pass

    def _check_account(self):
        try:
            acc = self.gateway.get_account()
            if "canTrade" in acc:
                self.results.append(CheckResult("account_reachable", True, "Account API reachable"))
            else:
                self.results.append(CheckResult("account_reachable", False, acc.get("msg", "Unknown error")))
        except Exception as e:
            self.results.append(CheckResult("account_reachable", False, str(e)))

    def _check_can_trade(self):
        try:
            acc = self.gateway.get_account()
            can_trade = acc.get("canTrade", False)
            self.results.append(CheckResult("can_trade", can_trade,
                                            "Trading enabled" if can_trade else "Trading DISABLED"))
        except Exception as e:
            self.results.append(CheckResult("can_trade", False, str(e)))

    def _check_balance(self):
        try:
            acc = self.gateway.get_account()
            total = sum(float(a.get("walletBalance", 0)) for a in acc.get("assets", []))
            if total > 10:
                self.results.append(CheckResult("balance_sufficient", True, f"Balance: {total:.2f} USDT"))
            else:
                self.results.append(CheckResult("balance_sufficient", False, f"Balance too low: {total:.2f} USDT"))
        except Exception as e:
            self.results.append(CheckResult("balance_sufficient", False, str(e)))
