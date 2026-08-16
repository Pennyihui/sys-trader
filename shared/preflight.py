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
        else:
            # 账户响应缺 canTrade 键: 不能静默跳过, 标记为失败
            self.results.append(CheckResult("account_reachable", False, "missing canTrade"))
        can_trade = acc.get("canTrade", False)
        self.results.append(CheckResult("can_trade", can_trade,
                                        "enabled" if can_trade else "DISABLED"))
        # 提现权限 (P1-7): 交易账户建议禁提现, 开着只告警不阻断
        if acc.get("canWithdraw", False):
            self.results.append(CheckResult(
                "withdraw_permission", True,
                "WARN: 提现权限已开启, 建议在币安后台禁用该 Key 的提现"))
        else:
            self.results.append(CheckResult("withdraw_permission", True, "提现已禁用"))
        # 权益口径 (P0-4): totalWalletBalance 含未实现盈亏
        total_wb = acc.get("totalWalletBalance")
        total = float(total_wb) if total_wb else sum(
            float(a.get("walletBalance", 0)) for a in acc.get("assets", []))
        enough = total > 10
        self.results.append(CheckResult("balance_sufficient", enough, f"{total:.2f} USDT"))
        all_pass = all(r.passed for r in self.results)
        for r in self.results:
            logger.info("[%s] %s: %s", "PASS" if r.passed else "FAIL", r.name, r.message)
        return acc if all_pass else None
