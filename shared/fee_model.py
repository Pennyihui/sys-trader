"""手续费与滑点模型 — 计算每笔交易的实际成本。"""

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FeeConfig:
    taker_fee_pct: float = 0.0005   # 0.05%
    maker_fee_pct: float = 0.0002   # 0.02%
    slippage_pct: float = 0.0003    # 0.03% 默认滑点


class FeeModel:
    """计算交易成本。"""

    def __init__(self, config: FeeConfig = None):
        self.config = config or FeeConfig()

    def estimate_cost(self, order_type: str, quantity: float, price: float) -> dict:
        """估算单笔交易成本，返回明细。"""
        notional = quantity * price
        fee_pct = self.config.maker_fee_pct if order_type == "LIMIT" else self.config.taker_fee_pct
        fee = notional * fee_pct
        slippage = notional * self.config.slippage_pct if order_type == "MARKET" else 0.0
        total = fee + slippage
        return {
            "notional": round(notional, 2),
            "fee": round(fee, 2),
            "slippage": round(slippage, 2),
            "total_cost": round(total, 2),
            "fee_pct": fee_pct,
        }
