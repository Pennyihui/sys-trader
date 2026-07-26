"""永续合约资金费率追踪与影响计算。"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_FUNDING_INTERVAL_SECONDS = 8 * 3600  # 8h


@dataclass
class FundingRecord:
    symbol: str
    rate: float
    time: float = field(default_factory=time.time)


class FundingRateTracker:
    """追踪资金费率，计算持仓资金成本。"""

    def __init__(self):
        self._rates: Dict[str, FundingRecord] = {}

    def update(self, symbol: str, rate: float):
        self._rates[symbol] = FundingRecord(symbol=symbol, rate=rate)

    def get_rate(self, symbol: str) -> Optional[float]:
        r = self._rates.get(symbol)
        return r.rate if r else None

    def estimate_cost(self, symbol: str, position_value: float, hours: float = 8) -> float:
        rate = self.get_rate(symbol)
        if rate is None:
            return 0.0
        intervals = hours / 8
        return position_value * rate * intervals

    def annualized_rate(self, symbol: str) -> Optional[float]:
        rate = self.get_rate(symbol)
        if rate is None:
            return None
        return rate * 3 * 365

    def next_funding_time(self) -> float:
        now = time.time()
        elapsed = now % _FUNDING_INTERVAL_SECONDS
        return now + (_FUNDING_INTERVAL_SECONDS - elapsed)
