"""SignalEngine — unified entry point for 4-layer signal generation."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

# 仅在类型检查时导入，避免与 interface.py 的顶层 import 形成循环依赖
if TYPE_CHECKING:
    from signal_engine.interface import IStrategy


@dataclass
class Signal:
    symbol: str
    direction: str  # LONG / SHORT
    conviction: float
    entry_price: float
    stop_loss: float
    take_profit: float
    attribution: Dict[str, float] = field(default_factory=dict)
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class SignalEngine:
    def __init__(self, strategy: Optional["IStrategy"] = None):
        self._weekly_cache: Dict[str, Any] = {}
        self._daily_cache: Dict[str, Any] = {}
        self.strategy = strategy  # 可插拔策略

    def run(self, symbol: str, timeframe: str, ohlcv: List[dict]) -> Optional[Signal]:
        if not ohlcv:
            return None
        # 有策略且 timeframe 匹配时，走策略分析（支持任意策略时间框架）
        if self.strategy is not None and timeframe == self.strategy.timeframe:
            df = pd.DataFrame(ohlcv)
            return self.strategy.analyze(symbol, df)
        # 无策略或 timeframe 不匹配时回退到原有逻辑
        if timeframe == "1w":
            return self._run_weekly(symbol, ohlcv)
        elif timeframe == "1d":
            return self._run_daily(symbol, ohlcv)
        elif timeframe == "4h":
            return self._run_4h(symbol, ohlcv)
        return None

    def _run_weekly(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def _run_daily(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def _run_4h(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def get_weekly_context(self, symbol: str) -> Optional[Any]:
        return self._weekly_cache.get(symbol)

    def get_daily_context(self, symbol: str) -> Optional[Any]:
        return self._daily_cache.get(symbol)
