"""示例策略 — 简单的双均线交叉策略。"""

from typing import Optional

import pandas as pd

from signal_engine.interface import IStrategy, StrategyRegistry


@StrategyRegistry.register
class SMACrossStrategy(IStrategy):
    """简单双均线交叉策略：快线上穿慢线做多，下穿做空。"""

    name = "sma_cross"
    timeframe = "4h"

    def populate_indicators(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fast = self.config.get("fast_period", 7)
        slow = self.config.get("slow_period", 25)
        df["sma_fast"] = df["close"].rolling(fast).mean()
        df["sma_slow"] = df["close"].rolling(slow).mean()
        return df

    def populate_entry_trend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        df["enter_long"] = (df["sma_fast"] > df["sma_slow"]) & (
            df["sma_fast"].shift(1) <= df["sma_slow"].shift(1)
        )
        df["enter_short"] = (df["sma_fast"] < df["sma_slow"]) & (
            df["sma_fast"].shift(1) >= df["sma_slow"].shift(1)
        )
        return df

    def leverage(self, symbol: str) -> int:
        return self.config.get("leverage", 3)
