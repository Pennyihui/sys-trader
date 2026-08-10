"""高频均值回归策略 — RSI + 布林带。

参考: RSI(14) < 30 + 收盘价跌破下轨 → 做多
      RSI(14) > 70 + 收盘价突破上轨 → 做空
      回归中轨平仓。1h 时间框架，一天可产生多笔信号。
"""

from typing import Optional

import pandas as pd

from signal_engine.interface import IStrategy, StrategyRegistry


@StrategyRegistry.register
class MeanReversionStrategy(IStrategy):
    """RSI + 布林带均值回归策略。"""

    name = "mean_reversion"
    timeframe = "1h"
    max_open_positions = 3

    def populate_indicators(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        period = self.config.get("bb_period", 20)
        std_mult = self.config.get("bb_std", 2.0)
        rsi_period = self.config.get("rsi_period", 14)

        # 布林带
        df["bb_mid"] = df["close"].rolling(period).mean()
        df["bb_std"] = df["close"].rolling(period).std()
        df["bb_upper"] = df["bb_mid"] + std_mult * df["bb_std"]
        df["bb_lower"] = df["bb_mid"] - std_mult * df["bb_std"]

        # RSI
        delta = df["close"].diff()
        gain = delta.clip(lower=0).rolling(rsi_period).mean()
        loss = (-delta.clip(upper=0)).rolling(rsi_period).mean()
        rs = gain / loss.replace(0, 1e-10)
        df["rsi"] = 100 - (100 / (1 + rs))

        # 波动率过滤 (ATR 简化为 20 周期标准差)
        df["atr_pct"] = df["bb_std"] / df["bb_mid"].replace(0, 1e-10)

        return df

    def populate_entry_trend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        oversold = self.config.get("rsi_oversold", 30)
        overbought = self.config.get("rsi_overbought", 70)
        max_vol = self.config.get("max_volatility", 0.05)

        # 做多: RSI < 30 且收盘跌破下轨 (波动率不过高)
        df["enter_long"] = (
            (df["rsi"] < oversold)
            & (df["close"] < df["bb_lower"])
            & (df["atr_pct"] < max_vol)
        )
        # 做空: RSI > 70 且收盘突破上轨
        df["enter_short"] = (
            (df["rsi"] > overbought)
            & (df["close"] > df["bb_upper"])
            & (df["atr_pct"] < max_vol)
        )
        # 每根 K 线最多一个信号，避免连续开仓
        df["enter_long"] = df["enter_long"] & ~df["enter_short"]
        return df

    def populate_exit_trend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """回归中轨平仓。"""
        df["exit_long"] = df["close"] >= df["bb_mid"]
        df["exit_short"] = df["close"] <= df["bb_mid"]
        return df

    def custom_stoploss(self, symbol: str, entry_price: float,
                        current_price: float, direction: str) -> float:
        """固定止损 1.5%。"""
        pct = self.config.get("stop_pct", 0.015)
        if direction == "LONG":
            return round(entry_price * (1 - pct), 2)
        return round(entry_price * (1 + pct), 2)

    def leverage(self, symbol: str) -> int:
        return self.config.get("leverage", 3)
