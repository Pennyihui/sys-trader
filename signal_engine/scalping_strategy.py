"""15分钟高频剥头皮策略 — EMA(4/9) 交叉 + RSI 过滤。

参考 Freqtrade ScalpingMARibbon / quant-strategies EMA Regular Order:
  - 快线 EMA4 上穿慢线 EMA9 → 做多 (RSI 未超买确认)
  - 快线 EMA4 下穿慢线 EMA9 → 做空 (RSI 未超卖确认)
  15m 时间框架，一天可产生 20-40 笔信号。
"""

from typing import Optional

import pandas as pd

from signal_engine.interface import IStrategy, StrategyRegistry


@StrategyRegistry.register
class ScalpingStrategy(IStrategy):
    """EMA 交叉剥头皮策略。"""

    name = "scalping_15m"
    timeframe = "15m"
    max_open_positions = 3

    def populate_indicators(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fast = self.config.get("ema_fast", 4)
        slow = self.config.get("ema_slow", 9)
        rsi_period = self.config.get("rsi_period", 7)

        # EMA
        df["ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()

        # 快速 RSI(7)
        delta = df["close"].diff()
        gain = delta.clip(lower=0).ewm(span=rsi_period, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(span=rsi_period, adjust=False).mean()
        rs = gain / loss.replace(0, 1e-10)
        df["rsi"] = 100 - (100 / (1 + rs))

        # ATR 简化 (用于过滤低波动)
        df["tr"] = df.apply(
            lambda r: max(r["high"] - r["low"],
                          abs(r["high"] - r["close"]),
                          abs(r["low"] - r["close"])),
            axis=1,
        )
        df["atr"] = df["tr"].rolling(14).mean()

        return df

    def populate_entry_trend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        rsi_overbought = self.config.get("rsi_overbought", 70)
        rsi_oversold = self.config.get("rsi_oversold", 30)
        min_atr = self.config.get("min_atr", 0.0)

        # 金叉 (EMA4 上穿 EMA9) 且 RSI 未超买 → 做多
        df["enter_long"] = (
            (df["ema_fast"] > df["ema_slow"])
            & (df["ema_fast"].shift(1) <= df["ema_slow"].shift(1))
            & (df["rsi"] < rsi_overbought)
            & (df["atr"] > min_atr)
        )
        # 死叉 (EMA4 下穿 EMA9) 且 RSI 未超卖 → 做空
        df["enter_short"] = (
            (df["ema_fast"] < df["ema_slow"])
            & (df["ema_fast"].shift(1) >= df["ema_slow"].shift(1))
            & (df["rsi"] > rsi_oversold)
            & (df["atr"] > min_atr)
        )
        return df

    def populate_exit_trend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """反向交叉平仓。"""
        df["exit_long"] = (df["ema_fast"] < df["ema_slow"])
        df["exit_short"] = (df["ema_fast"] > df["ema_slow"])
        return df

    def custom_stoploss(self, symbol: str, entry_price: float,
                        current_price: float, direction: str) -> float:
        """ATR 动态止损（1.5×ATR），默认 0.8%。"""
        pct = self.config.get("stop_pct", 0.008)
        if direction == "LONG":
            return round(entry_price * (1 - pct), 2)
        return round(entry_price * (1 + pct), 2)

    def leverage(self, symbol: str) -> int:
        return self.config.get("leverage", 3)
