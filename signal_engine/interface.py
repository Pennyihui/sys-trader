"""策略接口 — 可插拔策略基类（参考 Freqtrade IStrategy）。

策略实现者继承 IStrategy，实现钩子方法：
  - analyze()          主入口，返回信号或 None
  - populate_indicators()  计算指标（向量化）
  - populate_entry_trend() 入场信号
  - populate_exit_trend()  出场信号
  - custom_stoploss()   动态止损（可选）
  - leverage()          杠杆（可选）
"""

import abc
from typing import Any, Dict, List, Optional

import pandas as pd

from signal_engine.engine import Signal


class IStrategy(abc.ABC):
    """策略抽象基类。"""

    # 策略元信息
    name: str = "base"
    timeframe: str = "4h"
    max_open_positions: int = 3

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}

    # ─── 主入口 ───

    def analyze(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        """分析 K 线数据，返回交易信号或 None。

        默认流程: indicators → entry_trend → 构造 Signal
        子类可完全覆写。
        """
        df = self.populate_indicators(symbol, df)
        df = self.populate_entry_trend(symbol, df)
        entry = df.iloc[-1] if not df.empty else None
        if entry is None:
            return None
        direction = self._direction_from_entry(entry)
        if direction is None:
            return None
        price = float(entry.get("close", 0))
        return Signal(
            symbol=symbol, direction=direction,
            conviction=self._conviction(entry),
            entry_price=self._entry_price(entry, price),
            stop_loss=self._stop_loss(entry, price, direction),
            take_profit=self._take_profit(entry, price, direction),
        )

    # ─── 向量化钩子 ───

    @abc.abstractmethod
    def populate_indicators(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标，返回添加了指标列的 DataFrame。"""
        raise NotImplementedError

    @abc.abstractmethod
    def populate_entry_trend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """生成入场信号列 enter_long/enter_short（True/False）。"""
        raise NotImplementedError

    def populate_exit_trend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """生成出场信号列 exit_long/exit_short。默认无出场信号。"""
        return df

    # ─── 逐笔回调（可选覆写） ───

    def custom_stoploss(self, symbol: str, entry_price: float, current_price: float,
                        direction: str) -> float:
        """动态止损价。默认入场价 ± 2%。"""
        pct = self.config.get("default_stoploss_pct", 0.02)
        if direction == "LONG":
            return round(entry_price * (1 - pct), 2)
        return round(entry_price * (1 + pct), 2)

    def leverage(self, symbol: str) -> int:
        """杠杆倍数。默认 3x。"""
        return self.config.get("leverage", 3)

    # ─── 内部辅助 ───

    def _direction_from_entry(self, entry) -> Optional[str]:
        if bool(entry.get("enter_long", False)):
            return "LONG"
        if bool(entry.get("enter_short", False)):
            return "SHORT"
        return None

    def _conviction(self, entry) -> float:
        conv = entry.get("conviction", 0.5)
        try:
            return max(0.0, min(1.0, float(conv)))
        except (TypeError, ValueError):
            return 0.5

    def _entry_price(self, entry, default: float) -> float:
        return float(entry.get("entry_price", default))

    def _stop_loss(self, entry, price: float, direction: str) -> float:
        if "stop_loss" in entry and entry["stop_loss"] is not None:
            return float(entry["stop_loss"])
        return self.custom_stoploss(entry.get("symbol", ""), price, price, direction)

    def _take_profit(self, entry, price: float, direction: str) -> float:
        if "take_profit" in entry and entry["take_profit"] is not None:
            return float(entry["take_profit"])
        pct = self.config.get("default_take_profit_pct", 0.04)
        if direction == "LONG":
            return round(price * (1 + pct), 2)
        return round(price * (1 - pct), 2)


class StrategyRegistry:
    """策略注册表 — 按名称查找策略。"""

    _strategies: Dict[str, type] = {}

    @classmethod
    def register(cls, strategy_cls: type):
        cls._strategies[strategy_cls.name] = strategy_cls
        return strategy_cls

    @classmethod
    def get(cls, name: str, config: Optional[Dict] = None) -> IStrategy:
        if name not in cls._strategies:
            raise KeyError(f"Strategy '{name}' not registered")
        return cls._strategies[name](config=config)

    @classmethod
    def names(cls) -> List[str]:
        return list(cls._strategies.keys())
