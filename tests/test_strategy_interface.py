"""测试策略接口。"""
import pandas as pd
import pytest
from signal_engine.interface import IStrategy, StrategyRegistry
from signal_engine.simple_strategy import SMACrossStrategy
from signal_engine.engine import Signal, SignalEngine


def make_df(n=60, uptrend=True):
    """构造 K 线数据：前 n-1 根横盘，最后一根大幅涨/跌。

    均线交叉策略的 enter_long/enter_short 只在「交叉发生的那一根」为 True；
    若用单调趋势数据，交叉发生在中间某根而非最后一根，最后一根收不到信号。
    因此用「横盘后单根大涨/大跌」保证交叉恰发生在最后一行。
    """
    import numpy as np
    prices = np.full(n, 60000.0)
    prices[-1] = 65000.0 if uptrend else 55000.0
    return pd.DataFrame({
        "open": prices - 10, "high": prices + 100,
        "low": prices - 100, "close": prices,
        "volume": 100.0,
    })


class TestStrategyInterface:
    def test_abstract_cannot_instantiate(self):
        with pytest.raises(TypeError):
            IStrategy()

    def test_sma_cross_registered(self):
        assert "sma_cross" in StrategyRegistry.names()

    def test_sma_cross_analyze_returns_signal(self):
        strategy = StrategyRegistry.get("sma_cross")
        df = make_df()
        signal = strategy.analyze("BTCUSDT", df)
        # 最后一根大涨 → 快线上穿慢线 → LONG 信号
        assert signal is not None
        assert signal.direction == "LONG"

    def test_sma_cross_down_trend_short(self):
        strategy = StrategyRegistry.get("sma_cross")
        df = make_df(uptrend=False)  # 最后一根大跌 → 快线下穿慢线 → SHORT 信号
        signal = strategy.analyze("BTCUSDT", df)
        assert signal is not None
        assert signal.direction == "SHORT"

    def test_leverage_callback(self):
        strategy = StrategyRegistry.get("sma_cross", config={"leverage": 5})
        assert strategy.leverage("BTCUSDT") == 5

    def test_signal_engine_with_strategy(self):
        engine = SignalEngine(strategy=StrategyRegistry.get("sma_cross"))
        df = make_df()
        ohlcv = df.to_dict("records")
        signal = engine.run("BTCUSDT", "4h", ohlcv)
        assert signal is not None
        assert isinstance(signal, Signal)

    def test_signal_engine_without_strategy_returns_none(self):
        engine = SignalEngine()  # 无策略
        assert engine.run("BTCUSDT", "4h", [{"open": 1}]) is None
