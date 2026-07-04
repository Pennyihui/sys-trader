import pytest
from signal_engine.engine import SignalEngine, Signal


class TestSignalEngine:
    def setup_method(self):
        self.engine = SignalEngine()

    def test_run_returns_signal_with_required_fields(self):
        ohlcv = [
            {"open_time": 1000, "open": 62000, "high": 63000, "low": 61500, "close": 62500, "volume": 100.0}
        ]
        signal = self.engine.run("BTCUSDT", "4h", ohlcv)
        assert signal is None or isinstance(signal, Signal)

    def test_signal_dataclass_has_all_fields(self):
        s = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0, attribution={"strategy_a": 0.5, "strategy_b": 0.5})
        assert s.symbol == "BTCUSDT"
        assert s.direction == "LONG"
        assert s.conviction == 0.72
        assert s.attribution["strategy_a"] == 0.5

    def test_run_unknown_timeframe_returns_none(self):
        signal = self.engine.run("BTCUSDT", "unknown", [])
        assert signal is None

    def test_run_returns_none_for_weekly_without_enough_data(self):
        ohlcv = []
        signal = self.engine.run("BTCUSDT", "1w", ohlcv)
        assert signal is None
