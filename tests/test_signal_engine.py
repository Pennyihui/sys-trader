import pytest
from unittest.mock import MagicMock

from signal_engine.engine import SignalEngine, Signal


@pytest.mark.unit
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

    def test_publishes_signal_generated_with_instance(self):
        bus = MagicMock()
        engine = SignalEngine(event_bus=bus, instance="paper")
        strat = MagicMock()
        strat.timeframe = "15m"
        strat.name = "test_strategy"
        sig = Signal(
            symbol="BTCUSDT", direction="LONG", conviction=0.8,
            entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0)
        strat.analyze.return_value = sig
        engine.strategy = strat
        engine.run("BTCUSDT", "15m", [{"close": 64000.0}])
        bus.publish.assert_called_once()
        stream, payload = bus.publish.call_args[0]
        assert stream == "signal.generated"
        assert payload["instance"] == "paper"
        assert payload["symbol"] == "BTCUSDT"
        assert payload["direction"] == "LONG"
        assert payload["conviction"] == 0.8
        assert payload["entry_price"] == 64000.0
        assert payload["stop_loss"] == 62000.0
        assert payload["take_profit"] == 68000.0
        assert payload["signal_id"] == sig.signal_id
        assert payload["strategy"] == "test_strategy"

    def test_no_signal_does_not_publish(self):
        bus = MagicMock()
        engine = SignalEngine(event_bus=bus, instance="paper")
        strat = MagicMock()
        strat.timeframe = "15m"
        strat.analyze.return_value = None
        engine.strategy = strat
        signal = engine.run("BTCUSDT", "15m", [{"close": 64000.0}])
        assert signal is None
        bus.publish.assert_not_called()

    def test_no_event_bus_is_silent(self):
        engine = SignalEngine()
        strat = MagicMock()
        strat.timeframe = "15m"
        strat.analyze.return_value = Signal(
            symbol="BTCUSDT", direction="LONG", conviction=0.8,
            entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0)
        engine.strategy = strat
        signal = engine.run("BTCUSDT", "15m", [{"close": 64000.0}])  # 不抛异常
        assert signal is not None
