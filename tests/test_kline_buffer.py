import pytest
from market_data.kline_buffer import KlineBuffer, Kline


class TestKlineBuffer:
    def setup_method(self):
        self.buffer = KlineBuffer(max_size=100)

    def test_add_kline_appends(self):
        k = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62500.0, volume=100.5)
        self.buffer.add(k)
        assert self.buffer.count("BTCUSDT", "4h") == 1

    def test_get_klines_returns_correct_range(self):
        for i in range(10):
            k = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000 + i * 14400000, close_time=1000 + (i + 1) * 14400000, open=62000.0 + i * 100, high=63000.0, low=61500.0, close=62500.0, volume=100.0)
            self.buffer.add(k)
        result = self.buffer.get_klines("BTCUSDT", "4h", limit=3)
        assert len(result) == 3
        assert result[0].open_time < result[1].open_time
        assert result[-1].open_time == 1000 + 9 * 14400000

    def test_is_closed_detects_new_candle(self):
        k1 = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62500.0, volume=100.0, is_closed=False)
        self.buffer.add(k1)
        assert self.buffer.is_closed("BTCUSDT", "4h", 1000) is False

        k2 = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62600.0, volume=105.0, is_closed=True)
        self.buffer.add(k2)
        assert self.buffer.is_closed("BTCUSDT", "4h", 1000) is True

    def test_count_zero_for_empty_buffer(self):
        assert self.buffer.count("BTCUSDT", "4h") == 0
        assert self.buffer.count("ETHUSDT", "1d") == 0
