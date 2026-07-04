import pytest
from unittest.mock import patch, MagicMock
from market_data.feed import MarketDataFeed
from market_data.kline_buffer import Kline


class TestMarketDataFeed:
    def setup_method(self):
        self.feed = MarketDataFeed(symbols=["BTCUSDT"], testnet=True)

    def test_on_kline_message_parses_and_stores(self):
        msg = {
            "e": "kline", "E": 1700000000000, "s": "BTCUSDT",
            "k": {"t": 1700000000000, "T": 1700014400000, "o": "62000.0", "h": "63000.0", "l": "61500.0", "c": "62500.0", "v": "100.5", "x": True}
        }
        self.feed._on_kline_message(msg)
        kline_series = self.feed.buffer.get_klines("BTCUSDT", "4h")
        assert len(kline_series) >= 0

    def test_detect_timeframe_from_interval(self):
        assert self.feed._timeframe_from_interval("4h") == "4h"
        assert self.feed._timeframe_from_interval("1d") == "1d"
        assert self.feed._timeframe_from_interval("1w") == "1w"

    def test_mark_price_parsed_correctly(self):
        self.feed._on_mark_price_message({"s": "BTCUSDT", "p": "62450.0", "E": 1700000000000})
        assert self.feed._mark_prices.get("BTCUSDT") == 62450.0

    def test_stream_to_timeframe_mapping(self):
        mapping = self.feed._stream_timeframe_map(["BTCUSDT"])
        assert "btcusdt@kline_4h" in mapping
        assert mapping["btcusdt@kline_4h"] == "4h"
        assert mapping["btcusdt@kline_1d"] == "1d"
        assert mapping["btcusdt@kline_1w"] == "1w"

    def test_singleton_no_duplicate_klines_on_repeated_open_time(self):
        self.feed.buffer.add(Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62500.0, volume=100.0, is_closed=False))
        self.feed.buffer.add(Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63100.0, low=61500.0, close=62600.0, volume=105.0, is_closed=True))
        assert self.feed.buffer.count("BTCUSDT", "4h") == 1
        assert self.feed.buffer.is_closed("BTCUSDT", "4h", 1000) is True
