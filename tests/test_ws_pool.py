import pytest
from market_data.ws_pool import StreamSpec, build_stream_list, ConnectionPoolConfig


@pytest.mark.unit
class TestStreamSpec:
    def test_build_stream_list_for_single_symbol(self):
        specs = build_stream_list(["BTCUSDT"])
        streams = [s.stream_name for s in specs]
        assert "btcusdt@kline_4h" in streams
        assert "btcusdt@kline_1d" in streams
        assert "btcusdt@kline_1w" in streams
        assert "btcusdt@markprice" in streams

    def test_build_stream_list_for_multiple_symbols(self):
        specs = build_stream_list(["BTCUSDT", "ETHUSDT"])
        streams = [s.stream_name for s in specs]
        assert "btcusdt@kline_4h" in streams
        assert "ethusdt@kline_4h" in streams
        assert "btcusdt@kline_1d" in streams
        assert "ethusdt@kline_1d" in streams

    def test_build_stream_list_stream_count(self):
        specs = build_stream_list(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        assert len(specs) == 3 * 4

    def test_stream_name_format(self):
        specs = build_stream_list(["BTCUSDT"])
        for s in specs:
            assert "@" in s.stream_name
            assert s.stream_name == s.stream_name.lower()


@pytest.mark.unit
class TestConnectionPoolConfig:
    def test_pool_size_bounded_by_max(self):
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ARBUSDT"]
        config = ConnectionPoolConfig(max_pool_size=3)
        assert config.effective_pool_size(symbols) == 3

    def test_pool_size_bounded_by_symbol_count(self):
        symbols = ["BTCUSDT"]
        config = ConnectionPoolConfig(max_pool_size=5)
        assert config.effective_pool_size(symbols) == 1

    def test_round_robin_distribution(self):
        config = ConnectionPoolConfig(max_pool_size=3)
        specs = build_stream_list(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        bins = config.distribute(specs)
        assert len(bins) == 3
        assert sum(len(b) for b in bins) == len(specs)
