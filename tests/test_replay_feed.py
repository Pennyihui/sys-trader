"""ReplayFeed 测试 — 从 JSON 文件重放 K 线触发 on_kline_closed。"""

import json
import os

import pytest

from tools.replay_feed import ReplayFeed


def _write_klines(tmpdir, symbols=("BTCUSDT",), n=5):
    rows = []
    for i in range(n):
        rows.append({
            "open": 100 + i, "high": 102 + i, "low": 99 + i,
            "close": 101 + i, "volume": 10 + i, "open_time": i * 900,
        })
    for sym in symbols:
        path = os.path.join(tmpdir, f"{sym}_15m.json")
        with open(path, "w") as f:
            json.dump(rows, f)
    return rows


@pytest.mark.unit
def test_replay_triggers_on_kline_closed(tmp_path):
    rows = _write_klines(str(tmp_path))
    closes = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: closes.append((s, tf)))
    feed.start()
    feed.run_once()
    feed.stop()
    assert closes == [("BTCUSDT", "15m")]


@pytest.mark.unit
def test_replay_prices(tmp_path):
    rows = _write_klines(str(tmp_path))
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m")
    feed.start()
    feed.run_once()
    assert feed.get_last_price("BTCUSDT") == rows[-1]["close"]


@pytest.mark.unit
def test_replay_ohlcv_supports_attribute_access(tmp_path):
    """on_kline_closed 收到的 ohlcv 必须支持 k.open/k.high 属性访问。

    runner._on_kline_closed 用 k.open/k.high/k.low/k.close/k.volume
    构造 DataFrame —— 传 dict 会导致整条信号链静默失败 (AttributeError)。
    """
    rows = _write_klines(str(tmp_path))
    captured = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: captured.append(ohlcv))
    feed.start()
    feed.run_once()
    feed.stop()
    assert len(captured) == 1
    klines = captured[0]
    assert len(klines) == len(rows)
    assert klines[-1].close == rows[-1]["close"]
    assert klines[-1].open == rows[-1]["open"]
    assert klines[-1].volume == rows[-1]["volume"]
    assert klines[-1].is_closed is True


@pytest.mark.unit
def test_replay_multiple_symbols(tmp_path):
    rows = _write_klines(str(tmp_path), symbols=("BTCUSDT", "ETHUSDT"))
    closes = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT", "ETHUSDT"],
                      timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: closes.append((s, tf)))
    feed.start()
    feed.run_once()
    feed.stop()
    assert sorted(closes) == [("BTCUSDT", "15m"), ("ETHUSDT", "15m")]
    assert feed.get_last_price("ETHUSDT") == rows[-1]["close"]


@pytest.mark.unit
def test_replay_empty_file_does_not_crash(tmp_path):
    _write_klines(str(tmp_path))
    with open(os.path.join(str(tmp_path), "BTCUSDT_15m.json"), "w") as f:
        f.write("[]")
    closes = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: closes.append((s, tf)))
    feed.start()
    feed.run_once()
    feed.stop()
    assert closes == []
    assert feed.get_last_price("BTCUSDT") is None
