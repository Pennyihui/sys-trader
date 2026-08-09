"""ReplayFeed 测试 — 从 JSON 文件逐根重放 K 线触发 on_kline_closed。

重放语义与实盘对齐: 按 open_time 全局排序，每根 K 线闭合触发一次回调，
回调携带最近 BUFFER_WINDOW(100) 根滑动窗口。
"""

import json
import os

import pytest

from tools.replay_feed import ReplayFeed


def _write_klines(tmpdir, symbols=("BTCUSDT",), n=5, time_offset=0):
    rows = []
    for i in range(n):
        rows.append({
            "open": 100 + i, "high": 102 + i, "low": 99 + i,
            "close": 101 + i, "volume": 10 + i, "open_time": i * 900 + time_offset,
        })
    for sym in symbols:
        path = os.path.join(tmpdir, f"{sym}_15m.json")
        with open(path, "w") as f:
            json.dump(rows, f)
    return rows


@pytest.mark.unit
def test_replay_triggers_on_kline_closed(tmp_path):
    """每根 K 线闭合触发一次 (不再是每 symbol 一次全量)。"""
    rows = _write_klines(str(tmp_path))
    closes = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: closes.append((s, tf)))
    feed.start()
    feed.run_once()
    feed.stop()
    assert closes == [("BTCUSDT", "15m")] * len(rows)


@pytest.mark.unit
def test_replay_fires_per_bar_chronological(tmp_path):
    """跨 symbol 按 open_time 全局排序，逐根触发 (2 symbol × 5 根 = 10 次)。"""
    _write_klines(str(tmp_path), symbols=("BTCUSDT",), n=5, time_offset=0)
    _write_klines(str(tmp_path), symbols=("ETHUSDT",), n=5, time_offset=450)
    calls = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT", "ETHUSDT"], timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: calls.append((ohlcv[-1].open_time, s)))
    feed.start()
    feed.run_once()
    feed.stop()
    assert len(calls) == 10
    times = [t for t, _ in calls]
    assert times == sorted(times)  # 调用序列全局时间序
    assert calls[0] == (0, "BTCUSDT")
    assert calls[1] == (450, "ETHUSDT")
    assert calls[2] == (900, "BTCUSDT")
    assert calls[3] == (1350, "ETHUSDT")


@pytest.mark.unit
def test_replay_buffer_window(tmp_path):
    """重放 150 根: 回调携带滑动窗口, 满窗后恒为 100 根 (与实盘 KlineBuffer 一致)。"""
    _write_klines(str(tmp_path), n=150)
    captured = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: captured.append(ohlcv))
    feed.start()
    feed.run_once()
    feed.stop()
    assert len(captured) == 150
    assert len(captured[0]) == 1     # 第 1 根: 窗口只含自身
    assert len(captured[99]) == 100  # 第 100 根起窗口满
    assert len(captured[-1]) == 100  # 最后一次回调窗口 == 100


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
    assert len(captured) == len(rows)
    klines = captured[-1]  # 最后一次回调: 5 根 (< 100 窗口, 全量)
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
    assert closes.count(("BTCUSDT", "15m")) == len(rows)
    assert closes.count(("ETHUSDT", "15m")) == len(rows)
    assert feed.get_last_price("ETHUSDT") == rows[-1]["close"]


@pytest.mark.unit
def test_replay_on_bar_progress(tmp_path):
    """on_bar 每根触发一次: replay_runner 的 RSS 采样依赖该钩子。"""
    _write_klines(str(tmp_path), n=150)
    progress = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m")
    feed.start()
    feed.run_once(on_bar=lambda idx, sym, k: progress.append((idx, sym)))
    feed.stop()
    assert progress[0] == (1, "BTCUSDT")
    assert progress[-1] == (150, "BTCUSDT")
    assert len(progress) == 150


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
