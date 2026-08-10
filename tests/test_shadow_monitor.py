"""ShadowMonitor 测试 — 双实例信号对齐与执行质量统计。

背景（Task 19, 管道 spec 4.2）:
  live/paper 双实例并行运行，共享 EventBus（signal.generated 带 instance 标识）。
  ShadowMonitor 消费两实例的信号与成交事件，验证:
    - 信号对齐率 ≥ 95%（同 symbol+direction，或更精确的 signal_id 匹配）
    - 逐笔滑点（live 成交价 vs paper 成交价，bps）与填充率
  TCA 风格，不做相关性统计（低频样本不足）。
"""

import json
import os

import pytest

from tools.shadow_monitor import ShadowMonitor


@pytest.mark.unit
def test_signal_alignment_ratio():
    """10 笔一致 + 1 笔 paper 方向错位 → 对齐率 0.9"""
    mon = ShadowMonitor()
    for i in range(10):
        mon.record_signal("live", {"symbol": "BTCUSDT", "direction": "LONG", "ts": i})
        mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "LONG", "ts": i})
    mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "SHORT", "ts": 99})  # 错位
    ratio = mon.alignment_ratio()
    assert 0.9 <= ratio < 1.0


@pytest.mark.unit
def test_alignment_ratio_empty_live_returns_one():
    """无 live 信号时不判失败（避免空运行误报）"""
    mon = ShadowMonitor()
    mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "LONG"})
    assert mon.alignment_ratio() == 1.0


@pytest.mark.unit
def test_alignment_ratio_signal_id_preferred():
    """有 signal_id 时按 signal_id 匹配（更精确）；相同 symbol+direction 但不同信号不算对齐"""
    mon = ShadowMonitor()
    mon.record_signal("live", {"signal_id": "sig-1", "symbol": "BTCUSDT", "direction": "LONG"})
    mon.record_signal("paper", {"signal_id": "sig-1", "symbol": "BTCUSDT", "direction": "LONG"})
    mon.record_signal("live", {"signal_id": "sig-2", "symbol": "BTCUSDT", "direction": "SHORT"})
    mon.record_signal("paper", {"signal_id": "sig-9", "symbol": "BTCUSDT", "direction": "SHORT"})  # id 不同
    # 无 signal_id 的记录退回 symbol+direction 匹配
    mon.record_signal("live", {"symbol": "ETHUSDT", "direction": "LONG"})
    mon.record_signal("paper", {"symbol": "ETHUSDT", "direction": "LONG"})
    # sig-1 ✅, sig-2 ✗ (id 不同), ETHUSDT ✅ (fallback) → 2/3
    assert mon.alignment_ratio() == pytest.approx(2 / 3)


@pytest.mark.unit
def test_execution_quality_recorded():
    """滑点 = (live - paper)/paper * 10000 bps"""
    mon = ShadowMonitor()
    mon.record_fill("live", {"symbol": "BTCUSDT", "price": 64001.0})
    mon.record_fill("paper", {"symbol": "BTCUSDT", "price": 64000.0})
    stats = mon.execution_quality()
    assert stats["slippage_bps"] is not None  # (64001-64000)/64000*10000
    assert stats["slippage_bps"] == pytest.approx(0.15625, abs=0.01)
    assert stats["fill_rate"] == 1.0
    assert stats["samples"] == 1


@pytest.mark.unit
def test_execution_quality_no_fills_returns_none():
    """无成交样本 → slippage/fill_rate 为 None，不抛异常"""
    mon = ShadowMonitor()
    stats = mon.execution_quality()
    assert stats == {"slippage_bps": None, "fill_rate": None, "samples": 0}
    mon.record_fill("live", {"symbol": "BTCUSDT", "price": 64000.0})
    stats = mon.execution_quality()
    assert stats["slippage_bps"] is None  # paper 无成交 → 无法对比


@pytest.mark.unit
def test_report_saved(tmp_path):
    """落盘 JSON 报告包含 alignment_ratio"""
    mon = ShadowMonitor()
    mon.record_signal("live", {"symbol": "BTCUSDT", "direction": "LONG"})
    mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "LONG"})
    out = str(tmp_path / "shadow.json")
    mon.save_report(out)
    assert os.path.exists(out)
    assert json.load(open(out))["alignment_ratio"] == 1.0
