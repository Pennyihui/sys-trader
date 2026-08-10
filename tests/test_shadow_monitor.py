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


# ─── 质量评审补充: 消耗式匹配 / symbol 感知配对 / 空运行 pass 语义 ───

@pytest.mark.unit
def test_alignment_ratio_duplicate_ids_not_double_counted():
    """同一 paper 信号不可满足多条相同 signal_id 的 live 信号（消耗式匹配）"""
    mon = ShadowMonitor()
    mon.record_signal("live", {"signal_id": "sig-1", "symbol": "BTCUSDT", "direction": "LONG"})
    mon.record_signal("live", {"signal_id": "sig-1", "symbol": "BTCUSDT", "direction": "LONG"})
    mon.record_signal("paper", {"signal_id": "sig-1", "symbol": "BTCUSDT", "direction": "LONG"})
    # 一条 paper 只满足一条 live → 1/2 = 0.5，而非 1.0
    assert mon.alignment_ratio() == pytest.approx(0.5)


@pytest.mark.unit
def test_alignment_ratio_fallback_excludes_consumed():
    """有 signal_id 的 paper 记录只走 id 匹配，不被无 id 的 live 信号 fallback 双计"""
    mon = ShadowMonitor()
    mon.record_signal("live", {"signal_id": "sig-1", "symbol": "BTCUSDT", "direction": "LONG"})
    mon.record_signal("live", {"symbol": "BTCUSDT", "direction": "LONG"})  # 无 id，同方向
    mon.record_signal("paper", {"signal_id": "sig-1", "symbol": "BTCUSDT", "direction": "LONG"})
    # sig-1 消耗掉 paper 的 id 记录；paper 无 id 记录 → fallback 集合为空 → 仅 1/2
    assert mon.alignment_ratio() == pytest.approx(0.5)


@pytest.mark.unit
def test_execution_quality_multi_symbol_no_cross_pairing():
    """多 symbol 交错填充: 按 symbol 分组配对，不产生跨 symbol 假滑点"""
    mon = ShadowMonitor()
    mon.record_fill("live", {"symbol": "BTCUSDT", "price": 64001.0})
    mon.record_fill("live", {"symbol": "ETHUSDT", "price": 3200.0})
    mon.record_fill("paper", {"symbol": "ETHUSDT", "price": 3201.0})
    mon.record_fill("paper", {"symbol": "BTCUSDT", "price": 64000.0})
    stats = mon.execution_quality()
    # BTC: (64001-64000)/64000*10000=0.15625; ETH: (3200-3201)/3201*10000≈-3.1240
    assert stats["samples"] == 2
    assert stats["slippage_bps"] == pytest.approx((0.15625 - 3.1240) / 2, abs=0.01)
    assert stats["fill_rate"] == 1.0


@pytest.mark.unit
def test_execution_quality_signal_id_paired():
    """有 signal_id 时按 id 精确配对（与记录顺序无关），不按序错配"""
    mon = ShadowMonitor()
    mon.record_fill("live", {"signal_id": "f-1", "symbol": "BTCUSDT", "price": 100.0})
    mon.record_fill("live", {"signal_id": "f-2", "symbol": "BTCUSDT", "price": 200.0})
    mon.record_fill("paper", {"signal_id": "f-2", "symbol": "BTCUSDT", "price": 150.0})
    mon.record_fill("paper", {"signal_id": "f-1", "symbol": "BTCUSDT", "price": 110.0})
    stats = mon.execution_quality()
    # id 配对: f-1 (100-110)/110*10000≈-909.09; f-2 (200-150)/150*10000=3333.33 → 均值 1212.12
    # 若按记录序错配则为 (100-150)/150 与 (200-110)/110 → 均值 2424.24，可区分
    assert stats["samples"] == 2
    assert stats["slippage_bps"] == pytest.approx(1212.12, abs=0.5)
    assert stats["fill_rate"] == 1.0


@pytest.mark.unit
def test_report_empty_run_pass_null(tmp_path):
    """空运行（无 live 信号）pass 为 null 不静默通过；报告含 align_threshold"""
    mon = ShadowMonitor()
    mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "LONG"})
    out = str(tmp_path / "shadow_empty.json")
    report = mon.save_report(out)
    assert report["pass"] is None
    assert report["align_threshold"] == 0.95
    assert json.load(open(out))["pass"] is None
