"""
稳定性测试数量精度单元测试

背景（2026-08-08 稳定性测试第4轮发现）:
  stability_test.py 对所有标的统一 round(qty, 4)，但各币种 stepSize 不同:
    - BTCUSDT: stepSize=0.0001 (4位小数)  → 0.0015 ✅
    - ETHUSDT: stepSize=0.001  (3位小数)  → 0.0026 ❌ Precision over max
    - SOLUSDT: stepSize=0.01   (2位小数)  → 0.0678 ❌ Precision over max

align_qty_to_step 保证:
  1. 数量是 stepSize 的整数倍（交易所硬性要求）
  2. 不低于 min_qty（名义价值 ≥ 5 USDT 的保底数量，向下取整跌破时向上取整）
  3. 不高于 max_qty（名义价值 ≤ 100 USDT 的风险上限）
"""

import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.order_utils import align_qty_to_step  # noqa: E402


class TestAlignQtyToStep:
    """按交易所 stepSize 对齐数量"""

    # ─── BTCUSDT: stepSize=0.0001 ───

    def test_btc_already_aligned_stays_same(self):
        """0.0015 已是 0.0001 的整数倍 → 保持不变"""
        assert align_qty_to_step(0.0015, 0.0001, 0.000077, 0.00154) == 0.0015

    def test_btc_rounds_down_to_step(self):
        """0.00016 对齐到 0.0001 → 0.0001"""
        assert align_qty_to_step(0.00016, 0.0001, 0.000077, 0.0015) == 0.0001

    def test_btc_respects_min_qty_when_floor_breaks_it(self):
        """min_qty=0.000077, qty=0.00008 → floor 0.0 跌破 min → 向上取整 0.0001"""
        assert align_qty_to_step(0.00008, 0.0001, 0.000077, 0.0015) == 0.0001

    # ─── ETHUSDT: stepSize=0.001（真实失败场景）───

    def test_eth_rounds_up_to_step_when_floor_below_min(self):
        """ETH 真实失败: qty=0.0026, step=0.001, min=0.00261
        floor→0.002 跌破 min → 向上取整 0.003（名义 0.003*1914=5.74 ≥ 5）"""
        assert align_qty_to_step(0.001, 0.001, 0.00261, 0.0522) == 0.003

    def test_eth_rounds_down_when_floor_above_min(self):
        """qty=0.0032 → floor 0.003 ≥ min 0.001 → 0.003"""
        assert align_qty_to_step(0.0032, 0.001, 0.001, 0.01) == 0.003

    # ─── SOLUSDT: stepSize=0.01（真实失败场景）───

    def test_sol_rounds_up_to_step_when_floor_below_min(self):
        """SOL 真实失败: qty=0.0678, step=0.01, min=0.0679
        floor→0.06 跌破 min → 向上取整 0.07（名义 0.07*73.69=5.16 ≥ 5）"""
        assert align_qty_to_step(0.001, 0.01, 0.0679, 1.357) == 0.07

    def test_sol_rounds_down_when_floor_above_min(self):
        """qty=0.86 → floor 0.86 ≥ min 0.01 → 0.86"""
        assert align_qty_to_step(0.86, 0.01, 0.01, 1.35) == 0.86

    # ─── 边界 ───

    def test_clamps_to_max_qty(self):
        """qty 超 max_qty → clamp 到 max_qty 的 step 整数倍（向下）"""
        assert align_qty_to_step(0.013, 0.001, 0.001, 0.011) == 0.011

    def test_clamps_to_min_qty(self):
        """qty 低于 min_qty → 至少 min_qty（向上取整到 step）"""
        assert align_qty_to_step(0.0005, 0.001, 0.003, 0.01) == 0.003

    def test_zero_step_returns_qty_unchanged(self):
        """step_size=0 兜底：直接返回 clamp 后数量，不崩溃"""
        assert align_qty_to_step(0.0032, 0.0, 0.001, 0.01) == 0.0032
