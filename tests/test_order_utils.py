"""
下单数量对齐工具单元测试 — execution/order_utils.align_qty_to_step

从 tools/stability_test.py 迁出的通用数量对齐逻辑（2026-08-09 Task 2），
供 SystemRunner 与 stability_test 共用。

align_qty_to_step 保证:
  1. 数量是 stepSize 的整数倍（交易所硬性要求）
  2. 不低于 min_qty（名义价值保底，向下取整跌破时向上取整）
  3. 不高于 max_qty（风险上限）
"""

import pytest

from execution.order_utils import align_qty_to_step


@pytest.mark.unit
class TestAlignQtyToStep:
    def test_exact_step(self):
        assert align_qty_to_step(0.005, 0.001, 0.001, 10.0) == 0.005

    def test_floor_to_step(self):
        assert align_qty_to_step(0.0037, 0.001, 0.001, 10.0) == 0.003

    def test_floor_below_min_rounds_up(self):
        assert align_qty_to_step(0.0014, 0.001, 0.002, 10.0) == 0.002

    def test_clamp_max(self):
        assert align_qty_to_step(99.0, 1.0, 1.0, 50.0) == 50.0

    def test_no_step_size_passthrough(self):
        assert align_qty_to_step(5.0, 0.0, 1.0, 100.0) == 5.0
