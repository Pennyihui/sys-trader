"""下单数量对齐工具 — Binance stepSize 精度处理。"""

import math


def align_qty_to_step(qty: float, step_size: float, min_qty: float, max_qty: float) -> float:
    """将数量对齐到交易所 stepSize 的整数倍，且不低于 min_qty、不高于 max_qty。

    Binance 硬性要求数量必须是 stepSize 的整数倍，否则拒绝下单
    ("Precision is over the maximum defined for this asset")。
    各币种 stepSize 不同（BTC=0.0001, ETH=0.001, SOL=0.01），
    统一 round(qty, 4) 会导致 ETH/SOL 精度超限失败。

    对齐策略: clamp 到 [min_qty, max_qty] 后向下取整；
    若向下取整跌破 min_qty（名义价值保底），则向上取整到下一个 step。
    """
    if not step_size or step_size <= 0:
        return min(max(qty, min_qty), max_qty)
    q = min(max(qty, min_qty), max_qty)
    steps = round(q / step_size, 8)  # 消浮点误差: 0.0015/0.0001 → 15.0
    floored = math.floor(steps) * step_size
    if floored >= min_qty:
        return round(floored, 8)
    return round(math.ceil(steps) * step_size, 8)
