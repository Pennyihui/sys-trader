"""TCA 滑点分析 (2026-08-16 P2-5)。

对已成交入场单计算 成交均价 vs 限价 (到达价) 的滑点 (bps),
按 symbol 汇总。数据源: data/trades.db orders 表 (需 runner 已接线 DB 持久化)。

用法:
  python tools/tca.py --db data/trades.db
"""

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.database import TradeDatabase  # noqa: E402


def analyze(db: TradeDatabase):
    orders = db.get_orders(limit=100000)
    fills = [o for o in orders
             if o["status"] in ("FILLED", "PARTIALLY_FILLED")
             and float(o.get("filled_qty") or 0) > 0
             and float(o.get("avg_price") or 0) > 0
             and float(o.get("price") or 0) > 0]
    if not fills:
        print("无已成交订单记录 (runner 是否已接线 DB 持久化?)")
        return

    per_symbol = defaultdict(list)
    for o in fills:
        limit = float(o["price"])
        avg = float(o["avg_price"])
        qty = float(o["filled_qty"])
        side = o["side"]
        # 滑点: BUY 成交价高于限价为正滑点; SELL 相反
        bps = (avg - limit) / limit * 10_000 * (1 if side == "BUY" else -1)
        per_symbol[o["symbol"]].append((bps, qty, o))

    print(f"=== TCA 滑点分析 (共 {len(fills)} 笔成交) ===")
    for sym, rows in sorted(per_symbol.items()):
        bps_list = [r[0] for r in rows]
        notional = sum(r[1] * r[2]["avg_price"] for r in rows)
        print(
            f"{sym}: {len(rows)} 笔 | 加权名义 {notional:.2f} USDT | "
            f"滑点均值 {sum(bps_list)/len(bps_list):+.2f} bps | "
            f"最大 {max(bps_list):+.2f} bps | 最小 {min(bps_list):+.2f} bps"
        )
    print("\n注: 滑点 = 成交均价 vs 下单限价; 正值=不利滑点。")
    print("    到达价(tick 前)与真实到达价存在 tick 对齐差异, 长周期均值可信。")


def main():
    parser = argparse.ArgumentParser(description="TCA 滑点分析")
    parser.add_argument("--db", default=os.environ.get("DB_PATH", "data/trades.db"))
    args = parser.parse_args()
    db = TradeDatabase(args.db)
    try:
        analyze(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
