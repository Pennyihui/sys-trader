"""交易日志导出与绩效统计 (2026-08-16 P1-4)。

从 data/trades.db 导出订单/信号 CSV + 打印汇总统计。
胜率等往返指标需要配对入场/离场, 当前订单流无逐笔配对, 列为已知限制
(后续可基于 position.changed 事件流做配对)。

用法:
  python tools/trade_journal.py --db data/trades.db --out-dir data/exports
"""

import argparse
import csv
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.database import TradeDatabase  # noqa: E402

TAKER_FEE = 0.0005


def export(db: TradeDatabase, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    orders = db.get_orders(limit=100000)
    orders_path = os.path.join(out_dir, f"orders_{stamp}.csv")
    with open(orders_path, "w", newline="", encoding="utf-8-sig") as f:
        if orders:
            w = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
            w.writeheader()
            w.writerows(orders)
    print(f"订单导出: {orders_path} ({len(orders)} 行)")

    signals = db.get_signals(limit=100000)
    signals_path = os.path.join(out_dir, f"signals_{stamp}.csv")
    with open(signals_path, "w", newline="", encoding="utf-8-sig") as f:
        if signals:
            w = csv.DictWriter(f, fieldnames=list(signals[0].keys()))
            w.writeheader()
            w.writerows(signals)
    print(f"信号导出: {signals_path} ({len(signals)} 行)")


def summarize(db: TradeDatabase):
    orders = db.get_orders(limit=100000)
    print("\n=== 订单汇总 ===")
    by_status: dict = {}
    total_notional = 0.0
    est_fees = 0.0
    for o in orders:
        by_status[o["status"]] = by_status.get(o["status"], 0) + 1
        qty = float(o.get("filled_qty") or o.get("quantity") or 0)
        price = float(o.get("avg_price") or o.get("price") or 0)
        total_notional += qty * price
        est_fees += qty * price * TAKER_FEE
    print(f"订单状态分布: {by_status}")
    print(f"成交名义价值合计: {total_notional:.2f} USDT")
    print(f"估算手续费 (taker {TAKER_FEE:.2%}): {est_fees:.2f} USDT")
    print("\n注: 胜率/盈亏比需入场-离场配对, 当前未实现 (已知限制)")

    signals = db.get_signals(limit=100000)
    print(f"\n=== 信号汇总 === 共 {len(signals)} 条")
    by_symbol: dict = {}
    for s in signals:
        by_symbol[s["symbol"]] = by_symbol.get(s["symbol"], 0) + 1
    print(f"按 symbol: {by_symbol}")


def main():
    parser = argparse.ArgumentParser(description="交易日志导出与统计")
    parser.add_argument("--db", default=os.environ.get("DB_PATH", "data/trades.db"))
    parser.add_argument("--out-dir", default="data/exports")
    parser.add_argument("--no-export", action="store_true", help="只打印统计不导出 CSV")
    args = parser.parse_args()

    db = TradeDatabase(args.db)
    try:
        summarize(db)
        if not args.no_export:
            export(db, args.out_dir)
    finally:
        db.close()


if __name__ == "__main__":
    main()
