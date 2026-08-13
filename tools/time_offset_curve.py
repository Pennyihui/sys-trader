#!/usr/bin/env python
"""时间偏移曲线 — 读取 logs/time_offset.jsonl，输出终端曲线 + 可选 CSV。

数据源: OrderGateway._sync_server_time 每次成功校准追加一条
         {"ts": epoch_ms, "offset_ms": int}（正=本机比币安快，负=慢）。

用法:
  python tools/time_offset_curve.py              # 终端 ASCII 曲线（最近 24h）
  python tools/time_offset_curve.py --hours 72  # 最近 72 小时
  python tools/time_offset_curve.py --csv out.csv   # 导出 CSV（Excel/绘图用）
  python tools/time_offset_curve.py --stats     # 只看统计摘要

注意: 该偏移是交易系统真正用于签名的数值（server - 本机），
      绝对值受代理上下行不对称延迟影响，趋势/突变才是分析重点。
"""

import argparse
import json
import os
import time

DEFAULT_PATH = os.path.join("logs", "time_offset.jsonl")


def load_records(path: str, hours: int):
    cutoff = (time.time() - hours * 3600) * 1000
    records = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("ts", 0) >= cutoff:
                    records.append(rec)
    except FileNotFoundError:
        pass
    return records


def compute_stats(records):
    if not records:
        return None
    offsets = [r["offset_ms"] for r in records if "offset_ms" in r]
    if not offsets:
        return None
    n = len(offsets)
    mean = sum(offsets) / n
    sorted_offs = sorted(offsets)
    return {
        "count": n,
        "min": sorted_offs[0],
        "max": sorted_offs[-1],
        "mean": mean,
        "p50": sorted_offs[n // 2],
        "p95": sorted_offs[int(n * 0.95)],
        "latest": offsets[-1],
    }


def print_stats(records, hours):
    s = compute_stats(records)
    if s is None:
        print(f"最近 {hours}h 无数据（文件不存在或为空）")
        return
    print(f"=== 最近 {hours}h 时间偏移统计（{s['count']} 次采样）===")
    print(f"  最新: {s['latest']:+.0f} ms")
    print(f"  均值: {s['mean']:+.0f} ms")
    print(f"  中位: {s['p50']:+.0f} ms")
    print(f"  p95 : {s['p95']:+.0f} ms")
    print(f"  范围: [{s['min']:+.0f}, {s['max']:+.0f}] ms")
    print()


def print_curve(records, hours, width=80, height=20):
    if not records:
        print_stats(records, hours)
        return
    offsets = [r["offset_ms"] for r in records]
    lo, hi = min(offsets), max(offsets)
    if hi == lo:
        hi = lo + 1
    pad = max(len(f"{lo:+.0f}"), len(f"{hi:+.0f}"))

    # 按时间分桶（均匀采样到 width 个点）
    t0 = records[0]["ts"]
    t1 = records[-1]["ts"]
    if t1 == t0:
        t1 = t0 + 1
    buckets = [[] for _ in range(width)]
    for r in records:
        idx = int((r["ts"] - t0) / (t1 - t0) * width)
        idx = max(0, min(width - 1, idx))
        buckets[idx].append(r["offset_ms"])

    ys = []
    for b in buckets:
        ys.append(sum(b) / len(b) if b else None)

    print(f"=== 时间偏移曲线（最近 {hours}h，{len(records)} 次采样）===")
    for row in range(height - 1, -1, -1):
        frac = row / (height - 1)
        val = lo + (hi - lo) * frac
        line = f"{val:>{pad}.0f} |"
        for y in ys:
            if y is None:
                line += " "
                continue
            norm = (y - lo) / (hi - lo)
            target = round(norm * (height - 1))
            line += "#" if target == row else ("." if abs(target - row) <= 1 else " ")
        print(line)
    # 时间轴
    line = " " * (pad + 2) + "├" + "─" * width
    print(line)
    t0_s = time.strftime("%m-%d %H:%M", time.localtime(t0 / 1000))
    t1_s = time.strftime("%m-%d %H:%M", time.localtime(t1 / 1000))
    print(f"{'':>{pad + 1}} {t0_s}{'':>{width - len(t0_s) - len(t1_s) - 2}}{t1_s}")
    print()


def export_csv(records, path):
    import csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_iso", "epoch_ms", "offset_ms"])
        for r in records:
            w.writerow([
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"] / 1000)),
                r["ts"],
                r.get("offset_ms", ""),
            ])
    print(f"已导出 {len(records)} 条到 {path}")


def main():
    parser = argparse.ArgumentParser(description="时间偏移曲线（数据源 logs/time_offset.jsonl）")
    parser.add_argument("--hours", type=int, default=24, help="时间窗口（小时，默认 24）")
    parser.add_argument("--path", default=DEFAULT_PATH, help="JSONL 文件路径")
    parser.add_argument("--csv", default="", help="导出 CSV 到指定文件")
    parser.add_argument("--stats", action="store_true", help="只看统计摘要")
    args = parser.parse_args()

    records = load_records(args.path, args.hours)
    if args.csv:
        export_csv(records, args.csv)
        return
    print_stats(records, args.hours)
    if not args.stats:
        print_curve(records, args.hours)


if __name__ == "__main__":
    main()
