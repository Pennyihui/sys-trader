"""soak_watchdog — soak/实盘期间的进程健康记录：RSS + 错误计数，每小时追加 CSV。

用途：soak 7 天期间并行监控，每小时记录一次 RSS 与错误增量；
soak 结束时用 CSV 判定内存曲线平稳（无持续增长趋势）。

用法: python tools/soak_watchdog.py --out logs/soak_metrics.csv [--log logs/systrader.log] [--interval 3600] [--pid 12345]

说明: rss_mb 依赖 psutil（可选，未安装时回退 0.0）；--pid 指定监控目标进程
（缺省为 watchdog 自身）。
"""

import argparse
import os
import time
from typing import Optional


def rss_mb(pid: Optional[int] = None) -> float:
    """指定进程（缺省当前进程）RSS（MB）。psutil 未安装或探测失败时回退 0.0。"""
    try:
        import psutil
        proc = psutil.Process(pid) if pid is not None else psutil.Process()
        return proc.memory_info().rss / 1024 / 1024
    except Exception:
        return 0.0


def count_errors(log_path: str) -> int:
    """统计日志中 ERROR/WARNING 行数（累计值，由调用方计算增量）。"""
    if not log_path or not os.path.exists(log_path):
        return 0
    with open(log_path, encoding="utf-8", errors="ignore") as f:
        return sum(1 for line in f if "ERROR" in line or "WARNING" in line)


def collect_metrics(log_path: str = None, pid: int = None) -> dict:
    return {"ts": time.time(), "rss_mb": round(rss_mb(pid), 1),
            "errors_last_hour": count_errors(log_path)}


def sample_and_append(out: str, log_path: str, last_errors: int, pid: int = None) -> int:
    """采集一次指标并追加 CSV 行（errors_delta 列 = 错误增量），
    返回最新错误累计值供下次增量计算。"""
    m = collect_metrics(log_path, pid)
    delta = max(0, m["errors_last_hour"] - last_errors)
    with open(out, "a") as f:
        f.write(f"{int(m['ts'])},{m['rss_mb']},{delta}\n")
    return m["errors_last_hour"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="logs/soak_metrics.csv")
    parser.add_argument("--log", default="logs/systrader.log")
    parser.add_argument("--interval", type=int, default=3600)
    parser.add_argument("--pid", type=int, default=None,
                        help="监控指定 PID 的 RSS（缺省为 watchdog 自身进程）")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    header = "ts,rss_mb,errors_delta\n"
    if not os.path.exists(args.out):
        with open(args.out, "w") as f:
            f.write(header)
    last_errors = 0
    while True:
        last_errors = sample_and_append(args.out, args.log, last_errors, args.pid)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
