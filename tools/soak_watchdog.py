"""soak_watchdog — soak/实盘期间的进程健康记录：RSS + 错误计数，每小时追加 CSV。

用途：soak 7 天期间并行监控，每小时记录一次 RSS 与错误增量；
soak 结束时用 CSV 判定内存曲线平稳（无持续增长趋势）。

用法: python tools/soak_watchdog.py --out logs/soak_metrics.csv [--log logs/systrader.log] [--interval 3600] [--pid 12345]

说明: rss_mb 依赖 psutil（可选，未安装时回退 0.0）；--pid 指定监控目标进程
（缺省为 watchdog 自身）。
"""

import argparse
import os
import re
import time
from typing import Optional


def detect_runner_pid(log_path: str) -> Optional[int]:
    """解析 runner PID, 优先级: PID 文件 > 日志扫描 (含轮转文件)。

    2026-08-16: 主运行器启动时写 data/runner.pid (RUNNER_PID_PATH),
    日志轮转会把 "System running" 行滚出当前文件导致检测失败,
    以 PID 文件为准最可靠。
    """
    import os as _os
    pid_file = _os.environ.get("RUNNER_PID_PATH", "data/runner.pid")
    try:
        if _os.path.exists(pid_file):
            with open(pid_file, encoding="utf-8") as f:
                pid = int(f.read().strip())
            try:
                import psutil
                if psutil.pid_exists(pid):
                    return pid
            except Exception:
                return pid  # psutil 不可用时信任文件
    except (OSError, ValueError):
        pass
    # 回退: 扫描日志 (当前 + 轮转文件)
    candidates = []
    if log_path and _os.path.exists(log_path):
        candidates.append(log_path)
        rotated = log_path + ".1"
        if _os.path.exists(rotated):
            candidates.append(rotated)
    for path in candidates:
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = re.search(r"System running \(PID=(\d+)\)", line)
                    if m:
                        return int(m.group(1))
        except OSError:
            pass
    return None


def rss_mb(pid: Optional[int] = None) -> float:
    """指定进程（缺省当前进程）RSS（MB）。psutil 未安装或探测失败时回退 0.0。"""
    try:
        import psutil
        proc = psutil.Process(pid) if pid is not None else psutil.Process()
        return proc.memory_info().rss / 1024 / 1024
    except Exception:
        return 0.0


def cpu_pct(pid: Optional[int] = None) -> float:
    """指定进程 CPU 占用百分比 (0-100×核数)。失败回退 0.0。

    2026-08-16 面板二期: 运维看板需要 CPU 曲线 (此前只记录 RSS)。
    """
    try:
        import psutil
        proc = psutil.Process(pid) if pid is not None else psutil.Process()
        return proc.cpu_percent(interval=0.5)
    except Exception:
        return 0.0


def count_errors(log_path: str) -> int:
    """统计日志中 ERROR/WARNING 行数（累计值，由调用方计算增量）。"""
    if not log_path or not os.path.exists(log_path):
        return 0
    with open(log_path, encoding="utf-8", errors="ignore") as f:
        return sum(1 for line in f if "ERROR" in line or "WARNING" in line)


def _baseline_path(out: str) -> str:
    """错误累计值 sidecar 文件路径（重启后用于恢复基线）。"""
    return out + ".last"


def _load_baseline(out: str) -> Optional[int]:
    """读取上次错误累计值; sidecar 不存在或损坏返回 None (首轮播种基线)。"""
    try:
        with open(_baseline_path(out), encoding="utf-8") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _save_baseline(out: str, value: int):
    with open(_baseline_path(out), "w", encoding="utf-8") as f:
        f.write(str(value))


def collect_metrics(log_path: str = None, pid: int = None) -> dict:
    return {"ts": time.time(), "rss_mb": round(rss_mb(pid), 1),
            "cpu_pct": round(cpu_pct(pid), 1),
            "errors_last_hour": count_errors(log_path)}


def sample_and_append(out: str, log_path: str, last_errors: int, pid: int = None) -> int:
    """采集一次指标并追加 CSV 行（errors_delta 列 = 错误增量），
    返回最新错误累计值供下次增量计算。"""
    m = collect_metrics(log_path, pid)
    delta = max(0, m["errors_last_hour"] - last_errors)
    with open(out, "a") as f:
        f.write(f"{int(m['ts'])},{m['rss_mb']},{m['cpu_pct']},{delta}\n")
    return m["errors_last_hour"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="logs/soak_metrics.csv")
    parser.add_argument("--log", default="logs/systrader.log")
    parser.add_argument("--interval", type=int, default=3600)
    parser.add_argument("--pid", type=int, default=None,
                        help="监控指定 PID 的 RSS（缺省自动从日志解析 runner PID）")
    args = parser.parse_args()

    pid = args.pid
    if pid is None:
        pid = detect_runner_pid(args.log)
        if pid is None:
            print("WARN: 未从日志解析到 runner PID (System running 行), "
                  "RSS 将监控 watchdog 自身 (无意义); 可 --pid 显式指定")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    header = "ts,rss_mb,cpu_pct,errors_delta\n"
    if not os.path.exists(args.out):
        with open(args.out, "w") as f:
            f.write(header)
    last_errors = _load_baseline(args.out)
    while True:
        if last_errors is None:
            # 首次运行或 sidecar 缺失: 首轮播种基线, 不计增量,
            # 避免重启后 last_errors=0 把全量错误当增量误报
            last_errors = count_errors(args.log)
            _save_baseline(args.out, last_errors)
            time.sleep(args.interval)
            continue
        # 2026-08-16 修复: 自动解析的 runner PID 必须传给采样函数,
        # 否则 RSS/CPU 监控的是 watchdog 自身 (args.pid 仍为 None)
        last_errors = sample_and_append(args.out, args.log, last_errors, pid)
        _save_baseline(args.out, last_errors)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
