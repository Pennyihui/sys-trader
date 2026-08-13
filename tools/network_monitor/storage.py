"""存储模块 — 历史数据持久化 + 可用性统计。

历史存储: JSONL 文件 (network_history.jsonl)，每条一个探针结果。
统计:    从历史数据计算 uptime% / 断连次数 / 延迟均值。
"""

import json
import logging
import os
import time
from collections import defaultdict
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

HISTORY_PATH = os.path.join(os.path.dirname(__file__), "network_history.jsonl")
MAX_HISTORY_DAYS = 30  # 保留 30 天历史


def append_record(record: Dict):
    """追加一条探针记录到历史文件。"""
    try:
        with open(HISTORY_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error("写入历史失败: %s", e)


def load_history(hours: int = 24) -> List[Dict]:
    """加载最近 N 小时的历史记录。"""
    records = []
    cutoff = time.time() - hours * 3600
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    if rec.get("timestamp", 0) >= cutoff:
                        records.append(rec)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return records


def compute_stats(hours: int = 24) -> Dict:
    """计算最近 N 小时的网络统计。"""
    records = load_history(hours)
    if not records:
        return {
            "hours": hours, "total_probes": 0,
            "uptime_pct": None, "down_events": 0,
            "avg_gateway_ms": None, "avg_dns_ms": None,
            "last_status": "no_data",
        }

    total = len(records)
    ok = sum(1 for r in records if r.get("network_ok"))
    uptime_pct = round(ok / total * 100, 2) if total else None

    # 断连事件: network_ok 从 True -> False 的次数
    down_events = 0
    prev_ok = True
    for r in records:
        cur_ok = r.get("network_ok", False)
        if prev_ok and not cur_ok:
            down_events += 1
        prev_ok = cur_ok

    # 延迟均值（旧记录可能缺字段，用 .get 防 KeyError）
    gw_ms = [r.get("gateway_ms") for r in records if r.get("gateway_ms") is not None]
    dns_ms = [r.get("dns_ms") for r in records if r.get("dns_ms") is not None]

    last = records[-1]
    return {
        "hours": hours,
        "total_probes": total,
        "uptime_pct": uptime_pct,
        "down_events": down_events,
        "avg_gateway_ms": round(sum(gw_ms) / len(gw_ms), 1) if gw_ms else None,
        "avg_dns_ms": round(sum(dns_ms) / len(dns_ms), 1) if dns_ms else None,
        "last_status": "ok" if last.get("network_ok") else "down",
        "last_record": last,
    }


def get_timeline(hours: int = 24) -> List[Dict]:
    """返回时间线数据（用于图表），按分钟聚合。"""
    records = load_history(hours)
    buckets = defaultdict(list)
    for r in records:
        # 旧记录可能缺 timestamp，用 .get 防 KeyError
        ts = r.get("timestamp", 0)
        if not ts:
            continue
        minute = int(ts // 60)
        buckets[minute].append(r)

    timeline = []
    for minute in sorted(buckets.keys()):
        group = buckets[minute]
        ok_count = sum(1 for r in group if r.get("network_ok"))
        gw_ms = [r["gateway_ms"] for r in group if r.get("gateway_ms") is not None]
        dns_ms = [r["dns_ms"] for r in group if r.get("dns_ms") is not None]
        timeline.append({
            "ts": minute * 60,
            "ok": ok_count == len(group),
            "gateway_ms": round(sum(gw_ms) / len(gw_ms), 1) if gw_ms else None,
            "dns_ms": round(sum(dns_ms) / len(dns_ms), 1) if dns_ms else None,
        })
    return timeline