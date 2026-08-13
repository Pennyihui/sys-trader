"""健康检查器 - 并发测速所有节点，标记可用/不可用。

两阶段:
  阶段1: 经 mihomo 测通用连通性（gstatic 204 粗筛，全量节点）→ healthy / latency_ms
  阶段2: 传输探测（通过 mihomo 按节点测速，两类分开）:
         - binance_ok（交易关键）每轮全量探测，不受 CAP 限制 → 能到 fapi.binance.com
         - transfer_ok（YouTube，浏览器需求）环形窗口轮换 → 真能传数据的节点

实测教训（2026-08-07）:
  - 直接对节点发 HTTP CONNECT 是错的——vless/trojan/hysteria2 不讲 HTTP 协议，
    只有 mihomo 会翻译。所以探测必须通过 mihomo 的
    GET /proxies/{name}/delay?url=... API（测的是真实路径）。
  - 目标用 YouTube：用户真实需求是能看 YouTube。
"""

import logging
import json
import os
import time
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict

logger = logging.getLogger(__name__)

MAX_WORKERS = 50

# 阶段2 传输探测参数——通过 mihomo 按节点测速两个目标:
#   1. YouTube favicon（浏览器需求，transfer_ok）
#   2. fapi.binance.com（交易需求，binance_ok——能到实盘接口的节点必能到模拟盘）
# 用 favicon/轻接口而不是大文件：免费节点对新建连接限速严重，大文件会误杀
# "能到但慢"的节点。带宽由 mihomo 的 LB 健康检查（AUTO_URL）持续管理。
TRANSFER_URL = "https://www.youtube.com/favicon.ico"
BINANCE_URL = "https://fapi.binance.com/fapi/v1/time"
TRANSFER_TIMEOUT_MS = 8000     # 8s 内完成才算真健康
# YouTube 传输探测每轮最多探测数（环形窗口轮换，多轮覆盖全部健康节点）。
# 调大到 ≥ 健康节点总数后实际每轮全覆盖；binance 探测不受此上限约束（见 health_check）。
TRANSFER_PROBE_CAP = 3000

# 阶段1 连通性测速参数——经 mihomo 的 /proxies/{name}/delay API 测真实路径:
#   目标选 gstatic 204 探针（轻量、稳定，config 之前用的就是这个）做粗筛：
#   阶段1 只回答"节点通不通"，阶段2 才精筛"能否到币安/YouTube"，两阶段目标不冲突。
#   绝不能用直连 socket：免费机场节点在境外，国内网络直连必超时（实测前 500 节点
#   并发仅 164 个能直连），会把全量节点误杀成 unhealthy、healthy=0、配置退化。
STAGE1_URL = "https://www.gstatic.com/generate_204"
STAGE1_TIMEOUT_MS = 5000       # 5s 内完成才算通

_probe_round = 0


def _mihomo_delay(prefixed_name: str, target: str, timeout_ms: int) -> tuple:
    """经 mihomo 按节点测速（GET /proxies/{name}/delay?url=target）。

    返回 (ok, latency_ms)：delay 为限时内完成的整数/浮点毫秒数才算健康。
    import 放 try 内：跑在线程池里，核心未启动或 import 失败不能抛到健康循环外，
    一律按 (False, None) 处理。
    """
    q_name = urllib.parse.quote(prefixed_name, safe="")
    q_url = urllib.parse.quote(target, safe="")
    try:
        # import 放 try 内：探测跑在线程池里，import 失败不能抛到健康循环外中断探测
        from core_manager import CONTROLLER
        from config_generator import CONTROLLER_SECRET
        api = f"{CONTROLLER}/proxies/{q_name}/delay?url={q_url}&timeout={timeout_ms}"
        req = urllib.request.Request(
            api, headers={"Authorization": f"Bearer {CONTROLLER_SECRET}"}
        )
        with urllib.request.urlopen(req, timeout=timeout_ms / 1000 + 2) as resp:
            delay = json.loads(resp.read()).get("delay")
            if isinstance(delay, (int, float)) and 0 < delay < timeout_ms:
                return True, delay
    except Exception:
        pass
    return False, None


def _check_one(entry: dict, timeout: float) -> tuple:
    """阶段1: 经 mihomo 测通用连通性，返回 (name, is_healthy, latency_ms)。

    必须走 mihomo delay API 而非直连 socket——vless/trojan/hysteria2 不讲 HTTP，
    只有 mihomo 会翻译，直连境外节点在国内网络必超时。节点名用 _prefixed_name(entry)
    （带 source 前缀，与 mihomo 配置里的名字一致）；不在配置里的节点 mihomo 回 404，
    delay 为 null，按不健康处理。
    timeout 参数保留以维持签名不变，实际超时由 STAGE1_TIMEOUT_MS 控制。
    """
    server = entry.get("server", "")
    port = entry.get("port", 0)
    name = entry.get("name", "?")

    if not server or not port:
        return name, False, None

    ok, latency_ms = _mihomo_delay(_prefixed_name(entry), STAGE1_URL, STAGE1_TIMEOUT_MS)
    if ok:
        return name, True, latency_ms
    return name, False, None


def _prefixed_name(entry: dict) -> str:
    """节点在 mihomo 配置里的名字（与 config_generator.build_clash_proxies 一致）。"""
    return f"{entry.get('source', 'other')}-{entry['name']}"


def _load_config_proxy_names() -> set:
    """当前 mihomo.yaml 里存在的节点名集合（探测只测这些，避免 404 误标）。"""
    import yaml
    try:
        with open(os.path.join(os.path.dirname(__file__), "mihomo.yaml"), encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return {p["name"] for p in cfg.get("proxies", [])}
    except Exception:
        return set()


def _probe_url(prefixed_name: str, target: str) -> bool:
    """阶段2: 通过 mihomo 按节点测速（GET /proxies/{name}/delay?url=target）。

    mihomo 负责协议翻译（直接发 CONNECT 是错的——vless/trojan 不讲 HTTP）；
    delay 为限时内完成请求才算真健康。
    """
    ok, _ = _mihomo_delay(prefixed_name, target, TRANSFER_TIMEOUT_MS)
    return ok


def health_check(pool: Dict, timeout: float = 3.0) -> Dict:
    """并发测速所有节点，记录延迟，标记可用/不可用 + 传输探测。

    可用 → healthy=true, fail_count=0, latency_ms=连接耗时
    不可用 → healthy=false, fail_count+=1
    传输通过 → transfer_ok=true（auto 组只收这类）

    Args:
        pool: 本地池子数据
        timeout: 阶段1每个节点的连接超时

    Returns:
        更新后的池子
    """
    now = datetime.now(timezone.utc).isoformat()
    proxies = pool.get("proxies", [])
    name_map = {p["name"]: p for p in proxies}

    healthy_count = 0
    unhealthy_count = 0
    latencies = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_check_one, entry, timeout): entry["name"]
            for entry in proxies
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                _, is_healthy, latency_ms = future.result()
            except Exception:
                is_healthy, latency_ms = False, None

            entry = name_map.get(name)
            if not entry:
                continue

            entry["last_checked"] = now
            if is_healthy:
                entry["healthy"] = True
                entry["fail_count"] = 0
                entry["latency_ms"] = round(latency_ms, 1) if latency_ms else None
                if entry.get("latency_ms"):
                    latencies.append(entry["latency_ms"])
                healthy_count += 1
            else:
                entry["healthy"] = False
                entry["fail_count"] = entry.get("fail_count", 0) + 1
                entry["latency_ms"] = None
                unhealthy_count += 1

    # 阶段2: 传输探测（经 mihomo 按节点测速，分两类，重要性不同）:
    #   1. binance_ok（交易关键）→ 每轮全量探测，不受 CAP 限制，一轮覆盖全部健康节点
    #   2. transfer_ok（YouTube，浏览器需求）→ 环形窗口轮换（TRANSFER_PROBE_CAP），
    #      多轮覆盖，控制探测对 mihomo 的压力
    # 只测当前配置里存在的节点——不在配置里的节点 mihomo 会回 404，误标 False
    global _probe_round
    _probe_round += 1
    healthy_entries = [e for e in proxies if e.get("healthy")]
    config_names = _load_config_proxy_names()
    n = len(healthy_entries)
    if n > 0:
        bn_batch = [e for e in healthy_entries if _prefixed_name(e) in config_names]
        start = (_probe_round * TRANSFER_PROBE_CAP) % n
        yt_batch = [
            e for e in (healthy_entries + healthy_entries)[start:start + TRANSFER_PROBE_CAP]
            if _prefixed_name(e) in config_names
        ]
        with ThreadPoolExecutor(max_workers=40) as executor:
            futures = {}
            for e in yt_batch:
                futures[executor.submit(_probe_url, _prefixed_name(e), TRANSFER_URL)] = (e, "transfer_ok")
            for e in bn_batch:
                futures[executor.submit(_probe_url, _prefixed_name(e), BINANCE_URL)] = (e, "binance_ok")
            for f in as_completed(futures):
                entry, key = futures[f]
                entry[key] = f.result()
        yt_ok = sum(1 for e in healthy_entries if e.get("transfer_ok"))
        bn_ok = sum(1 for e in healthy_entries if e.get("binance_ok"))
        logger.info(
            "传输探测(经mihomo): YouTube %d 真健康, binance %d 真健康 "
            "(共 %d 健康节点, binance 探测 %d, YouTube 探测 %d)",
            yt_ok, bn_ok, n, len(bn_batch), len(yt_batch),
        )

    pool["last_updated"] = now
    elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(now)).total_seconds()
    if latencies:
        latencies.sort()
        logger.info(
            "健康检查完成: %d 可用, %d 不可用 (延迟中位数 %.0fms, p90 %.0fms)",
            healthy_count, unhealthy_count,
            latencies[len(latencies)//2], latencies[int(len(latencies)*0.9)],
        )
    else:
        logger.info(
            "健康检查完成: %d 可用, %d 不可用",
            healthy_count, unhealthy_count,
        )

    return pool
