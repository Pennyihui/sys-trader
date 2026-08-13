"""探针模块 — 持续检测网络状态。

每个探针返回 (ok: bool, latency_ms: float|None)。

探针类型:
  - gateway_ping : ping 默认网关 (本地链路)
  - dns_ping     : ping 223.5.5.5 (本地到互联网)
  - clash_port   : TCP 连接 127.0.0.1:7897 (Clash 代理)
  - pool_port    : TCP 连接 127.0.0.1:8765 (代理池服务)
"""

import logging
import re
import socket
import subprocess
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def _ping_latency(host: str, timeout_ms: int = 3000) -> Optional[float]:
    """ping 一个地址，返回延迟毫秒。失败返回 None。"""
    try:
        r = subprocess.run(
            ["ping", "-n", "1", "-w", str(timeout_ms), host],
            capture_output=True, text=True, timeout=timeout_ms // 1000 + 2,
            creationflags=subprocess.CREATE_NO_WINDOW,  # 不弹控制台窗口
        )
        if r.returncode != 0:
            return None
        # 匹配 "时间=XXms" 或 "time<1ms" 或 "time=XX ms"
        m = re.search(r"(?:时间|time)[=<\s]*([\d.]+)\s*ms", r.stdout)
        if m:
            return float(m.group(1))
        return 0.1  # ping 成功但没解析到时间
    except Exception:
        return None


def _port_open(host: str, port: int, timeout_s: float = 1.0) -> Optional[float]:
    """TCP 连接测试，返回连接耗时毫秒。失败返回 None。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout_s)
        start = time.time()
        s.connect((host, port))
        elapsed = (time.time() - start) * 1000
        s.close()
        return elapsed
    except Exception:
        return None


def get_default_gateway() -> Optional[str]:
    """从路由表获取默认网关。"""
    try:
        r = subprocess.run(
            ["route", "print", "0.0.0.0"],
            capture_output=True, text=True, timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW,  # 不弹控制台窗口
        )
        m = re.search(r"0\.0\.0\.0\s+0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)", r.stdout)
        return m.group(1) if m else None
    except Exception:
        return None


def run_all_probes() -> Dict:
    """运行全部探针，返回结果字典。"""
    gateway = get_default_gateway()

    gateway_latency = _ping_latency(gateway) if gateway else None
    dns_latency = _ping_latency("223.5.5.5")
    clash_latency = _port_open("127.0.0.1", 7897)
    pool_latency = _port_open("127.0.0.1", 8765)

    return {
        "timestamp": time.time(),
        "gateway": gateway or "unknown",
        "gateway_ok": gateway_latency is not None,
        "gateway_ms": gateway_latency,
        "dns_ok": dns_latency is not None,
        "dns_ms": dns_latency,
        "clash_ok": clash_latency is not None,
        "clash_ms": clash_latency,
        "pool_ok": pool_latency is not None,
        "pool_ms": pool_latency,
        # 网络整体状态: 网关和DNS都通 = 本地网络正常
        "network_ok": (gateway_latency is not None and dns_latency is not None),
    }