"""启动前预检脚本 — 一键检查运行依赖是否就绪, 避免"启动失败重试"的碰运气流程。

检查项:
  1. Redis    : ping localhost:6379 (Memurai), 输出 PING 延迟
  2. 代理     : GET https://testnet.binancefuture.com/fapi/v1/time 走 127.0.0.1:7897,
                延迟 >5000ms 判 FAIL (超出签名窗口)
  3. API keys : config/.env 存在 + BINANCE_API_KEY/SECRET 非空
  4. 服务端口 : 8000 (dashboard 后端) / 5173 (前端), --skip-services 跳过
  5. 主系统心跳: Redis systrader:heartbeat 流最后一条 age < 120s
  6. Clash    : 端口 7897 监听 (未监听时尝试定位进程辅助诊断)

用法:
    python tools/preflight_check.py                     # 全部检查
    python tools/preflight_check.py --skip-services     # 跳过服务端口检查
    python tools/preflight_check.py --proxy-port 7897 --redis-url redis://localhost:6379

退出码: 全部 PASS → 0; 任一 FAIL → 1
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import redis
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.config_loader import load_env

PROXY_TEST_URL = "https://testnet.binancefuture.com/fapi/v1/time"
HEARTBEAT_STREAM = "systrader:heartbeat"
DEFAULT_REDIS_URL = "redis://localhost:6379"
DEFAULT_PROXY_PORT = 7897
DEFAULT_SERVICE_PORTS = (8000, 5173)
HEARTBEAT_MAX_AGE_S = 120
PROXY_MAX_LATENCY_MS = 5000


# ─── 基础工具 ───

def _redis_client(redis_url: str) -> redis.Redis:
    """创建带超时的 Redis 客户端 (3s 连接/IO 超时, 避免卡死预检)。"""
    return redis.Redis.from_url(
        redis_url, decode_responses=True,
        socket_connect_timeout=3, socket_timeout=3,
    )


def _port_listening(port: int, host: str = "127.0.0.1") -> bool:
    """端口是否在监听 (connect_ex 返回 0)。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    try:
        return s.connect_ex((host, port)) == 0
    finally:
        s.close()


def _find_clash_process() -> str:
    """尽力探测 Clash 进程名, 失败时返回空串 (仅用于辅助诊断)。"""
    try:
        if sys.platform.startswith("win"):
            out = subprocess.run(["tasklist"], capture_output=True, text=True, timeout=5).stdout
            return ", ".join(ln.split()[0] for ln in out.splitlines() if "clash" in ln.lower()) or ""
        out = subprocess.run(["ps", "-A", "-o", "comm="], capture_output=True, text=True, timeout=5).stdout
        return ", ".join(sorted({ln.strip() for ln in out.splitlines() if "clash" in ln.lower()})) or ""
    except Exception:
        return ""


# ─── 检查项 (每项返回 (bool, str)) ───

def check_redis(redis_url: str = DEFAULT_REDIS_URL) -> Tuple[bool, str]:
    """Redis ping 可达性 + 延迟。"""
    try:
        client = _redis_client(redis_url)
        t0 = time.monotonic()
        result = client.ping()
        ms = (time.monotonic() - t0) * 1000
        if result:
            return True, f"PONG ({ms:.0f}ms)"
        return False, "ping 返回 False"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:80]}"


def check_proxy(proxy_port: int = DEFAULT_PROXY_PORT, max_latency_ms: int = PROXY_MAX_LATENCY_MS) -> Tuple[bool, str]:
    """经本地代理访问 Binance testnet, 延迟超过签名窗口 (默认 5000ms) 判 FAIL。"""
    proxy = f"http://127.0.0.1:{proxy_port}"
    proxies = {"http": proxy, "https": proxy}
    try:
        t0 = time.monotonic()
        r = requests.get(PROXY_TEST_URL, proxies=proxies, timeout=12)
        ms = (time.monotonic() - t0) * 1000
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        if ms > max_latency_ms:
            return False, f"延迟 {ms:.0f}ms 超窗 (>{max_latency_ms}ms)"
        return True, f"延迟 {ms:.0f}ms"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:80]}"


def check_api_keys(env_path: str = "config/.env") -> Tuple[bool, str]:
    """.env 存在且 BINANCE_API_KEY / BINANCE_API_SECRET 非空。"""
    if not os.path.exists(env_path):
        return False, f".env 不存在: {env_path}"
    load_env(env_path)
    key = os.environ.get("BINANCE_API_KEY", "")
    secret = os.environ.get("BINANCE_API_SECRET", "")
    if not key or not secret:
        return False, "BINANCE_API_KEY 或 BINANCE_API_SECRET 为空"
    return True, "BINANCE_API_KEY/BINANCE_API_SECRET 已配置"


def check_services(ports: Tuple[int, ...] = DEFAULT_SERVICE_PORTS) -> Tuple[bool, str]:
    """dashboard 后端 / 前端端口监听检查 (可选, --skip-services 跳过)。"""
    details = []
    all_ok = True
    for p in ports:
        ok = _port_listening(p)
        all_ok = all_ok and ok
        details.append(f"{p}:{'LISTENING' if ok else 'CLOSED'}")
    detail = ", ".join(details)
    return (all_ok, f"{detail} (端口全 LISTENING)") if all_ok else (False, detail)


def check_heartbeat(redis_url: str = DEFAULT_REDIS_URL, stream: str = HEARTBEAT_STREAM,
                    max_age_s: int = HEARTBEAT_MAX_AGE_S) -> Tuple[bool, str]:
    """主系统心跳: systrader:heartbeat 流最后一条消息的 age < 120s。"""
    try:
        client = _redis_client(redis_url)
        msgs = client.xrevrange(stream, count=1)
        if not msgs:
            return False, f"流 {stream} 无消息 (主系统未运行或从未发布心跳)"
        _msg_id, fields = msgs[0]
        payload_raw = fields.get("payload", "{}")
        if isinstance(payload_raw, bytes):
            payload_raw = payload_raw.decode("utf-8", errors="replace")
        payload = json.loads(payload_raw)
        ts_str = payload.get("timestamp", "")
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        if age < 0:
            # 时钟漂移导致心跳时间在未来: 判 FAIL, 避免负 age 直接通过
            return False, f"心跳时间在未来 (age={age:.0f}s), 请检查系统时钟漂移"
        if age > max_age_s:
            return False, f"最后心跳 {age:.0f}s 前 (>{max_age_s}s, 主系统疑似停滞)"
        return True, f"最后心跳 {age:.0f}s 前"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:80]}"


def check_clash(proxy_port: int = DEFAULT_PROXY_PORT) -> Tuple[bool, str]:
    """Clash 端口 7897 监听; 未监听时尝试定位进程辅助诊断。"""
    if _port_listening(proxy_port):
        return True, f"端口 {proxy_port} LISTENING (Clash 在跑)"
    hint = _find_clash_process()
    if hint:
        return False, f"端口 {proxy_port} 未监听, 但发现进程: {hint}"
    return False, f"端口 {proxy_port} 未监听, 未发现 Clash 进程"


# ─── 汇总 ───

def summarize(results: List[Tuple[str, bool, str]]) -> Tuple[bool, int]:
    """汇总检查结果: 返回 (是否全部通过, 退出码)。"""
    all_pass = all(ok for _name, ok, _detail in results)
    return all_pass, (0 if all_pass else 1)


def run_all(args: argparse.Namespace) -> List[Tuple[str, bool, str]]:
    """按固定顺序执行全部检查 (跳过项不进入结果)。"""
    checks = [
        ("Redis", *check_redis(args.redis_url)),
        ("代理", *check_proxy(args.proxy_port)),
        ("API keys", *check_api_keys(args.env)),
    ]
    if not args.skip_services:
        checks.append(("服务端口", *check_services()))
    checks.append(("主系统心跳", *check_heartbeat(args.redis_url)))
    checks.append(("Clash", *check_clash(args.proxy_port)))
    return checks


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="启动前预检: Redis/代理/API keys/服务端口/主系统心跳/Clash")
    parser.add_argument("--skip-services", action="store_true",
                        help="跳过 dashboard 后端(8000)/前端(5173) 端口检查")
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL,
                        help=f"Redis URL (默认 {DEFAULT_REDIS_URL})")
    parser.add_argument("--proxy-port", type=int, default=DEFAULT_PROXY_PORT,
                        help=f"代理端口 (默认 {DEFAULT_PROXY_PORT})")
    parser.add_argument("--env", default="config/.env", help=".env 路径 (默认 config/.env)")
    args = parser.parse_args(argv)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # Windows 控制台中文输出

    results = run_all(args)
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    all_pass, code = summarize(results)
    print("── " + ("全部检查通过, 可以启动" if all_pass else f"存在 {sum(1 for _, ok, _ in results if not ok)} 项失败, 请修复后重试"))
    return code


if __name__ == "__main__":
    sys.exit(main())
