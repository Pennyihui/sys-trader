"""verge-mihomo 核心生命周期管理 — 服务完全接管核心。

Clash Verge 已退出依赖链，核心归代理池服务管：
  - start_core(): 清场后 spawn（-f 服务自有配置 -d 自有数据目录），等端口就绪
  - reload_core(): 热重载（PUT /configs?force=true），不用杀进程
  - watchdog(): 端口 7897 不通 → 自动重启

数据目录（tools/proxy_pool/data/）首次运行时会从 Clash Verge 目录拷贝
geo 文件（Country.mmdb / geoip.dat / geosite.dat），之后完全独立。
"""

import json
import logging
import os
import shutil
import socket
import subprocess
import time
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

from config_generator import CONTROLLER_SECRET

MIHOMO_EXE = r"D:\小程序\Clash Verge\verge-mihomo.exe"
CLASH_VERGE_DIR = (
    r"C:\Users\Evan\AppData\Roaming\io.github.clash-verge-rev.clash-verge-rev"
)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "mihomo.yaml")
DATA_DIR = os.path.join(BASE_DIR, "data")
GEO_FILES = ["Country.mmdb", "geoip.dat", "geosite.dat"]

CONTROLLER = "http://127.0.0.1:9097"
# 探测端口 = mixed-port（交易系统和浏览器都走它，最直接的健康信号）
PROBE_HOST, PROBE_PORT = "127.0.0.1", 7897
START_TIMEOUT = 15


def _ensure_system_proxy():
    """确保系统代理指向 7897（浏览器走代理）。

    实测（2026-08-09）: Clash Verge 退出时会清掉系统代理（ProxyEnable=0），
    重启后浏览器直连外网失败。服务必须自己接管这个开关。
    """
    import winreg
    key_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE
        )
        winreg.SetValueEx(key, "ProxyEnable", 0, winreg.REG_DWORD, 1)
        winreg.SetValueEx(key, "ProxyServer", 0, winreg.REG_SZ, "127.0.0.1:7897")
        winreg.CloseKey(key)
        logger.info("系统代理已启用: 127.0.0.1:7897")
    except Exception as e:
        logger.error("设置系统代理失败: %s", e)


def _ensure_data_dir():
    """准备数据目录，首次运行拷贝 geo 文件。"""
    os.makedirs(DATA_DIR, exist_ok=True)
    for f in GEO_FILES:
        dst = os.path.join(DATA_DIR, f)
        if os.path.exists(dst):
            continue
        src = os.path.join(CLASH_VERGE_DIR, f)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            logger.info("已拷贝 geo 文件: %s", f)
        else:
            logger.warning("geo 文件缺失（%s 不存在，GEOSITE 规则会失效）: %s", src, f)


def is_alive(timeout: float = 1.0) -> bool:
    """核心是否在线：TCP 探测 mixed-port。"""
    try:
        with socket.create_connection((PROBE_HOST, PROBE_PORT), timeout=timeout):
            return True
    except OSError:
        return False


def start_core() -> bool:
    """清场后 spawn 核心，等待端口就绪。"""
    # 清场：杀掉可能残留的核心（包括 Clash Verge 时代拉起的，避免两个核心打架）
    subprocess.run(
        ["taskkill", "/f", "/im", "verge-mihomo.exe"],
        capture_output=True, timeout=10,
    )
    _ensure_data_dir()

    core_log = open(os.path.join(DATA_DIR, "core.log"), "ab")
    try:
        proc = subprocess.Popen(
            [MIHOMO_EXE, "-f", CONFIG_PATH, "-d", DATA_DIR],
            stdout=core_log, stderr=core_log, stdin=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    except FileNotFoundError:
        logger.error("核心二进制不存在: %s", MIHOMO_EXE)
        core_log.close()
        return False
    logger.info("核心已启动 pid=%s", proc.pid)

    for _ in range(START_TIMEOUT):
        if is_alive():
            logger.info("核心就绪: %s:%s 已监听", PROBE_HOST, PROBE_PORT)
            _ensure_system_proxy()
            return True
        time.sleep(1)
    logger.error("核心启动超时（%ds 内端口未就绪），日志: %s", START_TIMEOUT, core_log.name)
    return False


def reload_core() -> bool:
    """热重载配置（PUT /configs?force=true）。核心不在线时改为重启。"""
    if not is_alive():
        logger.warning("核心不在线，改为重新启动")
        return start_core()

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        payload = f.read()
    body = json.dumps({"payload": payload}).encode()
    req = urllib.request.Request(
        f"{CONTROLLER}/configs?force=true",
        data=body, method="PUT",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CONTROLLER_SECRET}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            ok = resp.status in (200, 204)
            logger.info("核心配置已热重载 (HTTP %s)", resp.status)
            return ok
    except urllib.error.HTTPError as e:
        logger.error("热重载失败 (HTTP %s): %s", e.code, e.read()[:200])
        return False
    except Exception as e:
        logger.error("热重载失败: %s", e)
        return False


def watchdog():
    """看门狗：核心不在线就拉起。服务循环每 tick 调用一次。"""
    if not is_alive():
        logger.warning("看门狗: %s:%s 未监听，重启核心", PROBE_HOST, PROBE_PORT)
        start_core()
