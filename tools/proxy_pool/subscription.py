"""订阅下载器 - 从订阅 URL 下载并解析 Clash 格式的节点列表。

健壮性设计:
  - 重试: 失败后重试 3 次，指数退避 (5s/10s/20s)
  - 流式读取: 分块读大文件，避免 IncompleteRead
  - 本地缓存: 下载失败时使用上次成功的缓存，节点池不断粮
"""

import logging
import os
import time
import urllib.request
import urllib.error
import ssl
import yaml

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
MAX_RETRIES = 3
RETRY_DELAYS = [5, 10, 20]  # 秒


def _cache_path(url: str) -> str:
    """根据 URL 生成缓存文件路径。"""
    import hashlib
    os.makedirs(CACHE_DIR, exist_ok=True)
    name = hashlib.md5(url.encode()).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{name}.yaml")


def _read_cache(url: str) -> str:
    """读取缓存内容，没有返回 None。"""
    path = _cache_path(url)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _write_cache(url: str, content: str):
    """写入缓存。"""
    path = _cache_path(url)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        logger.warning("写入缓存失败: %s", e)


def _download_raw(
    url: str,
    timeout: int,
    proxy_host: str,
    proxy_port: int,
) -> str:
    """下载订阅原始内容（流式读取，处理大文件）。"""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    proxy_handler = urllib.request.ProxyHandler({
        "http": f"http://{proxy_host}:{proxy_port}",
        "https": f"http://{proxy_host}:{proxy_port}",
    })
    opener = urllib.request.build_opener(proxy_handler)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "ClashVerge/1.0 "
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            ),
        },
    )

    # 流式读取：分 64KB 块，避免大文件一次性读导致 IncompleteRead
    with opener.open(req, timeout=timeout) as resp:
        chunks = []
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks).decode("utf-8")
    return raw


def _parse_proxies(raw: str) -> list[dict]:
    """解析 YAML 提取 proxies。"""
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError("订阅格式错误: 不是有效的 YAML 映射")
    proxies = data.get("proxies", [])
    if not isinstance(proxies, list):
        raise ValueError("订阅格式错误: proxies 不是列表")
    return proxies


def download_subscription(
    url: str,
    timeout: int = 60,
    proxy_host: str = "127.0.0.1",
    proxy_port: int = 7897,
) -> list[dict]:
    """从单个订阅 URL 下载 Clash 配置，提取所有代理节点。

    带重试 + 缓存兜底：
      - 尝试下载（最多 3 次，指数退避）
      - 全部失败 → 使用本地缓存（如果有）
      - 无缓存 → 抛异常

    Args:
        url: 订阅地址
        timeout: 超时秒数
        proxy_host: HTTP 代理主机
        proxy_port: HTTP 代理端口

    Returns:
        节点列表 [{name, type, server, port, ...}]

    Raises:
        ValueError: 下载失败且无缓存
    """
    if not url:
        raise ValueError("订阅地址为空")

    logger.info("下载订阅: %s (通过代理 %s:%s)", url, proxy_host, proxy_port)

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            raw = _download_raw(url, timeout, proxy_host, proxy_port)
            proxies = _parse_proxies(raw)
            _write_cache(url, raw)  # 成功后写缓存
            logger.info(
                "从订阅中提取到 %d 个节点 (第%d次尝试)",
                len(proxies), attempt + 1,
            )
            return proxies
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                logger.warning(
                    "第%d次下载失败: %s，%ds后重试",
                    attempt + 1, str(e)[:80], delay,
                )
                time.sleep(delay)

    # 全部失败 → 使用缓存
    cached = _read_cache(url)
    if cached is not None:
        try:
            proxies = _parse_proxies(cached)
            logger.warning(
                "下载全部失败，使用缓存: %d 个节点 (错误: %s)",
                len(proxies), str(last_error)[:80],
            )
            return proxies
        except Exception:
            pass

    raise ValueError(f"下载订阅失败(重试{MAX_RETRIES}次): {last_error}")


def parse_proxy_to_entry(proxy: dict) -> dict:
    """将 Clash 格式的 proxy 节点转换为标准化的池子条目。

    Args:
        proxy: Clash 格式的代理配置

    Returns:
        标准化的节点条目
    """
    entry = {
        "name": proxy.get("name", ""),
        "type": proxy.get("type", ""),
        "server": proxy.get("server", ""),
        "port": proxy.get("port", 0),
        "healthy": True,
        "last_checked": "",
        "fail_count": 0,
        "added_at": "",
        "source": "subscription",
    }

    # 根据类型复制特定字段
    ptype = proxy.get("type", "")
    if ptype == "ss":
        entry["cipher"] = proxy.get("cipher", "")
        entry["password"] = proxy.get("password", "")
        entry["udp"] = proxy.get("udp", False)
    elif ptype == "hysteria2":
        entry["password"] = proxy.get("password", "")
        entry["sni"] = proxy.get("sni", "")
        entry["skip_cert_verify"] = proxy.get("skip-cert-verify", False)
    elif ptype == "trojan":
        entry["password"] = proxy.get("password", "")
        entry["sni"] = proxy.get("sni", "")
        entry["skip_cert_verify"] = proxy.get("skip-cert-verify", False)
        entry["udp"] = proxy.get("udp", False)
    elif ptype == "vless":
        entry["uuid"] = proxy.get("uuid", "")
        entry["network"] = proxy.get("network", "tcp")
        entry["tls"] = proxy.get("tls", False)
        entry["sni"] = proxy.get("servername", "")
        entry["flow"] = proxy.get("flow", "")
    elif ptype == "vmess":
        entry["uuid"] = proxy.get("uuid", "")
        entry["cipher"] = proxy.get("cipher", "auto")
        entry["alterId"] = proxy.get("alterId", 0)
        entry["tls"] = proxy.get("tls", False)
        entry["sni"] = proxy.get("servername", "")
        entry["network"] = proxy.get("network", "tcp")

    return entry


def _source_key(url: str) -> str:
    """从订阅 URL 生成简短来源标识，用于按订阅源分组。

    例: github.com/Au1rxx/... -> au1rxx
        raw.githubusercontent.com/diplole/... -> diplole
        cdn.jsdelivr.net/gh/free18/... -> free18
        raw.githubusercontent.com/sunmiao4458/... -> sunmiao4458
        raw.githubusercontent.com/zhuhaiuk/... -> zhuhaiuk
    """
    import re
    m = re.search(
        r"(?:Au1rxx|diplole|free18|v2rayfreeclash|topfreeclash|ikuku|free-vpn|sunmiao4458|zhuhaiuk)",
        url, re.I,
    )
    if m:
        return m.group(0).lower()
    # 兜底: 用主机名
    m2 = re.search(r"//([^/]+)", url)
    return m2.group(1).split(".")[0] if m2 else "unknown"


def download_all_subscriptions(
    urls: list[str],
    timeout: int = 60,
    proxy_host: str = "127.0.0.1",
    proxy_port: int = 7897,
) -> list[dict]:
    """从多个订阅 URL 下载并合并所有节点，按名称去重。

    Args:
        urls: 订阅地址列表
        timeout: 每个订阅的超时秒数
        proxy_host: HTTP 代理主机
        proxy_port: HTTP 代理端口

    Returns:
        合并后的节点列表（按名称去重，失败源使用缓存）
    """
    all_proxies = []
    seen_names = set()
    success_count = 0

    for url in urls:
        if not url or not url.strip():
            continue
        # 用订阅 URL 主机名做来源标识（用于分组）
        source = _source_key(url)
        try:
            proxies = download_subscription(
                url.strip(), timeout, proxy_host, proxy_port
            )
            for p in proxies:
                name = p.get("name", "")
                if name and name not in seen_names:
                    seen_names.add(name)
                    p["source"] = source  # 标记来源
                    all_proxies.append(p)
            success_count += 1
            logger.info("从 %s 获取到 %d 个节点 (source=%s)", url, len(proxies), source)
        except Exception as e:
            logger.warning("从 %s 下载失败: %s", url, e)

    logger.info(
        "多订阅合并完成: 共 %d 个节点 (%d/%d 订阅成功)",
        len(all_proxies), success_count, len(urls),
    )
    return all_proxies