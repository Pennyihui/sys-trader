"""配置生成器 - 从池子生成完整的 mihomo 配置（服务自有的单一配置文件）。

服务完全接管 mihomo 核心后，本模块生成完整配置（不是片段）：
  - base 段: 端口(mixed 7897 / socks 7898 / http 7899)、mode rule、external-controller
  - 两档代理组:
      1) auto 组（load-balance round-robin，transfer_ok 节点）→ 浏览器/日常外网流量
      2) 8 个 auto-failover-* 组（url-test，按订阅源分组，币安健康检查）→ 交易系统
  - binance-failover 合并组（url-test，币安健康检查）→ 币安 REST 规则走它
  - 规则: binance 域名 → 严格组；geolocation-!cn → auto；cn → DIRECT
  - 8 个监听端口（7900-7907，绑定严格组，交易系统专用）
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# mihomo external-controller（core_manager.py 使用同一个 secret 做热重载）
CONTROLLER_SECRET = "proxy-pool-2026"

# base 段（原 Clash Verge config.yaml 的等价物，现在归服务管）
BASE_CONFIG = {
    "mixed-port": 7897,
    "socks-port": 7898,
    "port": 7899,
    "mode": "rule",
    "log-level": "info",
    "allow-lan": False,
    "ipv6": True,
    "external-controller": "127.0.0.1:9097",
    "secret": CONTROLLER_SECRET,
}

# 交易系统流量 → 严格组（binance 规则）
# 含 testnet 域名：模拟盘也必须走代理（为实盘同链路做准备）。
# 实测（2026-08-08）testnet.binancefuture.com 若走 MATCH→DIRECT，
# mihomo 直连会解析到不可达的 CloudFront IP 导致 dial timeout。
BINANCE_DOMAINS = [
    "fapi.binance.com",        # 期货实盘 API（代码里 14 处引用，最重要）
    "fstream.binance.com",     # 期货实盘 WS
    "api.binance.com",         # 现货实盘 API
    "ws-api.binance.com",      # 现货实盘 WS
    "stream.binance.com",      # 现货行情 WS
    "testnet.binancefuture.com",  # 期货模拟盘（订单/行情 REST）
    "testnet.binance.vision",     # 现货模拟盘（订单/行情 REST+WS）
]
# binance 规则指向合并组（所有源的 binance 真健康节点）。
# 实测教训（2026-08-08）: 单源组可能只有 3 个真健康节点（au1rxx），
# 轮换脆弱导致偶发失败；diplole 有 240 个真健康节点却闲置。
# 2026-08-14 改造: url-test + 币安健康检查——延迟最低节点被锁定，慢节点自动出局，
# 消除 round-robin 撞慢节点导致的延迟尖峰/时间戳超窗（-1021）。
FALLBACK_GROUP = "binance-failover"

# binance 健康检查 URL：测"节点→币安实盘"真实链路延迟（不是 gstatic 的 Google 延迟）。
# 交易 REST 是短连接，load-balance round-robin 每请求换节点=每次都可能轮到 421ms 慢节点；
# url-test 持续测量并锁定延迟最低节点，tolerance 100ms 内不切换避免抖动。
BINANCE_HEALTH_URL = "https://fapi.binance.com/fapi/v1/time"
BINANCE_HEALTH_INTERVAL = 60
BINANCE_HEALTH_TOLERANCE = 100

# auto 组（浏览器日常流量）: load-balance round-robin，只收"真能传数据"的节点
# （health_checker 的 transfer_ok 标记，见 health_checker.py）。
# 实测结论（2026-08-06/07）: TCP 健康 ≠ 能传数据；池子崩塌的根源是大量假健康节点。
# web-safe 组做兜底: fallback [auto, DIRECT] —— 池子全挂时浏览器走直连（CDN 类内容可访问），
# 池子恢复自动切回。彻底避免"网页连不上"。
AUTO_GROUP_MAX_NODES = 100
# 用户真实需求是 YouTube——健康测试用 YouTube 的 favicon（轻量、YouTube 专属连通性）
AUTO_URL = "https://www.youtube.com/favicon.ico"
AUTO_INTERVAL = 300
AUTO_MAX_LATENCY_MS = 800

# 监听端口映射: 端口 -> 源名, 拆分编号(None=不拆)
# 端口从 7900 开始，避开 Clash Verge Rev 的 mixed-port(7897)/port/socks-port
PORT_GROUPS = [
    (7900, "au1rxx", 1),
    (7901, "au1rxx", 2),
    (7902, "diplole", 1),
    (7903, "diplole", 2),
    (7904, "free18", 1),
    (7905, "free18", 2),
    (7906, "v2rayfreeclash", None),
    (7907, "topfreeclash", None),
]

GROUP_NAMES = [
    "auto-failover-au1rxx-1",
    "auto-failover-au1rxx-2",
    "auto-failover-diplole-1",
    "auto-failover-diplole-2",
    "auto-failover-free18-1",
    "auto-failover-free18-2",
    "auto-failover-v2rayfreeclash",
    "auto-failover-topfreeclash",
]

# build_clash_proxies 明确支持的节点协议，mihomo 配置里只放这些。
# 未知类型（ssr/anytls/…）会因缺必填字段让 mihomo 整份配置加载失败——
# 宁可跳过也不要让一个坏节点拖垮全局（新增订阅源可能带来这类节点）。
# http/socks5 不带认证字段即可（mihomo 默认无认证），其余字段在下方分支里显式装配。
SUPPORTED_PROXY_TYPES = {"ss", "hysteria2", "trojan", "vless", "vmess", "http", "socks5"}


def _group_proxies_by_source(pool: Dict) -> Dict[str, List[dict]]:
    """按 source 字段分组健康节点。"""
    groups: Dict[str, List[dict]] = {}
    for entry in pool.get("proxies", []):
        if not entry.get("healthy", False):
            continue
        source = entry.get("source", "unknown")
        groups.setdefault(source, []).append(entry)
    return groups


def build_clash_proxies(pool: Dict) -> List[dict]:
    """生成 Clash 格式的 proxies 列表（所有健康节点）。

    节点名加来源前缀（如 au1rxx-xxx），供 Script 增强按前缀分组。
    """
    proxies = []
    for entry in pool.get("proxies", []):
        if not entry.get("healthy", False):
            continue
        name = entry.get("name", "")
        ptype = entry.get("type", "")
        if not name or not ptype:
            continue
        # 白名单过滤：未明确支持的协议缺必填字段会让 mihomo 整份配置加载失败
        if ptype not in SUPPORTED_PROXY_TYPES:
            logger.debug("跳过不支持的节点类型 %s (%s)", ptype, name)
            continue
        source = entry.get("source", "other")
        prefixed = f"{source}-{name}"
        proxy = {
            "name": prefixed,
            "type": ptype,
            "server": entry.get("server", ""),
            "port": entry.get("port", 0),
        }
        if ptype == "ss":
            proxy["cipher"] = entry.get("cipher", "")
            proxy["password"] = entry.get("password", "")
            proxy["udp"] = entry.get("udp", False)
        elif ptype == "hysteria2":
            proxy["password"] = entry.get("password", "")
            proxy["sni"] = entry.get("sni", "")
            proxy["skip-cert-verify"] = entry.get("skip_cert_verify", True)
        elif ptype == "trojan":
            proxy["password"] = entry.get("password", "")
            proxy["sni"] = entry.get("sni", "")
            proxy["skip-cert-verify"] = entry.get("skip_cert_verify", True)
            proxy["udp"] = entry.get("udp", False)
        elif ptype == "vless":
            proxy["uuid"] = entry.get("uuid", "")
            proxy["network"] = entry.get("network", "tcp")
            proxy["tls"] = entry.get("tls", False)
            proxy["servername"] = entry.get("sni", "")
            proxy["flow"] = entry.get("flow", "")
        elif ptype == "vmess":
            proxy["uuid"] = entry.get("uuid", "")
            # mihomo 的 vmess 必须带 alterId；cipher 只接受固定几个值
            _cipher = entry.get("cipher", "auto")
            proxy["cipher"] = (
                _cipher if _cipher in ("auto", "aes-128-gcm", "chacha20-poly1305") else "auto"
            )
            proxy["alterId"] = entry.get("alterId", 0)
            proxy["tls"] = entry.get("tls", False)
            proxy["servername"] = entry.get("sni", "")
            proxy["network"] = entry.get("network", "tcp")
        # 传输探测标记: True=真实下载通过（auto 组只收这类节点）
        proxy["transfer_ok"] = entry.get("transfer_ok", False)
        proxy["binance_ok"] = entry.get("binance_ok", False)
        proxies.append(proxy)
    return proxies


def build_groups_and_listeners(pool: Dict) -> tuple[List[dict], List[dict]]:
    """按来源生成代理组 + 监听器。

    Returns:
        (proxy_groups, listeners)
    """
    by_source = _group_proxies_by_source(pool)

    proxy_groups = []
    listeners = []

    for port, source, part in PORT_GROUPS:
        entries = by_source.get(source, [])
        # 交易组只收 binance_ok 节点（能真实访问 fapi.binance.com 的才算数）
        verified = [e for e in entries if e.get("binance_ok")]
        if verified:
            entries = verified
        elif entries:
            logger.warning(
                "源 %s 无 binance 真健康节点(%d 个 TCP 健康)，暂时全部使用（探测多轮后会自动收紧）",
                source, len(entries),
            )
        # 大源拆两组：平分节点
        if part is not None:
            half = len(entries) // 2
            if part == 1:
                group_entries = entries[:half] if half > 0 else entries
            else:
                group_entries = entries[half:] if half > 0 else []
        else:
            group_entries = entries

        group_name = f"auto-failover-{source}" + (f"-{part}" if part else "")
        # 空组 mihomo 拒绝加载（proxies missing）→ 用 DIRECT 兜底（与 auto 组一致），
        # 等探测补充真节点后自动切回；DIRECT 是内建代理名，不带来源前缀
        if not group_entries:
            logger.warning("源 %s 无健康节点，组 %s 暂时用 DIRECT 兜底", source, group_name)
            prefixed_names = ["DIRECT"]
        else:
            # 节点名必须与 build_clash_proxies 一致（带来源前缀），否则 mihomo 解析失败
            prefixed_names = [
                f"{e.get('source', 'other')}-{e['name']}" for e in group_entries
            ]
        proxy_groups.append({
            "name": group_name,
            "type": "url-test",
            "proxies": prefixed_names,
            "url": BINANCE_HEALTH_URL,
            "interval": BINANCE_HEALTH_INTERVAL,
            "tolerance": BINANCE_HEALTH_TOLERANCE,
        })
        listeners.append({
            "name": f"in-{source}-{port}",
            "type": "mixed",
            "port": port,
            "proxy": group_name,
        })
        logger.info(
            "组 %s: %d 个节点 (端口 %d)",
            group_name, len(group_entries), port,
        )

    return proxy_groups, listeners


def generate_config_section(pool: Dict) -> Dict:
    """生成完整 Clash 配置片段。

    Returns:
        {"proxies": [...], "proxy-groups": [...], "listeners": [...]}
    """
    proxies = build_clash_proxies(pool)
    groups, listeners = build_groups_and_listeners(pool)
    return {
        "proxies": proxies,
        "proxy-groups": groups,
        "listeners": listeners,
    }


def generate_full_config(pool: Dict) -> Dict:
    """生成完整的 mihomo 配置（base + proxies + 两档组 + listeners + rules）。

    服务自有的 mihomo.yaml 由本函数全量生成——单一写入者，
    Clash Verge 不再参与，不会再有"组消失"问题。
    """
    proxies = build_clash_proxies(pool)
    groups, listeners = build_groups_and_listeners(pool)

    # auto 组候选：只要"真能传数据"的节点（health_checker 的传输探测标记）。
    # 实测教训（2026-08-06）: TCP 健康/低延迟与传输能力无相关性——2015 个"TCP 健康"
    # 节点里绝大多数一传数据就卡死，这是池子崩塌根源。
    candidates = [p for p in proxies if p.get("transfer_ok")]

    # auto 组：浏览器/日常外网流量（load-balance round-robin）。
    # 视频多并行连接会分摊到不同节点 → 聚合带宽；url 健康检查自动剔除坏节点；
    # 切节点只影响新连接，不中断现有视频流（url-test 会断）。
    auto_proxies = [p["name"] for p in candidates[:AUTO_GROUP_MAX_NODES]]
    if not auto_proxies:
        # 空组 mihomo 拒绝加载（proxies missing）→ 兜底填 DIRECT，等探测补充真节点
        auto_proxies = ["DIRECT"]
        logger.warning("无 transfer_ok 节点，auto 组暂时用 DIRECT 兜底")
    auto_group = {
        "name": "auto",
        "type": "load-balance",
        "strategy": "round-robin",
        "url": AUTO_URL,
        "interval": AUTO_INTERVAL,
        "proxies": auto_proxies,
    }

    # 浏览器兜底组: auto 全挂 → DIRECT（直连可达的 CDN 内容不受影响，池子恢复自动切回）
    web_safe_group = {
        "name": "web-safe",
        "type": "fallback",
        "url": AUTO_URL,
        "interval": AUTO_INTERVAL,
        "proxies": ["auto", "DIRECT"],
    }

    # binance 合并组：所有源的 binance 真健康节点（binance 规则走它，抗单源波动）
    # url-test 持续测量每个节点到 fapi.binance.com 的延迟并锁定最低者，
    # 慢节点自动出局；tolerance 100ms 内不切换，避免延迟抖动引发来回跳。
    bn_entries = [
        e for e in pool.get("proxies", [])
        if e.get("healthy") and e.get("binance_ok")
    ]
    if not bn_entries:
        bn_entries = [e for e in pool.get("proxies", []) if e.get("healthy")]
        logger.warning("无 binance 真健康节点，binance-failover 暂时使用全部健康节点")
    bn_names = [f"{e.get('source', 'other')}-{e['name']}" for e in bn_entries]
    if not bn_names:
        # 空组 mihomo 拒绝加载 → DIRECT 兜底（与 auto 组一致）
        bn_names = ["DIRECT"]
        logger.warning("无任何健康节点，binance-failover 暂时用 DIRECT 兜底")
    binance_group = {
        "name": "binance-failover",
        "type": "url-test",
        "url": BINANCE_HEALTH_URL,
        "interval": BINANCE_HEALTH_INTERVAL,
        "tolerance": BINANCE_HEALTH_TOLERANCE,
        "proxies": bn_names,
    }
    logger.info("binance-failover 组: %d 个真健康节点 (url-test)", len(bn_entries))

    # iyf-fixed 组：fallback，健康检查 URL 直接指向 pipecdn（视频 CDN）。
    # 关键设计（2026-08-08 实测）:
    #   - 免费节点几分钟就死；用 gstatic 做健康检查会选中"gstatic通但iyf不通"的节点
    #   - 健康 URL = pipecdn 根路径（无 CF 挑战、快速 404 响应）→ 选中的节点必然能到视频 CDN
    #   - 成员 = 全部 binance_ok + transfer_ok 节点（40+ 个，总有活的）
    #   - fallback 固定单节点（页面+视频同节点 → 签名一致、CF cookie 稳定），
    #     节点死了才切下一个（url-test 会随延迟自动轮换节点，破坏签名绑定）
    IYF_VERIFIED_PREFIXES = {"binance_ok", "transfer_ok"}
    iyf_members = [
        p["name"] for p in proxies
        if any(p.get(k) for k in IYF_VERIFIED_PREFIXES)
    ][:60]
    if not iyf_members:
        iyf_members = ["DIRECT"]
        logger.warning("无 iyf 可用节点，iyf-fixed 暂时用 DIRECT")
    iyf_group = {
        "name": "iyf-fixed",
        "type": "fallback",
        "url": "https://hss100.pipecdn.vip/",
        "interval": 120,
        "proxies": iyf_members,
    }

    # iyf.tv 路由（2026-08-08 实测，关键发现）:
    #   - 视频签名 URL 绑定"签发签名的节点 IP"——页面和分片必须走同一节点，
    #     轮换节点/直连都会 403/404（这是之前所有失败的根源）
    #   - 实测 20 个 binance 真健康节点中 9 个能拉视频（荷兰节点 239-377 KB/s 最快）
    #   - iyf-fixed 用 fallback 组：固定选一个节点（不轮换），节点死了才切下一个
    #     （切换后需刷新页面重新签发签名）
    #   - geosite 分类漏了 iyf.tv，才会被 geolocation-!cn 错误抓进代理
    rules = (
        [f"DOMAIN-SUFFIX,{d},{FALLBACK_GROUP}" for d in BINANCE_DOMAINS]
        + ["DOMAIN-SUFFIX,iyf.tv,iyf-fixed", "DOMAIN-SUFFIX,pipecdn.vip,iyf-fixed"]
        + ["GEOSITE,cn,DIRECT", "GEOSITE,geolocation-!cn,web-safe", "MATCH,DIRECT"]
    )

    # transfer_ok/binance_ok 只是候选筛选标记，不写进 mihomo 配置（mihomo 不认这些字段）
    for p in proxies:
        p.pop("transfer_ok", None)
        p.pop("binance_ok", None)

    return {
        **BASE_CONFIG,
        "proxies": proxies,
        "proxy-groups": [auto_group, web_safe_group, binance_group, iyf_group] + groups,
        "listeners": listeners,
        "rules": rules,
    }