"""mihomo 配置应用器 - 把完整配置写入服务自有文件并热重载核心。

服务完全接管核心后，本模块不再碰 Clash Verge 的任何文件：
  - 配置写入 tools/proxy_pool/mihomo.yaml（单一写入者）
  - 核心生命周期交给 core_manager（热重载 / 看门狗）
"""

import hashlib
import logging
import os
from typing import Dict

logger = logging.getLogger(__name__)

import yaml

MIHOMO_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mihomo.yaml")

# 上次写入的配置哈希（避免健康检查每 60s 无变化也热重载）
_last_written_hash = None


def update_clash_config(config: Dict) -> bool:
    """全量写入 mihomo.yaml（完整配置，不再是片段拼接）。"""
    try:
        # 原子写：先写临时文件再 os.replace，避免 watchdog 并发写时读到半截配置
        tmp_path = MIHOMO_CONFIG_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(config, f, allow_unicode=True, sort_keys=False)
        os.replace(tmp_path, MIHOMO_CONFIG_PATH)
        logger.info(
            "mihomo.yaml 已写入 (%d proxies, %d groups, %d listeners, %d rules)",
            len(config.get("proxies", [])),
            len(config.get("proxy-groups", [])),
            len(config.get("listeners", [])),
            len(config.get("rules", [])),
        )
        return True
    except Exception as e:
        logger.error("写入 mihomo.yaml 失败: %s", e)
        return False


def apply_config(pool: Dict, force_reload: bool = False) -> bool:
    """生成完整配置 → 写入 → 按需热重载核心。

    Args:
        pool: 代理池数据
        force_reload: 强制热重载（订阅更新/手动 --generate 时用 True；
                      健康检查循环用 False，配置没变化就跳过）
    """
    from config_generator import generate_full_config
    from core_manager import reload_core

    config = generate_full_config(pool)
    if not config["proxies"]:
        logger.warning("没有可用节点，跳过配置更新")
        return False

    if not update_clash_config(config):
        return False

    global _last_written_hash
    with open(MIHOMO_CONFIG_PATH, "rb") as f:
        current_hash = hashlib.md5(f.read()).hexdigest()

    if force_reload or current_hash != _last_written_hash:
        _last_written_hash = current_hash
        return reload_core()
    logger.info("配置未变化，跳过热重载")
    return True
