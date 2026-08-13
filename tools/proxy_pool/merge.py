"""合并引擎 - 将新节点合并到本地池子，保留旧可用节点，清理死节点。"""

import logging
from datetime import datetime, timezone
from typing import Dict

from subscription import parse_proxy_to_entry

logger = logging.getLogger(__name__)


def merge_proxies(
    pool: Dict,
    new_proxies: list[dict],
    max_fail_days: int = 7,
    max_fail_count: int = 10080,
) -> Dict:
    """合并新旧节点。

    策略：
      - 新节点有，旧池子没有 → 加入（标记为健康）
      - 新旧都有 → 保留旧池子的状态（可用/不可用/失败次数）
      - 旧池子有，新节点没有 → 保留（标记为"残留"）
      - 残留节点连续失败超过阈值 → 清理

    Args:
        pool: 本地池子数据
        new_proxies: 新下载的节点列表
        max_fail_days: 残留节点最大失败天数
        max_fail_count: 残留节点最大失败次数

    Returns:
        更新后的池子
    """
    now = datetime.now(timezone.utc).isoformat()
    existing = {p["name"]: p for p in pool.get("proxies", [])}
    seen_names = set()

    merged = []
    added_count = 0
    kept_count = 0
    failed_count = 0

    for new_p in new_proxies:
        name = new_p.get("name", "")
        if not name:
            continue
        seen_names.add(name)

        if name in existing:
            # 新旧都有 → 保留旧池子的状态
            old = existing[name]
            merged.append({
                **new_p,  # 新节点数据（server, port 等可能更新）
                "healthy": old.get("healthy", True),
                "last_checked": old.get("last_checked", ""),
                "fail_count": old.get("fail_count", 0),
                "added_at": old.get("added_at", now),
                # 传输探测标记（health_checker 打的）必须保留——new_p 来自订阅，
                # 不含 transfer_ok/binance_ok，直接重建会把"真健康"标记丢成 False
                "transfer_ok": old.get("transfer_ok", new_p.get("transfer_ok", False)),
                "binance_ok": old.get("binance_ok", new_p.get("binance_ok", False)),
                # 优先用新标记的 source，缺失时保留旧值
                "source": new_p.get("source") or old.get("source", "unknown"),
            })
            kept_count += 1
        else:
            # 新节点 → 加入
            entry = parse_proxy_to_entry(new_p)
            entry["added_at"] = now
            entry["healthy"] = True  # 新节点默认标记为可用（load-balance 会自动跳过坏的）
            entry["source"] = new_p.get("source", "unknown")
            merged.append(entry)
            added_count += 1

    # 旧池子有，新节点没有 → 保留（残留节点）
    removed_count = 0
    for name, old in existing.items():
        if name not in seen_names:
            fail_count = old.get("fail_count", 0)
            # 检查是否应该清理
            should_remove = False
            if fail_count >= max_fail_count:
                should_remove = True
            if old.get("added_at"):
                try:
                    added = datetime.fromisoformat(old["added_at"])
                    age_days = (datetime.now(timezone.utc) - added).days
                    if age_days > max_fail_days and not old.get("healthy", True):
                        should_remove = True
                except Exception:
                    pass

            if should_remove:
                removed_count += 1
                logger.info("清理死节点: %s (失败%d次)", name, fail_count)
            else:
                merged.append(old)
                kept_count += 1

    pool["proxies"] = merged
    pool["last_updated"] = now

    logger.info(
        "合并完成: 新增 %d, 保留 %d, 清理 %d, 总计 %d",
        added_count, kept_count, removed_count, len(merged),
    )

    return pool