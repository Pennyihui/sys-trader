"""ShadowMonitor — 影子交易验证：双实例信号对齐 + 逐笔执行质量（TCA 风格）。

比对对象: 实盘实例（live）与模拟实例（paper）的 signal.generated / order.filled 事件。
验收标准（管道 spec 4.2）: 信号对齐 ≥95%（align_threshold）+ 逐笔滑点/填充率 + 1 周无系统性偏差。
明确不做相关性统计（低频样本不足）。

双实例运行方式（Task 19 计划，两个 SystemRunner 进程并行，共享 EventBus，
signal.generated 事件带 instance 标识 — Task 7 埋点）:
  实例 A: python -m shared.runner --instance live --execution-mode live --risk-per-trade 0.002 --hours 168
  实例 B: python -m shared.runner --instance paper --execution-mode paper --risk-per-trade 0.002 --hours 168

事件 payload 形状（signal_engine/engine.py 埋点）:
  signal.generated: {symbol, direction, entry_price, stop_loss, take_profit, signal_id, ...}
  order.filled:     {symbol, price, qty, signal_id, ...}

接线说明: 实时订阅 EventBus 消费事件 → record_signal/record_fill 的接线（StateStore 式）
留后续任务；本期以 record_signal/record_fill 接口 + save_report 落盘 JSON 报告为准。

信号匹配语义（按 signal_id 有无分流，杜绝单条 paper 信号满足多条 live 信号）:
  - 有 signal_id: live 按 id 匹配 paper id 集合，消耗式匹配（set.remove）——
    同一 paper 信号最多满足一个 live 信号，防重复 id 虚增比率、隐藏 ghost。
  - 无 signal_id: 退回 (symbol, direction) fallback 集合，且只对无 id 的
    paper 记录生效（有 id 的 paper 记录只走 id 匹配，不被 fallback 双计）；
    fallback 不消耗——无 id 时 (symbol, direction) 是唯一身份，重复记录无法区分。
  - 对齐率 = 匹配数 / max(live 数, paper 数): 任一实例多出的未匹配信号计入惩罚，
    避免只比 live 侧导致 paper 侧的"幽灵信号"被忽略。

执行配对语义（防跨 symbol 假滑点）:
  - 优先按 signal_id 配对（live/paper 各自 {signal_id: price} 映射，取交集）；
  - 其余按 symbol 分组、组内按序配对；
  - 任一侧 price<=0 的配对跳过；samples = 实际参与滑点计算的样本数。
"""

import json
import logging
import threading
from typing import Dict, List

logger = logging.getLogger(__name__)


class ShadowMonitor:
    def __init__(self, align_threshold: float = 0.95):
        self.align_threshold = align_threshold
        # RLock（可重入）: save_report 持锁时内部调用 alignment_ratio/execution_quality
        # 也会取同一把锁，普通 Lock 会自死锁（计划初版代码的坑，测试复现为挂起）
        self._lock = threading.RLock()
        self.signals: Dict[str, List[dict]] = {"live": [], "paper": []}
        self.fills: Dict[str, List[dict]] = {"live": [], "paper": []}

    def record_signal(self, instance: str, data: dict):
        with self._lock:
            self.signals[instance].append(data)

    def record_fill(self, instance: str, data: dict):
        with self._lock:
            self.fills[instance].append(data)

    def alignment_ratio(self) -> float:
        """双实例信号对齐率（0~1，无 live 信号时视为 1.0 不判失败）。

        按 (signal_id 有无) 分流匹配，杜绝单条 paper 信号满足多条 live 信号:
          - 有 id: 消耗式匹配 paper id 集合（set.remove，防重复 id 双计）；
          - 无 id: 只对无 id 的 paper 记录做 (symbol, direction) fallback。
        对齐率 = 匹配数 / max(live 数, paper 数)，未匹配的多余信号（任一侧）计入惩罚。
        """
        with self._lock:
            live = self.signals["live"]
            paper = self.signals["paper"]
            if not live:
                return 1.0
            # paper 按 (signal_id 有无) 分流
            paper_ids = {s.get("signal_id") for s in paper if s.get("signal_id")}
            paper_fallback = {
                (s.get("symbol"), s.get("direction"))
                for s in paper if not s.get("signal_id")
            }
            matched = 0
            for s in live:
                sid = s.get("signal_id")
                if sid:
                    if sid in paper_ids:
                        paper_ids.remove(sid)  # 消耗式: 一条 paper 只满足一条 live
                        matched += 1
                elif (s.get("symbol"), s.get("direction")) in paper_fallback:
                    # fallback 不消耗: 无 id 时 (symbol, direction) 是唯一身份
                    matched += 1
            return matched / max(len(live), len(paper))

    def execution_quality(self) -> dict:
        """逐笔执行质量：live 成交价 vs paper 成交价滑点（bps）。

        配对规则: 优先按 signal_id 精确配对（取两实例 id 交集）；未配上的
        （无 id 或 id 未交集）按 symbol 分组、组内按序配对，杜绝跨 symbol 假滑点。
        任一侧 price<=0 的配对跳过。slippage_bps 为滑点均值；
        fill_rate = 配对笔数 / 两实例成交笔数较大者；samples = 实际参与
        滑点计算的样本数（与 slippage 样本一致）。无样本时各字段为 None。
        """
        with self._lock:
            live = self.fills["live"]
            paper = self.fills["paper"]
            if not live or not paper:
                return {"slippage_bps": None, "fill_rate": None, "samples": 0}

            # 1) 按 signal_id 精确配对
            live_by_id = {f.get("signal_id"): f for f in live if f.get("signal_id")}
            paper_by_id = {f.get("signal_id"): f for f in paper if f.get("signal_id")}
            paired_ids = set(live_by_id) & set(paper_by_id)
            pairs = [(live_by_id[sid], paper_by_id[sid]) for sid in paired_ids]

            # 2) 其余（无 id 或 id 未交集）按 symbol 分组、组内按序配对
            live_rest = [f for f in live if f.get("signal_id") not in paired_ids]
            paper_rest = [f for f in paper if f.get("signal_id") not in paired_ids]
            for sym in {f.get("symbol") for f in live_rest} & {f.get("symbol") for f in paper_rest}:
                live_sym = [f for f in live_rest if f.get("symbol") == sym]
                paper_sym = [f for f in paper_rest if f.get("symbol") == sym]
                pairs.extend(zip(live_sym, paper_sym))

            if not pairs:
                return {"slippage_bps": None, "fill_rate": None, "samples": 0}

            slips = []
            for live_fill, paper_fill in pairs:
                lpx = float(live_fill.get("price") or 0)
                ppx = float(paper_fill.get("price") or 0)
                if lpx <= 0 or ppx <= 0:  # 任一侧价格无效 → 跳过该配对
                    continue
                slips.append((lpx - ppx) / ppx * 10000)
            return {
                "slippage_bps": round(sum(slips) / len(slips), 2) if slips else None,
                "fill_rate": round(len(pairs) / max(len(live), len(paper)), 2),
                "samples": len(slips),
            }

    def save_report(self, path: str):
        with self._lock:
            live_count = len(self.signals["live"])
            ratio = self.alignment_ratio()
            report = {
                "alignment_ratio": round(ratio, 4),
                "align_threshold": self.align_threshold,
                "execution_quality": self.execution_quality(),
                "signal_count": {k: len(v) for k, v in self.signals.items()},
                "fill_count": {k: len(v) for k, v in self.fills.items()},
                # 空运行（无 live 信号）不做通过判定 → null；有样本才给出布尔
                "pass": ratio >= self.align_threshold if live_count else None,
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info("Shadow report: %s", report)
        return report
