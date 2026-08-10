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

匹配语义:
  - 信号对齐按 "优先 signal_id，无 signal_id 时退回 symbol+direction" 匹配。
    signal_id 是跨事件流（signal.generated ↔ order.filled）的唯一关联键（Task 7），
    两实例收到同一 signal_id 即视为同一信号，方向一致。
  - 对齐率 = 匹配数 / max(live 数, paper 数): 任一实例多出的未匹配信号计入惩罚，
    避免只比 live 侧导致 paper 侧的"幽灵信号"被忽略。
"""

import json
import logging
import threading
from typing import Dict, List, Optional

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

        优先按 signal_id 匹配（更精确，Task 7 埋点），无 signal_id 时退回
        (symbol, direction) 匹配。对齐率 = 匹配数 / max(live 数, paper 数)，
        未匹配的多余信号（任一侧）计入惩罚。
        """
        with self._lock:
            live = self.signals["live"]
            paper = self.signals["paper"]
            if not live:
                return 1.0
            paper_ids = {s.get("signal_id") for s in paper if s.get("signal_id")}
            paper_set = {(s.get("symbol"), s.get("direction")) for s in paper}
            matched = 0
            for s in live:
                sid = s.get("signal_id")
                if sid:
                    if sid in paper_ids:
                        matched += 1
                elif (s.get("symbol"), s.get("direction")) in paper_set:
                    matched += 1
            return matched / max(len(live), len(paper))

    def execution_quality(self) -> dict:
        """逐笔执行质量：live 成交价 vs paper 成交价滑点（bps，按序配对）。

        slippage_bps = (live_price - paper_price) / paper_price * 10000，逐笔取均值。
        fill_rate = 可配对笔数 / 两实例成交笔数较大者。无成交样本时各字段为 None。
        """
        with self._lock:
            live = self.fills["live"]
            paper = self.fills["paper"]
            if not live or not paper:
                return {"slippage_bps": None, "fill_rate": None, "samples": 0}
            pairs = min(len(live), len(paper))
            if pairs == 0:
                return {"slippage_bps": None, "fill_rate": None, "samples": 0}
            slips = []
            for i in range(pairs):
                base = float(paper[i].get("price") or 0)
                if base <= 0:
                    continue
                slips.append((float(live[i].get("price") or 0) - base) / base * 10000)
            return {
                "slippage_bps": round(sum(slips) / len(slips), 2) if slips else None,
                "fill_rate": round(pairs / max(len(live), len(paper)), 2),
                "samples": pairs,
            }

    def save_report(self, path: str):
        with self._lock:
            report = {
                "alignment_ratio": round(self.alignment_ratio(), 4),
                "execution_quality": self.execution_quality(),
                "signal_count": {k: len(v) for k, v in self.signals.items()},
                "fill_count": {k: len(v) for k, v in self.fills.items()},
                "pass": self.alignment_ratio() >= self.align_threshold,
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info("Shadow report: %s", report)
        return report
