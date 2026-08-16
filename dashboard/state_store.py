"""StateStore — EventBus 消费侧：维护 dashboard 所需的系统状态副本。"""

import logging
import threading
import time
from typing import Dict, List, Optional

from shared.event_bus import EventBus

logger = logging.getLogger(__name__)

STREAMS = [
    "position.changed", "order.filled", "signal.generated",
    "signal.approved", "signal.rejected", "heartbeat",
    "position.risk",
]


class StateStore:
    def __init__(self, event_bus: EventBus, instance_filter: str = "live",
                 max_signals: int = 50):
        self.bus = event_bus
        self.instance_filter = instance_filter
        self.max_signals = max_signals
        self._lock = threading.Lock()
        self.positions: Dict[str, dict] = {}
        self.equity: float = 0.0
        # 2026-08-16 审计: margin_ratio 默认 0.0 (原 1.0 会让面板在首个
        # equity 事件到达前闪现 100% 保证金率红色告警)
        self.margin_ratio: float = 0.0
        self.daily_pnl: float = 0.0
        self.drawdown: float = 0.0
        self.signals: List[dict] = []
        self.orders: List[dict] = []
        self.heartbeats: Dict[str, float] = {}
        self.assets: List[dict] = []  # 账户资产构成 (equity 事件携带, 2026-08-16)
        self.available_balance: float = 0.0  # 可用余额 (equity 事件, 面板二期)
        self.position_risks: Dict[str, dict] = {}  # 清算价/ADL (position.risk 事件, #2)
        self.fee_rate: Optional[float] = None  # 实际往返费率 (equity 事件, #1)
        self._threads: List[threading.Thread] = []

    def start(self):
        # 启动时从 Redis 流重放最近事件, 重建持仓/权益/信号/订单状态
        # (2026-08-16: dashboard 重启后 StateStore 从零开始, 持仓显示丢失;
        # 持仓事件只在开仓/对账漂移时发布, 不重放则面板永远看不到存量持仓)
        self._bootstrap()
        for stream in STREAMS:
            t = threading.Thread(
                target=self.bus.run_consumer,
                args=(stream, "dashboard", self._handle, 5, 100),
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        logger.info("StateStore consuming %d streams", len(STREAMS))

    def _bootstrap(self):
        """XREVRANGE 重放 position/signal/order 流事件 (倒序取回后按时间正序处理)。

        注意重放窗口必须 ≥ Redis 流 maxlen: position.changed 里 equity 事件
        每 60s 一条, 若只取 500 条 (~8h) 会把更早的开仓事件挤出窗口,
        dashboard 重启后存量持仓再次丢失 (BUG-026 复发, 2026-08-16 修复)。
        maxlen=10000 全量重放 ≈ 7 天, 正序回放后状态收敛到最新。
        """
        import json as _json
        replay_streams = [
            "position.changed", "signal.generated",
            "signal.approved", "signal.rejected", "order.filled",
        ]
        try:
            for stream in replay_streams:
                key = self.bus._key(stream)
                msgs = self.bus.redis.xrevrange(key, count=10000)
                events = []
                for _msg_id, fields in msgs:
                    payload = fields.get("payload")
                    if not payload:
                        continue
                    try:
                        ev = _json.loads(payload)
                    except ValueError:
                        continue
                    if ev.get("stream") == stream:
                        events.append({
                            "stream": stream,
                            "data": ev.get("data", {}),
                            "timestamp": ev.get("timestamp", ""),
                        })
                for ev in reversed(events):  # 正序重放
                    self._handle(ev)
            logger.info("StateStore bootstrap done (positions=%d, orders=%d)",
                        len(self.positions), len(self.orders))
        except Exception as e:
            logger.warning("StateStore bootstrap failed (Redis down?): %s", e)

    def positions_snapshot(self) -> Dict[str, dict]:
        """锁内快照 — DataCollector 遍历持仓用, 防消费线程并发改 dict
        (2026-08-16: 广播线程直接迭代 positions.items() 存在 RuntimeError 竞态)。"""
        with self._lock:
            return dict(self.positions)

    def stop(self):
        """仅 join 本实例的消费线程，不触碰共享 bus。

        注意：bus.stop() 会停掉 EventBus 上所有消费者——Task 12 注入
        共享 EventBus 后调用即杀全系统消费者，故这里不调用。消费线程是
        daemon，join 超时后随进程退出；per-consumer 优雅停止留待后续。
        """
        deadline = time.time() + 3
        for t in self._threads:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            t.join(timeout=remaining)
        self._threads.clear()

    def _should_accept(self, data: dict) -> bool:
        inst = data.get("instance", "live")
        return inst == self.instance_filter

    def _handle(self, event):
        if isinstance(event, dict):
            stream = event.get("stream", "")
            data = event.get("data", {})
            ts = event.get("timestamp", "")
        else:
            stream = getattr(event, "stream", "")
            data = getattr(event, "data", {}) or {}
            ts = getattr(event, "timestamp", "")
        if not self._should_accept(data):
            return
        # 面板二期 (2026-08-16): 事件时间戳随条目保留, 前端显示时间
        if ts and isinstance(data, dict) and "ts" not in data:
            data = {**data, "ts": ts}
        with self._lock:
            if stream == "position.changed":
                self._on_position(data)
            elif stream == "signal.generated":
                self.signals.append(data)
                self.signals = self.signals[-self.max_signals:]
            elif stream == "order.filled":
                self.orders.append(data)
                self.orders = self.orders[-self.max_signals:]
            elif stream == "heartbeat":
                self.heartbeats.update(data.get("modules", {}))
            elif stream == "position.risk":
                # 清算价/爆仓距离/ADL 排名 (2026-08-16 #2): 按 symbol 覆盖式存储
                sym = data.get("symbol")
                if sym:
                    self.position_risks[sym] = data
            elif stream in ("signal.approved", "signal.rejected"):
                self.signals.append({"decision": stream, **data})
                self.signals = self.signals[-self.max_signals:]

    def _update_metrics(self, data: dict):
        for attr, key in (("margin_ratio", "margin_ratio"),
                          ("daily_pnl", "daily_pnl"),
                          ("drawdown", "drawdown")):
            if data.get(key) is not None:
                setattr(self, attr, data[key])

    def _on_position(self, data: dict):
        event = data.get("event")
        if event == "open":
            self.positions[data["symbol"]] = data
        elif event == "close":
            self.positions.pop(data["symbol"], None)
            self.position_risks.pop(data["symbol"], None)
            if data.get("total_equity") is not None:
                self.equity = data["total_equity"]
            self._update_metrics(data)
        elif event == "equity":
            if data.get("total_equity") is not None:
                self.equity = data["total_equity"]
            if data.get("available_balance") is not None:
                self.available_balance = data["available_balance"]
            # 实际往返费率 (2026-08-16 #1): 面板保本价与盈亏口径同源
            if data.get("fee_rate") is not None:
                self.fee_rate = data["fee_rate"]
            if isinstance(data.get("assets"), list):
                self.assets = data["assets"]
            self._update_metrics(data)
