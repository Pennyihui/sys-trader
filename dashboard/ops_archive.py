"""OpsArchive — 运维历史归档 (2026-08-16 运维看板)。

消费 Redis 的 heartbeat / command 流并归档到 SQLite (data/ops_history.db):
  - heartbeat_history: 每 5s 一条 (kline闭合/订单/时间偏移/模块心跳年龄快照)
  - command_history:   运维命令事件 (pause/resume/emergency_stop/force_exit/...)

Redis Stream 有 maxlen=10000 (~14h), 归档后运维看板可看 7 天历史曲线
(参考 Prometheus+Grafana 的时序模式, 但零外部依赖)。

与 StateStore 使用不同的 consumer group, 互不干扰; Redis 不可用时
start 静默降级 (dashboard 仍可用, 只是无历史)。
"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(PROJECT_ROOT / "data" / "ops_history.db")


class OpsArchive:
    def __init__(self, db_path: str = DEFAULT_DB_PATH, retention_days: int = 7):
        self.db_path = db_path
        self.retention_days = retention_days
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._create_tables()
        self._threads: List[threading.Thread] = []
        self._stop = threading.Event()
        self.prune()

    def _create_tables(self):
        with self._lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS heartbeat_history (
                    ts REAL PRIMARY KEY,
                    instance TEXT,
                    kline_closes REAL,
                    orders_placed REAL,
                    orders_failed REAL,
                    server_time_offset REAL,
                    modules TEXT,
                    ws_connected REAL DEFAULT 0,
                    ws_total REAL DEFAULT 0,
                    funding_cost REAL DEFAULT 0,
                    risk_per_trade REAL DEFAULT 0,
                    max_leverage REAL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS command_history (
                    ts REAL PRIMARY KEY,
                    source TEXT,
                    command TEXT,
                    symbol TEXT
                );
                CREATE TABLE IF NOT EXISTS equity_history (
                    ts REAL PRIMARY KEY,
                    instance TEXT,
                    total_equity REAL,
                    available_balance REAL,
                    margin_ratio REAL,
                    daily_pnl REAL,
                    drawdown REAL
                );
                CREATE TABLE IF NOT EXISTS trade_history (
                    ts REAL PRIMARY KEY,
                    symbol TEXT,
                    direction TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    quantity REAL,
                    gross_pnl REAL,
                    fee REAL,
                    realized_pnl REAL
                );
                CREATE TABLE IF NOT EXISTS alert_history (
                    ts REAL PRIMARY KEY,
                    source TEXT,
                    message TEXT
                );
                CREATE TABLE IF NOT EXISTS lifecycle_history (
                    ts REAL PRIMARY KEY,
                    event TEXT,
                    pid INTEGER,
                    instance TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_hb_ts ON heartbeat_history(ts);
                CREATE INDEX IF NOT EXISTS idx_eq_ts ON equity_history(ts);
                CREATE INDEX IF NOT EXISTS idx_tr_ts ON trade_history(ts);
            """)
            # 旧库迁移: heartbeat_history 新增列 (2026-08-16 面板二期)
            try:
                self.conn.execute("ALTER TABLE heartbeat_history ADD COLUMN ws_connected REAL DEFAULT 0")
                self.conn.execute("ALTER TABLE heartbeat_history ADD COLUMN ws_total REAL DEFAULT 0")
                self.conn.execute("ALTER TABLE heartbeat_history ADD COLUMN funding_cost REAL DEFAULT 0")
                self.conn.execute("ALTER TABLE heartbeat_history ADD COLUMN risk_per_trade REAL DEFAULT 0")
                self.conn.execute("ALTER TABLE heartbeat_history ADD COLUMN max_leverage REAL DEFAULT 0")
            except Exception:
                pass  # 列已存在
            self.conn.commit()

    def prune(self):
        """删除 retention_days 之前的历史 (启动时 + 每 6h 由 _prune_loop 调用)。"""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.retention_days)).timestamp()
        with self._lock:
            total = 0
            for table in ("heartbeat_history", "command_history", "equity_history",
                          "trade_history", "alert_history", "lifecycle_history"):
                total += self.conn.execute(
                    f"DELETE FROM {table} WHERE ts < ?", (cutoff,)).rowcount
            self.conn.commit()
        if total:
            logger.info("OpsArchive pruned: %d rows", total)

    # ─── 事件处理 (EventBus 回调) ───

    @staticmethod
    def _event_ts(event) -> float:
        try:
            return datetime.fromisoformat(event.timestamp).timestamp()
        except (TypeError, ValueError):
            return time.time()

    def on_heartbeat(self, event):
        data = event.data or {}
        stats = data.get("stats", {}) or {}
        ts = self._event_ts(event)
        row = (
            ts,
            data.get("instance", ""),
            stats.get("kline_closes", 0),
            stats.get("orders_placed", 0),
            stats.get("orders_failed", 0),
            stats.get("server_time_offset", 0),
            json.dumps(data.get("modules", {}) or {}),
            stats.get("ws_connected", 0),
            stats.get("ws_total", 0),
            stats.get("funding_cost", 0),
            stats.get("risk_per_trade", 0),
            stats.get("max_leverage", 0),
        )
        try:
            with self._lock:
                self.conn.execute(
                    """INSERT INTO heartbeat_history
                       (ts, instance, kline_closes, orders_placed, orders_failed,
                        server_time_offset, modules, ws_connected, ws_total, funding_cost,
                        risk_per_trade, max_leverage)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(ts) DO UPDATE SET
                         kline_closes=excluded.kline_closes,
                         orders_placed=excluded.orders_placed,
                         orders_failed=excluded.orders_failed,
                         server_time_offset=excluded.server_time_offset,
                         modules=excluded.modules,
                         ws_connected=excluded.ws_connected,
                         ws_total=excluded.ws_total,
                         funding_cost=excluded.funding_cost,
                         risk_per_trade=excluded.risk_per_trade,
                         max_leverage=excluded.max_leverage""",
                    row)
                self.conn.commit()
        except Exception as e:
            logger.debug("OpsArchive heartbeat insert failed: %s", e)

    def on_position(self, event):
        """position.changed: equity → 权益曲线; close → 平仓交易明细。"""
        data = event.data or {}
        ts = self._event_ts(event)
        try:
            with self._lock:
                if data.get("event") == "equity":
                    self.conn.execute(
                        """INSERT OR REPLACE INTO equity_history
                           (ts, instance, total_equity, available_balance,
                            margin_ratio, daily_pnl, drawdown)
                           VALUES (?,?,?,?,?,?,?)""",
                        (ts, data.get("instance", ""),
                         data.get("total_equity", 0), data.get("available_balance", 0),
                         data.get("margin_ratio", 0), data.get("daily_pnl", 0),
                         data.get("drawdown", 0)))
                elif data.get("event") == "close":
                    self.conn.execute(
                        """INSERT OR REPLACE INTO trade_history
                           (ts, symbol, direction, entry_price, exit_price, quantity,
                            gross_pnl, fee, realized_pnl)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (ts, data.get("symbol", ""), data.get("direction", ""),
                         data.get("entry_price", 0), data.get("exit_price", 0),
                         data.get("quantity", 0), data.get("gross_pnl", 0),
                         data.get("fee", 0), data.get("realized_pnl", 0)))
                self.conn.commit()
        except Exception as e:
            logger.debug("OpsArchive position insert failed: %s", e)

    def on_alert(self, event):
        data = event.data or {}
        ts = self._event_ts(event)
        try:
            with self._lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO alert_history (ts, source, message) VALUES (?,?,?)",
                    (ts, data.get("source", ""), str(data.get("message", ""))[:500]))
                self.conn.commit()
        except Exception as e:
            logger.debug("OpsArchive alert insert failed: %s", e)

    def on_lifecycle(self, event):
        data = event.data or {}
        ts = self._event_ts(event)
        try:
            with self._lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO lifecycle_history (ts, event, pid, instance) "
                    "VALUES (?,?,?,?)",
                    (ts, data.get("event", ""), int(data.get("pid", 0) or 0),
                     data.get("instance", "")))
                self.conn.commit()
        except Exception as e:
            logger.debug("OpsArchive lifecycle insert failed: %s", e)

    def on_command(self, event):
        data = event.data or {}
        ts = self._event_ts(event)
        try:
            with self._lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO command_history (ts, source, command, symbol) "
                    "VALUES (?,?,?,?)",
                    (ts, data.get("source", ""), data.get("command", ""),
                     data.get("symbol", "")),
                )
                self.conn.commit()
        except Exception as e:
            logger.debug("OpsArchive command insert failed: %s", e)

    # ─── 查询 ───

    def history(self, hours: int = 24, max_points: int = 300) -> List[dict]:
        since = time.time() - hours * 3600
        with self._lock:
            rows = self.conn.execute(
                """SELECT ts, kline_closes, orders_placed, orders_failed,
                          server_time_offset, modules, ws_connected, ws_total,
                          funding_cost
                   FROM heartbeat_history WHERE ts >= ? ORDER BY ts ASC""",
                (since,),
            ).fetchall()
        result = [{
            "ts": r["ts"],
            "kline_closes": r["kline_closes"],
            "orders_placed": r["orders_placed"],
            "orders_failed": r["orders_failed"],
            "server_time_offset": r["server_time_offset"],
            "ws_connected": r["ws_connected"],
            "ws_total": r["ws_total"],
            "funding_cost": r["funding_cost"],
            "modules": json.loads(r["modules"] or "{}"),
        } for r in rows]
        # 降采样到 max_points (等距抽稀)
        if len(result) > max_points:
            step = len(result) / max_points
            result = [result[int(i * step)] for i in range(max_points)]
        return result

    def equity(self, hours: int = 24, max_points: int = 300) -> List[dict]:
        since = time.time() - hours * 3600
        with self._lock:
            rows = self.conn.execute(
                """SELECT ts, total_equity, available_balance, margin_ratio,
                          daily_pnl, drawdown
                   FROM equity_history WHERE ts >= ? ORDER BY ts ASC""",
                (since,)).fetchall()
        result = [dict(r) for r in rows]
        if len(result) > max_points:
            step = len(result) / max_points
            result = [result[int(i * step)] for i in range(max_points)]
        return result

    def trades(self, limit: int = 100) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM trade_history ORDER BY ts DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]

    def alerts(self, limit: int = 100) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM alert_history ORDER BY ts DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]

    def restarts(self, limit: int = 50) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM lifecycle_history ORDER BY ts DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]

    def latest(self) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM heartbeat_history ORDER BY ts DESC LIMIT 1").fetchone()
        if not row:
            return None
        return {
            "ts": row["ts"],
            "instance": row["instance"],
            "kline_closes": row["kline_closes"],
            "orders_placed": row["orders_placed"],
            "orders_failed": row["orders_failed"],
            "server_time_offset": row["server_time_offset"],
            "ws_connected": row["ws_connected"],
            "ws_total": row["ws_total"],
            "funding_cost": row["funding_cost"],
            "risk_per_trade": row["risk_per_trade"],
            "max_leverage": row["max_leverage"],
            "modules": json.loads(row["modules"] or "{}"),
        }

    def commands(self, limit: int = 100) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM command_history ORDER BY ts DESC LIMIT ?",
                (limit,)).fetchall()
        return [{"ts": r["ts"], "source": r["source"],
                 "command": r["command"], "symbol": r["symbol"]} for r in rows]

    # ─── 生命周期 ───

    def start(self, event_bus):
        """启动消费线程 (heartbeat/command/position/alert/lifecycle)。event_bus 为 None 时跳过。"""
        if event_bus is None:
            logger.warning("OpsArchive: 无 EventBus, 历史归档停用")
            return
        for stream, group, handler in (
            ("heartbeat", "ops-archive", self.on_heartbeat),
            ("command", "ops-archive-command", self.on_command),
            ("position.changed", "ops-archive-position", self.on_position),
            ("alert", "ops-archive-alert", self.on_alert),
            ("lifecycle", "ops-archive-lifecycle", self.on_lifecycle),
        ):
            t = threading.Thread(
                target=event_bus.run_consumer,
                args=(stream, group, handler, 10, 200),
                daemon=True, name=f"ops-archive-{stream}",
            )
            t.start()
            self._threads.append(t)
        # 2026-08-16 审计: 补上 docstring 承诺的周期性 prune (此前仅启动时一次)
        def _prune_loop():
            while not self._stop.is_set():
                self._stop.wait(timeout=6 * 3600)
                if not self._stop.is_set():
                    try:
                        self.prune()
                    except Exception as e:
                        logger.warning("OpsArchive prune failed: %s", e)
        prune_t = threading.Thread(target=_prune_loop, daemon=True,
                                   name="ops-archive-prune")
        prune_t.start()
        self._threads.append(prune_t)
        logger.info("OpsArchive consuming heartbeat/command/position/alert/lifecycle streams")

    def close(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)
        try:
            self.conn.close()
        except Exception:
            pass
