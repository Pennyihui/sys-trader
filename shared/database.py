"""SQLite 持久化 — 交易历史、订单记录、信号日志。"""

import sqlite3
import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: float
    status: str
    id: int = 0
    order_id: int = 0
    order_type_detail: str = ""
    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class TradeDatabase:
    def __init__(self, db_path: str = "data/trades.db"):
        # 2026-08-16 审计修复: 主循环线程(下单前 _persist_submit) 与 user stream
        # 线程(_persist_result) 都会写库, 默认 check_same_thread=True 会在首次
        # 真实成交时抛 ProgrammingError — 下单前抛=信号静默丢失, 下单后抛=裸仓。
        # check_same_thread=False + 全局锁串行化所有读写。
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._create_tables()

    def _execute(self, sql: str, params=()):
        with self._lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur

    def _query(self, sql: str, params=()):
        with self._lock:
            return self.conn.execute(sql, params).fetchall()

    def _create_tables(self):
        with self._lock:
            self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                status TEXT NOT NULL,
                order_id INTEGER DEFAULT 0,
                error TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                conviction REAL NOT NULL,
                price REAL NOT NULL,
                metadata TEXT DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS order_intents (
                id TEXT PRIMARY KEY,
                symbol TEXT, side TEXT, order_type TEXT,
                quantity REAL, price REAL,
                client_order_id TEXT,
                status TEXT DEFAULT 'PENDING',
                exchange_order_id TEXT DEFAULT '',
                error TEXT DEFAULT '',
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                status TEXT DEFAULT 'CREATED',
                exchange_order_id TEXT DEFAULT '',
                filled_qty REAL DEFAULT 0,
                avg_price REAL DEFAULT 0,
                fee REAL DEFAULT 0,
                error TEXT DEFAULT ''
            );
        """)
            self.conn.commit()

    def store_trade(self, trade: TradeRecord) -> int:
        return self._execute(
            "INSERT INTO trades (timestamp, symbol, side, order_type, quantity, price, status, order_id, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (trade.timestamp, trade.symbol, trade.side, trade.order_type,
             trade.quantity, trade.price, trade.status, trade.order_id, trade.error),
        ).lastrowid

    def store_signal(self, symbol: str, direction: str, conviction: float, price: float, metadata: Optional[Dict] = None):
        self._execute(
            "INSERT INTO signals (timestamp, symbol, direction, conviction, price, metadata) VALUES (?, ?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), symbol, direction, conviction, price, json.dumps(metadata or {})),
        )

    def get_trades(self, limit: int = 50) -> List[TradeRecord]:
        rows = self._query("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        return [TradeRecord(**dict(r)) for r in rows]

    def get_signals(self, limit: int = 20) -> List[Dict]:
        rows = self._query("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]

    def create_intent(self, symbol, side, order_type, quantity, price=0.0) -> dict:
        """创建订单 intent，返回 {id, client_order_id, ...}"""
        import uuid, time
        intent_id = str(uuid.uuid4())
        client_order_id = f"sys_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        self._execute(
            "INSERT INTO order_intents (id, symbol, side, order_type, quantity, price, client_order_id, status, exchange_order_id, error, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (intent_id, symbol, side, order_type, quantity, price, client_order_id,
             "PENDING", "", "", datetime.now(timezone.utc).isoformat()),
        )
        return {"id": intent_id, "client_order_id": client_order_id, "status": "PENDING"}

    def update_intent_status(self, intent_id, status, exchange_order_id="", error=""):
        self._execute(
            "UPDATE order_intents SET status=?, exchange_order_id=?, error=? WHERE id=?",
            (status, exchange_order_id, error, intent_id),
        )

    def get_pending_intents(self, limit=50) -> list[dict]:
        rows = self._query(
            "SELECT * FROM order_intents WHERE status='PENDING' OR status='SUBMITTED' ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in rows]

    def create_order(self, symbol, side, order_type, quantity, price=0.0) -> int:
        """创建订单记录，返回 order id。"""
        self._execute(
            "INSERT INTO orders (created_at, symbol, side, order_type, quantity, price) VALUES (?,?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(), symbol, side, order_type, quantity, price),
        )
        return self._query("SELECT last_insert_rowid()")[0][0]

    def update_order_status(self, order_id, status, exchange_order_id="", filled_qty=0, avg_price=0, fee=0, error=""):
        """更新订单状态。"""
        self._execute(
            "UPDATE orders SET status=?, exchange_order_id=?, filled_qty=?, avg_price=?, fee=?, error=? WHERE id=?",
            (status, exchange_order_id, filled_qty, avg_price, fee, error, order_id),
        )

    def get_orders(self, limit=50) -> list[dict]:
        """查询订单记录。"""
        rows = self._query("SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]

    def purge_orders(self, days: int = 90) -> int:
        """删除 N 天前的订单记录 (2026-08-16 P1-6 保留策略), 返回删除行数。"""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        n = self._execute(
            "DELETE FROM orders WHERE created_at < ?", (cutoff,)).rowcount
        if n:
            logger.info("Purged %d orders older than %d days", n, days)
        return n

    def purge_signals(self, days: int = 30) -> int:
        """删除 N 天前的信号记录, 返回删除行数。"""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        n = self._execute(
            "DELETE FROM signals WHERE timestamp < ?", (cutoff,)).rowcount
        if n:
            logger.info("Purged %d signals older than %d days", n, days)
        return n

    def get_order_by_exchange_id(self, exchange_order_id) -> Optional[dict]:
        """按交易所订单 ID 查询（用于对账）。"""
        rows = self._query(
            "SELECT * FROM orders WHERE exchange_order_id=? ORDER BY id DESC LIMIT 1",
            (exchange_order_id,),
        )
        return dict(rows[0]) if rows else None

    def close(self):
        with self._lock:
            self.conn.close()
