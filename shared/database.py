"""SQLite 持久化 — 交易历史、订单记录、信号日志。"""

import sqlite3
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
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
        """)
        self.conn.commit()

    def store_trade(self, trade: TradeRecord) -> int:
        cursor = self.conn.execute(
            "INSERT INTO trades (timestamp, symbol, side, order_type, quantity, price, status, order_id, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (trade.timestamp, trade.symbol, trade.side, trade.order_type,
             trade.quantity, trade.price, trade.status, trade.order_id, trade.error),
        )
        self.conn.commit()
        return cursor.lastrowid

    def store_signal(self, symbol: str, direction: str, conviction: float, price: float, metadata: Optional[Dict] = None):
        self.conn.execute(
            "INSERT INTO signals (timestamp, symbol, direction, conviction, price, metadata) VALUES (?, ?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), symbol, direction, conviction, price, json.dumps(metadata or {})),
        )
        self.conn.commit()

    def get_trades(self, limit: int = 50) -> List[TradeRecord]:
        rows = self.conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [TradeRecord(**dict(r)) for r in rows]

    def get_signals(self, limit: int = 20) -> List[Dict]:
        rows = self.conn.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def create_intent(self, symbol, side, order_type, quantity, price=0.0) -> dict:
        """创建订单 intent，返回 {id, client_order_id, ...}"""
        import uuid, time
        intent_id = str(uuid.uuid4())
        client_order_id = f"sys_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        self.conn.execute(
            "INSERT INTO order_intents (id, symbol, side, order_type, quantity, price, client_order_id, status, exchange_order_id, error, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (intent_id, symbol, side, order_type, quantity, price, client_order_id,
             "PENDING", "", "", datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()
        return {"id": intent_id, "client_order_id": client_order_id, "status": "PENDING"}

    def update_intent_status(self, intent_id, status, exchange_order_id="", error=""):
        self.conn.execute(
            "UPDATE order_intents SET status=?, exchange_order_id=?, error=? WHERE id=?",
            (status, exchange_order_id, error, intent_id),
        )
        self.conn.commit()

    def get_pending_intents(self, limit=50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM order_intents WHERE status='PENDING' OR status='SUBMITTED' ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()
