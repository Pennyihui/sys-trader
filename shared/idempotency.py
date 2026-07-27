"""订单幂等性 — clientOrderId 去重，防止重启重复下单。"""

import uuid
import time
import sqlite3
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

logger = logging.getLogger(__name__)


class IntentStatus(str, Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    FAILED = "FAILED"


@dataclass
class OrderIntent:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""
    side: str = ""
    order_type: str = ""
    quantity: float = 0.0
    price: float = 0.0
    client_order_id: str = ""
    status: str = IntentStatus.PENDING
    exchange_order_id: str = ""
    error: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class IdempotencyTracker:
    def __init__(self, db_path: str = "data/intents.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS order_intents (
                id TEXT PRIMARY KEY,
                symbol TEXT, side TEXT, order_type TEXT,
                quantity REAL, price REAL,
                client_order_id TEXT,
                status TEXT DEFAULT 'PENDING',
                exchange_order_id TEXT DEFAULT '',
                error TEXT DEFAULT '',
                created_at TEXT
            )
        """)
        self.conn.commit()

    def create_intent(self, symbol: str, side: str, order_type: str,
                      quantity: float, price: float = 0.0) -> OrderIntent:
        intent = OrderIntent(
            symbol=symbol, side=side, order_type=order_type,
            quantity=quantity, price=price,
            client_order_id=f"sys_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}",
        )
        self.conn.execute(
            "INSERT INTO order_intents VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (intent.id, intent.symbol, intent.side, intent.order_type,
             intent.quantity, intent.price, intent.client_order_id,
             intent.status, intent.exchange_order_id, intent.error, intent.created_at),
        )
        self.conn.commit()
        return intent

    def update_status(self, intent_id: str, status: str, exchange_order_id: str = "", error: str = ""):
        self.conn.execute(
            "UPDATE order_intents SET status=?, exchange_order_id=?, error=? WHERE id=?",
            (status, exchange_order_id, error, intent_id),
        )
        self.conn.commit()

    def get_pending_intents(self) -> List[OrderIntent]:
        rows = self.conn.execute(
            "SELECT * FROM order_intents WHERE status='PENDING' OR status='SUBMITTED'"
        ).fetchall()
        return [OrderIntent(**dict(r)) for r in rows]

    def close(self):
        self.conn.close()
