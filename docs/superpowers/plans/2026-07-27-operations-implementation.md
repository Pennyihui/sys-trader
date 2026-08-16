# 运维系统实现计划

**Goal:** 完整运维体系：幂等性、启动防护、启动前校验、持续对账、优雅关闭

**Architecture:** intent 表追踪订单生命周期，preflight 启动校验，定时对账循环

---

### Task 1: 幂等性 — Idempotency

**Files:**
- Create: `shared/idempotency.py`
- Create: `tests/test_idempotency.py`

- [ ] **Write test and implement**

`shared/idempotency.py`:
```python
"""订单幂等性 — clientOrderId 去重，防止重启重复下单。"""

import uuid
import time
import sqlite3
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

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
    """追踪订单 intent，确保每笔订单只执行一次。"""

    def __init__(self, db_path: str = "data/intents.db"):
        self.conn = sqlite3.connect(db_path)
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

    def get_pending_intents(self) -> list[OrderIntent]:
        rows = self.conn.execute(
            "SELECT * FROM order_intents WHERE status='PENDING' OR status='SUBMITTED'"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d.pop("id", None)
            result.append(OrderIntent(*r))
        return result
```

- [ ] **Test and commit**

### Task 2: 启动前校验 — Preflight

**Files:**
- Create: `shared/preflight.py`
- Create: `tests/test_preflight.py`

### Task 3: 持续对账 — Reconciler

**Files:**
- Create: `shared/reconciler.py`
- Create: `tests/test_reconciler.py`

### Task 4: 重写 Runner

**Files:**
- Modify: `shared/runner.py`
- Create: `tests/test_runner.py`

### Task 5: Logging 测试

**Files:**
- Create: `tests/test_logging.py`
