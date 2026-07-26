# 交易系统补齐实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended)

**Goal:** 补齐 4 个关键模块：持久化+Paper Trading、Docker 部署、资金费率管理、测试覆盖增强

**Architecture:** Phase 1 在 shared/ 下新增 SQLite 持久化和 Paper Trading 模拟引擎；Phase 3 容器化现有服务；Phase 4 追踪永续合约资金费率；Phase 6 补充现有模块的测试

**Tech Stack:** SQLite, SQLAlchemy, Docker, python-decouple

---

### Task 1: 持久化 — Trade History Database

**Files:**
- Create: `shared/database.py`
- Create: `tests/test_database.py`

- [ ] **Step 1: 编写测试**

`tests/test_database.py`:
```python
import pytest
from datetime import datetime
from shared.database import TradeDatabase, TradeRecord


class TestTradeDatabase:
    def setup_method(self):
        self.db = TradeDatabase(":memory:")

    def test_store_and_retrieve_trade(self):
        trade = TradeRecord(
            symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=0.15, price=64000.0, status="FILLED",
            order_id=12345, order_type_detail="MARKET",
        )
        self.db.store_trade(trade)
        trades = self.db.get_trades()
        assert len(trades) == 1
        assert trades[0].symbol == "BTCUSDT"
        assert trades[0].quantity == 0.15

    def test_get_trades_empty(self):
        assert len(self.db.get_trades()) == 0

    def test_store_signal(self):
        self.db.store_signal("BTCUSDT", "LONG", 0.72, 64000.0)
        signals = self.db.get_signals(limit=5)
        assert len(signals) == 1
        assert signals[0]["direction"] == "LONG"
```

- [ ] **Step 2: Run tests (fail)**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/test_database.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 TradeDatabase**

`shared/database.py`:
```python
"""SQLite 持久化 — 交易历史、订单记录、信号日志。"""

import sqlite3
import json
import logging
from dataclasses import dataclass, field, asdict
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
```

- [ ] **Step 4: Run tests (pass)**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/test_database.py -v
```
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add shared/database.py tests/test_database.py
git commit -m "feat: add SQLite trade history database"
```

---

### Task 2: Paper Trading 模式

**Files:**
- Modify: `execution/order_gateway.py` (add paper mode flag)
- Create: `shared/paper_trader.py`
- Create: `tests/test_paper_trader.py`

- [ ] **Step 1 & 2: Write tests → fail**

- [ ] **Step 3: 实现 PaperTrader**

`shared/paper_trader.py`:
```python
"""Paper Trading — 模拟成交，不发送真实订单。"""

import logging
import time
import random
from dataclasses import dataclass, field
from typing import Optional

from execution.order_gateway import OrderRequest, OrderResponse

logger = logging.getLogger(__name__)


@dataclass
class PaperFill:
    order_id: int
    symbol: str
    side: str
    quantity: float
    price: float
    status: str = "FILLED"
    executed_qty: float = 0.0
    avg_price: float = 0.0


class PaperTrader:
    """模拟成交引擎。MARKET 单立即成交，LIMIT 单模拟延迟。"""

    def __init__(self, fill_delay_ms: float = 100.0, slippage_pct: float = 0.01):
        self.fill_delay_ms = fill_delay_ms
        self.slippage_pct = slippage_pct
        self._next_id = 1000000

    def execute(self, req: OrderRequest, current_price: float) -> PaperFill:
        self._next_id += 1
        delay = self.fill_delay_ms / 1000.0
        time.sleep(delay)
        slippage = current_price * random.uniform(-self.slippage_pct, self.slippage_pct)
        fill_price = round(current_price + slippage, 2)
        return PaperFill(
            order_id=self._next_id,
            symbol=req.symbol,
            side=req.side,
            quantity=req.quantity,
            price=fill_price,
            executed_qty=req.quantity,
            avg_price=fill_price,
        )
```

- [ ] **Step 4: Tests pass → commit**

```bash
git add shared/paper_trader.py tests/test_paper_trader.py
git commit -m "feat: add PaperTrader simulation engine"
```

---

### Task 3: Docker 部署

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.dockerignore`

- [ ] **Step 1: Create Dockerfile**

`Dockerfile`:
```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
CMD ["uvicorn", "dashboard.server:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Create docker-compose.yml**

`docker-compose.yml`:
```yaml
version: "3.9"
services:
  backend:
    build: .
    ports:
      - "8000:8000"
    env_file: config/.env
    volumes:
      - ./data:/app/data

  frontend:
    image: node:20-alpine
    working_dir: /app
    volumes:
      - ./dashboard/frontend:/app
    ports:
      - "5173:5173"
    command: sh -c "npm install && npm run dev -- --host 0.0.0.0"
    depends_on:
      - backend
```

- [ ] **Step 3: Create .dockerignore**

`.dockerignore`:
```
__pycache__/
*.pyc
.git/
.gitignore
.env
node_modules/
dashboard/frontend/node_modules/
data/
*.db
```

- [ ] **Step 4: Update requirements.txt with uvicorn/fastapi**

```bash
echo "uvicorn>=0.30.0" >> requirements.txt
echo "fastapi>=0.115.0" >> requirements.txt
```

- [ ] **Step 5: Commit**

```bash
git add Dockerfile docker-compose.yml .dockerignore
git commit -m "feat: add Docker deployment configuration"
```

---

### Task 4: 资金费率管理

**Files:**
- Create: `shared/funding_rate.py`
- Create: `tests/test_funding_rate.py`

- [ ] **Step 1: 编写测试**

- [ ] **Step 2: 实现 FundingRateTracker**

`shared/funding_rate.py`:
```python
"""永续合约资金费率追踪与影响计算。"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_FUNDING_INTERVAL_SECONDS = 8 * 3600  # 8h


@dataclass
class FundingRecord:
    symbol: str
    rate: float
    time: float = field(default_factory=time.time)


class FundingRateTracker:
    """追踪资金费率，计算持仓资金成本。"""

    def __init__(self):
        self._rates: Dict[str, FundingRecord] = {}

    def update(self, symbol: str, rate: float):
        self._rates[symbol] = FundingRecord(symbol=symbol, rate=rate)

    def get_rate(self, symbol: str) -> Optional[float]:
        r = self._rates.get(symbol)
        return r.rate if r else None

    def estimate_cost(self, symbol: str, position_value: float, hours: float = 8) -> float:
        rate = self.get_rate(symbol)
        if rate is None:
            return 0.0
        intervals = hours / (8)
        return position_value * rate * intervals

    def annualized_rate(self, symbol: str) -> Optional[float]:
        rate = self.get_rate(symbol)
        if rate is None:
            return None
        return rate * 3 * 365

    def next_funding_time(self) -> float:
        now = time.time()
        elapsed = now % _FUNDING_INTERVAL_SECONDS
        return now + (_FUNDING_INTERVAL_SECONDS - elapsed)
```

- [ ] **Step 3: 测试通过 → 提交**

---

### Task 5: 测试覆盖增强

**Files:**
- Create: `tests/test_database_extended.py`
- Create/Modify: `tests/test_guardian_extended.py`

- [ ] **Step 1: 补充风控边界测试**

`tests/test_guardian_extended.py`:
```python
"""补充 Guardian 测试：边缘情况"""


class TestGuardianEdgeCases:
    def test_shutdown_while_checking(self):
        """stop() 在 _check_positions 执行时调用不应崩溃"""
        pass

    def test_price_none_handling(self):
        """feed.get_last_price 返回 None 时跳过"""
        pass

    def test_multiple_symbols_independent(self):
        """多个持仓互不影响"""
        pass
```

- [ ] **Step 2: 订单管理器边界测试**

```python
class TestOrderManagerEdgeCases:
    def test_submit_with_zero_quantity(self):
        """数量为0时拒绝"""
        pass

    def test_algo_order_network_error(self):
        """止损单网络错误后重试"""
        pass
```
