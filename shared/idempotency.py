"""订单幂等性 — 复用 TradeDatabase 的 order_intents 表。"""

import logging
from typing import Dict, List, Optional

from shared.database import TradeDatabase

logger = logging.getLogger(__name__)


class IdempotencyTracker:
    """订单幂等性追踪 — 薄封装 TradeDatabase 的 order_intents 表。"""

    def __init__(self, db: Optional[TradeDatabase] = None, db_path: str = "data/trades.db"):
        self.db = db or TradeDatabase(db_path)

    def create_intent(self, symbol, side, order_type, quantity, price=0.0) -> dict:
        return self.db.create_intent(symbol, side, order_type, quantity, price)

    def update_status(self, intent_id, status, exchange_order_id="", error=""):
        self.db.update_intent_status(intent_id, status, exchange_order_id, error)

    def get_pending_intents(self, limit=50) -> List[Dict]:
        return self.db.get_pending_intents(limit)

    def close(self):
        self.db.close()
