"""KlineArchive — 闭合 K 线持久化归档 (2026-08-16 P2-1)。

feed 每次闭合 candle 时 upsert 到 SQLite (data/kline.db),
重启免回填、为将来回测/分析供数。仅归档已闭合 K 线。
"""

import logging
import os
import sqlite3
import threading
import time
from typing import Optional

from market_data.kline_buffer import Kline

logger = logging.getLogger(__name__)


class KlineArchive:
    def __init__(self, db_path: str = "data/kline.db",
                 retention_days: Optional[int] = None):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS klines (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL,
                open REAL NOT NULL, high REAL NOT NULL,
                low REAL NOT NULL, close REAL NOT NULL,
                volume REAL NOT NULL,
                PRIMARY KEY (symbol, timeframe, open_time)
            )
        """)
        self.conn.commit()
        # 2026-08-16 审计: 归档长期无保留策略会无限膨胀, 启动时清理旧数据
        if retention_days is None:
            try:
                retention_days = int(os.environ.get("KLINE_ARCHIVE_DAYS", "90"))
            except ValueError:
                retention_days = 90
        if retention_days > 0:
            self._prune(retention_days)

    def _prune(self, days: int):
        cutoff = int(time.time() * 1000) - days * 86_400_000
        with self._lock:
            n = self.conn.execute(
                "DELETE FROM klines WHERE open_time < ?", (cutoff,)).rowcount
            self.conn.commit()
        if n:
            logger.info("KlineArchive pruned %d rows older than %d days", n, days)

    def upsert(self, kline: Kline):
        if not kline.is_closed:
            return
        with self._lock:
            try:
                self.conn.execute(
                    """INSERT INTO klines (symbol, timeframe, open_time, close_time,
                       open, high, low, close, volume)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(symbol, timeframe, open_time)
                       DO UPDATE SET close_time=excluded.close_time,
                                     open=excluded.open, high=excluded.high,
                                     low=excluded.low, close=excluded.close,
                                     volume=excluded.volume""",
                    (kline.symbol, kline.timeframe, kline.open_time, kline.close_time,
                     kline.open, kline.high, kline.low, kline.close, kline.volume),
                )
                self.conn.commit()
            except Exception as e:
                logger.debug("KlineArchive upsert failed: %s", e)

    def count(self, symbol: Optional[str] = None, timeframe: Optional[str] = None) -> int:
        with self._lock:
            if symbol and timeframe:
                row = self.conn.execute(
                    "SELECT COUNT(*) FROM klines WHERE symbol=? AND timeframe=?",
                    (symbol, timeframe)).fetchone()
            elif symbol:
                row = self.conn.execute(
                    "SELECT COUNT(*) FROM klines WHERE symbol=?", (symbol,)).fetchone()
            else:
                row = self.conn.execute("SELECT COUNT(*) FROM klines").fetchone()
            return int(row[0])

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
