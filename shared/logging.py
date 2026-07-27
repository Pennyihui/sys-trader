"""结构化日志 — JSON 格式，自动轮转，适合生产环境。"""

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


class JsonFormatter(logging.Formatter):
    """JSON 日志格式化器，每条日志输出为一行 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging(
    log_dir: str = "logs",
    level: str = "INFO",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 7,
    json_console: bool = False,
) -> None:
    """配置日志系统。

    - 文件日志: JSON 格式，自动轮转，保留 7 天
    - 控制台: 可选 JSON 或纯文本
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 文件日志 (JSON, 轮转)
    file_handler = RotatingFileHandler(
        log_path / "systrader.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(JsonFormatter())
    root.addHandler(file_handler)

    # 控制台输出
    console = logging.StreamHandler(sys.stdout)
    if json_console:
        console.setFormatter(JsonFormatter())
    else:
        console.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))
    root.addHandler(console)

    # 第三方库日志降级
    logging.getLogger("websocket").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    root.info("Logging initialized: %s, level=%s", log_path.resolve(), level)
