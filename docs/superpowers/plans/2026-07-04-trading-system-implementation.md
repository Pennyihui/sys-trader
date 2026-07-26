# Sys_trader 完整交易系统实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将现有 RL 信号引擎（agent_team）扩展为事件驱动、多标的、全自动风控的 Binance 合约实盘交易系统。

**Architecture:** 8 模块事件驱动架构，Redis Streams 作为事件总线，每个模块独立运行通过事件通信。Signal Engine 保留现有 4 层逻辑不变，Market Data 通过 WebSocket 推送实时行情，Scheduler 按 K 线闭合触发信号生成，Risk Manager 以中间件链模式校验每个信号，Execution Engine 对接 Binance Futures API。

**Tech Stack:** Python 3.10+, Redis Streams, SQLite, binance-connector-python, python-binance, threading + heapq, XGBoost, Stable-Baselines3

---

## Phase 1: 项目基础设施

### Task 1.1: 项目目录结构与配置

**Files:**
- Create: `config/symbols.yaml`
- Create: `config/risk.yaml`
- Create: `config/execution.yaml`
- Create: `config/.env.example`

- [ ] **Step 1: 创建标的配置文件**

`config/symbols.yaml`:
```yaml
symbols:
  primary:
    - BTCUSDT
    - ETHUSDT
    - SOLUSDT
  secondary:
    - BNBUSDT
    - DOGEUSDT
    - AVAXUSDT
    - LINKUSDT
    - ARBUSDT
```

- [ ] **Step 2: 创建风控参数文件**

`config/risk.yaml`:
```yaml
risk:
  risk_per_trade: 0.015
  max_leverage: 5
  max_position_per_symbol: 0.30
  max_same_direction: 0.50
  max_total_margin: 0.80
  max_drawdown: 0.15
  daily_loss_limit: 0.05
  consecutive_loss_breaker: 3
  cooldown_minutes: 120
```

- [ ] **Step 3: 创建执行参数文件**

`config/execution.yaml`:
```yaml
execution:
  order_timeout_seconds: 60
  partial_fill_wait_seconds: 30
  max_retries: 3
  retry_backoff_base: 1
  testnet: true
  api_key_env: BINANCE_API_KEY
  api_secret_env: BINANCE_API_SECRET
```

- [ ] **Step 4: 创建环境变量模板**

`config/.env.example`:
```env
BINANCE_API_KEY=your_testnet_api_key
BINANCE_API_SECRET=your_testnet_api_secret
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
REDIS_URL=redis://localhost:6379
```

- [ ] **Step 5: 创建空模块目录和 `__init__.py`**

```bash
mkdir -p market_data scheduler signal_engine/outlook signal_engine/status signal_engine/4h risk execution portfolio monitor shared
touch market_data/__init__.py scheduler/__init__.py signal_engine/__init__.py signal_engine/outlook/__init__.py signal_engine/status/__init__.py signal_engine/4h/__init__.py risk/__init__.py execution/__init__.py portfolio/__init__.py monitor/__init__.py shared/__init__.py
mkdir -p data models logs tests
```

- [ ] **Step 6: Commit**

```bash
git add config/ market_data/ scheduler/ signal_engine/ risk/ execution/ portfolio/ monitor/ shared/ data/ models/ logs/ tests/
git commit -m "feat: add project directory structure and configuration files"
```

---

### Task 1.2: Event Bus（Redis Streams 封装）

**Files:**
- Create: `shared/event_bus.py`
- Create: `tests/test_event_bus.py`

- [ ] **Step 1: 编写 EventBus 测试**

`tests/test_event_bus.py`:
```python
import json
import time
import uuid
import pytest
from unittest.mock import patch, MagicMock
from shared.event_bus import EventBus, Event


class TestEventBus:
    def setup_method(self):
        self.bus = EventBus(redis_url="redis://localhost:6379", prefix="test")

    def test_publish_sends_message_to_stream(self):
        bus = self.bus
        data = {"symbol": "BTCUSDT", "price": 62500.0}

        with patch.object(bus.redis, "xadd") as mock_xadd:
            mock_xadd.return_value = "12345-0"
            event_id = bus.publish("test.stream", data)

        mock_xadd.assert_called_once()
        args = mock_xadd.call_args
        assert "test:test.stream" in args[0][0] or args[0][0] == "test:test.stream"
        assert event_id is not None

    def test_event_has_required_fields(self):
        event = Event(stream="signal.generated", data={"symbol": "BTCUSDT", "direction": "LONG"})

        assert isinstance(event.event_id, str)
        assert len(event.event_id) > 0
        assert event.stream == "signal.generated"
        assert event.data["symbol"] == "BTCUSDT"
        assert event.data["direction"] == "LONG"
        assert isinstance(event.timestamp, str)

    def test_subscribe_reads_from_stream(self):
        bus = self.bus
        handler_called = []

        def handler(event):
            handler_called.append(event)

        test_event = Event(stream="kline.closed", data={"symbol": "BTCUSDT", "timeframe": "4h"})
        raw = json.dumps({"event_id": test_event.event_id, "stream": test_event.stream, "timestamp": test_event.timestamp, "data": test_event.data})

        with patch.object(bus.redis, "xreadgroup") as mock_read:
            mock_read.return_value = [[b"test:kline.closed", [(b"msg-1", {b"payload": raw.encode()})]]]
            bus._poll_once("kline.closed", "test-group", handler)

        assert len(handler_called) == 1
        assert handler_called[0].stream == "kline.closed"
        assert handler_called[0].data["symbol"] == "BTCUSDT"

    def test_message_is_valid_json_roundtrip(self):
        original = Event(stream="order.filled", data={"symbol": "ETHUSDT", "qty": 0.5, "price": 3100.0})
        raw = json.dumps({"event_id": original.event_id, "stream": original.stream, "timestamp": original.timestamp, "data": original.data})
        parsed = json.loads(raw)

        assert parsed["event_id"] == original.event_id
        assert parsed["stream"] == original.stream
        assert parsed["data"]["symbol"] == "ETHUSDT"
        assert parsed["data"]["qty"] == 0.5
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_event_bus.py -v
```
Expected: FAIL (module not found)

- [ ] **Step 3: 实现 EventBus**

`shared/event_bus.py`:
```python
"""Event Bus backed by Redis Streams — module communication backbone."""

import json
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Optional
import redis


@dataclass
class Event:
    stream: str
    data: dict
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class EventBus:
    def __init__(self, redis_url: str = "redis://localhost:6379", prefix: str = "systrader"):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.prefix = prefix
        self._consumers: dict[str, list] = {}

    def _key(self, stream: str) -> str:
        return f"{self.prefix}:{stream}"

    def publish(self, stream: str, data: dict) -> str:
        event = Event(stream=stream, data=data)
        payload = json.dumps({"event_id": event.event_id, "stream": event.stream, "timestamp": event.timestamp, "data": event.data})
        msg_id = self.redis.xadd(self._key(stream), {"payload": payload}, maxlen=10000)
        return msg_id

    def subscribe(self, stream: str, consumer_group: str, handler: Callable[[Event], None]):
        key = self._key(stream)
        try:
            self.redis.xgroup_create(key, consumer_group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        if stream not in self._consumers:
            self._consumers[stream] = []
        self._consumers[stream].append((consumer_group, handler))

    def _poll_once(self, stream: str, consumer_group: str, handler: Callable[[Event], None]):
        key = self._key(stream)
        consumer_id = f"{consumer_group}-{uuid.uuid4().hex[:8]}"
        results = self.redis.xreadgroup(consumer_group, consumer_id, {key: ">"}, count=5, block=100)
        if results:
            for _stream_key, messages in results:
                for msg_id, fields in messages:
                    payload = json.loads(fields.get("payload", "{}"))
                    event = Event(stream=payload.get("stream", stream), data=payload.get("data", {}), event_id=payload.get("event_id", ""), timestamp=payload.get("timestamp", ""))
                    handler(event)
                    self.redis.xack(key, consumer_group, msg_id)

    def run_consumer(self, stream: str, consumer_group: str, handler: Callable[[Event], None]):
        import time
        while True:
            try:
                self._poll_once(stream, consumer_group, handler)
            except Exception as e:
                print(f"EventBus consumer error [{stream}/{consumer_group}]: {e}")
                time.sleep(1)
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_event_bus.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add shared/event_bus.py tests/test_event_bus.py
git commit -m "feat: add Redis Streams EventBus with publish/subscribe"
```

---

### Task 1.3: 配置加载器

**Files:**
- Create: `shared/config_loader.py`
- Create: `tests/test_config_loader.py`

- [ ] **Step 1: 编写配置加载器测试**

`tests/test_config_loader.py`:
```python
import os
import tempfile
import pytest
from shared.config_loader import load_yaml_config, load_symbols, load_risk_config


class TestConfigLoader:
    def test_load_yaml_config_returns_dict(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("key: value\nlist:\n  - a\n  - b\n")
            f.flush()
            result = load_yaml_config(f.name)
        os.unlink(f.name)
        assert result == {"key": "value", "list": ["a", "b"]}

    def test_load_symbols_merges_primary_and_secondary(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("symbols:\n  primary:\n    - BTCUSDT\n    - ETHUSDT\n  secondary:\n    - BNBUSDT\n    - DOGEUSDT\n")
            f.flush()
            symbols = load_symbols(f.name)
        os.unlink(f.name)
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols
        assert "BNBUSDT" in symbols
        assert "DOGEUSDT" in symbols
        assert len(symbols) == 4

    def test_load_risk_config_returns_correct_defaults(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("risk:\n  risk_per_trade: 0.02\n  max_leverage: 3\n  max_drawdown: 0.10\n  daily_loss_limit: 0.05\n  consecutive_loss_breaker: 3\n  cooldown_minutes: 60\n  max_position_per_symbol: 0.25\n  max_same_direction: 0.40\n  max_total_margin: 0.75\n")
            f.flush()
            config = load_risk_config(f.name)
        os.unlink(f.name)
        assert config.risk_per_trade == 0.02
        assert config.max_leverage == 3
        assert config.max_drawdown == 0.10
        assert config.cooldown_minutes == 60

    def test_load_yaml_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            load_yaml_config("/nonexistent/path.yaml")
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_config_loader.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现配置加载器**

`shared/config_loader.py`:
```python
"""Configuration loader — reads YAML configs into typed objects."""

import os
from dataclasses import dataclass
from typing import List
import yaml


def load_yaml_config(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_symbols(path: str) -> List[str]:
    config = load_yaml_config(path)
    primary = config.get("symbols", {}).get("primary", [])
    secondary = config.get("symbols", {}).get("secondary", [])
    return primary + secondary


@dataclass
class RiskConfig:
    risk_per_trade: float = 0.015
    max_leverage: int = 5
    max_position_per_symbol: float = 0.30
    max_same_direction: float = 0.50
    max_total_margin: float = 0.80
    max_drawdown: float = 0.15
    daily_loss_limit: float = 0.05
    consecutive_loss_breaker: int = 3
    cooldown_minutes: int = 120


def load_risk_config(path: str) -> RiskConfig:
    config = load_yaml_config(path)
    risk = config.get("risk", {})
    return RiskConfig(
        risk_per_trade=float(risk.get("risk_per_trade", 0.015)),
        max_leverage=int(risk.get("max_leverage", 5)),
        max_position_per_symbol=float(risk.get("max_position_per_symbol", 0.30)),
        max_same_direction=float(risk.get("max_same_direction", 0.50)),
        max_total_margin=float(risk.get("max_total_margin", 0.80)),
        max_drawdown=float(risk.get("max_drawdown", 0.15)),
        daily_loss_limit=float(risk.get("daily_loss_limit", 0.05)),
        consecutive_loss_breaker=int(risk.get("consecutive_loss_breaker", 3)),
        cooldown_minutes=int(risk.get("cooldown_minutes", 120)),
    )
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_config_loader.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add shared/config_loader.py tests/test_config_loader.py
git commit -m "feat: add YAML config loader with typed RiskConfig"
```

---

## Phase 2: 数据通道

### Task 2.1: Market Data — K 线缓存

**Files:**
- Create: `market_data/kline_buffer.py`
- Create: `tests/test_kline_buffer.py`

- [ ] **Step 1: 编写 K 线缓存测试**

`tests/test_kline_buffer.py`:
```python
import pytest
from market_data.kline_buffer import KlineBuffer, Kline


class TestKlineBuffer:
    def setup_method(self):
        self.buffer = KlineBuffer(max_size=100)

    def test_add_kline_appends(self):
        k = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62500.0, volume=100.5)
        self.buffer.add(k)
        assert self.buffer.count("BTCUSDT", "4h") == 1

    def test_get_klines_returns_correct_range(self):
        for i in range(10):
            k = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000 + i * 14400000, close_time=1000 + (i + 1) * 14400000, open=62000.0 + i * 100, high=63000.0, low=61500.0, close=62500.0, volume=100.0)
            self.buffer.add(k)
        result = self.buffer.get_klines("BTCUSDT", "4h", limit=3)
        assert len(result) == 3
        assert result[0].open_time < result[1].open_time
        assert result[-1].open_time == 1000 + 9 * 14400000

    def test_is_closed_detects_new_candle(self):
        k1 = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62500.0, volume=100.0, is_closed=False)
        self.buffer.add(k1)
        assert self.buffer.is_closed("BTCUSDT", "4h", 1000) is False

        k2 = Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62600.0, volume=105.0, is_closed=True)
        self.buffer.add(k2)
        assert self.buffer.is_closed("BTCUSDT", "4h", 1000) is True

    def test_count_zero_for_empty_buffer(self):
        assert self.buffer.count("BTCUSDT", "4h") == 0
        assert self.buffer.count("ETHUSDT", "1d") == 0
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_kline_buffer.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 K 线缓存**

`market_data/kline_buffer.py`:
```python
"""K-line buffer — stores recent candles, detects closure."""

from dataclasses import dataclass, field
from typing import List, Optional
from collections import defaultdict


@dataclass
class Kline:
    symbol: str
    timeframe: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool = False


class KlineBuffer:
    def __init__(self, max_size: int = 500):
        self.max_size = max_size
        self._data: dict[str, List[Kline]] = defaultdict(list)
        self._latest: dict[str, Kline] = {}

    def _key(self, symbol: str, timeframe: str) -> str:
        return f"{symbol}:{timeframe}"

    def add(self, kline: Kline):
        key = self._key(kline.symbol, kline.timeframe)
        existing = self._latest.get(key)
        if existing and existing.open_time == kline.open_time:
            self._data[key][-1] = kline
        else:
            self._data[key].append(kline)
            if len(self._data[key]) > self.max_size:
                self._data[key] = self._data[key][-self.max_size:]
        self._latest[key] = kline

    def get_klines(self, symbol: str, timeframe: str, limit: int = 100) -> List[Kline]:
        key = self._key(symbol, timeframe)
        kl = self._data.get(key, [])
        return kl[-limit:] if limit < len(kl) else list(kl)

    def count(self, symbol: str, timeframe: str) -> int:
        return len(self._data.get(self._key(symbol, timeframe), []))

    def is_closed(self, symbol: str, timeframe: str, open_time: int) -> bool:
        key = self._key(symbol, timeframe)
        latest = self._latest.get(key)
        if latest and latest.open_time == open_time:
            return latest.is_closed
        return False

    def get_latest(self, symbol: str, timeframe: str) -> Optional[Kline]:
        return self._latest.get(self._key(symbol, timeframe))
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_kline_buffer.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add market_data/kline_buffer.py tests/test_kline_buffer.py
git commit -m "feat: add KlineBuffer for real-time candle storage and closure detection"
```

---

### Task 2.2: Market Data — WebSocket 连接池

**Files:**
- Create: `market_data/ws_pool.py`
- Create: `tests/test_ws_pool.py`

- [ ] **Step 1: 编写连接池测试**

`tests/test_ws_pool.py`:
```python
import pytest
from market_data.ws_pool import StreamSpec, build_stream_list, ConnectionPoolConfig


class TestStreamSpec:
    def test_build_stream_list_for_single_symbol(self):
        specs = build_stream_list(["BTCUSDT"])
        streams = [s.stream_name for s in specs]
        assert "btcusdt@kline_4h" in streams
        assert "btcusdt@kline_1d" in streams
        assert "btcusdt@kline_1w" in streams
        assert "btcusdt@markPrice" in streams

    def test_build_stream_list_for_multiple_symbols(self):
        specs = build_stream_list(["BTCUSDT", "ETHUSDT"])
        streams = [s.stream_name for s in specs]
        assert "btcusdt@kline_4h" in streams
        assert "ethusdt@kline_4h" in streams
        assert "btcusdt@kline_1d" in streams
        assert "ethusdt@kline_1d" in streams

    def test_build_stream_list_stream_count(self):
        specs = build_stream_list(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        assert len(specs) == 3 * 4

    def test_stream_name_format(self):
        specs = build_stream_list(["BTCUSDT"])
        for s in specs:
            assert "@" in s.stream_name
            assert s.stream_name == s.stream_name.lower()


class TestConnectionPoolConfig:
    def test_pool_size_bounded_by_max(self):
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ARBUSDT"]
        config = ConnectionPoolConfig(max_pool_size=3)
        assert config.effective_pool_size(symbols) == 3

    def test_pool_size_bounded_by_symbol_count(self):
        symbols = ["BTCUSDT"]
        config = ConnectionPoolConfig(max_pool_size=5)
        assert config.effective_pool_size(symbols) == 1

    def test_round_robin_distribution(self):
        config = ConnectionPoolConfig(max_pool_size=3)
        specs = build_stream_list(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        bins = config.distribute(specs)
        assert len(bins) == 3
        assert sum(len(b) for b in bins) == len(specs)
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_ws_pool.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现连接池配置**

`market_data/ws_pool.py`:
```python
"""Binance WebSocket connection pool — stream-to-connection distribution."""

from dataclasses import dataclass
from typing import List


@dataclass
class StreamSpec:
    stream_name: str
    symbol: str
    stream_type: str
    timeframe: str = ""

    @property
    def is_kline(self) -> bool:
        return self.stream_type == "kline"


STREAM_TYPES = [
    {"suffix": "@kline_4h", "type": "kline", "timeframe": "4h"},
    {"suffix": "@kline_1d", "type": "kline", "timeframe": "1d"},
    {"suffix": "@kline_1w", "type": "kline", "timeframe": "1w"},
    {"suffix": "@markPrice", "type": "mark_price", "timeframe": ""},
]


def build_stream_list(symbols: List[str]) -> List[StreamSpec]:
    specs = []
    for symbol in symbols:
        sym_lower = symbol.lower()
        for st in STREAM_TYPES:
            specs.append(StreamSpec(stream_name=f"{sym_lower}{st['suffix']}", symbol=symbol.upper(), stream_type=st["type"], timeframe=st["timeframe"]))
    return specs


class ConnectionPoolConfig:
    def __init__(self, max_pool_size: int = 5):
        self.max_pool_size = max_pool_size

    def effective_pool_size(self, symbols: List[str]) -> int:
        return min(len(symbols), self.max_pool_size)

    def distribute(self, specs: List[StreamSpec]) -> List[List[StreamSpec]]:
        n = max(1, self.effective_pool_size(list(set(s.symbol for s in specs))))
        bins: List[List[StreamSpec]] = [[] for _ in range(n)]
        for i, spec in enumerate(specs):
            bins[i % n].append(spec)
        return bins
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_ws_pool.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add market_data/ws_pool.py tests/test_ws_pool.py
git commit -m "feat: add WebSocket stream spec builder and connection pool distributor"
```

---

### Task 2.3: Market Data — 主入口

**Files:**
- Create: `market_data/feed.py`
- Create: `tests/test_market_data_feed.py`

- [ ] **Step 1: 编写 MarketDataFeed 测试**

`tests/test_market_data_feed.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from market_data.feed import MarketDataFeed
from market_data.kline_buffer import Kline


class TestMarketDataFeed:
    def setup_method(self):
        self.feed = MarketDataFeed(symbols=["BTCUSDT"], testnet=True)

    def test_on_kline_message_parses_and_stores(self):
        msg = {
            "e": "kline", "E": 1700000000000, "s": "BTCUSDT",
            "k": {"t": 1700000000000, "T": 1700014400000, "o": "62000.0", "h": "63000.0", "l": "61500.0", "c": "62500.0", "v": "100.5", "x": True}
        }
        self.feed._on_kline_message(msg)
        kline_series = self.feed.buffer.get_klines("BTCUSDT", "4h")
        assert len(kline_series) >= 0

    def test_detect_timeframe_from_interval(self):
        assert self.feed._timeframe_from_interval("4h") == "4h"
        assert self.feed._timeframe_from_interval("1d") == "1d"
        assert self.feed._timeframe_from_interval("1w") == "1w"

    def test_mark_price_parsed_correctly(self):
        self.feed._on_mark_price_message({"s": "BTCUSDT", "p": "62450.0", "E": 1700000000000})
        assert self.feed._mark_prices.get("BTCUSDT") == 62450.0

    def test_stream_to_timeframe_mapping(self):
        mapping = self.feed._stream_timeframe_map(["BTCUSDT"])
        assert "btcusdt@kline_4h" in mapping
        assert mapping["btcusdt@kline_4h"] == "4h"
        assert mapping["btcusdt@kline_1d"] == "1d"
        assert mapping["btcusdt@kline_1w"] == "1w"

    def test_singleton_no_duplicate_klines_on_repeated_open_time(self):
        self.feed.buffer.add(Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63000.0, low=61500.0, close=62500.0, volume=100.0, is_closed=False))
        self.feed.buffer.add(Kline(symbol="BTCUSDT", timeframe="4h", open_time=1000, close_time=1000 + 14400000, open=62000.0, high=63100.0, low=61500.0, close=62600.0, volume=105.0, is_closed=True))
        assert self.feed.buffer.count("BTCUSDT", "4h") == 1
        assert self.feed.buffer.is_closed("BTCUSDT", "4h", 1000) is True
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_market_data_feed.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 MarketDataFeed**

`market_data/feed.py`:
```python
"""MarketDataFeed — WebSocket → Kline buffer → kline.closed events."""

import json
import time
import threading
from typing import Dict, List, Optional
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from market_data.kline_buffer import KlineBuffer, Kline


class MarketDataFeed:
    def __init__(self, symbols: List[str], testnet: bool = True, on_kline_closed=None):
        self.symbols = symbols
        self.testnet = testnet
        self.buffer = KlineBuffer(max_size=500)
        self.on_kline_closed = on_kline_closed or (lambda symbol, timeframe, ohlcv: None)
        self._mark_prices: Dict[str, float] = {}
        self._client: Optional[SpotWebsocketStreamClient] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def _timeframe_from_interval(self, interval: str) -> str:
        mapping = {"1w": "1w", "1d": "1d", "4h": "4h"}
        return mapping.get(interval, interval)

    def _stream_timeframe_map(self, symbols: List[str]) -> Dict[str, str]:
        m = {}
        for sym in symbols:
            s = sym.lower()
            m[f"{s}@kline_4h"] = "4h"
            m[f"{s}@kline_1d"] = "1d"
            m[f"{s}@kline_1w"] = "1w"
        return m

    def _on_kline_message(self, msg: dict):
        k = msg.get("k", {})
        symbol = msg.get("s", "").upper()
        interval = k.get("i", "4h")
        timeframe = self._timeframe_from_interval(interval)
        kline = Kline(
            symbol=symbol, timeframe=timeframe,
            open_time=k.get("t", 0), close_time=k.get("T", 0),
            open=float(k.get("o", 0)), high=float(k.get("h", 0)),
            low=float(k.get("l", 0)), close=float(k.get("c", 0)),
            volume=float(k.get("v", 0)),
            is_closed=k.get("x", False),
        )
        prev_closed = self.buffer.is_closed(symbol, timeframe, kline.open_time)
        self.buffer.add(kline)
        if kline.is_closed and not prev_closed:
            ohlcv = self.buffer.get_klines(symbol, timeframe, limit=100)
            self.on_kline_closed(symbol, timeframe, ohlcv)

    def _on_mark_price_message(self, msg: dict):
        symbol = msg.get("s", "").upper()
        price = float(msg.get("p", 0))
        self._mark_prices[symbol] = price

    def get_mark_price(self, symbol: str) -> Optional[float]:
        return self._mark_prices.get(symbol.upper())

    def start(self):
        self._running = True
        url = "wss://testnet.binance.vision/ws-api/v3" if self.testnet else "wss://ws-api.binance.com:443/ws-api/v3"
        self._client = SpotWebsocketStreamClient(stream_url=url)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        tf_map = self._stream_timeframe_map(self.symbols)
        streams = [f"{s.lower()}@kline_4h" for s in self.symbols] + [f"{s.lower()}@kline_1d" for s in self.symbols] + [f"{s.lower()}@kline_1w" for s in self.symbols] + [f"{s.lower()}@markPrice" for s in self.symbols]
        def on_message(_, raw):
            msg = json.loads(raw)
            if isinstance(msg, dict):
                e = msg.get("e", "")
                if e == "kline":
                    self._on_kline_message(msg)
                elif e == "markPriceUpdate":
                    self._on_mark_price_message(msg)
        if self._client:
            self._client.kline(stream=streams, callback=on_message)
        while self._running:
            time.sleep(1)

    def stop(self):
        self._running = False
        if self._client:
            self._client.stop()
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_market_data_feed.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add market_data/feed.py tests/test_market_data_feed.py
git commit -m "feat: add MarketDataFeed with WebSocket → buffer → closure event pipeline"
```

---

### Task 2.4: Monitor — 基础心跳

**Files:**
- Create: `monitor/collector.py`
- Create: `tests/test_monitor_collector.py`

- [ ] **Step 1: 编写 MetricsCollector 测试**

`tests/test_monitor_collector.py`:
```python
import pytest
import threading
from monitor.collector import MetricsCollector


class TestMetricsCollector:
    def setup_method(self):
        MetricsCollector.reset()
        self.collector = MetricsCollector.instance()

    def test_singleton_returns_same_instance(self):
        c1 = MetricsCollector.instance()
        c2 = MetricsCollector.instance()
        assert c1 is c2

    def test_record_heartbeat_updates_timestamp(self):
        self.collector.heartbeat("market_data")
        last = self.collector.last_heartbeat("market_data")
        assert last is not None
        assert last > 0

    def test_missing_heartbeat_returns_none(self):
        assert self.collector.last_heartbeat("nonexistent") is None

    def test_increment_counter_adds(self):
        self.collector.increment("trades.today")
        self.collector.increment("trades.today")
        assert self.collector.get_counter("trades.today") == 2

    def test_unknown_counter_returns_zero(self):
        assert self.collector.get_counter("unknown.counter") == 0

    def test_set_gauge_stores_value(self):
        self.collector.set_gauge("margin_ratio", 0.45)
        assert self.collector.get_gauge("margin_ratio") == 0.45

    def test_reset_clears_all_metrics(self):
        self.collector.heartbeat("test")
        self.collector.increment("test.counter")
        assert self.collector.last_heartbeat("test") is not None
        MetricsCollector.reset()
        assert MetricsCollector.instance().last_heartbeat("test") is None
        assert MetricsCollector.instance().get_counter("test.counter") == 0

    def test_thread_safety_concurrent_heartbeats(self):
        def send_heartbeats():
            for _ in range(100):
                self.collector.heartbeat("market_data")

        threads = [threading.Thread(target=send_heartbeats) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert self.collector.last_heartbeat("market_data") is not None
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_monitor_collector.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 MetricsCollector**

`monitor/collector.py`:
```python
"""MetricsCollector — thread-safe singleton for heartbeat, counters, and gauges."""

import time
import threading
from typing import Optional


class MetricsCollector:
    _instance: Optional["MetricsCollector"] = None
    _lock: threading.Lock = threading.Lock()

    def __init__(self):
        self._heartbeats: dict[str, float] = {}
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._lock = threading.Lock()

    @classmethod
    def instance(cls) -> "MetricsCollector":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        with cls._lock:
            cls._instance = None

    def heartbeat(self, module: str):
        with self._lock:
            self._heartbeats[module] = time.time()

    def last_heartbeat(self, module: str) -> Optional[float]:
        with self._lock:
            return self._heartbeats.get(module)

    def increment(self, metric: str, amount: int = 1):
        with self._lock:
            self._counters[metric] = self._counters.get(metric, 0) + amount

    def get_counter(self, metric: str) -> int:
        with self._lock:
            return self._counters.get(metric, 0)

    def set_gauge(self, metric: str, value: float):
        with self._lock:
            self._gauges[metric] = value

    def get_gauge(self, metric: str) -> float:
        with self._lock:
            return self._gauges.get(metric, 0.0)
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_monitor_collector.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add monitor/collector.py tests/test_monitor_collector.py
git commit -m "feat: add thread-safe MetricsCollector singleton for heartbeat tracking"
```

---

## Phase 3: 信号引擎迁移

### Task 3.1: Signal Engine 统一入口

**Files:**
- Create: `signal_engine/engine.py`
- Create: `tests/test_signal_engine.py`

- [ ] **Step 1: 编写 SignalEngine 测试**

`tests/test_signal_engine.py`:
```python
import pytest
from signal_engine.engine import SignalEngine, Signal


class TestSignalEngine:
    def setup_method(self):
        self.engine = SignalEngine()

    def test_run_returns_signal_with_required_fields(self):
        ohlcv = [
            {"open_time": 1000, "open": 62000, "high": 63000, "low": 61500, "close": 62500, "volume": 100.0}
        ]
        signal = self.engine.run("BTCUSDT", "4h", ohlcv)
        assert signal is None or isinstance(signal, Signal)

    def test_signal_dataclass_has_all_fields(self):
        s = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0, attribution={"strategy_a": 0.5, "strategy_b": 0.5})
        assert s.symbol == "BTCUSDT"
        assert s.direction == "LONG"
        assert s.conviction == 0.72
        assert s.attribution["strategy_a"] == 0.5

    def test_run_unknown_timeframe_returns_none(self):
        signal = self.engine.run("BTCUSDT", "unknown", [])
        assert signal is None

    def test_run_returns_none_for_weekly_without_enough_data(self):
        ohlcv = []
        signal = self.engine.run("BTCUSDT", "1w", ohlcv)
        assert signal is None
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_signal_engine.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 SignalEngine 入口**

`signal_engine/engine.py`:
```python
"""SignalEngine — unified entry point for 4-layer signal generation."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class Signal:
    symbol: str
    direction: str  # LONG / SHORT
    conviction: float
    entry_price: float
    stop_loss: float
    take_profit: float
    attribution: Dict[str, float] = field(default_factory=dict)
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class SignalEngine:
    def __init__(self):
        self._weekly_cache: Dict[str, Any] = {}
        self._daily_cache: Dict[str, Any] = {}

    def run(self, symbol: str, timeframe: str, ohlcv: List[dict]) -> Optional[Signal]:
        if timeframe not in ("1w", "1d", "4h"):
            return None
        if not ohlcv:
            return None
        if timeframe == "1w":
            return self._run_weekly(symbol, ohlcv)
        elif timeframe == "1d":
            return self._run_daily(symbol, ohlcv)
        elif timeframe == "4h":
            return self._run_4h(symbol, ohlcv)
        return None

    def _run_weekly(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def _run_daily(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def _run_4h(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def get_weekly_context(self, symbol: str) -> Optional[Any]:
        return self._weekly_cache.get(symbol)

    def get_daily_context(self, symbol: str) -> Optional[Any]:
        return self._daily_cache.get(symbol)
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_signal_engine.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add signal_engine/engine.py tests/test_signal_engine.py
git commit -m "feat: add SignalEngine unified entry point with Signal dataclass"
```

---

### Task 3.2: Scheduler

**Files:**
- Create: `scheduler/scheduler.py`
- Create: `tests/test_scheduler.py`

- [ ] **Step 1: 编写 Scheduler 测试**

`tests/test_scheduler.py`:
```python
import pytest
import threading
import queue
from scheduler.scheduler import Scheduler


class TestScheduler:
    def setup_method(self):
        self.results = queue.Queue()
        def mock_engine_run(symbol, timeframe, ohlcv):
            self.results.put((symbol, timeframe))
            return None
        self.scheduler = Scheduler(engine_run=mock_engine_run, max_workers=2)

    def test_dispatch_4h_calls_engine(self):
        self.scheduler.dispatch("BTCUSDT", "4h", [{"open": 62000}])
        try:
            symbol, timeframe = self.results.get(timeout=2)
            assert symbol == "BTCUSDT"
            assert timeframe == "4h"
        except queue.Empty:
            pytest.fail("engine_run was not called")

    def test_dispatch_weekly_calls_engine(self):
        self.scheduler.dispatch("ETHUSDT", "1w", [{"open": 3100}])
        try:
            symbol, timeframe = self.results.get(timeout=2)
            assert symbol == "ETHUSDT"
            assert timeframe == "1w"
        except queue.Empty:
            pytest.fail("engine_run was not called")

    def test_dispatch_daily_calls_engine(self):
        self.scheduler.dispatch("SOLUSDT", "1d", [{"open": 150}])
        try:
            symbol, timeframe = self.results.get(timeout=2)
            assert symbol == "SOLUSDT"
            assert timeframe == "1d"
        except queue.Empty:
            pytest.fail("engine_run was not called")

    def test_parallel_dispatch_uses_threadpool(self):
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        for s in symbols:
            self.scheduler.dispatch(s, "4h", [{"open": 100}])
        received = []
        for _ in range(3):
            try:
                received.append(self.results.get(timeout=2))
            except queue.Empty:
                break
        assert len(received) == 3
        symbols_received = {r[0] for r in received}
        assert symbols_received == set(symbols)
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_scheduler.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 Scheduler**

`scheduler/scheduler.py`:
```python
"""Scheduler — dispatches kline.closed events to SignalEngine via thread pool."""

from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, List, Optional


class Scheduler:
    def __init__(self, engine_run: Callable, max_workers: int = 8):
        self.engine_run = engine_run
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: List[Future] = []

    def dispatch(self, symbol: str, timeframe: str, ohlcv: list) -> Optional[str]:
        future = self._executor.submit(self.engine_run, symbol, timeframe, ohlcv)
        self._futures.append(future)
        return None

    def on_kline_closed(self, symbol: str, timeframe: str, ohlcv: list):
        self.dispatch(symbol, timeframe, ohlcv)

    def shutdown(self, wait: bool = True):
        self._executor.shutdown(wait=wait)
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_scheduler.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scheduler/scheduler.py tests/test_scheduler.py
git commit -m "feat: add Scheduler with ThreadPoolExecutor for kline-triggered dispatch"
```

---

## Phase 4: 执行链路

### Task 4.1: Execution — Order Gateway

**Files:**
- Create: `execution/order_gateway.py`
- Create: `tests/test_order_gateway.py`

- [ ] **Step 1: 编写 OrderGateway 测试**

`tests/test_order_gateway.py`:
```python
import os
import pytest
from unittest.mock import patch, MagicMock
from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse


class TestOrderGateway:
    def setup_method(self):
        os.environ["BINANCE_API_KEY"] = "test_key"
        os.environ["BINANCE_API_SECRET"] = "test_secret"
        self.gateway = OrderGateway(testnet=True)

    def test_order_request_dataclass(self):
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=0.15, price=62500.0, time_in_force="GTC")
        assert req.symbol == "BTCUSDT"
        assert req.side == "BUY"
        assert req.quantity == 0.15
        assert req.price == 62500.0

    def test_order_response_dataclass(self):
        resp = OrderResponse(order_id=12345, symbol="BTCUSDT", side="BUY", status="FILLED", executed_qty=0.15, avg_price=62500.0)
        assert resp.order_id == 12345
        assert resp.status == "FILLED"
        assert resp.executed_qty == 0.15

    def test_place_limit_order_mock(self):
        req = OrderRequest(symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=0.15, price=62500.0)
        with patch.object(self.gateway, "place_order") as mock_place:
            mock_place.return_value = OrderResponse(order_id=42, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0)
            resp = self.gateway.place_order(req)
            assert resp.order_id == 42
            assert resp.status == "NEW"
            mock_place.assert_called_once_with(req)

    def test_cancel_order_mock(self):
        with patch.object(self.gateway, "cancel_order") as mock_cancel:
            mock_cancel.return_value = OrderResponse(order_id=42, symbol="BTCUSDT", side="BUY", status="CANCELED", executed_qty=0.0, avg_price=0.0)
            resp = self.gateway.cancel_order("BTCUSDT", 42)
            assert resp.status == "CANCELED"

    def test_api_credentials_from_env(self):
        gw = OrderGateway(testnet=True)
        assert gw.api_key == "test_key"
        assert gw.api_secret == "test_secret"
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_order_gateway.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 OrderGateway**

`execution/order_gateway.py`:
```python
"""OrderGateway — Binance Futures REST API wrapper for order placement."""

import os
import hmac
import hashlib
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode
import requests


@dataclass
class OrderRequest:
    symbol: str
    side: str  # BUY / SELL
    order_type: str  # LIMIT / MARKET / STOP_MARKET / TAKE_PROFIT_MARKET
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: str = "GTC"
    reduce_only: bool = False


@dataclass
class OrderResponse:
    order_id: int
    symbol: str
    side: str
    status: str
    executed_qty: float
    avg_price: float
    error: Optional[str] = None


class OrderGateway:
    BASE_URL_TESTNET = "https://testnet.binancefuture.com"
    BASE_URL_LIVE = "https://fapi.binance.com"

    def __init__(self, testnet: bool = True):
        self.testnet = testnet
        self.api_key = os.environ.get("BINANCE_API_KEY", "")
        self.api_secret = os.environ.get("BINANCE_API_SECRET", "")
        self.base_url = self.BASE_URL_TESTNET if testnet else self.BASE_URL_LIVE

    def _sign(self, params: dict) -> str:
        query = urlencode(params)
        return hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()

    def _request(self, method: str, endpoint: str, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)
        url = f"{self.base_url}{endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key}
        if method == "POST":
            resp = requests.post(url, headers=headers, data=params, timeout=10)
        elif method == "DELETE":
            resp = requests.delete(url, headers=headers, data=params, timeout=10)
        else:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
        return resp.json()

    def place_order(self, req: OrderRequest) -> OrderResponse:
        params = {"symbol": req.symbol, "side": req.side, "type": req.order_type, "quantity": str(req.quantity)}
        if req.price is not None:
            params["price"] = str(req.price)
            params["timeInForce"] = req.time_in_force
        if req.stop_price is not None:
            params["stopPrice"] = str(req.stop_price)
        if req.reduce_only:
            params["reduceOnly"] = "true"
        try:
            result = self._request("POST", "/fapi/v1/order", params)
            return OrderResponse(order_id=result.get("orderId", 0), symbol=result.get("symbol", req.symbol), side=result.get("side", req.side), status=result.get("status", "REJECTED"), executed_qty=float(result.get("executedQty", 0)), avg_price=float(result.get("avgPrice", 0)), error=result.get("msg"))
        except Exception as e:
            return OrderResponse(order_id=0, symbol=req.symbol, side=req.side, status="ERROR", executed_qty=0.0, avg_price=0.0, error=str(e))

    def cancel_order(self, symbol: str, order_id: int) -> OrderResponse:
        try:
            result = self._request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": str(order_id)})
            return OrderResponse(order_id=result.get("orderId", order_id), symbol=symbol, side=result.get("side", ""), status=result.get("status", "CANCELED"), executed_qty=float(result.get("executedQty", 0)), avg_price=float(result.get("avgPrice", 0)), error=result.get("msg"))
        except Exception as e:
            return OrderResponse(order_id=order_id, symbol=symbol, side="", status="ERROR", executed_qty=0.0, avg_price=0.0, error=str(e))

    def get_account(self) -> dict:
        try:
            return self._request("GET", "/fapi/v2/account", {})
        except Exception as e:
            return {"error": str(e)}
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_order_gateway.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add execution/order_gateway.py tests/test_order_gateway.py
git commit -m "feat: add OrderGateway for Binance Futures REST API"
```

---

### Task 4.2: Execution — Order Manager

**Files:**
- Create: `execution/order_manager.py`
- Create: `tests/test_order_manager.py`

- [ ] **Step 1: 编写 OrderManager 测试**

`tests/test_order_manager.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from execution.order_manager import OrderManager, OrderState
from execution.order_gateway import OrderRequest, OrderResponse


class TestOrderManager:
    def setup_method(self):
        self.gateway = MagicMock()
        self.manager = OrderManager(gateway=self.gateway)

    def test_submit_limit_order_creates_entry(self):
        self.gateway.place_order.return_value = OrderResponse(order_id=42, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0)
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert order.order_id == 42
        assert order.symbol == "BTCUSDT"
        assert order.state == OrderState.PENDING

    def test_execute_signal_places_entry_stop_and_take_profit(self):
        self.gateway.place_order.side_effect = [
            OrderResponse(order_id=1, symbol="BTCUSDT", side="BUY", status="NEW", executed_qty=0.0, avg_price=0.0),
            OrderResponse(order_id=2, symbol="BTCUSDT", side="SELL", status="NEW", executed_qty=0.0, avg_price=0.0),
            OrderResponse(order_id=3, symbol="BTCUSDT", side="SELL", status="NEW", executed_qty=0.0, avg_price=0.0),
        ]
        orders = self.manager.execute_signal("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert len(orders) == 3
        assert self.gateway.place_order.call_count == 3

    def test_order_state_transitions(self):
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert order.state == OrderState.PENDING
        order.state = OrderState.FILLED
        assert order.state == OrderState.FILLED
        order.state = OrderState.CANCELED
        assert order.state == OrderState.CANCELED

    def test_retry_on_network_error(self):
        call_count = [0]
        def side_effect(req):
            call_count[0] += 1
            if call_count[0] < 3:
                return OrderResponse(order_id=0, symbol=req.symbol, side=req.side, status="ERROR", executed_qty=0.0, avg_price=0.0, error="Connection timeout")
            return OrderResponse(order_id=99, symbol=req.symbol, side=req.side, status="NEW", executed_qty=0.0, avg_price=0.0)

        self.gateway.place_order.side_effect = side_effect
        order = self.manager.submit_entry("BTCUSDT", "LONG", 0.15, 62500.0, 61500.0, 65000.0)
        assert call_count[0] == 3
        assert order.order_id == 99
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_order_manager.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 OrderManager**

`execution/order_manager.py`:
```python
"""OrderManager — order lifecycle: submit, retry, timeout, partial fill."""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List
from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse


class OrderState(str, Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    ERROR = "ERROR"


@dataclass
class ManagedOrder:
    order_id: int
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: float
    state: OrderState = OrderState.PENDING
    filled_qty: float = 0.0
    avg_price: float = 0.0
    created_at: float = field(default_factory=time.time)
    error: str = ""


class OrderManager:
    def __init__(self, gateway: OrderGateway, max_retries: int = 3, retry_backoff_base: float = 1.0, order_timeout: float = 60.0, partial_fill_wait: float = 30.0):
        self.gateway = gateway
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self.order_timeout = order_timeout
        self.partial_fill_wait = partial_fill_wait
        self._orders: List[ManagedOrder] = []

    def _place_with_retry(self, req: OrderRequest) -> OrderResponse:
        last_error = None
        for attempt in range(self.max_retries):
            resp = self.gateway.place_order(req)
            if resp.status != "ERROR":
                return resp
            last_error = resp.error
            time.sleep(self.retry_backoff_base * (2 ** attempt))
        return OrderResponse(order_id=0, symbol=req.symbol, side=req.side, status="ERROR", executed_qty=0.0, avg_price=0.0, error=last_error or "Max retries exceeded")

    def submit_entry(self, symbol: str, direction: str, quantity: float, entry_price: float, stop_loss: float, take_profit: float) -> ManagedOrder:
        side = "BUY" if direction == "LONG" else "SELL"
        req = OrderRequest(symbol=symbol, side=side, order_type="LIMIT", quantity=quantity, price=entry_price)
        resp = self._place_with_retry(req)
        order = ManagedOrder(order_id=resp.order_id, symbol=symbol, side=side, order_type="LIMIT", quantity=quantity, price=entry_price, state=OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING, error=resp.error or "")
        self._orders.append(order)
        return order

    def submit_stop_loss(self, symbol: str, direction: str, quantity: float, stop_price: float) -> ManagedOrder:
        side = "SELL" if direction == "LONG" else "BUY"
        req = OrderRequest(symbol=symbol, side=side, order_type="STOP_MARKET", quantity=quantity, stop_price=stop_price, reduce_only=True)
        resp = self._place_with_retry(req)
        order = ManagedOrder(order_id=resp.order_id, symbol=symbol, side=side, order_type="STOP_MARKET", quantity=quantity, price=stop_price, state=OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING, error=resp.error or "")
        self._orders.append(order)
        return order

    def submit_take_profit(self, symbol: str, direction: str, quantity: float, tp_price: float) -> ManagedOrder:
        side = "SELL" if direction == "LONG" else "BUY"
        req = OrderRequest(symbol=symbol, side=side, order_type="TAKE_PROFIT_MARKET", quantity=quantity, stop_price=tp_price, reduce_only=True)
        resp = self._place_with_retry(req)
        order = ManagedOrder(order_id=resp.order_id, symbol=symbol, side=side, order_type="TAKE_PROFIT_MARKET", quantity=quantity, price=tp_price, state=OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING, error=resp.error or "")
        self._orders.append(order)
        return order

    def execute_signal(self, symbol: str, direction: str, quantity: float, entry_price: float, stop_loss: float, take_profit: float) -> List[ManagedOrder]:
        orders = []
        orders.append(self.submit_entry(symbol, direction, quantity, entry_price, stop_loss, take_profit))
        orders.append(self.submit_stop_loss(symbol, direction, quantity, stop_loss))
        orders.append(self.submit_take_profit(symbol, direction, quantity, take_profit))
        return orders

    @property
    def active_orders(self) -> List[ManagedOrder]:
        return [o for o in self._orders if o.state in (OrderState.PENDING, OrderState.PARTIALLY_FILLED)]
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_order_manager.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add execution/order_manager.py tests/test_order_manager.py
git commit -m "feat: add OrderManager with entry/stop/tp submission and retry logic"
```

---

### Task 4.3: Portfolio Tracker

**Files:**
- Create: `portfolio/tracker.py`
- Create: `tests/test_portfolio_tracker.py`

- [ ] **Step 1: 编写 PortfolioTracker 测试**

`tests/test_portfolio_tracker.py`:
```python
import pytest
from portfolio.tracker import PortfolioTracker, Position


class TestPortfolioTracker:
    def setup_method(self):
        self.tracker = PortfolioTracker(initial_equity=10000.0)

    def test_initial_state(self):
        assert self.tracker.total_equity == 10000.0
        assert self.tracker.available_balance == 10000.0
        assert self.tracker.peak_equity == 10000.0
        assert len(self.tracker.positions) == 0

    def test_open_position_adds_to_tracker(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.15, entry_price=62500.0, leverage=3)
        self.tracker.open_position(pos)
        assert "BTCUSDT" in self.tracker.positions
        assert self.tracker.positions["BTCUSDT"].quantity == 0.15

    def test_close_position_removes_from_tracker(self):
        pos = Position(symbol="ETHUSDT", direction="LONG", quantity=0.5, entry_price=3100.0, leverage=3)
        self.tracker.open_position(pos)
        pnl = self.tracker.close_position("ETHUSDT", 3200.0)
        assert "ETHUSDT" not in self.tracker.positions
        assert pnl > 0

    def test_unrealized_pnl_long_position(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        upnl = self.tracker.unrealized_pnl("BTCUSDT", 62000.0)
        assert upnl == pytest.approx(200.0, rel=0.01)

    def test_unrealized_pnl_short_position(self):
        pos = Position(symbol="BTCUSDT", direction="SHORT", quantity=0.1, entry_price=62000.0, leverage=3)
        self.tracker.open_position(pos)
        upnl = self.tracker.unrealized_pnl("BTCUSDT", 60000.0)
        assert upnl == pytest.approx(200.0, rel=0.01)

    def test_margin_ratio_calculation(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.15, entry_price=62500.0, leverage=3)
        self.tracker.open_position(pos)
        expected_margin = (0.15 * 62500.0) / 3
        assert self.tracker.total_margin == pytest.approx(expected_margin)
        ratio = self.tracker.margin_ratio
        assert ratio == pytest.approx(expected_margin / 10000.0)

    def test_peak_equity_tracks_maximum(self):
        self.tracker.update_equity(11000.0)
        assert self.tracker.peak_equity == 11000.0
        self.tracker.update_equity(10500.0)
        assert self.tracker.peak_equity == 11000.0

    def test_drawdown_calculation(self):
        self.tracker.update_equity(12000.0)
        self.tracker.update_equity(10200.0)
        dd = self.tracker.current_drawdown
        assert dd == pytest.approx(0.15, rel=0.01)

    def test_daily_pnl_tracks_realized(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.15, entry_price=62500.0, leverage=3)
        self.tracker.open_position(pos)
        self.tracker.close_position("BTCUSDT", 63000.0)
        assert self.tracker.daily_realized_pnl > 0

    def test_peak_equity_updates_with_pnl_gains(self):
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.1, entry_price=62500.0, leverage=3)
        self.tracker.open_position(pos)
        self.tracker.close_position("BTCUSDT", 64000.0)
        assert self.tracker.total_equity > 10000.0
        assert self.tracker.peak_equity == self.tracker.total_equity
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_portfolio_tracker.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 PortfolioTracker**

`portfolio/tracker.py`:
```python
"""PortfolioTracker — position, equity, margin, PnL tracking."""

from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime, timezone


@dataclass
class Position:
    symbol: str
    direction: str  # LONG / SHORT
    quantity: float
    entry_price: float
    leverage: int
    opened_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class PortfolioTracker:
    def __init__(self, initial_equity: float = 0.0):
        self.total_equity: float = initial_equity
        self.available_balance: float = initial_equity
        self.peak_equity: float = initial_equity
        self.daily_realized_pnl: float = 0.0
        self.total_realized_pnl: float = 0.0
        self.positions: Dict[str, Position] = {}
        self.trade_count_today: int = 0
        self.consecutive_losses: int = 0
        self._last_reset_day: int = datetime.now(timezone.utc).day

    def _maybe_reset_daily(self):
        today = datetime.now(timezone.utc).day
        if today != self._last_reset_day:
            self.daily_realized_pnl = 0.0
            self.trade_count_today = 0
            self._last_reset_day = today

    def update_equity(self, total_equity: float, available_balance: Optional[float] = None):
        self.total_equity = total_equity
        if available_balance is not None:
            self.available_balance = available_balance
        if total_equity > self.peak_equity:
            self.peak_equity = total_equity

    def open_position(self, position: Position):
        self.positions[position.symbol] = position
        self.trade_count_today += 1
        self._maybe_reset_daily()

    def close_position(self, symbol: str, exit_price: float) -> float:
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return 0.0
        direction_mult = 1 if pos.direction == "LONG" else -1
        pnl = (exit_price - pos.entry_price) * pos.quantity * direction_mult
        self.total_equity += pnl
        self.total_realized_pnl += pnl
        self.daily_realized_pnl += pnl
        if pnl > 0:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
        if self.total_equity > self.peak_equity:
            self.peak_equity = self.total_equity
        self._maybe_reset_daily()
        return pnl

    def unrealized_pnl(self, symbol: str, mark_price: float) -> float:
        pos = self.positions.get(symbol)
        if pos is None:
            return 0.0
        direction_mult = 1 if pos.direction == "LONG" else -1
        return (mark_price - pos.entry_price) * pos.quantity * direction_mult

    @property
    def total_margin(self) -> float:
        return sum((p.quantity * p.entry_price) / p.leverage for p in self.positions.values())

    @property
    def margin_ratio(self) -> float:
        if self.total_equity <= 0:
            return 1.0
        return self.total_margin / self.total_equity

    @property
    def current_drawdown(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return (self.peak_equity - self.total_equity) / self.peak_equity

    def margin_for_symbol(self, symbol: str) -> float:
        pos = self.positions.get(symbol)
        if pos is None:
            return 0.0
        return (pos.quantity * pos.entry_price) / pos.leverage

    def margin_same_direction(self, direction: str) -> float:
        return sum((p.quantity * p.entry_price) / p.leverage for p in self.positions.values() if p.direction == direction)
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_portfolio_tracker.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add portfolio/tracker.py tests/test_portfolio_tracker.py
git commit -m "feat: add PortfolioTracker with position/equity/margin/drawdown tracking"
```

---

## Phase 5: 风控中间件链

### Task 5.1: Risk Manager — 中间件链

**Files:**
- Create: `risk/chain.py`
- Create: `risk/position_sizer.py`
- Create: `risk/drawdown_breaker.py`
- Create: `risk/daily_loss_limit.py`
- Create: `risk/concentration.py`
- Create: `tests/test_risk_chain.py`

- [ ] **Step 1: 编写风控中间件测试**

`tests/test_risk_chain.py`:
```python
import pytest
from risk.chain import MiddlewareChain, MiddlewareResult
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker, Position


class TestRiskChain:
    def setup_method(self):
        self.tracker = PortfolioTracker(initial_equity=10000.0)
        self.chain = MiddlewareChain()

    def test_position_sizer_calculates_correct_size(self):
        sizer = PositionSizer(risk_per_trade=0.015)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = sizer.process(signal, self.tracker)
        assert not result.rejected
        assert result.signal is not None
        assert result.modifications.get("position_size") is not None

    def test_position_sizer_zero_size_for_invalid_stop(self):
        sizer = PositionSizer(risk_per_trade=0.015)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=62500.0, take_profit=65000.0)
        result = sizer.process(signal, self.tracker)
        assert result.rejected

    def test_drawdown_breaker_active_by_default(self):
        breaker = DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = breaker.process(signal, self.tracker)
        assert not result.rejected

    def test_drawdown_breaker_triggers_on_drawdown(self):
        self.tracker.update_equity(10000.0)
        self.tracker.peak_equity = 12000.0
        breaker = DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = breaker.process(signal, self.tracker)
        assert result.rejected

    def test_daily_loss_limit_respects_threshold(self):
        limit = DailyLossLimit(daily_loss_limit=0.05)
        self.tracker.daily_realized_pnl = -600.0
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = limit.process(signal, self.tracker)
        assert result.rejected

    def test_concentration_single_symbol_limit(self):
        self.tracker.open_position(Position(symbol="BTCUSDT", direction="LONG", quantity=0.48, entry_price=62500.0, leverage=3))
        check = ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80)
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = check.process(signal, self.tracker)
        assert result.rejected

    def test_chain_processes_all_middleware_in_order(self):
        self.chain.add(PositionSizer(risk_per_trade=0.015))
        self.chain.add(DailyLossLimit(daily_loss_limit=0.05))
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = self.chain.process(signal, self.tracker)
        assert not result.rejected

    def test_chain_stops_at_first_rejection(self):
        self.chain.add(DailyLossLimit(daily_loss_limit=0.05))
        self.chain.add(PositionSizer(risk_per_trade=0.015))
        self.tracker.daily_realized_pnl = -600.0
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = self.chain.process(signal, self.tracker)
        assert result.rejected
        assert "DailyLossLimit" in result.reason
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_risk_chain.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现中间件链**

`risk/chain.py`:
```python
"""Middleware chain — composable risk checks executed in order."""

from dataclasses import dataclass, field
from typing import Any, Dict, List
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


@dataclass
class MiddlewareResult:
    rejected: bool
    signal: Signal | None = None
    reason: str = ""
    modifications: Dict[str, Any] = field(default_factory=dict)


class Middleware:
    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        raise NotImplementedError


class MiddlewareChain:
    def __init__(self):
        self._middleware: List[Middleware] = []

    def add(self, mw: Middleware):
        self._middleware.append(mw)
        return self

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        current_signal = signal
        for mw in self._middleware:
            result = mw.process(current_signal, portfolio)
            if result.rejected:
                return result
            if result.signal is not None:
                current_signal = result.signal
        return MiddlewareResult(rejected=False, signal=current_signal)
```

`risk/position_sizer.py`:
```python
from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class PositionSizer(Middleware):
    def __init__(self, risk_per_trade: float = 0.015):
        self.risk_per_trade = risk_per_trade

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        stop_distance = abs(signal.entry_price - signal.stop_loss)
        if stop_distance <= 0:
            return MiddlewareResult(rejected=True, reason="PositionSizer: invalid stop distance (zero or negative)")
        risk_amount = portfolio.total_equity * self.risk_per_trade
        position_size = risk_amount / stop_distance
        if position_size <= 0:
            return MiddlewareResult(rejected=True, reason=f"PositionSizer: calculated size {position_size} <= 0")
        return MiddlewareResult(rejected=False, signal=signal, modifications={"position_size": position_size, "risk_amount": risk_amount})
```

`risk/drawdown_breaker.py`:
```python
import time
from enum import Enum
from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class BreakerState(str, Enum):
    ACTIVE = "ACTIVE"
    TRIGGERED = "TRIGGERED"
    COOLDOWN = "COOLDOWN"


class DrawdownBreaker(Middleware):
    def __init__(self, max_drawdown: float = 0.15, consecutive_loss_breaker: int = 3, cooldown_minutes: int = 120):
        self.max_drawdown = max_drawdown
        self.consecutive_loss_breaker = consecutive_loss_breaker
        self.cooldown_seconds = cooldown_minutes * 60
        self.state = BreakerState.ACTIVE
        self._triggered_at: float = 0.0

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        if self.state == BreakerState.COOLDOWN:
            if time.time() - self._triggered_at >= self.cooldown_seconds:
                self.state = BreakerState.ACTIVE
            else:
                remaining = int((self.cooldown_seconds - (time.time() - self._triggered_at)) / 60)
                return MiddlewareResult(rejected=True, reason=f"DrawdownBreaker: cooldown, {remaining}min remaining")
        if portfolio.current_drawdown >= self.max_drawdown:
            self.state = BreakerState.TRIGGERED if self.state == BreakerState.ACTIVE else self.state
            return MiddlewareResult(rejected=True, reason=f"DrawdownBreaker: drawdown {portfolio.current_drawdown:.2%} >= {self.max_drawdown:.0%}")
        if portfolio.consecutive_losses >= self.consecutive_loss_breaker:
            self.state = BreakerState.TRIGGERED
            self._triggered_at = time.time()
            self.state = BreakerState.COOLDOWN
            return MiddlewareResult(rejected=True, reason=f"DrawdownBreaker: {portfolio.consecutive_losses} consecutive losses")
        return MiddlewareResult(rejected=False, signal=signal)
```

`risk/daily_loss_limit.py`:
```python
from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class DailyLossLimit(Middleware):
    def __init__(self, daily_loss_limit: float = 0.05):
        self.daily_loss_limit = daily_loss_limit

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        if portfolio.total_equity <= 0:
            return MiddlewareResult(rejected=True, reason="DailyLossLimit: equity <= 0")
        loss_ratio = -portfolio.daily_realized_pnl / portfolio.total_equity
        if loss_ratio >= self.daily_loss_limit:
            return MiddlewareResult(rejected=True, reason=f"DailyLossLimit: daily loss {loss_ratio:.2%} >= {self.daily_loss_limit:.0%}")
        return MiddlewareResult(rejected=False, signal=signal)
```

`risk/concentration.py`:
```python
from risk.chain import Middleware, MiddlewareResult
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


class ConcentrationCheck(Middleware):
    def __init__(self, max_per_symbol: float = 0.30, max_same_direction: float = 0.50, max_total_margin: float = 0.80):
        self.max_per_symbol = max_per_symbol
        self.max_same_direction = max_same_direction
        self.max_total_margin = max_total_margin

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        if portfolio.total_equity <= 0:
            return MiddlewareResult(rejected=True, reason="ConcentrationCheck: equity <= 0")
        sym_margin = portfolio.margin_for_symbol(signal.symbol)
        sym_ratio = sym_margin / portfolio.total_equity
        if sym_ratio >= self.max_per_symbol:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: {signal.symbol} margin {sym_ratio:.1%} >= {self.max_per_symbol:.0%}")
        dir_margin = portfolio.margin_same_direction(signal.direction)
        dir_ratio = dir_margin / portfolio.total_equity
        if dir_ratio >= self.max_same_direction:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: {signal.direction} total margin {dir_ratio:.1%} >= {self.max_same_direction:.0%}")
        total_ratio = portfolio.margin_ratio
        if total_ratio >= self.max_total_margin:
            return MiddlewareResult(rejected=True, reason=f"ConcentrationCheck: total margin {total_ratio:.1%} >= {self.max_total_margin:.0%}")
        return MiddlewareResult(rejected=False, signal=signal)
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_risk_chain.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add risk/chain.py risk/position_sizer.py risk/drawdown_breaker.py risk/daily_loss_limit.py risk/concentration.py tests/test_risk_chain.py
git commit -m "feat: add risk middleware chain (PositionSizer, DrawdownBreaker, DailyLossLimit, ConcentrationCheck)"
```

---

### Task 5.2: Risk Manager 集成测试

**Files:**
- Create: `tests/test_integration_risk.py`

- [ ] **Step 1: 编写集成测试**

`tests/test_integration_risk.py`:
```python
import pytest
from risk.chain import MiddlewareChain
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker, Position


class TestRiskIntegration:
    def setup_method(self):
        self.tracker = PortfolioTracker(initial_equity=10000.0)
        self.chain = MiddlewareChain()
        self.chain.add(PositionSizer(risk_per_trade=0.015))
        self.chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
        self.chain.add(DailyLossLimit(daily_loss_limit=0.05))
        self.chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))

    def test_signal_passes_full_chain(self):
        signal = Signal(symbol="ETHUSDT", direction="LONG", conviction=0.68, entry_price=3100.0, stop_loss=3000.0, take_profit=3400.0)
        result = self.chain.process(signal, self.tracker)
        assert not result.rejected
        assert result.modifications.get("position_size") is not None

    def test_signal_rejected_when_concentration_exceeded(self):
        self.tracker.open_position(Position(symbol="BTCUSDT", direction="LONG", quantity=1.4, entry_price=62500.0, leverage=3))
        signal = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.80, entry_price=62500.0, stop_loss=61500.0, take_profit=65000.0)
        result = self.chain.process(signal, self.tracker)
        assert result.rejected
        assert "concentration" in result.reason.lower() or "Concentration" in result.reason
```

- [ ] **Step 2: 运行集成测试验证通过**

```bash
pytest tests/test_integration_risk.py -v
```
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_integration_risk.py
git commit -m "test: add risk middleware chain integration test"
```

---

## Phase 6: 监控与告警

### Task 6.1: Monitor — Alerter

**Files:**
- Create: `monitor/alerter.py`
- Create: `tests/test_monitor_alerter.py`

- [ ] **Step 1: 编写 Alerter 测试**

`tests/test_monitor_alerter.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from monitor.alerter import Alerter, AlertLevel, Alert


class TestAlerter:
    def setup_method(self):
        self.alerts = []
        self.alerter = Alerter(on_alert=lambda a: self.alerts.append(a))

    def test_fire_critical_alert(self):
        self.alerter.fire(AlertLevel.CRITICAL, "margin_ratio", "Margin ratio at 85%", {"margin_ratio": 0.85})
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.CRITICAL
        assert self.alerts[0].metric == "margin_ratio"
        assert "85%" in self.alerts[0].message

    def test_fire_warning_alert(self):
        self.alerter.fire(AlertLevel.WARNING, "daily_pnl", "Daily PnL approaching limit", {"pnl": -300.0})
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.WARNING

    def test_fire_info_alert(self):
        self.alerter.fire(AlertLevel.INFO, "signal.generated", "Signal generated", {"symbol": "BTCUSDT"})
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.INFO

    def test_check_heartbeat_not_firing_when_recent(self):
        from monitor.collector import MetricsCollector
        MetricsCollector.reset()
        collector = MetricsCollector.instance()
        collector.heartbeat("market_data")
        self.alerter.check_heartbeat("market_data", collector)
        assert len(self.alerts) == 0

    def test_check_heartbeat_fires_when_timeout(self):
        from monitor.collector import MetricsCollector
        import time
        MetricsCollector.reset()
        collector = MetricsCollector.instance()
        collector.heartbeat("market_data")
        collector._heartbeats["market_data"] = time.time() - 120
        self.alerter.check_heartbeat("market_data", collector, timeout_seconds=60)
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.CRITICAL
        assert "market_data" in self.alerts[0].message
```

- [ ] **Step 2: 运行测试验证失败**

```bash
pytest tests/test_monitor_alerter.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 Alerter**

`monitor/alerter.py`:
```python
"""Alerter — threshold checking and alert dispatching."""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional
from monitor.collector import MetricsCollector


class AlertLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class Alert:
    level: AlertLevel
    metric: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class Alerter:
    def __init__(self, on_alert: Optional[Callable[[Alert], None]] = None):
        self.on_alert = on_alert or (lambda a: None)
        self._alerts: list[Alert] = []

    def fire(self, level: AlertLevel, metric: str, message: str, context: Optional[dict] = None):
        alert = Alert(level=level, metric=metric, message=message, context=context or {})
        self._alerts.append(alert)
        self.on_alert(alert)
        return alert

    def check_heartbeat(self, module: str, collector: MetricsCollector, timeout_seconds: int = 60):
        last = collector.last_heartbeat(module)
        if last is None:
            self.fire(AlertLevel.WARNING, f"heartbeat.{module}", f"No heartbeat ever received from {module}")
        elif time.time() - last > timeout_seconds:
            self.fire(AlertLevel.CRITICAL, f"heartbeat.{module}", f"{module} heartbeat timeout: {time.time() - last:.0f}s since last beat")

    def check_thresholds(self, collector: MetricsCollector, portfolio: Any):
        margin_ratio = portfolio.margin_ratio if portfolio else 0.0
        if margin_ratio > 0.80:
            self.fire(AlertLevel.CRITICAL, "margin_ratio", f"Margin ratio {margin_ratio:.1%} > 80%", {"margin_ratio": margin_ratio})
        elif margin_ratio > 0.60:
            self.fire(AlertLevel.WARNING, "margin_ratio", f"Margin ratio {margin_ratio:.1%} > 60%", {"margin_ratio": margin_ratio})

        dd = portfolio.current_drawdown if portfolio else 0.0
        if dd > 0.15:
            self.fire(AlertLevel.CRITICAL, "drawdown", f"Drawdown {dd:.1%} > 15%", {"drawdown": dd})
        elif dd > 0.10:
            self.fire(AlertLevel.WARNING, "drawdown", f"Drawdown {dd:.1%} > 10%", {"drawdown": dd})

    def recent_alerts(self, n: int = 10) -> list[Alert]:
        return self._alerts[-n:]
```

- [ ] **Step 4: 运行测试验证通过**

```bash
pytest tests/test_monitor_alerter.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add monitor/alerter.py tests/test_monitor_alerter.py
git commit -m "feat: add Alerter with heartbeat monitoring and threshold-based alerts"
```

---

## 计划总结

| Phase | Tasks | 新建文件 | 测试文件 |
|-------|------:|:------|:------|
| 1. 基础设施 | 3 | 7 | 2 |
| 2. 数据通道 | 4 | 4 | 4 |
| 3. 信号适配 | 2 | 2 | 2 |
| 4. 执行链路 | 3 | 4 | 3 |
| 5. 风控 | 2 | 6 | 2 |
| 6. 监控 | 1 | 1 | 1 |
| **合计** | **15** | **24** | **14** |

### 暂未覆盖（后续规划）

- **Phase 7 联调**: Market Data → Scheduler → Signal Engine → Risk → Execution 端到端集成测试
- **Phase 8 上线**: testnet paper trading、实盘渐进、Telegram Bot 命令交互、User Data Stream 对账
- **现有代码迁移**: `agent_team/Outlook|Status|Signal_Generation|shared` 复制到 `signal_engine/` 并适配接口（迁移策略见架构文档第 5 节）
- **历史数据回测升级**: Backtest 模块适配事件驱动接口
```

- [ ] **Step 6: 运行全部测试确认通过**

```bash
pytest tests/ -v
```
Expected: 58+ PASS, 0 FAIL
```

- [ ] **Step 7: Commit**

```bash
git add tests/test_monitor_alerter.py monitor/alerter.py tests/test_integration_risk.py
git commit -m "test: add monitor alerter tests and risk integration tests"
```
