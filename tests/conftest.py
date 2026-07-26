"""
pytest 共享 Fixture — 所有测试文件共用。

使用方式:
  def test_something(mock_feed, portfolio, risk_chain, db):
      # 直接使用这些 fixture
"""

import os
import sys
import json
from unittest.mock import MagicMock

import pytest
from signal_engine.engine import Signal
from risk.chain import MiddlewareChain
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from portfolio.tracker import PortfolioTracker, Position
from shared.database import TradeDatabase, TradeRecord
from market_data.kline_buffer import KlineBuffer, Kline
from market_data.feed import MarketDataFeed


# ─── 路径 ───
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ─── 标记 ───

def pytest_configure(config):
    """注册自定义标记"""
    config.addinivalue_line("markers", "unit: 单元测试，不依赖外部服务")
    config.addinivalue_line("markers", "integration: 集成测试，依赖模块间协作")
    config.addinivalue_line("markers", "e2e: 端到端测试，依赖 testnet/实盘")
    config.addinivalue_line("markers", "slow: 运行时间较长的测试")


# ─── 基础 Fixture ───

@pytest.fixture
def mock_feed():
    """可控制价格的模拟行情源"""
    feed = MagicMock()
    feed.get_last_price.return_value = 64000.0
    feed.get_mark_price.return_value = 64000.0
    feed.buffer.get_klines.return_value = []
    feed.buffer.count.return_value = 0
    return feed


@pytest.fixture
def mock_gateway():
    """模拟下单网关"""
    gw = MagicMock()
    gw.place_order.return_value.status = "FILLED"
    gw.place_algo_order.return_value.status = "NEW"
    return gw


@pytest.fixture
def portfolio():
    """空持仓账户"""
    return PortfolioTracker(initial_equity=10000.0)


@pytest.fixture
def portfolio_with_positions(portfolio):
    """已有 BTC/ETH 持仓的账户"""
    portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
    portfolio.open_position(Position("ETHUSDT", "SHORT", 1.0, 3000.0, 3))
    return portfolio


@pytest.fixture
def db():
    """内存数据库"""
    return TradeDatabase(":memory:")


@pytest.fixture
def kline_buffer():
    """K 线缓存"""
    return KlineBuffer(max_size=500)


@pytest.fixture
def signal():
    """标准多头信号"""
    return Signal(
        symbol="BTCUSDT", direction="LONG", conviction=0.72,
        entry_price=64000.0, stop_loss=63000.0, take_profit=66000.0,
    )


@pytest.fixture
def short_signal():
    """标准空头信号"""
    return Signal(
        symbol="ETHUSDT", direction="SHORT", conviction=0.65,
        entry_price=3100.0, stop_loss=3150.0, take_profit=2950.0,
    )


@pytest.fixture
def risk_chain():
    """4 层风控中间件链"""
    chain = MiddlewareChain()
    chain.add(PositionSizer(risk_per_trade=0.015))
    chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
    chain.add(DailyLossLimit(daily_loss_limit=0.05))
    chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))
    return chain


# ─── 辅助函数 ───

def make_kline(symbol: str = "BTCUSDT", timeframe: str = "4h",
               open_time: int = 1700000000000, close: float = 64000.0,
               is_closed: bool = False) -> Kline:
    """快速构造一条 K 线"""
    return Kline(
        symbol=symbol, timeframe=timeframe,
        open_time=open_time, close_time=open_time + 14400000,
        open=close - 100, high=close + 200, low=close - 300,
        close=close, volume=100.0, is_closed=is_closed,
    )
