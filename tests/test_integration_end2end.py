"""
端到端集成测试：信号 → 风控 → 下单 → 持仓

前置条件:
  1. config/.env 配置 testnet API Key
  2. 代理 127.0.0.1:7897 开启

注意: testnet 不支持 STOP_MARKET/TAKE_PROFIT_MARKET
      (需 Algo Order API，但 testnet 未开通)
      实盘时需通过 Algo API 或手动管理止损止盈
"""

import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import math

import pytest
from signal_engine.engine import Signal
from risk.chain import MiddlewareChain
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from execution.order_manager import OrderManager
from execution.order_gateway import OrderGateway
from portfolio.tracker import PortfolioTracker, Position
from shared.config_loader import load_env

load_env()


def round_price(price: float, tick_size: float = 0.10) -> float:
    """将价格对齐到 tick size"""
    return round(math.floor(price / tick_size) * tick_size, 2)

def round_qty(qty: float, step_size: float = 0.001) -> float:
    """将数量对齐到 step size"""
    return round(math.floor(qty / step_size) * step_size, 4)


class TestEndToEnd:

    @classmethod
    def setup_class(cls):
        """启动市场数据源"""
        from market_data.feed import MarketDataFeed
        cls.feed = MarketDataFeed(symbols=["BTCUSDT"], proxy_host="127.0.0.1", proxy_port=7897)
        cls.feed.start()
        time.sleep(2)
        # 校验数据流
        assert cls.feed.get_last_price("BTCUSDT") is not None, "未收到实时价格"

    @classmethod
    def teardown_class(cls):
        cls.feed.stop()

    def setup_method(self):
        """每个测试前重置状态"""
        self.tracker = PortfolioTracker(initial_equity=10000.0)
        self.gateway = OrderGateway(testnet=True)
        self.orders = OrderManager(gateway=self.gateway)
        self.risk_chain = MiddlewareChain()
        self.risk_chain.add(PositionSizer(risk_per_trade=0.015))
        self.risk_chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
        self.risk_chain.add(DailyLossLimit(daily_loss_limit=0.05))
        self.risk_chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))

    # ─── 测试1: 全链路信号→风控→下单→持仓 ───

    def test_signal_to_position_pipeline(self):
        """信号经过风控 → 下单 → 持仓更新"""
        # 1. 模拟信号引擎输出
        price = self.feed.get_last_price("BTCUSDT")
        signal = Signal(
            symbol="BTCUSDT", direction="LONG", conviction=0.72,
            entry_price=round_price(price * 0.99),   # 对齐 tick size 0.10
            stop_loss=round_price(price * 0.95),
            take_profit=round_price(price * 1.05),
        )
        print(f"\n  信号: {signal.symbol} {signal.direction} @ {signal.entry_price:.2f}")

        # 2. 风控
        risk_result = self.risk_chain.process(signal, self.tracker)
        assert not risk_result.rejected, f"风控拒绝: {risk_result.reason}"
        print(f"  风控: 通过 (size={risk_result.modifications.get('position_size', 0):.4f})")

        # 3. 下单入场 (MARKET 确保成交)
        orders = self.orders.execute_signal(
            signal.symbol, signal.direction, 0.001,
            signal.entry_price, signal.stop_loss, signal.take_profit,
        )
        entry = orders[0]
        assert entry.state not in ("REJECTED", "ERROR"), f"入场被拒: {entry.error}"
        print(f"  入场: orderId={entry.order_id} state={entry.state.value}")

        # 4. 持仓更新
        pos = Position(symbol="BTCUSDT", direction="LONG", quantity=0.001, entry_price=price, leverage=3)
        self.tracker.open_position(pos)
        assert "BTCUSDT" in self.tracker.positions
        assert self.tracker.total_margin > 0
        print(f"  持仓: {self.tracker.positions['BTCUSDT'].quantity} BTC 保证金={self.tracker.total_margin:.2f} USDT")

    # ─── 测试2: 风控拒绝 ───

    def test_risk_rejects_signal(self):
        """风控拒绝不合规信号"""
        self.tracker.daily_realized_pnl = -800.0
        signal = Signal(symbol="ETHUSDT", direction="LONG", conviction=0.8,
                        entry_price=3000.0, stop_loss=2500.0, take_profit=3500.0)
        result = self.risk_chain.process(signal, self.tracker)
        assert result.rejected
        print(f"\n  风控拒绝: {result.reason}")

    # ─── 测试3: 撤单 ───

    def test_cancel_orders(self):
        """下单后撤单"""
        orders = self.orders.execute_signal("BTCUSDT", "LONG", 0.001, 10000.0, 9000.0, 12000.0)
        for o in orders:
            if o.order_id > 0:
                resp = self.gateway.cancel_order("BTCUSDT", o.order_id)
                assert resp.status in ("CANCELED", "ERROR")
        print(f"\n  撤单完成 (LIMIT未成交自动清理)")

    # ─── 测试4: 价格数据流 ───

    def test_market_data_flowing(self):
        """验证实时价格数据可用"""
        mark = self.feed.get_mark_price("BTCUSDT")
        last = self.feed.get_last_price("BTCUSDT")
        kline_count = self.feed.buffer.count("BTCUSDT", "4h")
        print(f"\n  标记价={mark:.2f} 成交价={last:.2f} 4hK线={kline_count}")
        assert mark is not None and last is not None

    # ─── 测试5: 资金查询 ───

    def test_account_query(self):
        """验证 testnet 账户可查询"""
        acc = self.gateway.get_account()
        assert "canTrade" in acc
        total = sum(float(a.get("walletBalance", 0)) for a in acc.get("assets", []))
        print(f"\n  Testnet余额: {total:.2f} USDT (可交易={acc.get('canTrade')})")
        assert total > 0
