"""稳定性测试运行器 — 高频策略 + testnet 真实下单，跑 24 小时。

功能:
  - 启动 MarketDataFeed (4路WS) + 回填历史数据
  - 15m K线闭合 → scalping_15m 策略 → 信号 → 风控 → testnet 真实下单
  - 每分钟记录状态快照
  - 检测: 数据停滞 / WS 断连 / 下单失败 / 信号生成

运行:
  python tools/stability_test.py --hours 24

输出:
  logs/systrader.log  — 全量日志
"""

import argparse
import logging
import os
import sys
import time
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from market_data.feed import MarketDataFeed
import signal_engine.scalping_strategy  # noqa: F401 注册15m剥头皮策略
from signal_engine.interface import StrategyRegistry
from signal_engine.engine import SignalEngine
from risk.chain import MiddlewareChain
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from execution.order_manager import OrderManager
from execution.order_gateway import OrderGateway, OrderRequest
from execution.order_utils import align_qty_to_step
from portfolio.tracker import PortfolioTracker, Position
from shared.config_loader import load_env
from shared.execution_mode import ExecutionMode, ExecutionModeManager
from shared.logging import setup_logging

load_env()
logger = logging.getLogger("stability")

STALE_THRESHOLD = 120
TESTNET = True  # testnet 实盘


class StabilityRunner:
    def __init__(self, symbols: list, hours: int = 24):
        self.symbols = symbols
        self.hours = hours
        self.feed: Optional[MarketDataFeed] = None
        self.engine: Optional[SignalEngine] = None
        self.risk_chain = MiddlewareChain()
        self.risk_chain.add(PositionSizer(risk_per_trade=0.015))
        self.risk_chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
        self.risk_chain.add(DailyLossLimit(daily_loss_limit=0.05))
        self.risk_chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))
        self.portfolio = PortfolioTracker(initial_equity=10000.0)
        self.gateway = OrderGateway(testnet=TESTNET)
        # testnet 真实下单：显式 LIVE（OrderManager 默认 DRY_RUN）
        self.orders = OrderManager(gateway=self.gateway, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))

        self.step_sizes: dict = {}  # symbol -> stepSize
        self.stats = {
            "signals": 0,
            "risk_rejected": 0,
            "orders_placed": 0,
            "orders_failed": 0,
            "kline_closes": 0,
            "stalls": 0,
            "start_time": time.time(),
        }
        self._last_data_ts: dict = {}
        self._last_close_ts: dict = {}

    def initialize(self):
        logger.info("=== 稳定性测试启动 (testnet真实下单) ===")
        logger.info("标的: %s | 时长: %dh | 策略: scalping_15m", self.symbols, self.hours)
        self.feed = MarketDataFeed(
            symbols=self.symbols,
            proxy_host="127.0.0.1", proxy_port=7897,
            redundant_connections=8,
            on_kline_closed=self._on_kline_closed,
        )
        self.feed.start()
        time.sleep(3)
        logger.info("获取各标的 stepSize...")
        self.step_sizes = self._fetch_step_sizes()
        logger.info("stepSize: %s", self.step_sizes or "获取失败(下单将退化为4位小数)")
        logger.info("回填历史数据...")
        self.feed.backfill(limit=200)
        self.engine = SignalEngine(strategy=StrategyRegistry.get("scalping_15m"))
        logger.info("信号引擎就绪")
        for sym in self.symbols:
            self._last_data_ts[sym] = time.time()

    def _on_kline_closed(self, symbol: str, timeframe: str, ohlcv):
        """1h K线闭合 → 信号 → 风控 → 下单。"""
        if timeframe != "15m":
            return
        self.stats["kline_closes"] += 1
        self._last_close_ts[symbol] = time.time()
        df = pd.DataFrame([{
            "open": k.open, "high": k.high, "low": k.low,
            "close": k.close, "volume": k.volume,
        } for k in ohlcv])
        try:
            signal = self.engine.run(symbol, "15m", df.to_dict("records"))
            if signal is None:
                return
            self.stats["signals"] += 1
            logger.info("SIGNAL %s %s conviction=%.2f entry=%.2f sl=%.2f tp=%.2f",
                        symbol, signal.direction, signal.conviction,
                        signal.entry_price, signal.stop_loss, signal.take_profit)
            self._execute_signal(signal)
        except Exception as e:
            logger.error("Signal engine error: %s", e)

    def _execute_signal(self, signal):
        """风控 → 下单 (testnet MARKET 单)。"""
        result = self.risk_chain.process(signal, self.portfolio)
        if result.rejected:
            self.stats["risk_rejected"] += 1
            logger.warning("RISK REJECTED %s %s: %s", signal.symbol, signal.direction, result.reason)
            return
        size = result.modifications.get("position_size", 0.001)
        # 限制名义价值在 5-100 USDT 之间（保底满足交易所最小 5 USDT 要求）
        price = self.feed.get_last_price(signal.symbol) or signal.entry_price
        min_qty = 5.0 / price if price else 0.001
        max_qty = 100.0 / price if price else 0.01
        step = self.step_sizes.get(signal.symbol, 0.0)
        qty = align_qty_to_step(size, step, min_qty, max_qty)
        side = "BUY" if signal.direction == "LONG" else "SELL"
        req = OrderRequest(symbol=signal.symbol, side=side, order_type="MARKET", quantity=qty)
        try:
            resp = self.gateway.place_order(req)
            if resp.status in ("FILLED", "NEW"):
                self.stats["orders_placed"] += 1
                self.portfolio.open_position(Position(
                    symbol=signal.symbol, direction=signal.direction,
                    quantity=resp.executed_qty or qty, entry_price=resp.avg_price or signal.entry_price,
                    leverage=3,
                ))
                logger.info("ORDER FILLED %s %s qty=%s price=%s id=%s",
                            signal.symbol, side, qty, resp.avg_price, resp.order_id)
            else:
                self.stats["orders_failed"] += 1
                logger.error("ORDER FAILED %s: %s", signal.symbol, resp.error)
        except Exception as e:
            self.stats["orders_failed"] += 1
            logger.error("ORDER EXCEPTION %s: %s", signal.symbol, e)

    def _check_stall(self):
        for sym in self.symbols:
            last = self.feed.get_last_price(sym)
            now = time.time()
            if last is not None:
                self._last_data_ts[sym] = now
            elif now - self._last_data_ts[sym] > STALE_THRESHOLD:
                self.stats["stalls"] += 1
                self._last_data_ts[sym] = now
                logger.warning("STALL %s: 无数据 %ds", sym, STALE_THRESHOLD)
                self._network_diag(reason=f"stall_{sym}")

    def _check_connections(self):
        if not self.feed or not self.feed._conns:
            return
        connected = sum(1 for c in self.feed._conns if c.connected)
        if connected < len(self.feed._conns):
            logger.warning("WS 连接降级: %d/%d 在线", connected, len(self.feed._conns))
            self._network_diag(reason=f"ws_downgrade_{connected}_{len(self.feed._conns)}")

    def _fetch_step_sizes(self) -> dict:
        """从 exchangeInfo 获取各标的的最小下单粒度 stepSize。

        Binance 要求下单数量必须是 stepSize 的整数倍（精度限制）。
        各币种不同: BTC=0.0001, ETH=0.001, SOL=0.01。
        失败时返回空 dict，下单退化为 4 位小数对齐（仅 BTC 可用）。
        """
        import requests
        try:
            r = requests.get(
                f"{OrderGateway.BASE_URL_TESTNET}/fapi/v1/exchangeInfo",
                proxies={"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"},
                timeout=10,
            )
            info = r.json()
            result = {}
            for s in info.get("symbols", []):
                if s.get("symbol") in self.symbols:
                    for f in s.get("filters", []):
                        if f.get("filterType") == "LOT_SIZE":
                            result[s["symbol"]] = float(f["stepSize"])
                            break
            return result
        except Exception as e:
            logger.warning("获取 stepSize 失败: %s", e)
            return {}

    def _network_diag(self, reason: str):
        """断连/停滞时记录网络诊断，用于确凿判断根因。

        诊断项:
          1. ping 默认网关        — 本地链路是否通
          2. ping 223.5.5.5      — 本地到互联网是否通（国内DNS）
          3. Clash 端口 7897     — 代理进程是否活着
          4. Clash API 8765      — 代理池服务是否活着

        结果解读:
          - 网关不通          → 本地 WiFi/网络断开（本地网络问题实锤）
          - 网关通+223.5.5.5 不通 → 本地到互联网中断（ISP问题）
          - 223.5.5.5 通      → 本地网络正常 → 问题在代理节点
        """
        import subprocess

        def ping(host: str) -> str:
            try:
                r = subprocess.run(
                    ["ping", "-n", "1", "-w", "2000", host],
                    capture_output=True, text=True, timeout=5,
                    creationflags=subprocess.CREATE_NO_WINDOW,  # 不弹控制台窗口
                )
                return "OK" if r.returncode == 0 else "FAIL"
            except Exception:
                return "ERR"

        # 获取默认网关（从路由表）
        gateway = self._get_default_gateway()

        diag = {
            "gateway_ping": ping(gateway) if gateway else "no-gateway",
            "dns_223": ping("223.5.5.5"),
            "clash_7897": "OPEN" if self._port_open(7897) else "CLOSED",
            "proxy_pool_8765": "OPEN" if self._port_open(8765) else "CLOSED",
        }
        logger.warning(
            "NETDIAG reason=%s | gateway=%s(%s) dns223=%s clash=%s pool=%s",
            reason, gateway or "?", diag["gateway_ping"], diag["dns_223"],
            diag["clash_7897"], diag["proxy_pool_8765"],
        )

    @staticmethod
    def _get_default_gateway() -> Optional[str]:
        """从 Windows 路由表获取默认网关。"""
        import re
        import subprocess
        try:
            r = subprocess.run(
                ["route", "print", "0.0.0.0"],
                capture_output=True, text=True, timeout=5,
                creationflags=subprocess.CREATE_NO_WINDOW,  # 不弹控制台窗口
            )
            # 匹配 "0.0.0.0          0.0.0.0      192.168.1.1"
            m = re.search(r"0\.0\.0\.0\s+0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)", r.stdout)
            return m.group(1) if m else None
        except Exception:
            return None

    @staticmethod
    def _port_open(port: int) -> bool:
        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except Exception:
            return False

    def _snapshot(self):
        elapsed = time.time() - self.stats["start_time"]
        prices = {sym: self.feed.get_last_price(sym) for sym in self.symbols}
        connected = sum(1 for c in self.feed._conns if c.connected) if self.feed._conns else 0
        logger.info(
            "SNAPSHOT t=%.0fm | prices=%s | ws=%d/4 | closes=%d | sig=%d rej=%d order=%d/%d | stalls=%d",
            elapsed / 60,
            {k: (round(v, 1) if v else None) for k, v in prices.items()},
            connected,
            self.stats["kline_closes"], self.stats["signals"], self.stats["risk_rejected"],
            self.stats["orders_placed"], self.stats["orders_failed"],
            self.stats["stalls"],
        )

    def run(self):
        self.initialize()
        end_time = time.time() + self.hours * 3600
        last_snapshot = 0
        try:
            while time.time() < end_time:
                time.sleep(5)
                self._check_stall()
                self._check_connections()
                if time.time() - last_snapshot >= 60:
                    self._snapshot()
                    last_snapshot = time.time()
        except KeyboardInterrupt:
            logger.info("手动中断")
        finally:
            self.report()

    def report(self):
        elapsed = time.time() - self.stats["start_time"]
        logger.info("=== 稳定性测试结束 ===")
        logger.info("运行时长: %.1f 小时", elapsed / 3600)
        logger.info("信号数: %d (%.2f/天)", self.stats["signals"], self.stats["signals"] / (elapsed / 3600) * 24)
        logger.info("风控拒绝: %d", self.stats["risk_rejected"])
        logger.info("下单成功: %d | 下单失败: %d", self.stats["orders_placed"], self.stats["orders_failed"])
        logger.info("K线闭合: %d | 数据停滞: %d", self.stats["kline_closes"], self.stats["stalls"])
        logger.info("当前持仓: %s", {s: p.direction for s, p in self.portfolio.positions.items()})
        ok = self.stats["stalls"] == 0 and self.stats["orders_failed"] == 0
        logger.info("结论: %s", "✅ 稳定" if ok else "⚠️ 存在问题")
        if self.feed:
            self.feed.stop()


def main():
    parser = argparse.ArgumentParser(description="稳定性测试 (testnet下单)")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    args = parser.parse_args()
    setup_logging(log_dir="logs", json_console=False)
    runner = StabilityRunner(symbols=args.symbols.split(","), hours=args.hours)
    runner.run()


if __name__ == "__main__":
    main()
