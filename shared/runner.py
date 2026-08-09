"""系统主入口 — 统一装配: 策略 + 风控 + OrderManager + K线接线。

生命周期:
  - 启动前校验 (PreflightChecker) / 订单幂等 (IdempotencyTracker) / 持续对账 (PositionReconciler)
  - 统一装配: SignalEngine (策略链) + MiddlewareChain (风控 4 件套) + OrderManager (执行层)
  - 15m K线闭合 → 信号 → 风控 → execute_signal 下单全链路
  - 数据停滞 / WS 断连检测 + 网络诊断

运行:
  python -m shared.runner --hours 24            # 限时 24h (testnet 真实下单)
  python -m shared.runner                       # 无限运行 (生产默认)
"""

import argparse
import logging
import os
import signal
import sys
import time
from typing import Optional

import pandas as pd

import signal_engine.scalping_strategy  # noqa: F401 注册15m剥头皮策略
from execution.order_gateway import OrderGateway
from execution.order_manager import OrderManager, OrderState
from execution.order_utils import align_qty_to_step
from market_data.feed import MarketDataFeed
from portfolio.tracker import PortfolioTracker, Position
from risk.chain import MiddlewareChain
from risk.concentration import ConcentrationCheck
from risk.daily_loss_limit import DailyLossLimit
from risk.drawdown_breaker import DrawdownBreaker
from risk.position_sizer import PositionSizer
from shared.config_loader import load_env
from shared.execution_mode import ExecutionMode, ExecutionModeManager
from shared.idempotency import IdempotencyTracker
from monitor.collector import MetricsCollector
from shared.logging import setup_logging
from shared.preflight import PreflightChecker
from shared.reconciler import PositionReconciler
from signal_engine.engine import SignalEngine
from signal_engine.interface import StrategyRegistry

logger = logging.getLogger(__name__)

STALE_THRESHOLD = 120

# Algo Order API 可用性探测 (2026-08-09):
#   GET https://testnet.binancefuture.com/fapi/v1/exchangeInfo via proxy 127.0.0.1:7897
#     → HTTP 200, 731 symbols, BTCUSDT 存在 (testnet 可达)
#   GET /fapi/v1/algoOrder?symbol=BTCUSDT (未带签名)
#     → HTTP 401 {"code":-2014,"msg":"API-key format invalid."}
#     → 端点存在 (非 404/连接失败)，签名后可用。止损/止盈条件单走此端点
#       (OrderManager.submit_stop_loss / submit_take_profit)。


class SystemRunner:
    """交易系统主控 — 统一装配策略/风控/执行层，管理模块生命周期。"""

    def __init__(self, testnet: bool = True, symbols: Optional[list] = None,
                 strategy_name: str = "scalping_15m",
                 execution_mode_name: str = "live",
                 risk_per_trade: float = 0.015, hours: int = 0,
                 instance: str = "live", event_bus=None):
        self.testnet = testnet
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.strategy_name = strategy_name
        self.execution_mode_name = execution_mode_name
        self.risk_per_trade = risk_per_trade
        self.hours = hours  # 0 = 无限运行（生产）
        self.instance = instance
        self.event_bus = event_bus  # 事件总线注入（可选，None 时静默）
        self.feed: Optional[MarketDataFeed] = None
        self.portfolio: Optional[PortfolioTracker] = None
        self.gateway: Optional[OrderGateway] = None
        self.idempotency: Optional[IdempotencyTracker] = None
        self.reconciler: Optional[PositionReconciler] = None
        self.heartbeat: Optional["HeartbeatPublisher"] = None
        self.engine: Optional[SignalEngine] = None
        self.risk_chain: Optional[MiddlewareChain] = None
        self.orders: Optional[OrderManager] = None
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

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info("Received %s, shutting down...", signal.Signals(signum).name)
        self.stop()

    def initialize(self):
        self.gateway = OrderGateway(testnet=self.testnet)
        self.portfolio = PortfolioTracker(event_bus=self.event_bus, instance=self.instance)
        # 注入优先：测试/嵌入式场景可预置 feed，仍强制接线 K线闭合回调
        if self.feed is None:
            self.feed = MarketDataFeed(
                symbols=self.symbols,
                proxy_host="127.0.0.1", proxy_port=7897,
                redundant_connections=8,
                on_kline_closed=self._on_kline_closed,
            )
        else:
            self.feed.on_kline_closed = self._on_kline_closed
        self.idempotency = IdempotencyTracker(
            db_path=os.environ.get("INTENTS_DB_PATH", "data/intents.db")
        )

        # 启动前校验 (单次 get_account, 缓存结果)
        preflight = PreflightChecker(self.gateway)
        acc = preflight.run_all()
        if acc is None:
            raise RuntimeError("Preflight checks failed")

        # 用校验时的账户数据初始化权益, 无需再调 API
        total = sum(float(a.get("walletBalance", 0)) for a in acc.get("assets", []))
        self.portfolio.update_equity(total)
        logger.info("Account equity: %.2f USDT", total)

        # 统一装配: 执行层 + 信号链 + 风控链
        mode = ExecutionModeManager(ExecutionMode(self.execution_mode_name.lower()))
        self.orders = OrderManager(
            gateway=self.gateway, execution_mode=mode,
            event_bus=self.event_bus, instance=self.instance,
        )
        self.engine = self._build_signal_chain()
        self.risk_chain = self._build_risk_chain()
        self.step_sizes = self._fetch_step_sizes()
        logger.info("stepSize: %s", self.step_sizes or "获取失败(下单将退化为4位小数)")

        # 启动行情
        self.feed.start()
        time.sleep(2)

        # 启动时对账 (使用缓存的账户数据)
        reconciler = PositionReconciler(self.gateway, self.portfolio)
        reconciler.reconcile(cached_account=acc)

        # 持续对账
        self.reconciler = reconciler
        self.reconciler.start()

        # 心跳发布线程: 周期读取 MetricsCollector 各模块心跳并发布 heartbeat 事件
        from shared.heartbeat_publisher import HeartbeatPublisher
        self.heartbeat = HeartbeatPublisher(self.event_bus, instance=self.instance)
        self.heartbeat.start()

        # 回填历史数据 + 记录数据时间戳
        self.feed.backfill(limit=200)
        for sym in self.symbols:
            self._last_data_ts[sym] = time.time()
        # 模块心跳: 覆盖启动窗口 (initialize 完成即标记 runner 存活)
        MetricsCollector.instance().heartbeat("runner")
        logger.info("System initialized")

    # ─── 统一装配 ───

    def _build_signal_chain(self) -> SignalEngine:
        """信号链: 按名称从 StrategyRegistry 取策略实例。"""
        return SignalEngine(
            strategy=StrategyRegistry.get(self.strategy_name),
            event_bus=self.event_bus, instance=self.instance,
        )

    def _build_risk_chain(self) -> MiddlewareChain:
        """风控链: 仓位 → 回撤 → 日亏损 → 集中度。"""
        chain = MiddlewareChain(event_bus=self.event_bus, instance=self.instance)
        chain.add(PositionSizer(risk_per_trade=self.risk_per_trade))
        chain.add(DrawdownBreaker(
            max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120
        ))
        chain.add(DailyLossLimit(daily_loss_limit=0.05))
        chain.add(ConcentrationCheck(
            max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80
        ))
        return chain

    # ─── 信号链: K线闭合 → 信号 → 风控 → 下单 ───

    def _on_kline_closed(self, symbol: str, timeframe: str, ohlcv):
        """K线闭合 → 信号 → 风控 → 下单 (时间框架由策略决定)。"""
        tf = getattr(getattr(self.engine, "strategy", None), "timeframe", "15m")
        if timeframe != tf:
            return
        self.stats["kline_closes"] += 1
        df = pd.DataFrame([{
            "open": k.open, "high": k.high, "low": k.low,
            "close": k.close, "volume": k.volume,
        } for k in ohlcv])
        try:
            signal = self.engine.run(symbol, tf, df.to_dict("records"))
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
        """风控 → 下单 (OrderManager 完整链路: 入场 LIMIT + 止损/止盈条件单)。"""
        # 持仓去重: 已有该 symbol 持仓或 PENDING 入场单时跳过, 避免叠单
        if signal.symbol in self.portfolio.positions:
            logger.info("SKIP %s: 已有持仓, 跳过重复开仓", signal.symbol)
            return
        if any(o.symbol == signal.symbol and o.state == OrderState.PENDING
               for o in self.orders.active_orders):
            logger.info("SKIP %s: 已有 PENDING 入场单, 跳过重复开仓", signal.symbol)
            return
        result = self.risk_chain.process(signal, self.portfolio)
        if result.rejected:
            self.stats["risk_rejected"] += 1
            logger.warning("RISK REJECTED %s %s: %s",
                           signal.symbol, signal.direction, result.reason)
            return
        size = result.modifications.get("position_size", 0.001)
        # 限制名义价值在 5-100 USDT 之间（保底满足交易所最小 5 USDT 要求）
        price = self.feed.get_last_price(signal.symbol) or signal.entry_price
        min_qty = 5.0 / price if price else 0.001
        max_qty = 100.0 / price if price else 0.01
        step = self.step_sizes.get(signal.symbol, 0.0)
        qty = align_qty_to_step(size, step, min_qty, max_qty)
        try:
            orders = self.orders.execute_signal(
                signal.symbol, signal.direction, qty,
                signal.entry_price, signal.stop_loss, signal.take_profit,
            )
            # 成功判定以入场单 (列表第 1 个) 的 state 为唯一判据:
            # OrderManager 将 FILLED/NEW 均映射为 PENDING, 仅 REJECTED/ERROR 为失败
            entry = orders[0] if orders else None
            if entry is None or entry.state in (OrderState.REJECTED, OrderState.ERROR):
                self.stats["orders_failed"] += 1
                err = entry.error if entry else "no orders returned"
                logger.error("ORDER FAILED %s: %s", signal.symbol, err)
                return
            # 止损/止盈被拒: 单独告警, 不影响 orders_placed 统计
            protection = [
                o for o in orders[1:]
                if o.state in (OrderState.REJECTED, OrderState.ERROR)
            ]
            if protection:
                logger.warning(
                    "SL/TP PROTECTION REJECTED %s: %s", signal.symbol,
                    "; ".join(f"{o.order_type}={o.error}" for o in protection),
                )
            self.stats["orders_placed"] += 1
            self.portfolio.open_position(Position(
                symbol=signal.symbol, direction=signal.direction,
                quantity=qty, entry_price=signal.entry_price, leverage=3,
            ))
            logger.info("ORDER PLACED %s %s qty=%s entry=%.2f",
                        signal.symbol, signal.direction, qty, signal.entry_price)
        except Exception as e:
            self.stats["orders_failed"] += 1
            logger.error("ORDER EXCEPTION %s: %s", signal.symbol, e)

    # ─── 健康监控 ───

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
        conns = self.feed._conns if self.feed else []
        connected = sum(1 for c in conns if c.connected)
        logger.info(
            "SNAPSHOT t=%.0fm | prices=%s | ws=%d/%d | closes=%d | sig=%d rej=%d order=%d/%d | stalls=%d",
            elapsed / 60,
            {k: (round(v, 1) if v else None) for k, v in prices.items()},
            connected, len(conns),
            self.stats["kline_closes"], self.stats["signals"], self.stats["risk_rejected"],
            self.stats["orders_placed"], self.stats["orders_failed"],
            self.stats["stalls"],
        )

    def report(self):
        elapsed = time.time() - self.stats["start_time"]
        logger.info("=== 运行结束 ===")
        logger.info("运行时长: %.1f 小时", elapsed / 3600)
        logger.info("信号数: %d (%.2f/天)", self.stats["signals"],
                    self.stats["signals"] / (elapsed / 3600) * 24 if elapsed else 0)
        logger.info("风控拒绝: %d", self.stats["risk_rejected"])
        logger.info("下单成功: %d | 下单失败: %d",
                    self.stats["orders_placed"], self.stats["orders_failed"])
        logger.info("K线闭合: %d | 数据停滞: %d", self.stats["kline_closes"], self.stats["stalls"])
        logger.info("当前持仓: %s",
                    {s: p.direction for s, p in self.portfolio.positions.items()})
        ok = self.stats["stalls"] == 0 and self.stats["orders_failed"] == 0
        logger.info("结论: %s", "✅ 稳定" if ok else "⚠️ 存在问题")
        if self.feed:
            self.feed.stop()

    # ─── 生命周期 ───

    def run_forever(self):
        logger.info("System running (PID=%d)", os.getpid())
        end_time = time.time() + self.hours * 3600 if self.hours > 0 else None
        last_snapshot = time.time()
        try:
            while True:
                # 模块心跳: 主循环每轮标记 runner 存活
                MetricsCollector.instance().heartbeat("runner")
                time.sleep(5)
                self._check_stall()
                self._check_connections()
                if time.time() - last_snapshot >= 60:
                    self._snapshot()
                    last_snapshot = time.time()
                if end_time is not None and time.time() >= end_time:
                    logger.info("运行时长已到 (%dh), 结束", self.hours)
                    self.report()
                    return
        except KeyboardInterrupt:
            logger.info("手动中断")
            self.report()

    def stop(self):
        logger.info("Shutting down...")
        if self.reconciler:
            self.reconciler.stop()
        if self.feed:
            self.feed.stop()
        if self.idempotency:
            self.idempotency.close()
        if self.heartbeat:
            self.heartbeat.stop()
        logger.info("Shutdown complete")
        sys.exit(0)

    @property
    def healthy(self) -> bool:
        return (self.feed is not None
                and self.feed.get_last_price("BTCUSDT") is not None)


def main():
    parser = argparse.ArgumentParser(description="交易系统主入口 (默认 testnet)")
    parser.add_argument("--strategy", default="scalping_15m", help="策略名称 (注册于 StrategyRegistry)")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT", help="逗号分隔的标的列表")
    parser.add_argument("--execution-mode", default="live", choices=["dry_run", "paper", "live"])
    parser.add_argument("--hours", type=int, default=0, help="运行时长(小时), 0=无限运行")
    parser.add_argument("--testnet", dest="testnet", action="store_true", default=True)
    parser.add_argument("--no-testnet", dest="testnet", action="store_false", help="连接实盘 (慎用)")
    parser.add_argument("--risk-per-trade", type=float, default=0.015, help="单笔风险比例")
    parser.add_argument("--instance", default="live", help="实例标识")
    args = parser.parse_args()
    load_env()
    setup_logging()
    runner = SystemRunner(
        testnet=args.testnet,
        symbols=args.symbols.split(","),
        strategy_name=args.strategy,
        execution_mode_name=args.execution_mode,
        risk_per_trade=args.risk_per_trade,
        hours=args.hours,
        instance=args.instance,
    )
    try:
        runner.initialize()
        runner.run_forever()
    except Exception:
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
