"""系统主入口 — 统一装配: 策略 + 风控 + OrderManager + K线接线。

生命周期:
  - 启动前校验 (PreflightChecker) / 订单幂等 (IdempotencyTracker) / 持续对账 (PositionReconciler)
  - 统一装配: SignalEngine (策略链) + MiddlewareChain (风控 4 件套) + OrderManager (执行层)
  - 15m K线闭合 → 信号 → 风控 → execute_signal 下单全链路
  - 数据停滞 / WS 断连检测 + 网络诊断
  - 连续停滞判定 → 熔断停单 (--stall-strikes, 需手动 resume)
  - PENDING 订单超时自动撤单 (--pending-timeout-minutes)
  - 关键指标注册 MetricsCollector gauge, 经 heartbeat 发布供 watchdog 检测

运行:
  python -m shared.runner --hours 24            # 限时 24h (testnet 真实下单)
  python -m shared.runner                       # 无限运行 (生产默认)
  python -m shared.runner --stall-strikes 5 --pending-timeout-minutes 45  # 自定阈值
"""

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Optional

import pandas as pd

import signal_engine.scalping_strategy  # noqa: F401 注册15m剥头皮策略
from execution.order_gateway import OrderGateway, OrderRequest
from execution.order_manager import OrderManager, OrderState
from execution.order_utils import align_qty_to_step
from market_data.feed import MarketDataFeed
from portfolio.tracker import PortfolioTracker, Position
from risk.chain import MiddlewareChain
from risk.available_margin import AvailableMarginCheck
from risk.concentration import ConcentrationCheck
from risk.daily_loss_limit import DailyLossLimit
from risk.daily_trade_limit import DailyTradeLimit
from risk.drawdown_breaker import DrawdownBreaker
from risk.leverage import LeverageController
from risk.max_stop_distance import MaxStopDistance
from risk.position_sizer import PositionSizer
from shared.config_loader import load_env
from shared.execution_mode import ExecutionMode, ExecutionModeManager
from shared.funding_monitor import FundingRateMonitor
from shared.idempotency import IdempotencyTracker
from monitor.collector import MetricsCollector
from monitor.dingtalk import DingTalkNotifier
from monitor.alerter import Alert, AlertLevel
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
                 instance: str = "live", event_bus=None,
                 stall_strikes: int = 3,
                 pending_timeout_minutes: int = 30):
        self.testnet = testnet
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.strategy_name = strategy_name
        self.execution_mode_name = execution_mode_name
        self.risk_per_trade = risk_per_trade
        self.hours = hours  # 0 = 无限运行（生产）
        self.instance = instance
        self.event_bus = event_bus  # 事件总线注入（可选，None 时静默）
        self.stall_strikes = stall_strikes  # 连续停滞判定次数达到后熔断停单 (Ops T5)
        self.pending_timeout_minutes = pending_timeout_minutes  # PENDING 超时撤单 (Ops T5)
        self.feed: Optional[MarketDataFeed] = None
        self.user_stream = None  # UserDataStream (成交/余额推送, 2026-08-16)
        self.funding_monitor: Optional[FundingRateMonitor] = None
        self._dingtalk = None  # DingTalkNotifier (资金费告警等, 2026-08-16)
        self._last_stream_equity = 0.0  # 推送触发的权益刷新节流
        self.kline_archive = None  # KlineArchive (P2-1)
        self._orderbook = None  # OrderbookDepth (P2-2, 懒加载)
        self.portfolio: Optional[PortfolioTracker] = None
        self.gateway: Optional[OrderGateway] = None
        self.idempotency: Optional[IdempotencyTracker] = None
        self.reconciler: Optional[PositionReconciler] = None
        self.heartbeat: Optional["HeartbeatPublisher"] = None
        self.engine: Optional[SignalEngine] = None
        self.risk_chain: Optional[MiddlewareChain] = None
        self.orders: Optional[OrderManager] = None
        self.db = None  # TradeDatabase (订单/成交持久化, 2026-08-16 P1)
        self.step_sizes: dict = {}  # symbol -> stepSize
        self.tick_sizes: dict = {}  # symbol -> tickSize (价格精度)
        self.execution_mode = None  # ExecutionModeManager (initialize 时装配)
        # 全局杠杆上限 (环境变量/ setparam 热更新, 2026-08-16)
        try:
            self.max_leverage = int(os.environ.get("MAX_LEVERAGE", "5"))
        except (TypeError, ValueError):
            self.max_leverage = 5
        self.stats = {
            "signals": 0,
            "risk_rejected": 0,
            "orders_placed": 0,
            "orders_failed": 0,
            "kline_closes": 0,
            "stalls": 0,
            "start_time": time.time(),
        }
        self._stall_strikes: dict = {}  # symbol -> 连续停滞判定次数 (Ops T5)
        self._circuit_breaker = None  # 熔断态: "emergency_stop" / None (kill switch)
        self._command_thread: Optional[threading.Thread] = None
        self._fills_lock = threading.Lock()  # 成交登记串行化 (D1, 2026-08-16)
        self._stopped = False  # stop() 幂等保护 (2026-08-16)
        # ── 风控补强状态 (2026-08-16: 自动减仓/回撤分级/资金费对账/每日摘要) ──
        self._last_deleverage_ts = 0.0  # 保证金率自动减仓节流
        self._drawdown_reduce_armed = True  # 回撤减仓档位: 回撤回落 20% 后重新武装
        self._last_reduce_ts = 0.0  # 回撤减仓冷却
        self._dingtalk_at_mobiles = [
            m.strip() for m in os.environ.get("DINGTALK_AT_MOBILES", "").split(",")
            if m.strip()
        ]
        self._last_digest_date = ""  # 每日摘要去重 (YYYY-MM-DD)
        self._last_digest_check = 0.0
        self._last_funding_sync = 0.0
        self._funding_state_path = os.environ.get(
            "FUNDING_INCOME_STATE", "data/funding_income_state.json")
        self._funding_last_tran: Optional[int] = None  # None = 首次运行待播种
        # ── 第七轮补强状态 (2026-08-16): 清算价/多资产/强平流 ──
        self._multi_assets: Optional[bool] = None  # 多资产保证金模式 (None=未检测)
        self._last_position_risk_sync = 0.0
        self._adl_warned: set = set()  # ADL 告警去重 (symbol)
        self._last_force_alert: dict = {}  # symbol -> 上次强平告警时间 (节流)
        self._force_order_stream = None

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info("Received %s, shutting down...", signal.Signals(signum).name)
        self.stop()
        sys.exit(0)

    def initialize(self):
        self.gateway = OrderGateway(testnet=self.testnet)
        # 实际手续费率 (2026-08-16 #1): FEE_RATE=auto 时查 commissionRate,
        # 盈亏/保本价/资金费口径全部用真实往返费率, 不再硬编码 0.001
        fee_rate = self._resolve_fee_rate()
        self.portfolio = PortfolioTracker(
            event_bus=self.event_bus, instance=self.instance, fee_rate=fee_rate)
        # 注入优先：测试/嵌入式场景可预置 feed，仍强制接线 K线闭合回调
        if self.feed is None:
            # K线历史归档 (P2-1): KLINE_ARCHIVE=1 时闭合 K 线持久化到 data/kline.db
            archive = None
            if os.environ.get("KLINE_ARCHIVE", "0") == "1":
                from market_data.kline_archive import KlineArchive
                archive = KlineArchive(os.environ.get("KLINE_DB_PATH", "data/kline.db"))
                self.kline_archive = archive
            self.feed = MarketDataFeed(
                symbols=self.symbols,
                proxy_host=os.environ.get("PROXY_HOST", "127.0.0.1"),
                proxy_port=int(os.environ.get("PROXY_PORT", "7897")),
                redundant_connections=8,
                on_kline_closed=self._on_kline_closed,
                archive=archive,
            )
        else:
            self.feed.on_kline_closed = self._on_kline_closed
        self.idempotency = IdempotencyTracker(
            db_path=os.environ.get("INTENTS_DB_PATH", "data/intents.db")
        )
        # 订单/成交持久化 (2026-08-16 P1): 此前 OrderManager(db=None) 从未落库,
        # 交易日志/对账/TCA 分析全部失明。附带保留策略 (P1-6)。
        from shared.database import TradeDatabase
        self.db = TradeDatabase(os.environ.get("DB_PATH", "data/trades.db"))
        self.db.purge_orders(days=int(os.environ.get("DB_RETENTION_DAYS", "90")))
        self.db.purge_signals(days=30)

        # 启动前校验 (单次 get_account, 缓存结果)
        preflight = PreflightChecker(self.gateway)
        acc = preflight.run_all()
        if acc is None:
            raise RuntimeError("Preflight checks failed")

        # 多资产保证金模式检测 (2026-08-16 #6): 开启后保证金/可用余额口径
        # 全部变化, 必须显式告警 (系统按单资产 USDT 口径设计)
        if os.environ.get("MULTI_ASSETS_CHECK", "1") != "0":
            self._multi_assets = self.gateway.get_multi_assets_mode()
            if self._multi_assets is True:
                logger.error(
                    "账户已开启多资产保证金模式 — 系统按单资产 USDT 口径计算, "
                    "可用余额改用 totalMarginBalance; 建议评估后决定是否关闭")
                if self.event_bus is not None:
                    try:
                        self.event_bus.publish("alert", {
                            "source": "multi_assets", "level": "CRITICAL",
                            "message": "多资产保证金模式已开启, 保证金口径已切换",
                        })
                    except Exception:
                        pass
            elif self._multi_assets is False:
                logger.info("账户为单资产保证金模式 (USDT 结算)")
        else:
            self._multi_assets = False

        # 用校验时的账户数据初始化权益, 无需再调 API
        # (2026-08-16 P0-4: totalWalletBalance 含未实现盈亏, 为回撤正确口径)
        acc_total = acc.get("totalWalletBalance")
        acc_assets = acc.get("assets") or []
        total = float(acc_total) if acc_total else sum(
            float(a.get("walletBalance", 0)) for a in acc_assets)
        breakdown = [
            {"asset": a.get("asset", "?"), "walletBalance": float(a.get("walletBalance", 0))}
            for a in acc_assets if float(a.get("walletBalance", 0) or 0) > 0
        ]
        self.portfolio.update_equity(total, available_balance=self._available_balance(acc, self._multi_assets),
                                     assets=breakdown)
        logger.info("Account equity: %.2f USDT (资产构成: %s)", total,
                    {b["asset"]: b["walletBalance"] for b in breakdown})
        # 保证金率/回撤阈值告警接线 (2026-08-16: Alerter 此前未接入主系统,
        # 保证金率 > 80% 等告警从未真正跑起来)
        if os.environ.get("MARGIN_ALERT", "1") != "0":
            from monitor.alerter import Alerter
            self.alerter = Alerter(on_alert=self._dispatch_alert)
        else:
            self.alerter = None

        # 统一装配: 执行层 + 信号链 + 风控链
        mode = ExecutionModeManager(ExecutionMode(self.execution_mode_name.lower()))
        self.execution_mode = mode
        # PAPER 模式接线 PaperTrader (用实时行情模拟成交), 否则条件单会抛错被吞 → 零下单
        paper_trader = None
        if mode.is_paper():
            from shared.paper_trader import PaperTrader
            paper_trader = PaperTrader(feed=self.feed, db=self.db)
        self.orders = OrderManager(
            gateway=self.gateway, execution_mode=mode,
            event_bus=self.event_bus, instance=self.instance,
            paper_trader=paper_trader,
            tick_sizes=self.tick_sizes,
            db=self.db,
        )
        self.engine = self._build_signal_chain()
        self.risk_chain = self._build_risk_chain()
        self.step_sizes, self.tick_sizes = self._fetch_exchange_filters()
        self.orders.tick_sizes = self.tick_sizes
        logger.info("stepSize: %s", self.step_sizes or "获取失败(下单将退化为4位小数)")
        logger.info("tickSize: %s", self.tick_sizes or "获取失败(价格退化为默认档位)")
        # 账户配置同步: 杠杆/持仓模式/保证金模式 (2026-08-16 P0)
        self._sync_account_config()

        # 启动行情
        self.feed.start()
        time.sleep(2)

        # 启动时对账 (使用缓存的账户数据)
        reconciler = PositionReconciler(self.gateway, self.portfolio,
                                        on_drift=self._on_reconcile_drift)
        reconciler.reconcile(cached_account=acc)

        # 持续对账
        self.reconciler = reconciler
        self.reconciler.start()

        # 心跳发布线程: 周期读取 MetricsCollector 各模块心跳并发布 heartbeat 事件
        from shared.heartbeat_publisher import HeartbeatPublisher
        self.heartbeat = HeartbeatPublisher(self.event_bus, instance=self.instance)
        self.heartbeat.start()

        # 资金费监控 (P0-3): 8h 周期计算持仓资金成本, 超阈值钉钉告警
        if os.environ.get("FUNDING_MONITOR", "1") != "0":
            self._setup_funding_monitor()

        # 订阅 command 流 (dashboard 控制台 → emergency_stop / resume kill switch)
        if self.event_bus is not None:
            def _on_command_event(event):
                self._handle_command(event.data)

            self._command_thread = threading.Thread(
                target=self.event_bus.run_consumer,
                args=("command", f"systrader-{self.instance}", _on_command_event, 5, 100),
                daemon=True,
            )
            self._command_thread.start()

        # 回填历史数据（停滞检测用 feed.get_last_update_ts, 无需本地基准）
        self.feed.backfill(limit=200)
        # User Data Stream: 成交/余额毫秒级推送 (2026-08-16 P0, 可 USER_DATA_STREAM=0 关闭)
        if os.environ.get("USER_DATA_STREAM", "1") != "0":
            self._start_user_stream()
        # 强平事件流 (2026-08-16 #7): 独立 WS, 默认关闭 — testnet 可能不支持
        # 该流, 独立连接隔离故障, 不影响主行情
        if os.environ.get("FORCE_ORDER_STREAM", "0") == "1":
            try:
                from market_data.force_order_stream import ForceOrderStream
                self._force_order_stream = ForceOrderStream(
                    symbols=self.symbols, testnet=self.testnet,
                    proxy_host=os.environ.get("PROXY_HOST", "127.0.0.1"),
                    proxy_port=int(os.environ.get("PROXY_PORT", "7897")),
                    on_force_order=self._on_force_order,
                )
                self._force_order_stream.start()
            except Exception as e:
                logger.error("强平事件流启动失败: %s", e)
                self._force_order_stream = None
        # 模块心跳: 覆盖启动窗口 (initialize 完成即标记 runner 存活)
        MetricsCollector.instance().heartbeat("runner")
        # PID 文件: soak_watchdog 监控目标的可靠来源 (2026-08-16 — 日志轮转后
        # "System running" 行会丢, 仅扫日志不可靠)
        try:
            pid_path = os.environ.get("RUNNER_PID_PATH", "data/runner.pid")
            os.makedirs(os.path.dirname(pid_path) or ".", exist_ok=True)
            with open(pid_path, "w", encoding="utf-8") as f:
                f.write(str(os.getpid()))
            self._pid_path = pid_path
        except Exception as e:
            logger.warning("写入 runner.pid 失败: %s", e)
            self._pid_path = None
        # 生命周期事件 → 运维面板重启历史 (2026-08-16 面板二期)
        if self.event_bus is not None:
            self.event_bus.publish("lifecycle", {
                "event": "started", "pid": os.getpid(), "instance": self.instance,
            })
        logger.info("System initialized")

    # ─── 统一装配 ───

    def _build_signal_chain(self) -> SignalEngine:
        """信号链: 按名称从 StrategyRegistry 取策略实例。"""
        return SignalEngine(
            strategy=StrategyRegistry.get(self.strategy_name),
            event_bus=self.event_bus, instance=self.instance,
        )

    def _build_risk_chain(self) -> MiddlewareChain:
        """风控链: 仓位 → 杠杆 → 回撤 → 日亏损 → 集中度（架构 §3.4 五件套）。"""
        chain = MiddlewareChain(event_bus=self.event_bus, instance=self.instance)
        max_leverage = self.max_leverage
        chain.add(PositionSizer(risk_per_trade=self.risk_per_trade))
        chain.add(LeverageController(max_leverage=max_leverage))
        # 下单前可用保证金检查 (2026-08-16): 防止保证金不足开仓被拒
        chain.add(AvailableMarginCheck(safety_ratio=0.9))
        chain.add(DrawdownBreaker(
            max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120
        ))
        chain.add(DailyLossLimit(daily_loss_limit=0.05))
        chain.add(ConcentrationCheck(
            max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80
        ))
        # 单日最大交易次数 (2026-08-16 #3): 0=禁用
        try:
            max_trades_day = int(os.environ.get("MAX_TRADES_DAY", "30"))
        except (TypeError, ValueError):
            max_trades_day = 30
        chain.add(DailyTradeLimit(max_trades=max_trades_day))
        # 最大止损距离校验 (2026-08-16 #4): 0=禁用
        try:
            max_stop_pct = float(os.environ.get("MAX_STOP_PCT", "0.05"))
        except (TypeError, ValueError):
            max_stop_pct = 0.05
        chain.add(MaxStopDistance(max_stop_pct=max_stop_pct))
        # 参数 gauges → heartbeat → 面板参数展示 (2026-08-16 面板二期)
        MetricsCollector.instance().set_gauge("risk_per_trade", self.risk_per_trade)
        MetricsCollector.instance().set_gauge("max_leverage", max_leverage)
        MetricsCollector.instance().set_gauge("max_trades_day", float(max_trades_day))
        MetricsCollector.instance().set_gauge("max_stop_pct", max_stop_pct)
        return chain

    # ─── 信号链: K线闭合 → 信号 → 风控 → 下单 ───

    def _on_kline_closed(self, symbol: str, timeframe: str, ohlcv):
        """K线闭合 → 信号 → 风控 → 下单 (时间框架由策略决定)。"""
        tf = getattr(getattr(self.engine, "strategy", None), "timeframe", "15m")
        if timeframe != tf:
            return
        self.stats["kline_closes"] += 1
        # Ops T5: 注册 gauge, 经 heartbeat payload 供 watchdog 检测 K线闭合停滞
        MetricsCollector.instance().set_gauge("kline_closes", self.stats["kline_closes"])
        # 只用已闭合 K 线求值: buffer 末行可能是新一根 forming candle
        # (备用连接已写入), 部分数据不能作为信号依据 (2026-08-16 审计修复)。
        closed = [k for k in ohlcv if getattr(k, "is_closed", True)]
        if not closed:
            return
        df = pd.DataFrame([{
            "open": k.open, "high": k.high, "low": k.low,
            "close": k.close, "volume": k.volume,
        } for k in closed])
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
        if self._circuit_breaker:
            logger.warning("Circuit breaker active (%s) — signal rejected",
                           self._circuit_breaker)
            self.stats["risk_rejected"] += 1
            return
        # 持仓去重: 已有该 symbol 持仓或 PENDING 入场单时跳过, 避免叠单。
        # D3 (2026-08-16): 只认 LIMIT 入场单 — 保护单 (SL/TP) PENDING 不挡新信号
        if signal.symbol in self.portfolio.positions:
            logger.info("SKIP %s: 已有持仓, 跳过重复开仓", signal.symbol)
            return
        if any(o.symbol == signal.symbol and o.state == OrderState.PENDING
               and o.order_type == "LIMIT"
               for o in self.orders.active_orders):
            logger.info("SKIP %s: 已有 PENDING 入场单, 跳过重复开仓", signal.symbol)
            return
        result = self.risk_chain.process(signal, self.portfolio)
        if result.rejected:
            self.stats["risk_rejected"] += 1
            logger.warning("RISK REJECTED %s %s: %s",
                           signal.symbol, signal.direction, result.reason)
            return
        # 仓位大小缺失时不设成功偏向默认值: PositionSizer 必须产出 position_size,
        # 缺失说明风控链装配异常, 拒绝该信号 (2026-08-16 审计: 原默认 0.001 会把
        # 装配 bug 变成 0.001 的超小仓静默下单)
        size = result.modifications.get("position_size")
        if size is None or size <= 0:
            self.stats["risk_rejected"] += 1
            logger.error("RISK REJECTED %s: position_size 缺失或非法 (%s)",
                         signal.symbol, size)
            return
        # 限制名义价值在 5-100 USDT 之间（保底满足交易所最小 5 USDT 要求）
        price = self.feed.get_last_price(signal.symbol) or signal.entry_price
        min_qty = 5.0 / price if price else 0.001
        max_qty = 100.0 / price if price else 0.01
        step = self.step_sizes.get(signal.symbol, 0.0)
        qty = align_qty_to_step(size, step, min_qty, max_qty)
        # P1-1 下单前价格保护: 信号价与现价偏差过大 → 拒绝
        # (行情已走远, 挂单要么立即成交要么永远不成交, 属于陈旧信号)
        deviation_pct = float(os.environ.get("MAX_ENTRY_DEVIATION", "0.005"))
        if price and price > 0:
            dev = abs(signal.entry_price - price) / price
            if dev > deviation_pct:
                self.stats["risk_rejected"] += 1
                logger.warning(
                    "RISK REJECTED %s: 入场价 %.2f 与现价 %.2f 偏差 %.2f%% > %.2f%%",
                    signal.symbol, signal.entry_price, price,
                    dev * 100, deviation_pct * 100)
                return
        # P2-2 深度滑点预检: ORDERBOOK_CHECK=1 时按吃单深度估算滑点, 超阈值拒绝
        if os.environ.get("ORDERBOOK_CHECK", "0") == "1":
            if self._orderbook is None:
                from market_data.orderbook import OrderbookDepth
                self._orderbook = OrderbookDepth(
                    testnet=self.testnet,
                    proxy_host=os.environ.get("PROXY_HOST", "127.0.0.1"),
                    proxy_port=int(os.environ.get("PROXY_PORT", "7897")),
                )
            book = self._orderbook.fetch(signal.symbol)
            if book is not None:
                side = "BUY" if signal.direction == "LONG" else "SELL"
                slip_bps = OrderbookDepth.estimate_slippage_bps(book, side, qty)
                max_bps = float(os.environ.get("MAX_SLIPPAGE_BPS", "10"))
                if slip_bps is not None and slip_bps > max_bps:
                    self.stats["risk_rejected"] += 1
                    logger.warning(
                        "RISK REJECTED %s: 深度滑点估算 %.1fbps > %.1fbps",
                        signal.symbol, slip_bps, max_bps)
                    return
        try:
            orders = self.orders.execute_signal(
                signal.symbol, signal.direction, qty,
                signal.entry_price, signal.stop_loss, signal.take_profit,
            )
            # 成功判定以入场单 (列表第 1 个) 的 state 为唯一判据:
            # OrderManager 将 NEW → PENDING / FILLED → FILLED / PARTIALLY_FILLED → PARTIALLY_FILLED,
            # 仅 REJECTED/ERROR 为失败 (入场被拒时 execute_signal 已跳过 SL/TP, 只返回入场单)
            entry = orders[0] if orders else None
            if entry is None or entry.state in (OrderState.REJECTED, OrderState.ERROR):
                self.stats["orders_failed"] += 1
                MetricsCollector.instance().set_gauge(
                    "orders_failed", self.stats["orders_failed"])
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
            MetricsCollector.instance().set_gauge(
                "orders_placed", self.stats["orders_placed"])
            # 2026-08-16 审计: 仅入场已成交才登记持仓; PENDING 由成交轮询
            # (sync_entry_fills) 确认后登记 + 补挂 SL/TP, 消除幽灵持仓。
            if entry.state in (OrderState.FILLED, OrderState.PARTIALLY_FILLED):
                self.portfolio.open_position(Position(
                    symbol=signal.symbol, direction=signal.direction,
                    quantity=entry.filled_qty or qty,
                    entry_price=entry.avg_price or signal.entry_price,
                    leverage=getattr(signal, "leverage", 3),
                ))
                logger.info("ORDER PLACED %s %s qty=%s entry=%.2f (已成交)",
                            signal.symbol, signal.direction, qty, signal.entry_price)
            else:
                logger.info("ORDER PLACED %s %s qty=%s entry=%.2f (PENDING, "
                            "成交确认后登记持仓并挂 SL/TP)",
                            signal.symbol, signal.direction, qty, signal.entry_price)
        except Exception as e:
            self.stats["orders_failed"] += 1
            MetricsCollector.instance().set_gauge(
                "orders_failed", self.stats["orders_failed"])
            logger.error("ORDER EXCEPTION %s: %s", signal.symbol, e)

    def _strategy_leverage(self, symbol: str) -> int:
        """取策略声明的杠杆 (登记持仓用), 失败退默认 3。"""
        try:
            strategy = getattr(self.engine, "strategy", None)
            if strategy is not None and hasattr(strategy, "leverage"):
                return int(strategy.leverage(symbol))
        except Exception:
            pass
        return 3

    # ─── 账户配置同步 (P0-1) ───

    def _sync_account_config(self):
        """启动时同步账户级配置: 持仓模式校验 + 杠杆设置 + 保证金模式。

        2026-08-16 P0: 此前系统从未在交易所侧设置杠杆, 风控按策略杠杆
        (默认 3x) 计算保证金, 实际却是账户默认杠杆 → 集中度/保证金率失真。
        """
        dual = self.gateway.get_position_mode_dual()
        if dual is True:
            logger.error(
                "账户为双向持仓模式 (hedge) — 系统按单向持仓设计, "
                "请先在交易所关闭双向持仓再运行!"
            )
        elif dual is None:
            logger.warning("无法查询持仓模式, 假定单向 (如实际为双向请立即停止)")
        for sym in self.symbols:
            lev = self._strategy_leverage(sym)
            actual = self.gateway.change_leverage(sym, lev)
            if actual is None:
                logger.error("SET LEVERAGE FAILED %s: 账户实际杠杆未知, "
                             "风控保证金计算可能失真", sym)
            self.gateway.set_margin_type(sym, "ISOLATED")

    # ─── User Data Stream (P0-2) ───

    def _resolve_fee_rate(self) -> float:
        """实际手续费率解析 (2026-08-16 #1)。

        FEE_RATE 显式数字 → 直接使用 (往返口径);
        FEE_RATE=auto (默认) → 查 /fapi/v1/commissionRate, 取各 symbol
        taker 费率最大值 × 2 (往返), 失败保留 0.001 默认 (fail-safe)。
        """
        env_fee = os.environ.get("FEE_RATE", "auto").strip()
        if env_fee and env_fee.lower() != "auto":
            try:
                v = float(env_fee)
                logger.info("FEE_RATE 显式覆盖: %.6f (往返口径)", v)
                return v
            except ValueError:
                logger.warning("FEE_RATE=%s 非法, 回退 auto", env_fee)
        best_taker = 0.0
        for sym in self.symbols:
            rate = self.gateway.get_commission_rate(sym)
            if not rate:
                continue
            try:
                taker = float(rate.get("takerCommissionRate", 0) or 0)
            except (TypeError, ValueError):
                continue
            best_taker = max(best_taker, taker)
            logger.info("Commission %s: maker=%s taker=%s", sym,
                        rate.get("makerCommissionRate"), rate.get("takerCommissionRate"))
        if best_taker > 0:
            fee = round(best_taker * 2, 6)
            logger.info("实际往返费率: %.6f (2 × taker %.6f)", fee, best_taker)
            return fee
        logger.warning("commissionRate 查询失败, 保留默认往返费率 0.001")
        return 0.001

    @staticmethod
    def _available_balance(acc: dict, multi_assets: Optional[bool]) -> float:
        """可用保证金口径 (2026-08-16 #6 修复)。

        原实现 sum(各资产 availableBalance) 单位混杂 (BTC 数量 + USDT 数量
        直接相加) 是错的。单资产模式: 取 USDT 的 availableBalance;
        多资产模式/无 USDT 条目: 用 totalMarginBalance。
        """
        assets = acc.get("assets") or []
        if multi_assets is not True:
            for a in assets:
                if a.get("asset") == "USDT":
                    return float(a.get("availableBalance", 0) or 0)
        return float(acc.get("totalMarginBalance", 0) or 0)

    def _sync_position_risks(self):
        """清算价/爆仓距离/ADL 同步 (2026-08-16 #2/#3)。

        周期拉取 /fapi/v3/positionRisk:
          - 发布 position.risk 事件 → 面板清算价/爆仓距离列
          - 爆仓距离 < LIQ_ALERT_PCT (默认 8%) → 自动减仓 (保护性平仓)
          - adlQuantile > 0 (进入 ADL 队列) → CRITICAL 告警一次
        仅 LIVE 模式生效, 查询失败静默保留旧状态 (fail-closed)。
        """
        if (self.execution_mode is None or not self.execution_mode.is_live()
                or self.portfolio is None or not self.portfolio.positions):
            return
        risks = self.gateway.get_position_risks()
        if risks is None:
            return
        try:
            threshold = float(os.environ.get("LIQ_ALERT_PCT", "0.08"))
        except (TypeError, ValueError):
            threshold = 0.08
        by_symbol = {r.get("symbol", ""): r for r in risks if isinstance(r, dict)}
        now = time.time()
        for sym, pos in self.portfolio.positions_snapshot().items():
            r = by_symbol.get(sym)
            if not r:
                continue
            try:
                liq = float(r.get("liquidationPrice") or 0)
                mark = float(r.get("markPrice") or 0)
                adl = int(r.get("adlQuantile") or 0)
            except (TypeError, ValueError):
                continue
            dist = abs(mark - liq) / mark if mark > 0 and liq > 0 else None
            if self.event_bus is not None:
                try:
                    self.event_bus.publish("position.risk", {
                        "instance": self.instance, "symbol": sym,
                        "liquidation_price": liq, "mark_price": mark,
                        "adl_quantile": adl,
                        "liq_distance_pct": round(dist, 6) if dist is not None else None,
                    })
                except Exception as e:
                    logger.debug("position.risk 发布失败: %s", e)
            # ADL 队列: 持仓可能被交易所自动强减 → CRITICAL 告警 (每 symbol 一次)
            if adl > 0 and sym not in self._adl_warned:
                self._adl_warned.add(sym)
                self._send_critical(
                    f"{sym} 进入 ADL 队列 (quantile={adl}) — 持仓可能被自动强减, "
                    f"距清算价 {dist:.1%}" if dist is not None else
                    f"{sym} 进入 ADL 队列 (quantile={adl}) — 持仓可能被自动强减")
            elif adl == 0:
                self._adl_warned.discard(sym)
            # 爆仓距离过近 → 自动减仓 (与保证金率减仓共用冷却节流)
            if dist is not None and threshold > 0 and dist < threshold:
                cooldown = float(os.environ.get("DELEVERAGE_COOLDOWN_SEC", "120"))
                if now - self._last_deleverage_ts >= cooldown:
                    self._last_deleverage_ts = now
                    self._protective_close(
                        f"{sym} 距清算价仅 {dist:.1%} (< {threshold:.1%}) — 自动减仓")
                    return

    def _on_margin_call(self, data: dict):
        """交易所保证金率告警 (MARGIN_CALL 用户流事件) — CRITICAL + @人。"""
        logger.error("MARGIN CALL 事件: %s", data.get("p", {}))
        self._send_critical(f"交易所保证金率告警 (MARGIN CALL): {data.get('p', {})}")

    def _on_force_order(self, data: dict):
        """大额强平事件告警 (2026-08-16 #7): 市场流动性踩踏前兆。

        名义价值 >= FORCE_ORDER_ALERT_USDT (默认 100000) 时 WARNING 告警,
        每 symbol 5 分钟节流防刷屏。
        """
        o = data.get("o", {}) or {}
        try:
            qty = float(o.get("q", 0) or 0)
            price = float(o.get("p", 0) or 0)
        except (TypeError, ValueError):
            return
        notional = qty * price
        try:
            threshold = float(os.environ.get("FORCE_ORDER_ALERT_USDT", "100000"))
        except (TypeError, ValueError):
            threshold = 100000.0
        if threshold <= 0 or notional < threshold:
            return
        sym = (o.get("s") or "?").upper()
        now = time.time()
        if now - self._last_force_alert.get(sym, 0.0) < 300:
            return
        self._last_force_alert[sym] = now
        msg = (f"大额强平 {sym} {o.get('S', '?')} qty={qty} @ {price:.2f} "
               f"(名义 {notional:.0f} USDT ≥ {threshold:.0f})")
        logger.warning("FORCE ORDER: %s", msg)
        if self._dingtalk is None:
            self._ensure_dingtalk()
        if self._dingtalk is not None:
            try:
                self._dingtalk.send(msg)
            except Exception as e:
                logger.error("钉钉强平告警发送失败: %s", e)
        if self.event_bus is not None:
            try:
                self.event_bus.publish("alert", {
                    "source": "force_order", "message": msg, "level": "WARNING",
                })
            except Exception:
                pass

    def _start_user_stream(self):
        from market_data.user_data_stream import UserDataStream
        try:
            self.user_stream = UserDataStream(
                gateway=self.gateway,
                on_order_update=self._on_user_order_update,
                on_account_update=self._on_user_account_update,
                on_margin_call=self._on_margin_call,
            )
            self.user_stream.start()
        except Exception as e:
            logger.error("User data stream 启动失败 (降级为轮询): %s", e)
            self.user_stream = None

    def _on_user_order_update(self, order: dict):
        """ORDER_TRADE_UPDATE 推送: 更新订单状态, 成交即登记持仓+补挂 SL/TP。"""
        try:
            newly = self.orders.on_user_order_update(order)
            if newly:
                self._register_fills(newly)
        except Exception as e:
            logger.error("User order update handler failed: %s", e)

    def _on_user_account_update(self, account: dict):
        """ACCOUNT_UPDATE 推送: 余额/保证金变化, 节流刷新权益 (10s 窗口)。"""
        now = time.time()
        if now - getattr(self, "_last_stream_equity", 0.0) >= 10:
            self._last_stream_equity = now
            self._refresh_equity()

    def _register_fills(self, filled):
        """成交确认后的统一收尾: 登记持仓 + 补挂 SL/TP (轮询与推送共用)。

        2026-08-16 D1/D2 修复: 串行化防双通道同秒重复登记; 部分成交增量
        处理 — 持仓已存在时只更新数量并按增量补挂保护。
        """
        with self._fills_lock:
            for order in filled:
                existing = self.portfolio.positions.get(order.symbol)
                filled_qty = order.filled_qty or order.quantity
                if existing is None:
                    self.portfolio.open_position(Position(
                        symbol=order.symbol,
                        direction="LONG" if order.side == "BUY" else "SHORT",
                        quantity=filled_qty,
                        entry_price=order.avg_price or order.price,
                        leverage=self._strategy_leverage(order.symbol),
                    ))
                    increment = filled_qty
                else:
                    # 部分成交余量补登记 (D2): 更新数量, 只按增量补保护
                    increment = max(0.0, filled_qty - existing.quantity)
                    if increment > 0:
                        self.portfolio.update_position(
                            order.symbol, existing.direction, filled_qty)
                        logger.info("FILL SYNC %s: 部分成交余量补登记 +%.4f (合计 %.4f)",
                                    order.symbol, increment, filled_qty)
                    else:
                        logger.debug("FILL SYNC %s: 已登记过, 跳过", order.symbol)
                        continue
                protection = self.orders.place_protection(order, qty=increment)
                rejected = [o for o in protection
                            if o.state in (OrderState.REJECTED, OrderState.ERROR)]
                if rejected:
                    logger.error("FILL SYNC %s: SL/TP 补挂失败 — 持仓无保护! %s",
                                 order.symbol,
                                 "; ".join(f"{o.order_type}={o.error}" for o in rejected))
                else:
                    logger.info("FILL SYNC %s: SL/TP 已补挂 (qty=%.4f)",
                                order.symbol, increment)

    def _sync_entry_fills(self):
        """PENDING 入场单成交轮询 (10s 兜底, 推送失效时仍能确认成交)。"""
        try:
            self._register_fills(self.orders.sync_entry_fills())
        except Exception as e:
            logger.error("Entry fill sync failed: %s", e)

    def _sync_algo_orders(self):
        """条件单触发检测 (2026-08-16 S1): 保护单从开放清单消失 = 已触发平仓。"""
        try:
            triggered = self.orders.sync_algo_orders()
            for symbol in triggered:
                self._on_protection_triggered(symbol)
        except Exception as e:
            logger.error("Algo order sync failed: %s", e)

    def _cancel_remaining_protection(self, symbol: str):
        """撤该 symbol 残余保护单 (已触发一边后, 另一边的条件单必须撤掉)。"""
        for o in list(getattr(self.orders, "active_orders", []) or []):
            if o.symbol == symbol and o.order_type in (
                    "STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
                self._cancel_one_order(o)

    def _on_protection_triggered(self, symbol: str):
        """保护单已触发 → 交易所已平仓: 撤残余保护单 + 同步本地平仓。"""
        pos = self.portfolio.positions.get(symbol)
        if pos is None:
            logger.warning("ALGO TRIGGERED %s: 本地无持仓, 仅清理残余保护单", symbol)
            self._cancel_remaining_protection(symbol)
            return
        price = self.feed.get_last_price(symbol) or pos.entry_price
        self._cancel_remaining_protection(symbol)
        pnl = self.portfolio.close_position(symbol, price)
        logger.warning("ALGO CLOSE %s @ %.2f pnl=%.2f (保护单触发平仓)",
                       symbol, price, pnl)

    # ─── Kill switch / 熔断 / 远程命令 ───

    def _handle_command(self, data: dict):
        """command 事件流处理 (dashboard / Telegram / redis-cli 共用):
          emergency_stop 熔断 / resume 恢复 / force_exit 手动平仓 /
          cancel_all 清场撤单 / setparam 动态参数。
        """
        command = data.get("command", "")
        if command == "emergency_stop":
            self._circuit_breaker = "emergency_stop"
            logger.warning("EMERGENCY STOP — 停止下单")
            self._cancel_active_orders()
        elif command == "resume":
            self._circuit_breaker = None
            logger.info("Circuit breaker cleared — trading resumed")
        elif command == "force_exit":
            symbol = (data.get("symbol") or "").upper()
            if symbol == "ALL":
                for sym in list(self.portfolio.positions.keys()):
                    self._force_exit_symbol(sym)
            else:
                self._force_exit_symbol(symbol)
        elif command == "cancel_all":
            symbol = (data.get("symbol") or "").upper()
            targets = self.symbols if symbol == "ALL" else ([symbol] if symbol else [])
            for sym in targets:
                self.gateway.cancel_all_open_orders(sym)
                # D4 (2026-08-16): 本地订单状态联动 — 否则本地仍 PENDING,
                # 自认"有保护"实则裸奔且永不清理
                for o in list(getattr(self.orders, "active_orders", []) or []):
                    if o.symbol == sym:
                        o.state = OrderState.CANCELED
                        logger.warning("CANCEL ALL %s: 本地订单 %s (%s) 同步置为已撤",
                                       sym, o.order_id, o.order_type)
        elif command == "setparam":
            self._apply_param(data.get("key", ""), data.get("value"))
        elif command == "pause":
            # pause: 只停新单不撤单 (与 emergency_stop 区别, 2026-08-16 P0-6)
            self._circuit_breaker = "paused"
            logger.warning("PAUSED — 停止新信号 (已挂单保留)")

    def _apply_param(self, key: str, value):
        """动态参数热更新 (P2-3): 支持 risk_per_trade / max_leverage。"""
        def rebuild():
            # 2026-08-16 审计修复 (S5): 重建风控链会清零 DrawdownBreaker 的
            # COOLDOWN 状态 — 冷却期内调参即绕过熔断。重建时继承旧链熔断状态。
            old = getattr(self, "risk_chain", None)
            old_state, old_triggered = None, 0.0
            if old is not None:
                for mw in getattr(old, "_middleware", []) or []:
                    if isinstance(mw, DrawdownBreaker):
                        old_state, old_triggered = mw.state, mw._triggered_at
            self.risk_chain = self._build_risk_chain()
            if old_state is not None:
                for mw in getattr(self.risk_chain, "_middleware", []) or []:
                    if isinstance(mw, DrawdownBreaker):
                        mw.state = old_state
                        mw._triggered_at = old_triggered
        try:
            if key == "risk_per_trade":
                v = float(value)
                if not (0 < v <= 0.1):
                    raise ValueError("out of range (0, 0.1]")
                self.risk_per_trade = v
                rebuild()
                logger.warning("SETPARAM risk_per_trade = %s", v)
            elif key == "max_leverage":
                v = int(value)
                if not (1 <= v <= 20):
                    raise ValueError("out of range [1, 20]")
                # 2026-08-16 修复: 此前只重建链但 _build_risk_chain 读环境变量,
                # 热更新实际无效; 现在更新实例属性后重建才真正生效
                self.max_leverage = v
                rebuild()
                logger.warning("SETPARAM max_leverage = %s (重启后回退环境变量值)", v)
            else:
                logger.error("SETPARAM unsupported key: %s", key)
        except Exception as e:
            logger.error("SETPARAM failed %s=%s: %s", key, value, e)

    def _force_exit_symbol(self, symbol: str):
        """手动平仓 (Telegram /forceexit, dashboard): 市价 reduceOnly 平仓 +
        撤该 symbol 保护单 + 同步本地状态。"""
        if not symbol or symbol not in self.portfolio.positions:
            logger.warning("FORCE EXIT %s: 本地无持仓, 跳过", symbol)
            return
        pos = self.portfolio.positions[symbol]
        side = "SELL" if pos.direction == "LONG" else "BUY"
        price = self.feed.get_last_price(symbol)
        try:
            # 撤该 symbol 的全部活跃单: 保护单 (避免与市价单同时触发) +
            # PENDING 入场单 (2026-08-16 S4: 否则价格回踩会成交出用户没要的新仓)
            for o in list(getattr(self.orders, "active_orders", []) or []):
                if o.symbol == symbol:
                    self._cancel_one_order(o)
            resp = self.gateway.place_order(OrderRequest(
                symbol=symbol, side=side, order_type="MARKET",
                quantity=pos.quantity, reduce_only=True,
            ))
            if getattr(resp, "status", "") in ("FILLED", "PARTIALLY_FILLED"):
                exit_price = resp.avg_price or price or pos.entry_price
                pnl = self.portfolio.close_position(symbol, exit_price)
                logger.warning("FORCE EXIT %s %s qty=%s @ %.2f pnl=%.2f",
                               symbol, side, pos.quantity, exit_price, pnl)
            else:
                logger.error("FORCE EXIT %s: 平仓单未成交 status=%s error=%s",
                             symbol, getattr(resp, "status", "?"),
                             getattr(resp, "error", ""))
        except Exception as e:
            logger.error("FORCE EXIT %s exception: %s", symbol, e)

    def _dispatch_alert(self, alert):
        """Alerter 告警分发: 日志 + 钉钉 (2026-08-16)。

        CRITICAL 级告警带 @人 (DINGTALK_AT_MOBILES, #7), WARNING/INFO 普通发送。
        """
        logger.warning("RISK ALERT [%s] %s: %s", alert.level.value, alert.metric, alert.message)
        if self._dingtalk is None:
            self._ensure_dingtalk()
        if self._dingtalk is not None:
            try:
                msg = f"风控告警 [{alert.level.value}] {alert.metric}\n{alert.message}"
                if alert.level == AlertLevel.CRITICAL and self._dingtalk_at_mobiles:
                    self._dingtalk.send_at(msg, self._dingtalk_at_mobiles)
                else:
                    self._dingtalk.send(msg)
            except Exception as e:
                logger.error("钉钉告警发送失败: %s", e)

    def _send_critical(self, text: str):
        """CRITICAL 通知 (自动减仓等): 日志 + 钉钉 @人 + alert 流归档。"""
        logger.error("CRITICAL: %s", text)
        if self._dingtalk is None:
            self._ensure_dingtalk()
        if self._dingtalk is not None:
            try:
                if self._dingtalk_at_mobiles:
                    self._dingtalk.send_at(text, self._dingtalk_at_mobiles)
                else:
                    self._dingtalk.send(text)
            except Exception as e:
                logger.error("钉钉 CRITICAL 告警发送失败: %s", e)
        if self.event_bus is not None:
            try:
                self.event_bus.publish("alert", {
                    "source": "protective", "message": text, "level": "CRITICAL",
                })
            except Exception:
                pass

    def _ensure_dingtalk(self):
        """按环境变量惰性构建钉钉通知器 (无 webhook 时保持 None)。"""
        try:
            webhook = (os.environ.get("DINGTALK_WEBHOOK_URL")
                       or os.environ.get("DINGTALK_WEBHOOK") or "").strip()
            if not webhook:
                return
            self._dingtalk = DingTalkNotifier(
                webhook, secret=os.environ.get("DINGTALK_SECRET", ""))
        except Exception as e:
            logger.warning("钉钉初始化失败: %s", e)

    def _setup_funding_monitor(self):
        """资金费监控接线 (P0-3): 超阈值 → 日志 + 钉钉 (配置了 webhook 时)。

        记账口径由 FUNDING_ACCOUNTING 决定 (2026-08-16 #6):
          - income   (默认): 精确流水对账 (get_income + tranId 去重), 监控器只告警不记账
          - estimate: 旧估算口径 (rate × value × 8h), 仅在没有流水权限时用
          - off: 完全不记账
        """
        try:
            webhook = os.environ.get("DINGTALK_WEBHOOK_URL") or os.environ.get("DINGTALK_WEBHOOK")
            if webhook:
                self._dingtalk = DingTalkNotifier(
                    webhook, secret=os.environ.get("DINGTALK_SECRET", ""))
            threshold = float(os.environ.get("FUNDING_COST_THRESHOLD", "1.0"))
            accounting = os.environ.get("FUNDING_ACCOUNTING", "income")
            if accounting not in ("income", "estimate", "off"):
                logger.warning("FUNDING_ACCOUNTING=%s 非法, 退化为 income", accounting)
                accounting = "income"
            self._funding_accounting = accounting
            on_cost = self._on_funding_cost if accounting == "estimate" else None
            self.funding_monitor = FundingRateMonitor(
                portfolio=self.portfolio,
                cost_threshold=threshold,
                on_alert=self._funding_alert,
                price_fn=self.feed.get_mark_price if self.feed else None,
                on_cost=on_cost,
                testnet=self.testnet,
                proxy_host=os.environ.get("PROXY_HOST", "127.0.0.1"),
                proxy_port=int(os.environ.get("PROXY_PORT", "7897")),
            )
            self.funding_monitor.start()
            logger.info("资金费记账口径: %s", accounting)
        except Exception as e:
            logger.error("Funding monitor 启动失败: %s", e)
            self.funding_monitor = None

    def _sync_funding_income(self):
        """精确资金费对账 (2026-08-16 #6): 用 /fapi/v1/income 流水按 tranId
        去重逐笔记账, 替代估算口径。仅 LIVE 模式生效; 首次运行播种 last_tran
        不补历史 (避免与旧估算口径重复记账)。

        income < 0 = 支付资金费; 正数记 0 (资金费返还是极小概率事件,
        不改变已实现盈亏口径, 只记录日志)。
        """
        if (self.execution_mode is None or not self.execution_mode.is_live()
                or self.portfolio is None):
            return
        if getattr(self, "_funding_accounting", "income") != "income":
            return
        if self._funding_last_tran is None:
            try:
                with open(self._funding_state_path, "r", encoding="utf-8") as f:
                    self._funding_last_tran = int(json.load(f).get("last_tran", 0))
            except (OSError, ValueError):
                self._funding_last_tran = None  # 无状态文件 → 播种
        start_ms = int((time.time() - 72 * 3600) * 1000)
        records = self.gateway.get_income(
            income_type="FUNDING_FEE", start_time_ms=start_ms, limit=1000)
        if records is None:
            # 端点不可用 (testnet 无 income 权限 / 网络): 从未成功对账过 → 回退估算
            if self._funding_last_tran is None:
                self._enable_estimate_fallback()
            return
        if not records:
            return  # 流水为空: 不记账 (fail-closed)
        fresh = [r for r in records if int(r.get("tranId", 0) or 0) > (self._funding_last_tran or 0)]
        max_tran = max((int(r.get("tranId", 0) or 0) for r in records), default=0)
        if self._funding_last_tran is None:
            # 首次运行: 只播种游标, 历史流水不补记
            self._funding_last_tran = max_tran
            logger.info("FUNDING INCOME 播种 last_tran=%d (历史流水不补记)", max_tran)
            self._write_funding_state()
            return
        if fresh:
            cost = sum(
                min(float(r.get("income", 0) or 0), 0.0)
                for r in fresh
                if (r.get("asset") or "USDT") == "USDT"
            )
            if cost < 0:
                self.portfolio.add_funding_fee(-cost)
                logger.info("FUNDING INCOME 精确对账: %d 笔资金费合计 %.4f USDT 已计入盈亏",
                            len(fresh), cost)
            else:
                logger.info("FUNDING INCOME: %d 笔新流水, 合计 %.4f (无净支出, 不记账)",
                            len(fresh), cost)
            self._funding_last_tran = max_tran
            self._write_funding_state()

    def _write_funding_state(self):
        try:
            os.makedirs(os.path.dirname(self._funding_state_path) or ".", exist_ok=True)
            with open(self._funding_state_path, "w", encoding="utf-8") as f:
                json.dump({"last_tran": self._funding_last_tran}, f)
        except Exception as e:
            logger.warning("资金费对账状态写入失败: %s", e)

    def _enable_estimate_fallback(self):
        """income 流水不可用时的估算记账回退 (2026-08-16 #6 降级)。

        仅在从未成功对账过 (无游标) 时启用, 防止 income 成功后端点故障
        造成估算+流水双记账。把 FundingRateMonitor.on_cost 动态接回估算口径。
        """
        if getattr(self, "_estimate_fallback_on", False):
            return
        self._estimate_fallback_on = True
        if getattr(self, "funding_monitor", None) is not None:
            self.funding_monitor.on_cost = self._on_funding_cost
            logger.warning("FUNDING INCOME 流水不可用 (testnet 无权限/网络) — "
                           "资金费回退估算记账口径")

    def _on_funding_cost(self, symbol: str, cost: float):
        """资金费记账回调: 结算周期成本计入本地已实现盈亏 (2026-08-16)。
        FUNDING_ACCOUNTING=estimate 时的旧估算口径; income 口径走 _sync_funding_income。
        """
        try:
            self.portfolio.add_funding_fee(cost)
            logger.info("FUNDING COST %s: %.4f USDT 已计入盈亏", symbol, cost)
        except Exception as e:
            logger.error("资金费记账失败 %s: %s", symbol, e)

    # ─── 风控补强 (2026-08-16): 自动减仓 / 回撤分级 / 每日摘要 ───

    def _protective_check(self):
        """60s 权益刷新后的持仓保护检查 (#1 保证金率自动减仓 + #2 回撤分级)。

        - 保证金率 > MARGIN_DELEVERAGE_THRESHOLD: 关最大持仓, 冷却后仍超继续关
        - 回撤 >= DRAWDOWN_REDUCE_TIER (且 < 熔断档 15%): 关最大持仓一次,
          回撤回落到档位 80% 以下才重新武装 (防止连续关仓)
        均只对 LIVE 模式生效 (paper/dry_run 无真实仓位可减)。
        """
        if (self.execution_mode is None or not self.execution_mode.is_live()
                or self.portfolio is None or not self.portfolio.positions):
            return
        now = time.time()
        try:
            threshold = float(os.environ.get("MARGIN_DELEVERAGE_THRESHOLD", "0.8"))
            tier = float(os.environ.get("DRAWDOWN_REDUCE_TIER", "0.12"))
        except (TypeError, ValueError):
            threshold, tier = 0.0, 0.0
        if threshold > 0 and self.portfolio.margin_ratio > threshold:
            cooldown = float(os.environ.get("DELEVERAGE_COOLDOWN_SEC", "120"))
            if now - self._last_deleverage_ts >= cooldown:
                self._last_deleverage_ts = now
                self._protective_close(
                    f"保证金率 {self.portfolio.margin_ratio:.1%} 超过 "
                    f"自动减仓阈值 {threshold:.1%} — 关闭保证金占用最大持仓")
            return
        if tier > 0 and self.portfolio.current_drawdown >= tier:
            # 15% 熔断档由 DrawdownBreaker 负责; 本档只做一次减仓
            if self._drawdown_reduce_armed and now - self._last_reduce_ts >= 600:
                self._drawdown_reduce_armed = False
                self._last_reduce_ts = now
                self._protective_close(
                    f"回撤 {self.portfolio.current_drawdown:.1%} 达到减仓档 "
                    f"{tier:.1%} — 关闭保证金占用最大持仓")
        elif tier > 0 and self.portfolio.current_drawdown < tier * 0.8:
            self._drawdown_reduce_armed = True  # 回撤回落 20% → 重新武装

    def _protective_close(self, reason: str):
        """关闭保证金占用最大的持仓 (风险敞口最大的一个) + CRITICAL 告警 @人。"""
        positions = self.portfolio.positions_snapshot()
        if not positions:
            return
        try:
            symbol = max(positions, key=lambda s: self.portfolio.margin_for_symbol(s))
        except Exception:
            symbol = next(iter(positions))
        logger.error("PROTECTIVE CLOSE %s: %s", symbol, reason)
        self._send_critical(f"[自动减仓] {symbol}: {reason}")
        self._force_exit_symbol(symbol)
        # 减仓后立即刷一次权益, 让下一次阈值判断基于最新数据
        self._refresh_equity()

    def _maybe_send_daily_digest(self):
        """每日钉钉运营摘要 (2026-08-16 #5): DIGEST_HOUR:DIGEST_MINUTE 起
        10 分钟窗口内发一次 (进程重启后按日期去重), 无钉钉时只记日志。
        """
        if os.environ.get("DIGEST_ENABLED", "1") == "0":
            return
        now = datetime.now()
        if time.time() - self._last_digest_check < 30:
            return
        self._last_digest_check = time.time()
        try:
            hour = int(os.environ.get("DIGEST_HOUR", "8"))
            minute = int(os.environ.get("DIGEST_MINUTE", "0"))
        except (TypeError, ValueError):
            hour, minute = 8, 0
        if not (now.hour == hour and minute <= now.minute < minute + 10):
            return
        today = now.strftime("%Y-%m-%d")
        if self._last_digest_date == today:
            return
        self._last_digest_date = today
        try:
            digest = self._compose_digest()
            logger.info("DAILY DIGEST:\n%s", digest)
            if self._dingtalk is None:
                self._ensure_dingtalk()
            if self._dingtalk is not None:
                self._dingtalk.send(digest)
        except Exception as e:
            logger.error("每日摘要发送失败: %s", e)

    def _compose_digest(self) -> str:
        """汇总当日运行统计 + 账户状态 + 持仓明细, 生成钉钉文本摘要。"""
        elapsed = time.time() - self.stats["start_time"]
        p = self.portfolio
        breaker = self._circuit_breaker or "无"
        lines = [
            f"[SysTrader] 每日运营摘要 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"运行 {elapsed / 3600:.1f}h | 信号 {self.stats['signals']} | "
            f"风控拒绝 {self.stats['risk_rejected']} | "
            f"下单 {self.stats['orders_placed']}/{self.stats['orders_failed']} 失败",
            f"K线闭合 {self.stats['kline_closes']} | 数据停滞 {self.stats['stalls']} | "
            f"熔断: {breaker}",
            f"权益 {p.total_equity:.2f} | 当日盈亏 {p.daily_realized_pnl:+.2f} | "
            f"累计盈亏 {p.total_realized_pnl:+.2f}",
            f"保证金率 {p.margin_ratio:.1%} | 回撤 {p.current_drawdown:.1%} | "
            f"手续费累计 {p.total_fees:.2f} | 资金费累计 {p.total_funding_fees:.2f}",
            f"今日开仓 {p.trade_count_today} 次",
        ]
        if p.positions:
            lines.append(f"持仓 {len(p.positions)}:")
            for sym, pos in sorted(p.positions.items()):
                mark = self.feed.get_last_price(sym) if self.feed else None
                upnl = p.unrealized_pnl(sym, mark) if mark else None
                upnl_s = f" 未实现{upnl:+.2f}" if upnl is not None else ""
                lines.append(
                    f"  {sym} {'多' if pos.direction == 'LONG' else '空'} "
                    f"{pos.quantity} @ {pos.entry_price:.4g} ({pos.leverage}x){upnl_s}")
        else:
            lines.append("持仓 0")
        return "\n".join(lines)

    def _funding_alert(self, msg: str):
        """资金费超阈值告警: 日志 + 钉钉(含 alert 流归档) / 无钉钉时直发 alert 流。"""
        logger.warning("FUNDING ALERT: %s", msg)
        if self._dingtalk is not None:
            try:
                self._dingtalk.send(f"资金费率告警\n{msg}")
            except Exception as e:
                logger.error("钉钉告警发送失败: %s", e)
        elif self.event_bus is not None:
            try:
                self.event_bus.publish("alert", {
                    "source": "funding_monitor", "message": msg,
                })
            except Exception as e:
                logger.debug("alert 事件发布失败: %s", e)

    def _cancel_active_orders(self):
        """熔断时撤活跃入场单, **保留持仓保护单**。

        2026-08-16 审计: 原实现把 SL/TP 条件单也一并撤掉, 熔断瞬间持仓
        立即变成裸仓。止损/止盈是持仓保护, 熔断只应阻止新开仓 (撤入场单),
        保护单保留; 需要平仓时由人工/对账处理。
        """
        try:
            active = getattr(self.orders, "active_orders", []) or []
            kept = 0
            for order in active:
                if getattr(order, "order_type", "") in (
                        "STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
                    kept += 1
                    continue
                if getattr(order, "state", None) and order.state.value not in ("FILLED", "CANCELED"):
                    self._cancel_one_order(order)
            if kept:
                logger.warning("EMERGENCY STOP: 保留 %d 个持仓保护单 (SL/TP 不撤)",
                               kept)
        except Exception as e:
            logger.error("Cancel active orders failed: %s", e)

    def _cancel_one_order(self, order):
        """单个订单撤单，按类型分流 (LIMIT → cancel_order, 条件单 → cancel_algo_order)。

        2026-08-16 审计修复 (S2): 撤单失败不再无条件标记 CANCELED——
        - status == "CANCELED": 交易所确认已撤 → 本地标记 CANCELED
        - status == "REJECTED": 通常 -2011 未知订单 (已不存在) → 标记 CANCELED
        - status == "ERROR" (网络失败/限流): 订单在交易所可能还活着 →
          **保持 PENDING**, 下一轮超时检测重试, 绝不让本地状态与交易所脱节
        """
        symbol, oid = order.symbol, order.order_id
        if order.order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
            resp = self.gateway.cancel_algo_order(symbol, oid)
        else:
            resp = self.gateway.cancel_order(symbol, oid)
        status = getattr(resp, "status", "")
        if status in ("CANCELED", "REJECTED"):
            order.state = OrderState.CANCELED
        elif status == "ERROR":
            logger.error("Order cancel FAILED %s id=%s (%s) — 保持 PENDING 重试",
                         symbol, oid, getattr(resp, "error", ""))
        else:
            logger.warning("Order cancel not confirmed %s id=%s status=%s: %s",
                           symbol, oid, status, getattr(resp, "error", ""))
            order.state = OrderState.CANCELED

    def _refresh_equity(self):
        """周期刷新账户权益 (2026-08-16 P0-4: 改用 totalWalletBalance)。

        totalWalletBalance 含未实现盈亏, 是回撤/日亏损熔断的正确口径;
        旧实现用各资产 walletBalance 之和 (不含未实现), 浮亏时回撤失真。
        availableBalance 取各资产可用余额之和 (下单能力口径)。
        """
        try:
            acc = self.gateway.get_account()
            if not isinstance(acc, dict) or acc.get("error"):
                return
            total_wb = acc.get("totalWalletBalance")
            total = float(total_wb) if total_wb else 0.0
            assets = acc.get("assets") or []
            # 可用保证金口径 (2026-08-16 #6): 单资产取 USDT, 多资产取 totalMarginBalance
            available = self._available_balance(acc, self._multi_assets)
            if total <= 0 and assets:
                total = sum(float(a.get("walletBalance", 0)) for a in assets)
            if total <= 0:
                return
            # 资产构成 (非零余额), 随 equity 事件下发 dashboard (防"权益缩水"误读)
            breakdown = [
                {"asset": a.get("asset", "?"), "walletBalance": float(a.get("walletBalance", 0))}
                for a in assets if float(a.get("walletBalance", 0) or 0) > 0
            ]
            self.portfolio.update_equity(total, available_balance=available,
                                         assets=breakdown)
            logger.debug("Equity refreshed: %.2f USDT (available=%.2f)", total, available)
        except Exception as e:
            logger.error("Equity refresh failed: %s", e)

    def _on_reconcile_drift(self, report):
        """对账漂移回调 — 交易所为权威源, 三类漂移分别处理 (2026-08-16 审计扩展)。

        - local_only (本地有/交易所无): 交易所持仓已消失 → close_position 同步本地
        - remote_only (交易所有/本地无): 本地失明 (重启/幽灵仓) → 导入本地, 恢复管理
        - qty_mismatch (数量不一致): 本地对齐交易所数量 (含做空方向)
        """
        try:
            for sym in report.details.get("local_only", []):
                price = self.feed.get_last_price(sym) if self.feed else None
                if price is None:
                    logger.warning("RECONCILE %s: 交易所持仓消失但无行情, 跳过本地平仓同步", sym)
                    continue
                # 2026-08-16 S1: 平仓联动撤残余保护单, 防旧 TP/SL 误平后续新仓
                self._cancel_remaining_protection(sym)
                pnl = self.portfolio.close_position(sym, price)
                logger.warning("RECONCILE %s: 交易所持仓消失 → 本地平仓 @ %.2f (pnl=%.2f)",
                               sym, price, pnl)
            for item in report.details.get("remote_only", []):
                sym = item if isinstance(item, str) else item.get("symbol")
                qty = 0.0
                entry = 0.0
                if isinstance(item, dict):
                    qty = float(item.get("qty", 0) or 0)
                    entry = float(item.get("entry", 0) or 0)
                if sym is None or qty == 0:
                    continue
                direction = "LONG" if qty > 0 else "SHORT"
                position = Position(
                    symbol=sym, direction=direction,
                    quantity=abs(qty),
                    entry_price=entry or (self.feed.get_last_price(sym) if self.feed else 0.0),
                    leverage=self._strategy_leverage(sym),
                )
                self.portfolio.open_position(position)
                logger.warning("RECONCILE %s: 交易所持仓本地缺失 → 导入 %s qty=%s entry=%.2f",
                               sym, direction, abs(qty), position.entry_price)
            for item in report.details.get("qty_mismatch", []):
                sym = item.get("symbol") if isinstance(item, dict) else None
                if sym is None or sym not in self.portfolio.positions:
                    continue
                pos = self.portfolio.positions[sym]
                direction = (item.get("remote_direction")
                             if isinstance(item, dict) else None) or "LONG"
                remote_qty = abs(float(item.get("remote", 0) or 0)) if isinstance(item, dict) else pos.quantity
                if direction != pos.direction or abs(remote_qty - pos.quantity) > 0.0001:
                    logger.warning(
                        "RECONCILE %s: 数量漂移 本地 %s %s → 对齐交易所 %s %s",
                        sym, pos.direction, pos.quantity, direction, remote_qty,
                    )
                    # 锁内更新 (2026-08-16: 原直接改字段, 与风控读取并发)
                    self.portfolio.update_position(sym, direction, remote_qty)
        except Exception as e:
            logger.error("Reconcile drift handler failed: %s", e)

    def _check_pending_timeouts(self):
        """PENDING 订单超时检测: 超时自动撤单，避免僵尸单 (每 60s 轮询)。

        判定: active_orders 中 state == PENDING 且存在时长 > pending_timeout_minutes。
        例外: 该 symbol 有持仓时跳过——止损/止盈条件单是持仓保护，不适用
        "价格没回踩到位"的超时撤单。(2026-08-16 起持仓只在成交后登记,
        此豁免只命中保护单; PENDING 入场单无持仓, 正常走超时撤单)
        """
        if self.pending_timeout_minutes <= 0:
            return  # 0 = 禁用超时撤单
        active = getattr(self.orders, "active_orders", None) or []
        if not active:
            return
        positions = getattr(self.portfolio, "positions", None) or {}
        now = time.time()
        for order in list(active):
            if getattr(order, "state", None) != OrderState.PENDING:
                continue
            if order.symbol in positions:
                continue  # 持仓保护单 / 已成交入场单, 不撤
            age = now - order.created_at
            if age <= self.pending_timeout_minutes * 60:
                continue
            logger.error("PENDING TIMEOUT %s %s orderId=%s age=%.1fm (>%dm) — 自动撤单",
                         order.symbol, order.order_type, order.order_id,
                         age / 60, self.pending_timeout_minutes)
            self._cancel_one_order(order)

    # ─── 健康监控 ───

    def _check_stall(self):
        """数据停滞检测（2026-08-16 审计重写）。

        旧实现以 `get_last_price(sym) is None` 判停滞，但缓存价一旦收到过
        一笔 aggTrade 就永不为 None——WS 断连后价格静止也永不触发熔断，
        整条防线形同虚设。改为按"最后行情消息年龄"判断: 每 symbol 最近
        一条行情消息超过 STALE_THRESHOLD 秒即为停滞 (与基准比较无竞态)。
        """
        for sym in self.symbols:
            now = time.time()
            last_ts = self.feed.get_last_update_ts(sym)
            if last_ts is None:
                # 从未收到该 symbol 行情: 跳过 (启动窗口不误判)
                continue
            if now - last_ts <= STALE_THRESHOLD:
                # 数据新鲜: 清零连续停滞计数
                self._stall_strikes[sym] = 0
                continue
            self.stats["stalls"] += 1
            strikes = self._stall_strikes.get(sym, 0) + 1
            self._stall_strikes[sym] = strikes
            logger.warning("STALL %s: 无数据 %ds (strike %d/%d)",
                           sym, STALE_THRESHOLD, strikes, self.stall_strikes)
            self._network_diag(reason=f"stall_{sym}")
            # 连续 stall_strikes 次停滞判定 → 熔断停单 (复用 kill switch 语义:
            # 停新单 + 撤活跃单; 不自动恢复, 需人工 resume)
            if self.stall_strikes > 0 and strikes >= self.stall_strikes:
                self._stall_strikes[sym] = 0  # 防重复触发, 恢复后重新计数
                if self._circuit_breaker is None:
                    logger.error("STALL BREAKER %s: 连续 %d 次停滞判定 "
                                 "(每次无数据 %ds) — 触发熔断停单",
                                 sym, self.stall_strikes, STALE_THRESHOLD)
                    self._handle_command({"command": "emergency_stop"})
                    logger.error("stall 熔断已触发, 需手动 resume 恢复")

    def _check_connections(self):
        if not self.feed or not self.feed._conns:
            return
        connected = sum(1 for c in self.feed._conns if c.connected)
        # WS 连接数 gauges → heartbeat → 运维面板断连趋势 (2026-08-16 面板二期)
        MetricsCollector.instance().set_gauge("ws_connected", connected)
        MetricsCollector.instance().set_gauge("ws_total", len(self.feed._conns))
        if connected < len(self.feed._conns):
            logger.warning("WS 连接降级: %d/%d 在线", connected, len(self.feed._conns))
            self._network_diag(reason=f"ws_downgrade_{connected}_{len(self.feed._conns)}")
        # 主连接静默断流看护 (2026-08-17): TCP 存活但主连接无消息 → 强制切主。
        # 备用连接一直在喂价格, 整体 stalls=0/ws=8/8 全绿, 唯独 K 线闭合
        # (仅主连接触发) 停滞 — 此检查填补该盲区。
        try:
            stale = self.feed.primary_stale_seconds()
            if stale >= 0 and stale > 90:
                self.feed.force_primary_switch()
        except Exception as e:
            logger.debug("主连接看护检查失败: %s", e)

    def _fetch_exchange_filters(self):
        """从 exchangeInfo 获取 stepSize (数量精度) 与 tickSize (价格精度)。

        Binance 要求下单数量是 stepSize 整数倍、价格是 tickSize 整数倍:
        BTC=0.0001/0.10, ETH=0.001/0.01, SOL=0.01/0.001 等。
        失败时返回 ({}, {}): 数量退化为 4 位小数、价格用 OrderManager 内置默认档位。
        """
        import requests
        try:
            base_url = (
                OrderGateway.BASE_URL_LIVE if not self.testnet
                else OrderGateway.BASE_URL_TESTNET
            )
            r = requests.get(
                f"{base_url}/fapi/v1/exchangeInfo",
                proxies=self.gateway.proxies if self.gateway else None,
                timeout=10,
            )
            info = r.json()
            steps, ticks = {}, {}
            for s in info.get("symbols", []):
                if s.get("symbol") not in self.symbols:
                    continue
                for f in s.get("filters", []):
                    if f.get("filterType") == "LOT_SIZE":
                        steps[s["symbol"]] = float(f["stepSize"])
                    elif f.get("filterType") == "PRICE_FILTER":
                        ticks[s["symbol"]] = float(f["tickSize"])
            return steps, ticks
        except Exception as e:
            logger.warning("获取 exchangeInfo 过滤器失败: %s", e)
            return {}, {}

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
        # 2026-08-17: 结论行用 ASCII — emoji 在 GBK 控制台 (stability_test 直跑)
        # 会 UnicodeEncodeError 崩掉 (✅\u2705 无法编码)
        logger.info("结论: %s", "PASS (stable)" if ok else "FAIL (issues)")
        if self.feed:
            self.feed.stop()

    # ─── 生命周期 ───

    def run_forever(self):
        logger.info("System running (PID=%d)", os.getpid())
        end_time = time.time() + self.hours * 3600 if self.hours > 0 else None
        last_snapshot = time.time()
        last_pending_check = time.time()
        last_equity_check = time.time()
        last_fill_sync = time.time()
        last_funding_sync = time.time()
        try:
            while True:
                # 模块心跳: 主循环每轮标记 runner 存活
                MetricsCollector.instance().heartbeat("runner")
                time.sleep(5)
                self._check_stall()
                self._check_connections()
                # 周期刷新账户权益, 保证风控链 (回撤/日亏损/连亏) 基准不过期
                if time.time() - last_equity_check >= 60:
                    self._refresh_equity()
                    # 保证金率/回撤阈值告警 (2026-08-16 接线)
                    if getattr(self, "alerter", None) is not None:
                        try:
                            self.alerter.check_thresholds(None, self.portfolio)
                        except Exception as e:
                            logger.error("风险阈值检查失败: %s", e)
                    # 自动减仓 / 回撤分级响应 (2026-08-16 #1/#2)
                    try:
                        self._protective_check()
                    except Exception as e:
                        logger.error("持仓保护检查失败: %s", e)
                    # 清算价/爆仓距离/ADL 同步 (2026-08-16 #2/#3)
                    try:
                        self._sync_position_risks()
                    except Exception as e:
                        logger.error("持仓风险同步失败: %s", e)
                    # K线闭合 REST 兜底 (2026-08-17): WS kline 流停滞自愈,
                    # 防 testnet kline stream 断推 11h 的静默失明重演
                    try:
                        if self.feed is not None:
                            self.feed.poll_closures_from_rest()
                    except Exception as e:
                        logger.debug("K线闭合 REST 兜底失败: %s", e)
                    last_equity_check = time.time()
                # 精确资金费对账 (2026-08-16 #6): 10min 周期拉 income 流水
                try:
                    interval = float(os.environ.get("FUNDING_INCOME_INTERVAL_SEC", "600"))
                except (TypeError, ValueError):
                    interval = 600.0
                if interval > 0 and time.time() - last_funding_sync >= interval:
                    try:
                        self._sync_funding_income()
                    except Exception as e:
                        logger.error("资金费流水对账失败: %s", e)
                    last_funding_sync = time.time()
                # 每日钉钉运营摘要 (2026-08-16 #5): 内部按 30s 节流 + 日期去重
                try:
                    self._maybe_send_daily_digest()
                except Exception as e:
                    logger.error("每日摘要检查失败: %s", e)
                # PENDING 入场单成交轮询 + 条件单触发检测:
                # 确认成交 → 登记持仓 + 补挂 SL/TP
                if time.time() - last_fill_sync >= 10:
                    self._sync_entry_fills()
                    self._sync_algo_orders()
                    last_fill_sync = time.time()
                if time.time() - last_snapshot >= 60:
                    self._snapshot()
                    last_snapshot = time.time()
                if time.time() - last_pending_check >= 60:
                    self._check_pending_timeouts()
                    last_pending_check = time.time()
                # PAPER 模式: 轮询模拟盘条件单 (SL/TP) 触发成交, 保持模拟持仓有保护
                if (self.execution_mode is not None
                        and self.execution_mode.is_paper()
                        and self.orders is not None):
                    try:
                        self.orders.poll_paper_conditionals()
                    except Exception as e:
                        logger.error("Paper conditional poll failed: %s", e)
                if end_time is not None and time.time() >= end_time:
                    logger.info("运行时长已到 (%dh), 结束", self.hours)
                    return
        except KeyboardInterrupt:
            logger.info("手动中断")
        except Exception:
            # 2026-08-16 审计: 主循环异常不再直接冒泡到 main() 裸退——先落报告
            # 再走 finally 清理 (撤单/关 feed 由 stop() 负责)。
            logger.exception("主循环异常, 正在清理退出")
        finally:
            self.report()
            self.stop()

    def stop(self):
        # 2026-08-16: 幂等保护 — 信号处理器与 finally 可能双调用
        if self._stopped:
            return
        self._stopped = True
        logger.info("Shutting down...")
        try:
            if self.event_bus is not None:
                try:
                    self.event_bus.publish("lifecycle", {
                        "event": "stopped", "pid": os.getpid(),
                        "instance": self.instance,
                    })
                except Exception:
                    pass
            if getattr(self, "_pid_path", None):
                try:
                    os.remove(self._pid_path)
                except OSError:
                    pass
            if self.funding_monitor:
                self.funding_monitor.stop()
            if self.user_stream:
                self.user_stream.stop()
            if getattr(self, "_force_order_stream", None):
                self._force_order_stream.stop()
            if self.reconciler:
                self.reconciler.stop()
            if self.heartbeat:
                self.heartbeat.stop()
            if self.event_bus:
                self.event_bus.stop()
            # command 消费线程是 daemon, 显式 join 确保停止 (2026-08-16 审计)
            if self._command_thread and self._command_thread.is_alive():
                self._command_thread.join(timeout=2)
            if self.feed:
                self.feed.stop()
            if self.idempotency:
                self.idempotency.close()
            if self.db:
                self.db.close()
            if self.kline_archive:
                self.kline_archive.close()
        finally:
            logger.info("Shutdown complete")

    @property
    def healthy(self) -> bool:
        return (self.feed is not None
                and self.feed.get_last_price("BTCUSDT") is not None)


def main():
    parser = argparse.ArgumentParser(description="交易系统主入口 (默认 testnet)")
    parser.add_argument("--strategy", default="scalping_15m", help="策略名称 (注册于 StrategyRegistry)")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT", help="逗号分隔的标的列表")
    parser.add_argument("--execution-mode", default="live", choices=["dry_run", "paper", "live"])
    parser.add_argument("--hours", type=float, default=0,
                        help="运行时长(小时), 0=无限运行 (支持小数, OPS-002)")
    parser.add_argument("--testnet", dest="testnet", action="store_true", default=True)
    parser.add_argument("--no-testnet", dest="testnet", action="store_false", help="连接实盘 (慎用)")
    parser.add_argument("--risk-per-trade", type=float, default=0.015, help="单笔风险比例")
    parser.add_argument("--instance", default="live", help="实例标识")
    parser.add_argument("--stall-strikes", type=int, default=3,
                        help="连续停滞判定次数达到后熔断停单 (默认 3, 0=只告警不熔断)")
    parser.add_argument("--pending-timeout-minutes", type=int, default=30,
                        help="PENDING 订单超时自动撤单阈值 (分钟, 默认 30, 0=禁用)")
    args = parser.parse_args()
    load_env()
    setup_logging()
    from shared.event_bus import EventBus
    event_bus = EventBus(redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"))
    runner = SystemRunner(
        testnet=args.testnet,
        symbols=args.symbols.split(","),
        strategy_name=args.strategy,
        execution_mode_name=args.execution_mode,
        risk_per_trade=args.risk_per_trade,
        hours=args.hours,
        instance=args.instance,
        event_bus=event_bus,
        stall_strikes=args.stall_strikes,
        pending_timeout_minutes=args.pending_timeout_minutes,
    )
    try:
        runner.initialize()
        runner.run_forever()
    except Exception:
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
