"""Telegram 远程控制机器人 (2026-08-16 P0-6)。

独立进程: 长轮询 getUpdates, 命令发布到 Redis command 流 (与 dashboard
共用通道), 状态从 heartbeat/position.changed 流读取。

依赖: requests + redis (已在 requirements.txt)。不引入新库。

环境变量:
  TELEGRAM_BOT_TOKEN  机器人 token (BotFather)
  TELEGRAM_CHAT_ID    允许的 chat_id (逗号分隔, 空=不限制)
  REDIS_URL           Redis 连接串 (默认 redis://localhost:6379)

命令:
  /status            运行统计 (心跳 + kline/订单 gauges)
  /positions         当前持仓 (position.changed 最近状态)
  /pause /resume     暂停/恢复新信号
  /stop              熔断停单 (撤活跃入场单, 保留保护单)
  /forceexit <SYM|ALL>  手动市价平仓
  /cancelall <SYM|ALL>  清场撤单 (含保护单, 谨慎)
  /help              帮助
"""

import argparse
import json
import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="[TG] %(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("telegram_bot")

API_BASE = "https://api.telegram.org/bot{token}"


class TelegramBot:
    def __init__(self, token: str, chat_ids=None, redis_url: str = "redis://localhost:6379",
                 prefix: str = "systrader"):
        self.token = token
        self.allowed = set(chat_ids or [])
        self.prefix = prefix
        import redis
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self._offset = 0

    # ── Telegram API ──

    def _api(self, method: str, **params):
        url = API_BASE.format(token=self.token) + f"/{method}"
        try:
            resp = requests.post(url, json=params, timeout=15)
            data = resp.json()
            if not data.get("ok"):
                logger.error("Telegram API %s failed: %s", method, data)
                return None
            return data.get("result")
        except Exception as e:
            logger.error("Telegram API %s exception: %s", method, e)
            return None

    def send(self, chat_id, text: str):
        self._api("sendMessage", chat_id=chat_id, text=text[:4000])

    def _authed(self, chat_id) -> bool:
        # fail-closed: 未配置白名单 (allowed 为空) 时拒绝一切命令
        # (2026-08-16 审计: 原实现空白名单=不限制, 任何人可 /stop /forceexit)
        return bool(self.allowed) and str(chat_id) in self.allowed

    # ── Redis 状态读取 ──

    def _latest_event(self, stream: str):
        try:
            msgs = self.redis.xrevrange(f"{self.prefix}:{stream}", count=1)
            if not msgs:
                return None
            payload = msgs[0][1].get("payload")
            return json.loads(payload) if payload else None
        except Exception as e:
            logger.warning("Redis read %s failed: %s", stream, e)
            return None

    def _positions_snapshot(self):
        """从 position.changed 流重建最新持仓 (最近 200 条)。"""
        try:
            msgs = self.redis.xrevrange(f"{self.prefix}:position.changed", count=200)
            positions = {}
            for _msg_id, fields in msgs:
                try:
                    ev = json.loads(fields.get("payload", "{}"))
                except ValueError:
                    continue
                data = ev.get("data", {})
                event = data.get("event")
                sym = data.get("symbol")
                if event == "open" and sym:
                    positions[sym] = data
                elif event == "close" and sym:
                    positions.pop(sym, None)
            return positions
        except Exception as e:
            logger.warning("positions snapshot failed: %s", e)
            return {}

    def _publish_command(self, command: str, symbol: str = ""):
        data = {"command": command}
        if symbol:
            data["symbol"] = symbol
        payload = json.dumps({
            "event_id": f"tg-{int(time.time() * 1000)}",
            "stream": "command",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
            "data": data,
        }, ensure_ascii=False)
        try:
            self.redis.xadd(f"{self.prefix}:command", {"payload": payload})
            return True
        except Exception as e:
            logger.error("publish command failed: %s", e)
            return False

    # ── 命令处理 ──

    def handle(self, chat_id: int, text: str):
        if not self._authed(chat_id):
            self.send(chat_id, "未授权的 chat_id")
            return
        parts = text.strip().split()
        cmd = parts[0].lower().lstrip("/")
        arg = parts[1].upper() if len(parts) > 1 else ""

        if cmd == "status":
            hb = self._latest_event("heartbeat")
            if not hb:
                self.send(chat_id, "暂无心跳 (系统未运行?)")
                return
            stats = hb.get("data", {}).get("stats", {})
            modules = hb.get("data", {}).get("modules", {})
            mods = ", ".join(f"{m}:{age}s" for m, age in modules.items())
            self.send(chat_id, (
                f"✅ 运行中 (instance={hb.get('data', {}).get('instance', '?')})\n"
                f"K线闭合: {stats.get('kline_closes', '?')}\n"
                f"下单 成功/失败: {stats.get('orders_placed', '?')}/{stats.get('orders_failed', '?')}\n"
                f"时间偏移: {stats.get('server_time_offset', '?')}ms\n"
                f"模块心跳年龄: {mods}"
            ))
        elif cmd == "positions":
            positions = self._positions_snapshot()
            if not positions:
                self.send(chat_id, "当前无持仓")
                return
            lines = [
                f"{s}: {d.get('direction')} qty={d.get('quantity')} entry={d.get('entry_price')}"
                for s, d in positions.items()
            ]
            self.send(chat_id, "持仓:\n" + "\n".join(lines))
        elif cmd in ("pause", "resume", "stop"):
            self._publish_command("pause" if cmd == "pause" else
                                  ("emergency_stop" if cmd == "stop" else "resume"))
            self.send(chat_id, f"命令已发送: {cmd}")
        elif cmd in ("forceexit", "force_exit"):
            self._publish_command("force_exit", arg or "ALL")
            self.send(chat_id, f"平仓命令已发送: {arg or 'ALL'}")
        elif cmd in ("cancelall", "cancel_all"):
            self._publish_command("cancel_all", arg or "ALL")
            self.send(chat_id, f"清场撤单命令已发送: {arg or 'ALL'}")
        elif cmd in ("help", "start"):
            self.send(chat_id, (
                "/status 运行统计\n/positions 持仓\n"
                "/pause 暂停新信号\n/resume 恢复\n/stop 熔断停单\n"
                "/forceexit SYM|ALL 手动平仓\n/cancelall SYM|ALL 清场撤单"
            ))
        else:
            self.send(chat_id, f"未知命令: {cmd} (/help)")

    # ── 长轮询 ──

    def poll_once(self):
        updates = self._api("getUpdates", offset=self._offset, timeout=25)
        if not updates:
            return
        for upd in updates:
            self._offset = upd.get("update_id", 0) + 1
            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            chat = msg.get("chat", {})
            text = msg.get("text") or ""
            chat_id = chat.get("id")
            if chat_id is None:
                continue
            logger.info("chat=%s cmd=%s", chat_id, text)
            try:
                self.handle(chat_id, text)
            except Exception as e:
                logger.error("handle failed: %s", e)

    def run_forever(self):
        logger.info("Telegram bot started (offset=%d)", self._offset)
        while True:
            try:
                self.poll_once()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error("poll exception: %s", e)
                time.sleep(5)


def main():
    # 项目根入 sys.path (从任意目录运行均可 import shared/monitor 等)
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    parser = argparse.ArgumentParser(description="Telegram 远程控制机器人")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379"))
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""),
                        help="允许的 chat_id (逗号分隔, 空=不限制)")
    args = parser.parse_args()

    from shared.config_loader import load_env
    try:
        load_env()
    except Exception:
        pass
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN 未配置 (config/.env)")
        sys.exit(1)

    chat_ids = [c.strip() for c in args.chat_id.split(",") if c.strip()]
    if not chat_ids:
        logger.error("TELEGRAM_CHAT_ID 未配置 — 出于安全, 机器人拒绝启动 "
                     "(fail-closed)。请在 config/.env 配置你的 chat_id。")
        sys.exit(1)
    bot = TelegramBot(token, chat_ids=chat_ids, redis_url=args.redis_url)
    bot.run_forever()


if __name__ == "__main__":
    main()
