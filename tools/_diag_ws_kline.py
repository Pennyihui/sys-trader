"""临时诊断: testnet WS 15m kline 流端到端验证 (12 秒窗口)。"""
import json
import threading
import time
import websocket as ws_client

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
url = ("wss://stream.binancefuture.com/stream?streams=" +
       "/".join(f"{s}@kline_15m" for s in SYMBOLS) +
       "/" + "/".join(f"{s}@aggTrade" for s in SYMBOLS))

stats = {"kline": 0, "agg": 0, "kline_closed": 0, "sample": None}
ws = [None]


def on_message(_, raw):
    try:
        d = json.loads(raw)
        inner = d.get("data", d)
        e = inner.get("e", "")
        if e == "kline":
            stats["kline"] += 1
            k = inner.get("k", {})
            if k.get("x"):
                stats["kline_closed"] += 1
            if stats["sample"] is None:
                stats["sample"] = {"s": inner.get("s"), "i": k.get("i"),
                                   "open_time": k.get("t"), "x": k.get("x"),
                                   "close": k.get("c")}
        elif e == "aggTrade":
            stats["agg"] += 1
    except Exception as ex:
        print("parse err:", ex)


def runner():
    ws[0] = ws_client.WebSocketApp(
        url, on_message=on_message,
        on_error=lambda w, e: print("WS error:", e),
    )
    ws[0].run_forever(http_proxy_host="127.0.0.1", http_proxy_port=7897,
                      proxy_type="http", ping_interval=60, ping_timeout=30,
                      http_proxy_timeout=20)


t = threading.Thread(target=runner, daemon=True)
t.start()
time.sleep(12)
if ws[0]:
    ws[0].close()
print(json.dumps(stats, ensure_ascii=False, indent=1))
