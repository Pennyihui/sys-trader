"""检查：行情 vs 下单 连接状态"""
import os, sys, hmac, hashlib, time, requests
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.config_loader import load_env
load_env()

proxies = {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"}

# 1. 实盘合约行情（公开，无key）
try:
    r = requests.get("https://fapi.binance.com/fapi/v1/klines",
                     params={"symbol": "BTCUSDT", "interval": "15m", "limit": 1},
                     proxies=proxies, timeout=8)
    d = r.json()
    if isinstance(d, list):
        print(f"[实盘行情] OK close={float(d[-1][4]):.2f}")
    else:
        print(f"[实盘行情] {d}")
except Exception as e:
    print(f"[实盘行情] 失败: {type(e).__name__}: {str(e)[:60]}")

# 2. 实盘合约账户（需要key）
key = os.environ.get("BINANCE_API_KEY", "")
secret = os.environ.get("BINANCE_API_SECRET", "")
if key:
    params = {"timestamp": int(time.time() * 1000)}
    q = urlencode(params)
    sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    try:
        r = requests.get(f"https://fapi.binance.com/fapi/v2/account?{q}&signature={sig}",
                         headers={"X-MBX-APIKEY": key}, proxies=proxies, timeout=8)
        d = r.json()
        if "canTrade" in d:
            print(f"[实盘账户] OK canTrade={d['canTrade']}")
        else:
            print(f"[实盘账户] {d}")
    except Exception as e:
        print(f"[实盘账户] 失败: {type(e).__name__}: {str(e)[:60]}")
else:
    print("[实盘账户] 跳过（无key或读不到）")

# 3. testnet 账户（与实盘一样，无 key 时跳过，避免带空 key/secret 打 401）
if key:
    try:
        params = {"timestamp": int(time.time() * 1000)}
        q = urlencode(params)
        sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
        r = requests.get(f"https://testnet.binancefuture.com/fapi/v2/account?{q}&signature={sig}",
                         headers={"X-MBX-APIKEY": key}, proxies=proxies, timeout=8)
        d = r.json()
        if "canTrade" in d:
            total = sum(float(a.get("walletBalance", 0)) for a in d.get("assets", []))
            print(f"[testnet账户] OK 余额={total:.2f}")
        else:
            print(f"[testnet账户] {d}")
    except Exception as e:
        print(f"[testnet账户] 失败: {type(e).__name__}: {str(e)[:60]}")
else:
    print("[testnet账户] 跳过（无key或读不到）")
