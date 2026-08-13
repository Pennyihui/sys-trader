"""网络连通性检查 — 本地/代理/API 三层"""
import requests, socket, time

# 1. DNS
try:
    ip = socket.gethostbyname("fapi.binance.com")
    print(f"[DNS] fapi.binance.com -> {ip} OK")
except Exception as e:
    print(f"[DNS] 失败: {e}")

# 2. 网关（动态获取默认网关，不硬编码 192.168.1.1）
import re, subprocess
gw = None
try:
    r = subprocess.run(["route", "print", "0.0.0.0"], capture_output=True, text=True, timeout=5)
    m = re.search(r"0\.0\.0\.0\s+0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)", r.stdout)
    gw = m.group(1) if m else None
except Exception:
    pass
if gw:
    r = subprocess.run(["ping", "-n", "1", "-w", "2000", gw], capture_output=True, text=True)
    # 用 returncode 判断，不依赖 ping 输出的语言（中文"时间="/英文"time="）
    print(f"[网关] {gw} {'通' if r.returncode == 0 else '不通'}")
else:
    print("[网关] 无法获取默认网关")

# 3. 互联网
r = subprocess.run(["ping", "-n", "1", "-w", "2000", "223.5.5.5"], capture_output=True, text=True)
print(f"[互联网] {'通' if r.returncode == 0 else '不通'}")

# 4. Clash 代理
proxies = {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"}
try:
    t0 = time.time()
    r = requests.get("https://api.binance.com/api/v3/ping", proxies=proxies, timeout=8)
    print(f"[Binance] 延迟={(time.time()-t0)*1000:.0f}ms 状态={r.status_code}")
except Exception as e:
    print(f"[Binance] 失败: {type(e).__name__}: {str(e)[:80]}")

# 5. Testnet
try:
    t0 = time.time()
    r = requests.get("https://testnet.binancefuture.com/fapi/v1/ping", proxies=proxies, timeout=8)
    print(f"[Testnet] 延迟={(time.time()-t0)*1000:.0f}ms 状态={r.status_code}")
except Exception as e:
    print(f"[Testnet] 失败: {type(e).__name__}: {str(e)[:80]}")
