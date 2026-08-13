"""验证 OrderGateway 下单请求实际走不走代理"""
import os, sys, time, json, socket
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("=== 1. 环境变量 ===")
print(f"HTTP_PROXY  = {os.environ.get('HTTP_PROXY', '(unset)')}")
print(f"HTTPS_PROXY = {os.environ.get('HTTPS_PROXY', '(unset)')}")

print("\n=== 2. requests 实际会用哪个代理 ===")
import requests
print(f"requests.getproxies() = {requests.utils.getproxies()}")

print("\n=== 3. OrderGateway 实际行为 ===")
from execution.order_gateway import OrderGateway
gw = OrderGateway(testnet=True)
# 检查有没有 proxies 属性
print(f"gw 有 proxies 属性: {hasattr(gw, 'proxies')}")

print("\n=== 4. 对比测试: 显式代理 vs 无代理 ===")
proxies = {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"}

# 4a. 无 proxies（模拟当前 OrderGateway 行为）
try:
    t0 = time.time()
    r = requests.get("https://testnet.binancefuture.com/fapi/v1/ping", timeout=8)
    print(f"[无代理] testnet ping: {r.status_code} 延迟={(time.time()-t0)*1000:.0f}ms")
except Exception as e:
    print(f"[无代理] testnet ping 失败: {type(e).__name__}: {str(e)[:60]}")

# 4b. 显式代理（模拟修复后）
try:
    t0 = time.time()
    r = requests.get("https://testnet.binancefuture.com/fapi/v1/ping", proxies=proxies, timeout=8)
    print(f"[走代理] testnet ping: {r.status_code} 延迟={(time.time()-t0)*1000:.0f}ms")
except Exception as e:
    print(f"[走代理] testnet ping 失败: {type(e).__name__}: {str(e)[:60]}")

# 4c. 实盘对比
try:
    t0 = time.time()
    r = requests.get("https://fapi.binance.com/fapi/v1/ping", timeout=8)
    print(f"[无代理] 实盘 ping: {r.status_code} 延迟={(time.time()-t0)*1000:.0f}ms")
except Exception as e:
    print(f"[无代理] 实盘 ping 失败: {type(e).__name__}: {str(e)[:60]}")

try:
    t0 = time.time()
    r = requests.get("https://fapi.binance.com/fapi/v1/ping", proxies=proxies, timeout=8)
    print(f"[走代理] 实盘 ping: {r.status_code} 延迟={(time.time()-t0)*1000:.0f}ms")
except Exception as e:
    print(f"[走代理] 实盘 ping 失败: {type(e).__name__}: {str(e)[:60]}")
