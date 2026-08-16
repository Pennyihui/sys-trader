"""HTTP API Server — 暴露代理池状态供 Dashboard 和外部系统查询。"""

import json
import logging
import os
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict

logger = logging.getLogger(__name__)

POOL_PATH = os.path.join(os.path.dirname(__file__), "proxy_pool.json")


def _load_pool() -> Dict:
    try:
        with open(POOL_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


class PoolAPIHandler(BaseHTTPRequestHandler):
    """HTTP API 请求处理器。"""

    # 2026-08-16 审计: /proxies 返回含 password/uuid 的节点全量, 必须鉴权;
    # 同时移除 CORS *, 防任意网页跨域窃取代理凭据
    SECRET = os.environ.get("PROXY_POOL_API_TOKEN", "proxy-pool-2026")

    def _authed(self) -> bool:
        auth = self.headers.get("Authorization", "")
        return auth == f"Bearer {self.SECRET}"

    def _send_json(self, data: dict, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def do_GET(self):
        pool = _load_pool()
        proxies = pool.get("proxies", [])
        healthy = [p for p in proxies if p.get("healthy")]
        unhealthy = [p for p in proxies if not p.get("healthy")]

        # 解析 path + query（不再做整串精确匹配，参数顺序变化也能命中）
        path, _, query = self.path.partition("?")
        params = urllib.parse.parse_qs(query)

        if path == "/health":
            self._send_json({
                "status": "ok",
                "service": "proxy-pool",
                "pool_exists": bool(pool.get("proxies")),
            })

        elif self.path == "/status":
            self._send_json({
                "status": "ok",
                "total": len(proxies),
                "healthy": len(healthy),
                "unhealthy": len(unhealthy),
                "last_updated": pool.get("last_updated", ""),
                "cleanup_days": pool.get("cleanup", {}).get("max_fail_days", 7),
            })

        elif path == "/proxies":
            if not self._authed():
                self._send_json({"status": "error", "message": "unauthorized"}, 401)
                return
            if params.get("healthy", ["false"])[0].lower() == "true":
                self._send_json({
                    "status": "ok",
                    "total": len(healthy),
                    "proxies": healthy,
                })
            else:
                self._send_json({
                    "status": "ok",
                    "total": len(proxies),
                    "proxies": proxies,
                })

        elif path == "/metrics":
            self._send_json({
                "status": "ok",
                "total": len(proxies),
                "healthy": len(healthy),
                "unhealthy": len(unhealthy),
                "healthy_ratio": round(len(healthy) / max(len(proxies), 1), 4),
                "last_updated": pool.get("last_updated", ""),
            })

        else:
            self._send_json({"status": "error", "message": "unknown endpoint"}, 404)

    def log_message(self, format, *args):
        logger.info("API %s", format % args)


def run_api_server(host: str = "127.0.0.1", port: int = 8765):
    """启动 HTTP API 服务（阻塞）。"""
    server = HTTPServer((host, port), PoolAPIHandler)
    logger.info("API 服务启动: http://%s:%s", host, port)
    logger.info("  GET /status    - 池子统计")
    logger.info("  GET /proxies   - 节点列表")
    logger.info("  GET /health    - 心跳")
    logger.info("  GET /metrics   - 指标")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        logger.info("API 服务已停止")