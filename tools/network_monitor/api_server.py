"""HTTP API — 暴露网络监控状态，供 Dashboard 和外部查询。"""

import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

import storage

logger = logging.getLogger(__name__)

# 最近一次探针结果（由主循环写入）
LATEST_PROBE: dict = {}


class NetworkAPIHandler(BaseHTTPRequestHandler):
    def _send_json(self, data: dict, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def do_GET(self):
        path = self.path.split("?")[0]
        query = self.path.split("?", 1)[1] if "?" in self.path else ""

        # 解析 hours 参数
        hours = 24
        for kv in query.split("&"):
            if kv.startswith("hours="):
                try:
                    hours = int(kv.split("=")[1])
                except ValueError:
                    pass

        if path == "/health":
            self._send_json({
                "status": "ok",
                "service": "network-monitor",
                "has_data": bool(LATEST_PROBE),
            })

        elif path == "/status":
            self._send_json({
                "status": "ok",
                "latest": LATEST_PROBE,
                "stats_1h": storage.compute_stats(1),
                "stats_24h": storage.compute_stats(24),
            })

        elif path == "/stats":
            self._send_json({
                "status": "ok",
                **storage.compute_stats(hours),
            })

        elif path == "/history":
            self._send_json({
                "status": "ok",
                "count": len(storage.load_history(hours)),
            })

        elif path == "/timeline":
            self._send_json({
                "status": "ok",
                "timeline": storage.get_timeline(hours),
            })

        else:
            self._send_json({"status": "error", "message": "unknown endpoint"}, 404)

    def log_message(self, format, *args):
        logger.info("API %s", format % args)


def run_api_server(host: str = "127.0.0.1", port: int = 8766):
    server = HTTPServer((host, port), NetworkAPIHandler)
    logger.info("网络监控 API 启动: http://%s:%s", host, port)
    logger.info("  GET /status    - 当前状态 + 统计")
    logger.info("  GET /stats     - 可用性统计")
    logger.info("  GET /timeline  - 时间线（图表数据）")
    logger.info("  GET /health    - 心跳")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        logger.info("API 服务已停止")