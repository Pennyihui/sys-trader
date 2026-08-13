"""sensenova 转发代理 — 把 x-api-key / Authorization: Bearer 转为 Bearer。

Claude Code v2.1+ 发 Authorization: Bearer，旧版发 x-api-key；sensenova
(token.sensenova.cn) 只认 Authorization: Bearer。这个脚本在本地启动一个
HTTP 代理把两种头统一转成 Bearer 来解决这个问题。

用法:
    python tools/sensenova_proxy.py

然后在 .claude/settings.json 里把 ANTHROPIC_BASE_URL 改为:
    http://localhost:18080/v1
"""

import http.server
import json
import urllib.request
import urllib.error
import sys
import logging

logging.basicConfig(level=logging.INFO, format="[PROXY] %(message)s")
logger = logging.getLogger(__name__)

UPSTREAM = "https://token.sensenova.cn"
PORT = 18080

class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self._forward("GET")

    def do_POST(self):
        self._forward("POST")

    def do_PUT(self):
        self._forward("PUT")

    def do_DELETE(self):
        self._forward("DELETE")

    def _forward(self, method):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""

        # 兼容两种鉴权头（Claude Code v2.1+ 发 Authorization: Bearer，旧版发 x-api-key），
        # 统一转为 Authorization: Bearer 发给 sensenova
        api_key = self.headers.get("x-api-key", "")
        if not api_key:
            auth = self.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                api_key = auth[7:].strip()
        upstream_headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        }

        upstream_url = f"{UPSTREAM}{self.path}"
        req = urllib.request.Request(upstream_url, data=body, headers=upstream_headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
                self.send_response(resp.status)
                for k, v in resp.headers.items():
                    if k.lower() in ("content-type", "content-length", "x-request-id"):
                        self.send_header(k, v)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
                logger.info("%s %s -> %s", method, self.path, resp.status)
        except urllib.error.HTTPError as e:
            data = e.read()
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)
            logger.warning("%s %s -> %s %s", method, self.path, e.code, data[:100])
        except Exception as e:
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            err = json.dumps({"error": {"message": str(e)}}).encode()
            self.wfile.write(err)
            logger.error("%s %s -> ERROR: %s", method, self.path, e)

    def log_message(self, format, *args):
        pass  # 用 logging 代替

if __name__ == "__main__":
    server = http.server.HTTPServer(("127.0.0.1", PORT), ProxyHandler)
    print(f"sensenova proxy running on http://127.0.0.1:{PORT}")
    print(f"Set ANTHROPIC_BASE_URL=http://127.0.0.1:{PORT}/v1 in settings.json")
    print(f"Upstream: {UPSTREAM}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()
