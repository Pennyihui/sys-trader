/**
 * sensenova 转发代理 — 接受 x-api-key 或 Authorization: Bearer
 *
 * Claude Code v2.1+ 发的是 Authorization: Bearer，不是 x-api-key。
 * 这个代理把两种方式都转成 Bearer 发给 sensenova。
 *
 * 用法:
 *   node tools/proxy.js
 *
 * Quent/.claude/settings.json 里设:
 *   ANTHROPIC_BASE_URL=http://127.0.0.1:15721
 */

const http = require("http");
const https = require("https");

const UPSTREAM = "token.sensenova.cn";
const PORT = 15721;

const server = http.createServer((req, res) => {
  const ts = new Date().toISOString().substring(11, 19);

  // 从 x-api-key 或 Authorization: Bearer 中提取 key
  let apiKey = req.headers["x-api-key"] || "";
  if (!apiKey && req.headers["authorization"]) {
    const m = req.headers["authorization"].match(/^Bearer\s+(.+)$/i);
    if (m) apiKey = m[1];
  }

  const logKey = apiKey ? apiKey.substring(0, 10) + "..." : "(missing)";
  process.stdout.write(`[${ts}] key:${logKey} `);

  if (!apiKey) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "no api key found" }));
    process.stdout.write("REJECTED\n");
    return;
  }

  // 收集请求体
  let body = [];
  req.on("data", (chunk) => body.push(chunk));
  req.on("end", () => {
    body = Buffer.concat(body);

    const options = {
      hostname: UPSTREAM,
      path: req.url,
      method: req.method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
        "Content-Length": body.length,
      },
    };

    const proxyReq = https.request(options, (proxyRes) => {
      let chunks = [];
      proxyRes.on("data", (c) => chunks.push(c));
      proxyRes.on("end", () => {
        const data = Buffer.concat(chunks);
        res.writeHead(proxyRes.statusCode, {
          "Content-Type": proxyRes.headers["content-type"] || "application/json",
          "Access-Control-Allow-Origin": "*",
        });
        res.end(data);
        process.stdout.write(`-> ${proxyRes.statusCode}\n`);
      });
    });

    proxyReq.on("error", (e) => {
      console.error(`[${ts}] ERROR: ${e.message}`);
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: { message: e.message } }));
    });

    if (body.length > 0) proxyReq.write(body);
    proxyReq.end();
  });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`sensenova proxy running on http://127.0.0.1:${PORT}`);
  console.log(`Accepts: x-api-key OR Authorization: Bearer`);
  console.log(`Upstream: https://${UPSTREAM}`);
});
