import http from "node:http";
import { configureGatewayClient, gatewayRequest, probeProxy, nextProxyIndex } from "./proxy-client.mjs";

const PORT = parseInt(process.env.PORT || "9090", 10);
const MODEL = process.env.MODEL || "deepseek-v4-flash-free";
const UPSTREAM_URL = process.env.UPSTREAM_URL || "https://opencode.ai/zen/v1/chat/completions";
const USER_AGENT = process.env.USER_AGENT || "opencode/1.18.9";
const API_KEY = process.env.API_KEY || "";
const DIRECT_FALLBACK = (process.env.DIRECT_FALLBACK || "true").toLowerCase() !== "false";
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || "12000", 10);
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || "4", 10);
const REFRESH_SECONDS = parseInt(process.env.REFRESH_SECONDS || "1800", 10);
const REFRESH_URL = process.env.REFRESH_URL || "";

const RAW_PROXIES = (process.env.PROXY_LIST || "")
  .split(/[\s,]+/)
  .map((s) => s.trim())
  .filter(Boolean);
let proxyList = [...RAW_PROXIES];

configureGatewayClient({ upstreamUrl: UPSTREAM_URL, userAgent: USER_AGENT });

const upstreamPath = (() => {
  try {
    return new URL(UPSTREAM_URL).pathname;
  } catch {
    return "/zen/v1/chat/completions";
  }
})();

function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function assertAuth(req, res) {
  if (!API_KEY) return true;
  const bearer = req.headers["authorization"] || "";
  const xkey = req.headers["x-api-key"] || "";
  return bearer === `Bearer ${API_KEY}` || xkey === API_KEY;
}

async function refreshPool() {
  if (!REFRESH_URL) return;
  try {
    const r = await fetch(REFRESH_URL, { timeout: 20000 });
    const data = await r.json();
    const entries = Array.isArray(data) ? data : data.data || [];
    const candidates = new Set();
    for (const e of entries) {
      if (!e || !e.ip || !e.port) continue;
      const protos = String(e.protocols || "").toLowerCase();
      if (!protos.includes("http")) continue;
      candidates.add(`${e.ip}:${e.port}`);
    }
    const current = new Set(proxyList.map((p) => p.toLowerCase()));
    const fresh = [];
    for (const c of [...candidates]) {
      if (!current.has(c.toLowerCase())) {
        try {
          if (await probeProxy(c)) fresh.push(c);
        } catch {}
      }
    }
    if (fresh.length > 0) {
      proxyList = [...proxyList, ...fresh].slice(-200);
      console.log(`[refresh] proxy pool now ${proxyList.length} (added ${fresh.length})`);
    }
  } catch (e) {
    console.error(`[refresh] failed: ${e.message}`);
  }
}

if (REFRESH_URL) {
  refreshPool();
  setInterval(() => refreshPool(), REFRESH_SECONDS * 1000);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const path = url.pathname;
  const method = req.method;

  res.on("error", () => {});
  if (method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    });
    return res.end();
  }

  if (path === "/health" || path === "/") {
    return json(res, 200, { ok: true, model: MODEL, proxyCount: proxyList.length, proxies: proxyList });
  }

  if (path === "/v1/models" || path === "/models") {
    return json(res, 200, {
      object: "list",
      data: [
        {
          id: MODEL,
          object: "model",
          created: 1786990648,
          owned_by: "opencode-gateway",
        },
      ],
    });
  }

  const isChat = path === "/v1/chat/completions" || path === "/chat/completions";
  if (!isChat || method !== "POST") {
    return json(res, 404, { error: { message: `not found: ${method} ${path}` } });
  }

  if (!assertAuth(req, res)) {
    return json(res, 401, { error: { message: "invalid API key" } });
  }

  let bodyRaw = "";
  try {
    for await (const ch of req) bodyRaw += ch;
  } catch {
    return json(res, 400, { error: { message: "failed to read body" } });
  }

  let payload;
  try {
    payload = JSON.parse(bodyRaw || "{}");
  } catch {
    return json(res, 400, { error: { message: "invalid JSON body" } });
  }

  payload.model = payload.model || MODEL;
  const wantStream = !!payload.stream;

  let upstreamBody;
  try {
    upstreamBody = JSON.stringify(payload);
  } catch {
    return json(res, 400, { error: { message: "body not JSON-serializable" } });
  }

  const result = await gatewayRequest({
    proxyList,
    path: upstreamPath,
    body: upstreamBody,
    stream: wantStream,
    timeoutMs: TIMEOUT_MS,
    direct: DIRECT_FALLBACK,
    maxAttempts: MAX_ATTEMPTS,
  });

  if (result.via === "none") console.error(`[req] all attempts failed (proxies=${proxyList.length}) ${result.body.slice(0, 300)}`);
  else console.log(`[req] via=${result.via} status=${result.status}`);

  if (wantStream && result.status === 200 && result.bodyStream) {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
    });
    result.bodyStream.on("error", (e) => {
      res.destroy();
    });
    result.bodyStream.on("end", () => res.end());
    res.on("close", () => result.bodyStream.destroy());
    result.bodyStream.on("data", (chunk) => {
      try {
        res.write(chunk);
      } catch {}
    });
    return;
  }

  json(res, result.status, (() => {
    try {
      return JSON.parse(result.body);
    } catch {
      return { error: { message: result.body } };
    }
  })());
});

server.listen(PORT, () => {
  console.log(`opencode-gateway listening on :${PORT}`);
  console.log(`model=${MODEL} proxies=${proxyList.length} direct_fallback=${DIRECT_FALLBACK}`);
});