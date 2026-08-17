import net from "node:net";
import tls from "node:tls";
import { PassThrough } from "node:stream";
import https from "node:https";

const cfg = {
  upstreamHost: "opencode.ai",
  userAgent: "opencode/1.18.9",
};

export function configureGatewayClient(options = {}) {
  if (options.upstreamUrl) {
    try {
      const u = new URL(options.upstreamUrl);
      cfg.upstreamHost = u.hostname;
    } catch {
      cfg.upstreamHost = "opencode.ai";
    }
  }
  if (options.userAgent) cfg.userAgent = options.userAgent;
}

const DEFAULT_PROXY_PORT = 8080;

function parseProxy(entry) {
  if (!entry) return null;
  let s = String(entry).trim();
  if (!s) return null;
  s = s.replace(/^https?:\/\//i, "");
  if (s.includes("@")) s = s.split("@").pop();
  const idx = s.lastIndexOf(":");
  if (idx === -1) return { host: s, port: DEFAULT_PROXY_PORT };
  return { host: s.slice(0, idx), port: parseInt(s.slice(idx + 1), 10) || DEFAULT_PROXY_PORT };
}

let cursor = 0;
export function nextProxyIndex(proxyList) {
  cursor = (cursor + 1) % Math.max(proxyList.length, 1);
  return cursor;
}

function readHead(socket) {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    const onData = (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      const i = buf.indexOf("\r\n\r\n");
      if (i !== -1) {
        socket.off("data", onData);
        const head = buf.subarray(0, i).toString("latin1").split("\r\n");
        const [codeLine, ...hdrLines] = head;
        const status = parseInt(codeLine.split(" ")[1] || "0", 10);
        const headers = {};
        for (const h of hdrLines) {
          const c = h.indexOf(":");
          if (c === -1) continue;
          headers[h.slice(0, c).trim().toLowerCase()] = (headers[h.slice(0, c).trim().toLowerCase()]
            ? headers[h.slice(0, c).trim().toLowerCase()] + ", "
            : "") + h.slice(c + 1).trim();
        }
        resolve({ status, headers, body: buf.subarray(i + 4) });
      }
    };
    socket.on("data", onData);
    socket.on("error", (e) => reject(e));
    socket.on("end", () => reject(new Error("connection closed before response head")));
  });
}

function connectViaProxy(proxy, timeoutMs) {
  return new Promise((resolve, reject) => {
    const sock = net.connect({ host: proxy.host, port: proxy.port });
    sock.setTimeout(timeoutMs, () => {
      sock.destroy(new Error(`proxy connect timeout (${proxy.host}:${proxy.port})`));
    });
    sock.on("connect", () => {
      sock.write(
        `CONNECT ${cfg.upstreamHost}:443 HTTP/1.1\r\nHost: ${cfg.upstreamHost}:443\r\nProxy-Connection: keep-alive\r\n\r\n`
      );
    });
    sock.on("error", (e) => reject(e));
    readHead(sock)
      .then(({ status }) => {
        if (status >= 200 && status < 300) resolve(sock);
        else {
          sock.destroy();
          reject(new Error(`proxy CONNECT refused (${status}) via ${proxy.host}:${proxy.port}`));
        }
      })
      .catch((e) => reject(e));
  });
}

function tlsWrap(sock, timeoutMs) {
  return new Promise((resolve, reject) => {
    const t = tls.connect({ socket: sock, servername: cfg.upstreamHost });
    t.setTimeout(timeoutMs, () => t.destroy(new Error("TLS handshake timeout")));
    t.on("secureConnect", () => {
      t.removeAllListeners("timeout");
      resolve(t);
    });
    t.on("error", (e) => reject(e));
  });
}

function buildRequestLine(method, path, headers, bodyBuf) {
  const hs = ["Host: " + cfg.upstreamHost, "Connection: close", "Content-Length: " + bodyBuf.length];
  for (const [k, v] of Object.entries(headers)) {
    const lk = k.toLowerCase();
    if (lk === "host" || lk === "connection" || lk === "content-length") continue;
    hs.push(`${k}: ${v}`);
  }
  return `${method} ${path} HTTP/1.1\r\n${hs.join("\r\n")}\r\n\r\n`;
}

function collectBody(source, leftovers, stream) {
  const chunks = leftovers.length ? [leftovers] : [];
  const done = new Promise((resolve, reject) => {
    source.on("data", (ch) => chunks.push(ch));
    source.on("error", (e) => reject(e));
    source.on("end", () => resolve(Buffer.concat(chunks)));
    source.on("close", () => {
      if (!source.readableEnded) reject(new Error("upstream closed connection prematurely"));
    });
  });
  if (!stream) return { stream: null, done };
  const out = new PassThrough();
  if (leftovers.length) out.write(leftovers);
  source.on("data", (ch) => out.write(ch));
  source.on("end", () => out.end());
  source.on("error", () => {
    try {
      out.destroy();
    } catch {}
  });
  return { stream: out, done };
}

export async function gatewayRequest({ proxyList = [], method = "POST", path = "/zen/v1/chat/completions", headers = {}, body = "", stream = false, timeoutMs = 20000, direct = true, maxAttempts = 6 } = {}) {
  const bodyBuf = Buffer.from(typeof body === "string" ? body : JSON.stringify(body), "utf8");
  const reqHeaders = {
    "User-Agent": cfg.userAgent,
    "Content-Type": "application/json",
    Accept: stream ? "text/event-stream" : "application/json",
    ...headers,
  };

  const failures = [];
  const attempts = [];

  if (proxyList.length > 0) {
    const chosen = [];
    const start = nextProxyIndex(proxyList);
    for (let i = 0; i < Math.min(proxyList.length, maxAttempts); i++) {
      chosen.push(proxyList[(start + i) % proxyList.length]);
    }
    for (const p of chosen) {
      const proxy = parseProxy(p);
      attempts.push({ via: p });
      try {
        const pex = await connectViaProxy(proxy, timeoutMs);
        const tlsSock = await tlsWrap(pex, timeoutMs);
        tlsSock.on("error", () => {});
        const hp = readHead(tlsSock);
        tlsSock.write(buildRequestLine(method, path, reqHeaders, bodyBuf));
        tlsSock.write(bodyBuf);
        const { status, headers: rh, leftovers } = await hp;
        attempts[attempts.length - 1].status = status;
        const bodyRes = collectBody(tlsSock, leftovers, stream);
        if (stream && status === 200) {
          return { status, headers: rh, bodyStream: bodyRes.stream, via: p };
        }
        const full = (await bodyRes.done).toString("utf8");
        attempts[attempts.length - 1].status = status;
        if (status >= 200 && status < 300) return { status, headers: rh, body: full, via: p };
        if (status >= 300 && status < 500 && status !== 429) {
          return { status, headers: rh, body: full, via: p };
        }
        failures.push({ p, status });
      } catch (e) {
        attempts[attempts.length - 1].error = e.message;
        failures.push({ p, err: e.message, stack: e.stack });
      }
    }
  }

  if (direct) {
    try {
      const res = await new Promise((resolve, reject) => {
        const req = https.request(
          {
            hostname: cfg.upstreamHost,
            port: 443,
            path,
            method,
            headers: { ...reqHeaders, "Content-Length": bodyBuf.length, Connection: "close" },
          },
          resolve
        );
        req.setTimeout(timeoutMs, () => req.destroy(new Error("direct request timeout")));
        req.on("error", reject);
        req.write(bodyBuf);
        req.end();
      });
      const status = res.statusCode;
      const bodyRes = collectBody(res, Buffer.alloc(0), stream);
      if (stream && status === 200) {
        return { status, headers: res.headers, bodyStream: bodyRes.stream, via: "direct" };
      }
      const full = (await bodyRes.done).toString("utf8");
      return { status, headers: res.headers, body: full, via: "direct" };
    } catch (e) {
      failures.push({ direct: e.message });
    }
  }

  const last = failures[failures.length - 1] || {};
  return { status: last.status || 502, headers: {}, body: JSON.stringify({ error: { message: `all ${attempts.length} proxy attempt(s) failed: ${JSON.stringify(failures)}`, type: "upstream_failure" } }), via: "none" };
}

export async function probeProxy(proxyEntry, timeoutMs = 12000) {
  const proxy = parseProxy(proxyEntry);
  if (!proxy) return false;
  try {
    const pex = await connectViaProxy(proxy, timeoutMs);
    const tlsSock = await tlsWrap(pex, timeoutMs);
    const body = JSON.stringify({
      model: "deepseek-v4-flash-free",
      messages: [{ role: "user", content: "ping" }],
      max_tokens: 1,
    });
    const hp = readHead(tlsSock);
    tlsSock.write(buildRequestLine("POST", "/zen/v1/chat/completions", { "User-Agent": cfg.userAgent, "Content-Type": "application/json", "Content-Length": body.length }, Buffer.from(body)));
    tlsSock.write(Buffer.from(body));
    const { status } = await hp;
    tlsSock.destroy();
    return status === 200;
  } catch {
    return false;
  }
}

export async function mergeProxies(currentList, candidates, probeAll = true) {
  const set = new Set(currentList.map((p) => p.toLowerCase().trim()));
  for (const c of candidates) set.add(c.toLowerCase().trim());
  return [...set];
}