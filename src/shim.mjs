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
const FORCE_MODEL = (process.env.FORCE_MODEL || "true").toLowerCase() !== "false";
const MODEL_CONTEXT = parseInt(process.env.MODEL_CONTEXT_WINDOW || "200000", 10);
const MODEL_ALIASES = (process.env.MODEL_ALIASES || "gpt-4o,gpt-4o-mini,gpt-4.1-mini")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const resolveModel = (requested) => (FORCE_MODEL ? MODEL : requested || MODEL);

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

function toPlainText(parts) {
  if (typeof parts === "string") return parts;
  if (Array.isArray(parts)) {
    return parts
      .map((p) => {
        if (typeof p === "string") return p;
        if (p && typeof p.text === "string") return p.text;
        return "";
      })
      .join("\n");
  }
  if (parts && typeof parts.text === "string") return parts.text;
  return "";
}

function normalizeTools(tools) {
  if (!Array.isArray(tools)) return tools;
  return tools.map((t) => {
    if (!t || typeof t !== "object") return t;
    if (t.type === "function" && t.function && typeof t.function === "object") return t;
    if (t.type === "function" && t.name) {
      const fn = {
        name: t.name,
        description: t.description || "",
        parameters: t.parameters || { type: "object", properties: {} },
      };
      if (t.strict != null) fn.strict = t.strict;
      return { type: "function", function: fn };
    }
    return t;
  });
}

function responsesToMessages(input, instructions) {
  const out = [];
  if (instructions) {
    const instr = toPlainText(instructions).trim();
    if (instr) out.push({ role: "system", content: instr });
  }
  const items = Array.isArray(input) ? input : [input];
  for (const it of items) {
    if (typeof it === "string") out.push({ role: "user", content: it });
    else if (it && it.type === "message") out.push({ role: it.role || "user", content: toPlainText(it.content) });
    else if (it && it.type === "input_text") out.push({ role: "user", content: it.text });
    else if (it && it.type === "function_call_output") out.push({ role: "tool", tool_call_id: it.call_id, content: String(it.output ?? "") });
  }
  const merged = [];
  for (const m of out) {
    const last = merged[merged.length - 1];
    if (last && last.role === m.role && last.role !== "tool") last.content += "\n" + m.content;
    else merged.push({ ...m });
  }
  return merged;
}

function buildResponsesObject(up, model) {
  const content = up.choices?.[0]?.message?.content || "";
  const msg = up.choices?.[0]?.message || {};
  const usage = up.usage || {};
  const output = [];
  if (Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
    for (const tc of msg.tool_calls) {
      output.push({
        id: tc.id || "call_" + Date.now().toString(36),
        type: "function_call",
        status: "completed",
        call_id: tc.id || "call_" + Date.now().toString(36),
        name: tc.function?.name || "",
        arguments: tc.function?.arguments || "",
      });
    }
  } else {
    output.push({
      type: "message",
      id: "msg_" + Date.now().toString(36),
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text: content, annotations: [] }],
    });
  }
  return {
    id: "resp_" + String(up.id || Date.now().toString(36)).replace(/^(chatcmpl|router)[-_]?/, ""),
    object: "response",
    created_at: up.created || Math.floor(Date.now() / 1000),
    status: "completed",
    model: up.model || model,
    output,
    usage: {
      input_tokens: usage.prompt_tokens || 0,
      output_tokens: usage.completion_tokens || 0,
      total_tokens: usage.total_tokens || 0,
    },
  };
}

function responsesSse(reqStream, res, model) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "*",
  });
  const id = "resp_" + Date.now().toString(36);
  const itemId = "msg_" + Date.now().toString(36);
  const send = (event, data) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  const responseSkel = (status) => ({ id, object: "response", status, model, output: [] });
  send("response.created", { type: "response.created", response: responseSkel("in_progress") });
  send("response.in_progress", { type: "response.in_progress", response: responseSkel("in_progress") });
  send("response.output_item.added", {
    type: "response.output_item.added",
    output_index: 0,
    item: { id: itemId, type: "message", status: "in_progress", role: "assistant", content: [] },
  });
  send("response.content_part.added", {
    type: "response.content_part.added",
    item_id: itemId,
    output_index: 0,
    content_index: 0,
    part: { type: "output_text", text: "", annotations: [] },
  });

  const onError = (e) => {
    try {
      res.destroy();
    } catch {}
  };
  reqStream.on("error", onError);
  res.on("close", () => reqStream.destroy());

  let buf = "";
  let text = "";
  const toolCalls = {};
  reqStream.on("data", (chunk) => {
    buf += chunk.toString("utf8");
    let idx;
    while ((idx = buf.indexOf("\n")) !== -1) {
      const line = buf.slice(0, idx);
      buf = buf.slice(idx + 1);
      if (!line.startsWith("data: ")) continue;
      const data = line.slice(6).trim();
      if (data === "[DONE]") continue;
      let obj;
      try {
        obj = JSON.parse(data);
      } catch {
        continue;
      }
      const delta = obj.choices?.[0]?.delta;
      if (!delta) continue;
      if (delta.content) {
        text += delta.content;
        send("response.output_text.delta", { type: "response.output_text.delta", item_id: itemId, output_index: 0, content_index: 0, delta: delta.content });
      }
      if (Array.isArray(delta.tool_calls)) {
        for (const tc of delta.tool_calls) {
          const key = tc.index != null ? String(tc.index) : (tc.id || "0");
          if (!toolCalls[key]) {
            toolCalls[key] = { id: tc.id || `call_${key}_${Date.now().toString(36)}`, name: "", arguments: "" };
            send("response.output_item.added", {
              type: "response.output_item.added",
              output_index: Object.keys(toolCalls).length,
              item: { id: toolCalls[key].id, type: "function_call", status: "in_progress", call_id: toolCalls[key].id, name: "", arguments: "" },
            });
          }
          if (tc.id) toolCalls[key].id = tc.id;
          if (tc.function?.name) toolCalls[key].name += tc.function.name;
          if (tc.function?.arguments != null) toolCalls[key].arguments += tc.function.arguments;
        }
      }
    }
  });
  reqStream.on("end", () => {
    const calls = Object.values(toolCalls);
    if (calls.length > 0) {
      for (const tc of calls) {
        send("response.output_item.done", {
          type: "response.output_item.done",
          output_index: 0,
          item: { id: tc.id, type: "function_call", status: "completed", call_id: tc.id, name: tc.name, arguments: tc.arguments },
        });
      }
      send("response.completed", { type: "response.completed", response: { id, object: "response", status: "completed", model, output: calls.map((tc) => ({ id: tc.id, type: "function_call", status: "completed", call_id: tc.id, name: tc.name, arguments: tc.arguments })), usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 } } });
    } else {
      send("response.output_text.done", { type: "response.output_text.done", item_id: itemId, output_index: 0, content_index: 0, text });
      send("response.content_part.done", {
        type: "response.content_part.done",
        item_id: itemId,
        output_index: 0,
        content_index: 0,
        part: { type: "output_text", text, annotations: [] },
      });
      const msg = { id: itemId, type: "message", status: "completed", role: "assistant", content: [{ type: "output_text", text, annotations: [] }] };
      send("response.output_item.done", { type: "response.output_item.done", output_index: 0, item: msg });
      send("response.completed", { type: "response.completed", response: { id, object: "response", status: "completed", model, output: [msg], usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 } } });
    }
    res.end();
  });
}

async function handleResponses(payload, res) {
  const wantStream = !!payload.stream;
  const messages = responsesToMessages(payload.input, payload.instructions);
  const chatBody = { model: resolveModel(payload.model), messages, stream: wantStream };
  if (payload.max_output_tokens) chatBody.max_tokens = payload.max_output_tokens;
  if (payload.temperature != null) chatBody.temperature = payload.temperature;
  if (payload.top_p != null) chatBody.top_p = payload.top_p;
  if (payload.tools) chatBody.tools = normalizeTools(payload.tools);
  if (payload.tool_choice != null) chatBody.tool_choice = payload.tool_choice;

  const result = await gatewayRequest({
    proxyList,
    path: upstreamPath,
    body: JSON.stringify(chatBody),
    stream: wantStream,
    timeoutMs: TIMEOUT_MS,
    direct: DIRECT_FALLBACK,
    maxAttempts: MAX_ATTEMPTS,
  });
  if (result.via === "none") console.error(`[resp] all attempts failed ${result.body.slice(0, 300)}`);
  else console.log(`[resp] via=${result.via} status=${result.status}`);

  if (wantStream && result.status === 200 && result.bodyStream) {
    return responsesSse(result.bodyStream, res, chatBody.model);
  }
  if (result.status >= 200 && result.status < 300) {
    try {
      const up = JSON.parse(result.body);
      return json(res, result.status, buildResponsesObject(up, chatBody.model));
    } catch {
      return json(res, result.status, { error: { message: result.body } });
    }
  }
  try {
    const up = JSON.parse(result.body);
    return json(res, result.status, up);
  } catch {
    return json(res, result.status, { error: { message: result.body } });
  }
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
    const mk = (id) => ({
      id,
      object: "model",
      created: 1786990648,
      owned_by: "opencode-gateway",
      context_window: MODEL_CONTEXT,
      context_length: MODEL_CONTEXT,
      max_model_len: MODEL_CONTEXT,
      limits: { context: MODEL_CONTEXT, output: 128000 },
      meta: {
        context_length: MODEL_CONTEXT,
        context_window: MODEL_CONTEXT,
        limits: { context: MODEL_CONTEXT, output: 128000 },
        max_model_len: MODEL_CONTEXT,
      },
    });
    return json(res, 200, {
      object: "list",
      data: [mk(MODEL), ...MODEL_ALIASES.map((a) => mk(a))],
    });
  }

  const isResp = path === "/v1/responses" || path === "/responses";
  const isChat = path === "/v1/chat/completions" || path === "/chat/completions";
  if ((!isResp && !isChat) || method !== "POST") {
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

  if (isResp) {
    return handleResponses(payload, res);
  }

  payload.model = resolveModel(payload.model);
  const wantStream = !!payload.stream;
  if (payload.tools) payload.tools = normalizeTools(payload.tools);

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