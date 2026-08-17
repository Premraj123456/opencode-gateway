# OpenAI-compatible gateway for opencode free models

Exposes `deepseek-v4-flash-free` (and other opencode `/zen/v1` free models) as a standard
`/v1/chat/completions` API, routing each request through a rotating pool of HTTP proxies so the
free-tier per-IP rate limit gets spread across many exit IPs.

## How it works

- Upstream: `https://opencode.ai/zen/v1/chat/completions` (keyless, free) — requires the
  `User-Agent: opencode/<version>` header, which this gateway always sends.
- On 429/connection failure it rotates to the next proxy; if every proxy fails it can fall back to a
  direct connection (`DIRECT_FALLBACK`).
- Supports streaming (`stream: true` -> SSE passthrough) and non-streaming.

## Run locally

```powershell
$env:PROXY_LIST = "103.18.205.162:8080,104.128.228.69:8118,122.3.87.41:8080"
$env:PORT = "9090"
npm start
```

```powershell
$body = @{ model = "deepseek-v4-flash-free"; messages = @(@{ role = "user"; content = "Say hi" }) } | ConvertTo-Json -Depth 6
Invoke-RestMethod -Uri "http://127.0.0.1:9090/v1/chat/completions" -Method Post -ContentType "application/json" -Body $body
```

## Deploy on Render (free)

1. Push this folder to a GitHub repo.
2. In Render dashboard: **New -> Blueprint** -> connect the repo (uses `render.yaml`), or create a
   **New Web Service** from the repo with:
   - Build: `npm install` / Start: `npm start`
   - Health check path: `/health`
   - Free plan
3. Add env vars:
   - `PROXY_LIST` — comma/newline-separated `host:port` proxies (no scheme). Get working ones with
     the scan script (below) or paste your own.
   - `API_KEY` — optional. If set, the gateway requires `Authorization: Bearer <key>`.
   - `DIRECT_FALLBACK` — `true` to use a direct connection when all proxies fail.
   - `REFRESH_URL` — optional geonode proxy-list URL to auto-merge freshly-working proxies
     (e.g. `https://proxylist.geonode.com/api/proxy-list?page=1&limit=500&sort_by=responseTime&sort_type=asc`).
4. Use the service URL as the OpenAI-compatible base:
   `https://<your-service>.onrender.com/v1` — point your agent tool (e.g. "hermes agent") at it with
   model `deepseek-v4-flash-free`. Add a cron job (e.g. cron-job.org, every 10 min) pinging
   `/health` to stop Render free tier from cold-sleeping.

## Finding proxies that aren't rate-limited

Free datacenter proxies are heavily shared and many are already throttled to 429 by the gateway.
`scripts/scan-proxies.ps1` fetches a geonode list and tests each candidate against the gateway,
printing only the ones that respond 200:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/scan-proxies.ps1 -Limit 500
```

Paste the `OK|host:port` lines into `PROXY_LIST`.

## Env reference

| Var | Default | Purpose |
| --- | --- | --- |
| `PORT` | `9090` | Listen port |
| `MODEL` | `deepseek-v4-flash-free` | Model id served & forwarded |
| `UPSTREAM_URL` | `https://opencode.ai/zen/v1/chat/completions` | Upstream OpenAI-compatible endpoint |
| `USER_AGENT` | `opencode/1.18.9` | Header the gateway expects to see |
| `API_KEY` | empty | Require Bearer/x-api-key auth when set |
| `FORCE_MODEL` | `true` | Alias any requested model to `MODEL` upstream (client can ask for gpt-4o, gets your model) |
| `MODEL_ALIASES` | `gpt-4o,gpt-4o-mini,gpt-4.1-mini` | Extra ids advertised in `/v1/models` with `context_window` |
| `MODEL_CONTEXT_WINDOW` | `200000` | `context_window` advertised in `/v1/models` (e.g. hermes uses it for compression-model checks) |
| `PROXY_LIST` | empty | `host:port` proxy pool (comma/newline) |
| `DIRECT_FALLBACK` | `true` | Use direct connection after all proxies fail |
| `MAX_ATTEMPTS` | `6` | Max proxy attempts per request |
| `TIMEOUT_MS` | `30000` | Per-attempt timeout |
| `REFRESH_URL` | empty | geonode-style list URL to auto-refresh the pool |
| `REFRESH_SECONDS` | `1800` | Pool refresh interval |