#!/usr/bin/env node
"use strict";

/*
 * Real SentinelRunner inferred from the provided successful Codex log.
 *
 * This file intentionally does not use the old Python/mock PoW shortcut. It
 * runs the public Sentinel SDK in a browser-like VM, requests the Sentinel
 * challenge, lets the SDK build the enforcement proof, then returns the exact
 * headers the Python chain needs:
 *
 *   OpenAI-Sentinel-Token:    {"p":"...","t":"...","c":"...","id":"...","flow":"..."}
 *   OpenAI-Sentinel-SO-Token: {"so":"...","c":"...","id":"...","flow":"..."}
 *
 * Nguồn SDK (theo thứ tự ưu tiên, không cần mạng nếu có bản local):
 *   1. file sdk_<version>.js đặt cạnh script (vd sdk_20260810913b.js)
 *   2. cache .cache/sentinel-<version>-*.js
 *   3. tải mạng từ OpenAI (bootstrap /sentinel/<version>/sdk.js)
 * Mặc định version = "20260810913b" (khớp SDK browser thật 2026-09-03 đang
 * phục vụ); ghi đè bằng env CODEX_SENTINEL_SDK_VERSION nếu cần.
 *
 * Input is one JSON object on stdin. Important fields:
 *   flow, persona, device_id, proxy, fingerprint, timeout_ms, sentinel_sdk_url
 * Debug action "requirements_only": chạy SDK tới bước requirements rồi dừng
 * (không gọi mạng) — dùng để kiểm chứng patch/VM offline.
 */

const crypto = require("crypto");
const childProcess = require("child_process");
const fs = require("fs");
const http = require("http");
const https = require("https");
const path = require("path");
const tls = require("tls");
const vm = require("vm");

const SENTINEL_VERSION = process.env.CODEX_SENTINEL_SDK_VERSION || "20260810913b";
const SENTINEL_REQ_URL = "https://sentinel.openai.com/backend-api/sentinel/req";
const SENTINEL_REFERER =
  `https://sentinel.openai.com/backend-api/sentinel/frame.html?sv=${SENTINEL_VERSION}`;

/*
 * Sentinel endpoint depends on the flow (from real browser capture):
 *   chatgpt.com:  chatgpt_checkout, checkout_session_approval, conversation
 *   sentinel.openai.com: auth flows (authorize_continue, email_otp_validate,
 *                        oauth_create_account, username_password_create, ...)
 */
function sentinelEndpoint(flow) {
  const f = String(flow || "").trim().toLowerCase();
  const chatgptHostFlows = new Set([
    "chatgpt_checkout",
    "chatgpt",
    "checkout_session_approval",
    "conversation",
    "checkout",
  ]);
  const host = chatgptHostFlows.has(f) ? "chatgpt.com" : "sentinel.openai.com";
  return {
    host,
    reqUrl: `https://${host}/backend-api/sentinel/req`,
    frameUrl: `https://${host}/backend-api/sentinel/frame.html?sv=${SENTINEL_VERSION}`,
    origin: `https://${host}`,
  };
}
const DEFAULT_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";

const SDK_GLOBAL_PATCH = "var SentinelSDK=";
const SDK_GLOBAL_REPLACEMENT = "globalThis.SentinelSDK=";
const INSTANCE_PATCH = "var P=new _;";
const INSTANCE_REPLACEMENT = "var P=new _;globalThis.__debugP=P;";
const EXPOSE_PATCH =
  "return o?r?.[n(63)]?ce({so:o,c:r[n(63)]},t):o:null},t.token=ye,t}({});";
const EXPOSE_REPLACEMENT =
  "return o?r?.[n(63)]?ce({so:o,c:r[n(63)]},t):o:null},t.token=ye,t.__debug_n=_n,t.__debug_bindProof=D,t}({});";

function clean(value) {
  return String(value == null ? "" : value).trim();
}

function readStdin() {
  return new Promise((resolve) => {
    let data = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
      data += chunk;
    });
    process.stdin.on("end", () => resolve(data));
  });
}

function jsonOut(obj) {
  process.stdout.write(JSON.stringify(obj));
}

function fail(error, extra = {}) {
  jsonOut({
    ok: false,
    mode: "real",
    version: "real-sentinel-runner-sdk-v1",
    token_generated: false,
    error: String(error || "failed"),
    ...extra,
  });
}

function uuid4() {
  if (crypto.randomUUID) return crypto.randomUUID();
  const bytes = crypto.randomBytes(16);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function normalizeProxy(proxyText) {
  let value = clean(proxyText);
  if (!value || /^(none|direct|false|0)$/i.test(value)) return "";
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(value)) value = `http://${value}`;
  return value;
}

function timeoutNumber(input) {
  const n = Number(input && (input.timeout_ms || input.timeout || process.env.CODEX_NODE_TIMEOUT_MS));
  return Number.isFinite(n) && n >= 1000 ? n : 45000;
}

function httpRequestDirect({ method, urlText, body = "", headers = {}, timeoutMs }) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlText);
    const client = url.protocol === "https:" ? https : http;
    const req = client.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port || (url.protocol === "https:" ? 443 : 80),
        path: url.pathname + url.search,
        method,
        timeout: timeoutMs,
        rejectUnauthorized: true,
        headers,
      },
      (res) => {
        let text = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          text += chunk;
        });
        res.on("end", () => resolve({ status: res.statusCode || 0, headers: res.headers || {}, text }));
      }
    );
    req.on("timeout", () => req.destroy(new Error("request_timeout")));
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

function httpRequestViaHttpProxy({ proxyText, method, urlText, body = "", headers = {}, timeoutMs }) {
  return new Promise((resolve, reject) => {
    const proxy = new URL(proxyText);
    if (proxy.protocol !== "http:") {
      reject(new Error(`unsupported_node_proxy_protocol:${proxy.protocol}`));
      return;
    }
    const target = new URL(urlText);
    if (target.protocol !== "https:") {
      reject(new Error(`unsupported_target_protocol:${target.protocol}`));
      return;
    }
    const targetPort = target.port || 443;
    const connectHeaders = {};
    if (proxy.username || proxy.password) {
      connectHeaders["Proxy-Authorization"] =
        "Basic " + Buffer.from(`${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`).toString("base64");
    }
    const connect = http.request({
      hostname: proxy.hostname,
      port: proxy.port || 80,
      method: "CONNECT",
      path: `${target.hostname}:${targetPort}`,
      timeout: timeoutMs,
      headers: connectHeaders,
    });
    connect.on("connect", (res, socket) => {
      if ((res.statusCode || 0) < 200 || (res.statusCode || 0) >= 300) {
        socket.destroy();
        reject(new Error(`proxy_connect_failed:${res.statusCode}`));
        return;
      }
      const tlsSocket = tls.connect(
        {
          socket,
          servername: target.hostname,
          rejectUnauthorized: true,
        },
        () => {
          const req = https.request(
            {
              host: target.hostname,
              servername: target.hostname,
              path: target.pathname + target.search,
              method,
              agent: false,
              createConnection: () => tlsSocket,
              rejectUnauthorized: true,
              timeout: timeoutMs,
              headers,
            },
            (resp) => {
              let text = "";
              resp.setEncoding("utf8");
              resp.on("data", (chunk) => {
                text += chunk;
              });
              resp.on("end", () => resolve({ status: resp.statusCode || 0, headers: resp.headers || {}, text }));
            }
          );
          req.on("timeout", () => req.destroy(new Error("request_timeout")));
          req.on("error", reject);
          if (body) req.write(body);
          req.end();
        }
      );
      tlsSocket.setTimeout(timeoutMs, () => tlsSocket.destroy(new Error("tls_connect_timeout")));
      tlsSocket.on("error", reject);
    });
    connect.on("timeout", () => connect.destroy(new Error("proxy_connect_timeout")));
    connect.on("error", reject);
    connect.end();
  });
}

function httpRequest(opts) {
  const proxy = normalizeProxy(opts.proxy || "");
  if (proxy) return httpRequestViaHttpProxy({ ...opts, proxyText: proxy });
  return httpRequestDirect(opts);
}

async function fetchText({ url, headers, timeoutMs, proxy }) {
  const result = await httpRequest({
    method: "GET",
    urlText: url,
    headers,
    timeoutMs,
    proxy,
  });
  if (result.status < 200 || result.status >= 300) {
    throw new Error(`http_${result.status}:${url}:${String(result.text || "").slice(0, 220)}`);
  }
  return result.text || "";
}

async function fetchJson({ url, body, headers, timeoutMs, proxy }) {
  let result;
  try {
    result = await httpRequest({
      method: "POST",
      urlText: url,
      body,
      headers: {
        ...headers,
        "Content-Length": Buffer.byteLength(body),
      },
      timeoutMs,
      proxy,
    });
    if (result.status < 200 || result.status >= 300) {
      throw new Error(`http_${result.status}:${url}:${String(result.text || "").slice(0, 220)}`);
    }
  } catch (err) {
    if (!pythonFallbackEnabled()) {
      throw new Error(`node_json_post_failed:${err && err.message ? err.message : err}`);
    }
    const text = postJsonViaPython({ url, body, headers, proxy, timeoutMs });
    result = { status: 200, text };
  }
  try {
    return JSON.parse(result.text || "{}");
  } catch {
    throw new Error(`invalid_json:${String(result.text || "").slice(0, 220)}`);
  }
}

function postJsonViaPython({ url, body, headers, proxy, timeoutMs }) {
  const candidates = pythonCandidates();
  if (!candidates.length) {
    throw new Error("python_fallback_disabled");
  }
  const script = String.raw`
import json, sys
url = sys.argv[1]
body = sys.argv[2]
proxy = sys.argv[3] if len(sys.argv) > 3 else ""
timeout = float(sys.argv[4]) / 1000.0 if len(sys.argv) > 4 else 45.0
headers = json.loads(sys.argv[5] if len(sys.argv) > 5 else "{}")
def out(text):
    sys.stdout.buffer.write(str(text or "").encode("utf-8"))
    raise SystemExit(0)
try:
    from curl_cffi import requests as curl_requests
    kwargs = {"data": body, "headers": headers, "timeout": timeout, "impersonate": "chrome"}
    if proxy:
        kwargs["proxies"] = {"http": proxy, "https": proxy}
    r = curl_requests.post(url, **kwargs)
    if 200 <= int(r.status_code) < 300:
        out(r.text)
except Exception:
    pass
try:
    import requests
    kwargs = {"data": body.encode("utf-8"), "headers": headers, "timeout": timeout, "verify": False}
    if proxy:
        kwargs["proxies"] = {"http": proxy, "https": proxy}
    r = requests.post(url, **kwargs)
    if 200 <= int(r.status_code) < 300:
        out(r.text)
    raise RuntimeError("HTTP %s: %s" % (getattr(r, "status_code", "?"), (getattr(r, "text", "") or "")[:160]))
except Exception as exc:
    sys.stderr.write(str(exc))
    raise SystemExit(1)
`;
  let lastError = "";
  for (const py of candidates) {
    const prefix = pythonArgsPrefix();
    const pyArgs = [
      ...prefix,
      "-c",
      script,
      url,
      body,
      proxy || "",
      String(timeoutMs || 45000),
      JSON.stringify(headers || {}),
    ];
    const proc = childProcess.spawnSync(
      py,
      pyArgs,
      {
        encoding: "utf8",
        maxBuffer: 10 * 1024 * 1024,
        windowsHide: true,
      }
    );
    if (proc.status === 0 && clean(proc.stdout)) {
      return proc.stdout;
    }
    lastError = clean(proc.stderr || proc.stdout || proc.error || `exit ${proc.status}`);
  }
  throw new Error(lastError || "python_json_post_failed");
}

function cachePathForSdk(url) {
  const key = Buffer.from(url).toString("base64url").slice(0, 80);
  return path.join(__dirname, ".cache", `sentinel-${SENTINEL_VERSION}-${key}.js`);
}

function cachePathForSdkInDir(dir, url) {
  const key = Buffer.from(url).toString("base64url").slice(0, 80);
  return path.join(dir, `sentinel-${SENTINEL_VERSION}-${key}.js`);
}

function sentinelCacheDirs() {
  const dirs = [];
  const explicit = clean(process.env.CODEX_SENTINEL_SDK_CACHE_DIR);
  if (explicit) dirs.push(explicit);
  dirs.push(path.join(__dirname, ".cache"));
  return [...new Set(dirs.map((item) => path.resolve(item)))];
}

function pythonFallbackEnabled() {
  return /^(1|true|yes|on)$/i.test(clean(process.env.CODEX_SENTINEL_ENABLE_PY_FALLBACK));
}

function pythonArgsPrefix() {
  const rawJson = clean(process.env.CODEX_SENTINEL_PY_ARGS_PREFIX_JSON);
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      if (Array.isArray(parsed)) return parsed.map(clean).filter(Boolean);
    } catch {
      // Fall back to the simple whitespace form below.
    }
  }
  const raw = clean(process.env.CODEX_SENTINEL_PY_ARGS_PREFIX);
  return raw ? raw.split(/\s+/).filter(Boolean) : [];
}

function pythonCandidates() {
  if (!pythonFallbackEnabled()) {
    return [];
  }
  const candidates = [];
  if (process.env.PYTHON) candidates.push(process.env.PYTHON);
  if (process.env.CODEX_PYTHON) candidates.push(process.env.CODEX_PYTHON);
  candidates.push("python", "py");
  return [...new Set(candidates.map(clean).filter(Boolean))];
}

function validateSdkSource(source) {
  const text = String(source || "");
  if (!text || !text.includes("Sentinel")) {
    throw new Error("Sentinel SDK cache invalid: missing Sentinel marker");
  }
  patchSdk(text);
  return text;
}

function cachedSdkFiles(urls) {
  const files = [];
  for (const dir of sentinelCacheDirs()) {
    for (const url of urls) {
      files.push(cachePathForSdkInDir(dir, url));
    }
    try {
      if (fs.existsSync(dir)) {
        for (const name of fs.readdirSync(dir)) {
          const lower = String(name || "").toLowerCase();
          const prefix = `sentinel-${String(SENTINEL_VERSION).toLowerCase()}-`;
          if (lower.startsWith(prefix) && lower.endsWith(".js")) {
            files.push(path.join(dir, name));
          }
        }
      }
    } catch {
      // Cache search is best-effort.
    }
  }
  return [...new Set(files)];
}

function readCachedSdk(urls) {
  const errors = [];
  for (const file of cachedSdkFiles(urls)) {
    try {
      if (!fs.existsSync(file)) continue;
      const cached = fs.readFileSync(file, "utf8");
      validateSdkSource(cached);
      return { source: cached, url: `file://${file}`, cache: "hit", path: file };
    } catch (err) {
      errors.push(`cache:${file}:${err.message}`);
    }
  }
  return { errors };
}

/*
 * SDK local đặt cạnh script dạng "sdk_<version>.js" (vd: sdk_20260810913b.js).
 * Có file này thì KHÔNG cần tải mạng / không phụ thuộc proxy — chạy offline.
 */
function readLocalSdk() {
  const wanted = String(SENTINEL_VERSION).toLowerCase();
  const names = [];
  const plain = path.join(__dirname, `sdk_${SENTINEL_VERSION}.js`);
  if (!fs.existsSync(plain)) {
    try {
      for (const name of fs.readdirSync(__dirname)) {
        const m = /^sdk_([0-9a-f]+)\.js$/i.exec(name);
        if (m && m[1].toLowerCase() === wanted) names.push(name);
      }
    } catch {
      // best effort
    }
  }
  names.unshift(`sdk_${SENTINEL_VERSION}.js`);
  for (const name of [...new Set(names)]) {
    const file = path.join(__dirname, name);
    try {
      if (!fs.existsSync(file)) continue;
      const source = fs.readFileSync(file, "utf8");
      validateSdkSource(source);
      return { source, url: `file://${file}`, cache: "local", path: file };
    } catch (err) {
      // thử nguồn tiếp theo
    }
  }
  return {};
}

function sdkUrls(input) {
  const explicit = clean(input.sentinel_sdk_url || input.sdk_url || process.env.CODEX_SENTINEL_SDK_URL);
  const ep = sentinelEndpoint(clean(input.flow));
  // /backend-api/sentinel/sdk.js is only a 900-byte bootstrap that cannot be
  // patched; the real SDK lives at the versioned path on the same host.
  const isBootstrap = /\/backend-api\/sentinel\/sdk\.js$/.test(explicit);
  const urls = [];
  if (isBootstrap) {
    const wrapHost = explicit.startsWith("https://chatgpt.com")
      ? "chatgpt.com"
      : "sentinel.openai.com";
    urls.push(`https://${wrapHost}/sentinel/${SENTINEL_VERSION}/sdk.js`);
  }
  if (explicit) urls.push(explicit);
  // chatgpt-hosted flows load the SDK from chatgpt.com in real browsers.
  if (ep.host === "chatgpt.com") {
    urls.push(`https://chatgpt.com/sentinel/${SENTINEL_VERSION}/sdk.js`);
  }
  urls.push(`https://sentinel.openai.com/sentinel/${SENTINEL_VERSION}/sdk.js`);
  if (!isBootstrap) {
    urls.push("https://chatgpt.com/backend-api/sentinel/sdk.js");
    urls.push("https://sentinel.openai.com/backend-api/sentinel/sdk.js");
  }
  return [...new Set(urls)];
}

async function getSdkSource(input) {
  const timeoutMs = timeoutNumber(input);
  const proxy = normalizeProxy(input.proxy || input.proxy_url || process.env.HTTPS_PROXY || process.env.HTTP_PROXY);
  const errors = [];
  const urls = sdkUrls(input);
  const local = readLocalSdk();
  if (local.source) return local;
  const cached = readCachedSdk(urls);
  if (cached.source) return cached;
  if (Array.isArray(cached.errors)) errors.push(...cached.errors);
  if (/^(1|true|yes|on)$/i.test(clean(input.cache_only || process.env.CODEX_SENTINEL_CACHE_ONLY))) {
    throw new Error(`sentinel_sdk_unavailable:cache_only:${errors.join(" | ").slice(0, 800)}`);
  }
  for (const url of urls) {
    const cacheFile = cachePathForSdk(url);
    try {
      if (fs.existsSync(cacheFile)) {
        const cached = fs.readFileSync(cacheFile, "utf8");
        validateSdkSource(cached);
        return { source: cached, url, cache: "hit", path: cacheFile };
      }
    } catch (err) {
      errors.push(`cache:${url}:${err.message}`);
    }
    try {
      const source = await fetchText({
        url,
        proxy,
        timeoutMs,
        headers: {
          Accept: "*/*",
          Referer: "https://auth.openai.com/",
          "User-Agent": DEFAULT_UA,
        },
      });
      validateSdkSource(source);
      try {
        fs.mkdirSync(path.dirname(cacheFile), { recursive: true });
        fs.writeFileSync(cacheFile, source);
      } catch {
        // Cache is only an optimization.
      }
      return { source, url, cache: "miss" };
    } catch (err) {
      errors.push(`fetch:${url}:${err.message}`);
    }
    try {
      const source = fetchSdkViaPython({ url, proxy, timeoutMs });
      validateSdkSource(source);
      try {
        fs.mkdirSync(path.dirname(cacheFile), { recursive: true });
        fs.writeFileSync(cacheFile, source);
      } catch {
        // Cache is only an optimization.
      }
      return { source, url, cache: "python-fetch" };
    } catch (err) {
      errors.push(`python:${url}:${err.message}`);
    }
  }
  throw new Error(`sentinel_sdk_unavailable:${errors.join(" | ").slice(0, 800)}`);
}

function fetchSdkViaPython({ url, proxy, timeoutMs }) {
  const candidates = pythonCandidates();
  if (!candidates.length) {
    throw new Error("python_fallback_disabled");
  }
  const script = String.raw`
import os, sys
url = sys.argv[1]
proxy = sys.argv[2] if len(sys.argv) > 2 else ""
timeout = float(sys.argv[3]) / 1000.0 if len(sys.argv) > 3 else 45.0
headers = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://auth.openai.com/",
    "Sec-Fetch-Dest": "script",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
}
def out(text):
    sys.stdout.buffer.write(str(text or "").encode("utf-8"))
    raise SystemExit(0)
try:
    from curl_cffi import requests as curl_requests
    kwargs = {"headers": headers, "timeout": timeout, "impersonate": "chrome"}
    if proxy:
        kwargs["proxies"] = {"http": proxy, "https": proxy}
    r = curl_requests.get(url, **kwargs)
    if 200 <= int(r.status_code) < 300 and "SentinelSDK" in (r.text or ""):
        out(r.text)
except Exception:
    pass
try:
    import requests
    kwargs = {"headers": headers, "timeout": timeout, "verify": False}
    if proxy:
        kwargs["proxies"] = {"http": proxy, "https": proxy}
    r = requests.get(url, **kwargs)
    if 200 <= int(r.status_code) < 300 and "SentinelSDK" in (r.text or ""):
        out(r.text)
    raise RuntimeError("HTTP %s: %s" % (getattr(r, "status_code", "?"), (getattr(r, "text", "") or "")[:160]))
except Exception as exc:
    sys.stderr.write(str(exc))
    raise SystemExit(1)
`;
  let lastError = "";
  for (const py of candidates) {
    const proc = childProcess.spawnSync(py, ["-c", script, url, proxy || "", String(timeoutMs || 45000)], {
      encoding: "utf8",
      maxBuffer: 5 * 1024 * 1024,
      windowsHide: true,
    });
    if (proc.status === 0 && clean(proc.stdout).includes("SentinelSDK")) {
      return proc.stdout;
    }
    lastError = clean(proc.stderr || proc.stdout || proc.error || `exit ${proc.status}`);
  }
  throw new Error(lastError || "python_sdk_fetch_failed");
}

/*
 * Các marker patch phụ thuộc phiên bản SDK (obfuscation khác nhau giữa các bản):
 *   - 20260219f9f6: instance "var P=new _;" / export tail "...t.token=ye,t}({});"
 *     với hàm bindProof "D" và generator "fn_ _n".
 *   - 20260810913b (bản browser thật 2026-09-03 đang phục vụ): instance
 *     "var E=new O;" / export tail "...t.token=je,t}({});" — generator tương
 *     đương "_n" bị đổi tên thành "Rn", "D" (WeakMap-setter) giữ nguyên.
 */
function sdkPatchMarkers(version) {
  if (String(version || "") === "20260810913b") {
    return {
      instanceFrom: "var E=new O;",
      instanceTo: "var E=new O;globalThis.__debugP=E;",
      exposeFrom: "t.token=je,t}({});",
      exposeTo: "t.token=je,t.__debug_n=Rn,t.__debug_bindProof=D,t}({});",
    };
  }
  return {
    instanceFrom: INSTANCE_PATCH,
    instanceTo: INSTANCE_REPLACEMENT,
    exposeFrom: EXPOSE_PATCH,
    exposeTo: EXPOSE_REPLACEMENT,
  };
}

function patchSdk(source, version) {
  const markers = sdkPatchMarkers(version || SENTINEL_VERSION);
  let sdk = String(source || "");
  sdk = sdk.replace(SDK_GLOBAL_PATCH, SDK_GLOBAL_REPLACEMENT);
  sdk = sdk.replace(markers.instanceFrom, markers.instanceTo);
  sdk = sdk.replace(markers.exposeFrom, markers.exposeTo);
  if (!sdk.includes("globalThis.__debugP")) {
    throw new Error(`Sentinel SDK patch failed: __debugP not injected (v${version || SENTINEL_VERSION})`);
  }
  if (!sdk.includes("__debug_bindProof")) {
    throw new Error(`Sentinel SDK patch failed: bindProof not exposed (v${version || SENTINEL_VERSION})`);
  }
  return sdk;
}

function fingerprint(input) {
  const fp = input && typeof input.fingerprint === "object" ? input.fingerprint : {};
  const language = clean(fp.language) || "zh-CN";
  const languages = Array.isArray(fp.languages) && fp.languages.length
    ? fp.languages.map(clean).filter(Boolean)
    : ["zh-CN", "zh-Hans-CN"];
  return {
    user_agent: clean(fp.user_agent) || DEFAULT_UA,
    language,
    languages,
    screen_width: Number(fp.screen_width || fp.width || 2400) || 2400,
    screen_height: Number(fp.screen_height || fp.height || 1080) || 1080,
    hardware_concurrency: Number(fp.hardware_concurrency || 16) || 16,
    device_memory: Number(fp.device_memory || 8) || 8,
    platform: clean(fp.platform) || "Win32",
    timezone: clean(fp.timezone) || "Asia/Shanghai",
  };
}

function buildSandbox(payload, sdkSource) {
  return {
    __payload: payload,
    __sdkSource: sdkSource,
    console: { log() {}, warn() {}, error() {}, info() {}, debug() {} },
    Math,
    Date,
    JSON,
    Promise,
    Map,
    WeakMap,
    Set,
    Array,
    Object,
    String,
    Number,
    Boolean,
    RegExp,
    Error,
    TypeError,
    Uint8Array,
    Uint16Array,
    Uint32Array,
    Int8Array,
    Int16Array,
    Int32Array,
    Float32Array,
    Float64Array,
    ArrayBuffer,
    DataView,
    TextEncoder,
    TextDecoder,
    URL,
    URLSearchParams,
    Buffer,
    setTimeout: (cb, delay = 0, ...args) => global.setTimeout(cb, Number(delay) || 0, ...args),
    clearTimeout: (id) => global.clearTimeout(id),
    setInterval: (cb, delay = 0, ...args) => global.setInterval(cb, Number(delay) || 0, ...args),
    clearInterval: (id) => global.clearInterval(id),
    queueMicrotask: (cb) => {
      if (typeof cb === "function") Promise.resolve().then(cb);
    },
  };
}

function runtimeSource() {
  return String.raw`
function createStorage() {
  const map = new Map();
  return {
    get length() { return map.size; },
    clear() { map.clear(); },
    getItem(key) { return map.has(String(key)) ? map.get(String(key)) : null; },
    key(index) { return Array.from(map.keys())[Number(index)] || null; },
    setItem(key, value) { map.set(String(key), String(value)); },
    removeItem(key) { map.delete(String(key)); },
  };
}

function createElement(tagName) {
  const tag = String(tagName || "div").toLowerCase();
  return {
    nodeType: 1,
    tagName: tag.toUpperCase(),
    nodeName: tag.toUpperCase(),
    style: {},
    children: [],
    childNodes: [],
    src: "",
    href: "",
    id: "",
    className: "",
    contentWindow: { postMessage() {} },
    appendChild(child) { this.children.push(child); this.childNodes.push(child); return child; },
    removeChild(child) {
      this.children = this.children.filter((x) => x !== child);
      this.childNodes = this.childNodes.filter((x) => x !== child);
      return child;
    },
    setAttribute(name, value) { this[String(name)] = String(value); },
    getAttribute(name) { return this[String(name)] || null; },
    hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this, String(name)); },
    addEventListener(event, cb) { if (event === "load" && typeof cb === "function") cb(); },
    removeEventListener() {},
    dispatchEvent() { return true; },
    getBoundingClientRect() {
      return { x: 0, y: 0, width: 0, height: 0, top: 0, left: 0, right: 0, bottom: 0 };
    },
  };
}

function installRuntime(payload) {
  const width = Number(payload.screen_width || 2400);
  const height = Number(payload.screen_height || 1080);
  const frameUrl = String(payload.__frame_url || "https://sentinel.openai.com/backend-api/sentinel/frame.html");
  const sdkSrc = String(payload.__sdk_src || "https://sentinel.openai.com/backend-api/sentinel/sdk.js");
  let frameOrigin = "";
  try {
    frameOrigin = new URL(frameUrl).origin;
  } catch {
    frameOrigin = "https://sentinel.openai.com";
  }
  const screen = {
    width,
    height,
    availWidth: width,
    availHeight: height - 40,
    colorDepth: 24,
    pixelDepth: 24,
  };
  const scripts = [];
  const documentElement = createElement("html");
  documentElement.clientWidth = width;
  documentElement.clientHeight = height;

  const document = {
    readyState: "complete",
    hidden: false,
    visibilityState: "visible",
    referrer: payload.__referrer || (frameOrigin === "https://chatgpt.com" ? "https://chatgpt.com/" : "https://auth.openai.com/"),
    URL: frameUrl,
    documentURI: frameUrl,
    baseURI: frameUrl,
    cookie: "oai-did=" + encodeURIComponent(payload.device_id || ""),
    scripts,
    currentScript: { src: sdkSrc, getAttribute() { return null; } },
    documentElement,
    body: createElement("body"),
    head: createElement("head"),
    createElement(tag) {
      const el = createElement(tag);
      if (String(tag).toLowerCase() === "script") scripts.push(el);
      return el;
    },
    createElementNS(_ns, tag) { return this.createElement(tag); },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    getElementById() { return null; },
    getElementsByTagName() { return []; },
    addEventListener() {},
    removeEventListener() {},
    dispatchEvent() { return true; },
  };

  const performance = {
    now: () => Number(payload.performance_now || 12345.67),
    timeOrigin: Number(payload.time_origin || Date.now() - 12345),
    memory: { jsHeapSizeLimit: Number(payload.js_heap_size_limit || 4294705152) },
    getEntriesByType() { return []; },
    mark() {},
    measure() {},
  };

  globalThis.window = globalThis;
  globalThis.self = globalThis;
  globalThis.top = globalThis;
  globalThis.parent = globalThis;
  globalThis.frames = globalThis;
  globalThis.document = document;
  globalThis.navigator = {
    userAgent: String(payload.user_agent || "Mozilla/5.0"),
    appVersion: String(payload.user_agent || "Mozilla/5.0"),
    appName: "Netscape",
    language: String(payload.language || "zh-CN"),
    languages: Array.isArray(payload.languages) ? payload.languages : ["zh-CN", "zh-Hans-CN"],
    hardwareConcurrency: Number(payload.hardware_concurrency || 16),
    deviceMemory: Number(payload.device_memory || 8),
    platform: String(payload.platform || "Win32"),
    product: "Gecko",
    productSub: "20030107",
    vendor: "Google Inc.",
    webdriver: false,
    cookieEnabled: true,
    onLine: true,
    maxTouchPoints: 0,
    plugins: undefined,
    mimeTypes: undefined,
    clipboard: {},
    permissions: { query: async () => ({ state: "prompt", onchange: null }) },
  };
  const frameLocation = (() => {
    try {
      const u = new URL(frameUrl);
      return {
        href: u.href,
        origin: u.origin,
        protocol: u.protocol,
        host: u.host,
        hostname: u.hostname,
        port: u.port,
        pathname: u.pathname,
        search: u.search,
        hash: u.hash,
        assign() {},
        replace() {},
        reload() {},
        toString() { return u.href; },
      };
    } catch {
      return null;
    }
  })();
  if (frameLocation) {
    globalThis.location = frameLocation;
  } else {
    globalThis.location = {
      href: frameUrl,
      origin: frameOrigin,
      protocol: frameUrl.startsWith("https:") ? "https:" : "http:",
      host: frameUrl,
      hostname: frameUrl,
      pathname: "/",
      search: "",
      hash: "",
      assign() {},
      replace() {},
      reload() {},
      toString() { return this.href; },
    };
  }
  globalThis.screen = screen;
  globalThis.innerWidth = width;
  globalThis.innerHeight = height;
  globalThis.outerWidth = width;
  globalThis.outerHeight = height;
  globalThis.devicePixelRatio = 1;
  globalThis.performance = performance;
  globalThis.localStorage = createStorage();
  globalThis.sessionStorage = createStorage();
  globalThis.__sentinel_init_pending = [];
  globalThis.__sentinel_token_pending = [];
  globalThis.requestIdleCallback = (cb) => {
    if (typeof cb === "function") cb({ didTimeout: false, timeRemaining: () => 50 });
    return 1;
  };
  globalThis.cancelIdleCallback = () => {};
  globalThis.requestAnimationFrame = (cb) => {
    if (typeof cb === "function") cb(performance.now());
    return 1;
  };
  globalThis.cancelAnimationFrame = () => {};
  globalThis.addEventListener = () => {};
  globalThis.removeEventListener = () => {};
  globalThis.dispatchEvent = () => true;
  globalThis.postMessage = () => {};
  globalThis.atob = (input) => Buffer.from(String(input || ""), "base64").toString("binary");
  globalThis.btoa = (input) => Buffer.from(String(input || ""), "binary").toString("base64");
  globalThis.URL = URL;
  globalThis.URLSearchParams = URLSearchParams;
  globalThis.Event = class Event { constructor(type) { this.type = type; } };
  globalThis.CustomEvent = class CustomEvent extends globalThis.Event {
    constructor(type, init) {
      super(type);
      this.detail = init && Object.prototype.hasOwnProperty.call(init, "detail") ? init.detail : null;
    }
  };
  globalThis.MessageChannel = class MessageChannel {
    constructor() {
      this.port1 = { postMessage() {}, addEventListener() {}, removeEventListener() {}, start() {}, close() {} };
      this.port2 = { postMessage() {}, addEventListener() {}, removeEventListener() {}, start() {}, close() {} };
    }
  };
  globalThis.matchMedia = (query) => ({
    media: String(query || ""),
    matches: false,
    onchange: null,
    addListener() {},
    removeListener() {},
    addEventListener() {},
    removeEventListener() {},
    dispatchEvent() { return false; },
  });
  globalThis.getComputedStyle = () => ({ getPropertyValue() { return ""; } });
  globalThis.history = { length: 1, state: null, back() {}, forward() {}, go() {}, pushState() {}, replaceState() {} };
  globalThis.chrome = { runtime: {}, app: {}, csi() {}, loadTimes() {} };
  globalThis.CSS = { supports() { return true; } };
  globalThis.indexedDB = {
    open() { return { onerror: null, onsuccess: null, onupgradeneeded: null, result: {}, error: null }; },
    deleteDatabase() { return {}; },
  };
  globalThis.fetch = async () => { throw new Error("fetch should not be called inside Sentinel VM"); };
  globalThis.crypto = {
    randomUUID: () => String(payload.session_id || "00000000-0000-4000-8000-000000000000"),
    getRandomValues(arr) {
      for (let i = 0; i < arr.length; i += 1) arr[i] = Math.floor(Math.random() * 256);
      return arr;
    },
  };
}

async function runSentinelAction() {
  const payload = globalThis.__payload || {};
  installRuntime(payload);
  eval(String(globalThis.__sdkSource || ""));

  if (payload.action === "requirements") {
    const requestP = await globalThis.__debugP.getRequirementsToken();
    return { request_p: requestP };
  }

  if (payload.action === "solve") {
    const challenge = payload.challenge || {};
    const requestP = String(payload.request_p || "").trim();
    const finalP = await globalThis.__debugP.getEnforcementToken(challenge);
    globalThis.SentinelSDK.__debug_bindProof(challenge, requestP);
    const dx = challenge && challenge.turnstile ? challenge.turnstile.dx : null;
    const tValue = dx ? await globalThis.SentinelSDK.__debug_n(challenge, dx) : null;
    return { final_p: finalP, t: tValue, raw_so: challenge.so || challenge.t || "" };
  }

  throw new Error("unsupported Sentinel action: " + payload.action);
}
`;
}

async function runSdkAction(sdkSource, payload) {
  const patchedSdk = patchSdk(sdkSource);
  const sandbox = buildSandbox(payload, patchedSdk);
  const context = vm.createContext(sandbox);
  const script = new vm.Script(`${runtimeSource()}\nrunSentinelAction();`);
  const result = await script.runInContext(context, { timeout: 30000 });
  if (!result || typeof result !== "object") {
    throw new Error("Sentinel SDK returned empty result");
  }
  return result;
}

function decodePDiagnostic(p) {
  const token = clean(p);
  const prefix = token.startsWith("gAAAAAB") ? "gAAAAAB" : token.startsWith("gAAAAAC") ? "gAAAAAC" : "";
  if (!prefix) return { decoded: false, len: token.length };
  const b64 = token.slice(prefix.length).split("~")[0];
  try {
    const decoded = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
    const out = { decoded: true, len: token.length };
    if (Array.isArray(decoded)) {
      for (const idx of [0, 2, 3, 5, 7, 8, 9, 10, 11, 12, 14, 16, 24]) {
        if (idx in decoded) out[String(idx)] = decoded[idx];
      }
    }
    return out;
  } catch {
    return { decoded: false, len: token.length };
  }
}

async function fetchChallenge({ input, flow, deviceId, requestP }) {
  const ep = sentinelEndpoint(flow);
  const cookie = clean(input.cookie || input.cookies || "");
  const headers = {
    Accept: "*/*",
    "Accept-Language": ep.host === "chatgpt.com"
      ? "en-US,en;q=0.9"
      : "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Content-Type": "text/plain;charset=UTF-8",
    Origin: ep.origin,
    Referer: ep.frameUrl,
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": ep.host === "chatgpt.com" ? "same-origin" : "same-origin",
    "User-Agent": DEFAULT_UA,
  };
  if (cookie) headers.Cookie = cookie;
  return fetchJson({
    url: ep.reqUrl,
    body: JSON.stringify({ p: requestP, id: deviceId, flow }),
    proxy: normalizeProxy(input.proxy || input.proxy_url || process.env.HTTPS_PROXY || process.env.HTTP_PROXY),
    timeoutMs: timeoutNumber(input),
    headers,
  });
}

async function generateSentinel(input) {
  const flow = clean(input.flow) || "authorize_continue";
  const deviceId = clean(input.device_id) || uuid4();
  const fp = fingerprint(input);
  const sdk = await getSdkSource(input);
  if (clean(input.action).toLowerCase() === "cache_check" || flow === "__cache_check__") {
    return {
      ok: true,
      mode: "real",
      version: "real-sentinel-runner-sdk-v1",
      flow,
      token_generated: false,
      cache_check: true,
      diagnostics: {
        sdk_url: sdk.url,
        sdk_cache: sdk.cache,
        sdk_path: sdk.path || "",
      },
    };
  }
  const basePayload = {
    flow,
    persona: clean(input.persona) || "chatgpt-noauth",
    device_id: deviceId,
    session_id: clean(input.session_id) || uuid4(),
    ...fp,
  };
  const ep = sentinelEndpoint(flow);
  // Mirror the real browser frame for this flow so the SDK fingerprints the
  // same origin/script src that a genuine checkout session would present.
  basePayload.__frame_url = ep.frameUrl;
  basePayload.__sdk_src = sdk.url.startsWith("http") ? sdk.url : (input.sentinel_sdk_url || `https://${ep.host}/backend-api/sentinel/sdk.js`);

  const requirements = await runSdkAction(sdk.source, {
    ...basePayload,
    action: "requirements",
  });
  const requestP = clean(requirements.request_p);
  if (!requestP) throw new Error("empty_requirements_token");

  // Chẩn đoán (offline): chỉ chạy SDK tới bước requirements rồi dừng, không gọi mạng.
  if (clean(input.action).toLowerCase() === "requirements_only") {
    return {
      ok: true,
      mode: "real",
      version: "real-sentinel-runner-sdk-v1",
      flow,
      persona: clean(input.persona) || "",
      token_generated: false,
      action: "requirements_only",
      request_p: requestP,
      diagnostics: {
        sdk_url: sdk.url,
        sdk_cache: sdk.cache,
        sdk_path: sdk.path || "",
        sdk_version: SENTINEL_VERSION,
        request_p_diagnostic: decodePDiagnostic(requestP),
      },
    };
  }

  const challenge = await fetchChallenge({ input, flow, deviceId, requestP });
  const c = clean(challenge.token || challenge.c);
  if (!c) throw new Error("sentinel_empty_token");

  const solved = await runSdkAction(sdk.source, {
    ...basePayload,
    action: "solve",
    request_p: requestP,
    challenge,
  });
  const p = clean(solved.final_p || solved.p);
  const t = clean(solved.t || solved.raw_so || challenge.so || challenge.t);
  if (!p) throw new Error("empty_enforcement_token");
  if (!t) throw new Error("empty_so_token");

  const token = JSON.stringify({ p, t, c, id: deviceId, flow });
  const soToken = JSON.stringify({ so: t, c, id: deviceId, flow });
  return {
    ok: true,
    mode: "real",
    version: "real-sentinel-runner-sdk-v1",
    flow,
    persona: clean(input.persona) || "",
    token_generated: true,
    token,
    so_token: soToken,
    diagnostics: {
      sdk_url: sdk.url,
      sdk_cache: sdk.cache,
      has_turnstile: Boolean(challenge.turnstile),
      has_so: Boolean(t),
      pow_required: Boolean((challenge.proofofwork || challenge.pow || {}).required),
      fields: {
        p: p.length,
        t: t.length,
        c: c.length,
        id: deviceId.length,
        flow: flow.length,
        so: soToken.length,
      },
      p_diagnostic: decodePDiagnostic(p),
      request_p_diagnostic: decodePDiagnostic(requestP),
    },
  };
}

async function main() {
  const raw = await readStdin();
  const input = raw ? JSON.parse(raw) : {};
  const result = await generateSentinel(input);
  jsonOut(result);
}

main().catch((err) => {
  fail(err && err.stack ? err.stack : err);
  process.exitCode = 1;
});
