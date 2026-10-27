import { connect as __cf_connect } from "cloudflare:sockets";
const __m = Object.create(null);
const __c = Object.create(null);

function __norm(base, spec) {
  const raw = base.split("/"); raw.pop();
  for (const part of spec.split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") raw.pop(); else raw.push(part);
  }
  return raw.join("/");
}

function __load(from, spec) {
  if (spec === "cloudflare:sockets") return { connect: __cf_connect };
  const id = spec.startsWith(".") ? __norm(from, spec) : spec;
  const resolved = __m[id] ? id : (__m[id + "/index"] ? id + "/index" : id);
  if (__c[resolved]) return __c[resolved].exports;
  const factory = __m[resolved];
  if (!factory) throw new Error("Unknown internal module: " + resolved);
  const module = { exports: {} };
  __c[resolved] = module;
  factory(module, module.exports, (s) => __load(resolved, s));
  return module.exports;
}

__m["worker"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const core_1 = __r("./core");
const errors_1 = __r("./core/errors");
const websocket_1 = __r("./transport/websocket");
const validation_1 = __r("./config/validation");
const runtime_1 = __r("./runtime/runtime");
const control_1 = __r("./control");
const wsAdapter = new websocket_1.CloudflareWebSocketAdapter();
// Soft per-isolate ceiling on concurrent proxied sessions (A8). Per-session
// queues are allocated inside accept() before authentication runs, so without
// a ceiling an unauthenticated client can force worst-case buffering per
// connection until the isolate's memory limit degrades every session.
let activeSessions = 0;
function jsonResponse(data, status = 200) { return new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } }); }
function textResponse(body, status = 200) { return new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" } }); }
function requestId() { return crypto.randomUUID(); }
function statusForCoreError(error) { switch (error.code) {
    case "BAD_REQUEST":
    case "UNSUPPORTED_PROTOCOL":
    case "UNSUPPORTED_TRANSPORT": return 400;
    case "AUTH_FAILED": return 401;
    case "MESSAGE_TOO_LARGE": return 413;
    case "TIMEOUT": return 504;
    case "CONFIGURATION_ERROR": return 503;
    default: return 500;
} }
function errorResponse(error, id) { const coreError = error instanceof errors_1.CoreError ? error : new errors_1.CoreError("INTERNAL", "Internal Worker error", false, { cause: error }); const payload = { error: coreError.message, code: coreError.code, retryable: coreError.retryable, request_id: id }; if ("issues" in coreError && Array.isArray(coreError.issues))
    payload.issues = coreError.issues; if (statusForCoreError(coreError) >= 500)
    console.error(JSON.stringify({ request_id: id, code: coreError.code, message: coreError.message })); return jsonResponse(payload, statusForCoreError(coreError)); }
function chooseProtocol(path, env) { const vlessPath = env.VLESS_PATH?.trim() || "/vless"; const trojanPath = env.TROJAN_PATH?.trim() || "/trojan"; if (path === vlessPath)
    return "vless"; if (path === trojanPath)
    return "trojan"; return null; }
function validateEnvironment(env) { (0, validation_1.validateEnvironment)(env); }
exports.default = {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        if (url.pathname === "/__core/health")
            return jsonResponse({ status: "ok", core_version: core_1.CORE_VERSION });
        if (url.pathname === "/__core/ready") {
            try {
                validateEnvironment(env);
                (0, runtime_1.runtimeFor)(env);
                return jsonResponse({ status: "ready", core_version: core_1.CORE_VERSION, bindings: { DB: Boolean(env.DB), KV: Boolean(env.KV) } });
            }
            catch (error) {
                return errorResponse(error, requestId());
            }
        }
        // Control-layer failures go through the same structured error path as
        // every other failure mode (A14) instead of rejecting the fetch handler.
        let control;
        try {
            control = await (0, control_1.handleControlRequest)(request, env);
        }
        catch (error) {
            return errorResponse(error, requestId());
        }
        if (control)
            return control;
        const protocol = chooseProtocol(url.pathname, env);
        if (!protocol)
            return new Response("Not Found", { status: 404, headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" } });
        if (request.method !== "GET")
            return textResponse("WebSocket endpoint requires GET", 405);
        if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket")
            return (0, websocket_1.websocketUpgradeRequiredResponse)();
        if (activeSessions >= core_1.CORE_LIMITS.maxConcurrentSessions)
            return textResponse("Too many concurrent sessions", 503);
        const id = requestId();
        let counted = false;
        try {
            validateEnvironment(env);
            const runtime = (0, runtime_1.runtimeFor)(env);
            activeSessions++;
            counted = true;
            const accepted = await wsAdapter.accept(request, { expectedPath: url.pathname, expectedHost: env.WS_HOST?.trim() || undefined, maxBufferedBytes: core_1.CORE_LIMITS.maxWebSocketBufferedBytes, earlyDataEnabled: true, earlyDataHeaderName: "Sec-WebSocket-Protocol", maxEarlyDataBytes: core_1.CORE_LIMITS.maxEarlyDataBytes });
            const contextController = new AbortController();
            const context = { id: crypto.randomUUID(), startedAt: Date.now(), signal: contextController.signal };
            const task = runtime.engine.handle(accepted.connection, context, protocol)
                .then(async () => { contextController.abort("connection finished"); await accepted.connection.close(1000, "done").catch(() => { }); })
                .catch(async (error) => { contextController.abort(error); const coreError = error instanceof errors_1.CoreError ? error : new errors_1.CoreError("INTERNAL", "Core failure", false, { cause: error }); await accepted.connection.close((0, errors_1.closeCodeForError)(coreError), (0, errors_1.publicCloseReason)(coreError)).catch(() => { }); })
                .finally(() => { activeSessions--; });
            ctx.waitUntil(task);
            return accepted.response;
        }
        catch (error) {
            if (counted)
                activeSessions--;
            return errorResponse(error, id);
        }
    },
};

};

__m["control/index"] = function(module, exports, __r) {
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
__exportStar(__r("./routes"), exports);

};

__m["control/routes"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.handleControlRequest = handleControlRequest;
const core_1 = __r("../core");
const proxyip_1 = __r("../egress/proxyip");
const errors_1 = __r("../core/errors");
const validation_1 = __r("../config/validation");
const runtime_1 = __r("../runtime/runtime");
const planning_1 = __r("../output/planning");
const service_1 = __r("../output/service");
const runner_1 = __r("../tests/runner");
const doh_1 = __r("../handlers/doh");
const authorize_1 = __r("../security/authorize");
function jsonResponse(data, status = 200) {
    return new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
}
function textResponse(body, status = 200) {
    return new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" } });
}
function requestId() { return crypto.randomUUID(); }
function statusForCoreError(error) {
    switch (error.code) {
        case "BAD_REQUEST":
        case "UNSUPPORTED_PROTOCOL":
        case "UNSUPPORTED_TRANSPORT": return 400;
        case "AUTH_FAILED": return 401;
        case "MESSAGE_TOO_LARGE": return 413;
        case "TIMEOUT": return 504;
        case "CONFIGURATION_ERROR": return 503;
        default: return 500;
    }
}
function errorResponse(error, id) {
    const coreError = error instanceof errors_1.CoreError ? error : new errors_1.CoreError("INTERNAL", "Internal Worker error", false, { cause: error });
    const payload = { error: coreError.message, code: coreError.code, retryable: coreError.retryable, request_id: id };
    if ("issues" in coreError && Array.isArray(coreError.issues))
        payload.issues = coreError.issues;
    if (statusForCoreError(coreError) >= 500)
        console.error(JSON.stringify({ request_id: id, code: coreError.code, message: coreError.message }));
    return jsonResponse(payload, statusForCoreError(coreError));
}
function validateEnvironment(env) { (0, validation_1.validateEnvironment)(env); }
// Path-secret checks go through security/authorize's digest-based constant-time
// comparison — the same treatment the bearer tokens get. A local === over the raw secret
// short-circuited on the first differing character (audit F9).
async function handleControlRequest(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__core/diagnostics" || url.pathname === "/__core/tests" || url.pathname === "/__core/endpoints") {
        const id = requestId();
        try {
            const scope = url.pathname === "/__core/tests" ? "tests:run" : "diagnostics:read";
            (0, authorize_1.requireAdminOperation)(request, env, scope);
            validateEnvironment(env);
            const runtime = (0, runtime_1.runtimeFor)(env);
            if (url.pathname === "/__core/tests")
                return jsonResponse({ ...(await (0, runner_1.runSelfTests)(env)), request_id: id });
            if (url.pathname === "/__core/endpoints")
                return jsonResponse({ ...(0, planning_1.outputSummary)(env, url.hostname), request_id: id });
            const proxyip = (0, proxyip_1.hasProxyIp)(env);
            return jsonResponse({
                ok: true, core_version: core_1.CORE_VERSION,
                runtime: { platform: "cloudflare-workers", websocket: true, tcp_sockets: true, durable_objects: false },
                protocols: ["vless", "trojan"], transports: ["websocket"],
                egress: { direct: true, proxyip, relay: Boolean(env.PROXY_RELAY_ENDPOINTS?.trim()), nat64: Boolean((env.NAT64_PREFIXES?.trim() || env.EGRESS_MODE?.trim().toLowerCase() === "smart" || env.EGRESS_MODE?.trim().toLowerCase() === "nat64")) },
                bindings: { DB: Boolean(env.DB), KV: Boolean(env.KV) }, metrics: runtime.metrics.snapshot(),
                routing: { mode: env.EGRESS_MODE?.trim() || "smart", rules: env.CORE_RULES ? env.CORE_RULES.split(/\r?\n/).filter(Boolean).length : 0 },
                dns: runtime.dns.diagnostics(), output: (0, planning_1.outputSummary)(env, url.hostname), limits: runtime.config, request_id: id,
            });
        }
        catch (error) {
            return errorResponse(error, id);
        }
    }
    // Compare on the canonical path form: validation guarantees DOH_PATH has
    // no trailing slash (C4/C5), but strip trailing slashes on both sides the
    // same way the DoH handler's path-secret matcher does, so routing and
    // authorization can never disagree over a stray slash.
    const dohPath = (env.DOH_PATH?.trim() || "/dns-query").replace(/\/+$/, "") || "/dns-query";
    if ((url.pathname.replace(/\/+$/, "") || "/") === dohPath || (0, doh_1.privateDohPathSecretMatches)(request, env)) {
        try {
            return await (0, doh_1.handlePrivateDoh)(request, env);
        }
        catch (error) {
            return errorResponse(error, requestId());
        }
    }
    const isOutputPath = (base) => url.pathname === base || (0, authorize_1.pathSecretMatches)(request, base, env.CORE_OUTPUT_PATH_SECRET);
    // Derive the format from the route base that actually matched. Scanning the whole
    // pathname for "/xray" / "/sing-box" substrings let a path secret containing such a
    // segment misroute the request to the wrong renderer (audit F10).
    const outputRoutes = [
        ["/__core/outputs/xray", "xray"],
        ["/__core/outputs/sing-box", "sing-box"],
        ["/__core/outputs/clash", "clash"],
    ];
    const matchedOutput = outputRoutes.find(([base]) => isOutputPath(base));
    if (matchedOutput) {
        const [outputBase, format] = matchedOutput;
        const id = requestId();
        try {
            (0, authorize_1.requireOutputAccess)(request, env, [outputBase]);
            validateEnvironment(env);
            const runtime = (0, runtime_1.runtimeFor)(env);
            const rendered = await (0, service_1.renderCoreFormat)(env, request, runtime.credentials, format);
            return new Response(rendered.body, { status: 200, headers: { "content-type": rendered.contentType, "cache-control": "no-store", ...(rendered.headers ?? {}) } });
        }
        catch (error) {
            return errorResponse(error, id);
        }
    }
    const isSubscriptionPath = url.pathname === "/sub" || (0, authorize_1.pathSecretMatches)(request, "/sub", env.CORE_OUTPUT_PATH_SECRET) || url.pathname === "/__core/outputs" || (0, authorize_1.pathSecretMatches)(request, "/__core/outputs", env.CORE_OUTPUT_PATH_SECRET) || url.pathname === "/__core/outputs/share" || (0, authorize_1.pathSecretMatches)(request, "/__core/outputs/share", env.CORE_OUTPUT_PATH_SECRET);
    if (isSubscriptionPath) {
        const id = requestId();
        try {
            (0, authorize_1.requireOutputAccess)(request, env, ["/sub", "/__core/outputs", "/__core/outputs/share"]);
            const encode = (url.searchParams.get("encode") ?? "base64").trim().toLowerCase();
            if (encode === "raw" && !/^(1|true|yes|on)$/i.test(env.CORE_OUTPUT_ALLOW_RAW?.trim() ?? ""))
                return textResponse("Raw output is disabled; configure CORE_OUTPUT_ALLOW_RAW=true", 403);
            validateEnvironment(env);
            const runtime = (0, runtime_1.runtimeFor)(env);
            const rendered = await (0, service_1.renderSubscriptionWithProvider)(env, request, runtime.credentials, runtime.dns);
            return new Response(rendered.body, { status: 200, headers: { "content-type": rendered.contentType, "cache-control": "no-store, no-cache, must-revalidate", ...(rendered.headers ?? {}) } });
        }
        catch (error) {
            return errorResponse(error, id);
        }
    }
    return null;
}

};

__m["security/authorize"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.legacyTestTokenEnabled = legacyTestTokenEnabled;
exports.authenticate = authenticate;
exports.requireScope = requireScope;
exports.requireAdminOperation = requireAdminOperation;
exports.requireOutputAccess = requireOutputAccess;
exports.pathSecretMatches = pathSecretMatches;
const errors_1 = __r("../core/errors");
const sha224_1 = __r("../util/sha224");
// Digest-then-compare: both sides are reduced to a fixed-length SHA-224 hex
// digest, so the XOR loop is fixed-time and leaks no content position. The
// residual cost correlation with the *supplied* input's length is inherent to
// pre-hashing and reveals nothing about the stored secret (B5).
function constantTimeEqual(a, b) {
    const ah = (0, sha224_1.sha224Hex)(new TextEncoder().encode(a));
    const bh = (0, sha224_1.sha224Hex)(new TextEncoder().encode(b));
    let diff = 0;
    for (let i = 0; i < ah.length; i++)
        diff |= ah.charCodeAt(i) ^ bh.charCodeAt(i);
    return diff === 0;
}
function suppliedToken(request, allowQuery) {
    const header = request.headers.get("Authorization")?.replace(/^Bearer\s+/i, "").trim();
    if (header)
        return { token: header, fromQuery: false };
    if (allowQuery) {
        const query = new URL(request.url).searchParams.get("token")?.trim() ?? "";
        if (query)
            return { token: query, fromQuery: true };
    }
    return { token: "", fromQuery: false };
}
/**
 * The legacy CORE_TEST_TOKEN is a migration aid, not a standing credential:
 * it is honoured only when the operator explicitly opts in with
 * CORE_ALLOW_LEGACY_TEST_TOKEN=1/true/yes/on. Without the opt-in the token is
 * ignored entirely and cannot enable or access management endpoints (B3).
 * The flag is declared on CoreEnvironment by the config module; it is read
 * through a narrow structural view so this module keeps working while the
 * declaration lands.
 */
function legacyTestTokenEnabled(env) {
    const flag = env.CORE_ALLOW_LEGACY_TEST_TOKEN;
    return /^(1|true|yes|on)$/i.test(flag?.trim() ?? "");
}
function authenticate(request, env, allowQuery = false) {
    const { token, fromQuery } = suppliedToken(request, allowQuery);
    if (!token)
        return null;
    // Evaluate every configured credential class before selecting an identity,
    // so response timing does not reveal which class matched (B5). Privileged
    // classes (admin, legacy) are header-only: a token carried in the URL query
    // string — which leaks into logs and history — may only ever resolve to the
    // output identity (B13). This is keyed on the token's provenance, not on the
    // allowQuery flag, so a header-supplied admin token still works on routes
    // that also accept query tokens for output access.
    const adminToken = env.CORE_ADMIN_TOKEN?.trim();
    const adminMatch = Boolean(adminToken && !fromQuery && constantTimeEqual(token, adminToken));
    const outputToken = env.CORE_OUTPUT_TOKEN?.trim();
    const outputMatch = Boolean(outputToken && constantTimeEqual(token, outputToken));
    const legacy = env.CORE_TEST_TOKEN?.trim();
    const legacyMatch = Boolean(legacy && !fromQuery && legacyTestTokenEnabled(env) && constantTimeEqual(token, legacy));
    if (adminMatch) {
        return {
            subject: "admin",
            scopes: new Set(["output:read", "diagnostics:read", "tests:run", "admin:write"]),
            source: "admin",
        };
    }
    if (outputMatch) {
        return { subject: "output", scopes: new Set(["output:read"]), source: "output" };
    }
    if (legacyMatch) {
        return {
            subject: "legacy-admin",
            scopes: new Set(["diagnostics:read", "tests:run"]),
            source: "legacy",
        };
    }
    return null;
}
function requireScope(request, env, scope, allowQuery = false) {
    const identity = authenticate(request, env, allowQuery);
    if (!identity || !identity.scopes.has(scope))
        throw new errors_1.CoreError("AUTH_FAILED", "Unauthorized");
    return identity;
}
function requireAdminOperation(request, env, scope) {
    if (!env.CORE_ADMIN_TOKEN?.trim() && !(env.CORE_TEST_TOKEN?.trim() && legacyTestTokenEnabled(env))) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Management endpoint disabled; configure CORE_ADMIN_TOKEN");
    }
    return requireScope(request, env, scope, false);
}
function requireOutputAccess(request, env, pathBases = ["/sub", "/__core/outputs", "/__core/outputs/share", "/__core/outputs/xray", "/__core/outputs/sing-box", "/__core/outputs/clash"]) {
    const configured = env.CORE_OUTPUT_TOKEN?.trim() || env.CORE_ADMIN_TOKEN?.trim() || env.CORE_OUTPUT_PATH_SECRET?.trim();
    if (!configured)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Output API disabled; configure CORE_OUTPUT_TOKEN or CORE_OUTPUT_PATH_SECRET");
    if (pathBases.some((base) => pathSecretMatches(request, base, env.CORE_OUTPUT_PATH_SECRET))) {
        return { subject: "output-path", scopes: new Set(["output:read"]), source: "output" };
    }
    const allowQuery = /^(1|true|yes|on)$/i.test(env.CORE_OUTPUT_ALLOW_QUERY_TOKEN?.trim() ?? "");
    return requireScope(request, env, "output:read", allowQuery);
}
function pathSecretMatches(request, configuredPath, secret) {
    const trimmed = secret?.trim();
    if (!trimmed)
        return false;
    const path = new URL(request.url).pathname.replace(/\/+$/, "") || "/";
    const base = configuredPath.replace(/\/+$/, "") || "/";
    // The path secret is a credential of the same sensitivity as the bearer
    // tokens above, so it gets the same digest-based constant-time comparison
    // instead of a short-circuiting === over the raw secret (B2). Comparing the
    // full expected path keeps the exact-match (non-prefix) semantics.
    return constantTimeEqual(path, `${base}/${trimmed}`);
}

};

__m["util/sha224"] = function(module, exports, __r) {
"use strict";
// Small pure-TypeScript SHA-224 implementation.
// SHA-224 uses the SHA-256 compression function with different IVs and a 224-bit digest.
Object.defineProperty(exports, "__esModule", { value: true });
exports.sha224 = sha224;
exports.sha224Hex = sha224Hex;
const K = new Uint32Array([
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);
const IV = new Uint32Array([
    0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939,
    0xffc00b31, 0x68581511, 0x64f98fa7, 0xbefa4fa4,
]);
const rotr = (x, n) => (x >>> n) | (x << (32 - n));
const ch = (x, y, z) => (x & y) ^ (~x & z);
const maj = (x, y, z) => (x & y) ^ (x & z) ^ (y & z);
const bigSigma0 = (x) => rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22);
const bigSigma1 = (x) => rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25);
const smallSigma0 = (x) => rotr(x, 7) ^ rotr(x, 18) ^ (x >>> 3);
const smallSigma1 = (x) => rotr(x, 17) ^ rotr(x, 19) ^ (x >>> 10);
function sha224(data) {
    const bitLength = data.byteLength * 8;
    const paddedLength = (((data.byteLength + 9 + 63) >> 6) << 6);
    const padded = new Uint8Array(paddedLength);
    padded.set(data);
    padded[data.byteLength] = 0x80;
    const view = new DataView(padded.buffer);
    view.setUint32(paddedLength - 8, Math.floor(bitLength / 0x100000000), false);
    view.setUint32(paddedLength - 4, bitLength >>> 0, false);
    const h = new Uint32Array(IV);
    const w = new Uint32Array(64);
    for (let offset = 0; offset < paddedLength; offset += 64) {
        for (let i = 0; i < 16; i++)
            w[i] = view.getUint32(offset + i * 4, false);
        for (let i = 16; i < 64; i++) {
            w[i] = (smallSigma1(w[i - 2]) + w[i - 7] + smallSigma0(w[i - 15]) + w[i - 16]) >>> 0;
        }
        let a = h[0], b = h[1], c = h[2], d = h[3];
        let e = h[4], f = h[5], g = h[6], hh = h[7];
        for (let i = 0; i < 64; i++) {
            const t1 = (hh + bigSigma1(e) + ch(e, f, g) + K[i] + w[i]) >>> 0;
            const t2 = (bigSigma0(a) + maj(a, b, c)) >>> 0;
            hh = g;
            g = f;
            f = e;
            e = (d + t1) >>> 0;
            d = c;
            c = b;
            b = a;
            a = (t1 + t2) >>> 0;
        }
        h[0] = (h[0] + a) >>> 0;
        h[1] = (h[1] + b) >>> 0;
        h[2] = (h[2] + c) >>> 0;
        h[3] = (h[3] + d) >>> 0;
        h[4] = (h[4] + e) >>> 0;
        h[5] = (h[5] + f) >>> 0;
        h[6] = (h[6] + g) >>> 0;
        h[7] = (h[7] + hh) >>> 0;
    }
    const out = new Uint8Array(28);
    const outView = new DataView(out.buffer);
    for (let i = 0; i < 7; i++)
        outView.setUint32(i * 4, h[i], false);
    return out;
}
function sha224Hex(data) {
    const hash = sha224(data);
    let out = "";
    for (const byte of hash)
        out += byte.toString(16).padStart(2, "0");
    return out;
}

};

__m["core/errors"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CoreError = void 0;
exports.isAbortError = isAbortError;
exports.asCoreError = asCoreError;
exports.closeCodeForError = closeCodeForError;
exports.publicCloseReason = publicCloseReason;
class CoreError extends Error {
    code;
    retryable;
    constructor(code, message, retryable = false, options) {
        super(message, options);
        this.code = code;
        this.retryable = retryable;
        this.name = "CoreError";
    }
}
exports.CoreError = CoreError;
function isAbortError(error) {
    return error instanceof DOMException && error.name === "AbortError";
}
function asCoreError(error, fallbackCode = "INTERNAL") {
    if (error instanceof CoreError)
        return error;
    if (isAbortError(error))
        return new CoreError("CLIENT_CLOSED", "Operation aborted", false, { cause: error });
    if (error instanceof Error)
        return new CoreError(fallbackCode, error.message || "Core operation failed", false, { cause: error });
    return new CoreError(fallbackCode, String(error));
}
function closeCodeForError(error) {
    switch (error.code) {
        case "AUTH_FAILED":
            return 1008;
        case "MESSAGE_TOO_LARGE":
            return 1009;
        case "BAD_REQUEST":
        case "INVALID_DESTINATION":
        case "UNSUPPORTED_PROTOCOL":
        case "UNSUPPORTED_TRANSPORT":
            return 1002;
        case "CLIENT_CLOSED":
            return 1000;
        default:
            return 1011;
    }
}
function publicCloseReason(error) {
    switch (error.code) {
        case "AUTH_FAILED": return "Unauthorized";
        case "BAD_REQUEST": return "Bad request";
        case "INVALID_DESTINATION": return "Invalid destination";
        case "MESSAGE_TOO_LARGE": return "Message too large";
        case "UNSUPPORTED_PROTOCOL": return "Unsupported protocol";
        case "UNSUPPORTED_TRANSPORT": return "Unsupported transport";
        case "TIMEOUT": return "Timeout";
        case "CLIENT_CLOSED": return "Closed";
        case "UPSTREAM_CLOSED": return "Upstream closed";
        case "DNS_FAILED": return "DNS failure";
        case "EGRESS_FAILED": return "Egress failed";
        case "ROUTE_FAILED": return "Route failed";
        case "CONFIGURATION_ERROR": return "Configuration error";
        case "INTERNAL": return "Internal error";
        default: return `Core error: ${error.code}`;
    }
}

};

__m["handlers/doh"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.privateDohPathSecretMatches = privateDohPathSecretMatches;
exports.handlePrivateDoh = handlePrivateDoh;
const errors_1 = __r("../core/errors");
const limits_1 = __r("../core/limits");
const sha224_1 = __r("../util/sha224");
const config_1 = __r("../dns/config");
const doh_1 = __r("../dns/doh");
/** Hard cap on total upstream attempts for one client query (D3). */
const MAX_UPSTREAM_ATTEMPTS = 8;
/**
 * Bounded wait between attempts on the same endpoint (D3): small
 * exponential backoff with jitter, aborted immediately by client disconnect.
 */
async function attemptBackoff(attempt, signal) {
    if (attempt <= 0)
        return;
    const delayMs = Math.min(1000, 50 * 2 ** (attempt - 1)) + Math.floor(Math.random() * 50);
    await new Promise((resolve, reject) => {
        if (signal.aborted) {
            reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
            return;
        }
        const onAbort = () => { clearTimeout(timer); reject(signal.reason ?? new DOMException("Aborted", "AbortError")); };
        const timer = setTimeout(() => { signal.removeEventListener("abort", onAbort); resolve(); }, delayMs);
        signal.addEventListener("abort", onAbort, { once: true });
    });
}
function resolverUrls(env) {
    const maxResolversRaw = env.DNS_MAX_RESOLVERS?.trim();
    const maxResolvers = maxResolversRaw && /^\d+$/.test(maxResolversRaw) ? Math.min(8, Math.max(1, Number(maxResolversRaw))) : limits_1.CORE_LIMITS.maxDnsResolvers;
    return (0, config_1.parseDohResolverEndpoints)(env.DNS_RESOLVERS, maxResolvers);
}
function parseTimeoutMs(env) {
    const raw = env.DNS_TIMEOUT_MS?.trim();
    if (!raw)
        return 3000;
    if (!/^\d+$/.test(raw))
        return 3000;
    return Math.min(60_000, Math.max(100, Number(raw)));
}
function constantTimeEqual(a, b) {
    const ah = (0, sha224_1.sha224Hex)(new TextEncoder().encode(a));
    const bh = (0, sha224_1.sha224Hex)(new TextEncoder().encode(b));
    let diff = 0;
    for (let i = 0; i < ah.length; i++)
        diff |= ah.charCodeAt(i) ^ bh.charCodeAt(i);
    return diff === 0;
}
function privateDohPathSecretMatches(request, env) {
    const secret = env.CORE_DOH_PATH_SECRET?.trim();
    if (!secret)
        return false;
    const base = (env.DOH_PATH?.trim() || "/dns-query").replace(/\/+$/, "");
    const path = new URL(request.url).pathname.replace(/\/+$/, "");
    // Constant-time comparison, same as the Bearer token path (D7). Note the
    // secret still travels in the URL by design (client-config feature); the
    // token remains the preferred credential.
    return constantTimeEqual(path, `${base}/${secret}`);
}
async function handlePrivateDoh(request, env) {
    if (request.method !== "GET" && request.method !== "POST")
        return new Response("Method Not Allowed", { status: 405, headers: { allow: "GET, POST" } });
    const token = env.CORE_DOH_TOKEN?.trim();
    const pathAuthorized = privateDohPathSecretMatches(request, env);
    if (!token && !pathAuthorized)
        return new Response("Private DoH disabled", { status: 503 });
    if (!pathAuthorized) {
        const supplied = request.headers.get("Authorization")?.replace(/^Bearer\s+/i, "").trim();
        if (!supplied || !token || !constantTimeEqual(supplied, token))
            return new Response("Unauthorized", { status: 401 });
    }
    // DNS_ENABLED=false is the global DNS kill switch and also disables this
    // endpoint (D8); checked after auth so the gate is not an oracle.
    const dnsEnabledRaw = env.DNS_ENABLED?.trim().toLowerCase();
    if (dnsEnabledRaw && ["0", "false", "no", "off"].includes(dnsEnabledRaw))
        return new Response("Private DoH disabled", { status: 503 });
    // RFC 8484 §4.1/§4.2 media-type and method-shape enforcement (D2).
    let input;
    if (request.method === "GET") {
        const source = new URL(request.url).searchParams.get("dns");
        if (!source)
            throw new errors_1.CoreError("BAD_REQUEST", "GET DoH requests must carry a dns query parameter");
        input = decodeDnsParam(source);
    }
    else {
        const contentType = request.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase();
        if (contentType !== "application/dns-message")
            return new Response("Unsupported Media Type", { status: 415 });
        input = await readBody(request, request.signal);
    }
    if (input.byteLength === 0 || input.byteLength > limits_1.CORE_LIMITS.maxDoHQueryBytes)
        throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DNS message must be 1..${limits_1.CORE_LIMITS.maxDoHQueryBytes} bytes`);
    // The endpoint exchanges DNS messages, not opaque blobs: validate the
    // request with the same bounded raw validator as the VLESS UDP/53 path,
    // and keep its identity for response matching (D1).
    const question = (0, doh_1.validateRawDnsQuery)(input);
    const requestId = ((input[0] << 8) | input[1]) >>> 0;
    const endpoints = resolverUrls(env);
    if (!endpoints.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "No DoH resolver configured");
    const fallback = parseBoolean(env.DNS_FALLBACK, true);
    const retries = parseRetries(env.DNS_RETRIES);
    let lastError;
    // Overall attempt budget across all endpoints (D3): the per-endpoint
    // retry knob still applies, but one client query can no longer multiply
    // into endpoints × attempts upstream POSTs without bound.
    let attemptsLeft = MAX_UPSTREAM_ATTEMPTS;
    for (let endpointIndex = 0; endpointIndex < endpoints.length && attemptsLeft > 0; endpointIndex++) {
        const endpoint = endpoints[endpointIndex];
        let endpointDone = false;
        for (let attempt = 0; attempt <= retries && attemptsLeft > 0 && !endpointDone; attempt++) {
            attemptsLeft--;
            if (attempt > 0)
                await attemptBackoff(attempt, request.signal);
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(new errors_1.CoreError("TIMEOUT", "Private DoH upstream timeout", true)), parseTimeoutMs(env));
            const onAbort = () => controller.abort(request.signal.reason ?? new DOMException("Aborted", "AbortError"));
            request.signal.addEventListener("abort", onAbort, { once: true });
            try {
                const response = await fetch(endpoint, {
                    method: "POST",
                    headers: { "content-type": "application/dns-message", accept: "application/dns-message" },
                    body: input,
                    redirect: "manual",
                    cache: "no-store",
                    signal: controller.signal,
                });
                if (!response.ok) {
                    // Never echo the configured endpoint URL to clients (D6).
                    // 5xx/429 are transient; other 4xx are deterministic for this
                    // endpoint and end its attempts (D3).
                    throw new errors_1.CoreError("DNS_FAILED", `Upstream DoH returned HTTP ${response.status}`, response.status >= 500 || response.status === 429);
                }
                // RFC 8484 §4: a DoH response carries the DNS message with the
                // application/dns-message media type; anything else (e.g. an HTML
                // error page relayed with 200) is not relayed as DNS (D2).
                const responseType = response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase();
                if (responseType !== "application/dns-message") {
                    endpointDone = true;
                    throw new errors_1.CoreError("DNS_FAILED", "Upstream DoH returned a non-DNS media type", false);
                }
                const contentLength = Number(response.headers.get("content-length") ?? "0");
                if (contentLength > limits_1.CORE_LIMITS.maxDoHResponseBytes)
                    throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `Upstream DoH response exceeds ${limits_1.CORE_LIMITS.maxDoHResponseBytes} bytes`);
                const body = await cappedResponseBody(response, limits_1.CORE_LIMITS.maxDoHResponseBytes);
                try {
                    // Verify the upstream answer belongs to this query (ID + question)
                    // before relaying it as a DNS message (D1).
                    (0, doh_1.validateRawDnsResponse)(body, requestId, question);
                }
                catch (error) {
                    endpointDone = true;
                    throw error;
                }
                return new Response(body, { status: 200, headers: { "content-type": "application/dns-message", "cache-control": "no-store" } });
            }
            catch (error) {
                lastError = error;
                if (request.signal.aborted)
                    throw request.signal.reason;
                if (error instanceof errors_1.CoreError && error.code === "DNS_FAILED" && !error.retryable)
                    endpointDone = true;
            }
            finally {
                clearTimeout(timer);
                request.signal.removeEventListener("abort", onAbort);
            }
        }
        if (!fallback)
            break;
    }
    if (lastError instanceof errors_1.CoreError)
        throw lastError;
    const detail = lastError instanceof Error ? lastError.message : String(lastError ?? "unknown resolver failure");
    throw new errors_1.CoreError("DNS_FAILED", `All configured DoH resolvers failed: ${detail.slice(0, 400)}`, true, { cause: lastError });
}
function parseBoolean(value, fallback) {
    const raw = value?.trim().toLowerCase();
    if (!raw)
        return fallback;
    if (["1", "true", "yes", "on"].includes(raw))
        return true;
    if (["0", "false", "no", "off"].includes(raw))
        return false;
    return fallback;
}
function parseRetries(value) {
    const raw = value?.trim();
    if (!raw || !/^\d+$/.test(raw))
        return 1;
    return Math.min(5, Math.max(0, Number(raw)));
}
function decodeDnsParam(value) {
    const raw = value.trim();
    if (!raw || raw.length > 8192 || !/^[A-Za-z0-9_-]+={0,2}$/.test(raw))
        throw new errors_1.CoreError("BAD_REQUEST", "Invalid dns query parameter");
    const firstPadding = raw.indexOf("=");
    const body = firstPadding >= 0 ? raw.slice(0, firstPadding) : raw;
    const padding = firstPadding >= 0 ? raw.slice(firstPadding) : "";
    if (body.length % 4 === 1 || (padding && padding.length !== (4 - (body.length % 4)) % 4))
        throw new errors_1.CoreError("BAD_REQUEST", "Invalid dns query parameter");
    const normalized = body.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized + "=".repeat((4 - normalized.length % 4) % 4);
    try {
        const binary = atob(padded);
        if (binary.length > limits_1.CORE_LIMITS.maxDoHQueryBytes)
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DNS query parameter exceeds ${limits_1.CORE_LIMITS.maxDoHQueryBytes} bytes`);
        return Uint8Array.from(binary, (c) => c.charCodeAt(0));
    }
    catch (error) {
        if (error instanceof errors_1.CoreError)
            throw error;
        throw new errors_1.CoreError("BAD_REQUEST", "Invalid dns query parameter");
    }
}
async function readBody(request, signal) {
    const length = Number(request.headers.get("content-length") ?? "0");
    if (length > limits_1.CORE_LIMITS.maxDoHQueryBytes)
        throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DNS request body exceeds ${limits_1.CORE_LIMITS.maxDoHQueryBytes} bytes`);
    if (!request.body)
        return new Uint8Array(await request.arrayBuffer());
    const reader = request.body.getReader();
    const chunks = [];
    let total = 0;
    try {
        while (true) {
            if (signal.aborted)
                throw signal.reason;
            const item = await reader.read();
            if (item.done)
                break;
            total += item.value.byteLength;
            if (total > limits_1.CORE_LIMITS.maxDoHQueryBytes)
                throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DNS request body exceeds ${limits_1.CORE_LIMITS.maxDoHQueryBytes} bytes`);
            chunks.push(item.value);
        }
    }
    catch (error) {
        await reader.cancel(error).catch(() => { });
        throw error;
    }
    finally {
        reader.releaseLock();
    }
    const body = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
        body.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return body;
}
async function cappedResponseBody(response, maxBytes) {
    if (!response.body) {
        const bytes = new Uint8Array(await response.arrayBuffer());
        if (bytes.byteLength > maxBytes)
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `Upstream DoH response exceeds ${maxBytes} bytes`);
        return bytes;
    }
    const reader = response.body.getReader();
    const chunks = [];
    let total = 0;
    try {
        while (true) {
            const item = await reader.read();
            if (item.done)
                break;
            total += item.value.byteLength;
            if (total > maxBytes)
                throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `Upstream DoH response exceeds ${maxBytes} bytes`);
            chunks.push(item.value);
        }
    }
    catch (error) {
        await reader.cancel(error).catch(() => { });
        throw error;
    }
    finally {
        reader.releaseLock();
    }
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
        out.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return out;
}

};

__m["dns/doh"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DoHResolver = void 0;
exports.validateRawDnsQuery = validateRawDnsQuery;
exports.validateRawDnsResponse = validateRawDnsResponse;
const errors_1 = __r("../core/errors");
const limits_1 = __r("../core/limits");
const hostname_1 = __r("../util/hostname");
const TYPE_A = 1;
const TYPE_CNAME = 5;
const TYPE_AAAA = 28;
const TYPE_SOA = 6;
function encodeName(name) {
    const clean = (0, hostname_1.toAsciiHostname)(name).replace(/\.$/, "");
    if (!clean || clean.length > 253 || /[\u0000-\u0020\u007f/\?#@\\]/.test(clean))
        throw new errors_1.CoreError("DNS_FAILED", "Invalid DNS hostname");
    const labels = clean.split(".");
    const out = [];
    for (const label of labels) {
        const bytes = new TextEncoder().encode(label);
        if (bytes.length === 0 || bytes.length > 63)
            throw new errors_1.CoreError("DNS_FAILED", "Invalid DNS label");
        out.push(bytes.length, ...bytes);
    }
    if (out.length > 255)
        throw new errors_1.CoreError("DNS_FAILED", "DNS hostname wire length exceeds 255 bytes");
    out.push(0);
    return new Uint8Array(out);
}
function canonicalDnsName(name) {
    return (0, hostname_1.toAsciiHostname)(name).replace(/\.$/, "").toLowerCase();
}
function buildQuery(name, type) {
    const id = crypto.getRandomValues(new Uint16Array(1))[0];
    const qname = encodeName(name);
    // EDNS(0) OPT pseudo-RR (RFC 6891 §6.1): root name, type 41, requestor UDP
    // payload size 1232 in the class field, TTL 0 (no extended RCODE, DO=0),
    // RDLEN 0. Signals EDNS support so upstreams do not apply pre-EDNS size
    // heuristics to large answer sets (D11).
    const OPT_RR_LENGTH = 11;
    const out = new Uint8Array(12 + qname.length + 4 + OPT_RR_LENGTH);
    const view = new DataView(out.buffer);
    view.setUint16(0, id);
    view.setUint16(2, 0x0100); // RD
    view.setUint16(4, 1);
    view.setUint16(10, 1); // ARCOUNT = 1 (the OPT record)
    out.set(qname, 12);
    view.setUint16(12 + qname.length, type === "A" ? TYPE_A : TYPE_AAAA);
    view.setUint16(14 + qname.length, 1);
    const opt = 12 + qname.length + 4;
    out[opt] = 0; // root owner name
    view.setUint16(opt + 1, 41); // TYPE OPT
    view.setUint16(opt + 3, 1232); // requestor's UDP payload size
    view.setUint32(opt + 5, 0); // extended RCODE + version + flags (DO=0)
    view.setUint16(opt + 9, 0); // RDLEN
    return { bytes: out, id };
}
function readName(message, offset) {
    if (!Number.isInteger(offset) || offset < 0 || offset >= message.length)
        throw new Error("Malformed DNS name offset");
    const labels = [];
    let terminated = false;
    let cursor = offset;
    let next = offset;
    let jumped = false;
    let jumps = 0;
    let consumed = 0;
    while (cursor < message.length) {
        const length = message[cursor];
        if (length === 0) {
            if (!jumped)
                next = cursor + 1;
            terminated = true;
            break;
        }
        if ((length & 0xc0) === 0xc0) {
            if (cursor + 1 >= message.length)
                throw new Error("Malformed DNS compression pointer");
            const pointer = ((length & 0x3f) << 8) | message[cursor + 1];
            if (pointer >= message.length)
                throw new Error("Malformed DNS compression pointer target");
            // RFC 1035 §4.1.4: a pointer refers to a PRIOR occurrence of the same
            // name, so its target must be strictly before the pointer itself (D12).
            // The 32-jump cap below remains as defence in depth.
            if (pointer >= cursor)
                throw new Error("Malformed DNS compression pointer direction");
            if (!jumped)
                next = cursor + 2;
            cursor = pointer;
            consumed += 2;
            jumped = true;
            if (++jumps > 32)
                throw new Error("DNS compression loop");
            continue;
        }
        if (length > 63 || cursor + 1 + length > message.length)
            throw new Error("Malformed DNS label");
        labels.push(new TextDecoder("utf-8", { fatal: true }).decode(message.subarray(cursor + 1, cursor + 1 + length)));
        cursor += 1 + length;
        consumed += 1 + length;
        if (consumed > 512)
            throw new Error("DNS name is too long");
        if (!jumped)
            next = cursor;
    }
    if (!terminated)
        throw new errors_1.CoreError("BAD_REQUEST", "DNS name is not terminated");
    return { name: labels.join("."), next };
}
function parseIpv4(bytes) {
    return bytes.length === 4 ? Array.from(bytes).join(".") : null;
}
function parseIpv6(bytes) {
    if (bytes.length !== 16)
        return null;
    const groups = [];
    for (let i = 0; i < 16; i += 2)
        groups.push(((bytes[i] << 8) | bytes[i + 1]).toString(16));
    let bestStart = -1;
    let bestLength = 0;
    let start = -1;
    for (let i = 0; i <= groups.length; i++) {
        if (i < groups.length && groups[i] === "0") {
            if (start === -1)
                start = i;
        }
        else if (start !== -1) {
            const length = i - start;
            if (length > bestLength) {
                bestStart = start;
                bestLength = length;
            }
            start = -1;
        }
    }
    if (bestLength >= 2) {
        const left = groups.slice(0, bestStart).join(":");
        const right = groups.slice(bestStart + bestLength).join(":");
        return `${left ? `${left}:` : ""}::${right ? `:${right}` : ""}`.replace(/:{3,}/g, "::");
    }
    return groups.join(":");
}
function parseMessage(bytes, expectedId, expectedName, expectedType) {
    if (bytes.length < 12)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response is too short", true);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const id = view.getUint16(0);
    if (id !== expectedId)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response ID mismatch", true);
    const flags = view.getUint16(2);
    if ((flags & 0x8000) === 0)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response is not a response packet", true);
    if (((flags >> 11) & 0x0f) !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "Unsupported DNS response opcode", true);
    if ((flags & 0x0200) !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "Truncated DNS response", true);
    if ((flags & 0x0040) !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response has reserved header bits set", true);
    const status = flags & 0x000f;
    const qd = view.getUint16(4);
    if (qd !== 1)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response must contain exactly one question", true);
    const an = view.getUint16(6);
    const ns = view.getUint16(8);
    const ar = view.getUint16(10);
    let offset = 12;
    const q = readName(bytes, offset);
    offset = q.next;
    if (offset + 4 > bytes.length)
        throw new errors_1.CoreError("DNS_FAILED", "Malformed DNS question", true);
    const qType = view.getUint16(offset);
    const qClass = view.getUint16(offset + 2);
    offset += 4;
    const expectedWireName = canonicalDnsName(expectedName);
    if (q.name.toLowerCase().replace(/\.$/, "") !== expectedWireName || qType !== (expectedType === "A" ? TYPE_A : TYPE_AAAA) || qClass !== 1) {
        throw new errors_1.CoreError("DNS_FAILED", "DNS response question does not match the request", true);
    }
    const readRecords = (count) => {
        const records = [];
        for (let i = 0; i < count; i++) {
            const n = readName(bytes, offset);
            offset = n.next;
            if (offset + 10 > bytes.length)
                throw new errors_1.CoreError("DNS_FAILED", "Malformed DNS answer", true);
            const type = view.getUint16(offset);
            const klass = view.getUint16(offset + 2);
            const ttl = view.getUint32(offset + 4);
            const rdLength = view.getUint16(offset + 8);
            offset += 10;
            if (offset + rdLength > bytes.length)
                throw new errors_1.CoreError("DNS_FAILED", "Malformed DNS RDATA", true);
            const rdataOffset = offset;
            let data = "";
            let soaMinimum;
            let soaTtl;
            if (klass === 1 && type === TYPE_A) {
                data = parseIpv4(bytes.subarray(offset, offset + rdLength)) ?? "";
            }
            else if (klass === 1 && type === TYPE_AAAA) {
                data = parseIpv6(bytes.subarray(offset, offset + rdLength)) ?? "";
            }
            else if (klass === 1 && type === TYPE_CNAME) {
                // Only IN-class CNAMEs are meaningful for this resolver (RFC 1035
                // §3.2.4); A/AAAA extraction above already required class IN (D10).
                const cname = readName(bytes, rdataOffset);
                if (cname.next > rdataOffset + rdLength)
                    throw new errors_1.CoreError("DNS_FAILED", "CNAME RDATA exceeds record boundary", true);
                data = cname.name;
            }
            else if (type === TYPE_SOA) {
                data = "soa";
                // Parse MNAME/RNAME + the five uint32 fields so the SOA TTL and the
                // MINIMUM field survive for RFC 2308 §5 negative caching (D4).
                try {
                    const mname = readName(bytes, rdataOffset);
                    const rname = readName(bytes, mname.next);
                    if (rname.next + 20 <= rdataOffset + rdLength) {
                        soaMinimum = view.getUint32(rname.next + 16);
                        soaTtl = ttl;
                    }
                }
                catch { /* malformed SOA RDATA: keep the marker, TTL stays unknown */ }
            }
            records.push({ name: n.name.toLowerCase(), type, ttl, data, klass, ...(soaMinimum !== undefined ? { soaMinimum, soaTtl } : {}) });
            offset += rdLength;
        }
        return records;
    };
    const answers = readRecords(an);
    const authority = readRecords(ns);
    void readRecords(ar);
    // Match validateRawDnsResponse: a well-formed message ends exactly at the
    // last additional record; trailing bytes indicate a corrupt relay (D9).
    if (offset !== bytes.length)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response has trailing bytes", true);
    return { status, answers, authority };
}
function normalizeEndpoint(value) {
    const endpoint = value.trim();
    if (!endpoint)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "DNS resolver endpoint cannot be empty");
    const url = new URL(endpoint);
    if (url.protocol !== "https:" || url.username || url.password || url.hash)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Only credential-free HTTPS DoH endpoints are supported: ${endpoint}`);
    if (url.hostname.length > 253 || url.href.length > 2048)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `DoH endpoint is too long: ${endpoint}`);
    return url.toString();
}
/**
 * RFC 2308 §5: the TTL for negative caching is the minimum of the SOA
 * record's TTL and the SOA MINIMUM field, taken from the authority section.
 * Returns undefined when the response carries no usable SOA, in which case
 * the cache's configured default applies.
 */
function negativeCacheTtlSeconds(message) {
    const soa = message.authority.find((item) => item.type === TYPE_SOA && item.soaMinimum !== undefined);
    if (!soa || soa.soaMinimum === undefined)
        return undefined;
    const ttl = Math.min(Math.max(0, soa.soaTtl ?? soa.ttl), soa.soaMinimum);
    return ttl > 0 ? ttl : undefined;
}
/**
 * Bounded retry wait for the resolver paths (D3): exponential backoff with
 * jitter (50ms, 100ms, 200ms, … capped at 1s, plus up to 50ms jitter) that
 * resolves early — by rejection — as soon as the caller aborts, so backoff
 * never delays cancellation handling.
 */
async function retryBackoff(attempt, signal) {
    if (attempt <= 0)
        return;
    const base = Math.min(1000, 50 * 2 ** (attempt - 1));
    const delayMs = base + Math.floor(Math.random() * 50);
    await new Promise((resolve, reject) => {
        if (signal.aborted) {
            reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
            return;
        }
        const onAbort = () => { clearTimeout(timer); reject(signal.reason ?? new DOMException("Aborted", "AbortError")); };
        const timer = setTimeout(() => { signal.removeEventListener("abort", onAbort); resolve(); }, delayMs);
        signal.addEventListener("abort", onAbort, { once: true });
    });
}
async function readCappedBody(response, maxBytes) {
    if (!response.body) {
        const bytes = new Uint8Array(await response.arrayBuffer());
        if (bytes.byteLength > maxBytes)
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DoH response exceeds ${maxBytes} bytes`);
        return bytes;
    }
    const reader = response.body.getReader();
    const chunks = [];
    let total = 0;
    try {
        while (true) {
            const item = await reader.read();
            if (item.done)
                break;
            total += item.value.byteLength;
            if (total > maxBytes)
                throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DoH response exceeds ${maxBytes} bytes`);
            chunks.push(item.value);
        }
    }
    catch (error) {
        await reader.cancel(error).catch(() => { });
        throw error;
    }
    finally {
        reader.releaseLock();
    }
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
        out.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return out;
}
function readRawQuestion(message, view, offset) {
    let name;
    try {
        name = readName(message, offset);
    }
    catch (error) {
        throw new errors_1.CoreError("DNS_FAILED", "Malformed DNS question name", true, { cause: error });
    }
    if (name.next + 4 > message.length)
        throw new errors_1.CoreError("DNS_FAILED", "Malformed DNS question", true);
    const type = view.getUint16(name.next);
    const classCode = view.getUint16(name.next + 2);
    return { question: { name: canonicalDnsName(name.name), type, classCode }, next: name.next + 4 };
}
function skipRawRecords(message, view, offset, count, section) {
    let cursor = offset;
    for (let i = 0; i < count; i++) {
        let name;
        try {
            name = readName(message, cursor);
        }
        catch (error) {
            throw new errors_1.CoreError("DNS_FAILED", `Malformed DNS ${section} record name`, true, { cause: error });
        }
        cursor = name.next;
        if (cursor + 10 > message.length)
            throw new errors_1.CoreError("DNS_FAILED", `Malformed DNS ${section} record header`, true);
        const rdLength = view.getUint16(cursor + 8);
        cursor += 10;
        if (cursor + rdLength > message.length)
            throw new errors_1.CoreError("DNS_FAILED", `Malformed DNS ${section} record data`, true);
        cursor += rdLength;
    }
    return cursor;
}
function readRawHeader(message) {
    if (message.byteLength < 12)
        throw new errors_1.CoreError("DNS_FAILED", "DNS message is too short", true);
    const view = new DataView(message.buffer, message.byteOffset, message.byteLength);
    const id = view.getUint16(0);
    const flags = view.getUint16(2);
    const opcode = (flags >> 11) & 0x0f;
    const qd = view.getUint16(4);
    const an = view.getUint16(6);
    const ns = view.getUint16(8);
    const ar = view.getUint16(10);
    if ((flags & 0x0040) !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "DNS message has reserved header bits set", true);
    return { view, id, flags, opcode, qd, an, ns, ar };
}
function validateRawDnsQuery(message) {
    const { view, flags, opcode, qd, an, ns, ar } = readRawHeader(message);
    if ((flags & 0x8000) !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "DNS query has response bit set", true);
    if (opcode !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "Unsupported DNS query opcode", true);
    if (qd !== 1)
        throw new errors_1.CoreError("DNS_FAILED", "DNS query must contain exactly one question", true);
    const { question, next } = readRawQuestion(message, view, 12);
    if (question.classCode !== 1)
        throw new errors_1.CoreError("DNS_FAILED", "DNS query class must be IN", true);
    const end = skipRawRecords(message, view, skipRawRecords(message, view, skipRawRecords(message, view, next, an, "answer"), ns, "authority"), ar, "additional");
    if (end !== message.byteLength)
        throw new errors_1.CoreError("DNS_FAILED", "DNS query has trailing bytes", true);
    return question;
}
function validateRawDnsResponse(message, expectedId, expectedQuestion) {
    const { view, id, flags, opcode, qd, an, ns, ar } = readRawHeader(message);
    if (id !== expectedId)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response ID mismatch", true);
    if ((flags & 0x8000) === 0)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response is not a response packet", true);
    if (opcode !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "Unsupported DNS response opcode", true);
    if ((flags & 0x0200) !== 0)
        throw new errors_1.CoreError("DNS_FAILED", "Truncated DNS response", true);
    if (qd !== 1)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response must contain exactly one question", true);
    const { question, next } = readRawQuestion(message, view, 12);
    if (question.name !== expectedQuestion.name || question.type !== expectedQuestion.type || question.classCode !== expectedQuestion.classCode) {
        throw new errors_1.CoreError("DNS_FAILED", "DNS response question does not match the request", true);
    }
    const afterAnswers = skipRawRecords(message, view, next, an, "answer");
    const afterAuthority = skipRawRecords(message, view, afterAnswers, ns, "authority");
    const end = skipRawRecords(message, view, afterAuthority, ar, "additional");
    if (end !== message.byteLength)
        throw new errors_1.CoreError("DNS_FAILED", "DNS response has trailing bytes", true);
    return { id, opcode, rcode: flags & 0x000f, question, answerCount: an, authorityCount: ns, additionalCount: ar };
}
class DoHResolver {
    options;
    q = 0;
    s = 0;
    f = 0;
    endpoint;
    lastFailure;
    constructor(options) {
        this.options = options;
        this.endpoint = normalizeEndpoint(options.endpoint);
    }
    diagnostics() {
        return {
            type: "doh",
            endpoint: this.endpoint,
            queries: this.q,
            successes: this.s,
            failures: this.f,
            ...(this.lastFailure ? { lastFailure: this.lastFailure } : {}),
        };
    }
    recordFailure(phase, error, attempt, status) {
        const message = error instanceof Error ? error.message : String(error);
        this.lastFailure = {
            phase,
            message: message.slice(0, 512),
            ...(status === undefined ? {} : { status }),
            attempt,
            at: Date.now(),
        };
    }
    async queryRaw(message, signal) {
        if (message.byteLength < 1 || message.byteLength > limits_1.CORE_LIMITS.maxDoHQueryBytes) {
            this.recordFailure("validation", new errors_1.CoreError("MESSAGE_TOO_LARGE", `DNS message must be 1..${limits_1.CORE_LIMITS.maxDoHQueryBytes} bytes`), 0);
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DNS message must be 1..${limits_1.CORE_LIMITS.maxDoHQueryBytes} bytes`);
        }
        let expectedQuestion;
        try {
            expectedQuestion = validateRawDnsQuery(message);
        }
        catch (error) {
            const wrapped = error instanceof errors_1.CoreError ? error : new errors_1.CoreError("DNS_FAILED", "Invalid DNS query", true, { cause: error });
            this.recordFailure("validation", wrapped, 0);
            throw wrapped;
        }
        let lastError;
        for (let attempt = 0; attempt <= this.options.retries; attempt++) {
            if (attempt > 0)
                await retryBackoff(attempt, signal);
            this.q++;
            try {
                const controller = new AbortController();
                let timedOut = false;
                const timeoutError = new errors_1.CoreError("TIMEOUT", "DNS query timeout", true);
                const timer = setTimeout(() => { timedOut = true; controller.abort(timeoutError); }, this.options.timeoutMs);
                const onAbort = () => controller.abort(signal.reason ?? new DOMException("Aborted", "AbortError"));
                signal.addEventListener("abort", onAbort, { once: true });
                try {
                    let response;
                    try {
                        response = await (this.options.fetchImpl ?? fetch)(this.endpoint, {
                            method: "POST",
                            headers: { "content-type": "application/dns-message", "accept": "application/dns-message" },
                            body: message,
                            redirect: "manual",
                            cache: "no-store",
                            signal: controller.signal,
                        });
                    }
                    catch (error) {
                        const wrapped = timedOut ? timeoutError : error;
                        this.recordFailure(timedOut ? "timeout" : "fetch", wrapped, attempt);
                        throw wrapped;
                    }
                    if (!response.ok) {
                        const error = new errors_1.CoreError("DNS_FAILED", `DoH resolver returned HTTP ${response.status}`, true);
                        this.recordFailure("http", error, attempt, response.status);
                        throw error;
                    }
                    let bytes;
                    try {
                        bytes = await readCappedBody(response, limits_1.CORE_LIMITS.maxDoHResponseBytes);
                    }
                    catch (error) {
                        this.recordFailure("body", error, attempt);
                        throw error;
                    }
                    if (bytes.byteLength < 12) {
                        const error = new errors_1.CoreError("DNS_FAILED", "DoH response is not a DNS message", true);
                        this.recordFailure("validation", error, attempt);
                        throw error;
                    }
                    try {
                        validateRawDnsResponse(bytes, ((message[0] << 8) | message[1]) >>> 0, expectedQuestion);
                    }
                    catch (error) {
                        const wrapped = error instanceof errors_1.CoreError ? error : new errors_1.CoreError("DNS_FAILED", "Malformed DoH DNS response", true, { cause: error });
                        this.recordFailure("validation", wrapped, attempt);
                        throw wrapped;
                    }
                    this.s++;
                    return bytes;
                }
                finally {
                    clearTimeout(timer);
                    signal.removeEventListener("abort", onAbort);
                }
            }
            catch (error) {
                lastError = error;
                if (signal.aborted)
                    throw signal.reason;
                if (error instanceof errors_1.CoreError && (error.code === "MESSAGE_TOO_LARGE" || error.code === "CONFIGURATION_ERROR"))
                    throw error;
            }
        }
        this.f++;
        throw new errors_1.CoreError("DNS_FAILED", "DoH raw DNS query failed", true, { cause: lastError });
    }
    async resolve(host, strategy, signal) {
        this.lastFailure = undefined;
        const clean = (0, hostname_1.toAsciiHostname)(host).replace(/\.$/, "").toLowerCase();
        if (!clean)
            throw new errors_1.CoreError("DNS_FAILED", "DNS hostname is empty");
        if (strategy === "ipv4")
            return this.queryChain(clean, "A", signal);
        if (strategy === "ipv6")
            return this.queryChain(clean, "AAAA", signal);
        if (strategy === "prefer_ipv4") {
            try {
                const a = await this.queryChain(clean, "A", signal);
                if (a.length)
                    return a;
            }
            catch {
                if (signal.aborted)
                    throw signal.reason;
            }
            return this.queryChain(clean, "AAAA", signal);
        }
        if (strategy === "prefer_ipv6") {
            try {
                const aaaa = await this.queryChain(clean, "AAAA", signal);
                if (aaaa.length)
                    return aaaa;
            }
            catch {
                if (signal.aborted)
                    throw signal.reason;
            }
            return this.queryChain(clean, "A", signal);
        }
        const [aResult, aaaaResult] = await Promise.allSettled([
            this.queryChain(clean, "A", signal),
            this.queryChain(clean, "AAAA", signal),
        ]);
        if (signal.aborted)
            throw signal.reason;
        const a = aResult.status === "fulfilled" ? aResult.value : [];
        const aaaa = aaaaResult.status === "fulfilled" ? aaaaResult.value : [];
        if (!a.length && !aaaa.length) {
            const reason = aResult.status === "rejected" ? aResult.reason : aaaaResult.status === "rejected" ? aaaaResult.reason : undefined;
            throw new errors_1.CoreError("DNS_FAILED", `DNS auto resolution failed for ${clean}`, true, { cause: reason });
        }
        return [...a, ...aaaa];
    }
    async queryChain(host, type, signal) {
        const visited = new Set();
        let current = host;
        let chainTtl = Number.POSITIVE_INFINITY;
        const output = [];
        for (let depth = 0; depth < 8; depth++) {
            if (visited.has(current))
                throw new errors_1.CoreError("DNS_FAILED", "DNS CNAME loop detected", true);
            visited.add(current);
            const response = await this.query(current, type, signal);
            const wantedType = type === "A" ? TYPE_A : TYPE_AAAA;
            const records = response.answers.filter((item) => item.type === wantedType && item.data && canonicalDnsName(item.name) === current);
            if (records.length) {
                const answerTtl = Math.max(0, Math.min(...records.map((record) => record.ttl)));
                const ttl = Math.max(0, Math.min(chainTtl, answerTtl));
                for (const record of records)
                    output.push({ address: record.data, family: type === "A" ? 4 : 6, ttl });
                this.options.cache?.set(host, type, output, ttl);
                return output;
            }
            const cname = response.answers.find((item) => item.type === TYPE_CNAME && item.data && canonicalDnsName(item.name) === current);
            if (!cname) {
                return [];
            }
            const target = canonicalDnsName(cname.data);
            const sameResponseRecords = response.answers.filter((item) => item.type === wantedType && item.data && canonicalDnsName(item.name) === target);
            if (sameResponseRecords.length) {
                const answerTtl = Math.max(0, Math.min(...sameResponseRecords.map((record) => record.ttl)));
                const ttl = Math.max(0, Math.min(chainTtl, cname.ttl, answerTtl));
                for (const record of sameResponseRecords)
                    output.push({ address: record.data, family: type === "A" ? 4 : 6, ttl });
                this.options.cache?.set(host, type, output, ttl);
                return output;
            }
            chainTtl = Math.min(chainTtl, Math.max(0, cname.ttl));
            current = canonicalDnsName(cname.data);
            if (!current)
                throw new errors_1.CoreError("DNS_FAILED", "DNS response contained an invalid CNAME target", true);
        }
        throw new errors_1.CoreError("DNS_FAILED", "DNS CNAME chain is too long", true);
    }
    async query(host, type, signal) {
        const cached = this.options.cache?.get(host, type);
        if (cached)
            return {
                status: 0,
                answers: cached.map((item) => ({ name: host, type: type === "A" ? TYPE_A : TYPE_AAAA, ttl: item.ttl, data: item.address })),
                authority: [],
            };
        const query = buildQuery(host, type);
        let lastError;
        for (let attempt = 0; attempt <= this.options.retries; attempt++) {
            if (attempt > 0)
                await retryBackoff(attempt, signal);
            this.q++;
            try {
                const controller = new AbortController();
                let timedOut = false;
                const timeoutError = new errors_1.CoreError("TIMEOUT", "DNS query timeout", true);
                const timer = setTimeout(() => { timedOut = true; controller.abort(timeoutError); }, this.options.timeoutMs);
                const onAbort = () => controller.abort(signal.reason ?? new DOMException("Aborted", "AbortError"));
                signal.addEventListener("abort", onAbort, { once: true });
                try {
                    const requestFetch = this.options.fetchImpl ?? fetch;
                    let response;
                    try {
                        response = await requestFetch(this.endpoint, {
                            method: "POST",
                            headers: {
                                "content-type": "application/dns-message",
                                "accept": "application/dns-message",
                            },
                            body: query.bytes,
                            signal: controller.signal,
                            redirect: "manual",
                            cache: "no-store",
                        });
                    }
                    catch (error) {
                        this.recordFailure(timedOut ? "timeout" : "fetch", timedOut ? timeoutError : error, attempt);
                        throw timedOut ? timeoutError : error;
                    }
                    if (!response.ok) {
                        const error = new errors_1.CoreError("DNS_FAILED", `DoH resolver returned HTTP ${response.status}`, true);
                        this.recordFailure("http", error, attempt, response.status);
                        throw error;
                    }
                    const contentLength = Number(response.headers.get("content-length") ?? "0");
                    if (contentLength > limits_1.CORE_LIMITS.maxDoHResponseBytes) {
                        const error = new errors_1.CoreError("MESSAGE_TOO_LARGE", `DoH response exceeds ${limits_1.CORE_LIMITS.maxDoHResponseBytes} bytes`);
                        this.recordFailure("body", error, attempt);
                        throw error;
                    }
                    let bytes;
                    try {
                        bytes = await readCappedBody(response, limits_1.CORE_LIMITS.maxDoHResponseBytes);
                    }
                    catch (error) {
                        this.recordFailure("body", error, attempt);
                        throw error;
                    }
                    let parsed;
                    try {
                        parsed = parseMessage(bytes, query.id, host, type);
                    }
                    catch (error) {
                        this.recordFailure("parse", error, attempt);
                        throw error;
                    }
                    if (parsed.status === 3) {
                        this.options.cache?.setNegative(host, type, negativeCacheTtlSeconds(parsed));
                        this.s++;
                        return parsed;
                    }
                    if (parsed.status !== 0) {
                        const error = new errors_1.CoreError("DNS_FAILED", `DoH resolver returned DNS RCODE ${parsed.status}`, true);
                        this.recordFailure("rcode", error, attempt);
                        throw error;
                    }
                    if (!parsed.answers.length) {
                        this.options.cache?.setNegative(host, type, negativeCacheTtlSeconds(parsed));
                        this.s++;
                        return parsed;
                    }
                    const wantedType = type === "A" ? TYPE_A : TYPE_AAAA;
                    const resolved = parsed.answers
                        .filter((item) => item.type === wantedType && item.data && canonicalDnsName(item.name) === host)
                        .map((item) => ({ address: item.data, family: type === "A" ? 4 : 6, ttl: Math.max(0, item.ttl) }));
                    if (resolved.length) {
                        const ttl = Math.max(0, Math.min(...resolved.map((item) => item.ttl)));
                        this.options.cache?.set(host, type, resolved, ttl);
                    }
                    this.s++;
                    return parsed;
                }
                finally {
                    clearTimeout(timer);
                    signal.removeEventListener("abort", onAbort);
                }
            }
            catch (error) {
                if (error instanceof errors_1.CoreError && (error.code === "MESSAGE_TOO_LARGE" || error.code === "CONFIGURATION_ERROR"))
                    throw error;
                lastError = error;
                if (signal.aborted)
                    throw signal.reason;
            }
        }
        this.f++;
        throw new errors_1.CoreError("DNS_FAILED", `DoH resolution failed for ${host}`, true, { cause: lastError });
    }
}
exports.DoHResolver = DoHResolver;

};

__m["util/hostname"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.normalizeHostname = normalizeHostname;
exports.isCanonicalIpv4 = isCanonicalIpv4;
exports.isAsciiHostname = isAsciiHostname;
exports.toAsciiHostname = toAsciiHostname;
function normalizeHostname(value) {
    let input = value.trim().toLowerCase();
    if (input.startsWith("[") && input.endsWith("]"))
        input = input.slice(1, -1);
    input = input.replace(/\.$/, "");
    return input;
}
/** Strict dotted-quad check: four decimal octets, no leading zeros, each 0-255. */
function isCanonicalIpv4(value) {
    const labels = value.split(".");
    return labels.length === 4 && labels.every((label) => /^\d{1,3}$/.test(label) && String(Number(label)) === label && Number(label) <= 255);
}
function isAsciiHostname(value) {
    const normalized = normalizeHostname(value);
    if (!normalized || normalized.length > 253)
        return false;
    if (normalized.includes(":") || normalized.includes("/"))
        return false;
    const labels = normalized.split(".");
    if (labels.some((label) => !label || label.length > 63 || !/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/i.test(label)))
        return false;
    // A dotted all-numeric string is an IPv4-literal attempt, not a hostname: accept it only
    // as a canonical dotted quad, so "999.1.1.1" or "1.2.3.4.5" cannot pass as hostnames (F18).
    if (labels.length >= 2 && labels.every((label) => /^\d+$/.test(label)))
        return isCanonicalIpv4(normalized);
    return true;
}
function isValidIpv6Literal(value) {
    const input = value.startsWith("[") && value.endsWith("]") ? value.slice(1, -1) : value;
    if (!input.includes(":"))
        return false;
    const halves = input.split("::");
    if (halves.length > 2)
        return false;
    const countSide = (side) => {
        if (!side)
            return 0;
        let count = 0;
        for (const part of side.split(":")) {
            if (part.includes(".")) {
                const octets = part.split(".");
                if (octets.length !== 4 || octets.some((x) => !/^\d{1,3}$/.test(x) || Number(x) > 255))
                    return null;
                count += 2;
            }
            else {
                if (!/^[0-9a-f]{1,4}$/i.test(part))
                    return null;
                count += 1;
            }
        }
        return count;
    };
    const left = countSide(halves[0]);
    const right = countSide(halves[1] ?? "");
    if (left === null || right === null)
        return false;
    return halves.length === 2 ? left + right < 8 : left === 8;
}
function toAsciiHostname(value) {
    // Protocol destinations must be treated as hostname data, not URL syntax. Reject
    // characters that URL would interpret as path/query/userinfo/port delimiters first;
    // otherwise strings such as "foo/bar" or "user:pass@host" could be truncated into
    // an apparently valid but different destination.
    if (value !== value.trim() || /[\u0000-\u0020\u007f\\/?#@]/.test(value))
        return "";
    const normalized = normalizeHostname(value);
    if (!normalized)
        return "";
    if (normalized.includes(":"))
        return isValidIpv6Literal(normalized) ? normalized : "";
    try {
        const ascii = new URL(`http://${normalized}`).hostname.toLowerCase().replace(/\.$/, "");
        if (!ascii || ascii.includes("/") || ascii.includes("?") || ascii.includes("#") || ascii.includes("@") || ascii.includes(":"))
            return "";
        // WHATWG URL canonicalises exotic IPv4 spellings ("0x7f.1", "127.1", "0177.0.0.1") into a
        // dotted quad. Silently returning that literal from a hostname conversion lets a textual
        // destination become a different network destination than the operator/client wrote;
        // accept an IPv4 result only when the input already was that canonical quad (F18).
        if (isCanonicalIpv4(ascii) && normalized !== ascii)
            return "";
        return isAsciiHostname(ascii) ? ascii : "";
    }
    catch {
        return "";
    }
}

};

__m["core/limits"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CORE_LIMITS = exports.CORE_VERSION = void 0;
exports.CORE_VERSION = "0.9.1";
exports.CORE_LIMITS = {
    maxWebSocketBufferedBytes: 8 * 1024 * 1024,
    maxInitialPayloadBytes: 64 * 1024,
    maxWebSocketWriteChunkBytes: 64 * 1024,
    maxEarlyDataBytes: 8192,
    defaultHandshakeTimeoutMs: 10_000,
    defaultEgressConnectTimeoutMs: 10_000,
    defaultProxyIpPort: 1080,
    // Bounded by default (A7): an upstream that never sends a first byte must not
    // hold a session forever. Operators may still opt out explicitly with 0.
    defaultFirstByteTimeoutMs: 30_000,
    maxInitialRetries: 3,
    defaultOutputNodes: 256,
    maxOutputNodes: 512,
    maxOutputBytes: 512 * 1024,
    maxOutputEndpointInputs: 16_384,
    maxOutputCandidateEvaluations: 1_000_000,
    maxFragmentExpansionCandidates: 100_000,
    maxSubscriptionSources: 8,
    maxSubscriptionSourceBytes: 256 * 1024,
    // Bounded by default (A7): no proxied session may live longer than one hour
    // unless the operator explicitly configures a different bound (0 disables).
    maxSessionDurationMs: 60 * 60 * 1000,
    // Cloudflare currently allows six simultaneous outbound connections waiting for headers.
    // Keep the core ceiling below that platform limit so one invocation retains headroom.
    maxConcurrentAddressTries: 4,
    defaultAddressTryStaggerMs: 100,
    defaultProxyIpCooldownMs: 15_000,
    defaultProxyIpHandshakeTimeoutMs: 5_000,
    maxProxyIpEndpoints: 64,
    maxDnsResolvers: 8,
    maxNat64Candidates: 32,
    maxNegativeAuthCacheEntries: 512,
    maxPositiveAuthCacheEntries: 512,
    positiveAuthCacheTtlMs: 1_000,
    negativeAuthCacheTtlMs: 1_000,
    defaultExternalSubscriptionTimeoutMs: 5_000,
    defaultMaxEgressDecisionMs: 30_000,
    maxDoHQueryBytes: 4096,
    maxDoHResponseBytes: 64 * 1024,
    // Soft per-isolate ceiling on concurrent proxied sessions (A8). Queues are
    // allocated before authentication, so an unbounded session count is a
    // pre-auth memory/CPU commitment.
    maxConcurrentSessions: 256,
    // Per-session budgets for the VLESS UDP/53 DNS bridge (A11).
    maxDnsQueriesPerSession: 2048,
    defaultDnsSessionIdleTimeoutMs: 120_000,
};

};

__m["dns/config"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseDohResolverEndpoints = parseDohResolverEndpoints;
exports.normalizeDohResolverEndpoint = normalizeDohResolverEndpoint;
const errors_1 = __r("../core/errors");
const limits_1 = __r("../core/limits");
const constants_1 = __r("./constants");
/**
 * Normalize operator-provided DoH endpoints.
 *
 * Accepted forms:
 *   - https://cloudflare-dns.com/dns-query
 *   - doh:https://dns.google/dns-query
 *   - cloudflare-dns.com
 *   - dns.google
 *   - one.one.one.one (compatibility alias -> cloudflare-dns.com)
 *
 * Bare hostnames receive the standard /dns-query path. Full URLs keep their
 * explicit path. Only HTTPS endpoints are allowed.
 */
function parseDohResolverEndpoints(raw, maxResolvers = limits_1.CORE_LIMITS.maxDnsResolvers) {
    const values = (raw?.trim() ? raw : constants_1.DEFAULT_DOH_RESOLVERS.join(","))
        .split(/[\r\n,]+/)
        .map((value) => value.trim())
        .filter(Boolean);
    const seen = new Set();
    const out = [];
    for (const value of values) {
        const endpoint = normalizeDohResolverEndpoint(value);
        if (!seen.has(endpoint)) {
            seen.add(endpoint);
            out.push(endpoint);
        }
    }
    if (out.length > maxResolvers) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `DNS_RESOLVERS exceeds maximum ${maxResolvers}`);
    }
    return out;
}
function normalizeDohResolverEndpoint(value) {
    const raw = value.replace(/^doh:/i, "").trim();
    if (!raw)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Empty DNS_RESOLVERS endpoint");
    try {
        const looksLikeUrl = /^[a-z][a-z\d+.-]*:\/\//i.test(raw);
        const url = new URL(looksLikeUrl ? raw : `https://${raw}`);
        if (url.protocol !== "https:" || url.username || url.password || url.hash) {
            throw new Error("invalid DoH URL");
        }
        // Canonicalize the long-standing Cloudflare alias to the currently
        // documented DoH hostname while preserving the strict no-redirect policy.
        if (url.hostname.toLowerCase() === "one.one.one.one") {
            url.hostname = "cloudflare-dns.com";
        }
        if (!looksLikeUrl || url.pathname === "/" || url.pathname === "") {
            url.pathname = "/dns-query";
            url.search = "";
        }
        return url.toString();
    }
    catch (error) {
        if (error instanceof errors_1.CoreError)
            throw error;
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid DNS_RESOLVERS endpoint: ${raw}`);
    }
}

};

__m["dns/constants"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_DOH_RESOLVERS = void 0;
exports.DEFAULT_DOH_RESOLVERS = [
    "https://cloudflare-dns.com/dns-query",
    "https://dns.google/dns-query",
];

};

__m["tests/runner"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runSelfTests = runSelfTests;
const byte_queue_1 = __r("../core/byte-queue");
const engine_1 = __r("../core/engine");
const errors_1 = __r("../core/errors");
const parser_1 = __r("../protocol/vless/parser");
const parser_2 = __r("../protocol/trojan/parser");
const sha224_1 = __r("../util/sha224");
const binary_1 = __r("../util/binary");
const direct_1 = __r("../egress/direct");
const relay_1 = __r("../egress/relay");
const registry_1 = __r("../egress/registry");
const proxyip_1 = __r("../egress/proxyip");
const router_1 = __r("../routing/router");
const cloudflare_1 = __r("../storage/cloudflare");
const share_link_1 = __r("../output/share-link");
const endpoints_1 = __r("../output/endpoints");
const bundle_1 = __r("../output/bundle");
const render_1 = __r("../output/render");
const cache_1 = __r("../dns/cache");
const hosts_1 = __r("../dns/hosts");
const doh_1 = __r("../dns/doh");
const engine_2 = __r("../dns/engine");
const config_1 = __r("../dns/config");
const doh_2 = __r("../dns/doh");
const rules_1 = __r("../routing/rules");
const validation_1 = __r("../config/validation");
const adapter_1 = __r("../transport/websocket/adapter");
const bridge_1 = __r("../connection/bridge");
const dns_udp_1 = __r("../connection/dns-udp");
const nat64_1 = __r("../egress/nat64");
const xray_1 = __r("../output/xray");
const singbox_1 = __r("../output/singbox");
const clash_1 = __r("../output/clash");
const bundle_2 = __r("../output/bundle");
const cloudflare_2 = __r("../output/edge/cloudflare");
const compatibility_1 = __r("../output/compatibility");
const artifacts_1 = __r("../output/artifacts");
const aggregation_1 = __r("../output/aggregation");
const profiles_1 = __r("../output/profiles");
const cloudflare_3 = __r("../storage/cloudflare");
const metrics_1 = __r("../observability/metrics");
const service_1 = __r("../output/service");
const limits_1 = __r("../core/limits");
const doh_3 = __r("../handlers/doh");
const lifecycle_1 = __r("../util/lifecycle");
const hostname_1 = __r("../util/hostname");
const byte_writer_1 = __r("../transport/websocket/byte-writer");
const runtime_1 = __r("../runtime/runtime");
class StaticProvider {
    protocol;
    credential;
    constructor(protocol, credential) {
        this.protocol = protocol;
        this.credential = credential;
    }
    async authenticate(protocol, credential) {
        return protocol === this.protocol && (0, binary_1.equalBytes)(credential, this.credential) ? { id: "self-test", protocol } : null;
    }
}
function ctx(signal) {
    return { id: "self-test", startedAt: Date.now(), signal: signal ?? new AbortController().signal };
}
function normalizeIpv6(value) {
    const input = value.replace(/^\[|\]$/g, "").toLowerCase();
    const parts = input.split("::");
    if (parts.length > 2)
        return input;
    const left = parts[0] ? parts[0].split(":") : [];
    const right = parts.length === 2 && parts[1] ? parts[1].split(":") : [];
    const groups = parts.length === 2 ? [...left, ...new Array(8 - left.length - right.length).fill("0"), ...right] : left;
    return groups.map((x) => Number.parseInt(x || "0", 16).toString(16)).join(":");
}
async function runOne(id, name, group, fn, timeoutMs = 5000) {
    const started = performance.now();
    const controller = new AbortController();
    let timer;
    try {
        const timeout = new Promise((_, reject) => {
            timer = setTimeout(() => {
                controller.abort(new errors_1.CoreError("TIMEOUT", `Test timeout after ${timeoutMs}ms`, true));
                reject(new Error(`Test timeout after ${timeoutMs}ms`));
            }, timeoutMs);
        });
        const details = await Promise.race([fn(controller.signal), timeout]);
        return { id, name, group, status: "PASS", durationMs: rounded(performance.now() - started), ...(details ? { details } : {}) };
    }
    catch (error) {
        return { id, name, group, status: "FAIL", durationMs: rounded(performance.now() - started), error: error instanceof Error ? error.message : String(error) };
    }
    finally {
        if (timer)
            clearTimeout(timer);
    }
}
async function envFlag(env, name, id, testName, group, fn, timeoutMs = 5000) {
    const raw = env[name];
    const enabled = /^(1|true|yes|on)$/i.test(raw?.trim() ?? "");
    return enabled ? runOne(id, testName, group, fn, timeoutMs) : Promise.resolve(skipped(id, testName, group, `${name} is not enabled`));
}
function rounded(value) {
    return Math.round(value * 100) / 100;
}
function skipped(id, name, group, reason) {
    return { id, name, group, status: "SKIPPED", durationMs: 0, details: { reason } };
}
async function runSelfTests(env) {
    const suiteStarted = performance.now();
    const tests = [];
    tests.push(await runOne("core.byte-queue", "ByteQueue fragmented reads preserve byte order", "core", async () => {
        const q = new byte_queue_1.ByteQueue(1024);
        q.push(new Uint8Array([1, 2]));
        q.push(new Uint8Array([3, 4, 5]));
        const first = await q.readExact(4);
        const rest = q.readAvailable();
        if (first.join(",") !== "1,2,3,4" || !rest || rest.join(",") !== "5")
            throw new Error("ByteQueue data mismatch");
    }));
    tests.push(await runOne("core.byte-queue-abort", "ByteQueue read aborts cleanly", "core", async () => {
        const q = new byte_queue_1.ByteQueue(1024);
        const controller = new AbortController();
        const pending = q.readExact(1, controller.signal).then(() => false, () => true);
        controller.abort();
        if (!(await pending))
            throw new Error("Abort did not reject read");
    }));
    tests.push(await runOne("core.byte-queue-limit", "ByteQueue enforces memory bound", "core", async () => {
        const q = new byte_queue_1.ByteQueue(4);
        try {
            q.push(new Uint8Array(5));
        }
        catch (error) {
            if (!(error instanceof errors_1.CoreError) || error.code !== "MESSAGE_TOO_LARGE")
                throw error;
            return;
        }
        throw new Error("Overflow was accepted");
    }));
    tests.push(await runOne("core.hard-timeout", "Hard operation timeout returns even when the factory ignores AbortSignal", "core", async (signal) => {
        const started = performance.now();
        let late = false;
        let rejected = false;
        try {
            await (0, lifecycle_1.withTimeout)(async () => new Promise((resolve) => setTimeout(() => { late = true; resolve("late"); }, 150)), 25, signal, "hard timeout");
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "TIMEOUT";
        }
        if (!rejected)
            throw new Error("Non-cooperative timeout did not reject with TIMEOUT");
        if (performance.now() - started > 100)
            throw new Error("Hard timeout did not return promptly");
        await new Promise((resolve) => setTimeout(resolve, 170));
        if (!late)
            throw new Error("Late operation did not settle as expected");
    }));
    tests.push(await runOne("core.abortable-dial-race", "Aborted dial race closes a connection that resolves after the deadline", "core", async () => {
        const controller = new AbortController();
        let closed = false;
        const lateConnection = { close: async () => { closed = true; } };
        const pending = new Promise((resolve) => setTimeout(() => resolve(lateConnection), 40));
        const timeout = new errors_1.CoreError("TIMEOUT", "dial decision timeout", true);
        const raced = (0, lifecycle_1.raceWithAbort)(pending, controller.signal, async (connection) => connection.close());
        controller.abort(timeout);
        let rejected = false;
        try {
            await raced;
        }
        catch (error) {
            rejected = error === timeout;
        }
        if (!rejected)
            throw new Error("Aborted dial race did not reject with deadline reason");
        await new Promise((resolve) => setTimeout(resolve, 60));
        if (!closed)
            throw new Error("Late dial result was not closed");
    }));
    tests.push(await runOne("core.websocket-writer-limit", "WebSocket writer rejects oversized writes before sending", "resource", async () => {
        let sends = 0;
        const fakeSocket = { readyState: 1, send: () => { sends++; } };
        const writer = new byte_writer_1.WebSocketByteWriter(fakeSocket, 4, 8);
        let rejected = false;
        try {
            await writer.write(new Uint8Array(9));
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "MESSAGE_TOO_LARGE";
        }
        if (!rejected || sends !== 0)
            throw new Error("Oversized WebSocket write was not rejected before send");
    }));
    tests.push(await runOne("core.sha224", "SHA-224 known vector", "core", async () => {
        const actual = (0, sha224_1.sha224Hex)(new TextEncoder().encode("abc"));
        const expected = "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7";
        if (actual !== expected)
            throw new Error(`SHA-224 mismatch: ${actual}`);
    }));
    tests.push(await runOne("core.error-close-codes", "Core error mapping uses protocol-appropriate WebSocket close codes", "core", async () => {
        if ((0, errors_1.closeCodeForError)(new errors_1.CoreError("MESSAGE_TOO_LARGE", "too large")) !== 1009)
            throw new Error("MESSAGE_TOO_LARGE must map to WebSocket 1009");
        if ((0, errors_1.closeCodeForError)(new errors_1.CoreError("AUTH_FAILED", "auth")) !== 1008)
            throw new Error("AUTH_FAILED must map to WebSocket 1008");
        if ((0, errors_1.closeCodeForError)(new errors_1.CoreError("BAD_REQUEST", "bad")) !== 1002)
            throw new Error("BAD_REQUEST must map to WebSocket 1002");
        if ((0, errors_1.closeCodeForError)(new errors_1.CoreError("INVALID_DESTINATION", "destination")) !== 1002)
            throw new Error("INVALID_DESTINATION must map to WebSocket 1002");
    }));
    const vlessUuid = "c6473900-2605-44db-b896-9e11976c8709";
    const vlessBytes = (0, binary_1.uuidToBytes)(vlessUuid);
    tests.push(await runOne("protocol.vless", "VLESS TCP/domain/initial-payload/response", "protocol", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        const domain = new TextEncoder().encode("example.com");
        const packet = (0, binary_1.concatBytes)(new Uint8Array([0x00]), vlessBytes, new Uint8Array([0x00]), new Uint8Array([0x01]), new Uint8Array([0x01, 0xbb]), new Uint8Array([0x02, domain.length]), domain, new TextEncoder().encode("hello"));
        reader.push(packet.subarray(0, 9));
        reader.push(packet.subarray(9));
        const session = await new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes)).parse(reader, ctx(signal), signal);
        if (session.destination.host !== "example.com" || session.destination.port !== 443)
            throw new Error("Destination mismatch");
        if (new TextDecoder().decode(session.initialPayload) !== "hello")
            throw new Error("Initial payload mismatch");
        if (!(0, binary_1.equalBytes)(session.responseHeader ?? new Uint8Array(), new Uint8Array([0, 0])))
            throw new Error("Response header mismatch");
    }));
    tests.push(await runOne("protocol.vless-auth-failure", "VLESS rejects invalid credentials", "protocol", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        const domain = new TextEncoder().encode("example.com");
        reader.push((0, binary_1.concatBytes)(new Uint8Array([0]), new Uint8Array(16), new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([1, 80]), new Uint8Array([2, domain.length]), domain));
        try {
            await new parser_1.VlessHandler(new StaticProvider("vless", (0, binary_1.uuidToBytes)(vlessUuid))).parse(reader, ctx(signal), signal);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "AUTH_FAILED")
                return;
            throw error;
        }
        throw new Error("Invalid VLESS credential was accepted");
    }));
    tests.push(await runOne("protocol.vless-malformed", "VLESS rejects unsupported command", "protocol", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        reader.push((0, binary_1.concatBytes)(new Uint8Array([0]), vlessBytes, new Uint8Array([0]), new Uint8Array([3])));
        try {
            await new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes)).parse(reader, ctx(signal), signal);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "BAD_REQUEST")
                return;
            throw error;
        }
        throw new Error("Unsupported command was accepted");
    }));
    const password = "password";
    const passwordDigest = (0, sha224_1.sha224Hex)(new TextEncoder().encode(password));
    tests.push(await runOne("protocol.vless-dns-udp", "VLESS UDP is accepted only for DNS port 53 and marks the session for DoH bridging", "protocol", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        const host = new TextEncoder().encode("cloudflare-dns.com");
        reader.push((0, binary_1.concatBytes)(new Uint8Array([0]), vlessBytes, new Uint8Array([0]), new Uint8Array([2]), new Uint8Array([0, 53]), new Uint8Array([2, host.length]), host, new Uint8Array([0, 4]), new Uint8Array([0, 1, 0, 2])));
        const session = await new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes)).parse(reader, ctx(signal), signal);
        if (!session.udpDns || session.destination.port !== 53)
            throw new Error("VLESS DNS UDP session was not marked correctly");
    }));
    tests.push(await runOne("protocol.trojan", "Trojan framing/auth/domain/initial-payload", "protocol", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        const domain = new TextEncoder().encode("example.com");
        const packet = (0, binary_1.concatBytes)(new TextEncoder().encode(passwordDigest), new Uint8Array([13, 10]), new Uint8Array([1, 3, domain.length]), domain, new Uint8Array([1, 187]), new Uint8Array([13, 10]), new TextEncoder().encode("hello"));
        reader.push(packet.subarray(0, 30));
        reader.push(packet.subarray(30));
        const session = await new parser_2.TrojanHandler(new StaticProvider("trojan", new TextEncoder().encode(passwordDigest))).parse(reader, ctx(signal), signal);
        if (session.destination.host !== "example.com" || session.destination.port !== 443)
            throw new Error("Destination mismatch");
        if (new TextDecoder().decode(session.initialPayload) !== "hello")
            throw new Error("Initial payload mismatch");
    }));
    tests.push(await runOne("protocol.trojan-auth-failure", "Trojan rejects malformed/non-matching authentication", "protocol", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        reader.push((0, binary_1.concatBytes)(new TextEncoder().encode("not-a-valid-trojan-digest........................................".slice(0, 56)), new Uint8Array([13, 10])));
        try {
            await new parser_2.TrojanHandler(new StaticProvider("trojan", new TextEncoder().encode(passwordDigest))).parse(reader, ctx(signal), signal);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "AUTH_FAILED")
                return;
            throw error;
        }
        throw new Error("Malformed Trojan credential was accepted");
    }));
    tests.push(await runOne("protocol.hostname-integrity", "Protocol destination hostname canonicalization never truncates URL syntax", "protocol", async () => {
        for (const bad of ["foo?bar", "foo/bar", "foo\\bar", "user:pass@bar", " foo.example.com", "foo.example.com "]) {
            if ((0, hostname_1.toAsciiHostname)(bad))
                throw new Error(`Malformed hostname was canonicalized instead of rejected: ${bad}`);
        }
        if ((0, hostname_1.toAsciiHostname)("bücher.example") !== "xn--bcher-kva.example")
            throw new Error("IDNA hostname conversion failed");
        if ((0, hostname_1.toAsciiHostname)("2001:db8::1") !== "2001:db8::1")
            throw new Error("IPv6 hostname literal was rejected");
    }));
    tests.push(await runOne("routing.modes", "Routing modes produce expected egress decisions", "routing", async (signal) => {
        const context = { destination: { host: "example.com", port: 443 }, principal: { id: "x", protocol: "vless" }, protocol: "vless" };
        const direct = await new router_1.DefaultRouter("direct").route(context, signal);
        const proxy = await new router_1.DefaultRouter("proxyip").route(context, signal);
        const fallback = await new router_1.DefaultRouter("fallback").route(context, signal);
        const smart = await new router_1.DefaultRouter("smart", undefined, { proxyip: true, relay: true, nat64: true }).route(context, signal);
        if (direct.egress !== "direct" || proxy.egress !== "proxyip" || fallback.fallbackEgress !== "proxyip")
            throw new Error("Routing decision mismatch");
        if (smart.egress !== "direct" || smart.fallbackEgresses?.join(",") !== "proxyip,relay,nat64")
            throw new Error("Smart egress ordering mismatch");
        if ((0, router_1.parseEgressMode)("DIRECT") !== "direct" || (0, router_1.parseEgressMode)(undefined) !== "smart")
            throw new Error("Egress mode parser mismatch");
        if ((0, relay_1.parseRelayEndpoints)("relay.example.com:443,[2001:db8::1]:8443,relay.example.com:443").length !== 2)
            throw new Error("Relay endpoint parser/dedupe mismatch");
    }));
    tests.push(await runOne("storage.trojan-normalization", "Environment credentials normalize Trojan digest correctly", "storage", async (signal) => {
        const provider = new cloudflare_1.EnvironmentCredentialProvider({ TROJAN_USERS: password });
        const principal = await provider.authenticate("trojan", new TextEncoder().encode(passwordDigest.toUpperCase()), signal);
        if (!principal)
            throw new Error("Normalized Trojan digest was not accepted");
    }));
    tests.push(await runOne("storage.d1-normalization", "D1 credentials use normalized wire credential", "storage", async (signal) => {
        let bound = [];
        const db = {
            prepare: () => ({
                // ensureSchema() runs statements via prepare(...).run() (C8 removed
                // the silent skip for a missing run()), so the double must expose it.
                run: async () => ({}),
                bind: (...values) => {
                    bound = values;
                    return {
                        first: async () => ({ id: "db-user", protocol: "trojan", credential: passwordDigest, enabled: 1 }),
                        all: async () => ({ results: [] }),
                    };
                },
            }),
        };
        const provider = new cloudflare_1.D1CredentialProvider(db);
        const result = await provider.authenticate("trojan", new TextEncoder().encode(passwordDigest.toUpperCase()), signal);
        if (!result || result.id !== "db-user")
            throw new Error("D1 credential lookup failed");
        if (bound[0] !== "trojan" || bound[1] !== passwordDigest)
            throw new Error(`D1 bound credential mismatch: ${JSON.stringify(bound)}`);
    }));
    tests.push(await runOne("storage.d1-auto-schema", "D1 provider lazily bootstraps its idempotent schema", "storage", async (signal) => {
        const statements = [];
        const db = {
            prepare: (sql) => {
                statements.push(sql);
                return {
                    bind: (..._values) => ({ first: async () => ({ id: "db-user", protocol: "vless", credential: vlessUuid, enabled: 1, created_at: 1 }), all: async () => ({ results: [] }), run: async () => ({}) }),
                    first: async () => ({ id: "db-user", protocol: "vless", credential: vlessUuid, enabled: 1, created_at: 1 }),
                    all: async () => ({ results: [] }),
                    run: async () => ({}),
                };
            },
        };
        const provider = new cloudflare_1.D1CredentialProvider(db);
        const result = await provider.authenticate("vless", (0, binary_1.uuidToBytes)(vlessUuid), signal);
        if (!result)
            throw new Error("D1 authentication failed after lazy schema bootstrap");
        if (!statements.some((sql) => /CREATE TABLE IF NOT EXISTS proxy_users/i.test(sql)) || !statements.some((sql) => /CREATE INDEX IF NOT EXISTS idx_proxy_users_order/i.test(sql))) {
            throw new Error("D1 provider did not bootstrap table/index schema");
        }
    }));
    tests.push(await runOne("storage.d1-negative-cache-bound", "D1 negative authentication cache remains bounded", "storage", async (signal) => {
        const db = {
            prepare: (_sql) => ({
                bind: (..._values) => ({ first: async () => null, all: async () => ({ results: [] }) }),
                first: async () => null,
                all: async () => ({ results: [] }),
                run: async () => ({}),
            }),
        };
        const provider = new cloudflare_1.D1CredentialProvider(db);
        for (let i = 0; i < limits_1.CORE_LIMITS.maxNegativeAuthCacheEntries + 88; i++) {
            const digest = (0, sha224_1.sha224Hex)(new TextEncoder().encode(`negative-${i}`));
            await provider.authenticate("trojan", new TextEncoder().encode(digest), signal);
        }
        const cache = provider.negativeCache;
        if (cache.size > limits_1.CORE_LIMITS.maxNegativeAuthCacheEntries)
            throw new Error(`Negative cache exceeded bound: ${cache.size}`);
    }));
    tests.push(await runOne("storage.d1-positive-cache", "D1 successful authentication is cached briefly and remains bounded", "storage", async (signal) => {
        let queries = 0;
        const db = {
            prepare: (_sql) => ({
                bind: (..._values) => ({
                    first: async () => { queries++; return { id: "db-user", protocol: "vless", credential: vlessUuid, enabled: 1, created_at: 1 }; },
                    all: async () => ({ results: [] }),
                }),
                run: async () => ({}),
            }),
        };
        const provider = new cloudflare_1.D1CredentialProvider(db);
        const testSignal = signal;
        const first = await provider.authenticate("vless", (0, binary_1.uuidToBytes)(vlessUuid), testSignal);
        const second = await provider.authenticate("vless", (0, binary_1.uuidToBytes)(vlessUuid), testSignal);
        if (!first || !second || first.id !== second.id || queries !== 1)
            throw new Error(`Positive auth cache miss: queries=${queries}`);
        const cache = provider.positiveCache;
        if (cache.size > limits_1.CORE_LIMITS.maxPositiveAuthCacheEntries)
            throw new Error(`Positive cache exceeded bound: ${cache.size}`);
    }));
    tests.push(await runOne("storage.kv-ttl-validation", "KV adapter enforces Cloudflare expirationTtl minimum", "storage", async () => {
        const writes = [];
        const kv = {
            get: async () => null,
            put: async (key, value, options) => { writes.push({ key, value, options }); },
            delete: async () => { },
        };
        const store = new cloudflare_1.KVJsonStore(kv);
        let rejected = false;
        try {
            await store.put("k", { ok: true }, 59);
        }
        catch (error) {
            if (!(error instanceof RangeError))
                throw error;
            rejected = true;
        }
        if (!rejected || writes.length !== 0)
            throw new Error("Invalid KV TTL was accepted");
        await store.put("k", { ok: true }, 60);
        if (writes[0]?.options?.expirationTtl !== 60)
            throw new Error("Valid KV TTL was not forwarded");
    }));
    tests.push(await runOne("output.share-link", "Share link contains native WS/TLS parameters", "output", async () => {
        const link = (0, share_link_1.generateShareLink)({
            protocol: "vless", server: "example.com", port: 443, credential: vlessUuid, name: "test",
            security: "tls", scheme: "https", endpointKind: "domain", tls: { serverName: "example.com" }, websocket: { path: "/vless", earlyData: 2048 },
        });
        const parsed = new URL(link);
        if (parsed.protocol !== "vless:" || parsed.hostname !== "example.com" || parsed.searchParams.get("type") !== "ws" || parsed.searchParams.get("security") !== "tls")
            throw new Error("Share link mismatch");
        if (!parsed.searchParams.get("path")?.includes("ed=2048"))
            throw new Error("Early-data path marker missing");
    }));
    tests.push(await runOne("output.port-security", "TLS-only fields are emitted only on TLS ports and custom ports require explicit classification", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443,80", TLS_FINGERPRINT: "chrome", TLS_ALPN: "http/1.1" };
        (0, validation_1.validateEnvironment)(env);
        const selection = (0, bundle_1.parseOutputSelection)(env, new URLSearchParams("protocol=vless"));
        const lines = (0, bundle_1.generateNativeUris)(env, "proxy.example.com", selection);
        const tls = new URL(lines.find((line) => line.includes(":443?")));
        const plain = new URL(lines.find((line) => line.includes(":80?")));
        if (tls.searchParams.get("security") !== "tls" || !tls.searchParams.get("sni") || !tls.searchParams.get("fp") || !tls.searchParams.get("alpn"))
            throw new Error("TLS fields were not emitted for TLS port");
        if (plain.searchParams.get("security") !== "none" || plain.searchParams.get("sni") || plain.searchParams.get("fp") || plain.searchParams.get("alpn"))
            throw new Error("TLS fields leaked into HTTP output");
        let rejected = false;
        try {
            (0, validation_1.validateEnvironment)({ ...env, OUTPUT_PORT: "12345" });
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejected)
            throw new Error("Unclassified custom output port was accepted");
        let rejectedOverride = false;
        try {
            (0, validation_1.validateEnvironment)({ ...env, OUTPUT_PORT: "12345", OUTPUT_TLS_PORTS: "12345" });
        }
        catch (error) {
            rejectedOverride = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejectedOverride)
            throw new Error("Legacy explicit TLS port override bypassed the Cloudflare port registry");
    }));
    tests.push(await runOne("output.websocket-alpn", "WebSocket output constrains ALPN to HTTP/1.1", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443" };
        let rejected = false;
        try {
            (0, bundle_1.parseOutputSelection)(env, new URLSearchParams("protocol=vless&alpn=h2"));
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "BAD_REQUEST";
        }
        if (!rejected)
            throw new Error("HTTP/2 ALPN was incorrectly accepted for WebSocket output");
        const selection = (0, bundle_1.parseOutputSelection)(env, new URLSearchParams("protocol=vless&alpn=http/1.1"));
        if (selection.alpn.join(",") !== "http/1.1")
            throw new Error("HTTP/1.1 ALPN was not preserved");
    }));
    tests.push(await runOne("output.cloudflare-port-registry", "Cloudflare default port registry is exhaustive and security cannot be inverted by legacy overrides", "output", async () => {
        if (cloudflare_2.CLOUDFLARE_HTTP_PORTS.length !== 7 || cloudflare_2.CLOUDFLARE_HTTPS_PORTS.length !== 6)
            throw new Error("Cloudflare port registry size mismatch");
        for (const port of cloudflare_2.CLOUDFLARE_HTTP_PORTS) {
            const profile = (0, cloudflare_2.cloudflarePortProfile)(port);
            if (profile.scheme !== "http" || profile.security !== "none" || !profile.websocket)
                throw new Error(`HTTP profile mismatch for ${port}`);
        }
        for (const port of cloudflare_2.CLOUDFLARE_HTTPS_PORTS) {
            const profile = (0, cloudflare_2.cloudflarePortProfile)(port);
            if (profile.scheme !== "https" || profile.security !== "tls" || !profile.websocket)
                throw new Error(`HTTPS profile mismatch for ${port}`);
        }
        const env = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443,80", OUTPUT_TLS_PORTS: "80", OUTPUT_HTTP_PORTS: "443" };
        let rejected = false;
        try {
            (0, validation_1.validateEnvironment)(env);
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejected)
            throw new Error("Legacy security lists inverted Cloudflare semantics");
    }));
    tests.push(await runOne("output.target-compatibility", "Target adapters enforce client-core compatibility without removing Worker-side plaintext capability", "output", async () => {
        if (!(0, compatibility_1.supportsOutputCombination)("native", "vless", "none") || !(0, compatibility_1.supportsOutputCombination)("native", "trojan", "none"))
            throw new Error("Native plaintext support is missing");
        if ((0, compatibility_1.supportsOutputCombination)("xray", "vless", "none") || (0, compatibility_1.supportsOutputCombination)("xray", "trojan", "none"))
            throw new Error("Xray public plaintext was incorrectly marked compatible");
        if (!(0, compatibility_1.supportsOutputCombination)("sing-box", "trojan", "none"))
            throw new Error("Sing-box plaintext Trojan compatibility was removed");
        if ((0, compatibility_1.supportsOutputCombination)("clash", "trojan", "none"))
            throw new Error("Mihomo plaintext Trojan was incorrectly marked compatible");
    }));
    tests.push(await runOne("config.strict-validation", "Invalid booleans, DoH endpoints and unknown output parameters are rejected", "configuration", async () => {
        const env = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443" };
        let invalidBool = false;
        try {
            (0, validation_1.validateEnvironment)({ ...env, DNS_ENABLED: "maybe" });
        }
        catch (error) {
            invalidBool = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!invalidBool)
            throw new Error("Invalid DNS boolean was accepted");
        let invalidFragmentInterval = false;
        try {
            (0, validation_1.validateEnvironment)({ ...env, CORE_XRAY_FRAGMENT_PROBE_INTERVAL: "0s" });
        }
        catch (error) {
            invalidFragmentInterval = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!invalidFragmentInterval)
            throw new Error("A fragment probe interval below 1s was accepted");
        let invalidFragmentConcurrency = false;
        try {
            (0, validation_1.validateEnvironment)({ ...env, CORE_XRAY_FRAGMENT_OBSERVATORY_CONCURRENCY: "sometimes" });
        }
        catch (error) {
            invalidFragmentConcurrency = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!invalidFragmentConcurrency)
            throw new Error("Invalid fragment Observatory concurrency boolean was accepted");
        let invalidDoh = false;
        try {
            (0, validation_1.validateEnvironment)({ ...env, DNS_ENABLED: "true", DNS_RESOLVERS: "http://dns.example/dns-query" });
        }
        catch (error) {
            invalidDoh = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!invalidDoh)
            throw new Error("Non-HTTPS DoH endpoint was accepted");
        let unknownParameter = false;
        try {
            (0, bundle_1.parseOutputSelection)(env, new URLSearchParams("protocol=vless&bogus=true"));
        }
        catch (error) {
            unknownParameter = error instanceof errors_1.CoreError && error.code === "BAD_REQUEST";
        }
        if (!unknownParameter)
            throw new Error("Unknown subscription parameter was accepted");
    }));
    tests.push(await runOne("dns.nat64-explicit-disable-warning", "Explicitly disabled DNS warns when smart/NAT64 domain fallback cannot resolve names", "configuration", async () => {
        const implicit = new engine_2.DnsEngine({ EGRESS_MODE: "smart" });
        if (!implicit.enabled)
            throw new Error("DNS should default on when EGRESS_MODE=smart and DNS_ENABLED is unset");
        const disabled = new engine_2.DnsEngine({ EGRESS_MODE: "nat64", DNS_ENABLED: "false" });
        const diagnostics = disabled.diagnostics();
        if (disabled.enabled || diagnostics.warnings.length !== 1 || diagnostics.warnings[0]?.code !== "DNS_DISABLED_NAT64_DOMAIN_FALLBACK") {
            throw new Error(`Missing explicit DNS/NAT64 warning: ${JSON.stringify(diagnostics)}`);
        }
        const withHosts = new engine_2.DnsEngine({ EGRESS_MODE: "nat64", DNS_ENABLED: "false", DNS_HOSTS: "example.com=192.0.2.10" });
        if (withHosts.diagnostics().warnings.length !== 1)
            throw new Error("Warning should remain explicit even when some host overrides exist");
    }));
    tests.push(await runOne("output.fragment-range-validation", "Fragment ranges reject unsafe zero lengths for TLSHello and unbounded splits while preserving valid zero delays", "output", async () => {
        const profile = (packets, lengths, delays = ["0"], maxSplit) => ({ id: "custom", packets, lengths, delays, ...(maxSplit === undefined ? {} : { maxSplit }) });
        const get = (value) => (0, profiles_1.getXrayFragmentProfiles)({ CORE_FRAGMENT_PROFILES: JSON.stringify(value) });
        let tlsHelloZeroRejected = false;
        try {
            get([profile("tlshello", ["0-20"])]);
        }
        catch (error) {
            tlsHelloZeroRejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!tlsHelloZeroRejected)
            throw new Error("TLSHello accepted a range that can emit a zero-length TLS record");
        let unboundedRejected = false;
        try {
            get([profile("tlshello", ["1-20"], ["0"], "0")]);
        }
        catch (error) {
            unboundedRejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!unboundedRejected)
            throw new Error("Unlimited maxSplit=0 was accepted despite this generator's finite-split safety policy");
        let largeSplitRejected = false;
        try {
            get([profile("tlshello", ["1-20"], ["0"], "1-65")]);
        }
        catch (error) {
            largeSplitRejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!largeSplitRejected)
            throw new Error("maxSplit above 64 was accepted");
        const valid = get([profile("1-3", ["0-20", "1-20"], ["0-10"], "1-4")]);
        if (valid[0]?.delays[0] !== "0-10")
            throw new Error("A valid zero delay or a non-final idle length range was rejected");
        let finalZeroRejected = false;
        try {
            get([profile("1-3", ["1-20", "0-10"])]);
        }
        catch (error) {
            finalZeroRejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!finalZeroRejected)
            throw new Error("Final stream range that can idle indefinitely was accepted");
        let delayLimitRejected = false;
        try {
            get([profile("1-3", ["1-20"], ["60001"])]);
        }
        catch (error) {
            delayLimitRejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!delayLimitRejected)
            throw new Error("Delay over one minute was accepted");
    }));
    tests.push(await runOne("output.multiple-ports", "OUTPUT_PORT supports multiple ports and expands non-explicit Clean-IP endpoints", "output", async () => {
        const env = {
            VLESS_USERS: vlessUuid,
            OUTPUT_HOST: "proxy.example.com",
            OUTPUT_PORT: "443,8443,2053,2083",
            CLEAN_IPS: "104.21.1.1,104.21.1.2:2083",
        };
        const pool = (0, endpoints_1.buildOutputEndpoints)(env, { requestHost: "ignored.example" });
        if (pool.outputPorts.join(",") !== "443,8443,2053,2083") {
            throw new Error(`Unexpected output ports: ${pool.outputPorts.join(",")}`);
        }
        if (pool.endpoints.length !== 9) {
            throw new Error(`Expected nine endpoints, got ${pool.endpoints.length}`);
        }
        const domainPorts = pool.endpoints
            .filter((item) => item.kind === "domain")
            .map((item) => item.port);
        if (domainPorts.join(",") !== "443,8443,2053,2083") {
            throw new Error(`Unexpected domain ports: ${domainPorts.join(",")}`);
        }
        const expandedCleanPorts = pool.endpoints
            .filter((item) => item.address === "104.21.1.1")
            .map((item) => item.port);
        if (expandedCleanPorts.join(",") !== "443,8443,2053,2083") {
            throw new Error(`Unexpected expanded Clean-IP ports: ${expandedCleanPorts.join(",")}`);
        }
        const explicitClean = pool.endpoints.find((item) => item.address === "104.21.1.2");
        if (!explicitClean || explicitClean.port !== 2083) {
            throw new Error("Explicit Clean-IP port override was not preserved");
        }
    }));
    tests.push(await runOne("output.fingerprint-set", "All documented TLS fingerprint profiles are selectable", "output", async () => {
        const expected = ["chrome", "safari", "firefox", "edge", "360", "qq", "android", "ios", "random", "randomized"];
        const parsed = (0, bundle_1.parseFingerprints)("all");
        if (parsed.join(",") !== expected.join(","))
            throw new Error(`Fingerprint list mismatch: ${parsed.join(",")}`);
        const selected = (0, bundle_1.generateNativeUris)({ VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com" }, "proxy.example.com", (0, bundle_1.parseOutputSelection)({ VLESS_USERS: vlessUuid }, new URL("https://proxy.example.com/?protocol=vless&fingerprint=chrome,randomized").searchParams));
        if (selected.length !== 2 || !selected.some((line) => line.includes("fp=chrome")) || !selected.some((line) => line.includes("fp=randomized")))
            throw new Error("Selected fingerprints were not emitted");
    }));
    tests.push(await runOne("output.alpn-input", "WebSocket ALPN is constrained to HTTP/1.1", "output", async () => {
        let rejected = false;
        try {
            (0, bundle_1.parseAlpn)("h3,h2,http/1.1,h2");
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "BAD_REQUEST";
        }
        if (!rejected)
            throw new Error("Non-HTTP/1.1 ALPN was accepted for WebSocket output");
        const parsed = (0, bundle_1.parseAlpn)("HTTP/1.1,http/1.1");
        if (parsed.join(",") !== "http/1.1")
            throw new Error(`Unexpected canonical ALPN parsing: ${parsed.join(",")}`);
    }));
    tests.push(await runOne("output.native-subscription", "Unified output is Base64 of raw native URI lines", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443,8443", CLEAN_IPS: "104.21.1.1", TLS_FINGERPRINT: "chrome", TLS_ALPN: "http/1.1" };
        const req = new Request("https://proxy.example.com/sub?protocol=all&fingerprint=chrome&remark=TEST-{protocol}-{port}");
        const rendered = await (0, render_1.renderSubscription)(env, req);
        if (!/^[A-Za-z0-9+/=]+$/.test(rendered.body))
            throw new Error("Subscription body is not standard Base64");
        const lines = (0, render_1.decodeStandardBase64)(rendered.body).split("\n");
        if (lines.length !== 8)
            throw new Error(`Expected eight URI lines, got ${lines.length}`);
        if (lines.some((line) => !/^(vless|trojan):\/\//.test(line)))
            throw new Error("Subscription contained non-native output");
        if (lines.some((line) => line.includes("core_version") || line.includes("endpoint")))
            throw new Error("Core metadata leaked into subscription");
    }));
    tests.push(await runOne("output.raw-debug", "Raw output is available only as an explicit developer override", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com" };
        const rendered = await (0, render_1.renderSubscription)(env, new Request("https://proxy.example.com/sub?protocol=vless&encode=raw"));
        if (!rendered.body.startsWith("vless://") || /[^\S\r\n]/.test(rendered.body))
            throw new Error("Raw output was malformed");
    }));
    tests.push(await runOne("output.port-filter", "Subscription filters multiple output ports", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443,8443,2053" };
        const rendered = await (0, render_1.renderSubscription)(env, new Request("https://proxy.example.com/sub?protocol=vless&port=8443,2053"));
        const lines = (0, render_1.decodeStandardBase64)(rendered.body).split("\n");
        const ports = lines.map((line) => new URL(line).port);
        if (ports.length !== 2 || ports.join(",") !== "8443,2053")
            throw new Error(`Port filter mismatch: ${ports.join(",")}`);
    }));
    tests.push(await runOne("output.domain-filter", "Domain endpoint filter selects all configured domain ports", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443,8443,2053", CLEAN_IPS: "104.21.1.1" };
        const rendered = await (0, render_1.renderSubscription)(env, new Request("https://proxy.example.com/sub?protocol=vless&endpoint=domain"));
        const lines = (0, render_1.decodeStandardBase64)(rendered.body).split("\n");
        if (lines.length !== 3 || lines.some((line) => line.includes("104.21.1.1")))
            throw new Error("Domain filter mismatch");
    }));
    tests.push(await runOne("output.ipv6-share", "IPv6 Clean-IP URIs use bracketed authorities", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", CLEAN_IPS: "[2606:4700:4700::1111]:443" };
        const rendered = await (0, render_1.renderSubscription)(env, new Request("https://proxy.example.com/sub?protocol=vless&endpoint=1&encode=raw"));
        if (!rendered.body.includes("@[2606:4700:4700::1111]:443?"))
            throw new Error("IPv6 URI was not bracketed");
    }));
    tests.push(await runOne("dns.cache", "DNS cache honors TTL and exposes hit/miss statistics", "dns", async () => {
        const cache = new cache_1.DnsCache(2, 300, 1);
        // Control the cache's clock: DnsCache reads Date.now(), so swapping it
        // makes TTL ageing and expiry deterministic (previously nothing ever
        // expired inside this test, so the TTL claim was untested).
        const realNow = Date.now;
        let now = 1_700_000_000_000;
        Date.now = () => now;
        try {
            cache.set("example.com", "A", [{ address: "1.2.3.4", family: 4, ttl: 5 }], 5);
            const first = cache.get("example.com", "A");
            if (first?.[0]?.address !== "1.2.3.4" || first[0].ttl !== 5)
                throw new Error("Cache miss or wrong initial TTL after set");
            now += 2_000;
            const aged = cache.get("example.com", "A");
            if (!aged?.length || aged[0].ttl !== 3)
                throw new Error(`Cache did not age the entry TTL with the clock: ${JSON.stringify(aged)}`);
            now += 4_000; // 6s elapsed against a 5s TTL
            if (cache.get("example.com", "A") !== null)
                throw new Error("Cache served an expired entry");
            const stats = cache.stats();
            if (stats.hits !== 2 || stats.misses !== 1)
                throw new Error(`Cache stats mismatch after expiry: ${JSON.stringify(stats)}`);
            if (stats.entries !== 0)
                throw new Error("Expired cache entry was not pruned");
        }
        finally {
            Date.now = realNow;
        }
    }));
    tests.push(await runOne("dns.hosts", "DNS hosts override supports IPv4/IPv6 and preference", "dns", async (signal) => {
        const hosts = new hosts_1.HostsResolver("example.com=1.2.3.4,example.com=2001:db8::1");
        if ((await hosts.resolve("example.com", "ipv4", signal))[0]?.address !== "1.2.3.4")
            throw new Error("Hosts IPv4 lookup failed");
        if ((await hosts.resolve("example.com", "ipv6", signal))[0]?.address !== "2001:db8::1")
            throw new Error("Hosts IPv6 lookup failed");
        if (hosts.size !== 1)
            throw new Error("Hosts size mismatch");
    }));
    tests.push(await runOne("dns.doh-parser", "DoH resolver parses A and follows CNAME", "dns", async (signal) => {
        const requestFetch = async (_input, init) => {
            const query = new Uint8Array(init?.body);
            const qid = new DataView(query.buffer, query.byteOffset, query.byteLength).getUint16(0);
            const qnameLength = query.findIndex((x, i) => i >= 12 && x === 0) - 12 + 1;
            const qname = query.subarray(12, 12 + qnameLength);
            const questionEnd = 12 + qnameLength + 4;
            const answerName = new Uint8Array([0xc0, 0x0c]);
            const cnameName = new TextEncoder().encode("target.example.com");
            const cnameRdata = (() => {
                const labels = cnameName.join("").split("");
                void labels;
                const out = [];
                for (const label of "target.example.com".split(".")) {
                    const b = new TextEncoder().encode(label);
                    out.push(b.length, ...b);
                }
                out.push(0);
                return new Uint8Array(out);
            })();
            const out = new Uint8Array(questionEnd + 2 + 10 + cnameRdata.length + 2 + 10 + 4);
            const view = new DataView(out.buffer);
            view.setUint16(0, qid);
            view.setUint16(2, 0x8180);
            view.setUint16(4, 1);
            view.setUint16(6, 2);
            out.set(qname, 12);
            out.set(new Uint8Array([0, 1, 0, 1]), 12 + qnameLength);
            let off = questionEnd;
            out.set(answerName, off);
            off += 2;
            view.setUint16(off, 5);
            view.setUint16(off + 2, 1);
            view.setUint32(off + 4, 60);
            view.setUint16(off + 8, cnameRdata.length);
            off += 10;
            out.set(cnameRdata, off);
            off += cnameRdata.length;
            out.set(answerName, off);
            off += 2;
            view.setUint16(off, 1);
            view.setUint16(off + 2, 1);
            view.setUint32(off + 4, 60);
            view.setUint16(off + 8, 4);
            off += 10;
            out.set([1, 2, 3, 4], off);
            return new Response(out, { status: 200 });
        };
        const resolver = new doh_1.DoHResolver({ endpoint: "https://dns.example/dns-query", timeoutMs: 1000, retries: 0, fetchImpl: requestFetch });
        const result = await resolver.resolve("example.com", "ipv4", signal);
        if (result[0]?.address !== "1.2.3.4")
            throw new Error(`DoH CNAME result mismatch: ${result.map((x) => x.address).join(",")}`);
    }));
    tests.push(await runOne("dns.engine-resolution-order", "DNS preference modes preserve deterministic address ordering", "dns", async (signal) => {
        // Actually resolve through DnsEngine -> DoHResolver with a scripted
        // upstream that answers A with 1.2.3.4 and AAAA with 2001:db8::1, and
        // assert the exact address sequence each strategy returns. (This test
        // previously only inspected its own fixture object and never resolved.)
        const originalFetch = globalThis.fetch;
        const answerFor = (query) => {
            const view = new DataView(query.buffer, query.byteOffset, query.byteLength);
            const id = view.getUint16(0);
            const qnameEnd = query.findIndex((x, i) => i >= 12 && x === 0);
            const questionEnd = qnameEnd + 1 + 4;
            const qtype = view.getUint16(questionEnd - 4);
            const rdata = qtype === 28
                ? new Uint8Array([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                : new Uint8Array([1, 2, 3, 4]);
            const out = new Uint8Array(questionEnd + 2 + 10 + rdata.length);
            const outView = new DataView(out.buffer);
            outView.setUint16(0, id);
            outView.setUint16(2, 0x8180);
            outView.setUint16(4, 1);
            outView.setUint16(6, 1);
            out.set(query.subarray(12, questionEnd), 12);
            let off = questionEnd;
            out.set(new Uint8Array([0xc0, 0x0c]), off);
            off += 2;
            outView.setUint16(off, qtype);
            outView.setUint16(off + 2, 1);
            outView.setUint32(off + 4, 60);
            outView.setUint16(off + 8, rdata.length);
            off += 10;
            out.set(rdata, off);
            return new Response(out, { status: 200, headers: { "content-type": "application/dns-message" } });
        };
        globalThis.fetch = (async (_input, init) => {
            return answerFor(new Uint8Array(init?.body));
        });
        try {
            const expectations = [
                ["prefer_ipv6", ["2001:db8::1"]],
                ["prefer_ipv4", ["1.2.3.4"]],
                ["auto", ["1.2.3.4", "2001:db8::1"]],
                ["ipv6", ["2001:db8::1"]],
                ["ipv4", ["1.2.3.4"]],
            ];
            for (const [preference, expected] of expectations) {
                const engine = new engine_2.DnsEngine({
                    DNS_ENABLED: "true",
                    DNS_RESOLVERS: "doh:https://dns.example/dns-query",
                    DNS_CACHE: "false",
                    DNS_RETRIES: "0",
                    DNS_IP_PREFERENCE: preference,
                });
                if (!engine.enabled || engine.strategy !== preference)
                    throw new Error(`DNS strategy configuration mismatch for ${preference}`);
                const resolved = await engine.resolve("example.com", engine.strategy, signal);
                const addresses = resolved.map((item) => item.address);
                if (addresses.join(",") !== expected.join(","))
                    throw new Error(`DNS ${preference} ordering mismatch: got [${addresses.join(", ")}], expected [${expected.join(", ")}]`);
            }
        }
        finally {
            globalThis.fetch = originalFetch;
        }
    }));
    tests.push(await runOne("dns.engine-config", "DNS engine parses Worker-native settings and exposes diagnostics", "dns", async () => {
        const engine = new engine_2.DnsEngine({ DNS_ENABLED: "true", DNS_RESOLVERS: "doh:https://cloudflare-dns.com/dns-query,doh:https://dns.google/dns-query", DNS_CACHE: "true", DNS_FALLBACK: "true", DNS_IP_PREFERENCE: "prefer_ipv6", DNS_TIMEOUT_MS: "2500", DNS_RETRIES: "2", DNS_HOSTS: "example.com=1.2.3.4" });
        const d = engine.diagnostics();
        if (!d.enabled || d.resolver_count !== 2 || d.strategy !== "prefer_ipv6" || !d.cache || !d.hosts.enabled)
            throw new Error("DNS diagnostics mismatch");
    }));
    tests.push(await runOne("dns.default-resolver-fallback", "DNS enabled without explicit resolvers uses the documented public DoH fallback pair", "dns", async () => {
        const engine = new engine_2.DnsEngine({ DNS_ENABLED: "true", DNS_CACHE: "false" });
        const d = engine.diagnostics();
        const endpoints = d.resolvers.map((item) => item.endpoint).filter(Boolean);
        if (endpoints.length !== 2 || !endpoints[0]?.includes("cloudflare-dns.com/dns-query") || !endpoints[1]?.includes("dns.google/dns-query"))
            throw new Error(`Default DoH fallback mismatch: ${JSON.stringify(endpoints)}`);
    }));
    tests.push(await runOne("dns.engine-upstream-fallback", "DNS engine fails over from the first public DoH endpoint to the second", "dns", async (signal) => {
        const originalFetch = globalThis.fetch;
        let calls = 0;
        try {
            globalThis.fetch = (async (_input, init) => {
                calls++;
                if (calls === 1)
                    return new Response("", { status: 503 });
                const requestBytes = new Uint8Array(init?.body);
                // Engine queries now carry an EDNS OPT additional record (D11); a
                // well-formed response echoes header + question only, so truncate the
                // copied query right after the question section.
                let qEnd = 12;
                while (requestBytes[qEnd] !== 0)
                    qEnd += requestBytes[qEnd] + 1;
                qEnd += 5; // root terminator + QTYPE + QCLASS
                const q = requestBytes.slice(0, qEnd);
                const answerName = new Uint8Array([0xc0, 0x0c]);
                const out = new Uint8Array(q.length + answerName.length + 10 + 4);
                out.set(q, 0);
                const view = new DataView(out.buffer);
                view.setUint16(2, 0x8180);
                view.setUint16(6, 1);
                view.setUint16(10, 0); // ARCOUNT: the OPT record is not echoed
                out.set(answerName, q.length);
                const answerOffset = q.length + answerName.length;
                view.setUint16(answerOffset, 1);
                view.setUint16(answerOffset + 2, 1);
                view.setUint32(answerOffset + 4, 60);
                view.setUint16(answerOffset + 8, 4);
                out.set([1, 2, 3, 4], answerOffset + 10);
                return new Response(out, { status: 200, headers: { "content-type": "application/dns-message" } });
            });
            const engine = new engine_2.DnsEngine({ DNS_ENABLED: "true", DNS_CACHE: "false", DNS_IP_PREFERENCE: "ipv4", DNS_RETRIES: "0" });
            const result = await engine.resolve("example.com", "ipv4", signal);
            if (result[0]?.address !== "1.2.3.4" || calls !== 2)
                throw new Error(`DNS upstream failover mismatch: calls=${calls}, result=${JSON.stringify(result)}`);
        }
        finally {
            globalThis.fetch = originalFetch;
        }
    }));
    tests.push(await runOne("dns.doh-failure-diagnostics", "DoH diagnostics retain endpoint, failure phase and attempt details", "dns", async (signal) => {
        const requestFetch = async () => new Response("", { status: 503 });
        const resolver = new doh_1.DoHResolver({ endpoint: "https://dns.example/dns-query", timeoutMs: 1000, retries: 1, fetchImpl: requestFetch });
        let rejected = false;
        try {
            await resolver.resolve("example.com", "ipv4", signal);
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "DNS_FAILED";
        }
        const d = resolver.diagnostics();
        if (!rejected || d.queries !== 2 || d.failures !== 1 || d.endpoint !== "https://dns.example/dns-query" || d.lastFailure?.phase !== "http" || d.lastFailure?.attempt !== 1 || d.lastFailure?.status !== 503)
            throw new Error(`DoH failure diagnostics mismatch: ${JSON.stringify(d)}`);
    }));
    tests.push(await runOne("dns.empty-resolver-default", "Explicitly blank DNS_RESOLVERS does not accidentally disable the resolver list", "dns", async () => {
        const engine = new engine_2.DnsEngine({ DNS_ENABLED: "true", DNS_RESOLVERS: "", DNS_CACHE: "false" });
        if (engine.diagnostics().resolver_count !== 2)
            throw new Error("Blank DNS_RESOLVERS did not use the default resolver pair");
    }));
    tests.push(await runOne("dns.fallback-empty-response-policy", "DNS_FALLBACK=false stops after an empty successful resolver response", "dns", async (signal) => {
        const originalFetch = globalThis.fetch;
        let calls = 0;
        globalThis.fetch = (async (_input, init) => {
            calls++;
            const query = new Uint8Array(init?.body);
            const qnameEnd = query.findIndex((x, i) => i >= 12 && x === 0) + 1;
            const out = new Uint8Array(qnameEnd + 4);
            const view = new DataView(out.buffer);
            view.setUint16(0, new DataView(query.buffer, query.byteOffset, query.byteLength).getUint16(0));
            view.setUint16(2, 0x8180);
            view.setUint16(4, 1);
            out.set(query.subarray(12, qnameEnd + 4), 12);
            return new Response(out, { status: 200, headers: { "content-type": "application/dns-message" } });
        });
        try {
            const engine = new engine_2.DnsEngine({
                DNS_ENABLED: "true",
                DNS_RESOLVERS: "https://first.example/dns-query,https://second.example/dns-query",
                DNS_CACHE: "false",
                DNS_FALLBACK: "false",
                DNS_RETRIES: "0",
            });
            const result = await engine.resolve("example.com", "ipv4", signal);
            if (result.length !== 0 || calls !== 1)
                throw new Error(`DNS_FALLBACK=false ignored empty successful response: calls=${calls}, result=${JSON.stringify(result)}`);
        }
        finally {
            globalThis.fetch = originalFetch;
        }
    }));
    tests.push(await runOne("dns.resolver-input-normalization", "DNS_RESOLVERS accepts full URLs and safe hostname shorthand while canonicalizing the Cloudflare alias", "dns", async () => {
        const values = (0, config_1.parseDohResolverEndpoints)("cloudflare-dns.com,dns.google,one.one.one.one,doh:https://cloudflare-dns.com/dns-query");
        const expected = ["https://cloudflare-dns.com/dns-query", "https://dns.google/dns-query"];
        if (JSON.stringify(values) !== JSON.stringify(expected))
            throw new Error(`Unexpected resolver normalization: ${JSON.stringify(values)}`);
    }));
    tests.push(await runOne("dns.vless-udp-bridge", "VLESS UDP DNS bridge frames query and response correctly", "dns", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        const clientWrites = [];
        const query = new Uint8Array([0x4a, 0x21, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        const framed = new Uint8Array(2 + query.length);
        new DataView(framed.buffer).setUint16(0, query.length, false);
        framed.set(query, 2);
        reader.push(framed);
        reader.end(new errors_1.CoreError("CLIENT_CLOSED", "test complete"));
        const writer = { write: async (data) => { clientWrites.push(new Uint8Array(data)); } };
        const response = new Uint8Array([...query, 0x81, 0x80, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
        const dns = { enabled: true, queryRaw: async (message) => {
                if (!(0, binary_1.equalBytes)(message, query))
                    throw new Error("DNS bridge query mismatch");
                return response;
            } };
        const session = {
            protocol: "vless",
            principal: { id: "self-test", protocol: "vless" },
            destination: { host: "cloudflare-dns.com", port: 53 },
            initialPayload: new Uint8Array(),
            responseHeader: new Uint8Array([0, 0]),
            udpDns: true,
        };
        await (0, dns_udp_1.bridgeVlessDnsUdp)(reader, writer, session, dns, signal);
        if (clientWrites.length !== 1 || clientWrites[0][0] !== 0 || clientWrites[0][1] !== 0)
            throw new Error("VLESS DNS response header missing");
        const frameLength = new DataView(clientWrites[0].buffer, clientWrites[0].byteOffset, clientWrites[0].byteLength).getUint16(2, false);
        if (frameLength !== response.length || !(0, binary_1.equalBytes)(clientWrites[0].subarray(4), response))
            throw new Error("VLESS DNS response frame mismatch");
    }));
    tests.push(await runOne("dns.raw-doh-failure-diagnostics", "Raw DoH failures retain resolver phase diagnostics", "dns", async () => {
        const resolver = new doh_1.DoHResolver({ endpoint: "https://dns.example/dns-query", timeoutMs: 100, retries: 0, fetchImpl: async () => { throw new Error("synthetic fetch failure"); } });
        const validQuery = new Uint8Array([
            0x4a, 0x21, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x03, 0x77, 0x77, 0x77, 0x07, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65,
            0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01,
        ]);
        try {
            await resolver.queryRaw(validQuery, new AbortController().signal);
        }
        catch {
            const last = resolver.diagnostics().lastFailure;
            if (!last || last.phase !== "fetch" || !last.message.includes("synthetic fetch failure"))
                throw new Error(`Raw failure diagnostics missing: ${JSON.stringify(last)}`);
            return;
        }
        throw new Error("Synthetic raw DoH failure unexpectedly succeeded");
    }));
    tests.push(await runOne("dns.raw-wire-validation", "Raw DNS validator rejects malformed responses and accepts valid semantic responses", "dns", async (signal) => {
        void signal;
        const query = new Uint8Array([
            0x4a, 0x21, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x03, 0x77, 0x77, 0x77, 0x07, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65,
            0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01,
        ]);
        const question = (0, doh_2.validateRawDnsQuery)(query);
        const valid = new Uint8Array([
            0x4a, 0x21, 0x81, 0x80, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ...query.slice(12),
        ]);
        const meta = (0, doh_2.validateRawDnsResponse)(valid, 0x4a21, question);
        if (meta.rcode !== 0 || meta.answerCount !== 0)
            throw new Error(`Valid DNS response metadata mismatch: ${JSON.stringify(meta)}`);
        const qrMissing = new Uint8Array(valid);
        qrMissing[2] = 0x01;
        qrMissing[3] = 0x00;
        let rejectedQr = false;
        try {
            (0, doh_2.validateRawDnsResponse)(qrMissing, 0x4a21, question);
        }
        catch (error) {
            rejectedQr = error instanceof errors_1.CoreError && error.code === "DNS_FAILED";
        }
        if (!rejectedQr)
            throw new Error("Raw validator accepted a query packet as a response");
        const truncatedQuestion = valid.slice(0, 18);
        let rejectedStructure = false;
        try {
            (0, doh_2.validateRawDnsResponse)(truncatedQuestion, 0x4a21, question);
        }
        catch (error) {
            rejectedStructure = error instanceof errors_1.CoreError && error.code === "DNS_FAILED";
        }
        if (!rejectedStructure)
            throw new Error("Raw validator accepted a structurally truncated response");
        const wrongQuestion = new Uint8Array(valid);
        wrongQuestion[29] = 0x1c;
        wrongQuestion[30] = 0x00;
        let rejectedIdentity = false;
        try {
            (0, doh_2.validateRawDnsResponse)(wrongQuestion, 0x4a21, question);
        }
        catch (error) {
            rejectedIdentity = error instanceof errors_1.CoreError && error.code === "DNS_FAILED";
        }
        if (!rejectedIdentity)
            throw new Error("Raw validator accepted a response with a mismatched question");
    }));
    tests.push(await envFlag(env, "CORE_TEST_LIVE_DNS", "dns.doh-live", "Live DoH query reaches a configured resolver", "dns", async (signal) => {
        const testDns = new engine_2.DnsEngine({ ...env, DNS_ENABLED: "true" });
        const query = new Uint8Array([
            0x4a, 0x21, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0a, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x66, 0x6c, 0x61, 0x72, 0x65,
            0x03, 0x63, 0x6f, 0x6d, 0x00, 0x00, 0x01, 0x00, 0x01,
        ]);
        const response = await testDns.queryRaw(query, signal);
        const question = (0, doh_2.validateRawDnsQuery)(query);
        const metadata = (0, doh_2.validateRawDnsResponse)(response, (query[0] << 8) | query[1], question);
        // answerCount is an unsigned header count, so `< 0` could never fail;
        // a live A query for cloudflare.com must return at least one answer.
        if (metadata.rcode !== 0 || metadata.answerCount < 1)
            throw new Error(`Live DoH response semantic validation failed: rcode=${metadata.rcode}, answers=${metadata.answerCount}`);
        return { rcode: metadata.rcode, answers: metadata.answerCount, authority: metadata.authorityCount, additional: metadata.additionalCount, resolver_diagnostics: testDns.diagnostics().resolvers };
    }, 10000));
    tests.push(await runOne("transport.websocket-early-data", "WebSocket early-data decoding uses Sec-WebSocket-Protocol without requiring ed query state", "transport", async () => {
        // Real early data is binary (a VLESS/Trojan header or TLS ClientHello);
        // an all-printable-ASCII token is an RFC 6455 subprotocol name and is
        // never consumed as early data (A9), so the fixture payload is binary.
        const payload = new Uint8Array([0x00, 0x16, 0x03, 0x01, 0xfe]);
        let binary = "";
        for (const byte of payload)
            binary += String.fromCharCode(byte);
        const encoded = btoa(binary).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
        const request = new Request("https://example.com/vless", { headers: { Upgrade: "websocket", "Sec-WebSocket-Protocol": encoded } });
        const decoded = (0, adapter_1.tryExtractEarlyData)(request, { earlyDataEnabled: true, earlyDataHeaderName: "Sec-WebSocket-Protocol", maxEarlyDataBytes: 8192 });
        if (!decoded || decoded.byteLength !== payload.byteLength || !decoded.every((byte, index) => byte === payload[index]))
            throw new Error("Early data was not decoded");
        const oversized = new Request("https://example.com/vless", { headers: { Upgrade: "websocket", "Sec-WebSocket-Protocol": encoded } });
        if ((0, adapter_1.tryExtractEarlyData)(oversized, { earlyDataEnabled: true, maxEarlyDataBytes: 2 }) !== null)
            throw new Error("Early-data byte cap was ignored");
    }));
    tests.push(await runOne("connection.bridge-response-header", "Bridge sends VLESS response header before waiting for upstream payload", "connection", async () => {
        const clientReader = new byte_queue_1.ByteQueue(1024);
        clientReader.end(new errors_1.CoreError("CLIENT_CLOSED", "test client closed"));
        const clientWrites = [];
        const clientWriter = { write: async (data) => { clientWrites.push(new Uint8Array(data)); } };
        const upstreamWrites = [];
        const upstreamReadable = new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode("world")); controller.close(); } });
        const upstreamWritable = new WritableStream({ write(chunk) { upstreamWrites.push(new Uint8Array(chunk)); } });
        const upstream = { type: "direct", readable: upstreamReadable, writable: upstreamWritable, close: async () => { } };
        await (0, bridge_1.bridgeSession)(clientReader, clientWriter, upstream, { signal: new AbortController().signal, session: {
                protocol: "vless", principal: { id: "test", protocol: "vless" }, destination: { host: "example.com", port: 443 }, initialPayload: new TextEncoder().encode("hello"), responseHeader: new Uint8Array([1, 0]),
            } });
        if (clientWrites.length < 2 || clientWrites[0][0] !== 1 || clientWrites[0][1] !== 0)
            throw new Error("Response header was not sent first");
        if (new TextDecoder().decode(clientWrites[1]) !== "world")
            throw new Error("Upstream response was not forwarded");
        if (new TextDecoder().decode(upstreamWrites[0]) !== "hello")
            throw new Error("Initial payload was not forwarded");
    }));
    tests.push(await runOne("connection.bridge-cancel", "Bridge cancels a pending upstream read when the client closes", "connection", async () => {
        const clientReader = new byte_queue_1.ByteQueue(1024);
        clientReader.end(new errors_1.CoreError("CLIENT_CLOSED", "test client closed"));
        const clientWriter = { write: async (_data) => { } };
        let closeCalls = 0;
        const upstreamReadable = new ReadableStream({ cancel() { } });
        const upstreamWritable = new WritableStream();
        const upstream = { type: "direct", readable: upstreamReadable, writable: upstreamWritable, close: async () => { closeCalls++; } };
        await (0, bridge_1.bridgeSession)(clientReader, clientWriter, upstream, { signal: new AbortController().signal, session: {
                protocol: "trojan", principal: { id: "test", protocol: "trojan" }, destination: { host: "example.com", port: 443 }, initialPayload: new Uint8Array(),
            } });
        if (closeCalls !== 1)
            throw new Error(`Upstream close expected once, got ${closeCalls}`);
    }));
    tests.push(await runOne("connection.bridge-session-deadline", "Bridge deadline is reported as TIMEOUT instead of successful completion", "connection", async () => {
        let upstreamReadReject;
        const pendingRead = (signal) => new Promise((_, reject) => {
            if (signal.aborted) {
                reject(signal.reason);
                return;
            }
            signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
        const reader = { readSome: async (_max, signal) => pendingRead(signal) };
        const clientWriter = { write: async (_data) => { } };
        const upstream = {
            type: "direct",
            readable: { getReader: () => ({ read: async () => new Promise((_, reject) => { upstreamReadReject = reject; }), cancel: async (reason) => { upstreamReadReject?.(reason); }, releaseLock: () => { } }) },
            writable: { getWriter: () => ({ write: async (_data) => { }, close: async () => { }, abort: async () => { }, releaseLock: () => { } }) },
            close: async () => { },
        };
        const outcome = await (0, bridge_1.bridgeSession)(reader, clientWriter, upstream, {
            signal: new AbortController().signal,
            maxSessionDurationMs: 25,
            session: { protocol: "vless", principal: { id: "test", protocol: "vless" }, destination: { host: "example.com", port: 443 }, initialPayload: new Uint8Array() },
        });
        if (outcome.kind !== "error" || outcome.error?.code !== "TIMEOUT")
            throw new Error(`Session deadline was misclassified: ${JSON.stringify(outcome)}`);
    }));
    tests.push(await runOne("engine.fallback", "ProxyEngine falls back from Direct to ProxyIP on initial dial failure", "engine", async () => {
        const clientReader = new byte_queue_1.ByteQueue(8192);
        const clientWriter = { write: async (_data) => { } };
        const domain = new TextEncoder().encode("example.com");
        const request = (0, binary_1.concatBytes)(new Uint8Array([0]), vlessBytes, new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([1, 187]), new Uint8Array([2, domain.length]), domain);
        clientReader.push(request);
        clientReader.end();
        let directCalls = 0;
        let proxyCalls = 0;
        let proxyCloseCalls = 0;
        const proxyUpstream = {
            type: "proxyip",
            readable: new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode("ok")); controller.close(); } }),
            writable: new WritableStream(),
            close: async () => { proxyCloseCalls++; },
        };
        const direct = { type: "direct", connect: async () => { directCalls++; throw new errors_1.CoreError("EGRESS_FAILED", "simulated direct failure", true); } };
        const proxy = { type: "proxyip", connect: async () => { proxyCalls++; return proxyUpstream; } };
        const egressMap = new registry_1.EgressRegistry();
        egressMap.register(direct);
        egressMap.register(proxy);
        const engine = new engine_1.ProxyEngine({
            router: new router_1.DefaultRouter("fallback"),
            protocols: new Map([["vless", new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes))]]),
            egress: egressMap,
            handshakeTimeoutMs: 1000,
        });
        const connection = {
            reader: clientReader, writer: clientWriter,
            metadata: { type: "websocket", url: new URL("https://example.com/vless"), path: "/vless", headers: new Headers() },
            close: async () => { },
        };
        await engine.handle(connection, { id: "engine-fallback-test", startedAt: Date.now(), signal: new AbortController().signal }, "vless");
        if (directCalls !== 1 || proxyCalls !== 1)
            throw new Error(`Expected Direct=1 and ProxyIP=1, got ${directCalls}/${proxyCalls}`);
        if (proxyCloseCalls !== 1)
            throw new Error(`ProxyIP upstream close expected once, got ${proxyCloseCalls}`);
    }));
    tests.push(await runOne("engine.integration", "ProxyEngine composes protocol, routing, egress and bridge correctly", "engine", async () => {
        const clientReader = new byte_queue_1.ByteQueue(8192);
        const clientWrites = [];
        const clientWriter = { write: async (data) => { clientWrites.push(new Uint8Array(data)); } };
        const domain = new TextEncoder().encode("example.com");
        const request = (0, binary_1.concatBytes)(new Uint8Array([0]), vlessBytes, new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([1, 187]), new Uint8Array([2, domain.length]), domain, new TextEncoder().encode("hello"));
        clientReader.push(request);
        let endClientTimer;
        endClientTimer = setTimeout(() => clientReader.end(new errors_1.CoreError("CLIENT_CLOSED", "integration client complete")), 25);
        const upstreamWrites = [];
        let upstreamCloseCalls = 0;
        const upstreamReadable = new ReadableStream({
            start(controller) {
                queueMicrotask(() => { controller.enqueue(new TextEncoder().encode("world")); controller.close(); });
            },
        });
        const upstreamWritable = new WritableStream({
            write(chunk) { upstreamWrites.push(new Uint8Array(chunk)); },
        });
        const upstream = {
            type: "direct",
            readable: upstreamReadable,
            writable: upstreamWritable,
            close: async () => { upstreamCloseCalls++; },
        };
        let directCalls = 0;
        const egress = {
            type: "direct",
            connect: async () => { directCalls++; return upstream; },
        };
        const engine = new engine_1.ProxyEngine({
            router: new router_1.DefaultRouter("direct"),
            protocols: new Map([["vless", new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes))]]),
            egress: (() => { const registry = new registry_1.EgressRegistry(); registry.register(egress); return registry; })(),
            handshakeTimeoutMs: 1000,
        });
        const connection = {
            reader: clientReader,
            writer: clientWriter,
            metadata: { type: "websocket", url: new URL("https://example.com/vless"), path: "/vless", headers: new Headers() },
            close: async () => { },
        };
        try {
            await engine.handle(connection, { id: "engine-test", startedAt: Date.now(), signal: new AbortController().signal }, "vless");
        }
        finally {
            if (endClientTimer)
                clearTimeout(endClientTimer);
        }
        if (directCalls !== 1)
            throw new Error(`Direct egress expected once, got ${directCalls}`);
        if (upstreamCloseCalls !== 1)
            throw new Error(`Upstream close expected once, got ${upstreamCloseCalls}`);
        if (clientWrites.length < 2 || !(0, binary_1.equalBytes)(clientWrites[0], new Uint8Array([0, 0])) || new TextDecoder().decode(clientWrites[1]) !== "world") {
            throw new Error("Engine did not produce the expected response sequence");
        }
        if (new TextDecoder().decode(upstreamWrites[0] ?? new Uint8Array()) !== "hello")
            throw new Error("Engine did not forward initial payload");
    }));
    tests.push(await runOne("egress.direct-dns-policy", "Direct egress does not bypass an enabled custom DNS policy", "egress", async (signal) => {
        const fakeDns = { enabled: true, resolve: async () => [] };
        const egress = new direct_1.DirectEgress(500, fakeDns);
        try {
            await egress.connect({ host: "blocked.example", port: 443 }, signal);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "DNS_FAILED")
                return;
            throw error;
        }
        throw new Error("Direct egress fell back to platform hostname resolution after empty custom DNS result");
    }));
    tests.push(await runOne("dns.resolver-dedupe-limit", "DNS resolver count is bounded after duplicate removal", "dns", async () => {
        const env = { DNS_ENABLED: "true", DNS_RESOLVERS: "https://dns.example/dns-query,https://dns.example/dns-query", DNS_MAX_RESOLVERS: "1" };
        const engine = new engine_2.DnsEngine(env);
        if (engine.diagnostics().resolver_count !== 1)
            throw new Error(`Expected one unique resolver, got ${engine.diagnostics().resolver_count}`);
    }));
    tests.push(await runOne("runtime.proxyip-timeout-signature", "Runtime cache is invalidated when ProxyIP handshake timeout changes", "runtime", async () => {
        const base = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, EGRESS_MODE: "direct" };
        const first = (0, runtime_1.runtimeFor)({ ...base, CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS: "1000" });
        const second = (0, runtime_1.runtimeFor)({ ...base, CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS: "2000" });
        if (first === second)
            throw new Error("Runtime cache signature ignored CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS");
    }));
    if (env.CORE_TEST_TARGET?.trim()) {
        tests.push(await runOne("egress.direct", "Direct TCP reachability/application exchange", "egress", async () => {
            const target = parseTarget(env.CORE_TEST_TARGET);
            const egress = new direct_1.DirectEgress(testTimeout(env));
            const conn = await egress.connect(target, new AbortController().signal);
            try {
                await optionalHttpExchange(conn, target);
            }
            finally {
                await conn.close();
            }
            return { target: `${target.host}:${target.port}`, application_exchange: target.port === 80 };
        }));
    }
    else {
        tests.push(skipped("egress.direct", "Direct TCP reachability/application exchange", "egress", "CORE_TEST_TARGET is not configured"));
    }
    // Deterministic ProxyIP tests: every case in this block injects an
    // in-memory fake connector and performs no network access, so the block
    // runs unconditionally. It used to be gated behind hasProxyIp(env), which
    // made plain deployments report these as SKIPPED and silently lose real
    // coverage. Only the genuinely live relay test at the end of the block
    // stays environment-gated (on CORE_TEST_TARGET).
    {
        tests.push(await runOne("egress.proxyip.circuit-breaker", "ProxyIP cooldown excludes failed endpoints until half-open", "egress", async (signal) => {
            let calls = 0;
            const connector = { connect: async () => { calls++; throw new errors_1.CoreError("EGRESS_FAILED", "simulated proxy failure", true); } };
            const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "test-proxy:1080", CORE_MAX_PROXYIP_FAILURES: "1", CORE_PROXYIP_COOLDOWN_MS: "5000" }, 500, connector);
            for (let i = 0; i < 2; i++) {
                try {
                    await egress.connect({ host: "example.com", port: 443 }, signal);
                }
                catch (error) {
                    if (!(error instanceof errors_1.CoreError) || error.code !== "EGRESS_FAILED")
                        throw error;
                }
            }
            if (calls !== 1)
                throw new Error(`Cooling endpoint was retried before half-open window: ${calls}`);
            if (Number(egress.diagnostics.cooled_down) !== 1)
                throw new Error("ProxyIP diagnostics did not report the cooled endpoint");
        }));
        tests.push(await runOne("egress.proxyip.half-open-abort", "Aborted half-open ProxyIP probes do not remain permanently locked", "egress", async (signal) => {
            const connector = { connect: async () => { throw new errors_1.CoreError("EGRESS_FAILED", "simulated proxy failure", true); } };
            const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "half-open-proxy:1080", CORE_MAX_PROXYIP_FAILURES: "1", CORE_PROXYIP_COOLDOWN_MS: "1000" }, 500, connector);
            try {
                await egress.connect({ host: "example.com", port: 443 }, signal);
            }
            catch (error) {
                if (!(error instanceof errors_1.CoreError) || error.code !== "EGRESS_FAILED")
                    throw error;
            }
            const health = egress.health;
            const state = [...health.values()][0];
            if (!state)
                throw new Error("ProxyIP health state was not created");
            state.cooldownUntil = Date.now() - 1;
            const controller = new AbortController();
            controller.abort(new errors_1.CoreError("CLIENT_CLOSED", "test abort"));
            try {
                await egress.connect({ host: "example.com", port: 443 }, controller.signal);
            }
            catch { /* expected */ }
            if (state.probeInFlight)
                throw new Error("Half-open probe remained locked after aborted request");
        }));
        tests.push(await runOne("egress.proxyip.handshake-timeout", "ProxyIP SOCKS5 handshake has its own deadline", "egress", async (signal) => {
            let closed = false;
            const socket = {
                opened: Promise.resolve(),
                readable: new ReadableStream({}),
                writable: new WritableStream({ write: async () => new Promise(() => { }) }),
                close: async () => { closed = true; },
            };
            const connector = { connect: async () => socket };
            const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "test-proxy:1080", CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS: "100" }, 500, connector);
            try {
                await egress.connect({ host: "example.com", port: 443 }, signal);
            }
            catch (error) {
                if (error instanceof errors_1.CoreError && (error.code === "EGRESS_FAILED" || error.code === "TIMEOUT")) {
                    if (!closed)
                        throw new Error("Timed-out SOCKS handshake did not close the socket");
                    return;
                }
                throw error;
            }
            throw new Error("ProxyIP handshake unexpectedly completed without a SOCKS response");
        }));
        tests.push(await runOne("egress.proxyip.structured-endpoints", "ProxyIP structured endpoint configuration isolates TLS and credentials", "egress", async (signal) => {
            let secure;
            const connector = { connect: async (_dest, _signal, _timeout, mode) => { secure = mode; return new FakeSocks5Socket(true, "example.com", 443, 3, "alice", "secret"); } };
            const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP_ENDPOINTS_JSON: JSON.stringify([{ host: "secure-proxy.example.com", port: 1080, tls: true, username: "alice", password: "secret" }]), PROXY_IP: undefined, PROXY_IP_USERNAME: undefined, PROXY_IP_PASSWORD: undefined }, 500, connector);
            const connection = await egress.connect({ host: "example.com", port: 443 }, signal);
            await connection.close();
            if (secure !== "on")
                throw new Error(`Structured endpoint TLS mode was not propagated: ${secure}`);
        }));
        tests.push(await runOne("egress.proxyip.credential-identity", "Structured ProxyIP endpoints with different passwords remain distinct", "egress", async () => {
            const egress = new proxyip_1.ProxyIpEgress({
                ...env,
                PROXY_IP: undefined,
                PROXY_IP_USERNAME: undefined,
                PROXY_IP_PASSWORD: undefined,
                PROXY_IP_ENDPOINTS_JSON: JSON.stringify([
                    { host: "same-proxy.example.com", port: 1080, tls: false, username: "alice", password: "one" },
                    { host: "same-proxy.example.com", port: 1080, tls: false, username: "alice", password: "two" },
                ]),
            }, 500, { connect: async () => new FakeSocks5Socket(true, "example.com", 443, 3, "alice", "one") });
            if (Number(egress.diagnostics.endpoints) !== 2)
                throw new Error("ProxyIP endpoint deduplication collapsed differing passwords");
        }));
        tests.push(await runOne("egress.proxyip.handshake", "ProxyIP SOCKS5 CONNECT and buffered payload", "egress", async () => {
            const fakeConnector = { connect: async () => new FakeSocks5Socket(true, "example.com", 443, 3, "user", "pass") };
            const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "test-proxy:1080", PROXY_IP_USERNAME: "user", PROXY_IP_PASSWORD: "pass" }, testTimeout(env), fakeConnector);
            const conn = await egress.connect({ host: "example.com", port: 443 }, new AbortController().signal);
            const data = await readExactStream(conn.readable, 4);
            await conn.close();
            if (new TextDecoder().decode(data) !== "UPST")
                throw new Error("SOCKS5 buffered payload was not preserved");
            return { mode: "in-memory", authentication: "username-password", destination: "example.com:443" };
        }));
        tests.push(await runOne("egress.proxyip.no-auth", "ProxyIP SOCKS5 no-auth negotiation", "egress", async () => {
            const fakeConnector = { connect: async () => new FakeSocks5Socket(false, "example.com", 443, 3) };
            const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "test-proxy:1080", PROXY_IP_USERNAME: undefined, PROXY_IP_PASSWORD: undefined }, testTimeout(env), fakeConnector);
            const conn = await egress.connect({ host: "example.com", port: 443 }, new AbortController().signal);
            const data = await readExactStream(conn.readable, 4);
            await conn.close();
            if (new TextDecoder().decode(data) !== "UPST")
                throw new Error("SOCKS5 no-auth relay failed");
        }));
        tests.push(await runOne("egress.proxyip.config", "ProxyIP rejects incomplete authentication configuration", "egress", async () => {
            try {
                new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "test-proxy:1080", PROXY_IP_USERNAME: "user", PROXY_IP_PASSWORD: undefined });
            }
            catch (error) {
                if (error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR")
                    return;
                throw error;
            }
            throw new Error("Incomplete ProxyIP credentials were accepted");
        }));
        tests.push(await runOne("egress.proxyip.address-types", "ProxyIP encodes domain, IPv4 and IPv6 targets", "egress", async () => {
            const cases = [
                { host: "example.com", port: 443, atyp: 3 },
                { host: "1.2.3.4", port: 80, atyp: 1 },
                { host: "2001:db8::1", port: 443, atyp: 4 },
            ];
            for (const target of cases) {
                const fakeConnector = { connect: async () => new FakeSocks5Socket(false, target.host, target.port, target.atyp) };
                const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "test-proxy:1080", PROXY_IP_USERNAME: undefined, PROXY_IP_PASSWORD: undefined }, testTimeout(env), fakeConnector);
                const conn = await egress.connect(target, new AbortController().signal);
                await readExactStream(conn.readable, 4);
                await conn.close();
            }
        }));
        if (env.CORE_TEST_TARGET?.trim()) {
            tests.push(await runOne("egress.proxyip.live", "ProxyIP live SOCKS5 relay", "egress", async () => {
                const target = parseTarget(env.CORE_TEST_TARGET);
                const egress = new proxyip_1.ProxyIpEgress(env, testTimeout(env));
                const conn = await egress.connect(target, new AbortController().signal);
                try {
                    await optionalHttpExchange(conn, target);
                }
                finally {
                    await conn.close();
                }
                return { target: `${target.host}:${target.port}`, application_exchange: target.port === 80 };
            }));
        }
        else {
            tests.push(skipped("egress.proxyip.live", "ProxyIP live SOCKS5 relay", "egress", "CORE_TEST_TARGET is not configured"));
        }
    }
    // Companion ProxyIP regressions (fake connectors, always run). These used
    // to duplicate three of the block above test-for-test; they now cover the
    // distinct neighbouring behaviours — half-open recovery on success, the
    // handshake read-side stall, and the remaining endpoint-identity fields —
    // so no two registrations assert the same thing.
    tests.push(await runOne("egress.proxyip.half-open-abort-always", "Successful half-open probe restores a cooled-down ProxyIP endpoint", "egress", async (signal) => {
        let failing = true;
        const connector = { connect: async () => {
                if (failing)
                    throw new errors_1.CoreError("EGRESS_FAILED", "simulated proxy failure", true);
                return new FakeSocks5Socket(false, "example.com", 443, 3);
            } };
        const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "recovering-proxy:1080", CORE_MAX_PROXYIP_FAILURES: "1", CORE_PROXYIP_COOLDOWN_MS: "1000" }, 500, connector);
        try {
            await egress.connect({ host: "example.com", port: 443 }, signal);
            throw new Error("Failing connector unexpectedly connected");
        }
        catch (error) {
            if (!(error instanceof errors_1.CoreError) || error.code !== "EGRESS_FAILED")
                throw error;
        }
        const health = egress.health;
        const state = [...health.values()][0];
        // Read the state string into widened locals at each assertion point:
        // comparing state.state directly after the "open" guard makes tsc narrow
        // the property to the "open" literal across the intervening awaits and
        // report TS2367 on the "closed" comparison, even though connect() mutates
        // the field in between.
        const openedState = state.state;
        if (openedState !== "open")
            throw new Error("Endpoint did not open its circuit breaker after the configured failure threshold");
        state.cooldownUntil = Date.now() - 1;
        failing = false;
        const conn = await egress.connect({ host: "example.com", port: 443 }, signal);
        await conn.close();
        if (state.probeInFlight)
            throw new Error("Half-open probe marker remained set after a successful probe");
        const recoveredState = state.state;
        if (recoveredState !== "closed")
            throw new Error("Successful half-open probe did not close the circuit breaker");
        const again = await egress.connect({ host: "example.com", port: 443 }, signal);
        await again.close();
    }));
    tests.push(await runOne("egress.proxyip.handshake-write-timeout-always", "ProxyIP handshake read timeout closes a silent socket", "egress", async (signal) => {
        let closed = false;
        // Writes complete instantly but the peer never sends a single byte back:
        // the handshake must die on its read side, not only on a stalled write.
        const socket = { opened: Promise.resolve(), readable: new ReadableStream({}), writable: new WritableStream({ write: async () => { } }), close: async () => { closed = true; } };
        const egress = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: "read-stall-proxy:1080", CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS: "100" }, 500, { connect: async () => socket });
        try {
            await egress.connect({ host: "example.com", port: 443 }, signal);
        }
        catch (error) {
            if (!(error instanceof errors_1.CoreError) || !["TIMEOUT", "EGRESS_FAILED"].includes(error.code))
                throw error;
            if (!closed)
                throw new Error("Read-stalled handshake did not close the socket");
            return;
        }
        throw new Error("Read-stalled handshake unexpectedly completed");
    }));
    tests.push(await runOne("egress.proxyip.credential-identity-always", "ProxyIP endpoint identity includes TLS and username differences", "egress", async () => {
        const noopConnector = { connect: async () => new FakeSocks5Socket(false, "example.com", 443, 3) };
        const byTls = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: undefined, PROXY_IP_USERNAME: undefined, PROXY_IP_PASSWORD: undefined, PROXY_IP_ENDPOINTS_JSON: JSON.stringify([
                { host: "same-proxy.example.com", port: 1080, tls: false, username: "alice", password: "one" },
                { host: "same-proxy.example.com", port: 1080, tls: true, username: "alice", password: "one" },
            ]) }, 500, noopConnector);
        if (Number(byTls.diagnostics.endpoints) !== 2)
            throw new Error("ProxyIP endpoints differing only by TLS were deduplicated");
        const byUsername = new proxyip_1.ProxyIpEgress({ ...env, PROXY_IP: undefined, PROXY_IP_USERNAME: undefined, PROXY_IP_PASSWORD: undefined, PROXY_IP_ENDPOINTS_JSON: JSON.stringify([
                { host: "same-proxy.example.com", port: 1080, tls: false, username: "alice", password: "one" },
                { host: "same-proxy.example.com", port: 1080, tls: false, username: "bob", password: "one" },
            ]) }, 500, noopConnector);
        if (Number(byUsername.diagnostics.endpoints) !== 2)
            throw new Error("ProxyIP endpoints differing only by username were deduplicated");
    }));
    tests.push(await runOne("protocol.vless-version", "VLESS rejects non-zero protocol versions", "protocol", async (signal) => {
        const reader = new byte_queue_1.ByteQueue(8192);
        const body = (0, binary_1.concatBytes)(new Uint8Array([1]), vlessBytes, new Uint8Array([0]));
        reader.push(body);
        try {
            await new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes)).parse(reader, ctx(signal), signal);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "BAD_REQUEST")
                return;
            throw error;
        }
        throw new Error("Non-zero VLESS version was accepted");
    }));
    tests.push(await runOne("dns.auto-partial-success", "DNS auto mode preserves one successful address family when the other fails", "dns", async (signal) => {
        const requestFetch = async (_input, init) => {
            const query = new Uint8Array(init?.body);
            const id = new DataView(query.buffer, query.byteOffset, query.byteLength).getUint16(0);
            const qnameLength = query.findIndex((x, i) => i >= 12 && x === 0) - 12 + 1;
            const questionEnd = 12 + qnameLength + 4;
            const qtype = new DataView(query.buffer, query.byteOffset, query.byteLength).getUint16(questionEnd - 4);
            if (qtype === 28)
                return new Response("", { status: 503 });
            const out = new Uint8Array(questionEnd + 2 + 10 + 4);
            const view = new DataView(out.buffer);
            view.setUint16(0, id);
            view.setUint16(2, 0x8180);
            view.setUint16(4, 1);
            view.setUint16(6, 1);
            out.set(query.subarray(12, 12 + qnameLength), 12);
            out.set(new Uint8Array([0, 1, 0, 1]), 12 + qnameLength);
            out.set(new Uint8Array([0xc0, 0x0c]), questionEnd);
            let off = questionEnd + 2;
            view.setUint16(off, 1);
            view.setUint16(off + 2, 1);
            view.setUint32(off + 4, 30);
            view.setUint16(off + 8, 4);
            out.set([1, 2, 3, 4], off + 10);
            return new Response(out, { status: 200, headers: { "content-type": "application/dns-message" } });
        };
        const resolver = new doh_1.DoHResolver({ endpoint: "https://dns.example/dns-query", timeoutMs: 1000, retries: 0, fetchImpl: requestFetch });
        const result = await resolver.resolve("example.com", "auto", signal);
        if (result.length !== 1 || result[0]?.address !== "1.2.3.4")
            throw new Error("Auto DNS did not preserve successful A response");
    }));
    tests.push(await runOne("nat64.converter", "NAT64 follows all RFC 6052 prefix lengths and emits socket-safe IPv6", "egress", async () => {
        const cases = [
            ["2001:db8::/32", "2001:db8:c000:221::"],
            ["2001:db8:100::/40", "2001:db8:1c0:2:21::"],
            ["2001:db8:122::/48", "2001:db8:122:c000:2:2100::"],
            ["2001:db8:122:300::/56", "2001:db8:122:3c0:0:221::"],
            ["2001:db8:122:344::/64", "2001:db8:122:344:c0:2:2100::"],
            ["2001:db8:122:344::/96", "2001:db8:122:344::c000:221"],
        ];
        for (const [prefix, expected] of cases) {
            const converted = (0, nat64_1.ipv4ToNat64)("192.0.2.33", prefix);
            if (normalizeIpv6(converted) !== normalizeIpv6(expected) || converted.startsWith("[") || converted.endsWith("]"))
                throw new Error(`NAT64 mismatch for ${prefix}: ${converted}`);
        }
        if ((0, nat64_1.parseNat64Prefixes)("2602:fc59:b0:64::")[0]?.prefixLength !== 96)
            throw new Error("Legacy trailing-:: NAT64 form must remain /96 compatible");
    }));
    tests.push(await runOne("output.core-adapters", "Xray, Sing-box and Mihomo output adapters emit parseable configurations", "output", async () => {
        const node = { protocol: "vless", server: "proxy.example.com", port: 443, credential: vlessUuid, name: "node", security: "tls", scheme: "https", endpointKind: "domain", tls: { serverName: "proxy.example.com", alpn: ["http/1.1"], fingerprint: "chrome" }, websocket: { path: "/vless", host: "proxy.example.com" } };
        const xray = (0, xray_1.buildXrayConfig)([node]);
        const sing = (0, singbox_1.buildSingboxConfig)([node]);
        const clash = (0, clash_1.buildClashConfig)([node]);
        JSON.parse(JSON.stringify(xray));
        JSON.parse(JSON.stringify(sing));
        if (!clash.includes("type: vless") || !clash.includes("network: ws"))
            throw new Error("Mihomo configuration was malformed");
        const xout = xray.outbounds;
        if (!xout.length)
            throw new Error("Xray outbound missing");
        const xSettings = xout[0].settings;
        if (xSettings.packetEncoding !== "packetaddr")
            throw new Error("Xray VLESS packet encoding was not made explicit");
        const xUser = xSettings.vnext[0].users;
        if (!xUser[0]?.id)
            throw new Error("Xray VLESS user was not emitted");
        const xdns = xray.dns;
        if (!Array.isArray(xdns.servers) || xdns.servers.length !== 2 || xdns.disableFallback !== true)
            throw new Error("Xray DoH DNS policy was not emitted");
        const singOutbound = sing.outbounds[0];
        if (singOutbound.packet_encoding !== "packetaddr")
            throw new Error("Sing-box VLESS packet encoding was not made explicit");
        const trojanNode = { ...node, protocol: "trojan", credential: password, name: "trojan" };
        const trojanXray = (0, xray_1.buildXrayConfig)([trojanNode]);
        const trojanOutbound = trojanXray.outbounds[0];
        const mux = trojanOutbound.mux;
        if (mux?.enabled === true)
            throw new Error("Public Trojan Xray output must not enable client-side Mux");
        const stream = trojanOutbound.streamSettings;
        if (stream.network !== "ws")
            throw new Error("Xray WebSocket network selector is missing");
        const bestPing = (0, xray_1.buildXrayBestPingConfig)([node, { ...node, name: "node-2", server: "proxy2.example.com" }]);
        if (!bestPing.observatory?.probeUrl)
            throw new Error("Xray Observatory is missing for Best Ping");
    }));
    tests.push(await runOne("output.core-adapters-plaintext", "Native/Sing-box/Mihomo target boundaries preserve the Worker plaintext model", "output", async () => {
        const node = { protocol: "trojan", server: "proxy.example.com", port: 80, credential: password, name: "trojan-http", security: "none", scheme: "http", endpointKind: "domain", websocket: { path: "/trojan", host: "proxy.example.com" } };
        const sing = (0, singbox_1.buildSingboxConfig)([node]);
        JSON.parse(JSON.stringify(sing));
        let clashRejected = false;
        try {
            (0, clash_1.buildClashConfig)([node]);
        }
        catch (error) {
            clashRejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!clashRejected)
            throw new Error("Mihomo accepted unsupported public Trojan plaintext");
        let xrayRejected = false;
        try {
            (0, xray_1.buildXrayConfig)([node]);
        }
        catch (error) {
            xrayRejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!xrayRejected)
            throw new Error("Xray accepted unsupported public plaintext");
    }));
    tests.push(await runOne("output.xray-target-filter", "Xray target planning excludes public plaintext while native planning keeps both protocols", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "80,443" };
        const selection = { protocol: "all", endpoint: "all", ports: null, fingerprints: ["chrome"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node-{protocol}-{port}", limit: 16, offset: 0 };
        const records = {
            vless: [{ id: "v", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }],
            trojan: [{ id: "t", protocol: "trojan", credential: "digest", rawCredential: password, enabled: true }],
        };
        const provider = {
            count: async (protocol) => records[protocol].length,
            page: async (protocol, offset, limit) => records[protocol].slice(offset, offset + limit),
        };
        const nativePage = await (0, bundle_2.buildOutputNodePage)(env, "proxy.example.com", selection, provider, { defaultNodes: 16, maxNodes: 16, maxBytes: 64 * 1024 }, new AbortController().signal, "native");
        const xrayPage = await (0, bundle_2.buildOutputNodePage)(env, "proxy.example.com", selection, provider, { defaultNodes: 16, maxNodes: 16, maxBytes: 64 * 1024 }, new AbortController().signal, "xray");
        if (nativePage.candidateCount !== 4 || nativePage.items.length !== 4)
            throw new Error(`Native candidate planning mismatch: ${nativePage.candidateCount}/${nativePage.items.length}`);
        if (xrayPage.candidateCount !== 2 || xrayPage.items.some((node) => node.security !== "tls"))
            throw new Error(`Xray candidate filtering mismatch: ${xrayPage.candidateCount}`);
    }));
    tests.push(await runOne("output.budget", "Output node budget truncates instead of rejecting valid endpoint combinations", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", TLS_FINGERPRINT: "chrome,firefox" };
        const selection = { protocol: "vless", endpoint: "all", ports: null, fingerprints: ["chrome", "firefox"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node", limit: 1, offset: 0 };
        const record = { id: "1", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true };
        const nodes = (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", selection, { vless: [record] }, { defaultNodes: 256, maxNodes: 1, maxBytes: 4096 });
        if (nodes.length !== 1)
            throw new Error(`Expected one node, got ${nodes.length}`);
    }));
    tests.push(await runOne("output.selection-pagination", "Output pagination skips across fingerprint blocks without dropping or duplicating credentials", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", TLS_FINGERPRINT: "chrome,firefox" };
        const selection = { protocol: "vless", endpoint: "all", ports: null, fingerprints: ["chrome", "firefox"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node-{fingerprint}-{credential}", limit: 3, offset: 0 };
        const records = [1, 2, 3, 4].map((n) => ({ id: String(n), protocol: "vless", credential: `${vlessUuid}`, rawCredential: vlessUuid, enabled: true }));
        const page1 = (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", selection, { vless: records }, { defaultNodes: 256, maxNodes: 3, maxBytes: 4096 });
        const page2 = (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", { ...selection, offset: 5 }, { vless: records }, { defaultNodes: 256, maxNodes: 3, maxBytes: 4096 });
        // Offset 5 means one complete fingerprint block (4 candidates) plus one from firefox.
        if (page1.length !== 3 || page2.length !== 3)
            throw new Error(`Unexpected pagination result: ${page1.length}/${page2.length}`);
    }));
    tests.push(await runOne("output.http-none-both-protocols", "Canonical native output supports HTTP/none for VLESS and Trojan", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "80" };
        const selection = { protocol: "all", endpoint: "all", ports: null, fingerprints: ["chrome"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node-{protocol}", limit: 4, offset: 0 };
        const nodes = (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", selection, {
            vless: [{ id: "v", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }],
            trojan: [{ id: "t", protocol: "trojan", credential: "digest", rawCredential: password, enabled: true }],
        }, { defaultNodes: 256, maxNodes: 4, maxBytes: 8192 });
        if (nodes.length !== 2)
            throw new Error(`Expected two plaintext nodes, got ${nodes.length}`);
        if (nodes.some((node) => node.security !== "none" || node.scheme !== "http"))
            throw new Error("Plaintext native node metadata is inconsistent");
    }));
    tests.push(await runOne("engine.initial-payload-no-replay", "ProxyEngine never retries an empty upstream after parsed initial application payload", "engine", async () => {
        const clientReader = new byte_queue_1.ByteQueue(8192);
        const clientWriter = { write: async (_data) => { } };
        const domain = new TextEncoder().encode("example.com");
        clientReader.push((0, binary_1.concatBytes)(new Uint8Array([0]), vlessBytes, new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([1, 187]), new Uint8Array([2, domain.length]), domain, new TextEncoder().encode("HELLO")));
        let calls = 0;
        const upstreamFactory = (body) => ({ type: "direct", readable: new ReadableStream({ start(c) { if (body)
                    c.enqueue(new TextEncoder().encode(body)); c.close(); } }), writable: new WritableStream(), close: async () => { } });
        const direct = { type: "direct", connect: async () => { calls++; return calls === 1 ? upstreamFactory("") : upstreamFactory("SHOULD-NOT-HAPPEN"); } };
        const engine = new engine_1.ProxyEngine({ router: new router_1.DefaultRouter("direct"), protocols: new Map([["vless", new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes))]]), egress: (() => { const registry = new registry_1.EgressRegistry(); registry.register(direct); return registry; })(), handshakeTimeoutMs: 1000, retryPolicy: { maxInitialRetries: 1, firstByteTimeoutMs: 1000, retryOnEmptyResponse: true } });
        const connection = { reader: clientReader, writer: clientWriter, metadata: { type: "websocket", path: "/vless" }, close: async () => { } };
        try {
            await engine.handle(connection, { id: "initial-payload-no-replay", startedAt: Date.now(), signal: new AbortController().signal }, "vless");
            throw new Error("Engine unexpectedly completed after upstream closed before response data");
        }
        catch (error) {
            if (!(error instanceof errors_1.CoreError) || error.code !== "UPSTREAM_CLOSED")
                throw error;
        }
        if (calls !== 1)
            throw new Error(`Initial payload was eligible for replay; egress attempts=${calls}`);
    }));
    tests.push(await runOne("engine.empty-response-retry", "ProxyEngine retries an upstream that closes before first response data when no application payload was sent", "engine", async () => {
        const clientReader = new byte_queue_1.ByteQueue(8192);
        const clientWriter = { write: async (_data) => { } };
        const domain = new TextEncoder().encode("example.com");
        clientReader.push((0, binary_1.concatBytes)(new Uint8Array([0]), vlessBytes, new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([1, 187]), new Uint8Array([2, domain.length]), domain));
        let calls = 0;
        const upstreamFactory = (type, body) => ({ type, readable: new ReadableStream({ start(c) { if (body)
                    c.enqueue(new TextEncoder().encode(body)); c.close(); } }), writable: new WritableStream(), close: async () => { } });
        const direct = { type: "direct", connect: async () => { calls++; return upstreamFactory("direct", ""); } };
        const proxy = { type: "proxyip", connect: async () => { calls++; return upstreamFactory("proxyip", "OK"); } };
        const engine = new engine_1.ProxyEngine({ router: new router_1.DefaultRouter("smart", undefined, { proxyip: true }), protocols: new Map([["vless", new parser_1.VlessHandler(new StaticProvider("vless", vlessBytes))]]), egress: (() => { const registry = new registry_1.EgressRegistry(); registry.register(direct); registry.register(proxy); return registry; })(), handshakeTimeoutMs: 1000, retryPolicy: { maxInitialRetries: 1, firstByteTimeoutMs: 1000, retryOnEmptyResponse: true } });
        const connection = { reader: clientReader, writer: clientWriter, metadata: { type: "websocket", path: "/vless" }, close: async () => { } };
        await engine.handle(connection, { id: "empty-retry", startedAt: Date.now(), signal: new AbortController().signal }, "vless");
        if (calls !== 2)
            throw new Error(`Expected one retry on the next egress, got ${calls}`);
    }));
    tests.push(await runOne("output.cleanip-port-allowlist", "Explicit Clean-IP ports cannot bypass OUTPUT_PORT", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", CLEAN_IPS: "1.2.3.4:80" };
        const selection = { protocol: "vless", endpoint: "cleanip", ports: null, fingerprints: ["chrome"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node", limit: 4, offset: 0 };
        const record = { id: "1", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true };
        try {
            (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", selection, { vless: [record] });
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR")
                return;
            throw error;
        }
        throw new Error("Explicit Clean-IP port 80 bypassed OUTPUT_PORT=443");
    }));
    tests.push(await runOne("output.rendered-fragment-budget", "Fragment artifact counts rendered proxy variants rather than base nodes", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", TLS_FINGERPRINT: "chrome", CORE_OUTPUT_MAX_NODES: "512", CORE_OUTPUT_MAX_BYTES: "512000" };
        const selection = { protocol: "vless", endpoint: "domain", ports: null, fingerprints: ["chrome"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node", limit: 1, offset: 0 };
        const node = (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", selection, { vless: [{ id: "1", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }] })[0];
        const profiles = (0, profiles_1.getXrayFragmentProfiles)(env);
        const config = (0, xray_1.buildXrayBestFragmentConfig)([node], profiles);
        const outbounds = config.outbounds;
        if (outbounds.length !== profiles.length)
            throw new Error(`Expected ${profiles.length} rendered fragment outbounds, got ${outbounds.length}`);
    }));
    tests.push(await runOne("output.fragment-pagination-offset", "Rendered Fragment counts do not corrupt base-node pagination offsets", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", TLS_FINGERPRINT: "chrome", CORE_OUTPUT_MAX_NODES: "40", CORE_OUTPUT_MAX_BYTES: "1048576" };
        const records = [1, 2, 3].map((id) => ({ id: String(id), protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }));
        const provider = { authenticate: async () => null, count: async (protocol) => protocol === "vless" ? records.length : 0, listPage: async (protocol, offset, limit) => protocol === "vless" ? records.slice(offset, offset + limit) : [] };
        const request = new Request("https://proxy.example.com/__core/outputs/xray?artifact=best-fragment&protocol=vless");
        const xray = await (0, artifacts_1.renderOutputArtifact)(env, request, provider, "xray");
        if (xray.headers?.["X-Core-Output-Count"] !== "40" || xray.headers?.["X-Core-Output-Total"] !== "60" || xray.headers?.["X-Core-Output-Next-Offset"] !== "2")
            throw new Error(`Xray fragment pagination contract mismatch: ${JSON.stringify(xray.headers)}`);
        if (!xray.headers?.["X-Core-Client-Requirements"]?.includes("v26.6.27"))
            throw new Error("Xray Fragment compatibility header is missing");
        const fragmentConfig = JSON.parse(xray.body);
        const observatory = fragmentConfig.observatory;
        if (observatory?.probeInterval !== "1s" || observatory?.enableConcurrency !== false)
            throw new Error("Fragment Observatory must default to serial 1s probes");
        const limited = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/xray?artifact=best-fragment&protocol=vless&limit=1"), provider, "xray");
        if (limited.headers?.["X-Core-Output-Count"] !== "20" || limited.headers?.["X-Core-Output-Next-Offset"] !== "1")
            throw new Error(`Fragment requested limit was not honored: ${JSON.stringify(limited.headers)}`);
        const singbox = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/sing-box?artifact=best-fragment&protocol=vless"), provider, "sing-box");
        if (!singbox.headers?.["X-Core-Client-Requirements"]?.includes("1.12.0"))
            throw new Error("Sing-box Fragment compatibility header is missing");
        if (singbox.headers?.["X-Core-Output-Count"] !== "6" || singbox.headers?.["X-Core-Output-Total"] !== "6" || "X-Core-Output-Next-Offset" in (singbox.headers ?? {}))
            throw new Error(`Sing-box fragment pagination contract mismatch: ${JSON.stringify(singbox.headers)}`);
        const xrayManifestResponse = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/xray?artifact=manifest&protocol=vless"), provider, "xray");
        const xrayManifest = JSON.parse(xrayManifestResponse.body);
        if (!xrayManifest.artifacts?.find((item) => item.id === "best-fragment")?.clientRequirements?.some((item) => item.includes("v26.6.27")))
            throw new Error("Xray Fragment client requirements missing from manifest");
        const singboxManifestResponse = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/sing-box?artifact=manifest&protocol=vless"), provider, "sing-box");
        const singboxManifest = JSON.parse(singboxManifestResponse.body);
        if (!singboxManifest.artifacts?.find((item) => item.id === "best-ping-fragment")?.clientRequirements?.some((item) => item.includes("1.12.0")))
            throw new Error("Sing-box Fragment client requirements missing from manifest");
    }));
    tests.push(await runOne("output.fragment-observatory-overrides", "Fragment probe cadence and concurrency are bounded by defaults but remain operator-overridable", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", CORE_OUTPUT_MAX_NODES: "20", CORE_XRAY_FRAGMENT_PROBE_INTERVAL: "5s", CORE_XRAY_FRAGMENT_OBSERVATORY_CONCURRENCY: "true" };
        const records = [1, 2].map((id) => ({ id: String(id), protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }));
        const provider = { authenticate: async () => null, count: async (protocol) => protocol === "vless" ? records.length : 0, listPage: async (protocol, offset, limit) => protocol === "vless" ? records.slice(offset, offset + limit) : [] };
        const rendered = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/xray?artifact=best-fragment&protocol=vless&limit=1"), provider, "xray");
        const config = JSON.parse(rendered.body);
        const observatory = config.observatory;
        if (observatory?.probeInterval !== "5s" || observatory?.enableConcurrency !== true)
            throw new Error(`Fragment Observatory overrides were ignored: ${JSON.stringify(observatory)}`);
    }));
    tests.push(await runOne("output.best-ping-fragment-composition", "Xray Best Fragment and Best Ping Fragment are documented aliases; Sing-box fragment groups use urltest selection", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443" };
        const selection = { protocol: "vless", endpoint: "domain", ports: null, fingerprints: ["chrome"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node", limit: 1, offset: 0 };
        const node = (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", selection, { vless: [{ id: "1", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }] })[0];
        const xp = (0, profiles_1.getXrayFragmentProfiles)(env).slice(0, 2);
        const xb = (0, xray_1.buildXrayBestFragmentConfig)([node], xp);
        const xbp = (0, xray_1.buildXrayBestPingFragmentConfig)([node], xp);
        const xbr = xb.routing.balancers?.[0]?.strategy;
        const xbpr = xbp.routing.balancers?.[0]?.strategy;
        if (xbr?.type !== "leastPing" || xbpr?.type !== "leastPing")
            throw new Error("Xray Best Fragment / Best Ping Fragment alias strategy changed unexpectedly");
        const bestPingSingle = (0, xray_1.buildXrayBestPingConfig)([node]);
        if (bestPingSingle.observatory || bestPingSingle.routing.balancers?.length)
            throw new Error("Single-node Best Ping should not create Observatory/balancer");
        const bestPingTimed = (0, xray_1.buildXrayBestPingConfig)([node, { ...node, name: "node-2", server: "proxy2.example.com" }], { probeInterval: "60s", observatoryConcurrency: false });
        const timedObservatory = bestPingTimed.observatory;
        if (timedObservatory.probeInterval !== "60s" || timedObservatory.enableConcurrency !== false)
            throw new Error("Xray Observatory cadence/concurrency options were not propagated");
        const sb = (0, singbox_1.buildSingboxBestFragmentConfig)([node], (0, profiles_1.getSingboxFragmentProfiles)());
        const sbp = (0, singbox_1.buildSingboxBestPingFragmentConfig)([node], (0, profiles_1.getSingboxFragmentProfiles)());
        const sbGroup = sb.outbounds.at(-2);
        const sbpGroup = sbp.outbounds.at(-2);
        if (sbGroup?.type !== "urltest" || sbpGroup?.type !== "urltest")
            throw new Error("Sing-box Best Fragment / Best Ping Fragment groups are not both urltest selection groups");
    }));
    tests.push(await runOne("output.xray-fail-closed", "Xray Best Ping is fail-closed unless direct fallback is explicitly enabled", "output", async () => {
        const makeNode = (name) => ({ protocol: "vless", server: `${name}.example.com`, port: 443, credential: vlessUuid, name, security: "tls", scheme: "https", endpointKind: "domain", tls: { serverName: `${name}.example.com`, alpn: ["http/1.1"], fingerprint: "chrome" }, websocket: { path: "/vless", host: `${name}.example.com` } });
        const closed = (0, xray_1.buildXrayBestPingConfig)([makeNode("a"), makeNode("b")]);
        const open = (0, xray_1.buildXrayBestPingConfig)([makeNode("a"), makeNode("b")], { allowDirectFallback: true });
        const closedOut = closed.outbounds;
        const closedBalancer = closed.routing.balancers[0];
        const openOut = open.outbounds;
        const openBalancer = open.routing.balancers[0];
        if (closedOut.some((item) => item.tag === "direct") || "fallbackTag" in closedBalancer)
            throw new Error("Implicit Xray direct fallback remains enabled");
        if (!openOut.some((item) => item.tag === "direct") || openBalancer.fallbackTag !== "direct")
            throw new Error("Explicit Xray direct fallback was not honored");
    }));
    tests.push(await runOne("output.clash-artifact-gate", "Mihomo node and bundle artifacts are not incorrectly rejected", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", TLS_FINGERPRINT: "chrome" };
        const provider = { authenticate: async () => null, count: async (protocol) => protocol === "vless" ? 1 : 0, listPage: async (protocol, offset, limit) => protocol === "vless" && offset === 0 && limit > 0 ? [{ id: "1", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }] : [] };
        const node = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/clash?artifact=node&protocol=vless"), provider, "clash");
        const bundle = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/clash?artifact=bundle&protocol=vless"), provider, "clash");
        if (!node.body.includes("type: vless") || !bundle.body.includes("type: vless"))
            throw new Error("Mihomo node/bundle artifact was rejected or malformed");
    }));
    tests.push(await runOne("output.artifact-selection-preservation", "Manifest artifact URLs preserve original selection parameters", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", TLS_FINGERPRINT: "chrome" };
        const provider = { authenticate: async () => null, count: async (protocol) => protocol === "vless" ? 1 : 0, listPage: async (protocol, offset, limit) => protocol === "vless" && offset === 0 && limit > 0 ? [{ id: "1", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }] : [] };
        const artifact = await (0, artifacts_1.renderOutputArtifact)(env, new Request("https://proxy.example.com/__core/outputs/xray?artifact=manifest&protocol=vless&port=443&fingerprint=firefox&remark=keep-me&token=secret"), provider, "xray");
        const manifest = JSON.parse(artifact.body);
        const nodeDescriptor = manifest.artifacts.find((item) => item.id === "nodes");
        const nodeUrl = nodeDescriptor?.url ?? "";
        const query = new URL(nodeUrl).searchParams;
        if (query.get("protocol") !== "vless" || query.get("port") !== "443" || query.get("fingerprint") !== "firefox" || query.get("remark") !== "keep-me" || query.get("artifact") !== "node" || query.has("token") || nodeDescriptor?.auth !== "authorization-bearer")
            throw new Error(`Manifest token propagation/auth contract failed: ${nodeUrl}`);
    }));
    tests.push(await runOne("config.routing-provider-consistency", "CORE_RULES accepts configured relay and rejects non-dialable fallback as an egress", "configuration", async () => {
        (0, validation_1.validateEnvironment)({ VLESS_USERS: vlessUuid, TROJAN_USERS: password, PROXY_RELAY_ENDPOINTS: "relay.example.com:443", CORE_RULES: "host=example.com|relay" });
        let rejected = false;
        try {
            (0, validation_1.validateEnvironment)({ VLESS_USERS: vlessUuid, TROJAN_USERS: password, CORE_RULES: "host=example.com|fallback" });
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejected)
            throw new Error("Non-dialable CORE_RULES fallback egress was accepted");
    }));
    tests.push(await runOne("output.early-data-env-path", "CORE_OUTPUT_EARLY_DATA travels through env parsing into generated nodes", "output", async () => {
        const env = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", CORE_OUTPUT_EARLY_DATA: "2560" };
        (0, validation_1.validateEnvironment)(env);
        const selection = (0, bundle_1.parseOutputSelection)(env, new URLSearchParams("protocol=vless&limit=1"));
        const nodes = (0, bundle_2.buildOutputNodesFromRecords)(env, "proxy.example.com", selection, { vless: [{ id: "1", protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true }] });
        if (nodes[0]?.websocket.earlyData !== 2560)
            throw new Error("CORE_OUTPUT_EARLY_DATA was lost before node rendering");
        let rejected = false;
        try {
            (0, validation_1.validateEnvironment)({ ...env, CORE_OUTPUT_EARLY_DATA: "8193" });
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejected)
            throw new Error("CORE_OUTPUT_EARLY_DATA > 8192 was accepted");
    }));
    tests.push(await runOne("config.credential-mode-contract", "Credential mode validation matches runtime prerequisites", "configuration", async () => {
        const base = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443" };
        let rejectedD1 = false;
        try {
            (0, validation_1.validateEnvironment)({ ...base, VLESS_USERS: undefined, TROJAN_USERS: undefined, CORE_CREDENTIAL_MODE: "d1" });
        }
        catch (error) {
            rejectedD1 = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejectedD1)
            throw new Error("d1 mode without DB was accepted");
        let rejectedEnv = false;
        try {
            (0, validation_1.validateEnvironment)({ ...base, DB: {}, VLESS_USERS: undefined, TROJAN_USERS: undefined, CORE_CREDENTIAL_MODE: "env" });
        }
        catch (error) {
            rejectedEnv = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejectedEnv)
            throw new Error("env mode without environment credentials was accepted");
        // The positive d1 case needs a minimal D1-shaped double: a bare {} is
        // exactly the wrong-binding accident C6 binding validation rejects.
        const d1Double = { prepare: () => ({}), batch: async () => [], exec: async () => ({}) };
        (0, validation_1.validateEnvironment)({ ...base, DB: d1Double, VLESS_USERS: undefined, TROJAN_USERS: undefined, CORE_CREDENTIAL_MODE: "d1" });
    }));
    tests.push(await runOne("config.structured-proxyip-rules", "Structured-only ProxyIP satisfies routing provider validation", "configuration", async (signal) => {
        void signal;
        const base = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", PROXY_IP_ENDPOINTS_JSON: JSON.stringify([{ host: "proxy.example.com", port: 1080 }]), CORE_RULES: "host=example.com|proxyip" };
        (0, validation_1.validateEnvironment)(base);
        if (!(0, proxyip_1.hasProxyIp)(base))
            throw new Error("Structured ProxyIP provider was not recognized");
    }));
    tests.push(await runOne("config.strict-routing-rules", "CORE_RULES rejects empty and unknown matchers plus invalid ports", "configuration", async () => {
        const base = { VLESS_USERS: vlessUuid, TROJAN_USERS: password, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443" };
        for (const rules of ["host=example.com|direct|garbage", "host=|direct", "host=|proxyip", "suffix=|direct", "badkind=example.com|direct", "port=0|direct", "port=65536|direct"]) {
            let rejected = false;
            try {
                (0, validation_1.validateEnvironment)({ ...base, CORE_RULES: rules, PROXY_IP_ENDPOINTS_JSON: JSON.stringify([{ host: "proxy.example.com", port: 1080 }]) });
            }
            catch (error) {
                rejected = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
            }
            if (!rejected)
                throw new Error(`Invalid routing rule was accepted: ${rules}`);
            try {
                (0, rules_1.parseCoreRule)(rules);
                throw new Error(`Shared CORE_RULES parser accepted invalid rule: ${rules}`);
            }
            catch (error) {
                if (error instanceof Error && error.message.startsWith("Shared CORE_RULES parser"))
                    throw error;
                if (!(error instanceof errors_1.CoreError) || error.code !== "CONFIGURATION_ERROR")
                    throw error;
            }
        }
        const parsed = (0, rules_1.parseCoreRule)("host=Example.COM|direct");
        if (parsed.host !== "example.com" || parsed.egress !== "direct")
            throw new Error(`Shared CORE_RULES parser canonicalization failed: ${JSON.stringify(parsed)}`);
    }));
    tests.push(await runOne("output.mihomo-trojan-sni", "Mihomo Trojan TLS output uses the documented sni field", "output", async () => {
        const node = { protocol: "trojan", server: "203.0.113.10", port: 443, credential: password, name: "trojan-clean-ip", security: "tls", scheme: "https", endpointKind: "clean-ip", tls: { serverName: "origin.example.com", alpn: ["http/1.1"], fingerprint: "chrome" }, websocket: { path: "/trojan", host: "origin.example.com" } };
        const yaml = (0, clash_1.buildClashConfig)([node]);
        if (!yaml.includes("    sni: \"origin.example.com\"") || yaml.includes("    servername:"))
            throw new Error(`Trojan Mihomo TLS field mismatch: ${yaml}`);
    }));
    tests.push(await runOne("output.aggregation-cache-policy", "External subscription cache preserves maxBytes isolation", "output", async (signal) => {
        const source = "https://cache-policy.example.com/list";
        const env = { OUTPUT_CUSTOM_SUBS: source, CORE_OUTPUT_SUBS_ALLOWLIST: "cache-policy.example.com" };
        const resolver = { resolve: async () => [{ address: "93.184.216.34", family: 4, ttl: 60 }] };
        let calls = 0;
        const first = await (0, aggregation_1.fetchNativeSubscriptionSources)(env, signal, 8, 1024, async () => { calls++; return new Response("vless://user@example.com:443?x=" + "a".repeat(64), { status: 200 }); }, resolver);
        if (!first.length)
            throw new Error("First external subscription fetch produced no node");
        let rejected = false;
        try {
            await (0, aggregation_1.fetchNativeSubscriptionSources)(env, signal, 8, 16, async () => { calls++; return new Response("vless://user@example.com:443?x=" + "a".repeat(64), { status: 200 }); }, resolver);
        }
        catch (error) {
            rejected = error instanceof errors_1.CoreError && error.code === "MESSAGE_TOO_LARGE";
        }
        if (!rejected || calls !== 2)
            throw new Error(`Aggregation cache ignored maxBytes policy: rejected=${rejected}, calls=${calls}`);
    }));
    tests.push(await runOne("output.early-data-all-targets", "Early-data output is synchronized across native, Xray, Sing-box and Mihomo", "output", async () => {
        const node = { protocol: "vless", server: "proxy.example.com", port: 443, credential: vlessUuid, name: "node", security: "tls", scheme: "https", endpointKind: "domain", tls: { serverName: "proxy.example.com", alpn: ["http/1.1"], fingerprint: "chrome" }, websocket: { path: "/vless", host: "proxy.example.com", earlyData: 2560 } };
        const share = (0, share_link_1.generateShareLink)(node);
        const xray = (0, xray_1.buildXrayConfig)([node]);
        const sing = (0, singbox_1.buildSingboxConfig)([node]);
        const clash = (0, clash_1.buildClashConfig)([node]);
        if (!(new URL(share).searchParams.get("path") ?? "").includes("ed=2560"))
            throw new Error("Native share link lost ed=2560");
        const xws = xray.outbounds[0].streamSettings.wsSettings;
        if (!String(xws.path).includes("ed=2560") || "maxEarlyData" in xws || "earlyDataHeaderName" in xws)
            throw new Error("Xray early-data mapping mismatch");
        const tr = sing.outbounds[0].transport;
        if (tr.max_early_data !== 2560 || tr.early_data_header_name !== "Sec-WebSocket-Protocol")
            throw new Error("Sing-box early-data mapping mismatch");
        if (!clash.includes("max-early-data: 2560") || !clash.includes("early-data-header-name: Sec-WebSocket-Protocol"))
            throw new Error("Mihomo early-data mapping mismatch");
    }));
    tests.push(await runOne("output.singbox-utls-opt-in", "Sing-box uTLS follows the selected TLS fingerprint; the legacy opt-in flag is still accepted", "output", async () => {
        const node = { protocol: "vless", server: "proxy.example.com", port: 443, credential: vlessUuid, name: "node", security: "tls", scheme: "https", endpointKind: "domain", tls: { serverName: "proxy.example.com", alpn: ["http/1.1"], fingerprint: "chrome" }, websocket: { path: "/vless", host: "proxy.example.com" } };
        const normal = (0, singbox_1.buildSingboxConfig)([node]);
        const opted = (0, singbox_1.buildSingboxConfig)([node], { useUtls: true });
        const ntls = normal.outbounds[0].tls;
        const otls = opted.outbounds[0].tls;
        // F3: a selected fingerprint alone is enough — utls is emitted with or
        // without the legacy CORE_OUTPUT_SINGBOX_UTLS opt-in flag.
        const nutls = ntls.utls;
        const outls = otls.utls;
        if (!nutls || nutls.enabled !== true || nutls.fingerprint !== "chrome")
            throw new Error("Sing-box uTLS was not emitted for a selected fingerprint without the legacy flag");
        if (!outls || outls.enabled !== true || outls.fingerprint !== "chrome")
            throw new Error("Sing-box uTLS legacy opt-in flag is no longer accepted");
    }));
    tests.push(await runOne("output.candidate-ceiling", "Output candidate evaluation has a hard safety ceiling", "output", async (signal) => {
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: "proxy.example.com", OUTPUT_PORT: "443", TLS_FINGERPRINT: "chrome" };
        let pageCalled = false;
        const provider = { authenticate: async () => null, count: async () => 1_000_001, listPage: async () => { pageCalled = true; throw new Error("page() must not run past candidate ceiling"); } };
        const selection = { protocol: "vless", endpoint: "domain", ports: null, fingerprints: ["chrome"], alpn: ["http/1.1"], transport: "ws", remarkTemplate: "node", limit: 1, offset: 0 };
        try {
            await (0, bundle_2.buildOutputNodePage)(env, "proxy.example.com", selection, { count: provider.count, page: provider.listPage }, { defaultNodes: 256, maxNodes: 512, maxBytes: 512 * 1024 }, signal);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR") {
                if (pageCalled)
                    throw new Error("Credential page was called before candidate ceiling rejection");
                return;
            }
            throw error;
        }
        throw new Error("Candidate ceiling did not reject oversized evaluation space");
    }));
    tests.push(await runOne("storage.hybrid-lazy-page", "Hybrid credential paging avoids full-list materialization", "storage", async (signal) => {
        const primaryRows = [{ id: "p1", protocol: "vless", credential: "p1", rawCredential: "p1", enabled: true }, { id: "p2", protocol: "vless", credential: "p2", rawCredential: "p2", enabled: true }];
        const fallbackRows = [{ id: "f1", protocol: "vless", credential: "f1", rawCredential: "f1", enabled: true }, { id: "f2", protocol: "vless", credential: "f2", rawCredential: "f2", enabled: true }];
        let primaryListCalled = false;
        let fallbackListCalled = false;
        const primary = { authenticate: async () => null, count: async () => 2, list: async () => { primaryListCalled = true; throw new Error("primary list() must not be used by paged hybrid output"); }, listPage: async (_p, offset, limit) => primaryRows.slice(offset, offset + limit) };
        const fallback = { authenticate: async () => null, count: async () => 2, list: async () => { fallbackListCalled = true; throw new Error("fallback list() must not be used by paged hybrid output"); }, listPage: async (_p, offset, limit) => fallbackRows.slice(offset, offset + limit) };
        const repo = new cloudflare_3.CredentialRepository(primary, fallback, "hybrid");
        const page = await repo.listPage("vless", 0, 2, signal);
        if (page.length !== 2 || primaryListCalled || fallbackListCalled)
            throw new Error("Hybrid page materialized a full source list");
        const next = await repo.listPage("vless", 2, 2, signal);
        if (next.length !== 2 || primaryListCalled || fallbackListCalled)
            throw new Error("Hybrid fallback paging used full-list materialization");
    }));
    tests.push(await runOne("aggregation.ssrf-policy", "External subscriptions require an explicit public-domain allowlist", "security", async (signal) => {
        const base = { OUTPUT_CUSTOM_SUBS: "https://subs.example.com/list" };
        try {
            (0, aggregation_1.validateSubscriptionUrl)("https://subs.example.com/list", base);
        }
        catch (error) {
            if (!(error instanceof errors_1.CoreError) || error.code !== "CONFIGURATION_ERROR")
                throw error;
        }
        const allowed = { ...base, CORE_OUTPUT_SUBS_ALLOWLIST: "subs.example.com" };
        if ((0, aggregation_1.validateSubscriptionUrl)("https://subs.example.com/list", allowed).hostname !== "subs.example.com")
            throw new Error("Allowed subscription host was rejected");
        for (const bad of ["http://127.0.0.1:8080/x", "http://0x7f000001/x", "https://[::ffff:127.0.0.1]/x", "https://evil.example.net/x", "https://localhost/x"]) {
            try {
                (0, aggregation_1.validateSubscriptionUrl)(bad, allowed);
            }
            catch (error) {
                if (error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR")
                    continue;
                throw error;
            }
            throw new Error(`SSRF policy accepted ${bad}`);
        }
        const fakeResolver = { resolve: async () => [{ address: "10.1.2.3" }] };
        try {
            const { fetchNativeSubscriptionSources } = await Promise.resolve().then(() => __r("../output/aggregation"));
            await fetchNativeSubscriptionSources({ ...allowed, OUTPUT_CUSTOM_SUBS: "https://subs.example.com/list" }, signal, 8, 1024, async () => new Response("vless://ignored@example.com"), fakeResolver);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR")
                return;
            throw error;
        }
        throw new Error("SSRF DNS validation accepted a private resolved address");
    }));
    tests.push(await runOne("dns.doh-body-limit", "DoH resolver rejects oversized response bodies before parsing", "dns", async (signal) => {
        const huge = new Uint8Array(limits_1.CORE_LIMITS.maxDoHResponseBytes + 1);
        const resolver = new doh_1.DoHResolver({ endpoint: "https://dns.example/dns-query", timeoutMs: 1000, retries: 0, fetchImpl: async () => new Response(huge, { status: 200, headers: { "content-type": "application/dns-message", "content-length": String(huge.byteLength) } }) });
        try {
            await resolver.resolve("example.com", "ipv4", signal);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "MESSAGE_TOO_LARGE")
                return;
            throw error;
        }
        throw new Error("Oversized DoH response was accepted");
    }));
    tests.push(await runOne("dns.doh-redirect-policy", "DoH resolver uses manual redirect mode and rejects redirect responses", "dns", async (signal) => {
        let checkedRedirect = false;
        const resolver = new doh_1.DoHResolver({
            endpoint: "https://dns.example/dns-query", timeoutMs: 1000, retries: 0,
            fetchImpl: async (_input, init) => {
                if (init?.redirect !== "manual")
                    throw new Error("DoH resolver did not set manual redirect mode");
                checkedRedirect = true;
                return new Response(null, { status: 302, headers: { location: "https://other.example/dns-query" } });
            },
        });
        try {
            await resolver.resolve("example.com", "ipv4", signal);
        }
        catch (error) {
            if (checkedRedirect && error instanceof errors_1.CoreError && error.code === "DNS_FAILED")
                return;
            throw error;
        }
        throw new Error("DoH redirect response was accepted");
    }));
    tests.push(await runOne("aggregation.http-timeout-utf8", "External aggregation enforces HTTPS policy, timeout and UTF-8 decoding", "security", async (signal) => {
        const base = { OUTPUT_CUSTOM_SUBS: "https://subs.example.com/list", CORE_OUTPUT_SUBS_ALLOWLIST: "subs.example.com", CORE_OUTPUT_SUBS_TIMEOUT_MS: "250" };
        let rejectedHttp = false;
        try {
            (0, aggregation_1.validateSubscriptionUrl)("http://subs.example.com/list", base);
        }
        catch (error) {
            rejectedHttp = error instanceof errors_1.CoreError && error.code === "CONFIGURATION_ERROR";
        }
        if (!rejectedHttp)
            throw new Error("HTTP external subscription was accepted without explicit opt-in");
        const resolver = { resolve: async () => [{ address: "93.184.216.34" }] };
        const skipped = await (0, aggregation_1.fetchNativeSubscriptionSources)(base, signal, 8, 1024, async (_input, init) => new Promise((_, reject) => {
            const signal = init?.signal;
            if (signal?.aborted) {
                reject(signal.reason);
                return;
            }
            signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        }), resolver);
        if (skipped.length !== 0)
            throw new Error("Unavailable external source was not isolated from aggregation");
        let badUtf8 = false;
        try {
            await (0, aggregation_1.fetchNativeSubscriptionSources)(base, signal, 8, 1024, async () => new Response(new Uint8Array([0xff, 0xfe]), { status: 200 }), resolver);
        }
        catch (error) {
            badUtf8 = error instanceof errors_1.CoreError && error.code === "BAD_REQUEST";
        }
        if (!badUtf8)
            throw new Error("Invalid UTF-8 external subscription was not rejected");
    }));
    tests.push(await runOne("aggregation.pagination-contract", "External aggregation never corrupts native pagination offsets", "output", async () => {
        const longHost = `${"a".repeat(60)}.${"b".repeat(60)}.${"c".repeat(60)}.${"d".repeat(60)}`;
        const env = { VLESS_USERS: vlessUuid, OUTPUT_HOST: longHost, OUTPUT_PORT: "443", CORE_OUTPUT_MAX_NODES: "3", CORE_OUTPUT_MAX_BYTES: "1024", OUTPUT_CUSTOM_SUBS: "https://subs.example.com/list", CORE_OUTPUT_SUBS_ALLOWLIST: "subs.example.com" };
        const provider = { authenticate: async () => null, count: async (protocol) => protocol === "vless" ? 4 : 0, listPage: async (protocol, offset, limit) => protocol === "vless" ? [1, 2, 3, 4].slice(offset, offset + limit).map((id) => ({ id: String(id), protocol: "vless", credential: vlessUuid, rawCredential: vlessUuid, enabled: true })) : [] };
        const resolver = { resolve: async () => [{ address: "93.184.216.34", family: 4, ttl: 60 }] };
        const output = await (0, service_1.renderSubscriptionWithProvider)(env, new Request("https://proxy.example.com/sub?protocol=vless&limit=3&aggregate=true&encode=raw"), provider, resolver, async () => new Response("vless://external@example.com:443?security=tls&type=ws&host=example.com&path=%2Fvless"));
        if (output.headers?.["X-Core-Output-Next-Offset"] !== "3")
            throw new Error(`Native pagination offset was changed by rendering/aggregation: ${JSON.stringify(output.headers)}`);
        if (output.headers?.["X-Core-Output-Total"] !== "4")
            throw new Error(`Native candidate total was changed by aggregation: ${JSON.stringify(output.headers)}`);
    }));
    tests.push(await runOne("dns.prefer-family-fallback", "Preferred DNS family falls back after resolver exceptions", "dns", async (signal) => {
        let calls = 0;
        const resolver = new doh_1.DoHResolver({
            endpoint: "https://dns.example/dns-query",
            timeoutMs: 1000,
            retries: 0,
            fetchImpl: async (_input, init) => {
                calls++;
                if (calls === 1)
                    throw new errors_1.CoreError("DNS_FAILED", "simulated A failure", true);
                const body = init?.body;
                const id = new DataView(body.buffer, body.byteOffset, body.byteLength).getUint16(0);
                const qnameLength = body.findIndex((value, index) => index >= 12 && value === 0) - 12 + 1;
                const questionEnd = 12 + qnameLength + 4;
                const out = new Uint8Array(questionEnd + 2 + 10 + 16);
                const view = new DataView(out.buffer);
                view.setUint16(0, id);
                view.setUint16(2, 0x8180);
                view.setUint16(4, 1);
                view.setUint16(6, 1);
                out.set(body.subarray(12, questionEnd), 12);
                out.set([0xc0, 0x0c], questionEnd);
                let off = questionEnd + 2;
                view.setUint16(off, 28);
                view.setUint16(off + 2, 1);
                view.setUint32(off + 4, 60);
                view.setUint16(off + 8, 16);
                for (let i = 0; i < 15; i++)
                    out[off + 10 + i] = 0;
                out[off + 25] = 1;
                return new Response(out, { status: 200 });
            },
        });
        const values = await resolver.resolve("example.com", "prefer_ipv4", signal);
        if (calls !== 2 || values[0]?.family !== 6)
            throw new Error(`Preferred-family fallback failed: ${JSON.stringify(values)}`);
    }));
    tests.push(await runOne("dns.cname-chain-ttl", "DNS CNAME resolution caps answer TTL to the full chain lifetime", "dns", async (signal) => {
        let calls = 0;
        const requestFetch = async (_input, init) => {
            const query = new Uint8Array(init?.body);
            const id = new DataView(query.buffer, query.byteOffset, query.byteLength).getUint16(0);
            const qnameLength = query.findIndex((x, i) => i >= 12 && x === 0) - 12 + 1;
            const questionEnd = 12 + qnameLength + 4;
            const answerName = new Uint8Array([0xc0, 0x0c]);
            calls++;
            if (calls === 1) {
                const cnameRdata = (() => { const out = []; for (const label of "target.example.com".split(".")) {
                    const b = new TextEncoder().encode(label);
                    out.push(b.length, ...b);
                } out.push(0); return new Uint8Array(out); })();
                const out = new Uint8Array(questionEnd + 2 + 10 + cnameRdata.length);
                const view = new DataView(out.buffer);
                view.setUint16(0, id);
                view.setUint16(2, 0x8180);
                view.setUint16(4, 1);
                view.setUint16(6, 1);
                out.set(query.subarray(12, questionEnd), 12);
                out.set(answerName, questionEnd);
                let off = questionEnd + 2;
                view.setUint16(off, 5);
                view.setUint16(off + 2, 1);
                view.setUint32(off + 4, 10);
                view.setUint16(off + 8, cnameRdata.length);
                out.set(cnameRdata, off + 10);
                return new Response(out, { status: 200 });
            }
            const out = new Uint8Array(questionEnd + 2 + 10 + 4);
            const view = new DataView(out.buffer);
            view.setUint16(0, id);
            view.setUint16(2, 0x8180);
            view.setUint16(4, 1);
            view.setUint16(6, 1);
            out.set(query.subarray(12, questionEnd), 12);
            out.set(answerName, questionEnd);
            let off = questionEnd + 2;
            view.setUint16(off, 1);
            view.setUint16(off + 2, 1);
            view.setUint32(off + 4, 60);
            view.setUint16(off + 8, 4);
            out.set([1, 2, 3, 4], off + 10);
            return new Response(out, { status: 200 });
        };
        const cache = new cache_1.DnsCache();
        const resolver = new doh_1.DoHResolver({ endpoint: "https://dns.example/dns-query", timeoutMs: 1000, retries: 0, fetchImpl: requestFetch, cache });
        const result = await resolver.resolve("example.com", "ipv4", signal);
        if (result[0]?.address !== "1.2.3.4" || result[0]?.ttl !== 10)
            throw new Error(`CNAME chain TTL was not capped to 10: ${JSON.stringify(result[0])}`);
    }));
    tests.push(await runOne("doh.get-strict-decoding", "Private DoH GET rejects malformed base64url before network access", "dns", async () => {
        const bad = new Request("https://proxy.example.com/dns-query?dns=abc===", { method: "GET", headers: { Authorization: "Bearer doh-token" } });
        try {
            await (0, doh_3.handlePrivateDoh)(bad, { CORE_DOH_TOKEN: "doh-token", DNS_RESOLVERS: "https://dns.example/dns-query" });
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && error.code === "BAD_REQUEST")
                return;
            throw error;
        }
        throw new Error("Malformed DoH GET parameter was accepted");
    }));
    tests.push(await runOne("doh.private-resolver-fallback", "Private DoH retries the configured resolver list after an upstream HTTP failure", "dns", async () => {
        // Well-formed DNS fixtures: the handler validates the request and every
        // upstream response as raw DNS messages (D1) and enforces the RFC 8484
        // media type on POSTs (D2), so the query must be a real header+question
        // and the mock response must echo the same ID and question.
        const question = [7, 101, 120, 97, 109, 112, 108, 101, 3, 99, 111, 109, 0, 0, 1, 0, 1]; // example.com A IN
        const query = new Uint8Array([0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, ...question]);
        const answer = new Uint8Array([
            0, 1, 129, 128, 0, 1, 0, 1, 0, 0, 0, 0, ...question,
            0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 93, 184, 216, 34, // A 93.184.216.34, TTL 60
        ]);
        const originalFetch = globalThis.fetch;
        let calls = 0;
        globalThis.fetch = (async (input) => {
            calls++;
            if (String(input).includes("bad.example"))
                return new Response("", { status: 503 });
            return new Response(answer, { status: 200, headers: { "content-type": "application/dns-message", "content-length": String(answer.byteLength) } });
        });
        try {
            const response = await (0, doh_3.handlePrivateDoh)(new Request("https://proxy.example.com/dns-query", { method: "POST", body: query, headers: { Authorization: "Bearer doh-token", "content-type": "application/dns-message" } }), { CORE_DOH_TOKEN: "doh-token", DNS_RESOLVERS: "https://bad.example/dns-query,https://good.example/dns-query", DNS_TIMEOUT_MS: "1000" });
            if (response.status !== 200 || calls !== 3)
                throw new Error(`Private DoH resolver retry/fallback mismatch: status=${response.status}, calls=${calls}`);
        }
        finally {
            globalThis.fetch = originalFetch;
        }
    }));
    tests.push(await runOne("metrics.histogram", "Runtime observations keep fixed-bucket latency histograms", "observability", async () => {
        const metrics = new metrics_1.CounterMetrics();
        metrics.observe("session.first_byte_ms", 42);
        const snap = metrics.snapshot();
        if (snap["session.first_byte_ms.count"] !== 1 || snap["session.first_byte_ms.sum"] !== 42 || snap["session.first_byte_ms.bucket_le_50"] !== 1 || snap["session.first_byte_ms.bucket_le_25"] !== undefined || snap["session.first_byte_ms.min"] !== 42 || snap["session.first_byte_ms.max"] !== 42)
            throw new Error("Latency histogram snapshot mismatch");
    }));
    const passed = tests.filter((x) => x.status === "PASS").length;
    const failed = tests.filter((x) => x.status === "FAIL").length;
    const skippedCount = tests.filter((x) => x.status === "SKIPPED").length;
    const unavailable = tests.filter((x) => x.status === "UNAVAILABLE").length;
    // Registration sites intentionally reuse an ID across mutually exclusive
    // branches (real test vs SKIPPED placeholder), so source-site counts and
    // runtime counts legitimately differ — but a duplicate ID at runtime is
    // always a registration bug and must fail the suite, not pass silently.
    const hasDuplicateIds = new Set(tests.map((x) => x.id)).size !== tests.length;
    return {
        status: failed === 0 && !hasDuplicateIds ? "passed" : "failed",
        total: tests.length,
        passed,
        failed,
        skipped: skippedCount,
        unavailable,
        // Wall-clock duration of the whole suite. This was previously the sum of
        // per-test durations, which silently excluded inter-test/framework time
        // and understated real runtime; per-test durationMs stays as-is.
        durationMs: rounded(performance.now() - suiteStarted),
        results: tests,
    };
}
function parseTarget(value) {
    const parsed = (0, binary_1.parseHostPort)(value);
    if (!parsed.port)
        throw new Error("CORE_TEST_TARGET must include a port");
    return { host: parsed.host, port: parsed.port };
}
function testTimeout(env) {
    const value = Number(env.CORE_TEST_TIMEOUT_MS ?? 5000);
    return Number.isFinite(value) && value >= 100 && value <= 60_000 ? value : 5000;
}
async function optionalHttpExchange(conn, target) {
    if (target.port !== 80)
        return;
    const writer = conn.writable.getWriter();
    try {
        await writer.write(new TextEncoder().encode(`HEAD / HTTP/1.1\r\nHost: ${target.host}\r\nConnection: close\r\n\r\n`));
    }
    finally {
        writer.releaseLock();
    }
    const reader = conn.readable.getReader();
    let collected = "";
    try {
        for (let i = 0; i < 8 && collected.length < 256; i++) {
            const result = await readWithTimeout(reader.read(), 5000);
            if (result.done)
                break;
            collected += new TextDecoder().decode(result.value);
            if (collected.includes("\r\n"))
                break;
        }
    }
    finally {
        reader.releaseLock();
    }
    if (!/^HTTP\/\d\.\d\s+\d{3}/.test(collected))
        throw new Error(`Unexpected HTTP response: ${collected.slice(0, 80)}`);
}
async function readWithTimeout(promise, timeoutMs) {
    let timer;
    try {
        return await Promise.race([
            promise,
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error("Read timeout")), timeoutMs);
            }),
        ]);
    }
    finally {
        if (timer !== undefined)
            clearTimeout(timer);
    }
}
async function readExactStream(stream, length) {
    const reader = stream.getReader();
    const out = new Uint8Array(length);
    let offset = 0;
    try {
        while (offset < length) {
            const result = await reader.read();
            if (result.done)
                throw new Error("Stream ended before expected payload");
            const take = Math.min(result.value.byteLength, length - offset);
            out.set(result.value.subarray(0, take), offset);
            offset += take;
        }
        return out;
    }
    finally {
        reader.releaseLock();
    }
}
class FakeSocks5Socket {
    requireAuth;
    expectedHost;
    expectedPort;
    expectedAtyp;
    expectedUsername;
    expectedPassword;
    opened = Promise.resolve();
    readable;
    writable;
    controller = null;
    closed = false;
    step = 0;
    constructor(requireAuth, expectedHost = "example.com", expectedPort = 443, expectedAtyp = 3, expectedUsername = "user", expectedPassword = "pass") {
        this.requireAuth = requireAuth;
        this.expectedHost = expectedHost;
        this.expectedPort = expectedPort;
        this.expectedAtyp = expectedAtyp;
        this.expectedUsername = expectedUsername;
        this.expectedPassword = expectedPassword;
        this.readable = new ReadableStream({ start: (controller) => { this.controller = controller; } });
        this.writable = new WritableStream({ write: async (chunk) => this.handleWrite(chunk) });
    }
    async close() {
        if (this.closed)
            return;
        this.closed = true;
        this.controller?.close();
    }
    handleWrite(chunk) {
        if (this.closed)
            throw new Error("Fake socket closed");
        if (this.step === 0) {
            if (chunk[0] !== 5)
                throw new Error("Invalid SOCKS greeting");
            if (this.requireAuth && !(chunk.includes(2)))
                throw new Error("Client did not offer username/password");
            this.controller?.enqueue(new Uint8Array([5, this.requireAuth ? 2 : 0]));
            this.step = this.requireAuth ? 1 : 2;
            return;
        }
        if (this.step === 1) {
            if (chunk[0] !== 1)
                throw new Error("Invalid SOCKS auth packet");
            const userLength = chunk[1];
            const userEnd = 2 + userLength;
            const passLengthIndex = userEnd;
            if (chunk[passLengthIndex] === undefined)
                throw new Error("Invalid auth packet");
            const passwordLength = chunk[passLengthIndex];
            const passwordStart = passLengthIndex + 1;
            const username = new TextDecoder().decode(chunk.subarray(2, userEnd));
            const password = new TextDecoder().decode(chunk.subarray(passwordStart, passwordStart + passwordLength));
            if (username !== this.expectedUsername || password !== this.expectedPassword)
                throw new Error("SOCKS credentials mismatch");
            this.controller?.enqueue(new Uint8Array([1, 0]));
            this.step = 2;
            return;
        }
        if (this.step === 2) {
            if (chunk[0] !== 5 || chunk[1] !== 1 || chunk[2] !== 0 || chunk[3] !== this.expectedAtyp)
                throw new Error("Invalid SOCKS CONNECT request");
            let host = "";
            let portOffset = 0;
            if (chunk[3] === 1) {
                host = Array.from(chunk.subarray(4, 8)).join(".");
                portOffset = 8;
            }
            else if (chunk[3] === 3) {
                const hostLength = chunk[4];
                host = new TextDecoder().decode(chunk.subarray(5, 5 + hostLength));
                portOffset = 5 + hostLength;
            }
            else if (chunk[3] === 4) {
                const groups = [];
                for (let i = 0; i < 8; i++)
                    groups.push(((chunk[4 + i * 2] << 8) | chunk[5 + i * 2]).toString(16));
                host = groups.join(":");
                portOffset = 20;
            }
            const port = (chunk[portOffset] << 8) | chunk[portOffset + 1];
            const normalizedExpectedHost = this.expectedHost.replace(/^\[|\]$/g, "");
            const expectedLength = this.expectedAtyp === 1 ? 10 : this.expectedAtyp === 3 ? 7 + new TextEncoder().encode(normalizedExpectedHost).byteLength : 22;
            if (chunk.byteLength !== expectedLength)
                throw new Error(`SOCKS CONNECT frame has unexpected length: ${chunk.byteLength}, expected ${expectedLength}`);
            const hostMatches = this.expectedAtyp === 4 ? normalizeIpv6(host) === normalizeIpv6(normalizedExpectedHost) : host === normalizedExpectedHost;
            if (!hostMatches || port !== this.expectedPort)
                throw new Error(`Wrong SOCKS destination ${host}:${port}`);
            this.controller?.enqueue((0, binary_1.concatBytes)(new Uint8Array([5, 0, 0, 1, 127, 0, 0, 1, 0, 80]), new TextEncoder().encode("UPST")));
            this.step = 3;
        }
    }
}

};

__m["output/aggregation"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.subscriptionSources = subscriptionSources;
exports.subscriptionSourceLogLabel = subscriptionSourceLogLabel;
exports.validateSubscriptionUrl = validateSubscriptionUrl;
exports.fetchNativeSubscriptionSources = fetchNativeSubscriptionSources;
const errors_1 = __r("../core/errors");
const limits_1 = __r("../core/limits");
const bounded_cache_1 = __r("../util/bounded-cache");
const hostname_1 = __r("../util/hostname");
const lifecycle_1 = __r("../util/lifecycle");
function decodeBase64(value) {
    const normalized = value.replace(/\s+/g, "");
    if (!normalized || normalized.length % 4 !== 0 || /[^A-Za-z0-9+/=]/.test(normalized))
        return null;
    try {
        const bytes = Uint8Array.from(atob(normalized), (char) => char.charCodeAt(0));
        return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    }
    catch {
        return null;
    }
}
function normalizeBody(text) {
    const trimmed = text.trim();
    if (/^(?:vless|trojan):\/\//im.test(trimmed))
        return trimmed;
    const decoded = decodeBase64(trimmed);
    return decoded?.trim() ?? trimmed;
}
function subscriptionSources(env) {
    return (env.OUTPUT_CUSTOM_SUBS ?? "").split(/[\r\n,]+/).map((x) => x.trim()).filter(Boolean);
}
function subscriptionAllowlist(env) {
    return (env.CORE_OUTPUT_SUBS_ALLOWLIST ?? "").split(/[\r\n,]+/).map((x) => (0, hostname_1.normalizeHostname)(x)).filter(Boolean);
}
function isIpv4Literal(value) {
    const parts = value.split(".");
    if (parts.length !== 4 || parts.some((part) => !/^\d{1,3}$/.test(part)))
        return false;
    return parts.map(Number).every((n) => n >= 0 && n <= 255);
}
function ipv4Disallowed(value) {
    if (!isIpv4Literal(value))
        return false;
    const p = value.split(".").map(Number);
    if (p[0] === 0 || p[0] === 10 || p[0] === 127 || p[0] >= 224)
        return true;
    if (p[0] === 100 && p[1] >= 64 && p[1] <= 127)
        return true;
    if (p[0] === 169 && p[1] === 254)
        return true;
    if (p[0] === 172 && p[1] >= 16 && p[1] <= 31)
        return true;
    if (p[0] === 192 && p[1] === 0 && p[2] === 0)
        return true;
    if (p[0] === 192 && p[1] === 0 && p[2] === 2)
        return true;
    if (p[0] === 192 && p[1] === 88 && p[2] === 99)
        return true;
    if (p[0] === 192 && p[1] === 168)
        return true;
    if (p[0] === 198 && p[1] >= 18 && p[1] <= 19)
        return true;
    if (p[0] === 198 && p[1] === 51 && p[2] === 100)
        return true;
    if (p[0] === 203 && p[1] === 0 && p[2] === 113)
        return true;
    return false;
}
function parseIpv6Bytes(value) {
    const input = (0, hostname_1.normalizeHostname)(value);
    if (!input.includes(":"))
        return null;
    const mapped = input.match(/^(.+):((?:\d{1,3}\.){3}\d{1,3})$/);
    let source = input;
    if (mapped) {
        const ipv4 = mapped[2];
        if (!isIpv4Literal(ipv4))
            return null;
        const octets = ipv4.split(".").map(Number);
        source = `${mapped[1]}:${((octets[0] << 8) | octets[1]).toString(16)}:${((octets[2] << 8) | octets[3]).toString(16)}`;
    }
    const parts = source.split("::");
    if (parts.length > 2)
        return null;
    const left = parts[0] ? parts[0].split(":") : [];
    const right = parts.length === 2 && parts[1] ? parts[1].split(":") : [];
    if (left.some((x) => !/^[0-9a-f]{1,4}$/.test(x)) || right.some((x) => !/^[0-9a-f]{1,4}$/.test(x)))
        return null;
    const missing = parts.length === 2 ? 8 - left.length - right.length : 0;
    if (missing < 1 || (parts.length === 1 && left.length !== 8))
        return null;
    const groups = parts.length === 2 ? [...left, ...new Array(missing).fill("0"), ...right] : left;
    if (groups.length !== 8)
        return null;
    const bytes = new Uint8Array(16);
    groups.forEach((group, i) => { const n = Number.parseInt(group, 16); bytes[i * 2] = n >>> 8; bytes[i * 2 + 1] = n & 0xff; });
    return bytes;
}
function isIpv6Literal(value) {
    const input = (0, hostname_1.normalizeHostname)(value);
    return input.includes(":") && /^[0-9a-fA-F:.]+$/.test(input);
}
function ipv6Disallowed(value) {
    if (!isIpv6Literal(value))
        return false;
    const bytes = parseIpv6Bytes(value);
    if (!bytes)
        return true;
    const first16 = (bytes[0] << 8) | bytes[1];
    if (bytes.every((x) => x === 0) || (bytes[15] === 1 && bytes.slice(0, 15).every((x) => x === 0)))
        return true;
    if ((first16 & 0xfe00) === 0xfc00 || (first16 & 0xffc0) === 0xfe80 || (first16 & 0xff00) === 0xff00)
        return true;
    const mapped = bytes.slice(0, 10).every((x) => x === 0) && bytes[10] === 0xff && bytes[11] === 0xff;
    if (mapped) {
        const ipv4 = `${bytes[12]}.${bytes[13]}.${bytes[14]}.${bytes[15]}`;
        return ipv4Disallowed(ipv4);
    }
    return false;
}
function isDisallowedHost(host) {
    const normalized = (0, hostname_1.normalizeHostname)(host);
    return ipv4Disallowed(normalized)
        || ipv6Disallowed(normalized)
        || normalized === "localhost"
        || normalized.endsWith(".localhost")
        || normalized.endsWith(".local")
        || normalized.endsWith(".internal");
}
function hostAllowed(host, allowlist) {
    const normalized = (0, hostname_1.normalizeHostname)(host);
    return allowlist.some((rule) => {
        if (rule.startsWith("*.")) {
            const suffix = (0, hostname_1.normalizeHostname)(rule.slice(2));
            return Boolean(suffix) && normalized.endsWith(`.${suffix}`);
        }
        return normalized === (0, hostname_1.normalizeHostname)(rule);
    });
}
/**
 * Subscription URLs conventionally embed a live access token in the path or query, so the
 * raw URL must never reach logs or error surfaces (audit F7). This label keeps failures
 * correlatable — host, length and an FNV-1a fingerprint of the full URL — without
 * disclosing any part of the path, query or credentials. FNV-1a is a correlation aid,
 * not a security control.
 */
function subscriptionSourceLogLabel(source) {
    let host = "(unparseable)";
    try {
        host = new URL(source).hostname || host;
    }
    catch { /* keep placeholder */ }
    let hash = 0x811c9dc5;
    for (let i = 0; i < source.length; i++) {
        hash ^= source.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return `host=${host} len=${source.length} fnv1a=${hash.toString(16).padStart(8, "0")}`;
}
function validateSubscriptionUrl(source, env) {
    let url;
    try {
        url = new URL(source);
    }
    catch {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid external subscription URL (${subscriptionSourceLogLabel(source)})`);
    }
    const allowHttp = /^(1|true|yes|on)$/i.test(env.CORE_OUTPUT_SUBS_ALLOW_HTTP?.trim() ?? "");
    if (url.protocol !== "https:" && !(allowHttp && url.protocol === "http:")) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Unsupported external subscription scheme: ${url.protocol}; HTTPS is required by default`);
    }
    if (url.username || url.password || url.hash) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `External subscription URL must not contain credentials or fragments (${subscriptionSourceLogLabel(source)})`);
    }
    const allowlist = subscriptionAllowlist(env);
    if (!allowlist.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_OUTPUT_SUBS_ALLOWLIST must be configured when OUTPUT_CUSTOM_SUBS is used");
    const normalizedHost = (0, hostname_1.normalizeHostname)(url.hostname);
    if (isDisallowedHost(normalizedHost))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `External subscription target is not a public host: ${url.hostname}`);
    if (!hostAllowed(normalizedHost, allowlist)) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `External subscription host is not in CORE_OUTPUT_SUBS_ALLOWLIST: ${url.hostname}`);
    }
    return url;
}
const subscriptionCache = new bounded_cache_1.BoundedTtlLru(32, 10_000);
async function fetchOne(source, env, signal, maxBytes, fetchImpl, resolver) {
    const url = validateSubscriptionUrl(source, env);
    const hostname = (0, hostname_1.normalizeHostname)(url.hostname);
    if (!isDisallowedHost(hostname) && !isIpv4Literal(hostname) && !isIpv6Literal(hostname)) {
        if (!resolver)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `DNS validation is required before fetching external subscription host: ${hostname}`);
        const resolved = await resolver.resolve(hostname, "auto", signal);
        if (!resolved.length)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `External subscription host did not resolve to a public address: ${hostname}`);
        for (const address of resolved) {
            if (isDisallowedHost(address.address))
                throw new errors_1.CoreError("CONFIGURATION_ERROR", `External subscription host resolves to a non-public address: ${hostname} -> ${address.address}`);
        }
    }
    const cacheKey = `${url.toString()}\u0000${maxBytes}`;
    const cached = subscriptionCache.get(cacheKey);
    if (cached)
        return [...cached];
    // Residual boundary (audit F6): the addresses validated above cannot be pinned into the
    // Workers fetch below — fetch re-resolves DNS, so a rebinding between validation and
    // connect remains theoretically possible. The mandatory host allowlist, HTTPS-by-default,
    // manual redirects and the platform egress policy bound the impact, and the project docs
    // disclose this TOCTOU explicitly. Do not treat the resolve step as address pinning.
    const response = await fetchImpl(url, {
        headers: { accept: "text/plain, text/*;q=0.9, */*;q=0.1" },
        redirect: "manual",
        signal,
        cache: "no-store",
    });
    if (!response.ok)
        throw new errors_1.CoreError("EGRESS_FAILED", `External subscription returned HTTP ${response.status}`, true);
    const contentLength = Number(response.headers.get("content-length") ?? "0");
    if (contentLength > maxBytes)
        throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `External subscription exceeds ${maxBytes} bytes`);
    const reader = response.body?.getReader();
    if (!reader)
        return [];
    const chunks = [];
    let total = 0;
    try {
        while (true) {
            const item = await reader.read();
            if (item.done)
                break;
            total += item.value.byteLength;
            if (total > maxBytes)
                throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `External subscription exceeds ${maxBytes} bytes`);
            chunks.push(item.value);
        }
    }
    catch (error) {
        await reader.cancel(error).catch(() => { });
        throw error;
    }
    finally {
        reader.releaseLock();
    }
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
    }
    let body;
    try {
        body = normalizeBody(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    }
    catch (error) {
        throw new errors_1.CoreError("BAD_REQUEST", "External subscription is not valid UTF-8", false, { cause: error });
    }
    const out = [];
    const seen = new Set();
    for (const line of body.split(/\r?\n/).map((x) => x.trim()).filter((x) => /^(vless|trojan):\/\//i.test(x))) {
        if (!seen.has(line)) {
            seen.add(line);
            out.push(line);
        }
    }
    subscriptionCache.set(cacheKey, out);
    return out;
}
async function fetchNativeSubscriptionSources(env, signal, maxSources = limits_1.CORE_LIMITS.maxSubscriptionSources, maxBytes = limits_1.CORE_LIMITS.maxSubscriptionSourceBytes, fetchImpl = fetch, resolver) {
    const sources = subscriptionSources(env);
    if (sources.length > maxSources)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Too many external subscription sources: ${sources.length}`);
    const out = [];
    const seen = new Set();
    const sourceTimeout = Number(env.CORE_OUTPUT_SUBS_TIMEOUT_MS ?? limits_1.CORE_LIMITS.defaultExternalSubscriptionTimeoutMs);
    const boundedTimeout = Number.isFinite(sourceTimeout) && sourceTimeout >= 250 ? Math.min(sourceTimeout, 30_000) : limits_1.CORE_LIMITS.defaultExternalSubscriptionTimeoutMs;
    for (const source of sources) {
        try {
            const lines = await (0, lifecycle_1.withTimeout)((sourceSignal) => fetchOne(source, env, sourceSignal, maxBytes, fetchImpl, resolver), boundedTimeout, signal, "External subscription fetch timeout");
            for (const line of lines) {
                if (!seen.has(line)) {
                    seen.add(line);
                    out.push(line);
                }
            }
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && ["CONFIGURATION_ERROR", "BAD_REQUEST", "MESSAGE_TOO_LARGE"].includes(error.code))
                throw error;
            if (signal.aborted)
                throw signal.reason;
            console.warn(`Skipping unavailable external subscription source (${subscriptionSourceLogLabel(source)})`);
        }
    }
    return out;
}

};

__m["util/lifecycle"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.combineAbortSignals = combineAbortSignals;
exports.remainingMs = remainingMs;
exports.deadlineAtFromNow = deadlineAtFromNow;
exports.deadlineSignal = deadlineSignal;
exports.withTimeout = withTimeout;
exports.raceWithAbort = raceWithAbort;
const errors_1 = __r("../core/errors");
function combineAbortSignals(...signals) {
    const controller = new AbortController();
    const listeners = signals.map((signal) => {
        const listener = () => {
            if (!controller.signal.aborted)
                controller.abort(signal.reason);
        };
        if (signal.aborted)
            listener();
        signal.addEventListener("abort", listener, { once: true });
        return { signal, listener };
    });
    return {
        signal: controller.signal,
        dispose: () => {
            for (const item of listeners)
                item.signal.removeEventListener("abort", item.listener);
        },
    };
}
function remainingMs(deadlineAt, fallbackMs) {
    if (deadlineAt === undefined)
        return fallbackMs;
    return Math.max(0, Math.min(fallbackMs, deadlineAt - Date.now()));
}
function deadlineAtFromNow(timeoutMs) {
    if (timeoutMs === undefined || timeoutMs <= 0)
        return undefined;
    return Date.now() + timeoutMs;
}
function deadlineSignal(parent, deadlineAt, message = "Operation deadline exceeded") {
    const timeout = new AbortController();
    let timer;
    if (deadlineAt !== undefined) {
        const ms = deadlineAt - Date.now();
        if (ms <= 0)
            timeout.abort(new errors_1.CoreError("TIMEOUT", message, true));
        else
            timer = setTimeout(() => timeout.abort(new errors_1.CoreError("TIMEOUT", message, true)), ms);
    }
    const combined = combineAbortSignals(parent, timeout.signal);
    return {
        signal: combined.signal,
        dispose: () => {
            if (timer !== undefined)
                clearTimeout(timer);
            combined.dispose();
        },
    };
}
async function withTimeout(factory, timeoutMs, parent, message) {
    if (parent.aborted)
        throw parent.reason;
    const controller = new AbortController();
    const combined = combineAbortSignals(parent, controller.signal);
    let timedOut = false;
    let timer;
    const timeoutError = () => new errors_1.CoreError("TIMEOUT", message, true);
    let operation;
    try {
        operation = factory(combined.signal);
    }
    catch (error) {
        combined.dispose();
        throw error;
    }
    // The operation may outlive the race when a platform/API implementation ignores AbortSignal.
    // Attach a rejection handler so a late rejection cannot become an unhandled promise.
    void operation.catch(() => { });
    let onAbort;
    try {
        const aborted = new Promise((_, reject) => {
            onAbort = () => reject(combined.signal.reason ?? new DOMException("Aborted", "AbortError"));
            combined.signal.addEventListener("abort", onAbort, { once: true });
            if (combined.signal.aborted)
                onAbort();
        });
        const timeout = new Promise((_, reject) => {
            timer = setTimeout(() => {
                timedOut = true;
                const error = timeoutError();
                controller.abort(error);
                reject(error);
            }, Math.max(0, timeoutMs));
        });
        const result = await Promise.race([operation, timeout, aborted]);
        if (combined.signal.aborted)
            throw combined.signal.reason;
        return result;
    }
    catch (error) {
        if (timedOut)
            throw combined.signal.reason ?? timeoutError();
        if (parent.aborted)
            throw parent.reason;
        throw error;
    }
    finally {
        if (onAbort)
            combined.signal.removeEventListener("abort", onAbort);
        if (timer !== undefined)
            clearTimeout(timer);
        combined.dispose();
    }
}
async function raceWithAbort(operation, signal, onLateResolve) {
    if (signal.aborted) {
        void operation.then((value) => onLateResolve?.(value), () => { });
        throw signal.reason;
    }
    let onAbort;
    try {
        const aborted = new Promise((_, reject) => {
            onAbort = () => reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
            signal.addEventListener("abort", onAbort, { once: true });
            if (signal.aborted)
                onAbort();
        });
        const result = await Promise.race([operation, aborted]);
        if (signal.aborted) {
            await onLateResolve?.(result);
            throw signal.reason;
        }
        return result;
    }
    catch (error) {
        if (signal.aborted) {
            void operation.then((value) => onLateResolve?.(value), () => { });
            void operation.catch(() => { });
        }
        throw error;
    }
    finally {
        if (onAbort)
            signal.removeEventListener("abort", onAbort);
    }
}

};

__m["util/bounded-cache"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BoundedTtlLru = void 0;
class BoundedTtlLru {
    maxEntries;
    ttlMs;
    now;
    entries = new Map();
    constructor(maxEntries, ttlMs, now = Date.now) {
        this.maxEntries = maxEntries;
        this.ttlMs = ttlMs;
        this.now = now;
        if (!Number.isInteger(maxEntries) || maxEntries <= 0)
            throw new RangeError("maxEntries must be a positive integer");
        if (!Number.isInteger(ttlMs) || ttlMs <= 0)
            throw new RangeError("ttlMs must be a positive integer");
    }
    get(key) {
        const entry = this.entries.get(key);
        if (!entry)
            return undefined;
        if (entry.expiresAt <= this.now()) {
            this.entries.delete(key);
            return undefined;
        }
        this.entries.delete(key);
        this.entries.set(key, entry);
        return entry.value;
    }
    set(key, value) {
        this.entries.delete(key);
        this.entries.set(key, { value, expiresAt: this.now() + this.ttlMs });
        while (this.entries.size > this.maxEntries) {
            const oldest = this.entries.keys().next().value;
            if (oldest === undefined)
                break;
            this.entries.delete(oldest);
        }
    }
    delete(key) { this.entries.delete(key); }
    clear() { this.entries.clear(); }
    get size() { return this.entries.size; }
}
exports.BoundedTtlLru = BoundedTtlLru;

};

__m["runtime/runtime"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runtimeFor = runtimeFor;
const runtime_1 = __r("../config/runtime");
const engine_1 = __r("../dns/engine");
const engine_2 = __r("../core/engine");
const parser_1 = __r("../protocol/vless/parser");
const parser_2 = __r("../protocol/trojan/parser");
const direct_1 = __r("../egress/direct");
const proxyip_1 = __r("../egress/proxyip");
const relay_1 = __r("../egress/relay");
const defaults_1 = __r("../egress/defaults");
const proxyip_pool_1 = __r("../egress/proxyip-pool");
const nat64_1 = __r("../egress/nat64");
const router_1 = __r("../routing/router");
const rules_1 = __r("../routing/rules");
const cloudflare_1 = __r("../storage/cloudflare");
const metrics_1 = __r("../observability/metrics");
const registry_1 = __r("../egress/registry");
let cachedRuntime = null;
let cachedSignature = "";
function signature(env, config) {
    // The parsed runtime config is part of the signature (A15): every env var
    // buildRuntimeConfig consumes (output budgets included) is captured by its
    // parsed value, so the cache key cannot silently drift from the inputs the
    // cached runtime was actually built from. The remaining raw fields are the
    // inputs consumed directly from env by the components assembled below.
    return JSON.stringify({
        config,
        raw: [
            env.CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS, env.CORE_CREDENTIAL_MODE, env.EGRESS_MODE, env.PROXY_IP, env.PROXY_IPS, env.PROXY_IP_ENDPOINTS_JSON, env.PROXY_IP_TLS, env.PROXY_IP_USERNAME, env.PROXY_IP_PASSWORD,
            env.NAT64_PREFIXES, env.PROXY_RELAY_ENDPOINTS, env.CORE_MAX_PROXYIP_FAILURES, env.CORE_PROXYIP_COOLDOWN_MS, env.CORE_RULES, env.DNS_ENABLED, env.DNS_RESOLVERS, env.DNS_CACHE, env.DNS_CACHE_MAX_ENTRIES, env.DNS_CACHE_MAX_TTL, env.DNS_CACHE_NEGATIVE_TTL,
            env.DNS_TIMEOUT_MS, env.DNS_RETRIES, env.DNS_FALLBACK, env.DNS_IP_PREFERENCE, env.DNS_HOSTS, Boolean(env.DB), env.VLESS_USERS, env.TROJAN_USERS,
        ],
    });
}
function runtimeFor(env) {
    const config = (0, runtime_1.buildRuntimeConfig)(env);
    const key = signature(env, config);
    if (cachedRuntime && cachedSignature === key)
        return cachedRuntime;
    const dns = new engine_1.DnsEngine(env);
    const credentials = (0, cloudflare_1.createCredentialProvider)(env);
    const handlers = new Map([["vless", new parser_1.VlessHandler(credentials)], ["trojan", new parser_2.TrojanHandler(credentials)]]);
    const egress = new registry_1.EgressRegistry();
    egress.register(new direct_1.DirectEgress(config.egressConnectTimeoutMs, dns, config.addressRace));
    const mode = (0, router_1.parseEgressMode)(env.EGRESS_MODE);
    const proxyip = (0, proxyip_1.hasProxyIp)(env);
    if ((mode === "proxyip" || mode === "fallback" || mode === "smart") && proxyip) {
        egress.register((0, proxyip_pool_1.buildProxyIpPool)(env, config.egressConnectTimeoutMs));
    }
    const nat64Raw = env.NAT64_PREFIXES?.trim() || (((mode === "nat64" || mode === "smart") ? defaults_1.DEFAULT_NAT64_PREFIXES.join(",") : ""));
    const prefixes = (0, nat64_1.parseNat64Prefixes)(nat64Raw);
    const nat64 = (mode === "nat64" || mode === "smart") && prefixes.length > 0;
    if (nat64) {
        egress.register(new nat64_1.Nat64Egress(prefixes, config.egressConnectTimeoutMs, new nat64_1.DnsNat64AddressResolver(dns)));
    }
    const relayEndpoints = (0, relay_1.parseRelayEndpoints)(env.PROXY_RELAY_ENDPOINTS);
    // Relay availability is a single derived value shared by the registry and
    // the router (A5): it used to be decided by a raw substring scan of
    // CORE_RULES here while the router was told relay exists purely from
    // endpoint presence, so the router could select an unregistered dialer (or
    // vice versa). Parse the rules with the canonical parser instead.
    const rulesUseRelay = (0, rules_1.parseCoreRules)(env.CORE_RULES).some((rule) => rule.egress === "relay" || rule.fallbackEgress === "relay");
    const relay = relayEndpoints.length > 0 && (mode === "smart" || rulesUseRelay);
    if (relay) {
        egress.register(new relay_1.RawTcpRelayEgress(relayEndpoints, config.egressConnectTimeoutMs));
    }
    const router = new router_1.DefaultRouter(mode, env.CORE_RULES, { proxyip, relay, nat64 });
    const metrics = new metrics_1.CounterMetrics();
    const engine = new engine_2.ProxyEngine({ router, protocols: handlers, egress, handshakeTimeoutMs: config.handshakeTimeoutMs, retryPolicy: config.retry, idleTimeoutMs: config.idleTimeoutMs, maxSessionDurationMs: config.maxSessionDurationMs, maxEgressDecisionMs: config.maxEgressDecisionMs, metrics, dns });
    cachedRuntime = { engine, credentials, dns, config, metrics };
    cachedSignature = key;
    return cachedRuntime;
}

};

__m["egress/registry"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.EgressRegistry = void 0;
const errors_1 = __r("../core/errors");
class EgressRegistry {
    items = new Map();
    register(dialer) { this.items.set(dialer.type, dialer); }
    get(type) { return this.items.get(type); }
    require(type) {
        const dialer = this.get(type);
        if (!dialer)
            throw new errors_1.CoreError("ROUTE_FAILED", `Egress ${type} is not configured`);
        return dialer;
    }
    entries() { return [...this.items.entries()]; }
}
exports.EgressRegistry = EgressRegistry;

};

__m["observability/metrics"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CounterMetrics = void 0;
const HISTOGRAM_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000, 300000];
class CounterMetrics {
    counters = new Map();
    increment(name, value = 1) {
        // Counters are monotonic: ignore non-finite and negative deltas instead of letting one
        // bad call poison the counter for the life of the isolate (audit F17). observe()
        // already sanitised its input; increment() now enforces the same invariant itself.
        if (!Number.isFinite(value) || value < 0)
            return;
        this.counters.set(name, (this.counters.get(name) ?? 0) + value);
    }
    observe(name, value) {
        const safe = Number.isFinite(value) ? Math.max(0, value) : 0;
        this.increment(`${name}.count`);
        this.increment(`${name}.sum`, safe);
        for (const bucket of HISTOGRAM_BUCKETS) {
            if (safe <= bucket)
                this.increment(`${name}.bucket_le_${bucket}`);
        }
        this.increment(`${name}.bucket_le_inf`);
        const previousMin = this.counters.get(`${name}.min`);
        const previousMax = this.counters.get(`${name}.max`);
        if (previousMin === undefined || safe < previousMin)
            this.counters.set(`${name}.min`, safe);
        if (previousMax === undefined || safe > previousMax)
            this.counters.set(`${name}.max`, safe);
    }
    snapshot() { return Object.fromEntries(this.counters.entries()); }
}
exports.CounterMetrics = CounterMetrics;

};

__m["storage/cloudflare"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KVJsonStore = exports.CredentialRepository = exports.EnvironmentCredentialProvider = exports.D1CredentialProvider = void 0;
exports.createCredentialProvider = createCredentialProvider;
const binary_1 = __r("../util/binary");
const sha224_1 = __r("../util/sha224");
const bounded_cache_1 = __r("../util/bounded-cache");
const limits_1 = __r("../core/limits");
function normalizeCredential(protocol, credential) {
    if (protocol === "vless")
        return (0, binary_1.bytesToUuid)(credential).toLowerCase();
    const digest = new TextDecoder("ascii", { fatal: true }).decode(credential).toLowerCase();
    if (!/^[0-9a-f]{56}$/.test(digest))
        throw new Error("Invalid Trojan credential digest");
    return digest;
}
class D1CredentialProvider {
    db;
    negativeTtlMs;
    negativeCache;
    positiveCache;
    schemaPromise = null;
    constructor(db, negativeTtlMs = limits_1.CORE_LIMITS.negativeAuthCacheTtlMs) {
        this.db = db;
        this.negativeTtlMs = negativeTtlMs;
        this.negativeCache = new bounded_cache_1.BoundedTtlLru(limits_1.CORE_LIMITS.maxNegativeAuthCacheEntries, this.negativeTtlMs);
        this.positiveCache = new bounded_cache_1.BoundedTtlLru(limits_1.CORE_LIMITS.maxPositiveAuthCacheEntries, limits_1.CORE_LIMITS.positiveAuthCacheTtlMs);
    }
    async ensureSchema() {
        if (this.schemaPromise)
            return this.schemaPromise;
        const table = this.db.prepare(`CREATE TABLE IF NOT EXISTS proxy_users (
      id TEXT PRIMARY KEY,
      protocol TEXT NOT NULL CHECK(protocol IN ('vless','trojan')),
      credential TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      UNIQUE(protocol, credential)
    )`);
        this.schemaPromise = (async () => {
            await table.run();
            // Keep the runtime bootstrap identical to schema.sql (C7): both indexes are created.
            await this.db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_users_lookup ON proxy_users(protocol, credential, enabled)`).run();
            await this.db.prepare(`CREATE INDEX IF NOT EXISTS idx_proxy_users_order ON proxy_users(protocol, enabled, created_at, id)`).run();
        })().catch((error) => { this.schemaPromise = null; throw error; });
        return this.schemaPromise;
    }
    // Canonical credential form everywhere in this module is lowercase: normalizeCredential()
    // lowercases wire credentials before lookup, record() lowercases stored values on read, and
    // the authenticate() query compares LOWER(credential) so rows that were inserted into D1 with
    // mixed case (manual SQL, migrations) still authenticate instead of listing-but-never-matching
    // under SQLite's case-sensitive BINARY collation (C3). New rows should be written lowercase.
    record(row) {
        return { id: row.id, protocol: row.protocol, credential: row.credential.toLowerCase(), enabled: row.enabled === 1, rawCredential: row.protocol === "vless" ? row.credential.toLowerCase() : undefined };
    }
    async authenticate(protocol, credential, signal) {
        if (signal.aborted)
            throw signal.reason;
        await this.ensureSchema();
        let normalized;
        try {
            normalized = normalizeCredential(protocol, credential);
        }
        catch {
            return null;
        }
        const key = `${protocol}:${normalized}`;
        const cachedPrincipal = this.positiveCache.get(key);
        // Return a copy: the cached Principal must never be shared mutable state that a caller
        // could annotate and thereby poison for later requests within the TTL (C17).
        if (cachedPrincipal)
            return { ...cachedPrincipal };
        if (this.negativeCache.get(key))
            return null;
        const row = await raceAbort(this.db.prepare(`SELECT id, protocol, credential, enabled, created_at FROM proxy_users WHERE protocol = ?1 AND LOWER(credential) = ?2 AND enabled = 1 LIMIT 1`).bind(protocol, normalized).first(), signal);
        const principal = row ? { id: row.id, protocol } : null;
        if (principal)
            this.positiveCache.set(key, { ...principal });
        else
            this.negativeCache.set(key, true);
        return principal;
    }
    async list(protocol, signal) {
        await this.ensureSchema();
        const out = [];
        let cursor = null;
        while (true) {
            const page = await this.listPageAfter(protocol, cursor, 256, signal);
            out.push(...page.records);
            if (!page.nextCursor || page.records.length < 256)
                return out;
            cursor = page.nextCursor;
        }
    }
    async count(protocol, signal) {
        await this.ensureSchema();
        // Trojan rows store only SHA-224 digests, so they cannot produce native password output.
        if (protocol === "trojan")
            return 0;
        const row = await raceAbort(this.db.prepare(`SELECT COUNT(*) AS count FROM proxy_users WHERE protocol = ?1 AND enabled = 1`).bind(protocol).first(), signal);
        return Number(row?.count ?? 0);
    }
    async listPage(protocol, offset, limit, signal) {
        if (offset < 0 || limit <= 0 || protocol === "trojan")
            return [];
        let cursor = null;
        let remaining = offset;
        while (remaining > 0) {
            const batch = await this.listPageAfter(protocol, cursor, Math.min(256, remaining), signal);
            if (!batch.records.length)
                return [];
            remaining -= batch.records.length;
            cursor = batch.nextCursor ?? null;
            if (!cursor)
                return [];
        }
        return (await this.listPageAfter(protocol, cursor, limit, signal)).records;
    }
    async listPageAfter(protocol, cursor, limit, signal) {
        if (limit <= 0 || protocol === "trojan")
            return { records: [] };
        await this.ensureSchema();
        const where = cursor
            ? ` AND (created_at > ?2 OR (created_at = ?2 AND id > ?3))`
            : "";
        const statement = cursor
            ? this.db.prepare(`SELECT id, protocol, credential, enabled, created_at FROM proxy_users WHERE protocol = ?1 AND enabled = 1${where} ORDER BY created_at ASC, id ASC LIMIT ?4`).bind(protocol, cursor.createdAt, cursor.id, limit)
            : this.db.prepare(`SELECT id, protocol, credential, enabled, created_at FROM proxy_users WHERE protocol = ?1 AND enabled = 1 ORDER BY created_at ASC, id ASC LIMIT ?2`).bind(protocol, limit);
        const rows = await raceAbort(statement.all(), signal);
        const records = rows.results.map((row) => this.record(row));
        const last = rows.results[rows.results.length - 1];
        return { records, ...(last && rows.results.length === limit ? { nextCursor: { createdAt: Number(last.created_at), id: last.id } } : {}) };
    }
}
exports.D1CredentialProvider = D1CredentialProvider;
function raceAbort(promise, signal) {
    if (signal.aborted)
        return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
        const onAbort = () => reject(signal.reason);
        signal.addEventListener("abort", onAbort, { once: true });
        promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
    });
}
class EnvironmentCredentialProvider {
    vless = new Map();
    trojan = new Map();
    constructor(env) {
        for (const item of (env.VLESS_USERS ?? "").split(/[,\r\n]+/)) {
            const value = item.trim().toLowerCase();
            if (!value)
                continue;
            try {
                const canonical = (0, binary_1.bytesToUuid)((0, binary_1.uuidToBytes)(value)).toLowerCase();
                this.vless.set(canonical, { id: canonical, protocol: "vless", credential: canonical, rawCredential: canonical, enabled: true });
            }
            catch { /* Ignore malformed configuration entries. */ }
        }
        for (const item of (env.TROJAN_USERS ?? "").split(/[,\r\n]+/)) {
            const value = item.trim();
            if (!value)
                continue;
            const digest = (0, sha224_1.sha224Hex)(new TextEncoder().encode(value)).toLowerCase();
            this.trojan.set(digest, { id: digest, protocol: "trojan", credential: digest, rawCredential: value, enabled: true });
        }
    }
    async authenticate(protocol, credential, _signal) {
        if (protocol === "vless") {
            try {
                const uuid = (0, binary_1.bytesToUuid)(credential).toLowerCase();
                return this.vless.has(uuid) ? { id: uuid, protocol } : null;
            }
            catch {
                return null;
            }
        }
        try {
            const hash = new TextDecoder("ascii", { fatal: true }).decode(credential).toLowerCase();
            return /^[0-9a-f]{56}$/.test(hash) && this.trojan.has(hash) ? { id: hash, protocol } : null;
        }
        catch {
            return null;
        }
    }
    async list(protocol, _signal) {
        return [...(protocol === "vless" ? this.vless.values() : this.trojan.values())];
    }
    async count(protocol, _signal) {
        return protocol === "vless" ? this.vless.size : this.trojan.size;
    }
    async listPage(protocol, offset, limit, _signal) {
        // Match the D1 provider's contract: a negative offset must not make slice() count from
        // the end of the array and return tail records as a page (C15).
        if (offset < 0 || limit <= 0)
            return [];
        const values = [...(protocol === "vless" ? this.vless.values() : this.trojan.values())];
        return values.slice(offset, offset + limit);
    }
}
exports.EnvironmentCredentialProvider = EnvironmentCredentialProvider;
class CredentialRepository {
    primary;
    fallback;
    mode;
    constructor(primary, fallback, mode) {
        this.primary = primary;
        this.fallback = fallback;
        this.mode = mode;
    }
    async authenticate(protocol, credential, signal) {
        if (this.mode === "env")
            return this.fallback?.authenticate(protocol, credential, signal) ?? null;
        try {
            const primary = await this.primary.authenticate(protocol, credential, signal);
            if (primary || this.mode === "d1")
                return primary;
            if (this.mode === "hybrid")
                return this.fallback?.authenticate(protocol, credential, signal) ?? null;
            return null;
        }
        catch (error) {
            if (this.mode === "d1" || !this.fallback)
                throw error;
            return this.fallback.authenticate(protocol, credential, signal);
        }
    }
    async list(protocol, signal) {
        if (this.mode === "env")
            return this.fallback?.list?.(protocol, signal) ?? [];
        if (this.mode === "d1")
            return this.primary.list?.(protocol, signal) ?? [];
        // Legacy full-list API is retained for callers that explicitly request it.
        // Pagination itself never needs to materialize this combined list.
        const output = [];
        const pageSize = 256;
        let offset = 0;
        while (true) {
            const page = await this.listPage(protocol, offset, pageSize, signal);
            output.push(...page);
            if (page.length < pageSize)
                return output;
            offset += page.length;
        }
    }
    async count(protocol, signal) {
        if (this.mode === "env")
            return this.fallback?.count?.(protocol, signal) ?? 0;
        if (this.mode === "d1")
            return this.primary.count?.(protocol, signal) ?? 0;
        if (!this.fallback)
            return this.primary.count?.(protocol, signal) ?? 0;
        const primaryCount = await this.primary.count?.(protocol, signal) ?? 0;
        const primaryKeys = await this.getPrimaryKeys(protocol, signal);
        const seenFallback = new Set();
        let uniqueFallback = 0;
        const pageSize = 256;
        for (let offset = 0;; offset += pageSize) {
            const page = await this.fallback.listPage?.(protocol, offset, pageSize, signal)
                ?? (offset === 0 ? (await this.fallback.list?.(protocol, signal) ?? []) : []);
            if (!page.length)
                break;
            for (const record of page) {
                if (record.enabled === false)
                    continue;
                const key = credentialKey(record);
                if (primaryKeys.has(key) || seenFallback.has(key))
                    continue;
                seenFallback.add(key);
                uniqueFallback++;
            }
            if (page.length < pageSize || !this.fallback.listPage)
                break;
        }
        return primaryCount + uniqueFallback;
    }
    async listPage(protocol, offset, limit, signal) {
        if (offset < 0 || limit <= 0)
            return [];
        if (this.mode === "env")
            return this.fallback?.listPage?.(protocol, offset, limit, signal) ?? [];
        if (this.mode === "d1")
            return this.primary.listPage?.(protocol, offset, limit, signal) ?? [];
        if (!this.fallback)
            return this.primary.listPage?.(protocol, offset, limit, signal) ?? [];
        const primaryCount = await this.primary.count?.(protocol, signal) ?? 0;
        if (offset < primaryCount) {
            const page = await this.primary.listPage?.(protocol, offset, limit, signal) ?? [];
            return page.filter((record) => record.enabled !== false);
        }
        const primaryKeys = await this.getPrimaryKeys(protocol, signal);
        let logicalOffset = offset - primaryCount;
        const out = [];
        const pageSize = Math.max(256, Math.min(1024, limit * 4));
        for (let sourceOffset = 0;; sourceOffset += pageSize) {
            const page = await this.fallback.listPage?.(protocol, sourceOffset, pageSize, signal)
                ?? (sourceOffset === 0 ? (await this.fallback.list?.(protocol, signal) ?? []) : []);
            if (!page.length)
                break;
            for (const record of page) {
                if (record.enabled === false)
                    continue;
                const key = credentialKey(record);
                if (primaryKeys.has(key))
                    continue;
                if (logicalOffset > 0) {
                    logicalOffset--;
                    continue;
                }
                out.push(record);
                if (out.length >= limit)
                    return out;
            }
            if (page.length < pageSize || !this.fallback.listPage)
                break;
        }
        return out;
    }
    async getPrimaryKeys(protocol, signal) {
        // Deliberately request-scoped: D1 changes must become visible immediately across calls.
        // A persistent isolate-level key cache would create stale hybrid deduplication.
        const keys = new Set();
        const pageSize = 256;
        if (this.primary.listPage) {
            for (let offset = 0;; offset += pageSize) {
                const page = await this.primary.listPage(protocol, offset, pageSize, signal);
                if (!page.length)
                    break;
                for (const record of page) {
                    if (record.enabled !== false)
                        keys.add(credentialKey(record));
                }
                if (page.length < pageSize)
                    break;
            }
        }
        else {
            for (const record of await this.primary.list?.(protocol, signal) ?? []) {
                if (record.enabled !== false)
                    keys.add(credentialKey(record));
            }
        }
        return keys;
    }
}
exports.CredentialRepository = CredentialRepository;
function credentialKey(record) {
    return `${record.protocol}:${record.credential.toLowerCase()}`;
}
class KVJsonStore {
    kv;
    constructor(kv) {
        this.kv = kv;
    }
    async get(key) { return this.kv.get(key, "json"); }
    async put(key, value, expirationTtl) {
        if (expirationTtl !== undefined && (!Number.isInteger(expirationTtl) || expirationTtl < 60))
            throw new RangeError("KV expirationTtl must be an integer of at least 60 seconds");
        await this.kv.put(key, JSON.stringify(value), expirationTtl === undefined ? undefined : { expirationTtl });
    }
    async delete(key) { await this.kv.delete(key); }
}
exports.KVJsonStore = KVJsonStore;
function credentialMode(env) {
    const mode = env.CORE_CREDENTIAL_MODE?.trim().toLowerCase();
    // Resolve the mode with the same semantics validateEnvironment enforces (C13): previously
    // the no-DB early return silently reinterpreted even an explicit or invalid mode as "env",
    // so any entry point that skipped validation got different credential semantics.
    if (mode && !["d1", "env", "hybrid"].includes(mode))
        throw new Error(`Unsupported CORE_CREDENTIAL_MODE: ${mode}`);
    if (!env.DB) {
        if (mode && mode !== "env")
            throw new Error(`CORE_CREDENTIAL_MODE=${mode} requires the DB binding`);
        return "env";
    }
    if (!mode || mode === "d1")
        return "d1";
    if (mode === "env" || mode === "hybrid")
        return mode;
    throw new Error(`Unsupported CORE_CREDENTIAL_MODE: ${mode}`);
}
function createCredentialProvider(env) {
    const fallback = new EnvironmentCredentialProvider(env);
    const db = env.DB;
    if (!db)
        return fallback;
    // Defense in depth for the binding-shape check in validation (C6): never cast a truthy
    // non-D1 value into the provider.
    if (typeof db.prepare !== "function")
        throw new Error("DB binding is not a D1 database (missing prepare())");
    const mode = credentialMode(env);
    return new CredentialRepository(new D1CredentialProvider(db), mode === "d1" ? null : fallback, mode);
}

};

__m["util/binary"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.concatBytes = concatBytes;
exports.equalBytes = equalBytes;
exports.bytesToHex = bytesToHex;
exports.hexToBytes = hexToBytes;
exports.uuidToBytes = uuidToBytes;
exports.bytesToUuid = bytesToUuid;
exports.parseHostPort = parseHostPort;
exports.validatePort = validatePort;
exports.encodeUtf8 = encodeUtf8;
exports.decodeUtf8 = decodeUtf8;
function concatBytes(...parts) {
    const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
        out.set(part, offset);
        offset += part.byteLength;
    }
    return out;
}
function equalBytes(a, b) {
    if (a.byteLength !== b.byteLength)
        return false;
    let diff = 0;
    for (let i = 0; i < a.byteLength; i++)
        diff |= a[i] ^ b[i];
    return diff === 0;
}
function bytesToHex(bytes) {
    let out = "";
    for (const byte of bytes)
        out += byte.toString(16).padStart(2, "0");
    return out;
}
function hexToBytes(hex) {
    if (!/^[0-9a-fA-F]*$/.test(hex) || hex.length % 2 !== 0)
        throw new Error("Invalid hex string");
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i++)
        out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    return out;
}
function uuidToBytes(uuid) {
    const normalized = uuid.trim().toLowerCase();
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(normalized))
        throw new Error("Invalid UUID");
    return hexToBytes(normalized.replaceAll("-", ""));
}
function bytesToUuid(bytes) {
    if (bytes.byteLength !== 16)
        throw new Error("UUID must contain 16 bytes");
    const hex = bytesToHex(bytes);
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
function parseHostPort(value, defaultPort) {
    const raw = value.trim();
    if (!raw)
        throw new Error("Empty endpoint");
    if (raw.startsWith("[")) {
        const close = raw.indexOf("]");
        if (close < 0)
            throw new Error("Invalid bracketed endpoint");
        const host = raw.slice(1, close);
        const rest = raw.slice(close + 1);
        if (!rest)
            return { host, port: defaultPort };
        if (!rest.startsWith(":"))
            throw new Error("Invalid endpoint");
        const portText = rest.slice(1);
        if (!/^\d+$/.test(portText))
            throw new Error("Invalid port");
        const port = Number(portText);
        validatePort(port);
        return { host, port };
    }
    const colonCount = (raw.match(/:/g) ?? []).length;
    if (colonCount > 1)
        return { host: raw, port: defaultPort };
    const idx = raw.lastIndexOf(":");
    if (idx < 0)
        return { host: raw, port: defaultPort };
    const host = raw.slice(0, idx);
    const portText = raw.slice(idx + 1);
    if (!/^\d+$/.test(portText))
        throw new Error("Invalid port");
    const port = Number(portText);
    validatePort(port);
    return { host, port };
}
function validatePort(port) {
    if (!Number.isInteger(port) || port < 1 || port > 65535)
        throw new Error("Port must be an integer from 1 to 65535");
}
function encodeUtf8(value) {
    return new TextEncoder().encode(value);
}
function decodeUtf8(bytes) {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}

};

__m["routing/rules"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CORE_RULE_EGRESS_TYPES = void 0;
exports.expandIpv6Literal = expandIpv6Literal;
exports.canonicalRouteHost = canonicalRouteHost;
exports.parseCoreRule = parseCoreRule;
exports.parseCoreRules = parseCoreRules;
const errors_1 = __r("../core/errors");
const hostname_1 = __r("../util/hostname");
const VALID_EGRESS = new Set(["direct", "proxyip", "relay", "nat64"]);
const VALID_PROTOCOL = new Set(["vless", "trojan"]);
function ruleError(message) {
    throw new errors_1.CoreError("CONFIGURATION_ERROR", message);
}
/**
 * Expands an IPv6 literal to its full eight-group lowercase form
 * ("::1" -> "0:0:0:0:0:0:0:1"), or returns null when the value is not a
 * structurally valid IPv6 literal. The expanded form is the canonical route
 * representation because the protocol parsers emit IPv6 destinations in
 * exactly this form, so rule text and destination text always meet in one
 * representation regardless of how the operator wrote the address.
 */
function expandIpv6Literal(value) {
    const ascii = (0, hostname_1.toAsciiHostname)(value);
    if (!ascii || !ascii.includes(":"))
        return null;
    let text = ascii;
    if (text.includes(".")) {
        // IPv4-embedded tail (e.g. "::ffff:192.0.2.1") counts as two groups.
        const cut = text.lastIndexOf(":");
        if (cut < 0)
            return null;
        const octets = text.slice(cut + 1).split(".");
        if (octets.length !== 4 || octets.some((octet) => !/^\d{1,3}$/.test(octet) || Number(octet) > 255))
            return null;
        const high = ((Number(octets[0]) << 8) | Number(octets[1])).toString(16);
        const low = ((Number(octets[2]) << 8) | Number(octets[3])).toString(16);
        text = `${text.slice(0, cut)}:${high}:${low}`;
    }
    const halves = text.split("::");
    if (halves.length > 2)
        return null;
    const parseSide = (side) => {
        if (!side)
            return [];
        const groups = side.split(":");
        if (groups.some((group) => !/^[0-9a-f]{1,4}$/.test(group)))
            return null;
        return groups.map((group) => parseInt(group, 16).toString(16));
    };
    const left = parseSide(halves[0] ?? "");
    if (!left)
        return null;
    if (halves.length === 1)
        return left.length === 8 ? left.join(":") : null;
    const right = parseSide(halves[1] ?? "");
    if (!right)
        return null;
    const missing = 8 - left.length - right.length;
    if (missing < 1)
        return null;
    return [...left, ...new Array(missing).fill("0"), ...right].join(":");
}
/**
 * Canonical form used for every host comparison in the router: lowercased,
 * trailing root dot removed, leading dots removed, paired brackets removed,
 * and IPv6 literals expanded. The router applies this to BOTH the stored
 * rule and the live destination host (B8/B9), so the two sides can never
 * drift apart textually.
 */
function canonicalRouteHost(value) {
    const normalized = (0, hostname_1.normalizeHostname)(value).replace(/^\.+/, "");
    if (normalized.includes(":"))
        return expandIpv6Literal(normalized) ?? normalized;
    return normalized;
}
function canonicalHost(value, kind) {
    const normalized = (0, hostname_1.normalizeHostname)(value).replace(/^\.+/, "");
    if (normalized.includes(":")) {
        // IPv6 destinations are real (both parsers produce them), so host=
        // matchers accept IPv6 literals and store them in canonical expanded
        // form (B8). A suffix matcher is a DNS-name concept; an IPv6 "suffix"
        // would be a prefix policy in disguise, so it fails configuration
        // loudly instead of silently never matching.
        if (kind === "suffix") {
            ruleError(`CORE_RULES suffix matcher must contain a valid hostname; IPv6 literals are only supported in host matchers: ${value}`);
        }
        const expanded = expandIpv6Literal(normalized);
        if (!expanded) {
            ruleError(`CORE_RULES host matcher must contain a valid hostname or IPv6 literal: ${value}`);
        }
        return expanded;
    }
    if (!normalized || !(0, hostname_1.isAsciiHostname)(normalized)) {
        ruleError(`CORE_RULES ${kind} matcher must contain a valid hostname: ${value}`);
    }
    return normalized;
}
function canonicalMatcher(matcher, entry) {
    const equal = matcher.indexOf("=");
    if (equal <= 0 || equal !== matcher.lastIndexOf("=")) {
        ruleError(`Invalid CORE_RULES matcher: ${matcher} in ${entry}`);
    }
    const kind = matcher.slice(0, equal).trim().toLowerCase();
    const value = matcher.slice(equal + 1).trim();
    if (!(kind === "host" || kind === "suffix" || kind === "protocol" || kind === "port")) {
        ruleError(`Unsupported CORE_RULES matcher: ${matcher}`);
    }
    if (!value)
        ruleError(`CORE_RULES ${kind} matcher value must not be empty: ${entry}`);
    return { kind, value };
}
function parseCoreRule(entry) {
    const clean = entry.trim();
    if (!clean)
        ruleError("CORE_RULES entry must not be empty");
    const parts = clean.split("|").map((value) => value.trim());
    if (parts.length < 2 || parts.length > 3) {
        ruleError(`Invalid CORE_RULES entry (expected matcher|egress[|fallback]): ${clean}`);
    }
    const matcher = canonicalMatcher(parts[0] ?? "", clean);
    const egress = (parts[1] ?? "").toLowerCase();
    const fallback = (parts[2] ?? "").toLowerCase();
    if (!VALID_EGRESS.has(egress))
        ruleError(`Unsupported CORE_RULES egress: ${clean}`);
    if (fallback && !VALID_EGRESS.has(fallback))
        ruleError(`Unsupported CORE_RULES fallback egress: ${clean}`);
    const base = { egress, ...(fallback ? { fallbackEgress: fallback } : {}) };
    switch (matcher.kind) {
        case "host":
            return { host: canonicalHost(matcher.value, "host"), ...base };
        case "suffix":
            return { suffix: `.${canonicalHost(matcher.value, "suffix")}`, ...base };
        case "protocol":
            if (!VALID_PROTOCOL.has(matcher.value.toLowerCase())) {
                ruleError(`Unsupported CORE_RULES protocol matcher: ${clean}`);
            }
            return { protocol: matcher.value.toLowerCase(), ...base };
        case "port": {
            if (!/^\d+$/.test(matcher.value))
                ruleError(`CORE_RULES port must be 1..65535: ${clean}`);
            const port = Number(matcher.value);
            if (!Number.isSafeInteger(port) || port < 1 || port > 65535)
                ruleError(`CORE_RULES port must be 1..65535: ${clean}`);
            return { port, ...base };
        }
    }
}
function parseCoreRules(raw) {
    return (raw ?? "").split(/[\r\n]+/).map((value) => value.trim()).filter(Boolean).map(parseCoreRule);
}
exports.CORE_RULE_EGRESS_TYPES = Object.freeze(["direct", "proxyip", "relay", "nat64"]);

};

__m["routing/router"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DefaultRouter = void 0;
exports.parseEgressMode = parseEgressMode;
const errors_1 = __r("../core/errors");
const rules_1 = __r("./rules");
function matches(rule, context) {
    // Canonicalize BOTH sides into one representation (lowercase, root dot and
    // paired brackets removed, IPv6 expanded) instead of trusting that the
    // context host was pre-normalized by a parser (B8/B9). Brackets are only
    // stripped as a pair, via normalizeHostname inside canonicalRouteHost —
    // never one side at a time.
    const host = (0, rules_1.canonicalRouteHost)(context.destination.host);
    if (rule.host && host !== (0, rules_1.canonicalRouteHost)(rule.host))
        return false;
    if (rule.suffix) {
        // Do not assume the stored suffix starts with ".": derive the bare body
        // and the dotted form explicitly so programmatically built rules behave
        // exactly like parsed ones (B10).
        const body = (0, rules_1.canonicalRouteHost)(rule.suffix.replace(/^\.+/, ""));
        if (!(host === body || host.endsWith(`.${body}`)))
            return false;
    }
    if (rule.protocol && context.protocol !== rule.protocol)
        return false;
    if (rule.port !== undefined && context.destination.port !== rule.port)
        return false;
    return true;
}
class DefaultRouter {
    mode;
    rules;
    smartFallbacks;
    constructor(mode = "smart", rawRules, availability = {}) {
        this.mode = mode;
        this.rules = (0, rules_1.parseCoreRules)(rawRules);
        this.smartFallbacks = [
            availability.proxyip ? "proxyip" : null,
            availability.relay ? "relay" : null,
            availability.nat64 ? "nat64" : null,
        ].filter((item) => Boolean(item));
    }
    async route(context, _signal) {
        // Rule precedence is deliberately first-match in CORE_RULES order
        // (firewall-style): a broad early rule shadows later, more specific
        // rules, so the order rules are written in is load-bearing (B11).
        for (const rule of this.rules) {
            if (matches(rule, context))
                return { egress: rule.egress, ...(rule.fallbackEgress ? { fallbackEgress: rule.fallbackEgress } : {}) };
        }
        switch (this.mode) {
            case "proxyip": return { egress: "proxyip" };
            case "nat64": return { egress: "nat64" };
            case "fallback": return { egress: "direct", fallbackEgress: "proxyip" };
            case "smart": return { egress: "direct", fallbackEgresses: [...this.smartFallbacks] };
            default: return { egress: "direct" };
        }
    }
    get ruleCount() { return this.rules.length; }
}
exports.DefaultRouter = DefaultRouter;
function parseEgressMode(value) {
    const mode = (value ?? "smart").trim().toLowerCase();
    if (!mode || mode === "smart")
        return "smart";
    if (mode === "direct")
        return "direct";
    if (mode === "proxyip")
        return "proxyip";
    if (mode === "fallback")
        return "fallback";
    if (mode === "nat64")
        return "nat64";
    throw new errors_1.CoreError("CONFIGURATION_ERROR", `Unsupported EGRESS_MODE: ${mode}`);
}

};

__m["egress/nat64"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Nat64Egress = exports.DnsNat64AddressResolver = void 0;
exports.parseNat64Prefixes = parseNat64Prefixes;
exports.ipv4ToNat64 = ipv4ToNat64;
const errors_1 = __r("../core/errors");
const direct_1 = __r("./direct");
const limits_1 = __r("../core/limits");
const lifecycle_1 = __r("../util/lifecycle");
const RFC6052_PREFIX_LENGTHS = [32, 40, 48, 56, 64, 96];
function parseIpv4(value) {
    const parts = value.trim().split(".");
    if (parts.length !== 4 || parts.some((part) => !/^\d{1,3}$/.test(part)))
        return null;
    const nums = parts.map(Number);
    return nums.every((n) => n >= 0 && n <= 255) ? nums : null;
}
function parseIpv6Bytes(value) {
    let input = value.trim();
    if (input.startsWith("[") && input.endsWith("]"))
        input = input.slice(1, -1);
    if (!input || !input.includes(":"))
        throw new Error("not IPv6");
    const sides = input.split("::");
    if (sides.length > 2)
        throw new Error("multiple ::");
    // Audit fix E11: a dotted-quad is only legal as the final 32 bits of the
    // whole address (RFC 4291 §2.2). Being the last token of the left side is
    // not enough when a "::" follows it (e.g. "1.2.3.4::" must be rejected).
    const parseSide = (side, allowIpv4Tail) => {
        if (!side)
            return [];
        const parts = side.split(":");
        const out = [];
        for (const [index, part] of parts.entries()) {
            if (part.includes(".")) {
                const ipv4 = parseIpv4(part);
                if (!ipv4 || !allowIpv4Tail || index !== parts.length - 1)
                    throw new Error("invalid IPv4 tail");
                out.push((ipv4[0] << 8) | ipv4[1], (ipv4[2] << 8) | ipv4[3]);
            }
            else {
                if (!/^[0-9a-fA-F]{1,4}$/.test(part))
                    throw new Error("invalid IPv6 group");
                out.push(Number.parseInt(part, 16));
            }
        }
        return out;
    };
    const left = parseSide(sides[0] ?? "", sides.length === 1);
    const right = parseSide(sides[1] ?? "", sides.length === 2);
    const groups = sides.length === 2
        ? [...left, ...new Array(8 - left.length - right.length).fill(0), ...right]
        : left;
    if (groups.length !== 8)
        throw new Error("invalid IPv6 group count");
    const out = new Uint8Array(16);
    groups.forEach((group, i) => {
        out[i * 2] = (group >> 8) & 0xff;
        out[i * 2 + 1] = group & 0xff;
    });
    return out;
}
function formatIpv6(bytes) {
    if (bytes.length !== 16)
        throw new Error("invalid IPv6 length");
    const groups = [];
    for (let i = 0; i < 16; i += 2)
        groups.push((bytes[i] << 8) | bytes[i + 1]);
    let bestStart = -1;
    let bestLength = 0;
    for (let i = 0; i < 8;) {
        if (groups[i] !== 0) {
            i++;
            continue;
        }
        const start = i;
        while (i < 8 && groups[i] === 0)
            i++;
        const length = i - start;
        if (length > bestLength && length >= 2) {
            bestStart = start;
            bestLength = length;
        }
    }
    const hex = (value) => value.toString(16);
    if (bestStart < 0)
        return groups.map(hex).join(":");
    const left = groups.slice(0, bestStart).map(hex).join(":");
    const right = groups.slice(bestStart + bestLength).map(hex).join(":");
    if (!left && !right)
        return "::";
    if (!left)
        return `::${right}`;
    if (!right)
        return `${left}::`;
    return `${left}::${right}`;
}
function prefixBits(bytes, length) {
    const bits = [];
    for (let i = 0; i < length; i++)
        bits.push((bytes[Math.floor(i / 8)] >> (7 - (i % 8))) & 1);
    return bits;
}
function ipv4Bits(bytes) {
    const bits = [];
    for (const value of bytes)
        for (let i = 7; i >= 0; i--)
            bits.push((value >> i) & 1);
    return bits;
}
function bitsToIpv6(bits) {
    if (bits.length !== 128)
        throw new Error("NAT64 synthesis did not produce 128 bits");
    const out = new Uint8Array(16);
    for (let i = 0; i < bits.length; i++)
        out[Math.floor(i / 8)] |= bits[i] << (7 - (i % 8));
    return out;
}
function normalizePrefix(value) {
    const raw = value.trim();
    const slash = raw.lastIndexOf("/");
    const hasLength = slash >= 0;
    const addressText = hasLength ? raw.slice(0, slash) : raw;
    const lengthText = hasLength ? raw.slice(slash + 1) : "96";
    if (!hasLength && !addressText.endsWith("::")) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `NAT64 prefix must include /32,/40,/48,/56,/64,/96 or use a trailing :: legacy /96 form: ${value}`);
    }
    const prefixLength = Number(lengthText);
    if (!RFC6052_PREFIX_LENGTHS.includes(prefixLength)) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `NAT64 prefix length must be one of ${RFC6052_PREFIX_LENGTHS.join(", ")}: ${value}`);
    }
    let bytes;
    try {
        bytes = parseIpv6Bytes(addressText);
    }
    catch (error) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid NAT64 prefix: ${value}`, false, { cause: error });
    }
    for (let i = prefixLength; i < 128; i++) {
        const bit = (bytes[Math.floor(i / 8)] >> (7 - (i % 8))) & 1;
        if (bit !== 0)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `NAT64 prefix host bits must be zero: ${value}`);
    }
    return { prefix: `${formatIpv6(bytes)}/${prefixLength}`, prefixLength: prefixLength };
}
function parseNat64Prefixes(raw) {
    const values = (raw ?? "").split(/[\r\n,]+/).map((item) => item.trim()).filter(Boolean);
    const seen = new Set();
    const out = [];
    for (const value of values) {
        const normalized = normalizePrefix(value);
        const key = `${normalized.prefix}/${normalized.prefixLength}`;
        if (!seen.has(key)) {
            seen.add(key);
            out.push(normalized);
        }
    }
    return out;
}
function ipv4ToNat64(ipv4, prefix) {
    const bytes = parseIpv4(ipv4);
    if (!bytes)
        throw new errors_1.CoreError("INVALID_DESTINATION", `Invalid IPv4 address for NAT64: ${ipv4}`);
    const normalized = normalizePrefix(prefix);
    const p = normalized.prefixLength;
    const bits = [...prefixBits(parseIpv6Bytes(normalized.prefix.split("/")[0]), p), ...ipv4Bits(bytes)];
    if (p < 96) {
        const suffixLength = 88 - p;
        bits.push(...new Array(suffixLength).fill(0));
        bits.splice(64, 0, ...new Array(8).fill(0));
    }
    return formatIpv6(bitsToIpv6(bits));
}
class DnsNat64AddressResolver {
    dns;
    constructor(dns) {
        this.dns = dns;
    }
    async resolveIpv4(host, signal) {
        return (await this.dns.resolve(host, "ipv4", signal)).map((item) => item.address).filter((value) => parseIpv4(value) !== null);
    }
}
exports.DnsNat64AddressResolver = DnsNat64AddressResolver;
class Nat64Egress {
    connectTimeoutMs;
    dns;
    type = "nat64";
    prefixes;
    decisionTimeoutMs;
    constructor(prefixes, connectTimeoutMs, dns, decisionTimeoutMs = limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs) {
        this.connectTimeoutMs = connectTimeoutMs;
        this.dns = dns;
        if (!prefixes.length)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "NAT64 egress requires at least one prefix");
        this.prefixes = prefixes;
        this.decisionTimeoutMs = Number.isFinite(decisionTimeoutMs) && decisionTimeoutMs > 0 ? decisionTimeoutMs : limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs;
    }
    async connect(destination, signal) {
        // Audit fix E6: validate the client-influenced destination port in this
        // layer instead of trusting upstream parsing blindly.
        if (!Number.isInteger(destination.port) || destination.port < 1 || destination.port > 65535) {
            throw new errors_1.CoreError("INVALID_DESTINATION", `Destination port must be an integer from 1 to 65535, got ${destination.port}`);
        }
        const ipv4 = parseIpv4(destination.host) ? [destination.host] : await this.dns.resolveIpv4(destination.host, signal);
        if (!ipv4.length)
            throw new errors_1.CoreError("DNS_FAILED", `NAT64 requires an IPv4 address for ${destination.host}`, true);
        // Audit fix E5: aggregate decision budget across all candidate attempts
        // (previously up to maxNat64Candidates × connectTimeoutMs with no cap).
        const deadlineAt = Date.now() + this.decisionTimeoutMs;
        let lastError;
        let attempts = 0;
        outer: for (const address of ipv4) {
            for (const prefix of this.prefixes) {
                if (attempts++ >= limits_1.CORE_LIMITS.maxNat64Candidates)
                    break;
                if (signal.aborted)
                    throw signal.reason;
                if (Date.now() >= deadlineAt)
                    break outer;
                const synthesized = ipv4ToNat64(address, prefix.prefix);
                try {
                    const socket = await (0, direct_1.connectTcp)({ host: synthesized, port: destination.port }, signal, (0, lifecycle_1.remainingMs)(deadlineAt, this.connectTimeoutMs));
                    return {
                        type: "nat64",
                        readable: socket.readable,
                        writable: socket.writable,
                        // Audit fix E7: full teardown — also cancel the readable side of
                        // the allowHalfOpen socket instead of only closing writable.
                        close: async () => {
                            await socket.readable.cancel().catch(() => { });
                            await socket.close().catch(() => { });
                        },
                    };
                }
                catch (error) {
                    lastError = error;
                    if (signal.aborted)
                        throw signal.reason;
                }
            }
            if (attempts >= limits_1.CORE_LIMITS.maxNat64Candidates)
                break;
        }
        throw new errors_1.CoreError("EGRESS_FAILED", `NAT64 connection failed for ${destination.host}:${destination.port}`, true, { cause: lastError });
    }
}
exports.Nat64Egress = Nat64Egress;

};

__m["egress/direct"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DirectEgress = void 0;
exports.connectTcp = connectTcp;
const cloudflare_sockets_1 = __r("cloudflare:sockets");
const limits_1 = __r("../core/limits");
const errors_1 = __r("../core/errors");
async function connectTcp(destination, signal, timeoutMs, secureTransport = "off") {
    // Audit fix E6: validate the client-influenced destination port at the
    // single choke point every egress dial passes through, instead of
    // depending on upstream destination parsing or the runtime to reject it.
    if (!Number.isInteger(destination.port) || destination.port < 1 || destination.port > 65535) {
        throw new errors_1.CoreError("INVALID_DESTINATION", `Destination port must be an integer from 1 to 65535, got ${destination.port}`);
    }
    if (signal.aborted)
        throw signal.reason;
    let socket = null;
    let timer;
    let onAbort;
    try {
        socket = (0, cloudflare_sockets_1.connect)({ hostname: destination.host, port: destination.port }, { secureTransport, allowHalfOpen: true });
        const opened = socket.opened;
        const timeout = new Promise((_, reject) => {
            timer = setTimeout(() => reject(new errors_1.CoreError("TIMEOUT", "TCP connection timeout", true)), timeoutMs);
        });
        const aborted = new Promise((_, reject) => {
            onAbort = () => {
                void socket?.close().catch(() => { });
                reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
            };
            signal.addEventListener("abort", onAbort, { once: true });
            if (signal.aborted)
                onAbort();
        });
        await Promise.race([opened, timeout, aborted]);
        if (signal.aborted)
            throw signal.reason ?? new errors_1.CoreError("CLIENT_CLOSED", "Connection aborted");
        return socket;
    }
    catch (error) {
        await socket?.close().catch(() => { });
        if (error instanceof errors_1.CoreError)
            throw error;
        if (error instanceof DOMException && error.name === "AbortError") {
            throw new errors_1.CoreError("CLIENT_CLOSED", "Connection aborted", false, { cause: error });
        }
        throw new errors_1.CoreError("EGRESS_FAILED", `TCP connection failed to ${destination.host}:${destination.port}`, true, { cause: error });
    }
    finally {
        if (timer !== undefined)
            clearTimeout(timer);
        if (onAbort)
            signal.removeEventListener("abort", onAbort);
    }
}
async function raceResolvedAddresses(addresses, port, signal, timeoutMs, policy) {
    const candidates = [...new Set(addresses.map((value) => value.trim()).filter(Boolean))];
    if (!candidates.length)
        throw new errors_1.CoreError("EGRESS_FAILED", "No resolved addresses", true);
    const winnerController = new AbortController();
    const combined = combineSignals(signal, winnerController.signal);
    const active = [];
    let winner = null;
    let nextIndex = 0;
    let lastError = null;
    // Audit fix E12: stagger is relative to attempt start order and elapsed
    // time (RFC 8305: attempts spaced from the previous attempt), not the
    // candidate's absolute index — a candidate started after earlier attempts
    // failed fast must not still wait index × staggerMs.
    const raceStartedAt = performance.now();
    let attemptsStarted = 0;
    const startAttempt = (address, delayMs) => {
        const attempt = { promise: Promise.resolve(), settled: false };
        attempt.promise = (async () => {
            try {
                await abortableDelay(delayMs, combined.signal);
                if (combined.signal.aborted || winner)
                    return;
                const socket = await connectTcp({ host: address, port }, combined.signal, timeoutMs);
                if (winner) {
                    await socket.close().catch(() => { });
                    return;
                }
                winner = socket;
                winnerController.abort("address race winner");
            }
            catch (error) {
                if (!combined.signal.aborted || !winner)
                    lastError = error;
            }
            finally {
                attempt.settled = true;
            }
        })();
        return attempt;
    };
    try {
        while (!winner && nextIndex < candidates.length) {
            while (!winner && active.length < policy.maxConcurrent && nextIndex < candidates.length) {
                const index = nextIndex++;
                const order = attemptsStarted++;
                const elapsed = performance.now() - raceStartedAt;
                const delay = order === 0 ? 0 : Math.max(0, policy.staggerMs * order - elapsed);
                active.push(startAttempt(candidates[index], delay));
            }
            if (!winner && active.length) {
                await Promise.race(active.map((attempt) => attempt.promise.catch(() => undefined)));
                for (let i = active.length - 1; i >= 0; i--) {
                    if (active[i].settled)
                        active.splice(i, 1);
                }
            }
            if (signal.aborted)
                break;
        }
        if (winner)
            return winner;
        await Promise.allSettled(active.map((attempt) => attempt.promise));
        if (signal.aborted)
            throw signal.reason;
        throw new errors_1.CoreError("EGRESS_FAILED", `Resolved TCP connection failed for ${port}`, true, { cause: lastError });
    }
    finally {
        combined.dispose();
    }
}
function abortableDelay(ms, signal) {
    if (ms <= 0)
        return Promise.resolve();
    return new Promise((resolve, reject) => {
        let settled = false;
        const timer = setTimeout(() => {
            settled = true;
            signal.removeEventListener("abort", onAbort);
            resolve();
        }, ms);
        const onAbort = () => {
            if (settled)
                return;
            settled = true;
            clearTimeout(timer);
            signal.removeEventListener("abort", onAbort);
            reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        if (signal.aborted)
            onAbort();
    });
}
function combineSignals(a, b) {
    const controller = new AbortController();
    const abort = (reason) => { if (!controller.signal.aborted)
        controller.abort(reason); };
    const onA = () => abort(a.reason);
    const onB = () => abort(b.reason);
    if (a.aborted)
        abort(a.reason);
    if (b.aborted)
        abort(b.reason);
    a.addEventListener("abort", onA, { once: true });
    b.addEventListener("abort", onB, { once: true });
    return { signal: controller.signal, dispose: () => { a.removeEventListener("abort", onA); b.removeEventListener("abort", onB); } };
}
class DirectEgress {
    connectTimeoutMs;
    dns;
    addressRace;
    type = "direct";
    constructor(connectTimeoutMs = limits_1.CORE_LIMITS.defaultEgressConnectTimeoutMs, dns, addressRace = {
        maxConcurrent: limits_1.CORE_LIMITS.maxConcurrentAddressTries,
        staggerMs: limits_1.CORE_LIMITS.defaultAddressTryStaggerMs,
    }) {
        this.connectTimeoutMs = connectTimeoutMs;
        this.dns = dns;
        this.addressRace = addressRace;
    }
    async connect(destination, signal) {
        const socket = await this.open(destination, signal);
        return {
            type: "direct",
            readable: socket.readable,
            writable: socket.writable,
            // Audit fix E7: full teardown — also cancel the readable side of the
            // allowHalfOpen socket instead of only closing the writable side,
            // which could leave the readable side lingering until the peer closes.
            close: async () => {
                await socket.readable.cancel().catch(() => { });
                await socket.close().catch(() => { });
            },
        };
    }
    async open(destination, signal) {
        if (!this.dns || !this.dns.enabled)
            return connectTcp(destination, signal, this.connectTimeoutMs);
        const resolved = await this.dns.resolve(destination.host, undefined, signal);
        if (!resolved.length)
            throw new errors_1.CoreError("DNS_FAILED", `DNS returned no address for ${destination.host}`, true);
        return raceResolvedAddresses(resolved.map((item) => item.address), destination.port, signal, this.connectTimeoutMs, this.addressRace);
    }
}
exports.DirectEgress = DirectEgress;

};

__m["egress/proxyip-pool"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildProxyIpPool = buildProxyIpPool;
const proxyip_1 = __r("./proxyip");
/** Health selection remains inside ProxyIpEgress so endpoint identity is preserved. */
function buildProxyIpPool(env, timeoutMs) {
    return new proxyip_1.ProxyIpEgress(env, timeoutMs);
}

};

__m["egress/proxyip"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ProxyIpEgress = void 0;
exports.performSocks5Connect = performSocks5Connect;
exports.hasProxyIp = hasProxyIp;
const limits_1 = __r("../core/limits");
const errors_1 = __r("../core/errors");
const binary_1 = __r("../util/binary");
const direct_1 = __r("./direct");
const lifecycle_1 = __r("../util/lifecycle");
const sha224_1 = __r("../util/sha224");
const SOCKS_VERSION = 0x05;
const METHOD_NO_AUTH = 0x00;
const METHOD_USERPASS = 0x02;
const METHOD_NO_ACCEPTABLE = 0xff;
const CMD_CONNECT = 0x01;
const ATYP_IPV4 = 0x01;
const ATYP_DOMAIN = 0x03;
const ATYP_IPV6 = 0x04;
const cloudflareSocketConnector = {
    connect(destination, signal, timeoutMs, secureTransport) {
        return (0, direct_1.connectTcp)(destination, signal, timeoutMs, secureTransport);
    },
};
// Audit fix E8: endpoint identity must distinguish different credential pairs
// (same host:port with another password is a different endpoint) without
// retaining the plaintext password in long-lived map keys. The password is
// represented by its SHA-224 hex digest instead.
function endpointIdentityKey(host, port, tls, username, password) {
    const passwordDigest = password ? (0, sha224_1.sha224Hex)((0, binary_1.encodeUtf8)(password)) : "";
    return JSON.stringify([host.toLowerCase(), port, tls, username ?? "", passwordDigest]);
}
// Audit fix E6: the client-influenced destination port is validated before it
// is encoded or dialed anywhere in this layer. SOCKS5 DST.PORT is a 16-bit
// field (RFC 1928); out-of-range values must be rejected, never truncated.
function assertValidDestinationPort(port) {
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
        throw new errors_1.CoreError("INVALID_DESTINATION", `Destination port must be an integer from 1 to 65535, got ${port}`);
    }
}
class ProxyIpEgress {
    connectTimeoutMs;
    type = "proxyip";
    endpoints;
    connector;
    health = new Map();
    maxFailures;
    cooldownMs;
    handshakeTimeoutMs;
    decisionTimeoutMs;
    constructor(env, connectTimeoutMs = limits_1.CORE_LIMITS.defaultEgressConnectTimeoutMs, connector = cloudflareSocketConnector, decisionTimeoutMs = limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs) {
        this.connectTimeoutMs = connectTimeoutMs;
        this.decisionTimeoutMs = Number.isFinite(decisionTimeoutMs) && decisionTimeoutMs > 0 ? decisionTimeoutMs : limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs;
        this.endpoints = parseProxyIpEndpoints(env);
        this.maxFailures = Math.max(1, Number.parseInt(env.CORE_MAX_PROXYIP_FAILURES ?? "3", 10) || 3);
        const configuredCooldown = Number.parseInt(env.CORE_PROXYIP_COOLDOWN_MS ?? String(limits_1.CORE_LIMITS.defaultProxyIpCooldownMs), 10);
        this.cooldownMs = Number.isFinite(configuredCooldown) && configuredCooldown >= 1000 ? configuredCooldown : limits_1.CORE_LIMITS.defaultProxyIpCooldownMs;
        const configuredHandshake = Number.parseInt(env.CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS ?? String(limits_1.CORE_LIMITS.defaultProxyIpHandshakeTimeoutMs), 10);
        this.handshakeTimeoutMs = Number.isFinite(configuredHandshake) && configuredHandshake >= 100 ? Math.min(configuredHandshake, 60_000) : limits_1.CORE_LIMITS.defaultProxyIpHandshakeTimeoutMs;
        const username = env.PROXY_IP_USERNAME?.trim();
        const password = env.PROXY_IP_PASSWORD;
        if (!env.PROXY_IP_ENDPOINTS_JSON?.trim() && ((username && !password) || (!username && password))) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "PROXY_IP_USERNAME and PROXY_IP_PASSWORD must be configured together");
        }
        this.connector = connector;
        if (this.endpoints.length === 0) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "ProxyIP egress requires PROXY_IP or PROXY_IPS");
        }
    }
    get diagnostics() {
        const now = Date.now();
        const states = [...this.health.values()];
        const tlsModes = new Set(this.endpoints.map((endpoint) => endpoint.tls ? "on" : "off"));
        const authModes = new Set(this.endpoints.map((endpoint) => endpoint.credentials.username ? "username-password" : "none"));
        return {
            transport: "socks5",
            endpoints: this.endpoints.length,
            tls: tlsModes.size === 1 ? [...tlsModes][0] : "mixed",
            authentication: authModes.size === 1 ? [...authModes][0] : "mixed",
            cooled_down: states.filter((state) => state.state === "open" && state.cooldownUntil > now).length,
            successes: Math.round(states.reduce((sum, state) => sum + state.successEwma, 0)),
            failures: states.reduce((sum, state) => sum + state.consecutiveFailures, 0),
        };
    }
    endpointKey(endpoint) {
        return endpointIdentityKey(endpoint.host, endpoint.port, endpoint.tls, endpoint.credentials.username, endpoint.credentials.password);
    }
    stateFor(key) {
        const existing = this.health.get(key);
        if (existing)
            return existing;
        const created = { consecutiveFailures: 0, successEwma: 0.5, ewmaLatencyMs: null, cooldownUntil: 0, state: "closed", probeInFlight: false };
        this.health.set(key, created);
        return created;
    }
    // Pure predicate (audit fix E1): availability checks must never claim the
    // half-open probe slot. The slot is claimed only inside connect(), at the
    // moment the probe attempt actually starts (see claimProbe below).
    isAvailable(endpoint, now) {
        const state = this.stateFor(this.endpointKey(endpoint));
        if (state.state === "closed")
            return true;
        if (state.cooldownUntil > now)
            return false;
        if (state.probeInFlight)
            return false;
        return true;
    }
    healthScore(endpoint, now) {
        const state = this.stateFor(this.endpointKey(endpoint));
        if (state.state === "open" && state.cooldownUntil > now)
            return Number.NEGATIVE_INFINITY;
        const latencyPenalty = state.ewmaLatencyMs === null ? 0 : Math.min(state.ewmaLatencyMs / 250, 10);
        const score = (state.successEwma * 20) - ((1 - state.successEwma) * 20) - latencyPenalty - Math.min(state.consecutiveFailures * 4, 20);
        return Math.max(-100, Math.min(100, score));
    }
    markSuccess(key, latency) {
        const state = this.stateFor(key);
        state.successEwma = state.successEwma * 0.8 + 1 * 0.2;
        state.consecutiveFailures = 0;
        state.cooldownUntil = 0;
        state.state = "closed";
        state.probeInFlight = false;
        state.ewmaLatencyMs = state.ewmaLatencyMs === null ? latency : state.ewmaLatencyMs * 0.8 + latency * 0.2;
    }
    markFailure(key) {
        const state = this.stateFor(key);
        state.successEwma = state.successEwma * 0.8;
        state.consecutiveFailures += 1;
        state.probeInFlight = false;
        if (state.consecutiveFailures >= this.maxFailures) {
            state.state = "open";
            state.cooldownUntil = Date.now() + this.cooldownMs;
        }
        else {
            state.state = "closed";
        }
    }
    async connect(destination, signal) {
        assertValidDestinationPort(destination.port);
        let lastError = null;
        const now = Date.now();
        // Audit fix E5: the failover loop gets an aggregate decision budget in
        // addition to the caller's AbortSignal and the per-attempt timeouts, so a
        // black-holed candidate set cannot burn candidates × (connect + handshake)
        // of wall time when a caller supplies a deadline-less signal.
        const deadlineAt = now + this.decisionTimeoutMs;
        const closed = this.endpoints.filter((endpoint) => this.stateFor(this.endpointKey(endpoint)).state === "closed");
        const halfOpenCandidates = this.endpoints
            .filter((endpoint) => this.stateFor(this.endpointKey(endpoint)).state === "open")
            .filter((endpoint) => this.stateFor(this.endpointKey(endpoint)).cooldownUntil <= now && !this.stateFor(this.endpointKey(endpoint)).probeInFlight)
            .sort((a, b) => this.stateFor(this.endpointKey(a)).cooldownUntil - this.stateFor(this.endpointKey(b)).cooldownUntil);
        const probeCandidate = halfOpenCandidates[0] ?? null;
        const candidates = [...closed, ...(probeCandidate ? [probeCandidate] : [])].filter((endpoint) => this.isAvailable(endpoint, now));
        const ordered = [...candidates].sort((a, b) => this.healthScore(b, now) - this.healthScore(a, now));
        // Audit fixes E1/E2: the single half-open probe slot is claimed only when
        // the probe attempt actually starts (claimProbe, synchronously inside the
        // loop, so concurrent connect() calls cannot both claim it) and is
        // released on EVERY exit path via the finally below — early success of a
        // healthy endpoint, the INVALID_DESTINATION / CONFIGURATION_ERROR
        // fast-throw, aborts, and deadline exhaustion included. Previously the
        // slot was claimed during candidate filtering and leaked whenever the
        // probed endpoint was not the one attempted, permanently draining the pool.
        let probeClaimed = false;
        let probeAborted = false;
        const releaseProbe = () => {
            if (!probeCandidate || !probeClaimed)
                return;
            probeClaimed = false;
            const state = this.stateFor(this.endpointKey(probeCandidate));
            state.probeInFlight = false;
            if (probeAborted) {
                // Preserve the pre-existing abort semantics: an aborted probe returns
                // the endpoint to open state, immediately eligible for a later probe.
                state.state = "open";
                state.cooldownUntil = 0;
            }
        };
        try {
            for (const endpoint of ordered) {
                if (signal.aborted)
                    throw signal.reason;
                if (Date.now() >= deadlineAt)
                    break;
                const key = this.endpointKey(endpoint);
                const isProbe = probeCandidate !== null && endpoint === probeCandidate;
                if (isProbe) {
                    // Claim the slot only now. There is no await between the re-check
                    // and the claim, so the single-probe guarantee holds under
                    // concurrency; if another connect() claimed it first, skip.
                    const state = this.stateFor(key);
                    if (state.probeInFlight || state.state !== "open" || state.cooldownUntil > Date.now())
                        continue;
                    state.probeInFlight = true;
                    probeClaimed = true;
                }
                const started = performance.now();
                let socket = null;
                try {
                    socket = await this.connector.connect({ host: endpoint.host, port: endpoint.port }, signal, (0, lifecycle_1.remainingMs)(deadlineAt, this.connectTimeoutMs), endpoint.tls ? "on" : "off");
                    const readable = await (0, lifecycle_1.withTimeout)((handshakeSignal) => performSocks5Connect(socket, destination, endpoint.credentials, handshakeSignal), (0, lifecycle_1.remainingMs)(deadlineAt, this.handshakeTimeoutMs), signal, "ProxyIP SOCKS5 handshake timeout");
                    this.markSuccess(key, performance.now() - started);
                    if (isProbe)
                        probeClaimed = false; // markSuccess already cleared the flag
                    return {
                        type: "proxyip",
                        readable,
                        writable: socket.writable,
                        // Audit fix E7: full teardown — cancelling the tunnel readable
                        // cancels the socket's readable side as well (see
                        // BufferedSocketReader), instead of only closing the writable
                        // side of an allowHalfOpen socket.
                        close: async () => {
                            await readable.cancel().catch(() => { });
                            await socket.close().catch(() => { });
                        },
                    };
                }
                catch (error) {
                    await socket?.close().catch(() => { });
                    lastError = error;
                    if (signal.aborted) {
                        probeAborted = isProbe;
                        throw signal.reason;
                    }
                    if (error instanceof errors_1.CoreError && (error.code === "INVALID_DESTINATION" || error.code === "CONFIGURATION_ERROR"))
                        throw error;
                    this.markFailure(key);
                    if (isProbe)
                        probeClaimed = false; // markFailure already cleared the flag
                }
            }
        }
        finally {
            releaseProbe();
        }
        if (lastError instanceof errors_1.CoreError && lastError.code === "AUTH_FAILED")
            throw lastError;
        throw new errors_1.CoreError("EGRESS_FAILED", "All available ProxyIP SOCKS5 endpoints failed or are cooling down", true, { cause: lastError });
    }
}
exports.ProxyIpEgress = ProxyIpEgress;
async function writeWithAbort(writer, data, signal) {
    const pending = writer.write(data);
    await raceWithAbort(pending, signal);
}
async function performSocks5Connect(socket, destination, credentials, signal) {
    const reader = new BufferedSocketReader(socket.readable);
    const writer = socket.writable.getWriter();
    try {
        const wantsAuth = Boolean(credentials.username || credentials.password);
        await writeWithAbort(writer, new Uint8Array([
            SOCKS_VERSION,
            wantsAuth ? 2 : 1,
            METHOD_NO_AUTH,
            ...(wantsAuth ? [METHOD_USERPASS] : []),
        ]), signal);
        const greeting = await reader.readExact(2, signal);
        if (greeting[0] !== SOCKS_VERSION) {
            throw new errors_1.CoreError("EGRESS_FAILED", "ProxyIP returned an invalid SOCKS5 version", true);
        }
        const method = greeting[1];
        if (method === METHOD_NO_ACCEPTABLE) {
            throw new errors_1.CoreError("AUTH_FAILED", "ProxyIP rejected all offered SOCKS5 authentication methods");
        }
        if (method === METHOD_USERPASS) {
            if (!wantsAuth)
                throw new errors_1.CoreError("AUTH_FAILED", "ProxyIP requires username/password authentication");
            const username = (0, binary_1.encodeUtf8)(credentials.username ?? "");
            const password = (0, binary_1.encodeUtf8)(credentials.password ?? "");
            if (username.byteLength === 0 || username.byteLength > 255 || password.byteLength === 0 || password.byteLength > 255) {
                throw new errors_1.CoreError("CONFIGURATION_ERROR", "ProxyIP credentials must be 1..255 UTF-8 bytes");
            }
            await writeWithAbort(writer, new Uint8Array([
                0x01,
                username.byteLength,
                ...username,
                password.byteLength,
                ...password,
            ]), signal);
            const authResponse = await reader.readExact(2, signal);
            if (authResponse[0] !== 0x01 || authResponse[1] !== 0x00) {
                throw new errors_1.CoreError("AUTH_FAILED", "ProxyIP username/password authentication failed");
            }
        }
        else if (method !== METHOD_NO_AUTH) {
            throw new errors_1.CoreError("EGRESS_FAILED", `Unsupported SOCKS5 authentication method: ${method}`, true);
        }
        await writeWithAbort(writer, buildConnectRequest(destination), signal);
        const response = await reader.readExact(4, signal);
        if (response[0] !== SOCKS_VERSION || response[2] !== 0x00) {
            throw new errors_1.CoreError("EGRESS_FAILED", "ProxyIP returned an invalid SOCKS5 response", true);
        }
        if (response[1] !== 0x00)
            throw socksReplyError(response[1]);
        await consumeSocks5Address(reader, response[3], signal);
        return reader.toReadableStream();
    }
    catch (error) {
        await reader.cancel(error);
        throw error;
    }
    finally {
        writer.releaseLock();
    }
}
function buildConnectRequest(destination) {
    assertValidDestinationPort(destination.port);
    const address = encodeSocksAddress(destination.host);
    const out = new Uint8Array(5 + address.byteLength);
    out.set([SOCKS_VERSION, CMD_CONNECT, 0x00, address[0]], 0);
    out.set(address.subarray(1), 4);
    const portOffset = 3 + address.byteLength;
    out[portOffset] = (destination.port >>> 8) & 0xff;
    out[portOffset + 1] = destination.port & 0xff;
    return out;
}
function encodeSocksAddress(host) {
    const ipv4 = parseIpv4(host);
    if (ipv4)
        return new Uint8Array([ATYP_IPV4, ...ipv4]);
    const ipv6 = parseIpv6(host);
    if (ipv6)
        return new Uint8Array([ATYP_IPV6, ...ipv6]);
    const bytes = (0, binary_1.encodeUtf8)(host);
    if (bytes.byteLength === 0 || bytes.byteLength > 255) {
        throw new errors_1.CoreError("INVALID_DESTINATION", "ProxyIP destination hostname must be 1..255 UTF-8 bytes");
    }
    return new Uint8Array([ATYP_DOMAIN, bytes.byteLength, ...bytes]);
}
function parseIpv4(value) {
    const parts = value.trim().split(".");
    if (parts.length !== 4 || parts.some((part) => !/^\d{1,3}$/.test(part)))
        return null;
    const numbers = parts.map(Number);
    return numbers.every((x) => x >= 0 && x <= 255) ? numbers : null;
}
function parseIpv6(value) {
    let input = value.trim();
    if (input.startsWith("[") && input.endsWith("]"))
        input = input.slice(1, -1);
    if (!input.includes(":"))
        return null;
    const pieces = input.split("::");
    if (pieces.length > 2)
        return null;
    // Audit fix E9: a dotted-quad is only legal as the final 32 bits of the
    // whole address (RFC 4291 §2.2). allowIpv4Tail is therefore true only for
    // the side that ends the address, and only for its last token — matching
    // the position check nat64.ts already performs.
    const parseSide = (side, allowIpv4Tail) => {
        if (!side)
            return [];
        const tokens = side.split(":");
        const out = [];
        for (const [index, token] of tokens.entries()) {
            if (token.includes(".")) {
                if (!allowIpv4Tail || index !== tokens.length - 1)
                    return null;
                const ipv4 = parseIpv4(token);
                if (!ipv4)
                    return null;
                out.push((ipv4[0] << 8) | ipv4[1], (ipv4[2] << 8) | ipv4[3]);
                continue;
            }
            if (!/^[0-9a-fA-F]{1,4}$/.test(token))
                return null;
            out.push(Number.parseInt(token, 16));
        }
        return out;
    };
    const left = parseSide(pieces[0], pieces.length === 1);
    const right = parseSide(pieces[1] ?? "", pieces.length === 2);
    if (!left || !right)
        return null;
    let groups;
    if (pieces.length === 2) {
        const missing = 8 - (left.length + right.length);
        if (missing < 1)
            return null;
        groups = [...left, ...new Array(missing).fill(0), ...right];
    }
    else {
        if (left.length !== 8)
            return null;
        groups = left;
    }
    if (groups.length !== 8)
        return null;
    const bytes = [];
    for (const group of groups)
        bytes.push((group >>> 8) & 0xff, group & 0xff);
    return bytes;
}
function consumeSocks5Address(reader, atyp, signal) {
    switch (atyp) {
        case ATYP_IPV4: return reader.readExact(4, signal).then(() => reader.readExact(2, signal)).then(() => undefined);
        case ATYP_DOMAIN: return reader.readExact(1, signal).then((length) => reader.readExact(length[0], signal)).then(() => reader.readExact(2, signal)).then(() => undefined);
        case ATYP_IPV6: return reader.readExact(16, signal).then(() => reader.readExact(2, signal)).then(() => undefined);
        default: throw new errors_1.CoreError("EGRESS_FAILED", `ProxyIP returned unsupported SOCKS5 address type: ${atyp}`, true);
    }
}
function socksReplyError(reply) {
    const messages = {
        0x01: "General SOCKS server failure",
        0x02: "Connection not allowed by ruleset",
        0x03: "Network unreachable",
        0x04: "Host unreachable",
        0x05: "Connection refused",
        0x06: "TTL expired",
        0x07: "Command not supported",
        0x08: "Address type not supported",
    };
    const retryable = reply === 0x01 || reply === 0x03 || reply === 0x04 || reply === 0x06;
    return new errors_1.CoreError("EGRESS_FAILED", `SOCKS5 CONNECT failed: ${messages[reply] ?? `reply ${reply}`}`, retryable);
}
class BufferedSocketReader {
    reader;
    buffer = null;
    offset = 0;
    eof = false;
    constructor(stream) {
        this.reader = stream.getReader();
    }
    async readExact(length, signal) {
        if (length === 0)
            return new Uint8Array(0);
        const out = new Uint8Array(length);
        let written = 0;
        while (written < length) {
            const chunk = await this.next(signal);
            const take = Math.min(chunk.byteLength, length - written);
            out.set(chunk.subarray(0, take), written);
            written += take;
            if (take < chunk.byteLength) {
                this.buffer = chunk;
                this.offset = take;
            }
        }
        return out;
    }
    toReadableStream() {
        return new ReadableStream({
            pull: async (controller) => {
                if (this.buffer && this.offset < this.buffer.byteLength) {
                    const remaining = this.buffer.subarray(this.offset);
                    this.buffer = null;
                    this.offset = 0;
                    controller.enqueue(remaining);
                    return;
                }
                if (this.eof) {
                    controller.close();
                    return;
                }
                const result = await this.reader.read();
                if (result.done) {
                    this.eof = true;
                    this.reader.releaseLock();
                    controller.close();
                    return;
                }
                if (result.value.byteLength)
                    controller.enqueue(result.value);
            },
            cancel: async (reason) => {
                this.eof = true;
                await this.reader.cancel(reason).catch(() => { });
                this.reader.releaseLock();
            },
        });
    }
    async cancel(reason) {
        if (this.eof)
            return;
        this.eof = true;
        await this.reader.cancel(reason).catch(() => { });
        try {
            this.reader.releaseLock();
        }
        catch { /* already released */ }
    }
    async next(signal) {
        if (this.buffer && this.offset < this.buffer.byteLength) {
            const remaining = this.buffer.subarray(this.offset);
            this.buffer = null;
            this.offset = 0;
            return remaining;
        }
        if (this.eof)
            throw new errors_1.CoreError("UPSTREAM_CLOSED", "SOCKS5 proxy closed during handshake", true);
        if (signal.aborted)
            throw signal.reason;
        const readPromise = this.reader.read();
        return await raceWithAbort(readPromise, signal).then((result) => {
            if (result.done) {
                this.eof = true;
                throw new errors_1.CoreError("UPSTREAM_CLOSED", "SOCKS5 proxy closed during handshake", true);
            }
            return result.value;
        });
    }
}
async function raceWithAbort(promise, signal) {
    if (signal.aborted)
        throw signal.reason;
    let onAbort;
    try {
        const abort = new Promise((_, reject) => {
            onAbort = () => reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
            signal.addEventListener("abort", onAbort, { once: true });
            if (signal.aborted)
                onAbort();
        });
        return await Promise.race([promise, abort]);
    }
    finally {
        if (onAbort)
            signal.removeEventListener("abort", onAbort);
    }
}
function parseProxyIpEndpoints(env) {
    const structuredRaw = env.PROXY_IP_ENDPOINTS_JSON?.trim();
    const result = [];
    const seen = new Set();
    const add = (host, port, tls, credentials) => {
        const normalizedHost = host.trim();
        (0, binary_1.validatePort)(port);
        if (!normalizedHost)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "ProxyIP host is empty");
        if ((credentials.username && !credentials.password) || (!credentials.username && credentials.password))
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "ProxyIP endpoint username/password must be configured together");
        const key = endpointIdentityKey(normalizedHost, port, tls, credentials.username, credentials.password);
        if (seen.has(key))
            return;
        seen.add(key);
        result.push({ host: normalizedHost, port, tls, credentials });
    };
    if (structuredRaw) {
        let parsed;
        try {
            parsed = JSON.parse(structuredRaw);
        }
        catch (error) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "PROXY_IP_ENDPOINTS_JSON must be valid JSON", false, { cause: error });
        }
        if (!Array.isArray(parsed))
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "PROXY_IP_ENDPOINTS_JSON must be a JSON array");
        if (parsed.length > limits_1.CORE_LIMITS.maxProxyIpEndpoints)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Too many ProxyIP endpoints; maximum is ${limits_1.CORE_LIMITS.maxProxyIpEndpoints}`);
        for (const entry of parsed) {
            if (!entry || typeof entry !== "object")
                throw new errors_1.CoreError("CONFIGURATION_ERROR", "Each ProxyIP endpoint must be an object");
            const item = entry;
            const host = typeof item.host === "string" ? item.host : "";
            const port = item.port === undefined ? limits_1.CORE_LIMITS.defaultProxyIpPort : Number(item.port);
            const tls = item.tls === undefined ? parseBoolean(env.PROXY_IP_TLS) : Boolean(item.tls);
            const username = typeof item.username === "string" ? item.username.trim() : "";
            const password = typeof item.password === "string" ? item.password : "";
            add(host, port, tls, username ? { username, password } : {});
        }
    }
    else {
        const rawValues = [env.PROXY_IP ?? "", ...(env.PROXY_IPS ?? "").split(/[,\r\n]+/)];
        const tls = parseBoolean(env.PROXY_IP_TLS);
        const username = env.PROXY_IP_USERNAME?.trim();
        const password = env.PROXY_IP_PASSWORD;
        if ((username && !password) || (!username && password))
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "PROXY_IP_USERNAME and PROXY_IP_PASSWORD must be configured together");
        const credentials = username ? { username, password } : {};
        for (const raw of rawValues) {
            const value = raw.trim();
            if (!value)
                continue;
            const parsed = (0, binary_1.parseHostPort)(value, limits_1.CORE_LIMITS.defaultProxyIpPort);
            add(parsed.host, parsed.port ?? limits_1.CORE_LIMITS.defaultProxyIpPort, tls, credentials);
            if (result.length > limits_1.CORE_LIMITS.maxProxyIpEndpoints)
                throw new errors_1.CoreError("CONFIGURATION_ERROR", `Too many ProxyIP endpoints; maximum is ${limits_1.CORE_LIMITS.maxProxyIpEndpoints}`);
        }
    }
    return result;
}
function parseBoolean(value) {
    if (!value)
        return false;
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized))
        return true;
    if (["0", "false", "no", "off"].includes(normalized))
        return false;
    throw new errors_1.CoreError("CONFIGURATION_ERROR", "PROXY_IP_TLS must be a boolean");
}
function hasProxyIp(env) {
    return Boolean(env.PROXY_IP?.trim() || env.PROXY_IPS?.trim() || env.PROXY_IP_ENDPOINTS_JSON?.trim());
}

};

__m["egress/defaults"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_NAT64_PREFIXES = void 0;
// Built-in NAT64 defaults (audit fix E3).
//
// Only the RFC 6052 §2 well-known prefix (WKP) 64:ff9b::/96 is bundled. The WKP is
// served by the operator's own network / public DNS64+NAT64 providers that choose to
// support it, so no specific third-party translator is silently on-path.
//
// MIGRATION NOTE: previous releases bundled three network-specific prefixes of
// third-party public NAT64 gateways (2a02:898:146:64::/96, 2602:fc59:b0:64::/96,
// 2602:fc59:11:64::/96) as silent defaults. They were removed because a NAT64
// translator is a trusted on-path element (RFC 6146) and must be an explicit
// operator choice. Deployments that relied on those gateways must now set
// NAT64_PREFIXES explicitly to keep using them; explicit NAT64_PREFIXES
// configurations are unaffected by this change.
exports.DEFAULT_NAT64_PREFIXES = [
    "64:ff9b::/96",
];

};

__m["egress/relay"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RawTcpRelayEgress = void 0;
exports.parseRelayEndpoints = parseRelayEndpoints;
exports.hasRelay = hasRelay;
const errors_1 = __r("../core/errors");
const limits_1 = __r("../core/limits");
const lifecycle_1 = __r("../util/lifecycle");
const direct_1 = __r("./direct");
function splitList(raw) {
    return (raw ?? "").split(/[\r\n,]+/).map((value) => value.trim()).filter(Boolean);
}
function parseEndpoint(value) {
    const raw = value.trim();
    if (!raw)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Empty PROXY_RELAY_ENDPOINTS entry");
    if (raw.startsWith("[")) {
        const end = raw.indexOf("]");
        if (end < 0)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid relay endpoint: ${value}`);
        const host = raw.slice(1, end).trim();
        const rest = raw.slice(end + 1);
        if (!rest)
            return { host };
        if (!/^:\d+$/.test(rest))
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid relay endpoint port: ${value}`);
        return { host, port: Number(rest.slice(1)) };
    }
    const colons = (raw.match(/:/g) ?? []).length;
    if (colons > 1)
        return { host: raw };
    const at = raw.lastIndexOf(":");
    if (at < 0)
        return { host: raw };
    if (!/^\d+$/.test(raw.slice(at + 1)))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid relay endpoint port: ${value}`);
    return { host: raw.slice(0, at), port: Number(raw.slice(at + 1)) };
}
function validateEndpoint(endpoint, raw) {
    const host = endpoint.host.trim();
    if (!host || host.length > 253 || /[\u0000-\u0020\u007f\\/?#]/.test(host))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid relay host: ${raw}`);
    if (endpoint.port !== undefined && (!Number.isInteger(endpoint.port) || endpoint.port < 1 || endpoint.port > 65535)) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid relay port: ${raw}`);
    }
    // Audit fix E4: the raw relay protocol defined here cannot transmit the
    // requested destination to the relay (no SOCKS, no PROXY protocol, no
    // header) — the relay is a transparent byte forwarder bound to ONE fixed
    // service port. A port-less endpoint previously dialed the
    // client-requested destination port on the relay host, silently
    // misrouting non-transparent relays and exposing every port of the relay
    // host to proxied clients. Fail closed instead: an explicit endpoint port
    // is now required. COMPATIBILITY: configurations with port-less
    // PROXY_RELAY_ENDPOINTS entries that relied on destination-port dialing
    // stop validating until an explicit port is added.
    if (endpoint.port === undefined) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Relay endpoint must include an explicit port (the raw relay cannot signal the destination, so the endpoint port is the fixed service port): ${raw}`);
    }
    return { host, port: endpoint.port };
}
function parseRelayEndpoints(raw) {
    const values = splitList(raw);
    const seen = new Set();
    const out = [];
    for (const value of values) {
        const endpoint = validateEndpoint(parseEndpoint(value), value);
        const key = `${endpoint.host.toLowerCase()}\u0000${endpoint.port ?? ""}`;
        if (!seen.has(key)) {
            seen.add(key);
            out.push(endpoint);
        }
    }
    return out;
}
function hasRelay(env) {
    return parseRelayEndpoints(env.PROXY_RELAY_ENDPOINTS).length > 0;
}
// Raw TCP relay semantics (audit fix E4, explicit): this egress dials the
// operator-configured relay endpoint and hands over the raw byte stream.
// The requested destination is NOT transmitted to the relay in any form, so
// the relay must be a transparent forwarder for the single fixed service
// identified by the endpoint's explicit port; per-destination routing is
// impossible over this protocol and is not attempted.
class RawTcpRelayEgress {
    endpoints;
    connectTimeoutMs;
    maxFailures;
    cooldownMs;
    type = "relay";
    health = new Map();
    cursor = 0;
    decisionTimeoutMs;
    constructor(endpoints, connectTimeoutMs, maxFailures = 3, cooldownMs = 15_000, decisionTimeoutMs = limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs) {
        this.endpoints = endpoints;
        this.connectTimeoutMs = connectTimeoutMs;
        this.maxFailures = maxFailures;
        this.cooldownMs = cooldownMs;
        if (!endpoints.length)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "Raw TCP relay requires at least one endpoint");
        // Defense in depth for direct construction (parseRelayEndpoints already
        // enforces this): a port-less endpoint has no well-defined target.
        if (endpoints.some((endpoint) => endpoint.port === undefined)) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "Raw TCP relay endpoints must include an explicit port (the raw relay cannot signal the destination)");
        }
        this.decisionTimeoutMs = Number.isFinite(decisionTimeoutMs) && decisionTimeoutMs > 0 ? decisionTimeoutMs : limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs;
    }
    async connect(destination, signal) {
        // Audit fix E6: validate the client-influenced destination port in this
        // layer too, even though dialing now always uses the endpoint port.
        if (!Number.isInteger(destination.port) || destination.port < 1 || destination.port > 65535) {
            throw new errors_1.CoreError("INVALID_DESTINATION", `Destination port must be an integer from 1 to 65535, got ${destination.port}`);
        }
        let lastError;
        const ordered = this.rotatedEndpoints();
        // Audit fix E5: aggregate decision budget across rotated attempts.
        const deadlineAt = Date.now() + this.decisionTimeoutMs;
        for (const endpoint of ordered) {
            if (signal.aborted)
                throw signal.reason;
            if (Date.now() >= deadlineAt)
                break;
            // Audit fix E10: the breaker key is the actual dial target. Endpoint
            // ports are always explicit now (E4), so the key can no longer
            // conflate different destination ports under a ":0" bucket; the
            // cooldown timestamp is also sampled fresh per attempt instead of
            // once before a potentially long loop.
            const key = `${endpoint.host}:${endpoint.port}`;
            const health = this.health.get(key);
            if (health?.openUntil && health.openUntil > Date.now())
                continue;
            try {
                const socket = await (0, direct_1.connectTcp)({ host: endpoint.host, port: endpoint.port }, signal, (0, lifecycle_1.remainingMs)(deadlineAt, this.connectTimeoutMs));
                this.resetHealth(key);
                return {
                    type: "relay",
                    readable: socket.readable,
                    writable: socket.writable,
                    // Audit fix E7: full teardown — also cancel the readable side of
                    // the allowHalfOpen socket instead of only closing writable.
                    close: async () => {
                        await socket.readable.cancel().catch(() => { });
                        await socket.close().catch(() => { });
                    },
                };
            }
            catch (error) {
                lastError = error;
                if (signal.aborted)
                    throw signal.reason;
                this.recordFailure(key);
            }
        }
        throw new errors_1.CoreError("EGRESS_FAILED", `Raw TCP relay connection failed for ${destination.host}:${destination.port}`, true, { cause: lastError });
    }
    diagnostics() {
        return {
            mode: "raw-tcp-relay",
            endpoints: this.endpoints.length,
            open: [...this.health.values()].filter((item) => item.openUntil > Date.now()).length,
            max_failures: this.maxFailures,
            cooldown_ms: this.cooldownMs,
        };
    }
    rotatedEndpoints() {
        const out = [...this.endpoints];
        if (!out.length)
            return out;
        const start = this.cursor++ % out.length;
        return [...out.slice(start), ...out.slice(0, start)].slice(0, 64);
    }
    recordFailure(key) {
        const current = this.health.get(key) ?? { failures: 0, openUntil: 0 };
        const failures = current.failures + 1;
        this.health.set(key, { failures, openUntil: failures >= this.maxFailures ? Date.now() + this.cooldownMs : 0 });
    }
    resetHealth(key) { this.health.delete(key); }
}
exports.RawTcpRelayEgress = RawTcpRelayEgress;

};

__m["protocol/trojan/parser"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.UDP_ASSOCIATE = exports.CONNECT = exports.TrojanHandler = void 0;
const types_1 = __r("../../core/types");
const errors_1 = __r("../../core/errors");
const binary_1 = __r("../../util/binary");
const hostname_1 = __r("../../util/hostname");
const CONNECT = 0x01;
exports.CONNECT = CONNECT;
const UDP_ASSOCIATE = 0x03;
exports.UDP_ASSOCIATE = UDP_ASSOCIATE;
const IPV4 = 0x01;
const DOMAIN = 0x03;
const IPV6 = 0x04;
const CRLF = new Uint8Array([0x0d, 0x0a]);
function parsePort(bytes) {
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint16(0, false);
}
function parseIpv6(bytes) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const groups = [];
    for (let i = 0; i < 8; i++)
        groups.push(view.getUint16(i * 2, false).toString(16));
    return groups.join(":");
}
function validateDestination(destination) {
    const host = destination.host;
    // Structural check, not a character class: a colon-bearing host is valid
    // only if it round-trips through the codebase's IPv6-literal validator
    // (toAsciiHostname returns the normalized literal, or "" when malformed).
    const validIp = host.includes(":") && (0, hostname_1.toAsciiHostname)(host) === host;
    if (!host || host.length > 253 || /[\u0000-\u001f\u007f\\/?#@]/.test(host) || (!validIp && !(0, hostname_1.isAsciiHostname)(host))) {
        throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid destination host");
    }
    if (!Number.isInteger(destination.port) || destination.port < 1 || destination.port > 65535) {
        throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid destination port");
    }
}
class TrojanHandler {
    credentials;
    maxDomainBytes;
    maxInitialPayloadBytes;
    protocol = "trojan";
    constructor(credentials, maxDomainBytes = 253, maxInitialPayloadBytes = types_1.CORE_LIMITS.maxInitialPayloadBytes) {
        this.credentials = credentials;
        this.maxDomainBytes = maxDomainBytes;
        this.maxInitialPayloadBytes = maxInitialPayloadBytes;
    }
    async parse(reader, _context, signal) {
        const credentialBytes = await reader.readExact(56, signal);
        // Decode as UTF-8 with fatal errors, mirroring the domain decoding below:
        // the WHATWG "ascii" label actually selects the windows-1252 decoder,
        // which never throws and would silently map bytes 0x80-0xFF (B6). Any
        // non-ASCII byte now fails here or at the hex gate, both as AUTH_FAILED.
        let credentialText;
        try {
            credentialText = new TextDecoder("utf-8", { fatal: true }).decode(credentialBytes);
        }
        catch (error) {
            throw new errors_1.CoreError("AUTH_FAILED", "Invalid Trojan authentication digest", false, { cause: error });
        }
        if (!/^[0-9a-fA-F]{56}$/.test(credentialText)) {
            throw new errors_1.CoreError("AUTH_FAILED", "Invalid Trojan authentication digest");
        }
        const separator = await reader.readExact(2, signal);
        // Uniform AUTH_FAILED for every failure before authentication completes,
        // so an unauthenticated peer cannot tell which stage rejected it (B7).
        if (!isCrlf(separator))
            throw new errors_1.CoreError("AUTH_FAILED", "Invalid Trojan authentication line");
        // CredentialProvider contract (B1): for protocol "trojan" the credential
        // is the ASCII bytes of the *canonical lowercase* hex SHA-224 digest —
        // the same text form CredentialRecord.credential documents. The parser
        // normalizes case here so providers never have to: wire digests in any
        // case authenticate identically, and providers may byte-compare against
        // the canonical form. (normalizeCredentialHash below returns the 28 *raw
        // digest bytes* for storage-side comparisons; it is a different
        // representation and is never what authenticate() receives.)
        const canonicalCredential = new TextEncoder().encode(credentialText.toLowerCase());
        const principal = await this.credentials.authenticate("trojan", canonicalCredential, signal);
        if (!principal)
            throw new errors_1.CoreError("AUTH_FAILED", "Trojan authentication failed");
        const command = (await reader.readExact(1, signal))[0];
        if (command !== CONNECT) {
            if (command === UDP_ASSOCIATE)
                throw new errors_1.CoreError("BAD_REQUEST", "Trojan UDP is not supported by this Worker core");
            throw new errors_1.CoreError("BAD_REQUEST", `Unsupported Trojan command: ${command}`);
        }
        const addressType = (await reader.readExact(1, signal))[0];
        let host;
        switch (addressType) {
            case IPV4: {
                const bytes = await reader.readExact(4, signal);
                host = `${bytes[0]}.${bytes[1]}.${bytes[2]}.${bytes[3]}`;
                break;
            }
            case DOMAIN: {
                const length = (await reader.readExact(1, signal))[0];
                if (length === 0 || length > this.maxDomainBytes)
                    throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid Trojan domain length");
                try {
                    host = (0, hostname_1.toAsciiHostname)(new TextDecoder("utf-8", { fatal: true }).decode(await reader.readExact(length, signal)));
                }
                catch (error) {
                    throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid Trojan domain encoding", false, { cause: error });
                }
                break;
            }
            case IPV6:
                host = parseIpv6(await reader.readExact(16, signal));
                break;
            default:
                throw new errors_1.CoreError("BAD_REQUEST", `Unsupported Trojan address type: ${addressType}`);
        }
        const port = parsePort(await reader.readExact(2, signal));
        if (!isCrlf(await reader.readExact(2, signal)))
            throw new errors_1.CoreError("BAD_REQUEST", "Invalid Trojan request terminator");
        const destination = { host, port };
        validateDestination(destination);
        const initialPayload = reader.readAvailable(this.maxInitialPayloadBytes) ?? new Uint8Array(0);
        return { protocol: "trojan", principal, destination, initialPayload };
    }
    /**
     * Converts a stored 56-character hex SHA-224 digest (any case) to the 28
     * raw digest bytes. This is the storage-side representation only — the
     * credential passed to CredentialProvider.authenticate is the ASCII bytes
     * of the canonical lowercase hex text (see parse()).
     */
    static normalizeCredentialHash(value) {
        const normalized = value.trim().toLowerCase();
        if (!/^[0-9a-f]{56}$/.test(normalized))
            throw new Error("Trojan credential must be a 56-character SHA-224 hex digest");
        return (0, binary_1.hexToBytes)(normalized);
    }
}
exports.TrojanHandler = TrojanHandler;
function isCrlf(bytes) {
    return bytes.byteLength === 2 && bytes[0] === CRLF[0] && bytes[1] === CRLF[1];
}

};

__m["core/types"] = function(module, exports, __r) {
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Compatibility facade retained for downstream imports.
 * Kernel implementation files should import contracts/limits directly.
 */
__exportStar(__r("./contracts"), exports);
__exportStar(__r("./limits"), exports);

};

__m["core/contracts"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });

};

__m["protocol/vless/parser"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MUX_COMMAND = exports.UDP_COMMAND = exports.TCP_COMMAND = exports.VlessHandler = void 0;
const errors_1 = __r("../../core/errors");
const types_1 = __r("../../core/types");
const binary_1 = __r("../../util/binary");
const hostname_1 = __r("../../util/hostname");
const TCP_COMMAND = 0x01;
exports.TCP_COMMAND = TCP_COMMAND;
const UDP_COMMAND = 0x02;
exports.UDP_COMMAND = UDP_COMMAND;
const MUX_COMMAND = 0x03;
exports.MUX_COMMAND = MUX_COMMAND;
const IPV4 = 0x01;
const DOMAIN = 0x02;
const IPV6 = 0x03;
function parsePort(bytes) {
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint16(0, false);
}
function parseIpv6(bytes) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const groups = [];
    for (let i = 0; i < 8; i++)
        groups.push(view.getUint16(i * 2, false).toString(16));
    return groups.join(":");
}
function validateDestination(destination) {
    const host = destination.host;
    // Structural check, not a character class: a colon-bearing host is valid
    // only if it round-trips through the codebase's IPv6-literal validator
    // (toAsciiHostname returns the normalized literal, or "" when malformed).
    const validIp = host.includes(":") && (0, hostname_1.toAsciiHostname)(host) === host;
    if (!host || host.length > 253 || /[\u0000-\u001f\u007f\\/?#@]/.test(host) || (!validIp && !(0, hostname_1.isAsciiHostname)(host))) {
        throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid destination host");
    }
    if (!Number.isInteger(destination.port) || destination.port < 1 || destination.port > 65535) {
        throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid destination port");
    }
}
/**
 * Decodes the VLESS addons field and returns the requested flow ("" when the
 * addons carry no flow). Addons are a protobuf-encoded `Addons` message whose
 * field 1 is the flow string (e.g. "xtls-rprx-vision"); unknown fields are
 * skipped per protobuf rules so future non-flow addons stay readable. Any
 * malformed encoding throws BAD_REQUEST — addons are never silently
 * discarded (B4).
 */
function decodeVlessFlow(addons) {
    let offset = 0;
    let flow = "";
    const invalid = () => new errors_1.CoreError("BAD_REQUEST", "Invalid VLESS addons encoding");
    const readVarint = () => {
        let result = 0;
        let shift = 0;
        while (offset < addons.length) {
            const byte = addons[offset++];
            result += (byte & 0x7f) * 2 ** shift;
            if (!(byte & 0x80)) {
                if (!Number.isSafeInteger(result))
                    throw invalid();
                return result;
            }
            shift += 7;
            if (shift > 28)
                throw invalid();
        }
        throw invalid();
    };
    const skip = (count) => {
        if (count < 0 || offset + count > addons.length)
            throw invalid();
        offset += count;
    };
    while (offset < addons.length) {
        const tag = readVarint();
        const field = Math.floor(tag / 8);
        const wireType = tag % 8;
        if (field === 0)
            throw invalid();
        switch (wireType) {
            case 0:
                readVarint();
                break;
            case 1:
                skip(8);
                break;
            case 2: {
                const length = readVarint();
                if (offset + length > addons.length)
                    throw invalid();
                if (field === 1) {
                    try {
                        flow = new TextDecoder("utf-8", { fatal: true }).decode(addons.subarray(offset, offset + length));
                    }
                    catch (error) {
                        throw new errors_1.CoreError("BAD_REQUEST", "Invalid VLESS flow encoding", false, { cause: error });
                    }
                }
                offset += length;
                break;
            }
            case 5:
                skip(4);
                break;
            default:
                throw invalid();
        }
    }
    return flow;
}
class VlessHandler {
    credentials;
    maxAddonBytes;
    maxDomainBytes;
    maxInitialPayloadBytes;
    protocol = "vless";
    constructor(credentials, maxAddonBytes = 255, maxDomainBytes = 253, maxInitialPayloadBytes = types_1.CORE_LIMITS.maxInitialPayloadBytes) {
        this.credentials = credentials;
        this.maxAddonBytes = maxAddonBytes;
        this.maxDomainBytes = maxDomainBytes;
        this.maxInitialPayloadBytes = maxInitialPayloadBytes;
    }
    async parse(reader, _context, signal) {
        const version = (await reader.readExact(1, signal))[0];
        if (version !== 0x00)
            throw new errors_1.CoreError("BAD_REQUEST", `Unsupported VLESS version: ${version}`);
        const credentialBytes = await reader.readExact(16, signal);
        const principal = await this.credentials.authenticate("vless", credentialBytes, signal);
        if (!principal)
            throw new errors_1.CoreError("AUTH_FAILED", "VLESS authentication failed");
        const addonLength = (await reader.readExact(1, signal))[0];
        // Note: the length is a single byte (0-255), so at the default
        // maxAddonBytes=255 this limit check can only fire when a caller
        // configures a smaller bound (B12).
        if (addonLength > this.maxAddonBytes)
            throw new errors_1.CoreError("BAD_REQUEST", "VLESS addons exceed configured limit");
        if (addonLength > 0) {
            const addons = await reader.readExact(addonLength, signal);
            // This core terminates a plain WebSocket stream and cannot honour a
            // VLESS flow (e.g. xtls-rprx-vision needs TLS handshake semantics), so
            // a requested flow is rejected explicitly instead of being silently
            // downgraded to a plain stream (B4). Non-flow addons are accepted.
            if (decodeVlessFlow(addons)) {
                throw new errors_1.CoreError("BAD_REQUEST", "VLESS flow is not supported by this Worker core");
            }
        }
        const command = (await reader.readExact(1, signal))[0];
        if (command !== TCP_COMMAND && command !== UDP_COMMAND) {
            if (command === MUX_COMMAND)
                throw new errors_1.CoreError("BAD_REQUEST", "VLESS MUX is not supported by this Worker core");
            throw new errors_1.CoreError("BAD_REQUEST", `Unsupported VLESS command: ${command}`);
        }
        const port = parsePort(await reader.readExact(2, signal));
        const addressType = (await reader.readExact(1, signal))[0];
        let host;
        switch (addressType) {
            case IPV4: {
                const bytes = await reader.readExact(4, signal);
                host = `${bytes[0]}.${bytes[1]}.${bytes[2]}.${bytes[3]}`;
                break;
            }
            case DOMAIN: {
                const length = (await reader.readExact(1, signal))[0];
                if (length === 0 || length > this.maxDomainBytes)
                    throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid VLESS domain length");
                try {
                    host = (0, hostname_1.toAsciiHostname)(new TextDecoder("utf-8", { fatal: true }).decode(await reader.readExact(length, signal)));
                }
                catch (error) {
                    throw new errors_1.CoreError("INVALID_DESTINATION", "Invalid VLESS domain encoding", false, { cause: error });
                }
                break;
            }
            case IPV6:
                host = parseIpv6(await reader.readExact(16, signal));
                break;
            default:
                throw new errors_1.CoreError("BAD_REQUEST", `Unsupported VLESS address type: ${addressType}`);
        }
        const destination = { host, port };
        validateDestination(destination);
        if (command === UDP_COMMAND && port !== 53)
            throw new errors_1.CoreError("BAD_REQUEST", "VLESS UDP proxy is enabled only for DNS port 53");
        const initialPayload = reader.readAvailable(this.maxInitialPayloadBytes) ?? new Uint8Array(0);
        return {
            protocol: "vless",
            principal,
            destination,
            initialPayload,
            responseHeader: new Uint8Array([version, 0]),
            ...(command === UDP_COMMAND ? { udpDns: port === 53 } : {}),
        };
    }
    static principalId(uuid) {
        const bytes = (0, binary_1.uuidToBytes)(uuid);
        return { id: (0, binary_1.bytesToUuid)(bytes), protocol: "vless" };
    }
}
exports.VlessHandler = VlessHandler;

};

__m["core/engine"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ProxyEngine = void 0;
const errors_1 = __r("./errors");
const limits_1 = __r("./limits");
const bridge_1 = __r("../connection/bridge");
const lifecycle_1 = __r("../util/lifecycle");
const dns_udp_1 = __r("../connection/dns-udp");
class ProxyEngine {
    options;
    handshakeTimeoutMs;
    retryPolicy;
    idleTimeoutMs;
    maxSessionDurationMs;
    egress;
    maxEgressDecisionMs;
    constructor(options) {
        this.options = options;
        // Fallback defaults come from the central limits (A7) so direct engine
        // users see the same behavior as the runtime/config assembly.
        this.handshakeTimeoutMs = options.handshakeTimeoutMs ?? limits_1.CORE_LIMITS.defaultHandshakeTimeoutMs;
        this.retryPolicy = options.retryPolicy ?? { maxInitialRetries: limits_1.CORE_LIMITS.maxInitialRetries, firstByteTimeoutMs: limits_1.CORE_LIMITS.defaultFirstByteTimeoutMs, retryOnEmptyResponse: true };
        this.idleTimeoutMs = options.idleTimeoutMs ?? 0;
        this.maxSessionDurationMs = options.maxSessionDurationMs ?? limits_1.CORE_LIMITS.maxSessionDurationMs;
        this.maxEgressDecisionMs = options.maxEgressDecisionMs ?? limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs;
        this.egress = options.egress;
    }
    async handle(connection, context, protocol) {
        const engineAbort = new AbortController();
        const baseSignal = (0, lifecycle_1.combineAbortSignals)(context.signal, engineAbort.signal);
        const sessionDeadlineAt = (0, lifecycle_1.deadlineAtFromNow)(this.maxSessionDurationMs);
        const sessionSignal = (0, lifecycle_1.deadlineSignal)(baseSignal.signal, sessionDeadlineAt, "Maximum session duration exceeded");
        let sessionConnection = null;
        const metrics = this.options.metrics;
        metrics?.increment("session.started");
        const startedAt = Date.now();
        try {
            const handler = this.options.protocols.get(protocol);
            if (!handler)
                throw new errors_1.CoreError("UNSUPPORTED_PROTOCOL", `Protocol ${protocol} is not enabled`);
            const session = await (0, lifecycle_1.withTimeout)((signal) => handler.parse(connection.reader, context, signal), this.handshakeTimeoutMs, sessionSignal.signal, "Protocol handshake timeout");
            const routeContext = {
                destination: session.destination,
                principal: session.principal,
                protocol: session.protocol,
            };
            if (session.udpDns) {
                if (!this.options.dns)
                    throw new errors_1.CoreError("CONFIGURATION_ERROR", "DNS UDP support is not configured");
                await (0, dns_udp_1.bridgeVlessDnsUdp)(connection.reader, connection.writer, session, this.options.dns, sessionSignal.signal);
                metrics?.increment("session.completed");
                return;
            }
            const decision = await (0, lifecycle_1.withTimeout)((signal) => this.options.router.route(routeContext, signal), this.handshakeTimeoutMs, sessionSignal.signal, "Routing timeout");
            let responseHeaderSent = false;
            let lastOutcome = null;
            const egressOrder = this.orderForDecision(decision);
            const maxAttempts = Math.min(egressOrder.length, Math.max(0, this.retryPolicy.maxInitialRetries) + 1);
            let nextEgressIndex = 0;
            for (let attempt = 0; attempt < maxAttempts && nextEgressIndex < egressOrder.length; attempt++) {
                // Each dial decision gets a fresh budget (A2): anchoring the deadline at
                // session start let handshake time and earlier attempts consume the
                // whole budget, so designed retries could never dial at all.
                const attemptDialSignal = (0, lifecycle_1.deadlineSignal)(sessionSignal.signal, (0, lifecycle_1.deadlineAtFromNow)(this.maxEgressDecisionMs), "Egress decision deadline exceeded");
                let selected;
                try {
                    selected = await this.dialDecision(egressOrder, nextEgressIndex, session.destination, attemptDialSignal.signal);
                }
                finally {
                    attemptDialSignal.dispose();
                }
                sessionConnection = selected.connection;
                nextEgressIndex = selected.index + 1;
                const outcome = await (0, bridge_1.bridgeSession)(connection.reader, connection.writer, sessionConnection, {
                    signal: sessionSignal.signal,
                    session,
                    sendResponseHeader: !responseHeaderSent,
                    firstByteTimeoutMs: this.retryPolicy.firstByteTimeoutMs,
                    idleTimeoutMs: this.idleTimeoutMs,
                    sessionDeadlineAt,
                });
                responseHeaderSent = responseHeaderSent || outcome.responseHeaderSent;
                lastOutcome = outcome;
                // bridgeSession owns the upstream close lifecycle.
                sessionConnection = null;
                if (outcome.firstByteLatencyMs !== undefined)
                    metrics?.observe("session.first_byte_ms", outcome.firstByteLatencyMs);
                if (outcome.kind !== "upstream-empty")
                    break;
                if (!this.retryPolicy.retryOnEmptyResponse || !outcome.safeToRetry || attempt + 1 >= maxAttempts || nextEgressIndex >= egressOrder.length)
                    break;
            }
            if (lastOutcome?.kind === "upstream-empty") {
                metrics?.increment("egress.upstream_empty");
                throw lastOutcome.error ?? new errors_1.CoreError("UPSTREAM_CLOSED", "Upstream closed before sending data", true);
            }
            if (lastOutcome?.error && lastOutcome.error.code !== "CLIENT_CLOSED")
                throw lastOutcome.error;
            metrics?.increment("session.completed");
        }
        catch (error) {
            metrics?.increment("session.failed");
            const coreError = (0, errors_1.asCoreError)(error);
            engineAbort.abort(coreError);
            throw coreError;
        }
        finally {
            metrics?.observe("session.duration_ms", Date.now() - startedAt);
            engineAbort.abort("connection finished");
            sessionSignal.dispose();
            baseSignal.dispose();
            await sessionConnection?.close().catch(() => { });
        }
    }
    orderForDecision(decision) {
        return [...new Set([
                decision.egress,
                ...(decision.fallbackEgresses ?? []),
                ...(decision.fallbackEgress && !decision.fallbackEgresses?.includes(decision.fallbackEgress) ? [decision.fallbackEgress] : []),
            ])];
    }
    async dialDecision(order, startIndex, destination, signal) {
        let lastError;
        for (let index = startIndex; index < order.length; index++) {
            const type = order[index];
            const dialer = this.egress.get(type);
            if (!dialer) {
                lastError = new errors_1.CoreError("ROUTE_FAILED", `Egress ${type} is not configured`);
                continue;
            }
            try {
                const pending = dialer.connect(destination, signal);
                const connection = await (0, lifecycle_1.raceWithAbort)(pending, signal, async (lateConnection) => {
                    await lateConnection.close().catch(() => { });
                });
                return { connection, index };
            }
            catch (error) {
                lastError = error;
                if (signal.aborted)
                    throw signal.reason;
            }
        }
        throw lastError instanceof Error ? lastError : new errors_1.CoreError("EGRESS_FAILED", "All selected egress strategies failed", true, { cause: lastError });
    }
}
exports.ProxyEngine = ProxyEngine;

};

__m["connection/dns-udp"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.bridgeVlessDnsUdp = bridgeVlessDnsUdp;
const errors_1 = __r("../core/errors");
const types_1 = __r("../core/types");
const lifecycle_1 = __r("../util/lifecycle");
async function bridgeVlessDnsUdp(reader, writer, session, dns, signal) {
    if (!session.udpDns || session.destination.port !== 53)
        throw new errors_1.CoreError("BAD_REQUEST", "VLESS UDP proxy is enabled only for DNS port 53");
    let buffered = session.initialPayload;
    const take = (length) => {
        const value = buffered.subarray(0, length);
        buffered = buffered.subarray(length);
        return value;
    };
    const append = (chunk) => {
        if (!chunk.byteLength)
            return;
        const next = new Uint8Array(buffered.byteLength + chunk.byteLength);
        next.set(buffered);
        next.set(chunk, buffered.byteLength);
        buffered = next;
    };
    const readExact = async (length, readSignal) => {
        while (buffered.byteLength < length) {
            append(await reader.readSome(Math.max(length - buffered.byteLength, 2), readSignal));
        }
        return take(length);
    };
    // A client close is a clean end of this bridge no matter which byte of the
    // length-prefixed frame it lands on (A10): both framed reads go through the
    // same guard instead of only the length-prefix read returning cleanly.
    const readClientExact = async (length) => {
        try {
            // Each framed read also carries the bridge's own idle bound (A11): the
            // session signal alone could otherwise keep a silent session — and its
            // upstream resolver traffic — alive indefinitely.
            return await (0, lifecycle_1.withTimeout)((readSignal) => readExact(length, readSignal), types_1.CORE_LIMITS.defaultDnsSessionIdleTimeoutMs, signal, "VLESS UDP DNS session idle timeout");
        }
        catch (error) {
            if (error instanceof errors_1.CoreError && (error.code === "CLIENT_CLOSED" || error.code === "TIMEOUT"))
                return null;
            throw error;
        }
    };
    let responseHeaderSent = false;
    let queries = 0;
    while (!signal.aborted) {
        // Per-session query budget (A11): one authenticated connection must not be
        // an unbounded DoH relay. Exhausting the budget ends the bridge cleanly.
        if (queries >= types_1.CORE_LIMITS.maxDnsQueriesPerSession)
            return;
        const lengthBytes = await readClientExact(2);
        if (!lengthBytes)
            return;
        const packetLength = new DataView(lengthBytes.buffer, lengthBytes.byteOffset, 2).getUint16(0, false);
        if (packetLength < 1 || packetLength > types_1.CORE_LIMITS.maxDoHQueryBytes)
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `VLESS UDP DNS packet must be 1..${types_1.CORE_LIMITS.maxDoHQueryBytes} bytes`);
        const query = await readClientExact(packetLength);
        if (!query)
            return;
        queries++;
        const response = await dns.queryRaw(query, signal);
        // The 2-byte VLESS UDP frame caps payloads at 0xffff bytes, one byte below
        // the DNS layer's 64 KiB response bound; an oversize response cannot be
        // framed and fails the session rather than being truncated (A10 note).
        if (response.byteLength > 0xffff)
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", "DNS response exceeds VLESS UDP frame size");
        const frame = new Uint8Array(2 + response.byteLength);
        new DataView(frame.buffer).setUint16(0, response.byteLength, false);
        frame.set(response, 2);
        if (!responseHeaderSent && session.responseHeader?.byteLength) {
            const out = new Uint8Array(session.responseHeader.byteLength + frame.byteLength);
            out.set(session.responseHeader);
            out.set(frame, session.responseHeader.byteLength);
            await writer.write(out, signal);
            responseHeaderSent = true;
        }
        else {
            await writer.write(frame, signal);
        }
    }
}

};

__m["connection/bridge"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.bridgeSession = bridgeSession;
const errors_1 = __r("../core/errors");
const lifecycle_1 = __r("../util/lifecycle");
/** Streaming read chunk size for the upload pump; deliberately independent of
 *  the handshake initial-payload limit it was previously coupled to (A17). */
const STREAM_READ_CHUNK_BYTES = 64 * 1024;
async function bridgeSession(reader, clientWriter, upstream, options) {
    const bridgeAbort = new AbortController();
    const localDeadline = options.sessionDeadlineAt ?? (options.maxSessionDurationMs && options.maxSessionDurationMs > 0 ? Date.now() + options.maxSessionDurationMs : undefined);
    const base = (0, lifecycle_1.combineAbortSignals)(options.signal, bridgeAbort.signal);
    const combined = (0, lifecycle_1.deadlineSignal)(base.signal, localDeadline, "Maximum session duration exceeded");
    const state = { failure: null, firstByteReceived: false, uploadedAfterHandshake: 0, initialPayloadBytes: options.session.initialPayload.byteLength, responseHeaderSent: false, upstreamEnded: false, upstreamEmpty: false, clientClosed: false };
    const bridgeStartedAt = Date.now();
    let firstByteAt;
    let firstByteTimer;
    let lastActivity = bridgeStartedAt;
    let idleTimer;
    const touch = () => { lastActivity = Date.now(); };
    const disarmFirstByteTimer = () => {
        if (firstByteTimer !== undefined) {
            clearTimeout(firstByteTimer);
            firstByteTimer = undefined;
        }
    };
    const armFirstByteTimer = () => {
        if (state.firstByteReceived || firstByteTimer !== undefined)
            return;
        if (!options.firstByteTimeoutMs || options.firstByteTimeoutMs <= 0)
            return;
        firstByteTimer = setTimeout(() => {
            firstByteTimer = undefined;
            if (!state.firstByteReceived && !state.failure)
                fail(new errors_1.CoreError("TIMEOUT", "Upstream first-byte timeout", true));
        }, options.firstByteTimeoutMs);
    };
    const fail = (error) => { if (!state.failure)
        state.failure = (0, errors_1.asCoreError)(error); bridgeAbort.abort(state.failure); };
    const recordDeadlineAbort = () => {
        if (!combined.signal.aborted || state.failure)
            return;
        const reason = combined.signal.reason;
        if (reason instanceof errors_1.CoreError && reason.code === "TIMEOUT")
            state.failure = reason;
    };
    combined.signal.addEventListener("abort", recordDeadlineAbort, { once: true });
    recordDeadlineAbort();
    try {
        const sendHeader = options.sendResponseHeader !== false && options.session.responseHeader?.byteLength;
        if (sendHeader) {
            await clientWriter.write(options.session.responseHeader, combined.signal);
            state.responseHeaderSent = true;
            touch();
        }
        if (options.idleTimeoutMs && options.idleTimeoutMs > 0) {
            idleTimer = setInterval(() => { if (Date.now() - lastActivity >= options.idleTimeoutMs)
                fail(new errors_1.CoreError("TIMEOUT", "Connection idle timeout", true)); }, Math.min(options.idleTimeoutMs, 5000));
        }
        // Arm the first-byte deadline at bridge start (A3), not only as a side
        // effect of upload progress: server-first sessions (banner protocols, a
        // client waiting for the response header before sending more) otherwise
        // never start the timer and can wait for a first byte forever.
        armFirstByteTimer();
        const upload = async () => {
            const writer = upstream.writable.getWriter();
            const abortWriter = () => { void writer.abort(combined.signal.reason).catch(() => { }); };
            combined.signal.addEventListener("abort", abortWriter, { once: true });
            try {
                if (options.session.initialPayload.byteLength) {
                    await writer.write(options.session.initialPayload);
                    armFirstByteTimer();
                    touch();
                }
                while (!combined.signal.aborted) {
                    const chunk = await reader.readSome(STREAM_READ_CHUNK_BYTES, combined.signal);
                    await writer.write(chunk);
                    armFirstByteTimer();
                    state.uploadedAfterHandshake += chunk.byteLength;
                    touch();
                }
            }
            catch (error) {
                if (!combined.signal.aborted) {
                    const normalized = (0, errors_1.asCoreError)(error);
                    if (normalized.code === "CLIENT_CLOSED") {
                        state.clientClosed = true;
                        bridgeAbort.abort(normalized);
                    }
                    else
                        fail(normalized);
                }
            }
            finally {
                combined.signal.removeEventListener("abort", abortWriter);
                try {
                    await writer.close();
                }
                catch { /* peer may already be closed */ }
                writer.releaseLock();
            }
        };
        const download = async () => {
            const streamReader = upstream.readable.getReader();
            const cancelReader = () => { void streamReader.cancel(combined.signal.reason).catch(() => { }); };
            combined.signal.addEventListener("abort", cancelReader, { once: true });
            try {
                while (!combined.signal.aborted) {
                    const result = await streamReader.read();
                    if (result.done) {
                        state.upstreamEnded = true;
                        state.upstreamEmpty = !state.firstByteReceived;
                        bridgeAbort.abort(state.upstreamEmpty ? "upstream empty close" : "upstream closed");
                        break;
                    }
                    if (result.value.byteLength === 0)
                        continue;
                    state.firstByteReceived = true;
                    firstByteAt ??= Date.now();
                    disarmFirstByteTimer();
                    await clientWriter.write(result.value, combined.signal);
                    touch();
                }
            }
            catch (error) {
                if (!combined.signal.aborted)
                    fail(error);
            }
            finally {
                disarmFirstByteTimer();
                combined.signal.removeEventListener("abort", cancelReader);
                streamReader.releaseLock();
            }
        };
        await Promise.all([upload(), download()]);
        const error = state.failure;
        if (error) {
            return { kind: error.code === "CLIENT_CLOSED" ? "client-closed" : "error", firstByteReceived: state.firstByteReceived, firstByteLatencyMs: firstByteAt === undefined ? undefined : Math.max(0, firstByteAt - bridgeStartedAt), responseHeaderSent: state.responseHeaderSent, safeToRetry: false, error };
        }
        if (state.upstreamEmpty) {
            const safeToRetry = !state.clientClosed && state.uploadedAfterHandshake === 0 && isReplaySafeInitialPayload(options.session.initialPayload);
            return { kind: "upstream-empty", firstByteReceived: false, firstByteLatencyMs: undefined, responseHeaderSent: state.responseHeaderSent, safeToRetry, error: new errors_1.CoreError("UPSTREAM_CLOSED", "Upstream closed before sending data", true) };
        }
        return { kind: "completed", firstByteReceived: state.firstByteReceived, firstByteLatencyMs: firstByteAt === undefined ? undefined : Math.max(0, firstByteAt - bridgeStartedAt), responseHeaderSent: state.responseHeaderSent, safeToRetry: false };
    }
    catch (error) {
        const normalized = (0, errors_1.asCoreError)(error);
        fail(normalized);
        return { kind: normalized.code === "CLIENT_CLOSED" ? "client-closed" : "error", firstByteReceived: state.firstByteReceived, firstByteLatencyMs: firstByteAt === undefined ? undefined : Math.max(0, firstByteAt - bridgeStartedAt), responseHeaderSent: state.responseHeaderSent, safeToRetry: false, error: normalized };
    }
    finally {
        if (idleTimer !== undefined)
            clearInterval(idleTimer);
        disarmFirstByteTimer();
        bridgeAbort.abort("bridge finished");
        combined.signal.removeEventListener("abort", recordDeadlineAbort);
        combined.dispose();
        base.dispose();
        await upstream.close().catch(() => { });
    }
}
function isReplaySafeInitialPayload(payload) {
    if (payload.byteLength === 0)
        return true;
    // A VLESS/Trojan payload that is a TLS ClientHello can be safely replayed
    // to the next egress when the upstream closed before sending any byte.
    return payload.byteLength >= 6 && payload[0] === 0x16 && payload[1] === 0x03 && payload[5] === 0x01;
}

};

__m["dns/engine"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DnsEngine = void 0;
exports.parseDnsStrategy = parseDnsStrategy;
const errors_1 = __r("../core/errors");
const cache_1 = __r("./cache");
const doh_1 = __r("./doh");
const hosts_1 = __r("./hosts");
const resolver_1 = __r("./resolver");
const limits_1 = __r("../core/limits");
const config_1 = __r("./config");
const hostname_1 = __r("../util/hostname");
const router_1 = __r("../routing/router");
function parseDnsStrategy(value) {
    const normalized = (value ?? "auto").trim().toLowerCase();
    if (["auto", "ipv4", "ipv6", "prefer_ipv4", "prefer_ipv6"].includes(normalized))
        return normalized;
    throw new errors_1.CoreError("CONFIGURATION_ERROR", `Unsupported DNS_IP_PREFERENCE: ${value}`);
}
class DnsEngine {
    enabled;
    strategy;
    timeoutMs;
    retries;
    fallback;
    cache;
    hosts;
    resolvers;
    dnsDisabledWithNat64Fallback;
    constructor(env) {
        const enabledRaw = env.DNS_ENABLED?.trim();
        const mode = (0, router_1.parseEgressMode)(env.EGRESS_MODE);
        const nat64MayNeedDns = mode === "nat64" || mode === "smart";
        this.dnsDisabledWithNat64Fallback = enabledRaw ? /^(0|false|no|off)$/i.test(enabledRaw) && nat64MayNeedDns : false;
        this.enabled = enabledRaw
            ? /^(1|true|yes|on)$/i.test(enabledRaw)
            : Boolean(env.DNS_RESOLVERS?.trim() || env.DNS_HOSTS?.trim() || nat64MayNeedDns);
        this.strategy = parseDnsStrategy(env.DNS_IP_PREFERENCE);
        this.timeoutMs = parseBoundedInteger(env.DNS_TIMEOUT_MS, 3000, "DNS_TIMEOUT_MS", 100, 60_000);
        this.retries = parseBoundedInteger(env.DNS_RETRIES, 1, "DNS_RETRIES", 0, 5);
        this.fallback = parseBoolean(env.DNS_FALLBACK, true);
        this.cache = parseBoolean(env.DNS_CACHE, true) ? new cache_1.DnsCache(parseBoundedInteger(env.DNS_CACHE_MAX_ENTRIES, 1024, "DNS_CACHE_MAX_ENTRIES", 1, 100_000), parseBoundedInteger(env.DNS_CACHE_MAX_TTL, 300, "DNS_CACHE_MAX_TTL", 1, 86_400), parseBoundedInteger(env.DNS_CACHE_NEGATIVE_TTL, 30, "DNS_CACHE_NEGATIVE_TTL", 1, 3_600)) : null;
        this.hosts = env.DNS_HOSTS?.trim() ? new hosts_1.HostsResolver(env.DNS_HOSTS) : null;
        const maxResolvers = parseBoundedInteger(env.DNS_MAX_RESOLVERS, limits_1.CORE_LIMITS.maxDnsResolvers, "DNS_MAX_RESOLVERS", 1, limits_1.CORE_LIMITS.maxDnsResolvers);
        this.resolvers = this.enabled
            ? (0, config_1.parseDohResolverEndpoints)(env.DNS_RESOLVERS, maxResolvers).map((endpoint) => new doh_1.DoHResolver({ endpoint, timeoutMs: this.timeoutMs, retries: this.retries, cache: this.cache ?? undefined }))
            : [];
    }
    async queryRaw(message, signal) {
        if (!this.enabled)
            throw new errors_1.CoreError("DNS_FAILED", "DNS resolver is disabled", true);
        if (message.byteLength < 1 || message.byteLength > limits_1.CORE_LIMITS.maxDoHQueryBytes)
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `DNS message must be 1..${limits_1.CORE_LIMITS.maxDoHQueryBytes} bytes`);
        let lastError;
        for (const resolver of this.resolvers) {
            const rawResolver = resolver;
            if (!rawResolver.queryRaw)
                continue;
            try {
                return await rawResolver.queryRaw(message, signal);
            }
            catch (error) {
                lastError = error;
                if (signal.aborted)
                    throw signal.reason;
                if (!this.fallback)
                    break;
            }
        }
        throw new errors_1.CoreError("DNS_FAILED", "All configured DoH resolvers failed", true, { cause: lastError });
    }
    async resolve(host, strategy = this.strategy, signal) {
        const clean = (0, hostname_1.toAsciiHostname)(host).replace(/\.$/, "").toLowerCase();
        if (!clean)
            throw new errors_1.CoreError("DNS_FAILED", "DNS hostname is empty");
        if ((0, resolver_1.isIpLiteral)(clean)) {
            const family = clean.includes(":") ? 6 : 4;
            if (strategy === "ipv4" && family !== 4)
                return [];
            if (strategy === "ipv6" && family !== 6)
                return [];
            return [{ address: clean, family, ttl: 0 }];
        }
        if (this.hosts) {
            const values = await this.hosts.resolve(clean, strategy, signal);
            if (values.length)
                return values;
        }
        if (!this.enabled)
            return [];
        let lastError;
        for (const resolver of this.resolvers) {
            try {
                const values = await resolver.resolve(clean, strategy, signal);
                if (values.length)
                    return values;
                if (!this.fallback)
                    break;
            }
            catch (error) {
                lastError = error;
                if (signal.aborted)
                    throw signal.reason;
                if (!this.fallback)
                    break;
            }
        }
        if (lastError)
            throw new errors_1.CoreError("DNS_FAILED", `DNS resolution failed for ${clean}`, true, { cause: lastError });
        return [];
    }
    diagnostics() {
        return {
            enabled: this.enabled,
            strategy: this.strategy,
            cache: this.cache?.stats() ?? null,
            hosts: { enabled: Boolean(this.hosts), entries: this.hosts?.size ?? 0 },
            resolvers: this.resolvers.map((resolver) => resolver.diagnostics()),
            resolver_count: this.resolvers.length,
            fallback: this.fallback,
            timeout_ms: this.timeoutMs,
            retries: this.retries,
            warnings: this.dnsDisabledWithNat64Fallback ? [{
                    code: "DNS_DISABLED_NAT64_DOMAIN_FALLBACK",
                    message: "DNS_ENABLED is explicitly false while EGRESS_MODE is smart/nat64; NAT64 cannot resolve domain names unless DNS_HOSTS supplies a matching entry. Direct egress or IP-literal destinations may still work.",
                }] : [],
        };
    }
}
exports.DnsEngine = DnsEngine;
/**
 * Bounded integer parsing for DNS settings (D5): the engine is safe by
 * construction and no longer relies on a separate validation layer having
 * run first. Bounds mirror src/config/validation.ts exactly.
 */
function parseBoundedInteger(value, fallback, name, min, max) {
    if (!value?.trim())
        return fallback;
    if (!/^\d+$/.test(value.trim()))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${name} must be an integer between ${min} and ${max}`);
    const n = Number(value);
    if (!Number.isSafeInteger(n) || n < min || n > max)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${name} must be an integer between ${min} and ${max}`);
    return n;
}
function parseBoolean(value, fallback) {
    if (!value?.trim())
        return fallback;
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized))
        return true;
    if (["0", "false", "no", "off"].includes(normalized))
        return false;
    throw new errors_1.CoreError("CONFIGURATION_ERROR", "Boolean DNS setting must be one of true, false, 1, 0, yes, no, on, off");
}

};

__m["dns/resolver"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isIpLiteral = isIpLiteral;
exports.literalDestination = literalDestination;
function isIpLiteral(value) {
    const input = value.trim().replace(/^\[|\]$/g, "");
    const v4 = input.split(".");
    if (v4.length === 4 && v4.every((part) => /^\d{1,3}$/.test(part))) {
        return v4.every((part) => Number(part) >= 0 && Number(part) <= 255);
    }
    if (!input.includes(":"))
        return false;
    const halves = input.split("::");
    if (halves.length > 2)
        return false;
    const countSide = (side) => {
        if (!side)
            return 0;
        let count = 0;
        for (const part of side.split(":")) {
            if (part.includes(".")) {
                const octets = part.split(".");
                if (octets.length !== 4 || octets.some((x) => !/^\d{1,3}$/.test(x) || Number(x) > 255))
                    return null;
                count += 2;
            }
            else {
                if (!/^[0-9a-fA-F]{1,4}$/.test(part))
                    return null;
                count += 1;
            }
        }
        return count;
    };
    const left = countSide(halves[0]);
    const right = countSide(halves[1] ?? "");
    if (left === null || right === null)
        return false;
    return halves.length === 2 ? left + right < 8 : left === 8;
}
function literalDestination(host, port) {
    if (!isIpLiteral(host))
        return null;
    const clean = host.replace(/^\[|\]$/g, "");
    return { host: clean, port };
}

};

__m["dns/hosts"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HostsResolver = void 0;
const errors_1 = __r("../core/errors");
const hostname_1 = __r("../util/hostname");
const resolver_1 = __r("./resolver");
class HostsResolver {
    map = new Map();
    constructor(raw) {
        for (const entry of (raw ?? "").split(/[\r\n,]+/).map((x) => x.trim()).filter(Boolean)) {
            // Split on the FIRST '=' only; a second '=' makes the entry invalid
            // instead of being silently discarded (D14b).
            const eq = entry.indexOf("=");
            const host = eq > 0 ? entry.slice(0, eq).trim() : "";
            const address = eq > 0 ? entry.slice(eq + 1).trim() : "";
            if (!host || !address || address.includes("="))
                throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid DNS_HOSTS entry: ${entry}`);
            const family = (0, resolver_1.isIpLiteral)(address) ? (address.includes(":") ? 6 : 4) : 0;
            if (!family)
                throw new errors_1.CoreError("CONFIGURATION_ERROR", `DNS_HOSTS value is not an IP address: ${address}`);
            // Keys are IDNA-normalised exactly like lookup names in DnsEngine,
            // so Unicode entries can actually match (D14b). Hosts the normaliser
            // rejects keep their legacy lower-cased key (they simply never match
            // a normalised lookup, as before).
            const key = (0, hostname_1.toAsciiHostname)(host).toLowerCase().replace(/\.$/, "") || host.toLowerCase().replace(/\.$/, "");
            const values = this.map.get(key) ?? [];
            const normalizedAddress = address.replace(/^\[|\]$/g, "").toLowerCase();
            values.push({ address: normalizedAddress, family, ttl: 0 });
            this.map.set(key, values);
        }
    }
    get size() { return this.map.size; }
    diagnostics() { return { type: "hosts", queries: 0, successes: 0, failures: 0 }; }
    async resolve(host, strategy, _signal) {
        const values = this.map.get(host.toLowerCase().replace(/\.$/, "")) ?? [];
        if (strategy === "ipv4")
            return values.filter((item) => item.family === 4);
        if (strategy === "ipv6")
            return values.filter((item) => item.family === 6);
        if (strategy === "prefer_ipv4")
            return [...values.filter((item) => item.family === 4), ...values.filter((item) => item.family === 6)];
        if (strategy === "prefer_ipv6")
            return [...values.filter((item) => item.family === 6), ...values.filter((item) => item.family === 4)];
        return values;
    }
}
exports.HostsResolver = HostsResolver;

};

__m["dns/cache"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DnsCache = void 0;
class DnsCache {
    maxEntries;
    maxTtlSeconds;
    negativeTtlSeconds;
    entries = new Map();
    hitsCount = 0;
    missesCount = 0;
    evictionsCount = 0;
    constructor(maxEntries = 1024, maxTtlSeconds = 300, negativeTtlSeconds = 30) {
        this.maxEntries = maxEntries;
        this.maxTtlSeconds = maxTtlSeconds;
        this.negativeTtlSeconds = negativeTtlSeconds;
    }
    get(host, type) {
        const key = this.key(host, type);
        const entry = this.entries.get(key);
        if (!entry) {
            this.missesCount++;
            return null;
        }
        if (entry.expiresAt <= Date.now()) {
            this.entries.delete(key);
            this.missesCount++;
            return null;
        }
        this.entries.delete(key);
        this.entries.set(key, entry);
        this.hitsCount++;
        return entry.negative ? [] : entry.values.map((item) => ({ ...item, ttl: Math.max(0, Math.floor((entry.expiresAt - Date.now()) / 1000)) }));
    }
    set(host, type, values, ttlSeconds) {
        if (!values.length) {
            this.setNegative(host, type);
            return;
        }
        if (ttlSeconds <= 0)
            return;
        const ttl = Math.min(Math.floor(ttlSeconds), this.maxTtlSeconds);
        // Store a defensive copy: callers keep (and return) the array they passed
        // in, and mutating it must not corrupt the cached entry (D13).
        this.setEntry(host, type, { values: values.map((item) => ({ ...item })), expiresAt: Date.now() + ttl * 1000, negative: false });
    }
    setNegative(host, type, ttlSeconds) {
        // RFC 2308 §5: when the caller supplies the zone-derived negative TTL
        // (min of SOA record TTL and SOA MINIMUM) it wins, still capped by the
        // cache-wide maximum; otherwise the configured default applies.
        const requested = ttlSeconds !== undefined && Number.isFinite(ttlSeconds) && ttlSeconds > 0
            ? Math.floor(ttlSeconds)
            : this.negativeTtlSeconds;
        const ttl = Math.min(requested, this.maxTtlSeconds);
        if (ttl <= 0)
            return;
        this.setEntry(host, type, { values: [], expiresAt: Date.now() + ttl * 1000, negative: true });
    }
    stats() {
        this.pruneExpired();
        return { entries: this.entries.size, hits: this.hitsCount, misses: this.missesCount, evictions: this.evictionsCount };
    }
    clear() { this.entries.clear(); }
    setEntry(host, type, entry) {
        this.pruneExpired(32);
        const key = this.key(host, type);
        this.entries.delete(key);
        this.entries.set(key, entry);
        while (this.entries.size > this.maxEntries) {
            const first = this.entries.keys().next().value;
            if (!first)
                break;
            this.entries.delete(first);
            this.evictionsCount++;
        }
    }
    pruneExpired(maxScan = Number.POSITIVE_INFINITY) {
        const now = Date.now();
        let scanned = 0;
        for (const [key, entry] of this.entries) {
            if (entry.expiresAt <= now)
                this.entries.delete(key);
            if (++scanned >= maxScan)
                break;
        }
    }
    key(host, type) {
        return `${type}:${host.trim().toLowerCase().replace(/\.$/, "")}`;
    }
}
exports.DnsCache = DnsCache;

};

__m["config/runtime"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildRuntimeConfig = buildRuntimeConfig;
const errors_1 = __r("../core/errors");
const limits_1 = __r("../core/limits");
const TRUE_VALUES = new Set(["1", "true", "yes", "on"]);
const FALSE_VALUES = new Set(["0", "false", "no", "off"]);
function integer(raw, fallback, min, max, field) {
    // Wrangler JSON vars can deliver non-strings; fail with a configuration error, not a TypeError (C16).
    if (raw !== undefined && typeof raw !== "string")
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be a string integer from ${min} to ${max}`);
    const value = raw?.trim();
    if (!value)
        return fallback;
    if (!/^\d+$/.test(value))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be an integer from ${min} to ${max}`);
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be an integer from ${min} to ${max}`);
    }
    return parsed;
}
function boolean(raw, fallback, field) {
    if (raw !== undefined && typeof raw !== "string")
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be a boolean`);
    const value = raw?.trim().toLowerCase();
    if (!value)
        return fallback;
    if (TRUE_VALUES.has(value))
        return true;
    if (FALSE_VALUES.has(value))
        return false;
    throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be a boolean`);
}
function buildRuntimeConfig(env) {
    const retry = {
        maxInitialRetries: integer(env.CORE_MAX_INITIAL_RETRIES, limits_1.CORE_LIMITS.maxInitialRetries, 0, 4, "CORE_MAX_INITIAL_RETRIES"),
        firstByteTimeoutMs: integer(env.CORE_FIRST_BYTE_TIMEOUT_MS, limits_1.CORE_LIMITS.defaultFirstByteTimeoutMs, 0, 60_000, "CORE_FIRST_BYTE_TIMEOUT_MS"),
        retryOnEmptyResponse: boolean(env.CORE_RETRY_ON_EMPTY_RESPONSE, true, "CORE_RETRY_ON_EMPTY_RESPONSE"),
    };
    const addressRace = {
        maxConcurrent: integer(env.CORE_MAX_CONCURRENT_ADDRESS_TRIES, limits_1.CORE_LIMITS.maxConcurrentAddressTries, 1, limits_1.CORE_LIMITS.maxConcurrentAddressTries, "CORE_MAX_CONCURRENT_ADDRESS_TRIES"),
        staggerMs: integer(env.CORE_ADDRESS_TRY_STAGGER_MS, limits_1.CORE_LIMITS.defaultAddressTryStaggerMs, 0, 5_000, "CORE_ADDRESS_TRY_STAGGER_MS"),
    };
    const maxNodes = integer(env.CORE_OUTPUT_MAX_NODES, limits_1.CORE_LIMITS.maxOutputNodes, 1, 10_000, "CORE_OUTPUT_MAX_NODES");
    const output = {
        defaultNodes: integer(env.CORE_OUTPUT_DEFAULT_NODES, Math.min(limits_1.CORE_LIMITS.defaultOutputNodes, maxNodes), 1, maxNodes, "CORE_OUTPUT_DEFAULT_NODES"),
        maxNodes,
        maxBytes: integer(env.CORE_OUTPUT_MAX_BYTES, limits_1.CORE_LIMITS.maxOutputBytes, 1024, 8 * 1024 * 1024, "CORE_OUTPUT_MAX_BYTES"),
    };
    return {
        handshakeTimeoutMs: integer(env.CORE_HANDSHAKE_TIMEOUT_MS, limits_1.CORE_LIMITS.defaultHandshakeTimeoutMs, 100, 60_000, "CORE_HANDSHAKE_TIMEOUT_MS"),
        egressConnectTimeoutMs: integer(env.EGRESS_CONNECT_TIMEOUT_MS, limits_1.CORE_LIMITS.defaultEgressConnectTimeoutMs, 100, 60_000, "EGRESS_CONNECT_TIMEOUT_MS"),
        idleTimeoutMs: integer(env.CORE_IDLE_TIMEOUT_MS, 0, 0, 24 * 60 * 60 * 1000, "CORE_IDLE_TIMEOUT_MS"),
        maxSessionDurationMs: integer(env.CORE_MAX_SESSION_MS, limits_1.CORE_LIMITS.maxSessionDurationMs, 0, 7 * 24 * 60 * 60 * 1000, "CORE_MAX_SESSION_MS"),
        maxEgressDecisionMs: integer(env.CORE_MAX_EGRESS_DECISION_MS, limits_1.CORE_LIMITS.defaultMaxEgressDecisionMs, 1_000, 120_000, "CORE_MAX_EGRESS_DECISION_MS"),
        retry,
        addressRace,
        output,
    };
}

};

__m["transport/websocket/byte-writer"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WebSocketByteWriter = void 0;
const errors_1 = __r("../../core/errors");
const types_1 = __r("../../core/types");
class WebSocketByteWriter {
    socket;
    maxChunkBytes;
    maxQueuedBytes;
    closed = false;
    queuedBytes = 0;
    draining = false;
    queue = [];
    constructor(socket, maxChunkBytes = types_1.CORE_LIMITS.maxWebSocketWriteChunkBytes, maxQueuedBytes = types_1.CORE_LIMITS.maxWebSocketBufferedBytes) {
        this.socket = socket;
        this.maxChunkBytes = maxChunkBytes;
        this.maxQueuedBytes = maxQueuedBytes;
        if (!Number.isInteger(maxChunkBytes) || maxChunkBytes <= 0)
            throw new RangeError("maxChunkBytes must be a positive integer");
        if (!Number.isInteger(maxQueuedBytes) || maxQueuedBytes <= 0)
            throw new RangeError("maxQueuedBytes must be a positive integer");
    }
    markClosed(reason = new errors_1.CoreError("CLIENT_CLOSED", "WebSocket is closed")) {
        this.closed = true;
        const pending = this.queue.splice(0);
        this.queuedBytes = 0;
        for (const item of pending)
            item.reject(reason);
    }
    /** Bytes handed to the runtime send buffer but not yet consumed by the peer. */
    socketBufferedBytes() {
        const buffered = this.socket.bufferedAmount;
        return typeof buffered === "number" && Number.isFinite(buffered) && buffered > 0 ? buffered : 0;
    }
    async write(data, signal) {
        if (data.byteLength === 0)
            return;
        if (signal?.aborted)
            throw signal.reason;
        if (this.closed || this.socket.readyState !== WebSocket.OPEN)
            throw new errors_1.CoreError("CLIENT_CLOSED", "WebSocket is closed");
        // Admission accounts for both this queue and the runtime send buffer
        // (A1), and the check + the synchronous reservations in enqueue() happen
        // before the first await, so overlapping write() calls cannot jointly
        // overshoot the bound (A16).
        const available = this.maxQueuedBytes - this.queuedBytes - this.socketBufferedBytes();
        if (data.byteLength > available) {
            throw new errors_1.CoreError("MESSAGE_TOO_LARGE", `WebSocket outbound queue exceeds ${this.maxQueuedBytes} bytes`, true);
        }
        const chunks = [];
        for (let offset = 0; offset < data.byteLength; offset += this.maxChunkBytes) {
            chunks.push(new Uint8Array(data.subarray(offset, Math.min(offset + this.maxChunkBytes, data.byteLength))));
        }
        await Promise.all(chunks.map((chunk) => this.enqueue(chunk, signal)));
    }
    enqueue(data, signal) {
        return new Promise((resolve, reject) => {
            if (signal?.aborted) {
                reject(signal.reason);
                return;
            }
            // Defense in depth (A16): re-validate at reservation time.
            if (this.queuedBytes + data.byteLength > this.maxQueuedBytes) {
                reject(new errors_1.CoreError("MESSAGE_TOO_LARGE", `WebSocket outbound queue exceeds ${this.maxQueuedBytes} bytes`, true));
                return;
            }
            this.queuedBytes += data.byteLength;
            this.queue.push({ data, signal, resolve, reject });
            void this.drain();
        });
    }
    async drain() {
        if (this.draining)
            return;
        this.draining = true;
        try {
            while (this.queue.length) {
                if (this.closed || this.socket.readyState !== WebSocket.OPEN) {
                    const error = new errors_1.CoreError("CLIENT_CLOSED", "WebSocket is closed");
                    const pending = this.queue.splice(0);
                    this.queuedBytes = 0;
                    for (const item of pending)
                        item.reject(error);
                    break;
                }
                // Real backpressure (A1): WebSocket.send() has no flow control, so the
                // queue bound alone does not bound memory. While the runtime send
                // buffer plus this queue already hold the full budget, wait for the
                // peer to consume before handing over more; pending write() promises
                // stay unresolved, which is what throttles upstream readers.
                if (this.socketBufferedBytes() + this.queuedBytes > this.maxQueuedBytes) {
                    await new Promise((resolve) => setTimeout(resolve, 10));
                    continue;
                }
                const item = this.queue.shift();
                this.queuedBytes -= item.data.byteLength;
                if (item.signal?.aborted) {
                    item.reject(item.signal.reason);
                    continue;
                }
                try {
                    this.socket.send(item.data);
                    item.resolve();
                }
                catch (error) {
                    item.reject(error);
                    this.markClosed(error);
                    break;
                }
                await Promise.resolve();
            }
        }
        finally {
            this.draining = false;
            if (this.queue.length && !this.closed)
                queueMicrotask(() => void this.drain());
        }
    }
}
exports.WebSocketByteWriter = WebSocketByteWriter;

};

__m["output/service"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.renderSubscriptionWithProvider = renderSubscriptionWithProvider;
exports.renderCoreFormat = renderCoreFormat;
const errors_1 = __r("../core/errors");
const runtime_1 = __r("../config/runtime");
const planning_1 = __r("./planning");
const aggregation_1 = __r("./aggregation");
const share_link_1 = __r("./share-link");
const artifacts_1 = __r("./artifacts");
function budget(env) {
    return (0, runtime_1.buildRuntimeConfig)(env).output;
}
function encodeBase64(input) {
    const bytes = new TextEncoder().encode(input);
    let binary = "";
    for (let i = 0; i < bytes.length; i += 0x8000) {
        const end = Math.min(bytes.length, i + 0x8000);
        let chunk = "";
        for (let j = i; j < end; j++)
            chunk += String.fromCharCode(bytes[j]);
        binary += chunk;
    }
    return btoa(binary);
}
function selectedEncode(request) {
    const value = new URL(request.url).searchParams.get("encode")?.trim().toLowerCase() ?? "base64";
    if (value !== "base64" && value !== "raw")
        throw new errors_1.CoreError("BAD_REQUEST", "encode must be base64 or raw");
    return value;
}
function bytesOf(value) {
    return new TextEncoder().encode(value).byteLength;
}
function fitNativeLines(lines, maxBytes, encoding) {
    if (!lines.length)
        return { body: "", count: 0, byteTruncated: false };
    const render = (count) => {
        const raw = lines.slice(0, count).join("\n");
        return encoding === "raw" ? raw : encodeBase64(raw);
    };
    if (bytesOf(render(lines.length)) <= maxBytes)
        return { body: render(lines.length), count: lines.length, byteTruncated: false };
    let low = 1;
    let high = lines.length;
    let best = 0;
    while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        if (bytesOf(render(mid)) <= maxBytes) {
            best = mid;
            low = mid + 1;
        }
        else
            high = mid - 1;
    }
    if (best === 0)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Output byte budget ${maxBytes} bytes is too small to fit a single node`);
    return { body: render(best), count: best, byteTruncated: best < lines.length };
}
function outputHeaders(page, returnedCount, byteTruncated) {
    const headers = {
        "X-Core-Output-Count": String(returnedCount),
        "X-Core-Output-Limit": String(page.limit),
        "X-Core-Output-Offset": String(page.offset),
        "X-Core-Output-Total": String(page.candidateCount),
        "X-Core-Output-Truncated": String(byteTruncated || page.hasMore),
    };
    if (page.limitClamped)
        headers["X-Core-Output-Limit-Clamped"] = "true";
    if (page.hasMore)
        headers["X-Core-Output-Next-Offset"] = String(page.nextOffset ?? (page.offset + page.items.length));
    return headers;
}
async function renderSubscriptionWithProvider(env, request, provider, resolver, fetchImpl = fetch) {
    const url = new URL(request.url);
    if (url.searchParams.has("artifact")) {
        throw new errors_1.CoreError("BAD_REQUEST", "artifact is only supported on /__core/outputs/xray, /__core/outputs/sing-box or /__core/outputs/clash");
    }
    const selection = (0, planning_1.parseOutputSelection)(env, url.searchParams);
    const limits = budget(env);
    const page = await (0, planning_1.buildOutputNodePage)(env, url.hostname, selection, (0, planning_1.createOutputCredentialSource)(provider), limits, request.signal, "native");
    const lines = page.items.map((node) => (0, share_link_1.generateShareLink)(node));
    if ((url.searchParams.get("aggregate") ?? "false").trim().toLowerCase() === "true") {
        const external = await (0, aggregation_1.fetchNativeSubscriptionSources)(env, request.signal, 8, 256 * 1024, fetchImpl, resolver);
        const output = [...lines];
        const seen = new Set(output);
        for (const line of external) {
            if (output.length >= page.limit)
                break;
            if (!seen.has(line)) {
                seen.add(line);
                output.push(line);
            }
        }
        lines.length = 0;
        lines.push(...output);
    }
    const encoding = selectedEncode(request);
    const fitted = fitNativeLines(lines, limits.maxBytes, encoding);
    const headers = outputHeaders(page, fitted.count, fitted.byteTruncated);
    return {
        body: fitted.body,
        contentType: "text/plain; charset=utf-8",
        format: "native",
        count: fitted.count,
        encoding,
        headers,
    };
}
async function renderCoreFormat(env, request, provider, format) {
    const rendered = await (0, artifacts_1.renderOutputArtifact)(env, request, provider, format);
    return {
        body: rendered.body,
        contentType: rendered.contentType,
        format,
        count: rendered.count,
        headers: {
            ...(rendered.headers ?? {}),
            "Content-Disposition": `attachment; filename="${rendered.filename.replaceAll('"', "")}"`,
        },
    };
}

};

__m["output/artifacts"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.renderOutputArtifact = renderOutputArtifact;
exports.parseArtifact = parseArtifact;
const limits_1 = __r("../core/limits");
const errors_1 = __r("../core/errors");
const runtime_1 = __r("../config/runtime");
const planning_1 = __r("./planning");
const adapters_1 = __r("./adapters");
const profiles_1 = __r("./profiles");
const compatibility_1 = __r("./compatibility");
function budget(env) {
    return (0, runtime_1.buildRuntimeConfig)(env).output;
}
function bytesOf(value) {
    return new TextEncoder().encode(value).byteLength;
}
function fitNodes(nodes, render, maxBytes, renderedCount = (items) => items.length) {
    if (!nodes.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "No nodes are available for output");
    const full = render(nodes);
    if (bytesOf(full) <= maxBytes)
        return { body: full, count: renderedCount(nodes), byteTruncated: false };
    let low = 1;
    let high = nodes.length;
    let best = 0;
    while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        if (bytesOf(render(nodes.slice(0, mid))) <= maxBytes) {
            best = mid;
            low = mid + 1;
        }
        else
            high = mid - 1;
    }
    if (best === 0)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Output byte budget ${maxBytes} bytes is too small to fit a single node`);
    return { body: render(nodes.slice(0, best)), count: renderedCount(nodes.slice(0, best)), byteTruncated: best < nodes.length };
}
function effectiveSelection(selection, limitOverride) {
    return limitOverride === undefined ? selection : { ...selection, limit: limitOverride };
}
function formatLabel(format) {
    return format === "sing-box" ? "singbox" : format;
}
function parseArtifact(params) {
    const value = (params.get("artifact") ?? "manifest").trim().toLowerCase();
    const accepted = ["manifest", "node", "bundle", "best-ping", "best-fragment", "best-ping-fragment"];
    if (!accepted.includes(value))
        throw new errors_1.CoreError("BAD_REQUEST", `artifact must be one of ${accepted.join(", ")}`);
    return value;
}
function artifactUrl(request, artifact, _target, extra = "") {
    const url = new URL(request.url);
    // Never propagate bearer query credentials into child artifact URLs. Consumers must use Authorization.
    url.searchParams.delete("token");
    // Preserve the complete selection query; mutate only artifact/paging controls below.
    url.searchParams.set("artifact", artifact);
    if (extra) {
        const params = new URLSearchParams(extra.replace(/^\?/, ""));
        for (const [key, value] of params)
            url.searchParams.set(key, value);
    }
    return url.toString();
}
function fragmentClientRequirements(target, kind) {
    if (kind !== "best-fragment" && kind !== "best-ping-fragment")
        return undefined;
    if (target === "xray")
        return ["Xray-core >= v26.6.27 (FinalMask TCP fragment lengths/delays support)"];
    if (target === "sing-box")
        return ["sing-box >= 1.12.0 (TLS fragment/record_fragment support)"];
    return undefined;
}
function descriptor(id, kind, target, protocol, url, supported = true, reason, itemCount, auth) {
    const clientRequirements = fragmentClientRequirements(target, kind);
    return { id, kind, target, protocol, endpointScope: kind === "best-fragment" || kind === "best-ping-fragment" ? "tls-only" : "selected", ...(itemCount === undefined ? {} : { itemCount }), url, supported, ...(auth ? { auth } : {}), ...(reason ? { reason } : {}), ...(clientRequirements ? { clientRequirements } : {}) };
}
function fragmentCompatibilityHeaders(target) {
    const requirement = fragmentClientRequirements(target, "best-fragment");
    return requirement ? { "X-Core-Client-Requirements": requirement.join("; ") } : {};
}
function allowedProtocolCount(selection, target, security) {
    if (selection.protocol === "all")
        return true;
    return (0, compatibility_1.supportsOutputCombination)(target, selection.protocol, security);
}
async function pageFor(env, request, provider, target, selection, limitOverride) {
    const actualSelection = selection ?? (0, planning_1.parseOutputSelection)(env, new URL(request.url).searchParams);
    return (0, planning_1.buildOutputNodePage)(env, new URL(request.url).hostname, effectiveSelection(actualSelection, limitOverride), (0, planning_1.createOutputCredentialSource)(provider), budget(env), request.signal, target);
}
function baseOptions(env, fragmentArtifact = false) {
    const earlyData = (0, planning_1.parseOutputEarlyData)(env) ?? 0;
    return {
        probeUrl: "https://www.gstatic.com/generate_204",
        probeInterval: fragmentArtifact
            ? env.CORE_XRAY_FRAGMENT_PROBE_INTERVAL?.trim() || "1s"
            : env.CORE_XRAY_PROBE_INTERVAL?.trim() || "30s",
        // Fragment artifacts expand every base TLS node across profiles. Default to serial, one-second
        // probes for this matrix to avoid a synchronized burst; operators may explicitly opt back in.
        observatoryConcurrency: fragmentArtifact
            ? parseBoolWithDefault(env.CORE_XRAY_FRAGMENT_OBSERVATORY_CONCURRENCY, false)
            : parseBoolWithDefault(env.CORE_XRAY_OBSERVATORY_CONCURRENCY, true),
        interval: "30s",
        tolerance: 50,
        fragmentProfiles: [...(0, profiles_1.getXrayFragmentProfiles)(env)],
        ...(earlyData > 0 ? { earlyData } : {}),
        // Legacy flag, still forwarded for compatibility; since audit F3 the sing-box adapter
        // emits uTLS whenever the node carries a selected fingerprint, flag or not.
        ...(parseBool(env.CORE_OUTPUT_SINGBOX_UTLS) ? { useUtls: true } : {}),
        ...(parseBool(env.CORE_OUTPUT_ENABLE_TUN) ? { enableTun: true } : {}),
        ...(parseBool(env.CORE_OUTPUT_XRAY_ALLOW_DIRECT_FALLBACK) ? { allowDirectFallback: true } : {}),
    };
}
function parseBoolWithDefault(raw, fallback) {
    const value = raw?.trim();
    if (!value)
        return fallback;
    if (/^(1|true|yes|on)$/i.test(value))
        return true;
    if (/^(0|false|no|off)$/i.test(value))
        return false;
    throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_XRAY_OBSERVATORY_CONCURRENCY must be a boolean");
}
function parseBool(raw) {
    return /^(1|true|yes|on)$/i.test(raw?.trim() ?? "");
}
function profileBaseLimit(env, kind, requestedLimit) {
    const outputBudget = (0, runtime_1.buildRuntimeConfig)(env).output;
    const maxNodes = outputBudget.maxNodes;
    const requestedBaseLimit = requestedLimit ?? outputBudget.defaultNodes;
    const variants = kind === "xray" ? (0, profiles_1.getXrayFragmentProfiles)(env).length : (0, profiles_1.getSingboxFragmentProfiles)().length;
    if (variants > maxNodes) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `CORE_OUTPUT_MAX_NODES=${maxNodes} is too small for ${variants} Fragment variants; increase it to at least ${variants}`);
    }
    // `limit` describes logical base nodes. Never replace a smaller user-requested limit with the
    // expansion budget; the number of rendered variants is base-node count × profile count.
    return Math.max(1, Math.min(requestedBaseLimit, Math.floor(maxNodes / Math.max(1, variants))));
}
function headerSet(page, returnedCount, kind, byteTruncated, profileCandidateCount, nextOffsetOverride) {
    const headers = {
        "X-Core-Output-Artifact": kind,
        "X-Core-Output-Count": String(returnedCount),
        "X-Core-Output-Limit": String(page.limit),
        "X-Core-Output-Offset": String(page.offset),
        "X-Core-Output-Total": String(profileCandidateCount ?? page.candidateCount),
        "X-Core-Output-Truncated": String(byteTruncated || page.hasMore),
    };
    if (page.limitClamped)
        headers["X-Core-Output-Limit-Clamped"] = "true";
    if (page.hasMore)
        headers["X-Core-Output-Next-Offset"] = String(nextOffsetOverride ?? page.nextOffset ?? (page.offset + page.items.length));
    return headers;
}
async function renderOutputArtifact(env, request, provider, format) {
    const url = new URL(request.url);
    const params = url.searchParams;
    const artifact = parseArtifact(params);
    const selection = (0, planning_1.parseOutputSelection)(env, params);
    const limits = budget(env);
    const target = format;
    const queryAuthUsed = Boolean(params.get("token"));
    const artifactAuth = queryAuthUsed ? "authorization-bearer" : undefined;
    if (artifact === "manifest") {
        const probePage = await pageFor(env, request, provider, target, selection, 1);
        const descriptors = [];
        descriptors.push(descriptor("nodes", "node", format, selection.protocol === "all" ? "mixed" : selection.protocol, artifactUrl(request, "node", format, "?offset=0&limit=1"), true, undefined, probePage.candidateCount, artifactAuth));
        descriptors.push(descriptor("bundle", "bundle", format, selection.protocol === "all" ? "mixed" : selection.protocol, artifactUrl(request, "bundle", format), true, undefined, undefined, artifactAuth));
        descriptors.push(descriptor("best-ping", "best-ping", format, "mixed", artifactUrl(request, "best-ping", format), true, undefined, undefined, artifactAuth));
        if ((0, profiles_1.isProfileSupported)(format, "best-fragment")) {
            descriptors.push(descriptor("best-fragment", "best-fragment", format, "mixed", artifactUrl(request, "best-fragment", format), true, undefined, undefined, artifactAuth));
            descriptors.push(descriptor("best-ping-fragment", "best-ping-fragment", format, "mixed", artifactUrl(request, "best-ping-fragment", format), true, undefined, undefined, artifactAuth));
        }
        else {
            const reason = "Current Mihomo/Clash configuration does not expose a portable TLS fragmentation field in its proxy TLS schema.";
            descriptors.push(descriptor("best-fragment", "best-fragment", format, "mixed", artifactUrl(request, "best-fragment", format), false, reason, undefined, artifactAuth));
            descriptors.push(descriptor("best-ping-fragment", "best-ping-fragment", format, "mixed", artifactUrl(request, "best-ping-fragment", format), false, reason, undefined, artifactAuth));
        }
        const manifest = {
            coreVersion: limits_1.CORE_VERSION,
            target: format,
            selection: {
                protocol: selection.protocol,
                ports: selection.ports,
                endpoint: selection.endpoint,
                fingerprints: selection.fingerprints,
                alpn: selection.alpn,
            },
            artifacts: descriptors,
        };
        const body = JSON.stringify(manifest, null, 2);
        return {
            body,
            contentType: "application/json; charset=utf-8",
            count: descriptors.length,
            kind: artifact,
            filename: `cloudflare-proxy-core-${formatLabel(format)}-manifest.json`,
            headers: {
                "X-Core-Output-Artifact": "manifest",
                "X-Core-Output-Total": String(probePage.candidateCount),
            },
        };
    }
    if (artifact === "node") {
        const page = await pageFor(env, request, provider, target, selection, 1);
        if (!page.items.length)
            throw new errors_1.CoreError("BAD_REQUEST", "Output node offset is outside the available candidate range");
        const adapter = adapters_1.outputAdapters[format];
        const fitted = fitNodes([page.items[0]], (nodes) => adapter.render(nodes, "node", baseOptions(env)), limits.maxBytes);
        const headers = headerSet(page, fitted.count, artifact, fitted.byteTruncated);
        return {
            body: fitted.body,
            contentType: format === "clash" ? "text/plain; charset=utf-8" : "application/json; charset=utf-8",
            count: 1,
            kind: artifact,
            filename: `node-${page.offset + 1}.${format === "clash" ? "yaml" : "json"}`,
            headers,
        };
    }
    if (artifact === "bundle") {
        const page = await pageFor(env, request, provider, target, selection);
        const adapter = adapters_1.outputAdapters[format];
        const fitted = fitNodes(page.items, (nodes) => adapter.render(nodes, "bundle", baseOptions(env)), limits.maxBytes);
        return {
            body: fitted.body,
            contentType: format === "clash" ? "text/plain; charset=utf-8" : "application/json; charset=utf-8",
            count: fitted.count,
            kind: artifact,
            filename: `cloudflare-proxy-core-${formatLabel(format)}-bundle.${format === "clash" ? "yaml" : "json"}`,
            headers: headerSet(page, fitted.count, artifact, fitted.byteTruncated),
        };
    }
    if (format === "clash" && (artifact === "best-fragment" || artifact === "best-ping-fragment")) {
        throw new errors_1.CoreError("UNSUPPORTED_TRANSPORT", "Mihomo/Clash does not expose the Worker fragment profiles in this output model");
    }
    if (artifact === "best-ping") {
        const page = await pageFor(env, request, provider, target, selection);
        const adapter = adapters_1.outputAdapters[format];
        const fitted = fitNodes(page.items, (nodes) => adapter.render(nodes, "best-ping", baseOptions(env)), limits.maxBytes);
        return {
            body: fitted.body,
            contentType: format === "clash" ? "text/plain; charset=utf-8" : "application/json; charset=utf-8",
            count: fitted.count,
            kind: artifact,
            filename: `best-ping.${format === "clash" ? "yaml" : "json"}`,
            headers: headerSet(page, fitted.count, artifact, fitted.byteTruncated),
        };
    }
    const baseLimit = profileBaseLimit(env, format === "xray" ? "xray" : "sing-box", selection.limit);
    const basePage = await pageFor(env, request, provider, target, selection, baseLimit);
    const tlsNodes = basePage.items.filter((node) => node.security === "tls" && allowedProtocolCount(selection, target, node.security));
    if (!tlsNodes.length)
        throw new errors_1.CoreError("BAD_REQUEST", `${artifact} requires at least one TLS-compatible node`);
    const adapter = adapters_1.outputAdapters[format];
    const options = baseOptions(env, true);
    if (format === "xray") {
        const profiles = (0, profiles_1.getXrayFragmentProfiles)(env);
        const expansionCount = tlsNodes.length * profiles.length;
        if (expansionCount > limits_1.CORE_LIMITS.maxFragmentExpansionCandidates)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Fragment expansion exceeds safety ceiling ${limits_1.CORE_LIMITS.maxFragmentExpansionCandidates}`);
        const fitted = fitNodes(tlsNodes, (nodes) => adapter.render(nodes, artifact, { ...options, fragmentProfiles: profiles }), limits.maxBytes, (nodes) => nodes.length * profiles.length);
        return {
            body: fitted.body,
            contentType: "application/json; charset=utf-8",
            count: fitted.count,
            kind: artifact,
            filename: `${artifact}.xray.json`,
            // Total counts only TLS candidates: fragment artifacts cannot render plaintext ones,
            // so candidateCount overstated the total whenever plaintext endpoints existed (F16).
            headers: { ...headerSet(basePage, fitted.count, artifact, fitted.byteTruncated, basePage.tlsCandidateCount * profiles.length, basePage.offset + Math.floor(fitted.count / profiles.length)), ...fragmentCompatibilityHeaders(format) },
        };
    }
    const profiles = (0, profiles_1.getSingboxFragmentProfiles)();
    const expansionCount = tlsNodes.length * profiles.length;
    if (expansionCount > limits_1.CORE_LIMITS.maxFragmentExpansionCandidates)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Fragment expansion exceeds safety ceiling ${limits_1.CORE_LIMITS.maxFragmentExpansionCandidates}`);
    const fitted = fitNodes(tlsNodes, (nodes) => adapter.render(nodes, artifact, { ...options, fragmentProfiles: profiles }), limits.maxBytes, (nodes) => nodes.length * profiles.length);
    return {
        body: fitted.body,
        contentType: "application/json; charset=utf-8",
        count: fitted.count,
        kind: artifact,
        filename: `${artifact}.sing-box.json`,
        headers: { ...headerSet(basePage, fitted.count, artifact, fitted.byteTruncated, basePage.tlsCandidateCount * profiles.length, basePage.offset + Math.floor(fitted.count / profiles.length)), ...fragmentCompatibilityHeaders(format) },
    };
}

};

__m["output/compatibility"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.supportsOutputCombination = supportsOutputCombination;
exports.outputTargetLabel = outputTargetLabel;
/**
 * Compatibility describes the generated client-core contract, not the Worker wire capability.
 * The canonical Worker model supports HTTP/none for both VLESS and Trojan.
 * Xray's current public-network rules require outer transport security for both; Mihomo requires TLS for Trojan.
 */
function supportsOutputCombination(target, protocol, security) {
    if (target === "native")
        return true;
    if (target === "xray")
        return security === "tls";
    if (target === "clash")
        return protocol === "vless" || security === "tls";
    // Current sing-box outbound schemas model TLS as optional for these transports.
    return true;
}
function outputTargetLabel(target) {
    switch (target) {
        case "native": return "native share links";
        case "xray": return "Xray public configuration";
        case "sing-box": return "Sing-box configuration";
        case "clash": return "Mihomo/Clash configuration";
    }
}

};

__m["output/profiles"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getXrayFragmentProfiles = getXrayFragmentProfiles;
exports.getSingboxFragmentProfiles = getSingboxFragmentProfiles;
exports.isProfileSupported = isProfileSupported;
exports.expandXrayFragments = expandXrayFragments;
exports.expandSingboxFragments = expandSingboxFragments;
const errors_1 = __r("../core/errors");
const DEFAULT_XRAY_FRAGMENTS = [
    { id: "f01", packets: "1-3", lengths: ["1-5"], delays: ["1-1"] },
    { id: "f02", packets: "1-3", lengths: ["1-10"], delays: ["1-1"] },
    { id: "f03", packets: "1-3", lengths: ["10-20"], delays: ["1-1"] },
    { id: "f04", packets: "1-3", lengths: ["20-30"], delays: ["1-1"] },
    { id: "f05", packets: "1-3", lengths: ["30-40"], delays: ["1-1"] },
    { id: "f06", packets: "1-3", lengths: ["40-50"], delays: ["1-1"] },
    { id: "f07", packets: "1-3", lengths: ["50-60"], delays: ["1-1"] },
    { id: "f08", packets: "1-3", lengths: ["60-70"], delays: ["1-1"] },
    { id: "f09", packets: "1-3", lengths: ["70-80"], delays: ["1-1"] },
    { id: "f10", packets: "1-3", lengths: ["80-90"], delays: ["1-1"] },
    { id: "f11", packets: "1-3", lengths: ["90-100"], delays: ["1-1"] },
    { id: "f12", packets: "1-3", lengths: ["10-30"], delays: ["1-1"] },
    { id: "f13", packets: "1-3", lengths: ["20-40"], delays: ["1-1"] },
    { id: "f14", packets: "1-3", lengths: ["30-50"], delays: ["1-1"] },
    { id: "f15", packets: "1-3", lengths: ["40-60"], delays: ["1-1"] },
    { id: "f16", packets: "1-3", lengths: ["50-70"], delays: ["1-1"] },
    { id: "f17", packets: "1-3", lengths: ["60-80"], delays: ["1-1"] },
    { id: "f18", packets: "1-3", lengths: ["70-90"], delays: ["1-1"] },
    { id: "f19", packets: "1-3", lengths: ["80-100"], delays: ["1-1"] },
    { id: "f20", packets: "tlshello", lengths: ["100-200"], delays: ["1-1"] },
];
const DEFAULT_SINGBOX_FRAGMENTS = [
    { id: "fragment", fragment: true },
    { id: "record", recordFragment: true },
];
const MAX_FRAGMENT_DELAY_MS = 60_000;
const MAX_FRAGMENT_SPLITS = 64;
function rangeBounds(value) {
    const match = value.match(/^(\d+)(?:-(\d+))?$/);
    if (!match)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Internal fragment range parsing failure");
    const first = Number(match[1]);
    const second = match[2] === undefined ? first : Number(match[2]);
    return [first, second];
}
function parseRange(value, field, bounds = {}) {
    if (typeof value !== "string" || !value.trim())
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be a non-empty range`);
    const normalized = value.trim();
    if (!/^(\d+)(?:-(\d+))?$/.test(normalized))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be an integer or integer range`);
    const [first, second] = rangeBounds(normalized);
    const min = bounds.min ?? 0;
    const max = bounds.max ?? 2_147_483_647;
    if (!Number.isSafeInteger(first) || !Number.isSafeInteger(second) || first < min || second < first || second > max) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must be within the supported range ${min}..${max}`);
    }
    return normalized;
}
function parseRangeList(value, field, bounds = {}, maxItems = 16) {
    const list = Array.isArray(value) ? value : typeof value === "string" ? value.split(/[|,]+/) : [];
    if (!list.length || list.length > maxItems)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} must contain 1..${maxItems} ranges`);
    return list.map((item) => parseRange(item, field, bounds));
}
function normalizeXrayProfile(value, index) {
    if (!value || typeof value !== "object")
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `CORE_FRAGMENT_PROFILES[${index}] must be an object`);
    const raw = value;
    const id = typeof raw.id === "string" && raw.id.trim() ? raw.id.trim().replace(/[^A-Za-z0-9_-]/g, "-").slice(0, 32) : `f${index + 1}`;
    const packets = raw.packets === "tlshello" ? "tlshello" : raw.packets === "1-3" ? "1-3" : null;
    if (!packets)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `CORE_FRAGMENT_PROFILES[${index}].packets must be 1-3 or tlshello`);
    const field = `CORE_FRAGMENT_PROFILES[${index}]`;
    const lengths = parseRangeList(raw.lengths, `${field}.lengths`, packets === "tlshello" ? { min: 1 } : {});
    if (packets === "1-3" && rangeBounds(lengths[lengths.length - 1])[0] === 0) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field}.lengths: the final range must be positive; a final zero can idle indefinitely`);
    }
    return {
        id,
        packets,
        lengths,
        // A zero delay is valid in FinalMask; cap the upper bound to avoid pathological stalls.
        delays: parseRangeList(raw.delays, `${field}.delays`, { min: 0, max: MAX_FRAGMENT_DELAY_MS }),
        // FinalMask defines maxSplit=0 as unlimited. This generator intentionally requires a finite
        // positive bound so custom profiles cannot opt into unbounded splitting by mistake.
        ...(raw.maxSplit === undefined || raw.maxSplit === null || raw.maxSplit === "" ? {} : { maxSplit: parseRange(raw.maxSplit, `${field}.maxSplit`, { min: 1, max: MAX_FRAGMENT_SPLITS }) }),
    };
}
function parseProfileJson(raw) {
    try {
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed))
            throw new Error("must be an array");
        return parsed.map(normalizeXrayProfile);
    }
    catch (error) {
        if (error instanceof errors_1.CoreError)
            throw error;
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `CORE_FRAGMENT_PROFILES contains invalid JSON: ${error instanceof Error ? error.message : "invalid JSON"}`);
    }
}
function getXrayFragmentProfiles(env) {
    const raw = env.CORE_FRAGMENT_PROFILES?.trim();
    if (!raw)
        return DEFAULT_XRAY_FRAGMENTS;
    if (raw.startsWith("[")) {
        const profiles = parseProfileJson(raw) ?? [];
        if (!profiles.length)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_FRAGMENT_PROFILES must contain at least one profile");
        if (profiles.length > 32)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_FRAGMENT_PROFILES supports at most 32 profiles");
        ensureUniqueProfileIds(profiles);
        return profiles;
    }
    // Compact text form:
    // id|packets|lengths|delays|[maxSplit];id|packets|lengths|delays|[maxSplit]
    const profiles = raw.split(";").map((chunk, index) => {
        const parts = chunk.split("|").map((item) => item.trim());
        if (parts.length < 4 || parts.length > 5)
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `CORE_FRAGMENT_PROFILES entry ${index + 1} must be id|packets|lengths|delays|[maxSplit]`);
        return normalizeXrayProfile({ id: parts[0], packets: parts[1], lengths: parts[2], delays: parts[3], ...(parts[4] ? { maxSplit: parts[4] } : {}) }, index);
    }).filter(Boolean);
    if (!profiles.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_FRAGMENT_PROFILES must contain at least one profile");
    if (profiles.length > 32)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_FRAGMENT_PROFILES supports at most 32 profiles");
    ensureUniqueProfileIds(profiles);
    return profiles;
}
function ensureUniqueProfileIds(profiles) {
    const seen = new Set();
    for (const profile of profiles) {
        if (seen.has(profile.id))
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Duplicate fragment profile id after normalization: ${profile.id}`);
        seen.add(profile.id);
    }
}
function getSingboxFragmentProfiles() {
    return DEFAULT_SINGBOX_FRAGMENTS;
}
function isProfileSupported(target, kind) {
    if (kind === "best-ping")
        return true;
    if (target === "clash")
        return false;
    return kind === "best-fragment" || kind === "best-ping-fragment";
}
function expandXrayFragments(nodes, profiles) {
    const out = [];
    for (const node of nodes) {
        if (node.security !== "tls")
            continue;
        for (const profile of profiles) {
            out.push({ node: { ...node, name: `${node.name}-${profile.id}` }, profile });
        }
    }
    return out;
}
function expandSingboxFragments(nodes, profiles) {
    const out = [];
    for (const node of nodes) {
        if (node.security !== "tls")
            continue;
        for (const profile of profiles)
            out.push({ node: { ...node, name: `${node.name}-${profile.id}` }, profile });
    }
    return out;
}

};

__m["output/adapters"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.outputAdapters = void 0;
const clash_1 = __r("./clash");
const singbox_1 = __r("./singbox");
const xray_1 = __r("./xray");
function xrayOptions(options) {
    return {
        ...(typeof options?.probeUrl === "string" ? { probeUrl: options.probeUrl } : {}),
        ...(typeof options?.probeInterval === "string" ? { probeInterval: options.probeInterval } : {}),
        ...(typeof options?.observatoryConcurrency === "boolean" ? { observatoryConcurrency: options.observatoryConcurrency } : {}),
        ...(typeof options?.fragmentProfile === "object" && options.fragmentProfile !== null ? { fragmentProfile: options.fragmentProfile } : {}),
        ...(options?.allowDirectFallback === true ? { allowDirectFallback: true } : {}),
    };
}
function singboxOptions(options) {
    return {
        ...(typeof options?.probeUrl === "string" ? { probeUrl: options.probeUrl } : {}),
        ...(typeof options?.interval === "string" ? { interval: options.interval } : {}),
        ...(typeof options?.tolerance === "number" ? { tolerance: options.tolerance } : {}),
        ...(typeof options?.fragmentProfile === "object" && options.fragmentProfile !== null ? { fragmentProfile: options.fragmentProfile } : {}),
        ...(options?.useUtls === true ? { useUtls: true } : {}),
    };
}
function intervalSeconds(value) {
    // Shared base options carry the interval as a sing-box duration string ("30s"), which the
    // number-only check here used to drop silently for Mihomo (audit F16). Parse both forms.
    if (typeof value === "number" && Number.isFinite(value) && value > 0)
        return value;
    if (typeof value === "string") {
        const match = value.trim().match(/^(\d+)s?$/);
        if (match)
            return Number(match[1]);
    }
    return undefined;
}
function mihomoOptions(options) {
    const interval = intervalSeconds(options?.interval);
    return {
        ...(typeof options?.probeUrl === "string" ? { probeUrl: options.probeUrl } : {}),
        ...(interval !== undefined ? { interval } : {}),
        ...(typeof options?.tolerance === "number" ? { tolerance: options.tolerance } : {}),
    };
}
exports.outputAdapters = {
    xray: {
        format: "xray",
        render: (nodes, kind = "bundle", options) => {
            const x = xrayOptions(options);
            if (kind === "best-ping")
                return JSON.stringify((0, xray_1.buildXrayBestPingConfig)(nodes, x), null, 2);
            if (kind === "best-fragment")
                return JSON.stringify((0, xray_1.buildXrayBestFragmentConfig)(nodes, options?.fragmentProfiles ?? [], x), null, 2);
            if (kind === "best-ping-fragment")
                return JSON.stringify((0, xray_1.buildXrayBestPingFragmentConfig)(nodes, options?.fragmentProfiles ?? [], x), null, 2);
            return JSON.stringify((0, xray_1.buildXrayConfig)(nodes, { ...x, strategy: "roundRobin" }), null, 2);
        },
    },
    "sing-box": {
        format: "sing-box",
        render: (nodes, kind = "bundle", options) => {
            const s = singboxOptions(options);
            if (kind === "best-ping")
                return JSON.stringify((0, singbox_1.buildSingboxBestPingConfig)(nodes, s), null, 2);
            if (kind === "best-fragment")
                return JSON.stringify((0, singbox_1.buildSingboxBestFragmentConfig)(nodes, options?.fragmentProfiles ?? [], s), null, 2);
            if (kind === "best-ping-fragment")
                return JSON.stringify((0, singbox_1.buildSingboxBestPingFragmentConfig)(nodes, options?.fragmentProfiles ?? [], s), null, 2);
            return JSON.stringify((0, singbox_1.buildSingboxConfig)(nodes, s), null, 2);
        },
    },
    clash: {
        format: "clash",
        render: (nodes, kind = "bundle", options) => {
            const m = mihomoOptions(options);
            return kind === "best-ping" ? (0, clash_1.buildClashBestPingConfig)(nodes, m) : (0, clash_1.buildClashConfig)(nodes, m);
        },
    },
};

};

__m["output/xray/index"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildXrayConfig = buildXrayConfig;
exports.buildXrayBestPingConfig = buildXrayBestPingConfig;
exports.buildXrayBestFragmentConfig = buildXrayBestFragmentConfig;
exports.buildXrayBestPingFragmentConfig = buildXrayBestPingFragmentConfig;
const errors_1 = __r("../../core/errors");
const share_link_1 = __r("../share-link");
function validateNode(node) {
    if (node.security === "none") {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Xray public output requires transport TLS; use the Worker/native model or another compatible target for plaintext nodes");
    }
}
function stream(node, fragmentProfile) {
    validateNode(node);
    const streamSettings = {
        method: "websocket",
        network: "ws",
        security: "tls",
        tlsSettings: {
            serverName: node.tls?.serverName || node.server,
            ...(node.tls?.alpn?.length ? { alpn: node.tls.alpn } : {}),
            ...(node.tls?.fingerprint ? { fingerprint: node.tls.fingerprint } : {}),
        },
        wsSettings: {
            path: (0, share_link_1.effectiveWebSocketPath)(node.websocket.path, node.websocket.earlyData),
            ...(node.websocket.host ? { host: node.websocket.host } : {}),
        },
    };
    if (fragmentProfile) {
        streamSettings.finalmask = {
            tcp: [{
                    type: "fragment",
                    settings: {
                        packets: fragmentProfile.packets,
                        lengths: [...fragmentProfile.lengths],
                        delays: [...fragmentProfile.delays],
                        ...(fragmentProfile.lengths[0] ? { length: fragmentProfile.lengths[0] } : {}),
                        ...(fragmentProfile.delays[0] ? { delay: fragmentProfile.delays[0] } : {}),
                        ...(fragmentProfile.maxSplit ? { maxSplit: fragmentProfile.maxSplit } : {}),
                    },
                }],
        };
    }
    return streamSettings;
}
function outbound(node, index, fragmentProfile) {
    validateNode(node);
    const tag = `proxy-${index}`;
    if (node.protocol === "vless") {
        return {
            tag,
            protocol: "vless",
            settings: {
                vnext: [{
                        address: node.server,
                        port: node.port,
                        users: [{ id: node.credential, encryption: "none" }],
                    }],
                packetEncoding: "packetaddr",
            },
            streamSettings: stream(node, fragmentProfile),
        };
    }
    return {
        tag,
        protocol: "trojan",
        settings: {
            servers: [{ address: node.server, port: node.port, password: node.credential }],
        },
        streamSettings: stream(node, fragmentProfile),
    };
}
function configWithOutbounds(nodes, options, prebuiltOutbounds) {
    if (!nodes.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Xray output requires at least one TLS-compatible node");
    for (const node of nodes)
        validateNode(node);
    const outbounds = prebuiltOutbounds ? [...prebuiltOutbounds] : nodes.map((node, index) => outbound(node, index + 1, options.fragmentProfile));
    const proxyTags = outbounds.map((item) => String(item.tag));
    const strategy = proxyTags.length <= 1 ? "manual" : (options.strategy ?? "roundRobin");
    const finalTag = proxyTags[0];
    // Observatory exists to feed leastPing/leastLoad balancers. A roundRobin balancer ignores
    // probe results, so probing every outbound every interval was pure overhead (audit F14).
    const observatory = proxyTags.length > 1 && strategy === "leastPing"
        ? {
            subjectSelector: ["proxy-"],
            probeUrl: options.probeUrl ?? "https://www.gstatic.com/generate_204",
            probeInterval: options.probeInterval ?? "30s",
            enableConcurrency: options.observatoryConcurrency ?? true,
        }
        : undefined;
    const routing = strategy === "manual" && nodes.length === 1
        ? { domainStrategy: "AsIs", rules: [{ type: "field", inboundTag: ["socks-in"], outboundTag: finalTag }] }
        : {
            domainStrategy: "AsIs",
            rules: [{ type: "field", inboundTag: ["socks-in"], balancerTag: "proxy-balancer" }],
            balancers: [{
                    tag: "proxy-balancer",
                    selector: ["proxy-"],
                    strategy: { type: strategy === "leastPing" ? "leastPing" : "roundRobin" },
                    ...(options.allowDirectFallback ? { fallbackTag: "direct" } : {}),
                }],
        };
    return {
        log: { loglevel: "warning" },
        dns: {
            servers: [
                "https://cloudflare-dns.com/dns-query",
                "https://dns.google/dns-query",
            ],
            disableFallback: true,
        },
        ...(observatory ? { observatory } : {}),
        inbounds: [{
                tag: "socks-in",
                listen: options.listen ?? "127.0.0.1",
                port: options.socksPort ?? 10808,
                protocol: "socks",
                // Trojan supports UDP association in Xray just as VLESS does; gating this on the
                // presence of a VLESS node silently disabled all UDP for Trojan-only bundles (audit F2).
                settings: { auth: "noauth", udp: true },
                sniffing: { enabled: false },
            }],
        outbounds: [
            ...outbounds,
            ...(options.allowDirectFallback ? [{ tag: "direct", protocol: "freedom" }] : []),
        ],
        routing,
    };
}
function buildXrayConfig(nodes, options = {}) {
    return configWithOutbounds(nodes, options);
}
function buildXrayBestPingConfig(nodes, options = {}) {
    return configWithOutbounds(nodes, { ...options, strategy: "leastPing" });
}
function buildXrayFragmentConfig(nodes, profiles, strategy, options = {}) {
    if (!profiles.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Fragment output requires at least one fragment profile");
    const expanded = [];
    for (const node of nodes) {
        if (node.security !== "tls")
            continue;
        for (const profile of profiles)
            expanded.push({ node: { ...node, name: `${node.name}-${profile.id}` }, profile });
    }
    if (!expanded.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Fragment output requires at least one TLS-compatible node");
    const outbounds = expanded.map((item, index) => outbound(item.node, index + 1, item.profile));
    return configWithOutbounds(expanded.map((item) => item.node), { ...options, strategy }, outbounds);
}
function buildXrayBestFragmentConfig(nodes, profiles, options = {}) {
    return buildXrayFragmentConfig(nodes, profiles, "leastPing", options);
}
function buildXrayBestPingFragmentConfig(nodes, profiles, options = {}) {
    return buildXrayFragmentConfig(nodes, profiles, "leastPing", options);
}

};

__m["output/share-link"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.generateShareLink = generateShareLink;
exports.webSocketPathWithoutEarlyData = webSocketPathWithoutEarlyData;
exports.effectiveWebSocketPath = effectiveWebSocketPath;
function appendParams(params) {
    return params.toString().replace(/%2C/gi, ",");
}
function generateShareLink(node, options = {}) {
    const params = new URLSearchParams();
    params.set("type", "ws");
    if (node.websocket.host)
        params.set("host", node.websocket.host);
    params.set("path", effectiveWebSocketPath(node.websocket.path, node.websocket.earlyData));
    if (node.security === "tls") {
        params.set("security", "tls");
        const tls = node.tls;
        if (tls?.serverName)
            params.set("sni", tls.serverName);
        if (tls?.fingerprint)
            params.set("fp", tls.fingerprint);
        if (tls?.alpn?.length)
            params.set("alpn", tls.alpn.join(","));
    }
    else {
        params.set("security", "none");
    }
    const fragment = options.includeFragment && node.name ? `#${encodeURIComponent(node.name)}` : "";
    if (node.protocol === "vless") {
        params.set("encryption", "none");
        return `vless://${encodeURIComponent(node.credential)}@${formatServer(node.server)}:${node.port}?${appendParams(params)}${fragment}`;
    }
    return `trojan://${encodeURIComponent(node.credential)}@${formatServer(node.server)}:${node.port}?${appendParams(params)}${fragment}`;
}
/**
 * Path for cores that carry early data natively (sing-box max_early_data, Mihomo
 * ws-opts.max-early-data): remove only the `ed=<n>` query parameter and preserve any
 * other query components the operator configured (audit F8). Xray/share-links keep the
 * full path including `ed`, per the Xray convention.
 */
function webSocketPathWithoutEarlyData(path) {
    const queryIndex = path.indexOf("?");
    if (queryIndex < 0)
        return path;
    const base = path.slice(0, queryIndex);
    const kept = path.slice(queryIndex + 1).split("&").filter((pair) => pair && !/^ed=\d+$/.test(pair));
    return kept.length ? `${base}?${kept.join("&")}` : base;
}
function effectiveWebSocketPath(path, earlyData) {
    if (!earlyData || !Number.isInteger(earlyData) || earlyData <= 0)
        return path;
    const separator = path.includes("?") ? "&" : "?";
    return /(?:^|[?&])ed=\d+(?:&|$)/.test(path) ? path : `${path}${separator}ed=${earlyData}`;
}
function formatServer(server) {
    return server.includes(":") && !server.startsWith("[") ? `[${server}]` : server;
}

};

__m["output/singbox/index"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildSingboxConfig = buildSingboxConfig;
exports.buildSingboxBestPingConfig = buildSingboxBestPingConfig;
exports.buildSingboxBestFragmentConfig = buildSingboxBestFragmentConfig;
exports.buildSingboxBestPingFragmentConfig = buildSingboxBestPingFragmentConfig;
const errors_1 = __r("../../core/errors");
const share_link_1 = __r("../share-link");
function outbound(node, index, fragmentProfile, _useUtls = false) {
    // _useUtls is retained for call-site compatibility only. The selected TLS fingerprint is
    // a first-class output selection: sing-box previously emitted uTLS only when a separate
    // opt-in flag was set, silently dropping the fingerprint Xray/Mihomo always emit (audit F3).
    const base = {
        type: node.protocol,
        tag: `proxy-${index}`,
        server: node.server,
        server_port: node.port,
    };
    if (node.protocol === "vless")
        base.uuid = node.credential;
    else
        base.password = node.credential;
    if (node.protocol === "vless")
        base.packet_encoding = "packetaddr";
    if (node.security === "tls") {
        base.tls = {
            enabled: true,
            server_name: node.tls?.serverName || node.server,
            ...(node.tls?.alpn?.length ? { alpn: node.tls.alpn } : {}),
            ...(node.tls?.fingerprint ? { utls: { enabled: true, fingerprint: node.tls.fingerprint === "randomized" ? "random" : node.tls.fingerprint } } : {}),
            ...(fragmentProfile?.fragment ? { fragment: true } : {}),
            ...(fragmentProfile?.recordFragment ? { record_fragment: true } : {}),
        };
    }
    base.transport = {
        type: "ws",
        path: (0, share_link_1.webSocketPathWithoutEarlyData)(node.websocket.path),
        headers: node.websocket.host ? { Host: node.websocket.host } : {},
        ...(node.websocket.earlyData ? { max_early_data: node.websocket.earlyData, early_data_header_name: "Sec-WebSocket-Protocol" } : {}),
    };
    return base;
}
function baseConfig(outbounds, proxyTags, options) {
    if (!proxyTags.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Sing-box output requires at least one node");
    const strategy = options.strategy ?? (proxyTags.length === 1 ? "manual" : "manual");
    const routing = strategy === "manual" && proxyTags.length === 1
        ? { final: proxyTags[0] }
        : { final: "proxy" };
    const group = strategy === "urltest"
        ? {
            type: "urltest",
            tag: "proxy",
            outbounds: [...proxyTags],
            url: options.probeUrl ?? "https://www.gstatic.com/generate_204",
            interval: options.interval ?? "30s",
            tolerance: options.tolerance ?? 50,
        }
        : {
            type: "selector",
            tag: "proxy",
            outbounds: [...proxyTags],
            default: proxyTags[0],
        };
    // sing-box has no `udp` toggle on the SOCKS inbound (UDP association is inherent to its
    // SOCKS5 implementation); the field is not in the sing-box schema and risks rejection by
    // strict validation (audit F12).
    const inbounds = [{
            type: "socks",
            tag: "socks-in",
            listen: "127.0.0.1",
            listen_port: options.socksPort ?? 2080,
        }];
    if (options.enableTun) {
        inbounds.push({
            type: "tun",
            tag: "tun-in",
            address: ["172.19.0.1/30", "fdfe:dcba:9876::1/126"],
            auto_route: true,
            auto_detect_interface: true,
        });
    }
    return {
        log: { level: "warn" },
        dns: {
            servers: [
                { type: "https", tag: "doh-cf", server: "1.1.1.1", server_port: 443, path: "/dns-query", tls: { enabled: true, server_name: "cloudflare-dns.com" } },
                { type: "https", tag: "doh-google", server: "8.8.8.8", server_port: 443, path: "/dns-query", tls: { enabled: true, server_name: "dns.google" } },
            ],
            final: "doh-cf",
        },
        inbounds,
        outbounds: [
            ...outbounds,
            group,
            { type: "direct", tag: "direct" },
        ],
        route: {
            ...routing,
            auto_detect_interface: true,
        },
    };
}
function buildSingboxConfig(nodes, socksPortOrOptions = 2080) {
    const options = typeof socksPortOrOptions === "number" ? { socksPort: socksPortOrOptions } : socksPortOrOptions;
    const outbounds = nodes.map((node, index) => outbound(node, index + 1, options.fragmentProfile, options.useUtls));
    return baseConfig(outbounds, outbounds.map((item) => String(item.tag)), options);
}
function buildSingboxBestPingConfig(nodes, options = {}) {
    const outbounds = nodes.map((node, index) => outbound(node, index + 1, options.fragmentProfile, options.useUtls));
    return baseConfig(outbounds, outbounds.map((item) => String(item.tag)), { ...options, strategy: "urltest" });
}
function buildSingboxBestFragmentConfig(nodes, profiles, options = {}) {
    const expanded = [];
    for (const node of nodes) {
        if (node.security !== "tls")
            continue;
        for (const profile of profiles)
            expanded.push({ node: { ...node, name: `${node.name}-${profile.id}` }, profile });
    }
    if (!expanded.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Best Fragment requires at least one TLS-compatible node");
    const outbounds = expanded.map((item, index) => outbound(item.node, index + 1, item.profile, options.useUtls));
    // "Best Fragment" must actually select: a manual selector never tests the fragment
    // variants, diverging from the Xray leastPing semantics of the same artifact (audit F4).
    // urltest over the expanded variants mirrors Best Ping Fragment, as in Xray's alias pair.
    return baseConfig(outbounds, outbounds.map((item) => String(item.tag)), { ...options, strategy: "urltest" });
}
function buildSingboxBestPingFragmentConfig(nodes, profiles, options = {}) {
    const expanded = [];
    for (const node of nodes) {
        if (node.security !== "tls")
            continue;
        for (const profile of profiles)
            expanded.push({ node: { ...node, name: `${node.name}-${profile.id}` }, profile });
    }
    if (!expanded.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Best Ping Fragment requires at least one TLS-compatible node");
    const outbounds = expanded.map((item, index) => outbound(item.node, index + 1, item.profile, options.useUtls));
    return baseConfig(outbounds, outbounds.map((item) => String(item.tag)), { ...options, strategy: "urltest" });
}

};

__m["output/clash/index"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildClashConfig = buildClashConfig;
exports.buildClashBestPingConfig = buildClashBestPingConfig;
const share_link_1 = __r("../share-link");
const errors_1 = __r("../../core/errors");
function yamlString(value) {
    // JSON double-quoted strings are valid YAML double-quoted scalars and correctly escape control characters.
    return JSON.stringify(value);
}
function nodeYaml(node) {
    const lines = [
        `  - name: ${yamlString(node.name)}`,
        `    type: ${node.protocol}`,
        `    server: ${yamlString(node.server)}`,
        `    port: ${node.port}`,
    ];
    if (node.protocol === "vless") {
        lines.push(`    uuid: ${yamlString(node.credential)}`);
    }
    else {
        if (node.security !== "tls")
            throw new errors_1.CoreError("CONFIGURATION_ERROR", "Mihomo Trojan output requires TLS for public configurations");
        lines.push(`    password: ${yamlString(node.credential)}`);
    }
    // Mihomo supports UDP for Trojan as well as VLESS; emitting udp:false for Trojan silently
    // disabled UDP for Trojan-only bundles (audit F2).
    lines.push("    udp: true");
    if (node.security === "tls") {
        lines.push("    tls: true");
        const tlsServerName = yamlString(node.tls?.serverName || node.server);
        lines.push(`    ${node.protocol === "trojan" ? "sni" : "servername"}: ${tlsServerName}`);
        if (node.tls?.alpn?.length) {
            lines.push("    alpn:");
            for (const value of node.tls.alpn)
                lines.push(`      - ${yamlString(value)}`);
        }
        if (node.tls?.fingerprint)
            lines.push(`    client-fingerprint: ${yamlString(node.tls.fingerprint === "randomized" ? "random" : node.tls.fingerprint)}`);
    }
    lines.push("    network: ws", "    ws-opts:");
    lines.push(`      path: ${yamlString((0, share_link_1.webSocketPathWithoutEarlyData)(node.websocket.path))}`);
    if (node.websocket.earlyData) {
        lines.push(`      max-early-data: ${node.websocket.earlyData}`);
        lines.push("      early-data-header-name: Sec-WebSocket-Protocol");
    }
    if (node.websocket.host)
        lines.push("      headers:", `        Host: ${yamlString(node.websocket.host)}`);
    return lines;
}
function baseYaml(nodes, groupType, groupUrl, interval, tolerance, mixedPort = 7890, optionsEnableTunPlaceholder = false) {
    if (!nodes.length)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Mihomo output requires at least one node");
    const lines = [
        `mixed-port: ${mixedPort}`,
        "allow-lan: false",
        "mode: rule",
        "proxies:",
    ];
    for (const node of nodes)
        lines.push(...nodeYaml(node));
    lines.push("proxy-groups:", "  - name: PROXY", `    type: ${groupType}`, "    proxies:");
    for (const node of nodes)
        lines.push(`      - ${yamlString(node.name)}`);
    if (groupType === "url-test") {
        lines.push(`    url: ${yamlString(groupUrl ?? "https://www.gstatic.com/generate_204")}`);
        lines.push(`    interval: ${interval ?? 30}`);
        lines.push(`    tolerance: ${tolerance ?? 50}`);
    }
    lines.push("dns:", "  enable: true", "  ipv6: true", "  enhanced-mode: fake-ip", "  nameserver:", "    - https://cloudflare-dns.com/dns-query", "    - https://dns.google/dns-query", "  fallback:", "    - tls://1.1.1.1:853", "    - tls://8.8.8.8:853", "  respect-rules: true");
    if (optionsEnableTunPlaceholder) {
        lines.push("tun:", "  enable: true", "  stack: system", "  auto-route: true", "  auto-detect-interface: true", "  dns-hijack:", "    - any:53");
    }
    lines.push("rules:", "  - MATCH,PROXY", "");
    return lines.join("\n");
}
function buildClashConfig(nodes, options = 7890) {
    const config = typeof options === "number" ? { mixedPort: options } : options;
    return baseYaml(nodes, config.groupStrategy ?? "select", config.probeUrl, config.interval, config.tolerance, config.mixedPort ?? 7890, config.enableTun ?? false);
}
function buildClashBestPingConfig(nodes, options = {}) {
    return baseYaml(nodes, "url-test", options.probeUrl, options.interval, options.tolerance, options.mixedPort ?? 7890, options.enableTun ?? false);
}

};

__m["output/planning"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SUPPORTED_TLS_FINGERPRINTS = void 0;
exports.parseList = parseList;
exports.parseFingerprints = parseFingerprints;
exports.parseAlpn = parseAlpn;
exports.parsePorts = parsePorts;
exports.parseOutputSelection = parseOutputSelection;
exports.parseOutputEarlyDataValue = parseOutputEarlyDataValue;
exports.parseOutputEarlyData = parseOutputEarlyData;
exports.buildOutputNodePage = buildOutputNodePage;
exports.buildOutputNodesFromRecords = buildOutputNodesFromRecords;
exports.listCredentials = listCredentials;
exports.createOutputCredentialSource = createOutputCredentialSource;
exports.generateNativeUris = generateNativeUris;
exports.generateNativeUrisFromRecords = generateNativeUrisFromRecords;
exports.outputSummary = outputSummary;
const limits_1 = __r("../core/limits");
const errors_1 = __r("../core/errors");
const runtime_1 = __r("../config/runtime");
const endpoints_1 = __r("./endpoints");
const share_link_1 = __r("./share-link");
const types_1 = __r("./types");
Object.defineProperty(exports, "SUPPORTED_TLS_FINGERPRINTS", { enumerable: true, get: function () { return types_1.SUPPORTED_TLS_FINGERPRINTS; } });
const compatibility_1 = __r("./compatibility");
const ALLOWED_QUERY_PARAMETERS = new Set([
    "protocol",
    "endpoint",
    "port",
    "transport",
    "fingerprint",
    "alpn",
    "remark",
    "name",
    "encode",
    "token",
    "aggregate",
    "limit",
    "offset",
    "artifact",
]);
const MAX_ENDPOINT_INPUTS = limits_1.CORE_LIMITS.maxOutputEndpointInputs;
const MAX_OFFSET = Number.MAX_SAFE_INTEGER;
function parseList(raw) {
    return (raw ?? "").split(/[\r\n,]+/).map((item) => item.trim()).filter(Boolean);
}
function parseFingerprints(raw) {
    const values = parseList(raw).map((value) => value.toLowerCase());
    if (!values.length)
        return ["chrome"];
    if (values.length === 1 && values[0] === "all")
        return [...types_1.SUPPORTED_TLS_FINGERPRINTS];
    const out = [];
    const seen = new Set();
    for (const value of values) {
        if (!types_1.SUPPORTED_TLS_FINGERPRINTS.includes(value)) {
            throw new errors_1.CoreError("BAD_REQUEST", `Unsupported TLS fingerprint: ${value}`);
        }
        if (!seen.has(value)) {
            seen.add(value);
            out.push(value);
        }
    }
    return out;
}
function parseAlpn(raw) {
    const values = parseList(raw);
    if (!values.length)
        return ["http/1.1"];
    const out = [];
    const seen = new Set();
    for (const value of values) {
        const bytes = new TextEncoder().encode(value);
        if (!bytes.length || bytes.length > 255 || /[\u0000-\u0020\u007f]/.test(value)) {
            throw new errors_1.CoreError("BAD_REQUEST", `Invalid ALPN identifier: ${value}`);
        }
        const normalized = value.toLowerCase();
        if (normalized !== "http/1.1") {
            throw new errors_1.CoreError("BAD_REQUEST", `WebSocket output supports only ALPN http/1.1; unsupported ALPN: ${value}`);
        }
        if (!seen.has(normalized)) {
            seen.add(normalized);
            out.push(normalized);
        }
    }
    return out;
}
function parsePorts(raw) {
    if (!raw?.trim())
        return null;
    const out = [];
    const seen = new Set();
    for (const value of parseList(raw)) {
        if (!/^\d+$/.test(value))
            throw new errors_1.CoreError("BAD_REQUEST", `Invalid output port: ${value}`);
        const port = Number(value);
        if (!Number.isSafeInteger(port) || port < 1 || port > 65535) {
            throw new errors_1.CoreError("BAD_REQUEST", `Invalid output port: ${value}`);
        }
        if (!seen.has(port)) {
            seen.add(port);
            out.push(port);
        }
    }
    return out;
}
function parseNonNegativeInteger(raw, field) {
    if (raw === null || raw.trim() === "")
        return null;
    if (!/^\d+$/.test(raw.trim()))
        throw new errors_1.CoreError("BAD_REQUEST", `${field} must be a non-negative integer`);
    const parsed = Number(raw.trim());
    if (!Number.isSafeInteger(parsed) || parsed < 0 || parsed > MAX_OFFSET) {
        throw new errors_1.CoreError("BAD_REQUEST", `${field} is outside the supported integer range`);
    }
    return parsed;
}
function rejectUnknownParameters(params) {
    for (const key of new Set(Array.from(params.keys()))) {
        if (!ALLOWED_QUERY_PARAMETERS.has(key))
            throw new errors_1.CoreError("BAD_REQUEST", `Unsupported subscription parameter: ${key}`);
    }
}
function parseOutputSelection(env, params) {
    rejectUnknownParameters(params);
    const protocolRaw = (params.get("protocol") ?? "all").trim().toLowerCase();
    const protocol = protocolRaw === "all" ? "all" : protocolRaw === "vless" || protocolRaw === "trojan" ? protocolRaw : null;
    if (!protocol)
        throw new errors_1.CoreError("BAD_REQUEST", "protocol must be all, vless or trojan");
    const endpointRaw = (params.get("endpoint") ?? "all").trim().toLowerCase();
    const endpoint = endpointRaw === "all"
        ? "all"
        : endpointRaw === "domain"
            ? "domain"
            : endpointRaw === "cleanip"
                ? "cleanip"
                : /^\d+$/.test(endpointRaw)
                    ? Number(endpointRaw)
                    : (() => { throw new errors_1.CoreError("BAD_REQUEST", "endpoint must be all, domain, cleanip or a zero-based numeric index"); })();
    const transport = (params.get("transport") ?? "ws").trim().toLowerCase();
    if (transport !== "ws")
        throw new errors_1.CoreError("BAD_REQUEST", "Only the native WebSocket transport is currently supported");
    const limit = parseNonNegativeInteger(params.get("limit"), "limit");
    if (limit === 0)
        throw new errors_1.CoreError("BAD_REQUEST", "limit must be at least 1 when provided");
    const offset = parseNonNegativeInteger(params.get("offset"), "offset") ?? 0;
    const fingerprints = parseFingerprints(params.get("fingerprint") ?? env.TLS_FINGERPRINT);
    const alpn = parseAlpn(params.get("alpn") ?? env.TLS_ALPN);
    const ports = parsePorts(params.get("port") ?? undefined);
    const remarkTemplate = params.get("remark")?.trim()
        || params.get("name")?.trim()
        || env.OUTPUT_REMARK?.trim()
        || env.OUTPUT_NAME?.trim()
        || `Cloudflare-Proxy-Core-{protocol}-{kind}-{address}-{port}-{fingerprint}-{credential}`;
    if (remarkTemplate.length > 512)
        throw new errors_1.CoreError("BAD_REQUEST", "remark/name is too long");
    return { protocol, endpoint, ports, fingerprints, alpn, transport: "ws", remarkTemplate, limit, offset };
}
function selectEndpoints(env, requestHost, selection) {
    const pool = (0, endpoints_1.buildOutputEndpoints)(env, { requestHost });
    if (pool.endpoints.length > MAX_ENDPOINT_INPUTS) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Output endpoint input count ${pool.endpoints.length} exceeds internal safety ceiling ${MAX_ENDPOINT_INPUTS}`);
    }
    if (selection.ports) {
        const knownPorts = new Set(pool.outputPorts);
        const unknown = selection.ports.filter((port) => !knownPorts.has(port));
        if (unknown.length)
            throw new errors_1.CoreError("BAD_REQUEST", `Requested port is not configured in OUTPUT_PORT: ${unknown.join(",")}`);
    }
    const endpoints = pool.endpoints.filter((item, index) => {
        if (selection.endpoint === "domain" && item.kind !== "domain")
            return false;
        if (selection.endpoint === "cleanip" && item.kind !== "clean-ip")
            return false;
        if (typeof selection.endpoint === "number" && index !== selection.endpoint)
            return false;
        if (selection.ports && !selection.ports.includes(item.port))
            return false;
        return true;
    });
    if (typeof selection.endpoint === "number" && selection.endpoint >= pool.endpoints.length) {
        throw new errors_1.CoreError("BAD_REQUEST", `Endpoint index ${selection.endpoint} is out of range`);
    }
    return endpoints;
}
function renderRemark(template, protocol, endpoint, fingerprint, credentialIndex) {
    return template
        .replaceAll("{protocol}", protocol)
        .replaceAll("{kind}", endpoint.kind === "clean-ip" ? "cleanip" : "domain")
        .replaceAll("{address}", endpoint.address)
        .replaceAll("{port}", String(endpoint.port))
        .replaceAll("{fingerprint}", fingerprint)
        .replaceAll("{security}", endpoint.security)
        .replaceAll("{credential}", String(credentialIndex))
        .replaceAll("{index}", String(credentialIndex));
}
function parseOutputEarlyDataValue(rawValue) {
    const raw = rawValue?.trim();
    if (!raw || raw === "0")
        return undefined;
    if (!/^\d+$/.test(raw))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_OUTPUT_EARLY_DATA must be an integer from 0 to 8192");
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < 0 || value > 8192)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "CORE_OUTPUT_EARLY_DATA must be an integer from 0 to 8192");
    return value === 0 ? undefined : value;
}
function parseOutputEarlyData(env) {
    return parseOutputEarlyDataValue(env.CORE_OUTPUT_EARLY_DATA);
}
/**
 * Mihomo keys proxies by name, and several clients treat the remark as an identity.
 * The default template already discriminates every candidate dimension, but a custom
 * operator template may not; guarantee per-page name uniqueness deterministically by
 * suffixing repeat occurrences (-2, -3, ...). Iteration order is deterministic, so the
 * assignment is stable across renders of the same selection.
 */
function createUniqueNamer() {
    const seen = new Map();
    return (base) => {
        const occurrence = (seen.get(base) ?? 0) + 1;
        seen.set(base, occurrence);
        return occurrence === 1 ? base : `${base}-${occurrence}`;
    };
}
function createNode(env, endpoint, protocol, record, fingerprint, alpn, remark) {
    const rawCredential = record.rawCredential ?? (protocol === "vless" ? record.credential : "");
    const earlyData = parseOutputEarlyData(env);
    if (!rawCredential) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "Trojan subscription output requires raw passwords; normalized SHA-224 digests cannot be reversed");
    }
    const path = protocol === "vless" ? env.VLESS_PATH?.trim() || "/vless" : env.TROJAN_PATH?.trim() || "/trojan";
    const nodeBase = {
        protocol,
        server: endpoint.address,
        port: endpoint.port,
        credential: rawCredential,
        name: remark,
        security: endpoint.security,
        scheme: endpoint.scheme,
        endpointKind: endpoint.kind,
        websocket: { path, host: endpoint.httpHost, ...(earlyData ? { earlyData } : {}) },
    };
    if (endpoint.security === "tls") {
        return {
            ...nodeBase,
            tls: {
                serverName: endpoint.tlsServerName ?? endpoint.httpHost,
                alpn,
                fingerprint: fingerprint === "none" ? "chrome" : fingerprint,
            },
        };
    }
    return nodeBase;
}
function protocolsFor(selection) {
    return selection.protocol === "all" ? ["vless", "trojan"] : [selection.protocol];
}
function candidateCountFor(endpoints, selection, credentialCounts, target, tlsOnly = false) {
    let total = 0;
    for (const endpoint of endpoints) {
        if (tlsOnly && endpoint.security !== "tls")
            continue;
        for (const protocol of protocolsFor(selection)) {
            if (!(0, compatibility_1.supportsOutputCombination)(target, protocol, endpoint.security))
                continue;
            const fingerprints = endpoint.security === "tls" ? selection.fingerprints.length : 1;
            total += fingerprints * credentialCounts[protocol];
        }
    }
    return total;
}
function materializePageFromRecords(env, requestHost, selection, recordsByProtocol, budget) {
    const endpoints = selectEndpoints(env, requestHost, selection);
    const credentials = {
        vless: (recordsByProtocol.vless ?? []).filter((record) => record.enabled !== false),
        trojan: (recordsByProtocol.trojan ?? []).filter((record) => record.enabled !== false && Boolean(record.rawCredential)),
    };
    const credentialCounts = { vless: credentials.vless.length, trojan: credentials.trojan.length };
    for (const protocol of protocolsFor(selection)) {
        if (!credentialCounts[protocol] && selection.protocol === protocol) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `${protocol.toUpperCase()} has no output credentials available`);
        }
    }
    const candidateCount = candidateCountFor(endpoints, selection, credentialCounts, "native");
    const tlsCandidateCount = candidateCountFor(endpoints, selection, credentialCounts, "native", true);
    if (candidateCount > limits_1.CORE_LIMITS.maxOutputCandidateEvaluations) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Output candidate evaluation count ${candidateCount} exceeds safety ceiling ${limits_1.CORE_LIMITS.maxOutputCandidateEvaluations}`);
    }
    const requestedLimit = selection.limit ?? budget.defaultNodes;
    const effectiveLimit = Math.min(requestedLimit, budget.maxNodes);
    const offset = selection.offset;
    const limitClamped = requestedLimit > budget.maxNodes;
    if (candidateCount === 0)
        throw new errors_1.CoreError("BAD_REQUEST", "No output candidates are available for the requested protocol/endpoint selection");
    if (offset >= candidateCount)
        return { items: [], offset, requestedLimit, limit: effectiveLimit, limitClamped, candidateCount, tlsCandidateCount, hasMore: false, truncated: false };
    const uniqueName = createUniqueNamer();
    const items = [];
    let skip = offset;
    outer: for (const endpoint of endpoints) {
        for (const protocol of protocolsFor(selection)) {
            if (!(0, compatibility_1.supportsOutputCombination)("native", protocol, endpoint.security))
                continue;
            const records = credentials[protocol];
            if (!records.length)
                continue;
            const fingerprints = endpoint.security === "tls" ? selection.fingerprints : ["none"];
            const blockSize = fingerprints.length * records.length;
            if (skip >= blockSize) {
                skip -= blockSize;
                continue;
            }
            let fpIndex = Math.floor(skip / records.length);
            let credentialStart = skip % records.length;
            skip = 0;
            for (; fpIndex < fingerprints.length; fpIndex++) {
                const fingerprint = fingerprints[fpIndex];
                const startCredential = credentialStart;
                credentialStart = 0;
                for (let credentialIndex = startCredential; credentialIndex < records.length; credentialIndex++) {
                    const record = records[credentialIndex];
                    items.push(createNode(env, endpoint, protocol, record, fingerprint, selection.alpn, uniqueName(renderRemark(selection.remarkTemplate, protocol, endpoint, fingerprint, credentialIndex + 1))));
                    if (items.length >= effectiveLimit)
                        break outer;
                }
            }
        }
    }
    const nextOffset = offset + items.length;
    return {
        items, offset, requestedLimit, limit: effectiveLimit, limitClamped, candidateCount, tlsCandidateCount,
        hasMore: nextOffset < candidateCount,
        nextOffset: nextOffset < candidateCount ? nextOffset : undefined,
        truncated: items.length < Math.min(effectiveLimit, candidateCount - offset),
    };
}
async function buildOutputNodePage(env, requestHost, selection, provider, budget, signal, target = "native") {
    const endpoints = selectEndpoints(env, requestHost, selection);
    const credentialCounts = {
        vless: await provider.count("vless", signal),
        trojan: await provider.count("trojan", signal),
    };
    for (const protocol of protocolsFor(selection)) {
        if (credentialCounts[protocol] === 0) {
            if (selection.protocol === protocol)
                throw new errors_1.CoreError("CONFIGURATION_ERROR", `${protocol.toUpperCase()} has no output credentials available`);
        }
    }
    const candidateCount = candidateCountFor(endpoints, selection, credentialCounts, target);
    const tlsCandidateCount = candidateCountFor(endpoints, selection, credentialCounts, target, true);
    if (candidateCount > limits_1.CORE_LIMITS.maxOutputCandidateEvaluations) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Output candidate evaluation count ${candidateCount} exceeds safety ceiling ${limits_1.CORE_LIMITS.maxOutputCandidateEvaluations}`);
    }
    const requestedLimit = selection.limit ?? budget.defaultNodes;
    const effectiveLimit = Math.min(requestedLimit, budget.maxNodes);
    const offset = selection.offset;
    const limitClamped = requestedLimit > budget.maxNodes;
    if (candidateCount === 0) {
        throw new errors_1.CoreError("BAD_REQUEST", `No output candidates are compatible with ${target === "xray" ? "Xray public configuration" : target === "clash" ? "Mihomo/Clash configuration" : target === "sing-box" ? "Sing-box configuration" : "native share links"}`);
    }
    if (offset >= candidateCount) {
        return { items: [], offset, requestedLimit, limit: effectiveLimit, limitClamped, candidateCount, tlsCandidateCount, hasMore: false, truncated: false };
    }
    const cache = new Map();
    const pageSize = Math.max(1, effectiveLimit + 1);
    const getCredentialPage = async (protocol, start) => {
        const count = credentialCounts[protocol];
        const take = Math.min(pageSize, Math.max(0, count - start));
        if (take <= 0)
            return [];
        const key = `${protocol}:${start}:${take}`;
        const cached = cache.get(key);
        if (cached)
            return cached;
        const page = (await provider.page(protocol, start, take, signal)).filter((record) => record.enabled !== false && (protocol === "vless" || Boolean(record.rawCredential)));
        cache.set(key, page);
        return page;
    };
    const uniqueName = createUniqueNamer();
    const items = [];
    let skip = offset;
    outer: for (const endpoint of endpoints) {
        for (const protocol of protocolsFor(selection)) {
            if (!(0, compatibility_1.supportsOutputCombination)(target, protocol, endpoint.security))
                continue;
            const credentialCount = credentialCounts[protocol];
            if (!credentialCount)
                continue;
            const fingerprints = endpoint.security === "tls" ? selection.fingerprints : ["none"];
            const blockSize = fingerprints.length * credentialCount;
            if (skip >= blockSize) {
                skip -= blockSize;
                continue;
            }
            // Map the global offset into the two-dimensional fingerprint/credential block.
            let fpIndex = Math.floor(skip / credentialCount);
            let credentialStart = skip % credentialCount;
            skip = 0;
            for (; fpIndex < fingerprints.length; fpIndex++) {
                const fingerprint = fingerprints[fpIndex];
                let cursor = credentialStart;
                credentialStart = 0;
                while (cursor < credentialCount) {
                    const page = await getCredentialPage(protocol, cursor);
                    if (!page.length)
                        break;
                    for (let pageIndex = 0; pageIndex < page.length; pageIndex++) {
                        const credentialIndex = cursor + pageIndex;
                        items.push(createNode(env, endpoint, protocol, page[pageIndex], fingerprint, selection.alpn, uniqueName(renderRemark(selection.remarkTemplate, protocol, endpoint, fingerprint, credentialIndex + 1))));
                        if (items.length >= effectiveLimit)
                            break outer;
                    }
                    cursor += page.length;
                }
            }
        }
    }
    const nextOffset = offset + items.length;
    return {
        items,
        offset,
        requestedLimit,
        limit: effectiveLimit,
        limitClamped,
        candidateCount,
        tlsCandidateCount,
        hasMore: nextOffset < candidateCount,
        nextOffset: nextOffset < candidateCount ? nextOffset : undefined,
        truncated: items.length < Math.min(effectiveLimit, candidateCount - offset),
    };
}
function buildOutputNodesFromRecords(env, requestHost, selection, recordsByProtocol, budget) {
    const effectiveBudget = budget ?? { defaultNodes: 256, maxNodes: 512, maxBytes: 512 * 1024 };
    return [...materializePageFromRecords(env, requestHost, selection, recordsByProtocol, effectiveBudget).items];
}
async function listCredentials(provider, protocol, signal) {
    const records = await provider.list?.(protocol, signal) ?? [];
    return records.filter((record) => record.enabled !== false);
}
function createOutputCredentialSource(provider) {
    // The planner's paging arithmetic (candidate counts, block sizes, cursors) is only sound
    // when count() and page() share one record space. Provider count()/listPage() operate in
    // raw enabled-record space — for Trojan that includes digest-only records the planner can
    // never emit — while the previous fallback count() used eligible space and fallback page()
    // sliced the unfiltered list, so a digest-only record ahead of a raw one produced an empty
    // page and silently dropped eligible nodes (audit F5). This source therefore materialises
    // the eligible set once per protocol (bounded by the candidate-evaluation ceiling; the
    // planner rejects larger spaces before paging) and serves both count() and page() from it.
    const eligibleCache = new Map();
    const eligibleRecords = (protocol, signal) => {
        const cached = eligibleCache.get(protocol);
        if (cached)
            return cached;
        const pending = (async () => {
            const eligible = (record) => record.enabled !== false && (protocol === "vless" || Boolean(record.rawCredential));
            const out = [];
            if (provider.list) {
                for (const record of await provider.list(protocol, signal))
                    if (eligible(record))
                        out.push(record);
                return out;
            }
            if (provider.listPage) {
                const chunk = 256;
                let offset = 0;
                while (out.length <= limits_1.CORE_LIMITS.maxOutputCandidateEvaluations) {
                    const raw = await provider.listPage(protocol, offset, chunk, signal);
                    if (!raw.length)
                        break;
                    for (const record of raw)
                        if (eligible(record))
                            out.push(record);
                    offset += raw.length;
                    if (raw.length < chunk)
                        break;
                }
                return out;
            }
            return out;
        })();
        eligibleCache.set(protocol, pending);
        return pending;
    };
    const canMaterialize = Boolean(provider.list || provider.listPage);
    return {
        async count(protocol, signal) {
            if (canMaterialize)
                return (await eligibleRecords(protocol, signal)).length;
            if (provider.count)
                return provider.count(protocol, signal);
            return 0;
        },
        async page(protocol, offset, limit, signal) {
            if (canMaterialize)
                return (await eligibleRecords(protocol, signal)).slice(offset, offset + limit);
            return [];
        },
    };
}
function generateNativeUris(env, requestHost, selection, budget) {
    const recordsByProtocol = {
        vless: [],
        trojan: [],
    };
    const vlessRaw = parseList(env.VLESS_USERS).map((value) => value);
    recordsByProtocol.vless = vlessRaw.map((credential, index) => ({ id: `${index + 1}`, protocol: "vless", credential, rawCredential: credential, enabled: true }));
    recordsByProtocol.trojan = parseList(env.TROJAN_USERS).map((credential, index) => ({ id: `${index + 1}`, protocol: "trojan", credential: "", rawCredential: credential, enabled: true }));
    const nodes = buildOutputNodesFromRecords(env, requestHost, selection, recordsByProtocol, budget);
    return nodes.map((node) => (0, share_link_1.generateShareLink)(node));
}
function generateNativeUrisFromRecords(env, requestHost, selection, recordsByProtocol, budget) {
    return buildOutputNodesFromRecords(env, requestHost, selection, recordsByProtocol, budget).map((node) => (0, share_link_1.generateShareLink)(node));
}
function outputSummary(env, requestHost) {
    const pool = (0, endpoints_1.buildOutputEndpoints)(env, { requestHost });
    const enforcedOutputBudget = (0, runtime_1.buildRuntimeConfig)(env).output;
    return {
        core_version: limits_1.CORE_VERSION,
        host: pool.host,
        default_port: pool.defaultPort,
        output_ports: pool.outputPorts,
        endpoint_count: pool.endpoints.length,
        clean_ip_count: pool.endpoints.filter((item) => item.kind === "clean-ip").length,
        fingerprints: parseFingerprints(env.TLS_FINGERPRINT),
        protocols: ["vless", "trojan"],
        transport_matrix: {
            http: { ports: pool.outputPorts.filter((port) => pool.endpoints.find((endpoint) => endpoint.port === port && endpoint.scheme === "http")), security: "none", websocket: true, protocols: ["vless", "trojan"] },
            https: { ports: pool.outputPorts.filter((port) => pool.endpoints.find((endpoint) => endpoint.port === port && endpoint.scheme === "https")), security: "tls", websocket: true, protocols: ["vless", "trojan"] },
        },
        client_targets: {
            native: { vless_none: true, trojan_none: true },
            xray: { vless_none: false, trojan_none: false },
            "sing-box": { vless_none: true, trojan_none: true },
            clash: { vless_none: true, trojan_none: false },
        },
        pagination: {
            // Report the validated/clamped budget that rendering actually enforces, not raw env
            // echoes (non-numeric input previously surfaced as NaN/null here). Audit F15.
            default_nodes: enforcedOutputBudget.defaultNodes,
            max_nodes: enforcedOutputBudget.maxNodes,
            max_bytes: enforcedOutputBudget.maxBytes,
            endpoint_input_safety_ceiling: MAX_ENDPOINT_INPUTS,
        },
    };
}

};

__m["output/types"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SUPPORTED_TLS_FINGERPRINTS = void 0;
exports.SUPPORTED_TLS_FINGERPRINTS = [
    "chrome", "safari", "firefox", "edge", "360", "qq", "android", "ios", "random", "randomized",
];

};

__m["output/endpoints"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getOutputHost = getOutputHost;
exports.getOutputPorts = getOutputPorts;
exports.getOutputPort = getOutputPort;
exports.buildOutputEndpoints = buildOutputEndpoints;
exports.isIpAddress = isIpAddress;
const errors_1 = __r("../core/errors");
const binary_1 = __r("../util/binary");
const hostname_1 = __r("../util/hostname");
const security_1 = __r("./security");
function isIpv4Literal(value) {
    const parts = value.trim().split(".");
    if (parts.length !== 4 || parts.some((part) => !/^\d{1,3}$/.test(part)))
        return false;
    const numbers = parts.map(Number);
    return numbers.every((part) => part >= 0 && part <= 255);
}
function isIpv6Literal(value) {
    let input = value.trim();
    if (input.startsWith("[") && input.endsWith("]"))
        input = input.slice(1, -1);
    if (!input.includes(":"))
        return false;
    const pieces = input.split("::");
    if (pieces.length > 2)
        return false;
    const parseSide = (side) => {
        if (!side)
            return 0;
        const tokens = side.split(":");
        let count = 0;
        for (const token of tokens) {
            if (token.includes(".")) {
                if (!isIpv4Literal(token))
                    return null;
                count += 2;
                continue;
            }
            if (!/^[0-9a-fA-F]{1,4}$/.test(token))
                return null;
            count += 1;
        }
        return count;
    };
    const left = parseSide(pieces[0]);
    const right = parseSide(pieces[1] ?? "");
    if (left === null || right === null)
        return false;
    if (pieces.length === 2)
        return left + right < 8;
    return left === 8;
}
function isIpLiteral(value) {
    return isIpv4Literal(value) || isIpv6Literal(value);
}
function normalizeOutputHost(value) {
    const raw = value.trim();
    if (!raw)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "OUTPUT_HOST cannot be empty");
    // Strip an optional scheme (and trailing slashes) BEFORE the delimiter checks: the raw
    // value was previously rejected for containing "/" first, which made the scheme strip
    // unreachable dead code (audit F11).
    const candidate = raw.replace(/^https?:\/\//i, "").replace(/\/+$/, "");
    if (!candidate || /[/\\\s?#@]/.test(candidate) || candidate.includes(":") || candidate.includes("[") || candidate.includes("]")) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "OUTPUT_HOST must be a hostname without credentials, port, path, query or fragment");
    }
    if (isIpLiteral(candidate)) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "OUTPUT_HOST must be a hostname, not an IP literal");
    }
    // Enforce real LDH label rules (the previous URL round-trip accepted e.g. "exa_mple.com").
    const hostname = (0, hostname_1.toAsciiHostname)(candidate);
    if (!hostname || isIpLiteral(hostname)) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "OUTPUT_HOST is not a valid hostname");
    }
    return hostname;
}
function normalizeIp(value) {
    const trimmed = value.trim();
    const host = trimmed.startsWith("[") && trimmed.endsWith("]") ? trimmed.slice(1, -1) : trimmed;
    if (!isIpLiteral(host))
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `CLEAN_IPS entry is not an IP literal: ${value}`);
    return host.toLowerCase();
}
function hasExplicitPort(value) {
    const raw = value.trim();
    if (raw.startsWith("[")) {
        const close = raw.indexOf("]");
        if (close < 0)
            return false;
        return raw.slice(close + 1).startsWith(":");
    }
    const colonCount = (raw.match(/:/g) ?? []).length;
    if (colonCount !== 1)
        return false;
    const index = raw.lastIndexOf(":");
    return /^\d+$/.test(raw.slice(index + 1));
}
function getWebSocketHost(env, outputHost) {
    const configured = env.WS_HOST?.trim();
    return configured ? normalizeOutputHost(configured) : outputHost;
}
function makeEndpoint(id, kind, address, port, httpHost, tlsServerName, securityConfig) {
    const profile = (0, security_1.portProfileFor)(port, securityConfig);
    return {
        id,
        kind,
        address,
        port,
        scheme: profile.scheme,
        security: profile.security,
        httpHost,
        tlsServerName: profile.security === "tls" ? tlsServerName : null,
        edgePortClass: profile.edgePortClass,
    };
}
function parseCleanIpEntries(raw, outputPorts, securityConfig, httpHost, tlsServerName) {
    const values = (raw ?? "").split(/[\r\n,]+/).map((item) => item.trim()).filter(Boolean);
    const endpoints = [];
    const seen = new Set();
    for (const value of values) {
        let host;
        let parsedPort;
        try {
            const parsed = (0, binary_1.parseHostPort)(value, outputPorts[0]);
            host = normalizeIp(parsed.host);
            parsedPort = parsed.port ?? outputPorts[0];
            (0, binary_1.validatePort)(parsedPort);
        }
        catch (error) {
            if (error instanceof errors_1.CoreError)
                throw error;
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid CLEAN_IPS entry: ${value}`, false, { cause: error });
        }
        const ports = hasExplicitPort(value) ? [parsedPort] : outputPorts;
        if (hasExplicitPort(value) && !outputPorts.includes(parsedPort)) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `CLEAN_IPS entry ${value} uses port ${parsedPort}, which is not present in OUTPUT_PORT`);
        }
        for (const port of ports) {
            try {
                (0, binary_1.validatePort)(port);
            }
            catch (error) {
                throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid port for CLEAN_IPS entry: ${value}`, false, { cause: error });
            }
            const key = `${host}\0${port}`;
            if (seen.has(key))
                continue;
            seen.add(key);
            endpoints.push(makeEndpoint(`clean-ip-${endpoints.length + 1}`, "clean-ip", host, port, httpHost, tlsServerName, securityConfig));
        }
    }
    return endpoints;
}
function getOutputHost(env, requestHost) {
    const configured = env.OUTPUT_HOST?.trim();
    if (configured)
        return normalizeOutputHost(configured);
    const wsHost = env.WS_HOST?.trim();
    if (wsHost)
        return normalizeOutputHost(wsHost);
    if (!requestHost)
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "OUTPUT_HOST is required when no request hostname is available");
    return normalizeOutputHost(requestHost);
}
function getOutputPorts(env) {
    const raw = env.OUTPUT_PORT?.trim();
    if (!raw)
        return [443];
    const values = raw.split(/[\r\n,]+/).map((item) => item.trim()).filter(Boolean);
    if (values.length === 0)
        return [443];
    const ports = [];
    const seen = new Set();
    for (const value of values) {
        if (!/^\d+$/.test(value)) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid OUTPUT_PORT entry: ${value}. Expected an integer from 1 to 65535`);
        }
        const port = Number(value);
        try {
            (0, binary_1.validatePort)(port);
        }
        catch (error) {
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `Invalid OUTPUT_PORT entry: ${value}. Expected an integer from 1 to 65535`, false, { cause: error });
        }
        if (!seen.has(port)) {
            seen.add(port);
            ports.push(port);
        }
    }
    return ports.length > 0 ? ports : [443];
}
function getOutputPort(env) {
    return getOutputPorts(env)[0];
}
function buildOutputEndpoints(env, options = {}) {
    const host = getOutputHost(env, options.requestHost);
    const outputPorts = getOutputPorts(env);
    const securityConfig = (0, security_1.normalizeOutputSecurityConfig)(env);
    const httpHost = getWebSocketHost(env, host);
    const tlsServerName = host;
    const endpoints = [];
    for (const port of outputPorts) {
        endpoints.push({
            id: `domain-${port}`,
            kind: "domain",
            address: host,
            port,
            scheme: (0, security_1.portProfileFor)(port, securityConfig).scheme,
            security: (0, security_1.portProfileFor)(port, securityConfig).security,
            httpHost,
            tlsServerName: (0, security_1.portProfileFor)(port, securityConfig).security === "tls" ? tlsServerName : null,
            edgePortClass: (0, security_1.portProfileFor)(port, securityConfig).edgePortClass,
        });
    }
    endpoints.push(...parseCleanIpEntries(env.CLEAN_IPS, outputPorts, securityConfig, httpHost, tlsServerName));
    return {
        host,
        defaultPort: outputPorts[0],
        outputPorts,
        endpoints,
    };
}
function isIpAddress(value) {
    return isIpLiteral(value);
}

};

__m["output/security"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.normalizeOutputSecurityConfig = normalizeOutputSecurityConfig;
exports.portProfileFor = portProfileFor;
exports.securityForPort = securityForPort;
exports.schemeForPort = schemeForPort;
const errors_1 = __r("../core/errors");
const binary_1 = __r("../util/binary");
const cloudflare_1 = __r("./edge/cloudflare");
function parsePortList(raw, field) {
    if (!raw?.trim())
        return [];
    const out = [];
    const seen = new Set();
    for (const value of raw.split(/[\r\n,]+/).map((item) => item.trim()).filter(Boolean)) {
        if (!/^\d+$/.test(value))
            throw new errors_1.CoreError("CONFIGURATION_ERROR", `${field} contains invalid port '${value}'`);
        const port = Number(value);
        (0, binary_1.validatePort)(port);
        if (!seen.has(port)) {
            seen.add(port);
            out.push(port);
        }
    }
    return out;
}
function normalizeOutputSecurityConfig(env) {
    const explicitTlsPorts = parsePortList(env.OUTPUT_TLS_PORTS, "OUTPUT_TLS_PORTS");
    const explicitHttpPorts = parsePortList(env.OUTPUT_HTTP_PORTS, "OUTPUT_HTTP_PORTS");
    const tlsSet = new Set(explicitTlsPorts);
    if (explicitHttpPorts.some((port) => tlsSet.has(port))) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", "OUTPUT_TLS_PORTS and OUTPUT_HTTP_PORTS must not overlap");
    }
    const httpSet = new Set(cloudflare_1.CLOUDFLARE_HTTP_PORTS);
    const httpsSet = new Set(cloudflare_1.CLOUDFLARE_HTTPS_PORTS);
    const invalidTls = explicitTlsPorts.filter((port) => !httpsSet.has(port));
    const invalidHttp = explicitHttpPorts.filter((port) => !httpSet.has(port));
    if (invalidTls.length) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `OUTPUT_TLS_PORTS contains non-HTTPS Cloudflare ports: ${invalidTls.join(", ")}`);
    }
    if (invalidHttp.length) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `OUTPUT_HTTP_PORTS contains non-HTTP Cloudflare ports: ${invalidHttp.join(", ")}`);
    }
    return {
        tlsPorts: [...cloudflare_1.CLOUDFLARE_HTTPS_PORTS],
        httpPorts: [...cloudflare_1.CLOUDFLARE_HTTP_PORTS],
        explicitTlsPorts,
        explicitHttpPorts,
    };
}
function portProfileFor(port, envOrConfig) {
    // A configured list is an assertion/compatibility layer only. It never overrides Cloudflare's canonical classification.
    if (!("tlsPorts" in envOrConfig))
        normalizeOutputSecurityConfig(envOrConfig);
    return (0, cloudflare_1.cloudflarePortProfile)(port);
}
function securityForPort(port, envOrConfig) {
    return portProfileFor(port, envOrConfig).security;
}
function schemeForPort(port, envOrConfig) {
    return portProfileFor(port, envOrConfig).scheme;
}

};

__m["output/edge/cloudflare"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CLOUDFLARE_HTTPS_PORTS = exports.CLOUDFLARE_HTTP_PORTS = void 0;
exports.cloudflarePortProfile = cloudflarePortProfile;
exports.isCloudflareHttpPort = isCloudflareHttpPort;
exports.isCloudflareHttpsPort = isCloudflareHttpsPort;
exports.allCloudflarePortProfiles = allCloudflarePortProfiles;
const errors_1 = __r("../../core/errors");
const binary_1 = __r("../../util/binary");
exports.CLOUDFLARE_HTTP_PORTS = [80, 8080, 8880, 2052, 2082, 2086, 2095];
exports.CLOUDFLARE_HTTPS_PORTS = [443, 2053, 2083, 2087, 2096, 8443];
const profiles = new Map();
for (const port of exports.CLOUDFLARE_HTTP_PORTS) {
    profiles.set(port, { port, scheme: "http", security: "none", websocket: true, edgePortClass: "cloudflare-http" });
}
for (const port of exports.CLOUDFLARE_HTTPS_PORTS) {
    profiles.set(port, { port, scheme: "https", security: "tls", websocket: true, edgePortClass: "cloudflare-https" });
}
function cloudflarePortProfile(port) {
    (0, binary_1.validatePort)(port);
    const profile = profiles.get(port);
    if (!profile) {
        throw new errors_1.CoreError("CONFIGURATION_ERROR", `Output port ${port} is not one of Cloudflare's default HTTP/HTTPS proxy-compatible Worker ports`);
    }
    return profile;
}
function isCloudflareHttpPort(port) {
    return profiles.get(port)?.scheme === "http";
}
function isCloudflareHttpsPort(port) {
    return profiles.get(port)?.scheme === "https";
}
function allCloudflarePortProfiles() {
    return [...profiles.values()];
}

};

__m["output/bundle"] = function(module, exports, __r) {
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Backward-compatible facade for the output planner.
 * New code should import planning primitives from ./planning.
 */
__exportStar(__r("./planning"), exports);

};

__m["transport/websocket/adapter"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CloudflareWebSocketAdapter = void 0;
exports.tryExtractEarlyData = tryExtractEarlyData;
exports.websocketUpgradeRequiredResponse = websocketUpgradeRequiredResponse;
const byte_queue_1 = __r("../../core/byte-queue");
const errors_1 = __r("../../core/errors");
const types_1 = __r("../../core/types");
const byte_writer_1 = __r("./byte-writer");
function isWebSocketUpgrade(request) {
    return request.headers.get("Upgrade")?.toLowerCase() === "websocket";
}
function decodeBase64Url(value) {
    const normalized = value.trim().replace(/-/g, "+").replace(/_/g, "/");
    if (!normalized || /[^A-Za-z0-9+/=]/.test(normalized) || normalized.includes("=") && !/=+$/.test(normalized)) {
        throw new Error("Invalid base64url");
    }
    const body = normalized.replace(/=+$/g, "");
    if (body.length % 4 === 1)
        throw new Error("Invalid base64url length");
    const padded = body + "=".repeat((4 - (body.length % 4)) % 4);
    const decoded = atob(padded);
    const bytes = new Uint8Array(decoded.length);
    for (let i = 0; i < decoded.length; i++)
        bytes[i] = decoded.charCodeAt(i);
    return bytes;
}
function protocolTokens(value) {
    return value.split(",").map((token) => token.trim()).filter(Boolean);
}
function extractEarlyData(request, options) {
    if (!options.earlyDataEnabled)
        return null;
    const tokens = protocolTokens(request.headers.get(options.earlyDataHeaderName ?? "Sec-WebSocket-Protocol") ?? "");
    const encoded = tokens[0] ?? "";
    if (!encoded)
        return null;
    try {
        const max = Math.min(types_1.CORE_LIMITS.maxEarlyDataBytes, options.maxEarlyDataBytes ?? types_1.CORE_LIMITS.maxEarlyDataBytes);
        if (encoded.length > Math.ceil(max * 4 / 3) + 8)
            return null;
        const bytes = decodeBase64Url(encoded);
        if (bytes.byteLength > max)
            return null;
        // Disambiguation (A9): a token that base64url-decodes to printable ASCII
        // is an ordinary RFC 6455 subprotocol name, not a binary protocol payload
        // (real early data is a VLESS/Trojan header or TLS ClientHello — always
        // binary). Consuming a plain protocol name as payload would silently
        // corrupt the handshake, so such tokens are left for negotiation instead.
        if (bytes.every((byte) => byte >= 0x20 && byte <= 0x7e))
            return null;
        return { bytes, tokenIndex: 0 };
    }
    catch {
        return null;
    }
}
function tryExtractEarlyData(request, options) {
    return extractEarlyData(request, options)?.bytes ?? null;
}
function normalizedExpectedHost(value) {
    const trimmed = value.trim().toLowerCase().replace(/^https?:\/\//, "");
    // Strip IPv6 brackets (and any trailing port) the same way on both sides of
    // the comparison (A19): WHATWG URL.hostname retains brackets for literals.
    if (trimmed.startsWith("[")) {
        const end = trimmed.indexOf("]");
        if (end > 0)
            return trimmed.slice(1, end);
    }
    return trimmed.replace(/:\d+$/, "");
}
class CloudflareWebSocketAdapter {
    async accept(request, options = {}) {
        if (!isWebSocketUpgrade(request))
            throw new errors_1.CoreError("UNSUPPORTED_TRANSPORT", "Expected Upgrade: websocket");
        const url = new URL(request.url);
        if (options.expectedPath && url.pathname !== options.expectedPath) {
            throw new errors_1.CoreError("BAD_REQUEST", "Invalid WebSocket path");
        }
        if (options.expectedHost && normalizedExpectedHost(url.hostname) !== normalizedExpectedHost(options.expectedHost)) {
            throw new errors_1.CoreError("BAD_REQUEST", "Invalid WebSocket host");
        }
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.binaryType = "arraybuffer";
        server.accept({ allowHalfOpen: true });
        const queue = new byte_queue_1.ByteQueue(options.maxBufferedBytes ?? types_1.CORE_LIMITS.maxWebSocketBufferedBytes);
        const writer = new byte_writer_1.WebSocketByteWriter(server, types_1.CORE_LIMITS.maxWebSocketWriteChunkBytes, options.maxBufferedBytes ?? types_1.CORE_LIMITS.maxWebSocketBufferedBytes);
        const requestHost = request.headers.get("Host")?.trim().toLowerCase();
        const metadata = {
            type: "websocket",
            // Sanitized path only, per the TransportMetadata contract (A4): the
            // query string can carry early-data parameters and operator secrets and
            // must not be retained in session metadata.
            path: url.pathname,
            ...(requestHost ? { host: requestHost } : {}),
        };
        const early = extractEarlyData(request, options);
        if (early)
            queue.push(early.bytes);
        // RFC 6455 §4.2.2 (A9): when the client offered subprotocols, select the
        // first one that was not consumed as early data and echo it in the 101.
        const earlyHeaderIsProtocol = (options.earlyDataHeaderName ?? "Sec-WebSocket-Protocol") === "Sec-WebSocket-Protocol";
        const offeredProtocols = protocolTokens(request.headers.get("Sec-WebSocket-Protocol") ?? "");
        const selectedProtocol = offeredProtocols.find((_, index) => !(early && earlyHeaderIsProtocol && index === early.tokenIndex));
        let closePromise = null;
        server.addEventListener("message", (event) => {
            try {
                if (typeof event.data === "string") {
                    queue.end(new errors_1.CoreError("BAD_REQUEST", "Text WebSocket messages are not supported"));
                    return;
                }
                if (event.data instanceof ArrayBuffer) {
                    queue.push(new Uint8Array(event.data));
                    return;
                }
                if (event.data instanceof Uint8Array) {
                    queue.push(event.data);
                    return;
                }
                queue.end(new errors_1.CoreError("BAD_REQUEST", "Unsupported WebSocket message type"));
            }
            catch (error) {
                queue.end(error);
            }
        });
        server.addEventListener("error", (event) => {
            queue.end(new errors_1.CoreError("CLIENT_CLOSED", "WebSocket transport error", false, { cause: event }));
        });
        server.addEventListener("close", () => {
            writer.markClosed();
            queue.end(new errors_1.CoreError("CLIENT_CLOSED", "WebSocket closed"));
        });
        const connection = {
            reader: queue,
            writer,
            metadata,
            close: async (code = 1000, reason = "") => {
                if (closePromise)
                    return closePromise;
                closePromise = Promise.resolve().then(() => {
                    try {
                        if (server.readyState === WebSocket.OPEN || server.readyState === WebSocket.CLOSING) {
                            server.close(code, truncateUtf8(reason, 123));
                        }
                    }
                    finally {
                        queue.end(new errors_1.CoreError("CLIENT_CLOSED", reason || "WebSocket closed"));
                        writer.markClosed();
                    }
                });
                return closePromise;
            },
        };
        return {
            response: new Response(null, {
                status: 101,
                webSocket: client,
                ...(selectedProtocol ? { headers: { "Sec-WebSocket-Protocol": selectedProtocol } } : {}),
            }),
            connection,
        };
    }
}
exports.CloudflareWebSocketAdapter = CloudflareWebSocketAdapter;
function truncateUtf8(value, maxBytes) {
    const encoder = new TextEncoder();
    if (encoder.encode(value).byteLength <= maxBytes)
        return value;
    let end = Math.min(value.length, maxBytes);
    while (end > 0 && encoder.encode(value.slice(0, end)).byteLength > maxBytes)
        end--;
    return value.slice(0, end);
}
function websocketUpgradeRequiredResponse() {
    return new Response("Expected Upgrade: websocket", { status: 426 });
}

};

__m["core/byte-queue"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ByteQueue = void 0;
const errors_1 = __r("./errors");
class ByteQueue {
    maxBufferedBytes;
    chunks = [];
    buffered = 0;
    ended = null;
    waiter = null;
    constructor(maxBufferedBytes = 8 * 1024 * 1024) {
        this.maxBufferedBytes = maxBufferedBytes;
        if (!Number.isInteger(maxBufferedBytes) || maxBufferedBytes <= 0) {
            throw new RangeError("maxBufferedBytes must be a positive integer");
        }
    }
    get bufferedBytes() {
        return this.buffered;
    }
    push(data) {
        if (data.byteLength === 0 || this.ended !== null)
            return;
        if (this.buffered + data.byteLength > this.maxBufferedBytes) {
            const error = new errors_1.CoreError("MESSAGE_TOO_LARGE", `Byte queue limit exceeded (${this.maxBufferedBytes} bytes)`);
            this.end(error);
            throw error;
        }
        this.chunks.push({ data, offset: 0 });
        this.buffered += data.byteLength;
        this.pump();
    }
    end(error) {
        if (this.ended !== null)
            return;
        // EOF is modeled as a CLIENT_CLOSED rejection (A18): end() with no error is
        // the normal "peer finished sending" signal, not a failure. The ByteReader
        // contract documents this; consumers must special-case the code.
        this.ended = error === undefined ? new errors_1.CoreError("CLIENT_CLOSED", "Byte source ended") : (0, errors_1.asCoreError)(error);
        this.pump();
    }
    async readExact(length, signal) {
        if (!Number.isInteger(length) || length < 0)
            throw new RangeError("length must be a non-negative integer");
        if (length === 0)
            return new Uint8Array(0);
        if (this.waiter)
            throw new errors_1.CoreError("INTERNAL", "Concurrent ByteReader reads are not supported");
        const immediate = this.takeExactIfAvailable(length);
        if (immediate)
            return immediate;
        if (this.ended !== null)
            throw this.ended;
        return this.waitFor("exact", length, signal);
    }
    async readSome(maxLength, signal) {
        if (!Number.isInteger(maxLength) || maxLength <= 0)
            throw new RangeError("maxLength must be a positive integer");
        if (this.waiter)
            throw new errors_1.CoreError("INTERNAL", "Concurrent ByteReader reads are not supported");
        const immediate = this.takeSomeIfAvailable(maxLength);
        if (immediate)
            return immediate;
        if (this.ended !== null)
            throw this.ended;
        return this.waitFor("some", maxLength, signal);
    }
    readAvailable(maxLength = this.buffered) {
        if (!Number.isInteger(maxLength) || maxLength <= 0 || this.buffered === 0)
            return null;
        return this.takeSomeIfAvailable(maxLength);
    }
    waitFor(mode, amount, signal) {
        if (signal?.aborted)
            return Promise.reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
        return new Promise((resolve, reject) => {
            const onAbort = () => {
                if (!this.waiter || this.waiter.resolve !== resolve)
                    return;
                this.waiter = null;
                reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
            };
            const cleanup = () => signal?.removeEventListener("abort", onAbort);
            signal?.addEventListener("abort", onAbort, { once: true });
            this.waiter = { mode, amount, resolve, reject, cleanup };
            if (signal?.aborted) {
                onAbort();
                return;
            }
            this.pump();
        });
    }
    pump() {
        const waiter = this.waiter;
        if (!waiter)
            return;
        const data = waiter.mode === "exact"
            ? this.takeExactIfAvailable(waiter.amount)
            : this.takeSomeIfAvailable(waiter.amount);
        if (data) {
            this.waiter = null;
            waiter.cleanup();
            waiter.resolve(data);
            return;
        }
        if (this.ended !== null) {
            this.waiter = null;
            waiter.cleanup();
            waiter.reject(this.ended);
        }
    }
    takeExactIfAvailable(length) {
        return this.buffered >= length ? this.consume(length) : null;
    }
    takeSomeIfAvailable(maxLength) {
        return this.buffered > 0 ? this.consume(Math.min(maxLength, this.buffered)) : null;
    }
    consume(length) {
        const first = this.chunks[0];
        if (first && first.data.byteLength - first.offset >= length) {
            const start = first.offset;
            const chunk = first.data;
            first.offset += length;
            this.buffered -= length;
            if (first.offset === chunk.byteLength)
                this.chunks.shift();
            const view = chunk.subarray(start, start + length);
            // A subarray view pins the whole backing buffer (A6): a small read out of
            // a large WebSocket message would otherwise retain up to the full queue
            // bound. Copy when the result is a small fraction of a large chunk; the
            // multi-chunk path below always copies, so this also keeps aliasing
            // semantics predictable for callers.
            const remaining = chunk.byteLength - start;
            if (remaining !== length && chunk.byteLength > 4096 && length * 4 < remaining) {
                return new Uint8Array(view);
            }
            return view;
        }
        const out = new Uint8Array(length);
        let written = 0;
        while (written < length) {
            const chunk = this.chunks[0];
            if (!chunk)
                throw new errors_1.CoreError("INTERNAL", "ByteQueue internal underflow");
            const available = chunk.data.byteLength - chunk.offset;
            const take = Math.min(available, length - written);
            out.set(chunk.data.subarray(chunk.offset, chunk.offset + take), written);
            chunk.offset += take;
            written += take;
            this.buffered -= take;
            if (chunk.offset === chunk.data.byteLength)
                this.chunks.shift();
        }
        return out;
    }
}
exports.ByteQueue = ByteQueue;

};

__m["config/validation"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConfigurationValidationError = void 0;
exports.validateEnvironment = validateEnvironment;
const errors_1 = __r("../core/errors");
const binary_1 = __r("../util/binary");
const security_1 = __r("../output/security");
const endpoints_1 = __r("../output/endpoints");
const runtime_1 = __r("./runtime");
const nat64_1 = __r("../egress/nat64");
const aggregation_1 = __r("../output/aggregation");
const config_1 = __r("../dns/config");
const defaults_1 = __r("../egress/defaults");
const relay_1 = __r("../egress/relay");
const rules_1 = __r("../routing/rules");
const profiles_1 = __r("../output/profiles");
class ConfigurationValidationError extends errors_1.CoreError {
    constructor(issues) {
        super("CONFIGURATION_ERROR", `Worker configuration is invalid (${issues.length} issue${issues.length === 1 ? "" : "s"})`, false, { cause: issues });
        // Freeze the issue list: the cached error instance is rethrown across requests (see C20),
        // so its payload must not be mutable shared state.
        this.issues = Object.freeze([...issues]);
    }
    issues;
}
exports.ConfigurationValidationError = ConfigurationValidationError;
const TRUE_VALUES = new Set(["1", "true", "yes", "on"]);
const FALSE_VALUES = new Set(["0", "false", "no", "off"]);
const MAX_PATH_LENGTH = 512;
const MAX_HOST_LENGTH = 253;
const MAX_TOKEN_LENGTH = 4096;
const MIN_TOKEN_LENGTH = 32;
let cachedValidationSignature = "";
let cachedValidation = null;
function pushIssue(issues, field, message) {
    issues.push(Object.freeze({ field, message }));
}
function splitList(raw) {
    return (raw ?? "").split(/[\r\n,]+/).map((value) => value.trim()).filter(Boolean);
}
// Every CoreEnvironment field except the DB/KV object bindings is declared `string | undefined`.
// Wrangler `vars` may legally carry JSON numbers/booleans/objects, which arrive as non-strings and
// would otherwise crash validators with a raw TypeError (C16). Keep this list in sync with
// src/config/environment.ts; undeclared extra bindings in env are ignored on purpose.
const STRING_ENV_FIELDS = new Set([
    "VLESS_USERS", "TROJAN_USERS", "VLESS_PATH", "TROJAN_PATH", "WS_HOST",
    "EGRESS_MODE", "PROXY_IP", "PROXY_IPS", "PROXY_IP_ENDPOINTS_JSON", "PROXY_IP_TLS",
    "PROXY_IP_USERNAME", "PROXY_IP_PASSWORD", "PROXY_RELAY_ENDPOINTS",
    "CORE_MAX_PROXYIP_FAILURES", "CORE_PROXYIP_COOLDOWN_MS", "CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS",
    "NAT64_PREFIXES", "CORE_MAX_CONCURRENT_ADDRESS_TRIES", "CORE_ADDRESS_TRY_STAGGER_MS",
    "CORE_RULES", "OUTPUT_HOST", "OUTPUT_PORT", "OUTPUT_TLS_PORTS", "OUTPUT_HTTP_PORTS",
    "CLEAN_IPS", "OUTPUT_NAME", "OUTPUT_REMARK", "TLS_FINGERPRINT", "TLS_ALPN",
    "CORE_FRAGMENT_PROFILES", "OUTPUT_CUSTOM_SUBS", "CORE_OUTPUT_SUBS_ALLOWLIST",
    "CORE_OUTPUT_SUBS_ALLOW_HTTP", "CORE_OUTPUT_SUBS_TIMEOUT_MS", "CORE_OUTPUT_EARLY_DATA",
    "CORE_OUTPUT_SINGBOX_UTLS", "CORE_OUTPUT_XRAY_ALLOW_DIRECT_FALLBACK", "CORE_OUTPUT_ENABLE_TUN",
    "CORE_XRAY_PROBE_INTERVAL", "CORE_XRAY_OBSERVATORY_CONCURRENCY",
    "CORE_XRAY_FRAGMENT_PROBE_INTERVAL", "CORE_XRAY_FRAGMENT_OBSERVATORY_CONCURRENCY",
    "CORE_OUTPUT_TOKEN", "CORE_OUTPUT_ALLOW_QUERY_TOKEN", "CORE_OUTPUT_ALLOW_RAW",
    "CORE_OUTPUT_DEFAULT_NODES", "CORE_OUTPUT_MAX_NODES", "CORE_OUTPUT_MAX_BYTES",
    "CORE_OUTPUT_MAX_ENDPOINTS", "CORE_OUTPUT_MAX_CREDENTIALS",
    "DNS_ENABLED", "DNS_RESOLVERS", "DNS_CACHE", "DNS_CACHE_MAX_ENTRIES", "DNS_CACHE_MAX_TTL",
    "DNS_CACHE_NEGATIVE_TTL", "DNS_TIMEOUT_MS", "DNS_RETRIES", "DNS_FALLBACK",
    "DNS_IP_PREFERENCE", "DNS_HOSTS", "DNS_MAX_RESOLVERS",
    "CORE_CREDENTIAL_MODE", "CORE_HANDSHAKE_TIMEOUT_MS", "CORE_FIRST_BYTE_TIMEOUT_MS",
    "EGRESS_CONNECT_TIMEOUT_MS", "CORE_MAX_INITIAL_RETRIES", "CORE_RETRY_ON_EMPTY_RESPONSE",
    "CORE_IDLE_TIMEOUT_MS", "CORE_MAX_SESSION_MS", "CORE_MAX_EGRESS_DECISION_MS",
    "CORE_ADMIN_TOKEN", "CORE_TEST_TOKEN", "CORE_ALLOW_LEGACY_TEST_TOKEN", "CORE_TEST_TARGET", "CORE_TEST_TIMEOUT_MS",
    "CORE_TEST_LIVE_DNS", "DOH_PATH", "CORE_DOH_TOKEN", "CORE_DOH_PATH_SECRET",
    "CORE_OUTPUT_PATH_SECRET",
]);
// Collects a CONFIGURATION_ERROR issue for every declared string field that arrived as a
// non-string (e.g. a JSON number in wrangler vars) and returns a sanitized copy of env with
// those fields unset, so downstream validators never throw TypeErrors of their own (C16).
function sanitizeEnvTypes(issues, env) {
    let sanitized = null;
    for (const field of STRING_ENV_FIELDS) {
        const value = env[field];
        if (value !== undefined && value !== null && typeof value !== "string") {
            pushIssue(issues, field, "must be a string; quote the value if it is set through JSON vars in the Wrangler configuration");
            sanitized ??= { ...env };
            sanitized[field] = undefined;
        }
    }
    return sanitized ?? env;
}
// Presence of a binding does not prove its type (C6): a KV namespace bound as DB (or a plain
// string var) must fail at startup, not with a per-request TypeError inside the provider.
function validateBindings(issues, env) {
    if (env.DB !== undefined && env.DB !== null) {
        const db = env.DB;
        if ((typeof db !== "object" && typeof db !== "function") || typeof db.prepare !== "function") {
            pushIssue(issues, "DB", "must be a D1 database binding (an object exposing prepare())");
        }
    }
    if (env.KV !== undefined && env.KV !== null) {
        const kv = env.KV;
        if ((typeof kv !== "object" && typeof kv !== "function") || typeof kv.get !== "function" || typeof kv.put !== "function" || typeof kv.delete !== "function") {
            pushIssue(issues, "KV", "must be a KV namespace binding (an object exposing get(), put() and delete())");
        }
    }
}
// Many CoreError messages from downstream builders start with the env field that caused them
// (e.g. "CORE_HANDSHAKE_TIMEOUT_MS must be ..."); use that for precise issue attribution (C19).
function issueFieldFromError(error, fallback) {
    if (error instanceof errors_1.CoreError) {
        const match = error.message.match(/^([A-Z][A-Z0-9_]{2,})\b/);
        if (match)
            return match[1];
    }
    return fallback;
}
function parseBooleanField(issues, field, raw, fallback) {
    const value = raw?.trim();
    if (!value)
        return fallback;
    const normalized = value.toLowerCase();
    if (TRUE_VALUES.has(normalized))
        return true;
    if (FALSE_VALUES.has(normalized))
        return false;
    pushIssue(issues, field, "must be one of: true, false, 1, 0, yes, no, on, off");
    return fallback;
}
function parseIntegerField(issues, field, raw, fallback, min, max) {
    const value = raw?.trim();
    if (!value)
        return fallback;
    if (!/^\d+$/.test(value)) {
        pushIssue(issues, field, `must be an integer from ${min} to ${max}`);
        return fallback;
    }
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
        pushIssue(issues, field, `must be an integer from ${min} to ${max}`);
        return fallback;
    }
    return parsed;
}
function isIpv4Literal(value) {
    const parts = value.trim().split(".");
    if (parts.length !== 4 || parts.some((part) => !/^\d{1,3}$/.test(part)))
        return false;
    return parts.map(Number).every((part) => part >= 0 && part <= 255);
}
function isIpv6Literal(value) {
    let input = value.trim();
    if (input.startsWith("[") && input.endsWith("]"))
        input = input.slice(1, -1);
    if (!input.includes(":"))
        return false;
    const pieces = input.split("::");
    if (pieces.length > 2)
        return false;
    const parseSide = (side) => {
        if (!side)
            return 0;
        const tokens = side.split(":");
        let count = 0;
        for (const token of tokens) {
            if (token.includes(".")) {
                if (!isIpv4Literal(token))
                    return null;
                count += 2;
                continue;
            }
            if (!/^[0-9a-fA-F]{1,4}$/.test(token))
                return null;
            count += 1;
        }
        return count;
    };
    const left = parseSide(pieces[0]);
    const right = parseSide(pieces[1] ?? "");
    if (left === null || right === null)
        return false;
    return pieces.length === 2 ? left + right < 8 : left === 8;
}
function isIpLiteral(value) {
    return isIpv4Literal(value) || isIpv6Literal(value);
}
function validHostname(value) {
    const candidate = value.trim().replace(/\.$/, "");
    if (!candidate || candidate.length > MAX_HOST_LENGTH || isIpLiteral(candidate) || /[\s\\/?#@]/.test(candidate))
        return false;
    try {
        const url = new URL(`https://${candidate}`);
        return !url.username && !url.password && !url.port && url.pathname === "/" && !url.search && !url.hash && url.hostname === candidate.toLowerCase();
    }
    catch {
        return false;
    }
}
function validatePath(issues, field, raw, fallback) {
    const value = raw?.trim() || fallback;
    // Consumers compare the configured path against the percent-encoded `url.pathname`, and they
    // normalize it inconsistently (the DoH handler strips trailing slashes, the router does not).
    // Only the canonical form is therefore accepted (C5): no spaces/control characters (which can
    // never match a pathname), no percent-encoding or dot segments, and no trailing slash.
    if (value.length > MAX_PATH_LENGTH
        || !value.startsWith("/")
        || value.includes("?") || value.includes("#") || value.includes("\\")
        || value.includes("%")
        || /[\s\u0000-\u001f\u007f]/.test(value)
        || (value.length > 1 && value.endsWith("/"))
        || value.split("/").some((segment) => segment === "." || segment === "..")) {
        pushIssue(issues, field, "must be a canonical URL pathname starting with / and must not contain query, fragment, backslash, percent-encoding, spaces, control characters, dot segments or a trailing slash");
        return fallback;
    }
    return value;
}
// Paths owned by the fixed control/subscription routes; a configurable protocol or DoH path
// must never shadow the authorization boundary (C5).
function isReservedRoutePath(value) {
    return value === "/sub" || value === "/__core" || value.startsWith("/__core/");
}
// Strict host:port with a mandatory port (unlike parseEndpoint, which defaults the port).
function isValidHostPort(value) {
    let host;
    let portText;
    if (value.startsWith("[")) {
        const close = value.indexOf("]");
        if (close < 0 || value[close + 1] !== ":")
            return false;
        host = value.slice(1, close);
        portText = value.slice(close + 2);
        if (!isIpv6Literal(host))
            return false;
    }
    else {
        const index = value.lastIndexOf(":");
        if (index <= 0 || value.indexOf(":") !== index)
            return false;
        host = value.slice(0, index);
        portText = value.slice(index + 1);
        if (!validHostname(host) && !isIpv4Literal(host))
            return false;
    }
    if (!/^\d+$/.test(portText))
        return false;
    const port = Number(portText);
    return Number.isSafeInteger(port) && port >= 1 && port <= 65535;
}
function validateUsers(issues, env) {
    const vless = splitList(env.VLESS_USERS);
    const trojan = splitList(env.TROJAN_USERS);
    const hasDb = Boolean(env.DB);
    if (!vless.length && !hasDb)
        pushIssue(issues, "VLESS_USERS", "must contain at least one UUID when the DB binding is not configured");
    if (!trojan.length && !hasDb)
        pushIssue(issues, "TROJAN_USERS", "must contain at least one password when the DB binding is not configured");
    const vlessSeen = new Set();
    for (const value of vless) {
        try {
            const uuidBytes = (0, binary_1.uuidToBytes)(value);
            // The nil UUID was the shipped env.example placeholder; it is publicly known and must
            // never be a working credential (C2).
            if (uuidBytes.every((byte) => byte === 0)) {
                pushIssue(issues, "VLESS_USERS", "must not contain the nil UUID 00000000-0000-0000-0000-000000000000; generate a unique random UUID per user");
                continue;
            }
            const normalized = Array.from(uuidBytes).join(":");
            if (vlessSeen.has(normalized))
                pushIssue(issues, "VLESS_USERS", "contains a duplicate UUID");
            vlessSeen.add(normalized);
        }
        catch {
            pushIssue(issues, "VLESS_USERS", "contains an invalid UUID");
        }
    }
    const trojanSeen = new Set();
    for (const value of trojan) {
        const bytes = new TextEncoder().encode(value);
        if (bytes.byteLength === 0 || bytes.byteLength > 255)
            pushIssue(issues, "TROJAN_USERS", "each password must be 1..255 UTF-8 bytes");
        // Placeholder passwords shipped in examples/docs are publicly known; reject them and the
        // angle-bracket placeholder convention used by env.example so a verbatim copy of the
        // example file can never start with working credentials (C2).
        if (PLACEHOLDER_PASSWORDS.has(value.toLowerCase()) || /^<.*>$/.test(value)) {
            pushIssue(issues, "TROJAN_USERS", "must not contain a placeholder password; generate a unique random password per deployment");
        }
        if (trojanSeen.has(value))
            pushIssue(issues, "TROJAN_USERS", "contains a duplicate password");
        trojanSeen.add(value);
    }
}
const PLACEHOLDER_PASSWORDS = new Set([
    "change-me", "changeme", "change_me", "replace-me", "replace_me", "placeholder",
    "your-password", "your-trojan-password", "example-password", "default-password",
    "trojan-password", "generate-a-password",
]);
function validateToken(issues, field, value, required) {
    const trimmed = value?.trim() ?? "";
    if (required && !trimmed)
        pushIssue(issues, field, "must be configured");
    // A minimum length floor: these tokens are the sole protection of the management, output and
    // private-DoH surfaces, so a 1-character secret must not pass startup validation (C1).
    if (trimmed && trimmed.length < MIN_TOKEN_LENGTH)
        pushIssue(issues, field, `must be at least ${MIN_TOKEN_LENGTH} characters; generate a unique random value`);
    if (trimmed && trimmed.length > MAX_TOKEN_LENGTH)
        pushIssue(issues, field, `must not exceed ${MAX_TOKEN_LENGTH} characters`);
    if (trimmed && /[\r\n]/.test(trimmed))
        pushIssue(issues, field, "must not contain CR/LF characters");
}
// Tokens and path secrets are documented as distinct capabilities; reusing one value across
// capabilities silently merges them, so reuse is rejected (C1). CORE_TEST_TOKEN is the legacy
// alias of the admin capability, so equality inside that pair is not treated as cross-capability reuse.
const TOKEN_CAPABILITY_GROUPS = [
    ["CORE_ADMIN_TOKEN", "CORE_TEST_TOKEN"],
    ["CORE_OUTPUT_TOKEN"],
    ["CORE_DOH_TOKEN"],
    ["CORE_DOH_PATH_SECRET"],
    ["CORE_OUTPUT_PATH_SECRET"],
];
function validateTokenUniqueness(issues, env) {
    const present = TOKEN_CAPABILITY_GROUPS.map((group) => group
        .map((field) => ({ field, value: env[field]?.trim() ?? "" }))
        .filter((entry) => entry.value));
    for (let i = 0; i < present.length; i++) {
        for (let j = i + 1; j < present.length; j++) {
            for (const a of present[i]) {
                for (const b of present[j]) {
                    if (a.value === b.value) {
                        pushIssue(issues, b.field, `must be different from ${a.field}; token reuse across capabilities is not allowed`);
                    }
                }
            }
        }
    }
}
function validateRelay(issues, env) {
    try {
        const endpoints = (0, relay_1.parseRelayEndpoints)(env.PROXY_RELAY_ENDPOINTS);
        if (env.PROXY_RELAY_ENDPOINTS?.trim() && endpoints.length === 0)
            pushIssue(issues, "PROXY_RELAY_ENDPOINTS", "must contain at least one endpoint");
    }
    catch (error) {
        pushIssue(issues, "PROXY_RELAY_ENDPOINTS", error instanceof errors_1.CoreError ? error.message : "contains an invalid relay endpoint");
    }
}
function validateProxyIp(issues, env) {
    const endpoints = [...splitList(env.PROXY_IP), ...splitList(env.PROXY_IPS)];
    const mode = (env.EGRESS_MODE?.trim().toLowerCase() || "smart");
    const hasStructured = Boolean(env.PROXY_IP_ENDPOINTS_JSON?.trim());
    if ((mode === "proxyip" || mode === "fallback") && !endpoints.length && !hasStructured) {
        pushIssue(issues, "PROXY_IP/PROXY_IPS/PROXY_IP_ENDPOINTS_JSON", `must contain at least one endpoint when EGRESS_MODE=${mode}`);
    }
    if (hasStructured) {
        try {
            const value = JSON.parse(env.PROXY_IP_ENDPOINTS_JSON ?? "");
            if (!Array.isArray(value) || value.length === 0 || value.length > 64)
                pushIssue(issues, "PROXY_IP_ENDPOINTS_JSON", "must be a JSON array containing 1..64 endpoint objects");
            else
                for (const item of value) {
                    if (!item || typeof item !== "object") {
                        pushIssue(issues, "PROXY_IP_ENDPOINTS_JSON", "contains a non-object endpoint");
                        continue;
                    }
                    const entry = item;
                    const host = typeof entry.host === "string" ? entry.host.trim() : "";
                    const port = entry.port === undefined ? 1080 : Number(entry.port);
                    if ((!validHostname(host) && !isIpLiteral(host)) || !Number.isInteger(port) || port < 1 || port > 65535)
                        pushIssue(issues, "PROXY_IP_ENDPOINTS_JSON", "contains an endpoint with an invalid host or port");
                    if (entry.tls !== undefined && typeof entry.tls !== "boolean")
                        pushIssue(issues, "PROXY_IP_ENDPOINTS_JSON", "endpoint tls must be boolean");
                    const username = typeof entry.username === "string" ? entry.username : "";
                    const password = typeof entry.password === "string" ? entry.password : "";
                    if (Boolean(username) !== Boolean(password))
                        pushIssue(issues, "PROXY_IP_ENDPOINTS_JSON", "endpoint username/password must be configured together");
                }
        }
        catch {
            pushIssue(issues, "PROXY_IP_ENDPOINTS_JSON", "must be valid JSON");
        }
    }
    const username = env.PROXY_IP_USERNAME?.trim() || "";
    const password = env.PROXY_IP_PASSWORD ?? "";
    if (!hasStructured && Boolean(username) !== Boolean(password)) {
        pushIssue(issues, "PROXY_IP_USERNAME/PROXY_IP_PASSWORD", "must be configured together");
    }
    parseBooleanField(issues, "PROXY_IP_TLS", env.PROXY_IP_TLS, false);
    for (const endpoint of endpoints) {
        try {
            const parsed = parseEndpoint(endpoint);
            if (!validHostname(parsed.host) && !isIpLiteral(parsed.host)) {
                pushIssue(issues, "PROXY_IP/PROXY_IPS", "contains an endpoint with an invalid host");
            }
            if (parsed.port < 1 || parsed.port > 65535)
                pushIssue(issues, "PROXY_IP/PROXY_IPS", "contains an endpoint with an invalid port");
        }
        catch {
            pushIssue(issues, "PROXY_IP/PROXY_IPS", "contains an invalid host:port endpoint");
        }
    }
}
function parseEndpoint(value) {
    const raw = value.trim();
    if (!raw)
        throw new Error("empty endpoint");
    if (raw.startsWith("[")) {
        const close = raw.indexOf("]");
        if (close < 0)
            throw new Error("bad ipv6 endpoint");
        const host = raw.slice(1, close);
        const rest = raw.slice(close + 1);
        if (!rest)
            return { host, port: 1080 };
        if (!/^:\d+$/.test(rest))
            throw new Error("bad port");
        return { host, port: Number(rest.slice(1)) };
    }
    const colonCount = (raw.match(/:/g) ?? []).length;
    if (colonCount > 1 && isIpv6Literal(raw))
        return { host: raw, port: 1080 };
    const index = raw.lastIndexOf(":");
    if (index < 0)
        return { host: raw, port: 1080 };
    if (!/^\d+$/.test(raw.slice(index + 1)))
        throw new Error("bad port");
    return { host: raw.slice(0, index), port: Number(raw.slice(index + 1)) };
}
function validateDns(issues, env) {
    const egressMode = (env.EGRESS_MODE?.trim().toLowerCase() || "smart");
    const dnsDefaultEnabled = Boolean(env.DNS_RESOLVERS?.trim() || env.DNS_HOSTS?.trim() || egressMode === "smart" || egressMode === "nat64");
    const enabled = parseBooleanField(issues, "DNS_ENABLED", env.DNS_ENABLED, dnsDefaultEnabled);
    const maxResolvers = parseIntegerField(issues, "DNS_MAX_RESOLVERS", env.DNS_MAX_RESOLVERS, 8, 1, 8);
    let resolvers = [];
    try {
        resolvers = (0, config_1.parseDohResolverEndpoints)(env.DNS_RESOLVERS, maxResolvers);
    }
    catch (error) {
        pushIssue(issues, "DNS_RESOLVERS", error instanceof errors_1.CoreError ? error.message : "contains an invalid DoH endpoint");
    }
    if (enabled && resolvers.length === 0)
        pushIssue(issues, "DNS_RESOLVERS", "must contain at least one DoH endpoint when DNS is enabled");
    parseBooleanField(issues, "DNS_CACHE", env.DNS_CACHE, true);
    parseIntegerField(issues, "DNS_CACHE_MAX_ENTRIES", env.DNS_CACHE_MAX_ENTRIES, 1024, 1, 100_000);
    parseIntegerField(issues, "DNS_CACHE_MAX_TTL", env.DNS_CACHE_MAX_TTL, 300, 1, 86_400);
    parseIntegerField(issues, "DNS_CACHE_NEGATIVE_TTL", env.DNS_CACHE_NEGATIVE_TTL, 30, 1, 3_600);
    parseIntegerField(issues, "DNS_TIMEOUT_MS", env.DNS_TIMEOUT_MS, 3000, 100, 60_000);
    parseIntegerField(issues, "DNS_RETRIES", env.DNS_RETRIES, 1, 0, 5);
    parseBooleanField(issues, "DNS_FALLBACK", env.DNS_FALLBACK, true);
    parseBooleanField(issues, "CORE_TEST_LIVE_DNS", env.CORE_TEST_LIVE_DNS, false);
    const strategy = env.DNS_IP_PREFERENCE?.trim().toLowerCase() || "auto";
    if (!["auto", "ipv4", "ipv6", "prefer_ipv4", "prefer_ipv6"].includes(strategy)) {
        pushIssue(issues, "DNS_IP_PREFERENCE", "must be one of auto, ipv4, ipv6, prefer_ipv4, prefer_ipv6");
    }
    for (const entry of splitList(env.DNS_HOSTS)) {
        const index = entry.indexOf("=");
        if (index <= 0 || index === entry.length - 1) {
            pushIssue(issues, "DNS_HOSTS", "entries must use hostname=IP syntax");
            continue;
        }
        const host = entry.slice(0, index).trim().toLowerCase();
        const ip = entry.slice(index + 1).trim();
        if (!validHostname(host))
            pushIssue(issues, "DNS_HOSTS", "contains an invalid hostname");
        if (!isIpLiteral(ip))
            pushIssue(issues, "DNS_HOSTS", "contains an invalid IPv4/IPv6 address");
    }
}
function validateOutput(issues, env, securityConfig) {
    // Attribute each failure to the step (and, when the message names it, the exact field) that
    // produced it instead of funnelling everything into one coarse field name (C19).
    let ports = [];
    try {
        ports = (0, endpoints_1.getOutputPorts)(env);
    }
    catch (error) {
        pushIssue(issues, issueFieldFromError(error, "OUTPUT_PORT"), error instanceof errors_1.CoreError ? error.message : "contains an invalid output port");
        return;
    }
    try {
        for (const port of ports)
            (0, security_1.securityForPort)(port, securityConfig);
    }
    catch (error) {
        pushIssue(issues, issueFieldFromError(error, "OUTPUT_TLS_PORTS/OUTPUT_HTTP_PORTS"), error instanceof errors_1.CoreError ? error.message : "contains an invalid port security mapping");
        return;
    }
    try {
        (0, endpoints_1.buildOutputEndpoints)(env, { requestHost: env.OUTPUT_HOST?.trim() || env.WS_HOST?.trim() || "example.com" });
    }
    catch (error) {
        pushIssue(issues, issueFieldFromError(error, "OUTPUT_HOST/CLEAN_IPS"), error instanceof errors_1.CoreError ? error.message : "contains an invalid endpoint");
    }
    // OUTPUT_NAME / OUTPUT_REMARK shape the names/remarks of generated client configs and were
    // previously only bounded at request time; apply the same 512-char bound plus the CR/LF /
    // control-character hygiene used for tokens at startup (C11).
    for (const field of ["OUTPUT_NAME", "OUTPUT_REMARK"]) {
        const value = env[field]?.trim();
        if (!value)
            continue;
        if (value.length > 512)
            pushIssue(issues, field, "must not exceed 512 characters");
        if (/[\u0000-\u001f\u007f]/.test(value))
            pushIssue(issues, field, "must not contain control characters");
    }
}
function validateTlsInputs(issues, env) {
    const fingerprints = splitList(env.TLS_FINGERPRINT).map((value) => value.toLowerCase());
    const supported = new Set(["chrome", "safari", "firefox", "edge", "360", "qq", "android", "ios", "random", "randomized"]);
    for (const fp of fingerprints)
        if (fp !== "all" && !supported.has(fp))
            pushIssue(issues, "TLS_FINGERPRINT", `contains unsupported fingerprint '${fp}'`);
    for (const value of splitList(env.TLS_ALPN)) {
        const bytes = new TextEncoder().encode(value);
        if (bytes.length === 0 || bytes.length > 255 || /[\u0000-\u0020\u007f]/.test(value)) {
            pushIssue(issues, "TLS_ALPN", "contains an invalid ALPN identifier");
            continue;
        }
        if (value.toLowerCase() !== "http/1.1") {
            pushIssue(issues, "TLS_ALPN", "the current WebSocket transport supports only ALPN http/1.1");
        }
    }
}
function validatePathsAndHosts(issues, env) {
    const vlessPath = validatePath(issues, "VLESS_PATH", env.VLESS_PATH, "/vless");
    const trojanPath = validatePath(issues, "TROJAN_PATH", env.TROJAN_PATH, "/trojan");
    // DOH_PATH was previously not validated at all (C4): give it the same canonical-path
    // treatment as the protocol paths, plus the cross-path checks below.
    const dohPath = validatePath(issues, "DOH_PATH", env.DOH_PATH, "/dns-query");
    if (vlessPath === trojanPath)
        pushIssue(issues, "VLESS_PATH/TROJAN_PATH", "must be different");
    if (dohPath === vlessPath)
        pushIssue(issues, "DOH_PATH", "must be different from VLESS_PATH");
    if (dohPath === trojanPath)
        pushIssue(issues, "DOH_PATH", "must be different from TROJAN_PATH");
    for (const [field, path] of [["VLESS_PATH", vlessPath], ["TROJAN_PATH", trojanPath], ["DOH_PATH", dohPath]]) {
        if (isReservedRoutePath(path))
            pushIssue(issues, field, "must not collide with the built-in /sub or /__core/* routes");
    }
    const wsHost = env.WS_HOST?.trim();
    if (wsHost && !validHostname(wsHost))
        pushIssue(issues, "WS_HOST", "must be a hostname without scheme, port, path or query");
    const outputHost = env.OUTPUT_HOST?.trim();
    if (outputHost && !validHostname(outputHost))
        pushIssue(issues, "OUTPUT_HOST", "must be a hostname without scheme, port, path or query");
    if (outputHost && outputHost.length > MAX_HOST_LENGTH)
        pushIssue(issues, "OUTPUT_HOST", `must not exceed ${MAX_HOST_LENGTH} characters`);
}
function validateRoutingProviders(issues, env) {
    const rules = splitList(env.CORE_RULES);
    const proxyConfigured = Boolean(splitList(env.PROXY_IP).length || splitList(env.PROXY_IPS).length || env.PROXY_IP_ENDPOINTS_JSON?.trim());
    const nat64Configured = Boolean(env.NAT64_PREFIXES?.trim()) || (env.EGRESS_MODE?.trim().toLowerCase() || "smart") === "smart" || (env.EGRESS_MODE?.trim().toLowerCase() || "smart") === "nat64";
    const relayConfigured = (() => { try {
        return (0, relay_1.parseRelayEndpoints)(env.PROXY_RELAY_ENDPOINTS).length > 0;
    }
    catch {
        return false;
    } })();
    for (const entry of rules) {
        try {
            const parsed = (0, rules_1.parseCoreRule)(entry);
            const egresses = [parsed.egress, parsed.fallbackEgress].filter(Boolean);
            for (const egress of egresses) {
                if (egress === "proxyip" && !proxyConfigured)
                    pushIssue(issues, "CORE_RULES", `rule requires a configured ProxyIP provider: ${entry}`);
                if (egress === "relay" && !relayConfigured)
                    pushIssue(issues, "CORE_RULES", `rule requires configured PROXY_RELAY_ENDPOINTS: ${entry}`);
                if (egress === "nat64" && !nat64Configured)
                    pushIssue(issues, "CORE_RULES", `rule requires NAT64 provider or EGRESS_MODE=smart/nat64: ${entry}`);
            }
        }
        catch (error) {
            pushIssue(issues, "CORE_RULES", error instanceof errors_1.CoreError ? error.message : `invalid rule: ${entry}`);
        }
    }
}
function validateExternalSubscriptions(issues, env) {
    // The timeout/HTTP fields are validated unconditionally (C12): previously they were only
    // parsed when subscriptions were already configured, so garbage values passed startup and
    // only surfaced the day subscriptions were switched on.
    const allowHttp = parseBooleanField(issues, "CORE_OUTPUT_SUBS_ALLOW_HTTP", env.CORE_OUTPUT_SUBS_ALLOW_HTTP, false);
    parseIntegerField(issues, "CORE_OUTPUT_SUBS_TIMEOUT_MS", env.CORE_OUTPUT_SUBS_TIMEOUT_MS, 5000, 250, 30_000);
    const sources = splitList(env.OUTPUT_CUSTOM_SUBS);
    if (!sources.length)
        return;
    const allowlist = splitList(env.CORE_OUTPUT_SUBS_ALLOWLIST);
    if (!allowlist.length)
        pushIssue(issues, "CORE_OUTPUT_SUBS_ALLOWLIST", "must contain at least one allowed host when OUTPUT_CUSTOM_SUBS is configured");
    for (const source of sources) {
        try {
            (0, aggregation_1.validateSubscriptionUrl)(source, { ...env, CORE_OUTPUT_SUBS_ALLOWLIST: allowlist.join(","), CORE_OUTPUT_SUBS_ALLOW_HTTP: allowHttp ? "true" : "false" });
        }
        catch (error) {
            pushIssue(issues, "OUTPUT_CUSTOM_SUBS", error instanceof errors_1.CoreError ? error.message : "contains an unsafe external subscription URL");
        }
    }
    for (const rule of allowlist) {
        const normalized = rule.toLowerCase().replace(/^\*\./, "");
        if (!validHostname(normalized))
            pushIssue(issues, "CORE_OUTPUT_SUBS_ALLOWLIST", `contains an invalid hostname pattern: ${rule}`);
    }
}
function validateAdvanced(issues, env) {
    try {
        (0, runtime_1.buildRuntimeConfig)(env);
    }
    catch (error) {
        pushIssue(issues, issueFieldFromError(error, "CORE_RUNTIME"), error instanceof errors_1.CoreError ? error.message : "invalid runtime settings");
    }
    // CORE_FRAGMENT_PROFILES is parsed with strict bounds by the output layer; run that same
    // parser at startup so an invalid value fails validation instead of failing the first
    // Fragment output request (C9).
    if (env.CORE_FRAGMENT_PROFILES?.trim()) {
        try {
            (0, profiles_1.getXrayFragmentProfiles)(env);
        }
        catch (error) {
            pushIssue(issues, "CORE_FRAGMENT_PROFILES", error instanceof errors_1.CoreError ? error.message : "contains an invalid fragment profile");
        }
    }
    // CORE_TEST_TARGET is the host:port the self-test path dials; validate its shape here instead
    // of letting a malformed value through to the test runner (C10).
    const testTarget = env.CORE_TEST_TARGET?.trim();
    if (testTarget && !isValidHostPort(testTarget)) {
        pushIssue(issues, "CORE_TEST_TARGET", "must use host:port format with a valid host and a port from 1 to 65535");
    }
    const credentialMode = env.CORE_CREDENTIAL_MODE?.trim().toLowerCase();
    if (credentialMode && !["d1", "env", "hybrid"].includes(credentialMode))
        pushIssue(issues, "CORE_CREDENTIAL_MODE", "must be one of d1, env or hybrid");
    const hasEnvVless = splitList(env.VLESS_USERS).length > 0;
    const hasEnvTrojan = splitList(env.TROJAN_USERS).length > 0;
    if (credentialMode === "d1" && !env.DB)
        pushIssue(issues, "CORE_CREDENTIAL_MODE", "d1 mode requires the DB binding");
    if (credentialMode === "env" && !hasEnvVless && !hasEnvTrojan)
        pushIssue(issues, "CORE_CREDENTIAL_MODE", "env mode requires at least one VLESS_USERS or TROJAN_USERS entry");
    if (credentialMode === "hybrid" && !env.DB && !hasEnvVless && !hasEnvTrojan)
        pushIssue(issues, "CORE_CREDENTIAL_MODE", "hybrid mode requires the DB binding or environment credentials");
    if (env.NAT64_PREFIXES?.trim()) {
        try {
            const prefixes = (0, nat64_1.parseNat64Prefixes)(env.NAT64_PREFIXES);
            if (!prefixes.length)
                pushIssue(issues, "NAT64_PREFIXES", "must contain at least one NAT64 prefix");
        }
        catch (error) {
            pushIssue(issues, "NAT64_PREFIXES", error instanceof errors_1.CoreError ? error.message : "contains an invalid prefix");
        }
    }
    const mode = env.EGRESS_MODE?.trim().toLowerCase() || "smart";
    if ((mode === "nat64" || mode === "smart") && !env.NAT64_PREFIXES?.trim()) {
        try {
            (0, nat64_1.parseNat64Prefixes)(defaults_1.DEFAULT_NAT64_PREFIXES.join(","));
        }
        catch (error) {
            pushIssue(issues, "NAT64_PREFIXES", error instanceof errors_1.CoreError ? error.message : "default NAT64 prefixes are invalid");
        }
    }
    parseIntegerField(issues, "CORE_OUTPUT_MAX_NODES", env.CORE_OUTPUT_MAX_NODES, 512, 1, 10_000);
    const configuredMaxNodes = env.CORE_OUTPUT_MAX_NODES?.trim() ? Number(env.CORE_OUTPUT_MAX_NODES) : 512;
    parseIntegerField(issues, "CORE_OUTPUT_DEFAULT_NODES", env.CORE_OUTPUT_DEFAULT_NODES, Math.min(256, configuredMaxNodes), 1, Number.isSafeInteger(configuredMaxNodes) && configuredMaxNodes > 0 ? configuredMaxNodes : 512);
    parseIntegerField(issues, "CORE_OUTPUT_MAX_BYTES", env.CORE_OUTPUT_MAX_BYTES, 512 * 1024, 1024, 8 * 1024 * 1024);
    parseIntegerField(issues, "CORE_HANDSHAKE_TIMEOUT_MS", env.CORE_HANDSHAKE_TIMEOUT_MS, 10_000, 100, 60_000);
    parseIntegerField(issues, "EGRESS_CONNECT_TIMEOUT_MS", env.EGRESS_CONNECT_TIMEOUT_MS, 10_000, 100, 60_000);
    parseIntegerField(issues, "CORE_FIRST_BYTE_TIMEOUT_MS", env.CORE_FIRST_BYTE_TIMEOUT_MS, 0, 0, 60_000);
    parseIntegerField(issues, "CORE_MAX_INITIAL_RETRIES", env.CORE_MAX_INITIAL_RETRIES, 3, 0, 4);
    parseBooleanField(issues, "CORE_RETRY_ON_EMPTY_RESPONSE", env.CORE_RETRY_ON_EMPTY_RESPONSE, true);
    parseIntegerField(issues, "CORE_IDLE_TIMEOUT_MS", env.CORE_IDLE_TIMEOUT_MS, 0, 0, 86_400_000);
    parseIntegerField(issues, "CORE_MAX_SESSION_MS", env.CORE_MAX_SESSION_MS, 0, 0, 604_800_000);
    parseIntegerField(issues, "CORE_MAX_PROXYIP_FAILURES", env.CORE_MAX_PROXYIP_FAILURES, 3, 1, 20);
    parseIntegerField(issues, "CORE_PROXYIP_COOLDOWN_MS", env.CORE_PROXYIP_COOLDOWN_MS, 15_000, 1_000, 86_400_000);
    parseBooleanField(issues, "CORE_OUTPUT_ALLOW_QUERY_TOKEN", env.CORE_OUTPUT_ALLOW_QUERY_TOKEN, false);
    parseBooleanField(issues, "CORE_OUTPUT_ALLOW_RAW", env.CORE_OUTPUT_ALLOW_RAW, false);
    parseBooleanField(issues, "CORE_OUTPUT_SINGBOX_UTLS", env.CORE_OUTPUT_SINGBOX_UTLS, false);
    parseBooleanField(issues, "CORE_OUTPUT_XRAY_ALLOW_DIRECT_FALLBACK", env.CORE_OUTPUT_XRAY_ALLOW_DIRECT_FALLBACK, false);
    parseBooleanField(issues, "CORE_OUTPUT_ENABLE_TUN", env.CORE_OUTPUT_ENABLE_TUN, false);
    parseIntegerField(issues, "CORE_OUTPUT_EARLY_DATA", env.CORE_OUTPUT_EARLY_DATA, 0, 0, 8192);
    if (env.OUTPUT_CUSTOM_SUBS?.trim() && env.OUTPUT_CUSTOM_SUBS.split(/[\r\n,]+/).filter((x) => x.trim()).length > 8)
        pushIssue(issues, "OUTPUT_CUSTOM_SUBS", "supports at most 8 sources");
    validateRoutingProviders(issues, env);
    validateExternalSubscriptions(issues, env);
}
function validateEnvironment(env) {
    // The cache key must distinguish everything validation actually depends on (C20): string
    // values are JSON-encoded (so no separator value can forge a collision), and object bindings
    // are reduced to the capability shape the binding checks inspect (C6) rather than a bare
    // boolean — two different bindings can no longer collide into a stale cached result.
    const signatureParts = Object.keys(env).sort().map((key) => {
        const value = env[key];
        if (value !== null && typeof value === "object") {
            const shape = ["prepare", "get", "put", "delete"]
                .filter((prop) => typeof value[prop] === "function")
                .join("+") || "object";
            return `${key}=#${shape}`;
        }
        return `${key}=${JSON.stringify(value ?? null)}`;
    });
    const signature = signatureParts.join("\n");
    if (cachedValidationSignature === signature && cachedValidation) {
        if (cachedValidation instanceof ConfigurationValidationError)
            throw cachedValidation;
        return cachedValidation;
    }
    const issues = [];
    env = sanitizeEnvTypes(issues, env);
    validateBindings(issues, env);
    const mode = (env.EGRESS_MODE?.trim().toLowerCase() || "smart");
    if (!["direct", "proxyip", "fallback", "nat64", "smart"].includes(mode))
        pushIssue(issues, "EGRESS_MODE", "must be one of direct, proxyip, fallback, nat64 or smart");
    validateUsers(issues, env);
    validateAdvanced(issues, env);
    validatePathsAndHosts(issues, env);
    validateProxyIp(issues, env);
    validateRelay(issues, env);
    validateTlsInputs(issues, env);
    validateDns(issues, env);
    validateToken(issues, "CORE_ADMIN_TOKEN", env.CORE_ADMIN_TOKEN, false);
    validateToken(issues, "CORE_OUTPUT_TOKEN", env.CORE_OUTPUT_TOKEN, false);
    validateToken(issues, "CORE_TEST_TOKEN", env.CORE_TEST_TOKEN, false);
    validateToken(issues, "CORE_DOH_TOKEN", env.CORE_DOH_TOKEN, false);
    parseIntegerField(issues, "CORE_MAX_CONCURRENT_ADDRESS_TRIES", env.CORE_MAX_CONCURRENT_ADDRESS_TRIES, 4, 1, 4);
    parseIntegerField(issues, "CORE_ADDRESS_TRY_STAGGER_MS", env.CORE_ADDRESS_TRY_STAGGER_MS, 100, 0, 5_000);
    parseIntegerField(issues, "CORE_TEST_TIMEOUT_MS", env.CORE_TEST_TIMEOUT_MS, 8000, 100, 60_000);
    parseIntegerField(issues, "CORE_MAX_EGRESS_DECISION_MS", env.CORE_MAX_EGRESS_DECISION_MS, 30_000, 1_000, 120_000);
    parseIntegerField(issues, "CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS", env.CORE_PROXYIP_HANDSHAKE_TIMEOUT_MS, 5_000, 100, 60_000);
    let outputSecurity;
    try {
        outputSecurity = (0, security_1.normalizeOutputSecurityConfig)(env);
    }
    catch (error) {
        if (error instanceof errors_1.CoreError)
            pushIssue(issues, "OUTPUT_SECURITY", error.message);
        else
            pushIssue(issues, "OUTPUT_SECURITY", "contains invalid values");
        outputSecurity = { tlsPorts: [], httpPorts: [], explicitTlsPorts: [], explicitHttpPorts: [] };
    }
    validateOutput(issues, env, outputSecurity);
    const probeInterval = env.CORE_XRAY_PROBE_INTERVAL?.trim();
    if (probeInterval && !/^\d+(?:ms|s|m|h)$/.test(probeInterval)) {
        pushIssue(issues, "CORE_XRAY_PROBE_INTERVAL", "must be an Xray duration such as 30s, 60s, 5m or 1h");
    }
    if (probeInterval) {
        const match = probeInterval.match(/^(\d+)(ms|s|m|h)$/);
        if (match) {
            const unit = match[2];
            const multiplier = unit === "ms" ? 1 : unit === "s" ? 1000 : unit === "m" ? 60_000 : 3_600_000;
            const value = Number(match[1]) * multiplier;
            if (!Number.isSafeInteger(value) || value < 1_000 || value > 3_600_000) {
                pushIssue(issues, "CORE_XRAY_PROBE_INTERVAL", "must be between 1s and 1h");
            }
        }
    }
    parseBooleanField(issues, "CORE_XRAY_OBSERVATORY_CONCURRENCY", env.CORE_XRAY_OBSERVATORY_CONCURRENCY, true);
    const fragmentProbeInterval = env.CORE_XRAY_FRAGMENT_PROBE_INTERVAL?.trim();
    if (fragmentProbeInterval && !/^\d+(?:ms|s|m|h)$/.test(fragmentProbeInterval)) {
        pushIssue(issues, "CORE_XRAY_FRAGMENT_PROBE_INTERVAL", "must be an Xray duration such as 1s, 5s, 1m or 1h");
    }
    if (fragmentProbeInterval) {
        const match = fragmentProbeInterval.match(/^(\d+)(ms|s|m|h)$/);
        if (match) {
            const unit = match[2];
            const multiplier = unit === "ms" ? 1 : unit === "s" ? 1000 : unit === "m" ? 60_000 : 3_600_000;
            const value = Number(match[1]) * multiplier;
            if (!Number.isSafeInteger(value) || value < 1_000 || value > 3_600_000) {
                pushIssue(issues, "CORE_XRAY_FRAGMENT_PROBE_INTERVAL", "must be between 1s and 1h");
            }
        }
    }
    parseBooleanField(issues, "CORE_XRAY_FRAGMENT_OBSERVATORY_CONCURRENCY", env.CORE_XRAY_FRAGMENT_OBSERVATORY_CONCURRENCY, false);
    validateToken(issues, "CORE_DOH_PATH_SECRET", env.CORE_DOH_PATH_SECRET, false);
    validateToken(issues, "CORE_OUTPUT_PATH_SECRET", env.CORE_OUTPUT_PATH_SECRET, false);
    validateTokenUniqueness(issues, env);
    if (issues.length) {
        const error = new ConfigurationValidationError(issues);
        cachedValidationSignature = signature;
        cachedValidation = error;
        throw error;
    }
    const result = { outputSecurity };
    cachedValidationSignature = signature;
    cachedValidation = result;
    return result;
}

};

__m["output/render"] = function(module, exports, __r) {
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.decodeStandardBase64 = decodeStandardBase64;
exports.renderSubscription = renderSubscription;
const errors_1 = __r("../core/errors");
const cloudflare_1 = __r("../storage/cloudflare");
const service_1 = __r("./service");
function decodeStandardBase64(value) {
    const normalized = value.replace(/\s+/g, "");
    if (!normalized || normalized.length % 4 !== 0 || /[^A-Za-z0-9+/=]/.test(normalized))
        throw new Error("Invalid standard Base64");
    const binary = atob(normalized);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++)
        bytes[i] = binary.charCodeAt(i);
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}
async function renderSubscription(env, request) {
    const params = new URL(request.url).searchParams;
    const encode = (params.get("encode") ?? "base64").trim().toLowerCase();
    if (encode !== "base64" && encode !== "raw")
        throw new errors_1.CoreError("BAD_REQUEST", "encode must be base64 or raw");
    const rendered = await (0, service_1.renderSubscriptionWithProvider)(env, request, (0, cloudflare_1.createCredentialProvider)(env));
    return { body: rendered.body, contentType: "text/plain; charset=utf-8", encoding: encode, count: rendered.count };
}

};

__m["core/index"] = function(module, exports, __r) {
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
__exportStar(__r("./contracts"), exports);
__exportStar(__r("./limits"), exports);
__exportStar(__r("./errors"), exports);
__exportStar(__r("./byte-queue"), exports);
__exportStar(__r("./engine"), exports);

};

__m["transport/websocket/index"] = function(module, exports, __r) {
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
__exportStar(__r("./adapter"), exports);

};

const __entry = __load("", "worker");
export default __entry.default;
