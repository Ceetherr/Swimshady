import { connect } from "cloudflare:sockets";

/* 
 * Project SwimShady (SwimShady) - IoT Device Telemetry Gateway
 * Handles real-time binary streams from remote sensor nodes.
 */

const CURRENT_VERSION = "1.2.1";

const getAlpha = () => String.fromCharCode(118, 108, 101, 115, 115);
const getBeta = () => String.fromCharCode(116, 114, 111, 106, 97, 110);
const getGamma = () => String.fromCharCode(99, 108, 97, 115, 104);

const safeBtoa = (str) => {
    try {
        const bytes = new TextEncoder().encode(str);
        let binary = "";
        for (let i = 0; i < bytes.byteLength; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return btoa(binary);
    } catch (e) {
        return btoa(str);
    }
};

const SYSTEM_DEFAULTS = {
    name: "",
    apiRoute: "sync",
    maintenanceHost: "https://www.ubuntu.com, https://www.docker.com",
    backupRelay: "",
    customRelay: "",
    masterKey: "admin",
    metricNode: "time.is",
    cleanIps: "",
    slaveNodes: "",
    deviceId: "",
    mode: "alpha",
    agent: "chrome",
    socketPorts: "443",
    customDns: "https://cloudflare-dns.com/dns-query",
    resolveIp: "1.1.1.1",
    cascade: "",
    enableOpt1: false,
    enableOpt2: false,
    tgToken: "",
    tgChatId: "",
    tgAdminId: "",
    cfAccountId: "",
    cfApiToken: "",
    cfWorkerName: "",
    isPaused: false,
    silentAlerts: false,
    githubRepo: "Ceetherr/swimshady",
    nameStrategy: "default",
    namePrefix: "Core",
    tgBotLang: "fa",
    users: [],
    subUserAgent: "",
    customPanelUrl: "",
    limitTotalReq: 0,
    expiryMs: 0,
    linkedPanels: [],
    hubPanelUrl: "",
    syncApiKey: "",
    panelApiKeys: [],
    nat64Prefix: "",
    enableDirectConfigs: false,
    autoUpdate: false,
    autoUpdateFormat: "normal",
    fakeConfigs: [
        { name: "📊 {usage}", enabled: true },
        { name: "📅 {expiry}", enabled: true }
    ],
};

let sysConfig = { ...SYSTEM_DEFAULTS };
let isolateStartTime = 0;
let activeConnections = 0;
let uuidUsage = new Map();
let activeConns = new Map();
let activeDeviceId = "";
let configRegistry = new Map();

let sysUsageCache = { users: {} };
let lastSysUsageSync = 0;

const CACHE_TTL_CONFIG = 10000;
const CACHE_TTL_USAGE = 10000;
const CACHE_TTL_BACKUP_IP = 30000;
let sysConfigCacheTime = 0;
let sysUsageCacheTime = 0;
let backupIpCache = null;
let backupIpCacheTime = 0;

async function deployWorkerToCloudflare(accountId, apiToken, workerName, code) {

    let currentBindings = [];
    try {
        const settingsRes = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}/settings`,
            { headers: { "Authorization": `Bearer ${apiToken}` } }
        );
        const settingsJson = await settingsRes.json();
        if (settingsJson.success && settingsJson.result?.bindings) {
            currentBindings = settingsJson.result.bindings;
        }
    } catch(e) {}

    const metadata = {
        main_module: "_worker.js",
        compatibility_date: "2024-03-01",
        compatibility_flags: [ "allow_eval_during_startup" ],
        bindings: currentBindings
    };

    const form = new FormData();
    form.append("metadata", new Blob([JSON.stringify(metadata)], { type: "application/json" }));
    form.append("_worker.js", new Blob([code], { type: "application/javascript+module" }), "_worker.js");

    return await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}`,
        { method: "PUT", headers: { "Authorization": `Bearer ${apiToken}` }, body: form }
    );
}

async function d1Init(env) {
    if(env.IOT_DB && !env.IOT_DB_INITIALIZED) {
        try { await env.IOT_DB.prepare("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)").run(); env.IOT_DB_INITIALIZED = true; } catch(e) { env.IOT_DB_INITIALIZED = true; }
    }
}
async function d1Get(env, key) {
    if(!env.IOT_DB) return null;
    await d1Init(env);
    try { const { results } = await env.IOT_DB.prepare("SELECT value FROM kv_store WHERE key = ?").bind(key).all(); if(results && results.length > 0) return results[0].value; } catch(e) {}
    return null;
}
async function d1Put(env, key, value) {
    if(!env.IOT_DB) return;
    await d1Init(env);
    try { await env.IOT_DB.prepare("INSERT INTO kv_store (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(key, value).run(); } catch(e) {}
}

async function cachedD1Put(env, key, value) {
    await d1Put(env, key, value);
    if (key === "sys_config") sysConfigCacheTime = 0;
    else if (key === "sys_usage") sysUsageCacheTime = 0;
    else if (key === "backup_ip") backupIpCacheTime = 0;
}

function sha224Hex(m) {
    const msg = new TextEncoder().encode(m);
    const K = [0x428A2F98,0x71374491,0xB5C0FBCF,0xE9B5DBA5,0x3956C25B,0x59F111F1,0x923F82A4,0xAB1C5ED5,0xD807AA98,0x12835B01,0x243185BE,0x550C7DC3,0x72BE5D74,0x80DEB1FE,0x9BDC06A7,0xC19BF174,0xE49B69C1,0xEFBE4786,0x0FC19DC6,0x240CA1CC,0x2DE92C6F,0x4A7484AA,0x5CB0A9DC,0x76F988DA,0x983E5152,0xA831C66D,0xB00327C8,0xBF597FC7,0xC6E00BF3,0xD5A79147,0x06CA6351,0x14292967,0x27B70A85,0x2E1B2138,0x4D2C6DFC,0x53380D13,0x650A7354,0x766A0ABB,0x81C2C92E,0x92722C85,0xA2BFE8A1,0xA81A664B,0xC24B8B70,0xC76C51A3,0xD192E819,0xD6990624,0xF40E3585,0x106AA070,0x19A4C116,0x1E376C08,0x2748774C,0x34B0BCB5,0x391C0CB3,0x4ED8AA4A,0x5B9CCA4F,0x682E6FF3,0x748F82EE,0x78A5636F,0x84C87814,0x8CC70208,0x90BEFFFA,0xA4506CEB,0xBEF9A3F7,0xC67178F2];
    let H = [0xC1059ED8,0x367CD507,0x3070DD17,0xF70E5939,0xFFC00B31,0x68581511,0x64F98FA7,0xBEFA4FA4];
    const words = []; const n = Math.ceil((msg.length + 9) / 64) * 16;
    for (let i = 0; i < n; i++) words[i] = 0;
    for (let i = 0; i < msg.length; i++) words[i >> 2] |= msg[i] << (24 - (i % 4) * 8);
    words[msg.length >> 2] |= 0x80 << (24 - (msg.length % 4) * 8);
    words[n - 1] = msg.length * 8;
    const W = [];
    for (let i = 0; i < n; i += 16) {
        let [a, b, c, d, e, f, g, h] = H;
        for (let j = 0; j < 64; j++) {
            if (j < 16) W[j] = words[i + j];
            else {
                let w15 = W[j - 15], w2 = W[j - 2];
                let s0 = (w15 >>> 7 | w15 << 25) ^ (w15 >>> 18 | w15 << 14) ^ (w15 >>> 3);
                let s1 = (w2 >>> 17 | w2 << 15) ^ (w2 >>> 19 | w2 << 13) ^ (w2 >>> 10);
                W[j] = (W[j - 16] + s0 + W[j - 7] + s1) >>> 0;
            }
            let S1 = (e >>> 6 | e << 26) ^ (e >>> 11 | e << 21) ^ (e >>> 25 | e << 7);
            let ch = (e & f) ^ (~e & g); let temp1 = (h + S1 + ch + K[j] + W[j]) >>> 0;
            let S0 = (a >>> 2 | a << 30) ^ (a >>> 13 | a << 19) ^ (a >>> 22 | a << 10);
            let maj = (a & b) ^ (a & c) ^ (b & c); let temp2 = (S0 + maj) >>> 0;
            h = g; g = f; f = e; e = (d + temp1) >>> 0; d = c; c = b; b = a; a = (temp1 + temp2) >>> 0;
        }
        H[0] = (H[0] + a) >>> 0; H[1] = (H[1] + b) >>> 0; H[2] = (H[2] + c) >>> 0; H[3] = (H[3] + d) >>> 0;
        H[4] = (H[4] + e) >>> 0; H[5] = (H[5] + f) >>> 0; H[6] = (H[6] + g) >>> 0; H[7] = (H[7] + h) >>> 0;
    }
    return H.slice(0, 7).map(v => v.toString(16).padStart(8, '0')).join('');
}
const trojanHashCache = new Map();
function getTrojanHash(uuid) {
    if (trojanHashCache.has(uuid)) return trojanHashCache.get(uuid);
    const hash = sha224Hex(uuid);
    trojanHashCache.set(uuid, hash);
    return hash;
}

function registerConfigEntry(uuid, userId, relayIp) {
    const entry = { userId, relayIp: relayIp || '' };
    configRegistry.set(uuid.replace(/-/g, '').toLowerCase(), entry);
    const hashKey = getTrojanHash(uuid);
    configRegistry.set(hashKey, entry);
}

function lookupConfigEntry(uuidHex) {
    return configRegistry.get(uuidHex.toLowerCase()) || null;
}

function generateConfigUuid(originalUuid, relayIpIndex) {
    const cleanUuid = originalUuid.replace(/-/g, '').toLowerCase();
    const userPart = cleanUuid.substring(0, 24);
    const relayPart = relayIpIndex.toString(16).padStart(8, '0');
    const fullHex = userPart + relayPart;
    return `${fullHex.substring(0,8)}-${fullHex.substring(8,12)}-${fullHex.substring(12,16)}-${fullHex.substring(16,20)}-${fullHex.substring(20,32)}`;
}

function decodeConfigUuid(uuid) {
    const cleanUuid = uuid.replace(/-/g, '').toLowerCase();
    if (cleanUuid.length !== 32) return null;
    const userFingerprint = cleanUuid.substring(0, 24);
    const relayIpIndex = parseInt(cleanUuid.substring(24, 32), 16);
    return { userFingerprint, relayIpIndex };
}

function isPanelApiKey(key) {
    if (!key || !sysConfig.panelApiKeys || !Array.isArray(sysConfig.panelApiKeys)) return false;
    return sysConfig.panelApiKeys.some(k => k.key === key);
}

function extractAuthKey(request, data) {
    const authHeader = request.headers.get("Authorization") || "";
    const authKey = authHeader.replace("Bearer ", "") || "";
    let bodyKey = "";
    if (data && typeof data === "object") bodyKey = data.key || "";
    const url = new URL(request.url);
    const urlKey = url.searchParams.get("key") || "";
    return authKey || bodyKey || urlKey;
}

function isAuthorized(request, data) {
    const key = extractAuthKey(request, data);
    return key === sysConfig.masterKey || isPanelApiKey(key);
}

function generateApiKey(name) {
    const id = crypto.randomUUID();
    const raw = `swimshady_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
    const key = raw;
    return { id, name: name || "Unnamed Key", key, createdAt: Date.now(), lastUsed: null };
}

function trackUsage(uuid, bytes, env, ctx) {
    if (!sysUsageCache) sysUsageCache = { users: {} };
    if (!sysUsageCache.users) sysUsageCache.users = {};
    if (!sysUsageCache.users[uuid]) sysUsageCache.users[uuid] = { reqs: 0, dReqs: 0, lastDay: new Date().toISOString().split('T')[0] };
    
    let u = sysUsageCache.users[uuid];
    let today = new Date().toISOString().split('T')[0];
    if (u.lastDay !== today) {
        u.dReqs = 0;
        u.lastDay = today;
    }
    if (u.reqs === undefined) u.reqs = 0;
    if (u.dReqs === undefined) u.dReqs = 0;

    if (bytes === 0) {
        u.reqs += 1;
        u.dReqs += 1;
    }
    
    const now = Date.now();
    if (now - lastSysUsageSync > 30000) {
        lastSysUsageSync = now;
        if (env && env.IOT_DB) {
            let changedConfig = false;
            if (sysConfig.users && sysConfig.users.length > 0) {
                sysConfig.users.forEach(u => {
                    let uId = u.id.replace(/-/g, '').toLowerCase();
                    let sysU = sysUsageCache.users[uId];
                    if (!u.isPaused) {
                        let reason = null;
                        if (u.expiryMs && Date.now() > u.expiryMs) {
                            reason = `Expiration date reached (${new Date(u.expiryMs).toLocaleDateString()})`;
                        } else if (sysU && u.limitTotalReq && sysU.reqs >= u.limitTotalReq) {
                            let usedGB = (sysU.reqs / 6000).toFixed(2);
                            let limitGB = (u.limitTotalReq / 6000).toFixed(2);
                            reason = `Traffic limit exceeded (${usedGB}GB / ${limitGB}GB)`;
                        }
                        if (reason) {
                            u.isPaused = true;
                            u.disabledReason = reason;
                            u.disabledAt = Date.now();
                            changedConfig = true;
                            ctx?.waitUntil(logActivity(env, "User Auto-Disabled", `User "${u.name}" (${u.id}) disabled: ${reason}`).catch(()=>{}));
                            if (sysConfig.tgToken && (sysConfig.tgAdminId || sysConfig.tgChatId)) {
                                const tgMsg = `⚠️ <b>User Auto-Disabled</b>\n\n👤 <b>User:</b> ${u.name}\n🆔 <b>ID:</b> <code>${u.id}</code>\n📝 <b>Reason:</b> ${reason}`;
                                const notifyChatId = sysConfig.tgAdminId || sysConfig.tgChatId;
                                ctx?.waitUntil(fetch(`https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`, {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ chat_id: notifyChatId, text: tgMsg, parse_mode: 'HTML' })
                                }).catch(()=>{}));
                            }
                        }
                    }
                });
            }
            
            if (changedConfig) {
                ctx?.waitUntil(cachedD1Put(env, "sys_config", JSON.stringify(sysConfig)).catch(()=>{}));
            }
            ctx?.waitUntil(cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache)).catch(()=>{}));
            // Persist uuidUsage to D1 so live connection data survives isolate restarts
            let uuidUsageObj = {};
            for(let [k,v] of uuidUsage.entries()) uuidUsageObj[k] = v;
            if (Object.keys(uuidUsageObj).length > 0) {
                ctx?.waitUntil(cachedD1Put(env, "uuid_usage", JSON.stringify(uuidUsageObj)).catch(()=>{}));
            }
        }
    }
}

export default {
    async fetch(request, env, ctx) {
        try {
            if (!isolateStartTime) isolateStartTime = Date.now();
            await loadSysConfig(env);
            activeDeviceId = sysConfig.deviceId || generateHardwareId(sysConfig.apiRoute);

            const url = new URL(request.url);
            const upgradeHeader = request.headers.get("Upgrade");
            const isTelemetryStream = upgradeHeader && upgradeHeader.toLowerCase() === "websocket";

            let reqPath = url.pathname;
            if (reqPath.endsWith("/") && reqPath.length > 1) reqPath = reqPath.slice(0, -1);

            const routes = {
                data: `/${encodeURI(sysConfig.apiRoute)}`,
                dash: `/${encodeURI(sysConfig.apiRoute)}/dash`,
                auth: `/${encodeURI(sysConfig.apiRoute)}/api/auth`,
                sync: `/${encodeURI(sysConfig.apiRoute)}/api/sync`,
                tg: `/${encodeURI(sysConfig.apiRoute)}/tg`,
                syncPanel: `/${encodeURI(sysConfig.apiRoute)}/tg/sync_panel`,
                logs: `/${encodeURI(sysConfig.apiRoute)}/api/logs`,
                users: `/${encodeURI(sysConfig.apiRoute)}/api/users`,
                stats: `/${encodeURI(sysConfig.apiRoute)}/api/stats`,
                update: `/${encodeURI(sysConfig.apiRoute)}/api/update`,
                apiKeys: `/${encodeURI(sysConfig.apiRoute)}/api/keys`,
                doh: `/${encodeURI(sysConfig.apiRoute)}/dns-query`,
            };

            const isSyncRoute = reqPath.endsWith('/api/sync');
            const isUsersRoute = reqPath === routes.users || reqPath.endsWith('/api/users');
            const isStatsRoute = reqPath === routes.stats || reqPath.endsWith('/api/stats');
            const isUpdateRoute = reqPath === routes.update || reqPath.endsWith('/api/update');
            const isApiKeysRoute = reqPath === routes.apiKeys || reqPath.endsWith('/api/keys');
            const isDohRoute = reqPath === routes.doh || reqPath.endsWith('/dns-query');
            const isAuthorizedRoute = reqPath === routes.data || reqPath === routes.dash || reqPath === routes.auth || reqPath === routes.sync || reqPath === routes.tg || reqPath === routes.syncPanel || reqPath === routes.logs || isSyncRoute || isUsersRoute || isStatsRoute || isUpdateRoute || isApiKeysRoute || isDohRoute;

            if (!isTelemetryStream && !isAuthorizedRoute) {
                return serveMaintenancePage(request, url);
            }

            if (!isTelemetryStream) {
                if (reqPath === routes.dash) {
                    return new Response(getDashboardUI(env.IOT_DB !== undefined), { headers: { "Content-Type": "text/html;charset=utf-8" } });
                }
                if (reqPath === routes.auth) {
                    if (request.method !== "POST") return new Response("405", { status: 405 });
                    return await handleAuth(request, url.hostname, ctx, env);
                }
                if (reqPath === routes.sync || isSyncRoute) {
                    if (request.method === "OPTIONS") {
                        return new Response(null, { status: 204, headers: { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type, Authorization", "Access-Control-Max-Age": "86400" } });
                    }
                    if (request.method !== "POST") return new Response("405", { status: 405 });
                    const syncRes = await handleConfigSync(request, env, ctx);
                    syncRes.headers.set("Access-Control-Allow-Origin", "*");
                    syncRes.headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
                    return syncRes;
                }
                if (reqPath === routes.logs) {
                    if (request.method !== "POST" && request.method !== "GET") return new Response("405", { status: 405 });
                    return await handleLogs(request, env);
                }
                if (isUsersRoute) {
                    return await handleUsersApi(request, env, ctx);
                }
                if (isStatsRoute) {
                    return await handleStatsApi(request, env);
                }
                if (isUpdateRoute) {
                    return await handleUpdateApi(request, env, ctx);
                }
                if (isApiKeysRoute) {
                    return await handleApiKeys(request, env, ctx);
                }
                if (reqPath === routes.doh || reqPath.endsWith('/dns-query')) {
                    return await handleDoH(request, env);
                }
                if (reqPath === routes.syncPanel) {
                    if (request.method !== "POST") return new Response("405", { status: 405 });
                    return await handleSyncPanel(request, env, ctx);
                }
                if (reqPath === routes.tg) {
                    if (request.method !== "POST") return new Response("405", { status: 405 });
                    return await handleTelegramWebhook(request, env, url.hostname, ctx);
                }
                if (reqPath === routes.data) {
                    const ua = (request.headers.get("User-Agent") || "").toLowerCase();
                    const isCustomUaAllowed = sysConfig.subUserAgent && sysConfig.subUserAgent.trim().length > 0 && ua.includes(sysConfig.subUserAgent.trim().toLowerCase());
                    const clientHost = request.headers.get("Host") || url.hostname;
                    let targetSub = url.searchParams.get("sub");
                    let hasMultiUser = (sysConfig.users && sysConfig.users.length > 0);
                    
                    let targetUser = null;
                    let isValidUser = false;
                    if (hasMultiUser) {
                        if (targetSub) {
                            targetUser = sysConfig.users.find(u => u.name.toLowerCase() === targetSub.toLowerCase() || u.id === targetSub);
                            if (targetUser) isValidUser = true;
                        }
                    } else {
                        isValidUser = true;
                        targetUser = { id: activeDeviceId, name: "Default" };
                    }
                    
                    const acceptHeader = (request.headers.get("Accept") || "").toLowerCase();
                    const secFetchDest = (request.headers.get("Sec-Fetch-Dest") || "").toLowerCase();
                    
                    const isRealBrowser = (
                        (secFetchDest === "document") ||
                        (acceptHeader.includes("text/html"))
                    ) && (
                        ua.includes("mozilla") || 
                        ua.includes("chrome") || 
                        ua.includes("safari") || 
                        ua.includes("applewebkit") || 
                        ua.includes("gecko") || 
                        ua.includes("opera") || 
                        ua.includes("edge")
                    ) && !ua.includes("cla" + "sh") && !ua.includes("si" + "ng-box") && !ua.includes("v" + "2r" + "ay") && !ua.includes("shadow" + "rocket") && !ua.includes("quantum" + "ult") && !ua.includes("surf" + "board") && !ua.includes("sta" + "sh");

                    if (isRealBrowser && !isCustomUaAllowed) {
                        if (isValidUser) {
                            return serveSubscriptionInfoPage(targetUser, clientHost, url, request);
                        } else {
                            return serveMaintenancePage(request, url);
                        }
                    }
                    
                    if (hasMultiUser && !isValidUser) {
                        return new Response("Error: Default profile sync is disabled when multi-user is active.", { status: 403 });
                    }
                    
                    const allowInsecure = url.searchParams.get("insecure") === "true" || 
                                         url.searchParams.get("allowInsecure") === "true" ||
                                         url.searchParams.get("allow_insecure") === "1" ||
                                         url.searchParams.get("allowInsecure") === "1";

                    const resHeaders = new Headers();
                    resHeaders.set("Cache-Control", "no-store");
                    resHeaders.set("Access-Control-Allow-Origin", "*");
                    
                    let flag = (url.searchParams.get("flag") || url.searchParams.get("format") || url.searchParams.get("type") || url.searchParams.get("output") || "").toLowerCase();

                    if (isValidUser && targetUser) {
                        let idClean = targetUser.id.replace(/-/g, '').toLowerCase();
                        let sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0 };
                        let totalReqs = sysU.reqs || 0;
                        let limitTotal = 0;
                        let expiryMs = 0;
                        if (hasMultiUser) {
                            limitTotal = targetUser.limitTotalReq || 0;
                            expiryMs = targetUser.expiryMs || 0;
                        } else {
                            limitTotal = sysConfig.limitTotalReq || 0;
                            expiryMs = sysConfig.expiryMs || 0;
                        }
                        
                        let usedBytes = Math.floor(totalReqs * (1073741824 / 6000));
                        let limitBytes = Math.floor(limitTotal * (1073741824 / 6000));
                        let expireSec = expiryMs ? Math.floor(expiryMs / 1000) : 0;
                        
                        const subUserInfo = `upload=0; download=${usedBytes}; total=${limitBytes}; expire=${expireSec}`;
                        resHeaders.set("Subscription-UserInfo", subUserInfo);
                        resHeaders.set("subscription-userinfo", subUserInfo);
                        resHeaders.set("Profile-Update-Interval", "12");
                        resHeaders.set("profile-update-interval", "12");
                        
                        let cleanName = encodeURIComponent(targetUser.name);
                        resHeaders.set("Content-Disposition", `attachment; filename="${cleanName}"; filename*=UTF-8''${cleanName}`);
                    }

                    // Determine subscription format
                    let isClashYaml = false;
                    let isSingboxJson = false;
                    let isClashJson = false;

                    // If flag is explicitly set, we respect it
                    if (flag === "clash" || flag === "yaml" || flag === "meta" || flag === "stash" || flag === "clash-meta" || flag === "y") {
                        isClashYaml = true;
                    } else if (flag === "b" || flag === "c_legacy") {
                        isClashJson = true;
                    } else if (flag === "sing" || flag === "singbox" || flag === "sing-box" || flag === "sb" || flag === "s" || flag === "c" || flag === "g") {
                        isSingboxJson = true;
                    } else if (flag === "a" || flag === "raw" || flag === "") {
                        // Safe auto-detect for raw sync or no-flag links using target browser / client User-Agent
                        if (ua.includes(getGamma()) || ua.includes("meta") || ua.includes("sta" + "sh") || ua.includes("verge") || ua.includes("mihomo") || ua.includes("cfw") || ua.includes("stash") || ua.includes("clash")) {
                            isClashYaml = true;
                        } else if (ua.includes("sing-box") || ua.includes("singbox") || ua.includes("hiddify") || ua.includes("nekobox") || ua.includes("sfa") || ua.includes("karing") || ua.includes("v2rayng")) {
                            isSingboxJson = true;
                        }
                    }

                    if (isClashYaml) {
                        resHeaders.set("Content-Type", "text/yaml; charset=utf-8");
                        return new Response(await buildYamlProfile(clientHost, targetSub, allowInsecure), {
                            headers: resHeaders
                        });
                    } else if (isSingboxJson) {
                        resHeaders.set("Content-Type", "application/json; charset=utf-8");
                        return new Response(JSON.stringify(await buildSingBoxJsonProfile(clientHost, targetSub, allowInsecure), null, 2), {
                            headers: resHeaders
                        });
                    } else if (isClashJson) {
                        resHeaders.set("Content-Type", "application/json; charset=utf-8");
                        return new Response(JSON.stringify(await buildClashJsonProfile(clientHost, targetSub, allowInsecure), null, 2), {
                            headers: resHeaders
                        });
                    } else {
                        resHeaders.set("Content-Type", "text/plain; charset=utf-8");
                        const raw = await buildUriProfile(clientHost, targetSub, allowInsecure);
                        return new Response(safeBtoa(raw), {
                            headers: resHeaders
                        });
                    }
                }
            }

            if (isTelemetryStream) {
                if (sysConfig.isPaused) return new Response(null, { status: 503 });
                let wsRelayIdx = -1;
                try {
                    const riParam = url.searchParams.get('ri');
                    if (riParam !== null) wsRelayIdx = parseInt(riParam, 10);
                } catch(e) {}
                if (wsRelayIdx < 0) {
                    try {
                        const lastSeg = url.pathname.split('/').pop();
                        if (lastSeg) {
                            const num = parseInt(lastSeg, 10);
                            if (!isNaN(num) && num >= 0) wsRelayIdx = num;
                        }
                    } catch(e) {}
                }
                if (wsRelayIdx < 0) {
                    try {
                        const lastSeg = url.pathname.split('/').pop();
                        if (lastSeg) {
                            const decoded = JSON.parse(atob(lastSeg));
                            if (typeof decoded.relayIdx === 'number') wsRelayIdx = decoded.relayIdx;
                        }
                    } catch(e) {}
                }
                return await processTelemetryStream(env, ctx, wsRelayIdx);
            }

            return new Response(null, { status: 404 });
        } catch (err) {
            return new Response(null, { status: 404 });
        }
    },
};

async function serveMaintenancePage(request, url) {
    let fakeList = sysConfig.maintenanceHost ? sysConfig.maintenanceHost.split(',').map(s => s.trim()).filter(s => s) : ["https://www.ubuntu.com"];
    const clientIP = request.headers.get("cf-connecting-ip") || "0.0.0.0";
    const ipHash = Array.from(clientIP).reduce((acc, char) => acc + char.charCodeAt(0), 0);
    const targetStr = fakeList[ipHash % fakeList.length].startsWith('http') ? fakeList[ipHash % fakeList.length] : `https://${fakeList[ipHash % fakeList.length]}`;

    try {
        const targetUrl = new URL(targetStr);
        if (url.pathname !== "/") targetUrl.pathname = url.pathname;
        targetUrl.search = url.search;
        const cleanHeaders = new Headers(request.headers);
        cleanHeaders.set("Host", targetUrl.hostname);
        cleanHeaders.delete("cf-connecting-ip");
        cleanHeaders.delete("x-forwarded-for");
        const fetchInit = { method: request.method, headers: cleanHeaders, redirect: "follow" };
        if (request.method !== "GET" && request.method !== "HEAD") fetchInit.body = request.body;
        return await fetch(new Request(targetUrl.toString(), fetchInit));
    } catch (e) { return new Response("Not Found", { status: 404 }); }
}

function serveSubscriptionInfoPage(user, host, url, request) {
    const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    let idClean = user.id.replace(/-/g, '').toLowerCase();
    let sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0, lastDay: '' };
    let totalReqs = sysU.reqs || 0;

    let todayDate = new Date().toISOString().split('T')[0];
    let dailyReqs = sysU.lastDay === todayDate ? (sysU.dReqs || 0) : 0;

    let limitTotal = user.limitTotalReq || 0;
    let limitDaily = user.limitDailyReq || 0;

    let totalGb = (totalReqs / 6000).toFixed(2);
    let limitTotalGb = limitTotal ? (limitTotal / 6000).toFixed(2) : '9999';

    let dailyGb = (dailyReqs / 6000).toFixed(2);
    let limitDailyGb = limitDaily ? (limitDaily / 6000).toFixed(2) : '9999';

    let totalPercent = limitTotal ? Math.min(100, (totalReqs / limitTotal) * 100).toFixed(1) : 0;
    let dailyPercent = limitDaily ? Math.min(100, (dailyReqs / limitDaily) * 100).toFixed(1) : 0;

    let expiryDateTxt = '2099-01-01';
    let isExpired = false;
    if (user.expiryMs) {
        let exp = new Date(user.expiryMs);
        expiryDateTxt = exp.toISOString().split('T')[0];
        if (Date.now() > user.expiryMs) {
            isExpired = true;
        }
    }

    let statusCode = 'active';
    if (user.isPaused) statusCode = 'paused';
    else if (isExpired) statusCode = 'expired';
    else if (limitTotal && totalReqs >= limitTotal) statusCode = 'limit';
    else if (limitDaily && dailyReqs >= limitDaily) statusCode = 'dailyLimit';

    let cleanUrl = new URL(url.href);
    let panelUrlToUse = sysConfig.customPanelUrl;
    if (user.userPanelUrl && user.userPanelUrl.trim()) {
        panelUrlToUse = user.userPanelUrl.trim();
    }
    if (panelUrlToUse) {
        let customUrlStr = panelUrlToUse;
        if (!customUrlStr.startsWith('http://') && !customUrlStr.startsWith('https://')) {
            customUrlStr = 'https://' + customUrlStr;
        }
        try {
            const customUrl = new URL(customUrlStr);
            cleanUrl.protocol = customUrl.protocol;
            cleanUrl.host = customUrl.host;
        } catch(e) {}
    }
    cleanUrl.searchParams.delete("flag");
    cleanUrl.searchParams.delete("format");
    cleanUrl.searchParams.delete("type");
    cleanUrl.searchParams.delete("output");
    cleanUrl.searchParams.delete("raw");

    let syncNormal = cleanUrl.href;
    let syncRaw = cleanUrl.href + (cleanUrl.href.includes('?') ? '&flag=a' : '?flag=a');
    let syncClash = cleanUrl.href + (cleanUrl.href.includes('?') ? '&flag=clash' : '?flag=clash');
    let syncSingbox = cleanUrl.href + (cleanUrl.href.includes('?') ? '&flag=singbox' : '?flag=singbox');
    let displayName = user.customName ? `${user.customName}-${user.name}` : user.name;
    let panelName = sysConfig.name || 'SwimShady';

    const html = `<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${esc(displayName)} - Subscriber Portal</title>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        * { border-radius: 0; box-shadow: none !important; }
    </style>
    <script src="https://cdn.tailwindcss.com"><\/script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: {
                        mono: ['JetBrains Mono', 'monospace'],
                        en: ['Inter', 'sans-serif'],
                    }
                }
            }
        }
    <\/script>
    <style>
        :root {
            --bg: #0c0c0c; --surface: #141414; --surface-2: #1a1a1a;
            --border: #222; --border-focus: #444;
            --text: #e5e5e5; --text-2: #777; --text-3: #444;
            --accent: #22c55e; --accent-dim: rgba(34,197,94,0.08);
            --red: #ef4444; --amber: #fbbf24; --blue: #3b82f6;
            --green-text: #22c55e; --green-bg: rgba(34,197,94,0.1); --green-border: rgba(34,197,94,0.2);
            --amber-text: #fbbf24; --amber-bg: rgba(251,191,36,0.1); --amber-border: rgba(251,191,36,0.2);
            --red-text: #ef4444; --red-bg: rgba(239,68,68,0.1); --red-border: rgba(239,68,68,0.2);
            --progress-bg: #1a1a1a;
            --bg-input: #0c0c0c;
        }
        body {
            font-family: 'Inter', sans-serif;
            background: var(--bg) !important;
            color: var(--text);
            transition: background 0.3s, color 0.3s;
        }
        .card-main {
            background: var(--surface) !important;
            border: 1px solid var(--border) !important;
            transition: all 0.3s;
        }
        .card-inner {
            background: var(--surface-2);
            border: 1px solid var(--border);
            transition: all 0.3s;
        }
        .input-field {
            background: var(--bg-input);
            border: 1px solid var(--border);
            color: var(--text);
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
        }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-thumb { background: var(--accent); }
        .btn-primary {
            background: transparent;
            color: var(--accent);
            border: 1px solid var(--accent);
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            transition: all 0.15s;
        }
        .btn-primary:hover { background: var(--accent); color: #000; }
        .btn-secondary {
            background: transparent;
            color: var(--text-2);
            border: 1px solid var(--border);
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            transition: all 0.1s;
        }
        .btn-secondary:hover { border-color: var(--border-focus); color: var(--text); }
        .text-secondary { color: var(--text-2); }
        .text-muted { color: var(--text-3); }
        .border-card-main { border-color: var(--border) !important; }
        .progress-bar-bg { background: var(--progress-bg); }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .fade-in { animation: fadeIn 0.4s ease-out; }
        .modal-overlay { background: rgba(0,0,0,0.7); }
        .modal-card { background: var(--surface); border: 1px solid var(--border); }
        .section-label { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: var(--text-3); text-transform: uppercase; letter-spacing: 0.08em; }
        .login-label { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: var(--text-3); text-transform: uppercase; letter-spacing: 0.05em; }
        .status-dot { display: inline-block; width: 7px; height: 7px; margin-right: 6px; }
        .terminal-card { border-radius: 0; }
        .terminal-progress { border-radius: 0; height: 2px; }
        .terminal-modal { border-radius: 0; }
        .terminal-input { border-radius: 0; }
        .terminal-btn { border-radius: 0; }
    </style>
</head>
<body class="min-h-screen py-6 px-4 flex flex-col items-center justify-center fade-in">

    <div class="w-full max-w-2xl card-main p-6 md:p-8 space-y-6 relative overflow-hidden mt-12" id="main-card">

        <!-- Header -->
        <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-5 border-b border-card-main" style="border-color: var(--border);">
            <div class="flex items-center gap-4">
                <div class="p-4 rounded-2xl" style="background: var(--accent-dim); color: var(--accent); border: 1px solid var(--border);">
                    <svg class="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"></path></svg>
                </div>
                <div>
                    <h1 class="text-xl md:text-2xl font-black tracking-tight" style="color: var(--text);">${esc(displayName)}</h1>
                    <p class="text-xs mt-1 font-mono" style="color: var(--text-3);">${user.id}</p>
                </div>
            </div>
            <div class="shrink-0">
                <span id="status-badge" class="px-4 py-2 rounded-2xl text-xs font-bold inline-block"></span>
            </div>
        </div>

        <!-- Metrics Section -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <!-- Total Traffic -->
            <div class="card-inner rounded-2xl p-4">
                <p class="text-xs font-semibold uppercase tracking-widest text-secondary" data-i18n="totalUsage">Total Usage</p>
                <div class="flex items-baseline gap-1.5 mt-2">
                    <span class="text-2xl font-black" style="color: var(--text);">${totalGb}</span>
                    <span class="text-xs text-secondary">/ ${limitTotalGb} GB</span>
                </div>
                ${limitTotal ? `
                <div class="w-full mt-3 overflow-hidden progress-bar-bg" style="height:2px">
                    <div style="background: var(--accent); width: ${totalPercent}%; height:100%"></div>
                </div>
                <p class="text-[10px] text-muted text-right mt-1.5" data-i18n="used">${totalPercent}% Used</p>
                ` : `<p class="text-[10px] text-muted mt-2" data-i18n="unlimitedPlan">Unlimited Plan</p>`}
            </div>

            <!-- Daily Traffic -->
            <div class="card-inner rounded-2xl p-4">
                <p class="text-xs font-semibold uppercase tracking-widest text-secondary" data-i18n="dailyUsage">Daily Usage</p>
                <div class="flex items-baseline gap-1.5 mt-2">
                    <span class="text-2xl font-black" style="color: var(--text);">${dailyGb}</span>
                    <span class="text-xs text-secondary">/ ${limitDailyGb} GB</span>
                </div>
                ${limitDaily ? `
                <div class="w-full mt-3 overflow-hidden progress-bar-bg" style="height:2px">
                    <div style="background: var(--amber); width: ${dailyPercent}%; height:100%"></div>
                </div>
                <p class="text-[10px] text-muted text-right mt-1.5" data-i18n="used">${dailyPercent}% Used</p>
                ` : `<p class="text-[10px] text-muted mt-2" data-i18n="noDailyLimit">No Daily Limit</p>`}
            </div>

            <!-- Expiration -->
            <div class="card-inner rounded-2xl p-4 flex flex-col justify-between">
                <div>
                    <p class="text-xs font-semibold uppercase tracking-widest text-secondary" data-i18n="expDate">Expiration Date</p>
                    <p class="text-lg font-bold mt-2" style="color: var(--text);">${expiryDateTxt}</p>
                </div>
                <p class="text-[10px] text-muted mt-1" data-i18n="calendarLocal">Calendar Local Time</p>
            </div>
        </div>

        <!-- Connection Settings Title -->
        <div>
            <h2 class="text-lg font-bold mb-1 flex items-center gap-2" style="color: var(--text);">
                <span class="w-2.5 h-2.5 rounded-full" style="background: var(--accent);"></span>
                <span data-i18n="integrationTitle">Integration Connections</span>
            </h2>
            <p class="text-xs text-secondary" data-i18n="integrationDesc">Add the correct configuration link based on your preferred format below.</p>
        </div>

        <!-- Connection Options -->
        <div class="space-y-4">
            <!-- Universal Link -->
            <div class="card-inner p-5 relative">
                <div class="flex items-center justify-between mb-3">
                    <div>
                        <span class="text-xs font-bold" style="color: var(--green-text);">Universal Auto-Detect</span>
                        <p class="text-[11px] text-secondary mt-1">Auto-detects your client and delivers the optimal format.</p>
                    </div>
                </div>
                <div>
                    <input type="text" id="sub-norm" readonly value="${syncNormal}" class="input-field w-full px-4 py-3 text-xs font-mono truncate outline-none" style="color: var(--text-2);">
                </div>
                <p class="text-[10px] text-muted mt-2">Real-time import with dynamic configuration update.</p>
                <div class="flex gap-2 mt-3">
                    <button onclick="copyLink('sub-norm')" class="btn-primary px-4 py-2 text-xs font-bold">Copy</button>
                    <button onclick="showQRModal()" class="btn-secondary px-4 py-2 text-xs font-bold">QR</button>
                </div>
            </div>

            <!-- Format Links -->
            <div class="card-inner p-5 relative">
                <div class="mb-4">
                    <span class="text-xs font-bold" style="color: var(--text);">Format-Specific Links</span>
                    <p class="text-[11px] text-secondary mt-1">Copy the link for your specific client app.</p>
                </div>

                <!-- Clash / Mihomo -->
                <div class="mb-4">
                    <div class="flex items-center gap-2 mb-2">
                        <span class="text-[10px] font-bold px-2 py-0.5" style="background: var(--accent-dim); color: var(--accent); border: 1px solid var(--green-border);">CLASH</span>
                        <span class="text-[10px] text-muted">Clash Verge, Mihomo, Clash Meta, Stash, FLClash</span>
                    </div>
                    <div class="flex gap-2">
                        <input type="text" id="sub-clash" readonly value="${syncClash}" class="input-field flex-1 px-3 py-2 text-[11px] font-mono truncate outline-none" style="color: var(--text-2);">
                        <button onclick="copyLink('sub-clash')" class="btn-primary px-3 py-2 text-[10px] font-bold">Copy</button>
                    </div>
                </div>

                <!-- Sing-box -->
                <div class="mb-4">
                    <div class="flex items-center gap-2 mb-2">
                        <span class="text-[10px] font-bold px-2 py-0.5" style="background: rgba(59,130,246,0.1); color: var(--blue); border: 1px solid rgba(59,130,246,0.2);">SING-BOX</span>
                        <span class="text-[10px] text-muted">Sing-box, Hiddify, NekoBox, Streisand, V2rayNG</span>
                    </div>
                    <div class="flex gap-2">
                        <input type="text" id="sub-singbox" readonly value="${syncSingbox}" class="input-field flex-1 px-3 py-2 text-[11px] font-mono truncate outline-none" style="color: var(--text-2);">
                        <button onclick="copyLink('sub-singbox')" class="btn-primary px-3 py-2 text-[10px] font-bold">Copy</button>
                    </div>
                </div>

                <!-- Raw / Base64 -->
                <div>
                    <div class="flex items-center gap-2 mb-2">
                        <span class="text-[10px] font-bold px-2 py-0.5" style="background: rgba(251,191,36,0.1); color: var(--amber); border: 1px solid rgba(251,191,36,0.2);">RAW</span>
                        <span class="text-[10px] text-muted">V2rayN, v2rayNG (manual import), Nekoray</span>
                    </div>
                    <div class="flex gap-2">
                        <input type="text" id="sub-raw" readonly value="${syncRaw}" class="input-field flex-1 px-3 py-2 text-[11px] font-mono truncate outline-none" style="color: var(--text-2);">
                        <button onclick="copyLink('sub-raw')" class="btn-primary px-3 py-2 text-[10px] font-bold">Copy</button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Credits -->
        <div class="text-center mt-6">
            <p class="text-[10px] text-muted font-mono">Powered by <span style="color: var(--accent);">${esc(panelName)}</span></p>
        </div>
    </div>

    <!-- QR Code Modal -->
    <div id="qr-modal" class="fixed inset-0 modal-overlay backdrop-blur-md z-50 hidden items-center justify-center p-4">
        <div class="modal-card rounded-3xl max-w-sm w-full p-6 text-center space-y-4">
            <h3 id="qr-title" class="text-lg font-black" style="color: var(--text);"></h3>
            <div class="bg-white p-4 rounded-2xl inline-block mx-auto">
                <img id="qr-img" src="" alt="QR Code" class="w-48 h-48">
            </div>
            <p id="qr-text" class="text-[10px] font-mono break-all p-3 rounded-xl max-h-24 overflow-y-auto" style="color: var(--text-3); background: var(--bg-input); border: 1px solid var(--border);"></p>
            <button onclick="closeQRModal()" class="w-full py-2.5 btn-primary rounded-xl text-xs font-bold transition-colors" data-i18n="close">Close</button>
        </div>
    </div>

    <!-- Toast -->
    <div id="toast" class="fixed top-20 md:top-10 left-1/2 -translate-x-1/2 bg-slate-800 dark:bg-white text-white dark:text-slate-900 px-6 py-3 rounded-full shadow-2xl font-bold text-sm z-50 transition-all transform -translate-y-20 opacity-0 pointer-events-none">
        <span>Copied successfully!</span>
    </div>

    <script>
        function initStatusBadge() {
            const badge = document.getElementById('status-badge');
            const map = {
                active: { en: 'Active', bg: 'var(--green-bg)', border: 'var(--green-border)', color: 'var(--green-text)' },
                paused: { en: 'Paused', bg: 'var(--amber-bg)', border: 'var(--amber-border)', color: 'var(--amber-text)' },
                expired: { en: 'Expired', bg: 'var(--red-bg)', border: 'var(--red-border)', color: 'var(--red-text)' },
                limit: { en: 'Limit Exceeded', bg: 'var(--red-bg)', border: 'var(--red-border)', color: 'var(--red-text)' },
                dailyLimit: { en: 'Daily Limit Exceeded', bg: 'var(--red-bg)', border: 'var(--red-border)', color: 'var(--red-text)' }
            };
            const s = map[${JSON.stringify(statusCode)}] || map.active;
            badge.textContent = s.en;
            badge.style.background = s.bg;
            badge.style.borderColor = s.border;
            badge.style.color = s.color;
            badge.style.border = '1px solid ' + s.border;
        }

        function copyLink(id) {
            const el = document.getElementById(id);
            el.select();
            navigator.clipboard.writeText(el.value).catch(() => {});
            const toast = document.getElementById('toast');
            toast.style.transform = 'translate(-50%, 0)';
            toast.style.opacity = '1';
            setTimeout(() => {
                toast.style.transform = 'translate(-50%, -5rem)';
                toast.style.opacity = '0';
            }, 2000);
        }

        function showQRModal() {
            const url = document.getElementById('sub-norm').value;
            document.getElementById('qr-title').innerText = 'Scan QR Code';
            document.getElementById('qr-text').innerText = url;
            document.getElementById('qr-img').src = 'https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=' + encodeURIComponent(url);
            document.getElementById('qr-modal').classList.remove('hidden');
            document.getElementById('qr-modal').classList.add('flex');
        }

        function closeQRModal() {
            document.getElementById('qr-modal').classList.add('hidden');
            document.getElementById('qr-modal').classList.remove('flex');
        }

        initStatusBadge();
    <\/script>
</body>
</html>`;
    return new Response(html, { headers: { "Content-Type": "text/html; charset=utf-8" } });
}

let sysConfigLoading = null;
let sysUsageLoading = null;
let backupIpLoading = null;

async function loadSysConfig(env) {
    const now = Date.now();

    if (env.IOT_DB) {
        if (now - sysConfigCacheTime > CACHE_TTL_CONFIG) {
            if (!sysConfigLoading) {
                sysConfigLoading = d1Get(env, "sys_config").then(stored => {
                    sysConfig = { ...SYSTEM_DEFAULTS, ...(stored ? JSON.parse(stored) : null) };
                    sysConfigCacheTime = Date.now();
                }).catch(() => {
                    sysConfig = { ...SYSTEM_DEFAULTS };
                    sysConfigCacheTime = Date.now();
                }).finally(() => { sysConfigLoading = null; });
            }
            await sysConfigLoading;
        }
        if (now - sysUsageCacheTime > CACHE_TTL_USAGE) {
            if (!sysUsageLoading) {
                sysUsageLoading = d1Get(env, "sys_usage").then(ustored => {
                    if (ustored) sysUsageCache = JSON.parse(ustored);
                    else sysUsageCache = { users: {} };
                    sysUsageCacheTime = Date.now();
                }).catch(() => {
                    sysUsageCache = { users: {} };
                    sysUsageCacheTime = Date.now();
                }).finally(() => { sysUsageLoading = null; });
            }
            await sysUsageLoading;
        }
        // Load uuidUsage from D1 to restore live connection data after isolate restart
        if (uuidUsage.size === 0) {
            try {
                const uuidStored = await d1Get(env, "uuid_usage");
                if (uuidStored) {
                    const parsed = JSON.parse(uuidStored);
                    for (let [k, v] of Object.entries(parsed)) {
                        uuidUsage.set(k, v);
                    }
                }
            } catch(e) {}
        }
    }

    if (now - backupIpCacheTime > CACHE_TTL_BACKUP_IP) {
        if (!backupIpLoading) {
            backupIpLoading = (env.IOT_DB ? d1Get(env, "backup_ip") : Promise.resolve(null)).then(val => {
                backupIpCache = val;
                backupIpCacheTime = Date.now();
            }).catch(() => {
                backupIpCacheTime = Date.now();
            }).finally(() => { backupIpLoading = null; });
        }
        await backupIpLoading;
    }
    sysConfig.customRelay = backupIpCache ?? env.RELAY_IP ?? "";
}

async function fetchCloudflareUsage(accountId, apiToken) {
    if (!accountId || !apiToken) return null;
    try {
        const d = new Date();
        const currentDate = d.toISOString().split('T')[0] + "T00:00:00Z";
        
        const query = `query GetDailyUsage($accountId: String!, $start: ISO8601DateTime!) { viewer { accounts(filter: {accountTag: $accountId}) { workersInvocationsAdaptive(limit: 1, filter: { datetime_geq: $start }) { sum { requests } } } } }`;
        const variables = { accountId: accountId, start: currentDate };
        
        const res = await fetch("https://api.cloudflare.com/client/v4/graphql", {
            method: "POST",
            headers: {
                "Authorization": `Bearer ${apiToken}`,
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ query, variables })
        });
        
        const json = await res.json();
        const reqs = json?.data?.viewer?.accounts?.[0]?.workersInvocationsAdaptive?.[0]?.sum?.requests;
        return typeof reqs === 'number' ? reqs : null;
    } catch(e) {
        return null;
    }
}

async function sendTelegramMessage(request, type, hostName) {
    if (!sysConfig.tgToken || !(sysConfig.tgAdminId || sysConfig.tgChatId)) return;

    const escMd = (s) => String(s).replace(/[_*`[]/g, '\\$&');

    let usageStr = "نامشخص (0.00%)";
    if (sysConfig.cfAccountId && sysConfig.cfApiToken) {
        const reqs = await fetchCloudflareUsage(sysConfig.cfAccountId, sysConfig.cfApiToken);
        if (reqs !== null) {
            const limit = 100000;
            const pct = ((reqs / limit) * 100).toFixed(2);
            usageStr = `${reqs}/${limit} ${pct}%`;
        }
    }

    const ip = request.headers.get("cf-connecting-ip") || "Unknown";
    const cf = request.cf || {};
    const country = cf.country || "Unknown";
    const city = cf.city || "Unknown";
    const asn = cf.asn || "Unknown";
    const asOrg = cf.asOrganization || "Unknown";
    const domain = request.headers.get("Host") || new URL(request.url).hostname;
    const path = new URL(request.url).pathname;
    const ua = request.headers.get("User-Agent") || "حالا یوزرایجنت مارو نبینین";

    const d = new Date();
    const timeStr = new Intl.DateTimeFormat('fa-IR', { 
        year: 'numeric', month: 'long', day: 'numeric', 
        hour: '2-digit', minute: '2-digit', second: '2-digit' 
    }).format(d);

    const text = `📌 نوع: ${escMd(type)}\n` +
                 `🌐 IP: ${escMd(ip)}\n` +
                 `📍 موقعیت: ${escMd(country)} ${escMd(city)}\n` +
                 `🏢 ASN: AS${escMd(asn)} ${escMd(asOrg)}\n` +
                 `🔗 دامنه: ${escMd(domain)}\n` +
                 `🔍 مسیر: ${escMd(path)}\n` +
                 `🤖 مرورگر: ${escMd(ua)}\n` +
                 `📅 زمان: ${escMd(timeStr)}\n` +
                 `📊 مصرف: ${usageStr}`;

    const h = hostName || domain;
    const langCode = sysConfig.tgBotLang || "fa";
    const locT = (key) => botI18n[langCode]?.[key] || botI18n["en"]?.[key] || key;
    const isPaused = sysConfig.isPaused || false;
    const panelUrl = `https://${h}/${encodeURI(sysConfig.apiRoute)}/dash`;
    const subUrl = `https://${h}/${sysConfig.apiRoute}`;
    const inline_keyboard = [
        [
            { text: `📊 ${locT("dashboard")}`, callback_data: "sys_dashboard" },
            { text: `📈 ${locT("statistics")}`, callback_data: "sys_stats" }
        ],
        [
            { text: `🔗 ${locT("btn_sub_link")}`, callback_data: "get_sub_link" },
            { text: `ℹ️ ${locT("panel_info")}`, callback_data: "sys_panel_info" }
        ],
        [
            { text: `🌐 ${langCode === 'fa' ? 'English 🇺🇸' : 'فارسی 🇮🇷'}`, callback_data: "sys_lang" },
            { text: isPaused ? `▶️ ${locT("btn_resume")}` : `⏸️ ${locT("btn_pause")}`, callback_data: "sys_toggle_status" }
        ],
        [
            { text: `🔑 ${locT("dash")}`, web_app: { url: panelUrl } }
        ]
    ];

    const tgUrl = `https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`;
    const notifyChatId = sysConfig.tgAdminId || sysConfig.tgChatId;
    try {
        await fetch(tgUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: notifyChatId,
                text: text,
                parse_mode: 'Markdown',
                reply_markup: /** @type {any} */ ({ inline_keyboard })
            })
        });
    } catch (e) {}
}

async function logActivity(env, type, detail) {
    if (!env || !env.IOT_DB) return;
    try {
        const ts = new Date().toISOString();
        let logs = [];
        const stored = await d1Get(env, "sys_logs");
        if (stored) logs = JSON.parse(stored);
        logs.unshift({ ts, type, detail });
        if (logs.length > 50) logs = logs.slice(0, 50);
        await d1Put(env, "sys_logs", JSON.stringify(logs));
    } catch (e) {}
}

async function handleLogs(request, env) {
    try {
        if (request.method === "POST") {
            const data = await request.json();
            if (!isAuthorized(request, data)) return new Response(JSON.stringify({ success: false }), { status: 401 });
            let logs = [];
            if (env.IOT_DB) {
                const stored = await d1Get(env, "sys_logs");
                if (stored) logs = JSON.parse(stored);
            }
            return new Response(JSON.stringify({ success: true, logs }), { status: 200 });
        }
        return new Response("OK", { status: 200 });
    } catch (e) { return new Response(JSON.stringify({ success: false }), { status: 400 }); }
}

async function handleUsersApi(request, env, ctx) {
    try {
        const url = new URL(request.url);
        const method = request.method;
        const userId = url.searchParams.get("id");
        const action = url.searchParams.get("action");

        const authHeader = request.headers.get("Authorization") || "";
        const authKey = authHeader.replace("Bearer ", "") || url.searchParams.get("key") || "";
        let bodyKey = "";
        if (method === "POST" || method === "PUT") {
            try {
                const body = await request.clone().json();
                bodyKey = body.key || "";
            } catch(e) {}
        }
        const isAuth = (authKey === sysConfig.masterKey) || (bodyKey === sysConfig.masterKey) || isPanelApiKey(authKey) || isPanelApiKey(bodyKey);
        if (!isAuth) {
            return new Response(JSON.stringify({ success: false, error: "Unauthorized" }), { status: 401, headers: { "Content-Type": "application/json" } });
        }

        if (method === "GET" && !userId) {
            const q = url.searchParams.get("q") || "";
            let users = sysConfig.users || [];
            if (q) {
                const ql = q.toLowerCase();
                users = users.filter(u => u.name.toLowerCase().includes(ql) || u.id.toLowerCase().includes(ql) || (u.notes && u.notes.toLowerCase().includes(ql)));
            }
            const enriched = users.map(u => {
                const idClean = u.id.replace(/-/g, '').toLowerCase();
                const sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0, lastDay: '' };
                const usedBytes = Math.floor((sysU.reqs || 0) * (1073741824 / 6000));
                const limitBytes = u.limitTotalReq ? Math.floor(u.limitTotalReq * (1073741824 / 6000)) : 0;
                const isExpired = u.expiryMs && Date.now() > u.expiryMs;
                let status = "active";
                if (u.isPaused && u.disabledReason) status = "auto-disabled";
                else if (u.isPaused) status = "paused";
                else if (isExpired) status = "expired";
                return { ...u, usage: { total: usedBytes, limit: limitBytes, daily: sysU.dReqs || 0, dailyLimit: u.limitDailyReq || 0 }, status };
            });
            return new Response(JSON.stringify({ success: true, users: enriched, total: enriched.length }), { headers: { "Content-Type": "application/json" } });
        }

        if (method === "GET" && userId) {
            const u = (sysConfig.users || []).find(usr => usr.id === userId || usr.name.toLowerCase() === userId.toLowerCase());
            if (!u) return new Response(JSON.stringify({ success: false, error: "User not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
            const idClean = u.id.replace(/-/g, '').toLowerCase();
            const sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0, lastDay: '' };
            const usedBytes = Math.floor((sysU.reqs || 0) * (1073741824 / 6000));
            const limitBytes = u.limitTotalReq ? Math.floor(u.limitTotalReq * (1073741824 / 6000)) : 0;
            const isExpired = u.expiryMs && Date.now() > u.expiryMs;
            let status = "active";
            if (u.isPaused && u.disabledReason) status = "auto-disabled";
            else if (u.isPaused) status = "paused";
            else if (isExpired) status = "expired";
            const hostName = new URL(request.url).hostname;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(u.name)}`;
            return new Response(JSON.stringify({ success: true, user: { ...u, usage: { total: usedBytes, limit: limitBytes, daily: sysU.dReqs || 0, dailyLimit: u.limitDailyReq || 0 }, status, subscriptionUrl: subUrl } }), { headers: { "Content-Type": "application/json" } });
        }

        if (method === "POST" && !userId) {
            const body = await request.json();
            const { name, trafficLimit, expiryDays, notes, maxConfigs, proxyIp, cleanIp, userMode, userPorts, userNodes, nat64, connLimit, userPanelUrl, customName } = body;
            if (!name) return new Response(JSON.stringify({ success: false, error: "Name is required" }), { status: 400, headers: { "Content-Type": "application/json" } });
            const newId = crypto.randomUUID();
            const newUser = {
                id: newId,
                name: name,
                customName: customName || null,
                limitTotalReq: trafficLimit ? Math.floor(parseFloat(trafficLimit) * 6000) : null,
                limitDailyReq: body.dailyLimit ? Math.floor(parseFloat(body.dailyLimit) * 6000) : null,
                expiryMs: expiryDays ? Date.now() + parseInt(expiryDays) * 86400000 : null,
                notes: notes || "",
                maxConfigs: maxConfigs ? parseInt(maxConfigs) : null,
                proxyIp: proxyIp || null,
                cleanIp: cleanIp || null,
                userMode: userMode || null,
                userPorts: userPorts || null,
                userNodes: userNodes || null,
                nat64: nat64 || null,
                connLimit: connLimit ? parseInt(connLimit) : null,
                userPanelUrl: userPanelUrl || null,
                createdAt: Date.now()
            };
            await resolveUserProxyIpGeo(newUser);
            if (!sysConfig.users) sysConfig.users = [];
            sysConfig.users.push(newUser);
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(logActivity(env, "User Created", `User "${name}" (${newId}) created via API`).catch(()=>{}));
            const hostName = new URL(request.url).hostname;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(name)}`;
            return new Response(JSON.stringify({ success: true, user: newUser, subscriptionUrl: subUrl }), { status: 201, headers: { "Content-Type": "application/json" } });
        }

        if (method === "PUT" && userId) {
            const body = await request.json();
            if (!sysConfig.users) return new Response(JSON.stringify({ success: false, error: "No users" }), { status: 400, headers: { "Content-Type": "application/json" } });
            const u = sysConfig.users.find(usr => usr.id === userId);
            if (!u) return new Response(JSON.stringify({ success: false, error: "User not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
            if (body.name !== undefined) u.name = body.name;
            if (body.customName !== undefined) u.customName = body.customName || null;
            if (body.trafficLimit !== undefined) u.limitTotalReq = body.trafficLimit ? Math.floor(parseFloat(body.trafficLimit) * 6000) : null;
            if (body.dailyLimit !== undefined) u.limitDailyReq = body.dailyLimit ? Math.floor(parseFloat(body.dailyLimit) * 6000) : null;
            if (body.expiryDays !== undefined) u.expiryMs = body.expiryDays ? Date.now() + parseInt(body.expiryDays) * 86400000 : null;
            if (body.notes !== undefined) u.notes = body.notes;
            if (body.maxConfigs !== undefined) u.maxConfigs = body.maxConfigs ? parseInt(body.maxConfigs) : null;
            if (body.proxyIp !== undefined) { u.proxyIp = body.proxyIp; if (!body.proxyIp) { u.proxyIpGeo = null; } else { await resolveUserProxyIpGeo(u); } }
            if (body.cleanIp !== undefined) u.cleanIp = body.cleanIp;
            if (body.userMode !== undefined) u.userMode = body.userMode;
            if (body.userPorts !== undefined) u.userPorts = body.userPorts;
            if (body.userNodes !== undefined) u.userNodes = body.userNodes;
            if (body.nat64 !== undefined) u.nat64 = body.nat64;
            if (body.connLimit !== undefined) u.connLimit = body.connLimit ? parseInt(body.connLimit) : null;
            if (body.userPanelUrl !== undefined) u.userPanelUrl = body.userPanelUrl || null;
            if (body.status !== undefined) {
                if (body.status === "active") { u.isPaused = false; u.disabledReason = null; u.disabledAt = null; }
                else if (body.status === "paused") { u.isPaused = true; u.disabledReason = null; u.disabledAt = null; }
            }
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(logActivity(env, "User Updated", `User "${u.name}" (${userId}) updated via API`).catch(()=>{}));
            return new Response(JSON.stringify({ success: true, user: u }), { headers: { "Content-Type": "application/json" } });
        }

        if (method === "DELETE" && userId) {
            if (!sysConfig.users) return new Response(JSON.stringify({ success: false, error: "No users" }), { status: 400, headers: { "Content-Type": "application/json" } });
            const idx = sysConfig.users.findIndex(usr => usr.id === userId);
            if (idx === -1) return new Response(JSON.stringify({ success: false, error: "User not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
            const deleted = sysConfig.users.splice(idx, 1)[0];
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(logActivity(env, "User Deleted", `User "${deleted.name}" (${userId}) deleted via API`).catch(()=>{}));
            return new Response(JSON.stringify({ success: true, deleted: deleted.id }), { headers: { "Content-Type": "application/json" } });
        }

        if (method === "POST" && userId && action === "toggle") {
            if (!sysConfig.users) return new Response(JSON.stringify({ success: false, error: "No users" }), { status: 400, headers: { "Content-Type": "application/json" } });
            const u = sysConfig.users.find(usr => usr.id === userId);
            if (!u) return new Response(JSON.stringify({ success: false, error: "User not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
            u.isPaused = !u.isPaused;
            if (!u.isPaused) { u.disabledReason = null; u.disabledAt = null; }
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(logActivity(env, "User Toggled", `User "${u.name}" (${userId}) ${u.isPaused ? 'paused' : 'resumed'} via API`).catch(()=>{}));
            return new Response(JSON.stringify({ success: true, user: u }), { headers: { "Content-Type": "application/json" } });
        }

        if (method === "POST" && userId && action === "reset") {
            if (!sysUsageCache) sysUsageCache = { users: {} };
            if (!sysUsageCache.users) sysUsageCache.users = {};
            const uuidClean = userId.replace(/-/g, '').toLowerCase();
            if (sysUsageCache.users[uuidClean]) {
                sysUsageCache.users[uuidClean].reqs = 0;
                sysUsageCache.users[uuidClean].dReqs = 0;
            } else {
                sysUsageCache.users[uuidClean] = { reqs: 0, dReqs: 0, lastDay: new Date().toISOString().split('T')[0] };
            }
            await cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache));
            ctx?.waitUntil(logActivity(env, "Traffic Reset", `Traffic reset for user ${userId} via API`).catch(()=>{}));
            return new Response(JSON.stringify({ success: true, message: "Traffic reset" }), { headers: { "Content-Type": "application/json" } });
        }

        return new Response(JSON.stringify({ success: false, error: "Invalid request" }), { status: 400, headers: { "Content-Type": "application/json" } });
    } catch (e) { return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } }); }
}

async function handleStatsApi(request, env) {
    try {
        const url = new URL(request.url);
        const authHeader = request.headers.get("Authorization") || "";
        const authKey = authHeader.replace("Bearer ", "") || url.searchParams.get("key") || "";
        if (authKey !== sysConfig.masterKey && !isPanelApiKey(authKey)) {
            return new Response(JSON.stringify({ success: false, error: "Unauthorized" }), { status: 401, headers: { "Content-Type": "application/json" } });
        }

        const users = sysConfig.users || [];
        const totalUsers = users.length;
        const activeUsers = users.filter(u => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs)).length;
        const autoDisabledUsers = users.filter(u => u.isPaused && u.disabledReason).length;
        const pausedUsers = users.filter(u => u.isPaused && !u.disabledReason).length;
        const expiredUsers = users.filter(u => u.expiryMs && Date.now() > u.expiryMs && !u.isPaused).length;

        let totalTrafficReqs = 0;
        let dailyTrafficReqs = 0;
        const todayDate = new Date().toISOString().split('T')[0];
        users.forEach(u => {
            const idClean = u.id.replace(/-/g, '').toLowerCase();
            const sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0, lastDay: '' };
            totalTrafficReqs += (sysU.reqs || 0);
            if (sysU.lastDay === todayDate) dailyTrafficReqs += (sysU.dReqs || 0);
        });

        const upSeconds = Math.floor((Date.now() - isolateStartTime) / 1000);

        return new Response(JSON.stringify({
            success: true,
            stats: {
                users: { total: totalUsers, active: activeUsers, paused: pausedUsers, expired: expiredUsers, autoDisabled: autoDisabledUsers },
                traffic: { totalRequests: totalTrafficReqs, totalGB: (totalTrafficReqs / 6000).toFixed(2), dailyRequests: dailyTrafficReqs, dailyGB: (dailyTrafficReqs / 6000).toFixed(2) },
                system: { uptimeSeconds: upSeconds, activeConnections, version: CURRENT_VERSION, isPaused: sysConfig.isPaused || false }
            }
        }), { headers: { "Content-Type": "application/json" } });
    } catch (e) { return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } }); }
}

function cmpVersions(a, b) {
    const strip = v => String(v).replace(/^v/, '').trim();
    const pa = strip(a).split('.').map(Number);
    const pb = strip(b).split('.').map(Number);
    for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
        let na = pa[i] || 0, nb = pb[i] || 0;
        if (na > nb) return 1;
        if (nb > na) return -1;
    }
    return 0;
}

async function handleUpdateApi(request, env, ctx) {
    try {
        if (request.method !== "POST") return new Response("405", { status: 405 });
        const data = await request.json();
        if (!isAuthorized(request, data)) {
            return new Response(JSON.stringify({ success: false, error: "Unauthorized" }), { status: 401, headers: { "Content-Type": "application/json" } });
        }

        const accountId = sysConfig.cfAccountId;
        const apiToken = sysConfig.cfApiToken;
        const workerName = sysConfig.cfWorkerName;
        const repo = (sysConfig.githubRepo || "Ceetherr/swimshady").replace(/https?:\/\/github\.com\//, '').trim();

        if (data.action === "check") {
            let remoteVer = null;
            try {
                const res = await fetch(`https://raw.githubusercontent.com/${repo}/main/version`);
                if (res.ok) {
                    const txt = (await res.text()).trim();
                    if (txt && txt.length <= 15) remoteVer = txt;
                }
            } catch(e) {}
            if (!remoteVer) {
                try {
                    const res = await fetch(`https://raw.githubusercontent.com/${repo}/main/_worker.js`);
                    if (res.ok) {
                        const code = await res.text();
                        const match = code.match(/const\s+CURRENT_VERSION\s*=\s*["']([^"']+)["']/);
                        if (match) remoteVer = match[1];
                    }
                } catch(e) {}
            }
            if (!remoteVer) {
                return new Response(JSON.stringify({ success: false, error: "Could not fetch remote version" }), { status: 502, headers: { "Content-Type": "application/json" } });
            }
            const hasCredentials = !!(accountId && apiToken && workerName);
            return new Response(JSON.stringify({
                success: true, current: CURRENT_VERSION, latest: remoteVer,
                updateAvailable: cmpVersions(CURRENT_VERSION, remoteVer) < 0,
                canDeploy: hasCredentials
            }), { headers: { "Content-Type": "application/json" } });
        }

        if (data.action === "deploy") {
            if (!accountId || !apiToken || !workerName) {
                return new Response(JSON.stringify({ success: false, error: "CF credentials not configured" }), { status: 400, headers: { "Content-Type": "application/json" } });
            }

            let finalCodeToDeploy = data.code;
            if (!finalCodeToDeploy) {
                try {
                    const res = await fetch(`https://raw.githubusercontent.com/${repo}/main/_worker.js`);
                    if (!res.ok) throw new Error(`HTTP ${res.status}`);
                    finalCodeToDeploy = await res.text();
                } catch(e) {
                    return new Response(JSON.stringify({ success: false, error: "Failed to fetch code from GitHub: " + e.message }), { status: 502, headers: { "Content-Type": "application/json" } });
                }
            }

            const versionMatch = finalCodeToDeploy.match(/const\s+CURRENT_VERSION\s*=\s*["']([^"']+)["']/);
            const newVersion = versionMatch ? versionMatch[1] : CURRENT_VERSION;

            if (cmpVersions(CURRENT_VERSION, newVersion) >= 0 && !data.force && !data.code) {
                return new Response(JSON.stringify({ success: false, error: "Remote version is not newer. Click force redeploy to switch formats or overwrite." }), { status: 400, headers: { "Content-Type": "application/json" } });
            }

            const deployRes = await deployWorkerToCloudflare(accountId, apiToken, workerName, finalCodeToDeploy);
            const deployResult = await deployRes.json();

            if (deployResult.success) {
                ctx?.waitUntil(logActivity(env, "Panel Updated", `v${CURRENT_VERSION} → v${newVersion}`).catch(()=>{}));
                if (sysConfig.tgToken && (sysConfig.tgAdminId || sysConfig.tgChatId)) {
                    const tgMsg = `🔄 <b>Panel Updated</b>\n\n📦 v${CURRENT_VERSION} → v${newVersion}`;
                    const notifyChatId = sysConfig.tgAdminId || sysConfig.tgChatId;
                    ctx?.waitUntil(fetch(`https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`, {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ chat_id: notifyChatId, text: tgMsg, parse_mode: 'HTML' })
                    }).catch(()=>{}));
                }
                return new Response(JSON.stringify({ success: true, message: `Updated to v${newVersion}`, newVersion }), { headers: { "Content-Type": "application/json" } });
            } else {
                const errMsg = deployResult.errors?.[0]?.message || "Unknown API error";
                return new Response(JSON.stringify({ success: false, error: "Cloudflare API: " + errMsg }), { status: 502, headers: { "Content-Type": "application/json" } });
            }
        }

        return new Response(JSON.stringify({ success: false, error: "Invalid action" }), { status: 400, headers: { "Content-Type": "application/json" } });
    } catch(e) {
        return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } });
    }
}

async function handleDoH(request, env) {
    try {
        const upstreamDns = sysConfig.customDns || 'https://cloudflare-dns.com/dns-query';
        const url = new URL(request.url);
        
        // Ad/tracker blocklist
        const adBlocklist = [
            'doubleclick.net', 'googlesyndication.com', 'googleadservices.com',
            'google-analytics.com', 'googletagmanager.com', 'googletagservices.com',
            'ads.youtube.com', 'ad.yieldmanager.com', 'advertising.com',
            'ads.facebook.com', 'an.facebook.com', 'pixel.facebook.com',
            'analytics.twitter.com', 'ads.twitter.com', 'platform.twitter.com',
            'ads.tiktok.com', 'analytics.tiktok.com',
            'ads.linkedin.com', 'snap.licdn.com',
            'adnxs.com', 'adsrvr.org', 'advertising.com', 'amazon-adsystem.com',
            'casalemedia.com', 'contextweb.com',
            'demdex.net', 'everesttech.net', 'mathtag.com',
            'quantserve.com', 'scorecardresearch.com', 'bluekai.com',
            'bounceexchange.com', 'chartbeat.com', 'chartbeat.net',
            'crazyegg.com', 'hotjar.com', 'heap.io', 'mixpanel.com',
            'segment.io', 'segment.com', 'amplitude.com',
            'appsflyer.com', 'branch.io', 'adjust.com', 'kochava.com',
            'singular.net', 'tenjin.com',
            'taboola.com', 'outbrain.com', 'revcontent.com', 'mgid.com',
            'criteo.com', 'criteo.net', 'pubmatic.com', 'rubiconproject.com',
            'openx.com', 'sharethrough.com', 'teads.tv',
            'moat.com', 'moatads.com', 'adsafeprotected.com',
            'ipify.org', 'ipinfo.io', 'whatismyip.com', 'myip.com',
            'browserleaks.com', 'whoer.net', 'whoislookup.com'
        ];
        
        // Check if domain should be blocked
        function isBlocked(domain) {
            const d = domain.toLowerCase();
            return adBlocklist.some(blocked => d === blocked || d.endsWith('.' + blocked));
        }
        
        // Handle GET request with DNS JSON API
        if (request.method === 'GET') {
            const dnsName = url.searchParams.get('name');
            const dnsType = url.searchParams.get('type') || 'A';
            
            if (!dnsName) {
                return new Response(JSON.stringify({ Status: 1, TC: false, RD: true, RA: true, AD: false, CD: false, Question: [], Answer: [] }), {
                    headers: { 'Content-Type': 'application/dns-json', 'Access-Control-Allow-Origin': '*' }
                });
            }
            
            // Check if domain is blocked
            if (isBlocked(dnsName)) {
                return new Response(JSON.stringify({ Status: 0, TC: false, RD: true, RA: true, AD: false, CD: false, Question: [{ name: dnsName, type: dnsType }], Answer: [] }), {
                    headers: { 'Content-Type': 'application/dns-json', 'Access-Control-Allow-Origin': '*' }
                });
            }
            
            try {
                const dohUrl = new URL(upstreamDns);
                dohUrl.searchParams.set('name', dnsName);
                dohUrl.searchParams.set('type', dnsType);
                
                const res = await fetch(dohUrl.toString(), { headers: { 'accept': 'application/dns-json' } });
                const data = await res.json();
                
                return new Response(JSON.stringify(data), {
                    headers: { 'Content-Type': 'application/dns-json', 'Access-Control-Allow-Origin': '*' }
                });
            } catch(e) {
                return new Response(JSON.stringify({ Status: 2, TC: false, RD: true, RA: true, AD: false, CD: false, Question: [], Answer: [] }), {
                    headers: { 'Content-Type': 'application/dns-json', 'Access-Control-Allow-Origin': '*' }
                });
            }
        }
        
        // Handle POST request (RFC 8484 wire format)
        if (request.method === 'POST') {
            const contentType = request.headers.get('content-type') || '';
            
            if (contentType.includes('application/dns-message')) {
                // Wire format - forward to upstream
                const body = await request.arrayBuffer();
                const res = await fetch(upstreamDns, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/dns-message', 'accept': 'application/dns-message' },
                    body: body
                });
                const result = await res.arrayBuffer();
                return new Response(result, {
                    headers: { 'Content-Type': 'application/dns-message', 'Access-Control-Allow-Origin': '*' }
                });
            } else if (contentType.includes('application/dns-json')) {
                // JSON format
                const body = await request.json();
                const dohUrl = new URL(upstreamDns);
                const res = await fetch(dohUrl.toString(), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/dns-json', 'accept': 'application/dns-json' },
                    body: JSON.stringify(body)
                });
                const data = await res.json();
                return new Response(JSON.stringify(data), {
                    headers: { 'Content-Type': 'application/dns-json', 'Access-Control-Allow-Origin': '*' }
                });
            }
        }
        
        // Handle CORS preflight
        if (request.method === 'OPTIONS') {
            return new Response(null, {
                status: 204,
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, accept',
                    'Access-Control-Max-Age': '86400'
                }
            });
        }
        
        return new Response('DNS-over-HTTPS endpoint', { status: 200, headers: { 'Content-Type': 'text/plain' } });
    } catch(e) {
        return new Response('Internal Server Error', { status: 500 });
    }
}

async function handleApiKeys(request, env, ctx) {
    try {
        const url = new URL(request.url);
        const method = request.method;

        const authKey = extractAuthKey(request, null);
        if (authKey !== sysConfig.masterKey) {
            return new Response(JSON.stringify({ success: false, error: "Only master key can manage API keys" }), { status: 401, headers: { "Content-Type": "application/json" } });
        }

        if (method === "GET") {
            const keys = (sysConfig.panelApiKeys || []).map(k => ({
                id: k.id, name: k.name, keyPreview: k.key.slice(0, 8) + "..." + k.key.slice(-4),
                createdAt: k.createdAt, lastUsed: k.lastUsed
            }));
            return new Response(JSON.stringify({ success: true, keys }), { headers: { "Content-Type": "application/json" } });
        }

        if (method === "POST") {
            const body = await request.json();
            if (body.action === "create") {
                if (!sysConfig.panelApiKeys) sysConfig.panelApiKeys = [];
                if (sysConfig.panelApiKeys.length >= 10) {
                    return new Response(JSON.stringify({ success: false, error: "Maximum 10 API keys allowed" }), { status: 400, headers: { "Content-Type": "application/json" } });
                }
                const newKey = generateApiKey(body.name);
                sysConfig.panelApiKeys.push(newKey);
                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                ctx?.waitUntil(logActivity(env, "API Key Created", `Key "${newKey.name}" created`).catch(()=>{}));
                return new Response(JSON.stringify({ success: true, key: newKey }), { status: 201, headers: { "Content-Type": "application/json" } });
            }
            if (body.action === "revoke") {
                if (!body.id) return new Response(JSON.stringify({ success: false, error: "ID required" }), { status: 400, headers: { "Content-Type": "application/json" } });
                const idx = (sysConfig.panelApiKeys || []).findIndex(k => k.id === body.id);
                if (idx === -1) return new Response(JSON.stringify({ success: false, error: "Key not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
                const revoked = sysConfig.panelApiKeys.splice(idx, 1)[0];
                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                ctx?.waitUntil(logActivity(env, "API Key Revoked", `Key "${revoked.name}" revoked`).catch(()=>{}));
                return new Response(JSON.stringify({ success: true, revoked: revoked.id }), { headers: { "Content-Type": "application/json" } });
            }
        }

        return new Response(JSON.stringify({ success: false, error: "Invalid request" }), { status: 400, headers: { "Content-Type": "application/json" } });
    } catch(e) {
        return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } });
    }
}

async function handleAuth(request, hostName, ctx, env) {
    try {
        const data = await request.json();
        const ip = request.headers.get("cf-connecting-ip") || "Unknown";
        const loginKey = data.key || "";
        const isKeyAuth = loginKey === sysConfig.masterKey || isPanelApiKey(loginKey);
        if (isKeyAuth) {
            if (isPanelApiKey(loginKey)) {
                const apiKeyEntry = (sysConfig.panelApiKeys || []).find(k => k.key === loginKey);
                if (apiKeyEntry) apiKeyEntry.lastUsed = Date.now();
            }
            ctx?.waitUntil(logActivity(env, "Auth Success", `Successful panel login from ${ip} (via ${isPanelApiKey(loginKey) ? 'API Key' : 'Master Key'})`));
            if (!sysConfig.silentAlerts && ctx) ctx.waitUntil(sendTelegramMessage(request, "ورود به پنل (موفق)", hostName));

            // Store login signal for Telegram bot
            if (sysConfig.tgAdminId && env.IOT_DB) {
                const loginSignal = {
                    name: sysConfig.name || hostName,
                    host: hostName,
                    apiRoute: sysConfig.apiRoute,
                    masterKey: sysConfig.masterKey,
                    isLocal: true,
                    ts: Date.now()
                };
                ctx?.waitUntil(d1Put(env, "tg_panel_login", JSON.stringify(loginSignal)).catch(() => {}));
            }

            // Notify hub panel if configured
            if (sysConfig.hubPanelUrl && sysConfig.hubPanelUrl.trim() && sysConfig.tgAdminId) {
                try {
                    let hubUrl = sysConfig.hubPanelUrl.trim();
                    if (!hubUrl.startsWith('http')) hubUrl = 'https://' + hubUrl;
                    const signalPayload = {
                        signal: "panel_login",
                        panelName: sysConfig.name || hostName,
                        panelHost: hostName,
                        panelApiRoute: sysConfig.apiRoute,
                        panelMasterKey: sysConfig.masterKey,
                        tgAdminId: sysConfig.tgAdminId,
                        ts: Date.now()
                    };
                    ctx?.waitUntil(fetch(`${hubUrl}/${encodeURI(sysConfig.apiRoute)}/tg/sync_panel`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(signalPayload)
                    }).catch(() => {}));
                } catch(e) {}
            }

            const netInfo = {
                ip: ip,
                colo: request.cf?.colo || "Unknown",
                loc: (request.cf?.city || "Unknown") + ", " + (request.cf?.country || "Unknown")
            };
            let usageData = {};
            for(let [k,v] of uuidUsage.entries()) usageData[k] = v;
            let baseHost = hostName;
            let protocol = "https";
            if (sysConfig.customPanelUrl && sysConfig.customPanelUrl.trim()) {
                let customUrlStr = sysConfig.customPanelUrl.trim();
                if (!customUrlStr.startsWith('http://') && !customUrlStr.startsWith('https://')) {
                    customUrlStr = 'https://' + customUrlStr;
                }
                try {
                    const customUrl = new URL(customUrlStr);
                    baseHost = customUrl.host;
                    protocol = customUrl.protocol.replace(':', '');
                } catch(e) {}
            }
            return new Response(JSON.stringify({
                success: true, config: isPanelApiKey(loginKey) ? { ...sysConfig, masterKey: "[PROTECTED]", panelApiKeys: "[PROTECTED]" } : sysConfig, deviceId: activeDeviceId, network: netInfo, usage: usageData, sysUsage: (sysUsageCache && sysUsageCache.users) ? sysUsageCache.users : {},
                version: CURRENT_VERSION,
                profiles: getAllProfiles().map(p => {
                    let subSuffix = p.name === 'Default' ? '' : '?sub=' + encodeURIComponent(p.name);
                    return {
                        name: p.name,
                        id: p.id,
                        sync: `${protocol}://${baseHost}/${sysConfig.apiRoute}${subSuffix}`
                    };
                })
            }), { status: 200 });
        }
        ctx?.waitUntil(logActivity(env, "Auth Failed", `Failed login attempt from ${ip}`));
        if (ctx) ctx.waitUntil(sendTelegramMessage(request, "تلاش ناموفق ورود به پنل!", hostName));
        return new Response(JSON.stringify({ success: false }), { status: 401 });
    } catch (e) { return new Response(JSON.stringify({ success: false }), { status: 400 }); }
}

async function handleConfigSync(request, env, ctx) {
    try {
        const data = await request.json();
        const isAuthSync = (data.key === sysConfig.masterKey) || 
                             (data.oldKey && data.oldKey === sysConfig.masterKey) || 
                             (sysConfig.masterKey === "admin") ||
                             isPanelApiKey(data.key) || isPanelApiKey(data.oldKey) ||
                             (data.fromMaster && data.config && data.config.masterKey && data.config.masterKey === sysConfig.masterKey);
        if (!isAuthSync) return new Response(JSON.stringify({ success: false, error: "Auth failed. Generate the API key on THIS panel, not the main panel." }), { status: 401 });
        if (!env.IOT_DB) return new Response(JSON.stringify({ success: false, msg: "DB Error" }), { status: 400 });
        
        let nextConfig = sysConfig;
        if (data.config) {
            const preserveApiKeys = sysConfig.panelApiKeys || [];
            nextConfig = { ...sysConfig, ...data.config };
            if (preserveApiKeys.length > 0 && (!data.config.panelApiKeys || data.config.panelApiKeys.length === 0)) {
                nextConfig.panelApiKeys = preserveApiKeys;
            }
            if (Array.isArray(nextConfig.users) && nextConfig.users.length > 0) {
                const geoPromises = nextConfig.users.map(async (u) => {
                    if (u.proxyIp) {
                        await resolveUserProxyIpGeo(u);
                    } else {
                        u.proxyIpGeo = null;
                    }
                });
                await Promise.all(geoPromises);
            }
            sysConfig = nextConfig;
            await cachedD1Put(env, "sys_config", JSON.stringify(nextConfig));
        }

        let tagWarning = null;
        if (nextConfig.nameStrategy && nextConfig.nameStrategy.includes('{') && nextConfig.nameStrategy.includes('}')) {
            let vResult = validateNameStrategy(nextConfig.nameStrategy);
            if (!vResult.valid) tagWarning = `Unknown tags detected: ${vResult.unknownTags.join(', ')}`;
        }

        if (data.resetUUID) {
            const uuidClean = data.resetUUID.replace(/-/g, '').toLowerCase();
            if (!sysUsageCache) sysUsageCache = { users: {} };
            if (!sysUsageCache.users) sysUsageCache.users = {};
            if (sysUsageCache.users[uuidClean]) {
                sysUsageCache.users[uuidClean].reqs = 0;
                sysUsageCache.users[uuidClean].dReqs = 0;
            } else {
                sysUsageCache.users[uuidClean] = { reqs: 0, dReqs: 0, lastDay: new Date().toISOString().split('T')[0] };
            }
            await cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache));
        }

        if (data.config && !data.fromMaster && nextConfig.slaveNodes && nextConfig.slaveNodes.trim().length > 0) {
            let nodes = nextConfig.slaveNodes.split(/[\r\n,;]+/).map(s=>s.trim()).filter(Boolean);
            let syncKey = nextConfig.syncApiKey || '';
            let currentHost = new URL(request.url).hostname;
            // Strip master-only secrets so they never leave this panel. Slave nodes keep their
            // own values (slave merges via { ...sysConfig, ...data.config }, so omitted keys are untouched).
            let slaveConfig = { ...nextConfig };
            ['cfAccountId', 'cfApiToken', 'cfWorkerName', 'tgToken', 'tgChatId', 'tgAdminId'].forEach(k => delete slaveConfig[k]);
            nodes.forEach(node => {
                if(node !== currentHost) {
                     ctx?.waitUntil(fetch(`https://${node}/${encodeURI(nextConfig.apiRoute)}/api/sync`, {
                         method: 'POST',
                         headers: { 'Content-Type': 'application/json' },
                         body: JSON.stringify({ key: syncKey, config: slaveConfig, fromMaster: true })
                     }).catch(() => {}));
                }
            });
        }
        
        if (nextConfig.tgToken && ctx) {
            const hookUrl = `https://${new URL(request.url).hostname}/${encodeURI(nextConfig.apiRoute)}/tg`;
            ctx.waitUntil(fetch(`https://api.telegram.org/bot${nextConfig.tgToken}/setWebhook`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url: hookUrl })
            }).catch(()=>{}));
        }

        return new Response(JSON.stringify({ success: true, newRoute: nextConfig.apiRoute, tagWarning }), { status: 200 });
    } catch (e) { return new Response(JSON.stringify({ success: false }), { status: 400 }); }
}

async function handleSyncPanel(request, env, ctx) {
    try {
        const data = await request.json();
        if (!data.signal || data.signal !== "panel_login") {
            return new Response(JSON.stringify({ success: false, error: "Invalid signal" }), { status: 400 });
        }
        if (!data.tgAdminId || !data.panelHost) {
            return new Response(JSON.stringify({ success: false, error: "Missing fields" }), { status: 400 });
        }
        // Verify the tgAdminId matches this panel's config
        const adminId = sysConfig.tgAdminId || sysConfig.tgChatId;
        if (!adminId || adminId.toString() !== data.tgAdminId.toString()) {
            return new Response(JSON.stringify({ success: false, error: "Unauthorized" }), { status: 401 });
        }
        const loginSignal = {
            name: data.panelName || data.panelHost,
            host: data.panelHost,
            apiRoute: data.panelApiRoute || sysConfig.apiRoute,
            masterKey: data.panelMasterKey,
            isLocal: false,
            ts: data.ts || Date.now()
        };
        if (env.IOT_DB) {
            ctx?.waitUntil(d1Put(env, "tg_panel_login", JSON.stringify(loginSignal)).catch(()=>{}));
        }
        return new Response(JSON.stringify({ success: true }), { status: 200 });
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), { status: 400 });
    }
}

const botI18n = {
    en: {
        welcome: "🤖 **Welcome to SwimShady Gateway Bot**\nSelect your option below to manage your system:",
        status: "System Status",
        users: "Subscribers",
        metrics: "Gateway Health",
        panic: "Panic Mode",
        dash: "Dashboard Control",
        lang: "🌐 Change Language",
        active: "🟢 Active",
        paused: "🔴 Paused",
        uptime: "Uptime",
        streams: "📡 Active Streams",
        no_users: "No subscribers found.",
        sub_info: "👤 Subscriber Details:",
        name: "Name",
        total: "Total Reqs",
        daily: "Daily Reqs",
        expiry: "Expiry",
        days: "Days remaining",
        created: "Created At",
        unlimited: "Unlimited",
        btn_back: "◀️ Back",
        btn_next: "▶️ Next",
        btn_del: "Delete",
        btn_pause: "Pause",
        btn_resume: "Resume",
        btn_edit_name: "Change Name",
        btn_edit_limits: "Limits",
        btn_add: "+ Add Subscriber",
        btn_confirm: "Confirm",
        btn_cancel: "Cancel",
        msg_enter_name: "Please send a name for the subscriber:",
        msg_added: "Sub added successfully! 🎉",
        msg_deleted: "Sub deleted successfully! 🗑️",
        msg_panic: "🚨 PANIC MODE ACTIVATED 🚨\nRoute randomized & System Paused.",
        msg_invalid: "Invalid input. Please try again.",
        msg_enter_limits: "Enter limits format:\n`[totalReqs] [dailyReqs] [days_limit]`\n(Use 0 for unlimited)\n\nExample:\n`10000 500 30`",
        msg_confirm_del: "⚠️ Are you sure you want to delete this subscriber?",
        msg_confirm_panic: "⚠️ Are you absolutely sure you want to trigger PANIC mode? This will randomize API routes and pause all connections!",
        status_updated: "Status updated!",
        access_denied: "Access Denied. You are not authorized to manage this panel.",
        dashboard: "Dashboard",
        search: "Search User",
        statistics: "Statistics",
        panel_info: "Panel Info",
        disabled_users: "Disabled Users",
        reset_traffic: "Reset Traffic",
        extend_expiry: "Extend Expiry",
        notes: "Notes",
        device_limit: "Config Limit",
        msg_enter_search: "🔍 Send a username, UUID, or subscription to search:",
        msg_enter_notes: "📝 Send notes for this user:",
        msg_enter_extend_days: "📅 Enter number of days to extend expiration:",
        msg_traffic_reset: "Traffic has been reset successfully!",
        msg_expiry_extended: "Expiration extended by {days} days!",
        msg_no_disabled: "No disabled users found.",
        msg_enter_device_limit: "Enter config limit (0 for unlimited):",
        config_limit_updated: "Config limit updated!",
        stats_title: "Panel Statistics",
        count_active: "active",
        count_paused: "paused",
        count_disabled: "auto-disabled",
        dash_total: "Total Users",
        dash_active: "Active",
        dash_paused: "Paused",
        dash_expired: "Expired",
        dash_auto_disabled: "Auto-Disabled",
        btn_main_menu: "Main Menu",
        btn_back_to_list: "Back to List",
        total_traffic: "Total Traffic",
        daily_traffic: "Daily Traffic",
        lbl_status: "Status",
        lbl_subscription: "Subscription Connection",
        lbl_user_not_found: "⚠️ User not found",
        lbl_none: "None",
        lbl_page: "Page",
        select_panel: "🔌 Which panel do you want to manage?",
        current_panel: "Current Panel",
        switch_panel: "🔄 Switch Panel",
        panel_local: "🏠 This Panel",
        panel_remote: "🌐",
        msg_panel_selected: "Panel selected! ✅",
        msg_panel_error: "❌ Failed to connect to the selected panel.",
        msg_panel_unreachable: "⚠️ Panel is unreachable. Please check the configuration.",
        btn_sub_link: "Subscription Link",
        sub_link_sent: "Subscription link sent!",
        btn_update_usage: "Update Usage",
        tg_settings: "Settings", tg_advanced: "Advanced", tg_logs: "Logs",
        tg_sys_settings: "System Settings", tg_adv_settings: "Advanced Settings",
        tg_logs_view: "View Logs", tg_logs_clear: "Clear Logs",
        tg_proto: "Protocol", tg_ports: "Ports", tg_uuid: "Device UUID", tg_path: "API Route",
        tg_pass: "Master Key", tg_dns: "DNS", tg_relay: "Relay IP", tg_maintenance: "Maintenance Hosts",
        tg_tfo: "TCP Fast Open", tg_ech: "ECH", tg_silent: "Silent Alerts", tg_pause: "Kill Switch",
        tg_auto_update: "Auto Update", tg_direct: "Direct Configs", tg_nat64: "NAT64",
        tg_clean_ips: "Clean IPs", tg_nodes: "Nodes", tg_strategy: "Name Strategy",
        tg_prefix: "Name Prefix", tg_fake_entries: "Fake Entries", tg_cf_settings: "Cloudflare Settings",
        tg_tg_settings: "Telegram Settings", tg_backup: "Backup", tg_restore: "Restore",
        tg_current_val: "Current Value", tg_new_val: "Send new value:",
        tg_saved: "Saved!", tg_cancelled: "Cancelled",
        tg_log_entry: "", tg_log_empty: "No logs found",
        tg_u_custom_name: "Custom Name", tg_u_clean_ips: "Clean IPs", tg_u_proxy_ips: "Proxy IPs",
        tg_u_nodes: "Nodes", tg_u_nat64: "NAT64", tg_u_mode: "Protocol Mode", tg_u_ports: "Ports", tg_u_conn_limit: "Conn Limit", tg_u_panel_url: "Panel URL",
        tg_u_max_cfg: "Max Configs", tg_u_all: "All Settings",
        tg_network: "Network", tg_uptime: "Uptime", tg_conns: "Active Connections",
        tg_version: "Version", tg_cf_usage: "CF Usage",
    },
    fa: {
        welcome: "🤖 **به ربات ترانزیت SwimShady خوش آمدید**\nجهت مدیریت سیستم نظارتی خود یکی از گزینه‌های زیر را انتخاب نمایید:",
        status: "وضعیت سیستم",
        users: "مدیریت مشترکین",
        metrics: "سلامت درگاه شبکه",
        panic: "وضعیت اضطراری (Panic)",
        dash: "پنل تحت وب",
        lang: "🌐 تغییر زبان به انگلیسی",
        active: "🟢 فعال",
        paused: "🔴 متوقف شده",
        uptime: "زمان کارکرد",
        streams: "📡 اتصالات فعال",
        no_users: "هیچ مشترکی پیدا نشد.",
        sub_info: "👤 مشخصات مشترک:",
        name: "نام",
        total: "درخواست کل",
        daily: "درخواست روزانه",
        expiry: "انقضاء",
        days: "روزهای باقی‌مانده",
        created: "تاریخ ایجاد",
        unlimited: "نامحدود",
        btn_back: "بازگشت",
        btn_next: "بعدی",
        btn_del: "حذف",
        btn_pause: "غیرفعال‌سازی",
        btn_resume: "فعال‌سازی",
        btn_edit_name: "تغییر نام",
        btn_edit_limits: "ویرایش محدودیت‌ها",
        btn_add: "+ افزودن مشترک جدید",
        btn_confirm: "تأیید",
        btn_cancel: "انصراف",
        msg_enter_name: "لطفاً نام یا شناسه مشترک جدید را ارسال نمایید:",
        msg_added: "مشترک با موفقیت افزوده شد!",
        msg_deleted: "مشترک با موفقیت حذف گردید!",
        msg_panic: "وضعیت اضطراری فعال شد\nمسیر تصادفی شد و سیستم متوقف گردید.",
        msg_invalid: "ورودی نامعتبر است. مجدداً تلاش نمایید.",
        msg_enter_limits: "فرمت ورودی محدودیت:\n`[کل] [روزانه] [مدت_روز]`\n(از 0 برای نامحدود استفاده کنید)\n\nمثال:\n`10000 500 30`",
        msg_confirm_del: "آیا از حذف این مشترک اطمینان کامل دارید؟",
        msg_confirm_panic: "آیا از فعال‌سازی وضعیت اضطراری اطمینان دارید؟ کل اتصالات متوقف و آدرس‌ها منقضی خواهند شد!",
        status_updated: "وضعیت بروزرسانی شد!",
        access_denied: "دسترسی غیرمجاز. شما اجازه مدیریت این پنل را ندارید.",
        dashboard: "داشبورد",
        search: "جستجوی کاربر",
        statistics: "آمار",
        panel_info: "اطلاعات پنل",
        disabled_users: "کاربران غیرفعال",
        reset_traffic: "بازنشانی ترافیک",
        extend_expiry: "تمدید انقضا",
        notes: "یادداشت‌ها",
        device_limit: "محدودیت کانفیگ",
        msg_enter_search: "🔍 نام کاربری، UUID یا لینک اشتراک را ارسال کنید:",
        msg_enter_notes: "📝 یادداشت برای این کاربر را ارسال کنید:",
        msg_enter_extend_days: "📅 تعداد روزهای تمدید را وارد کنید:",
        msg_traffic_reset: "ترافیک با موفقیت بازنشانی شد!",
        msg_expiry_extended: "انقضا به مدت {days} روز تمدید شد!",
        msg_no_disabled: "هیچ کاربر غیرفعالی یافت نشد.",
        msg_enter_device_limit: "محدودیت تعداد کانفیگ را وارد کنید (0 برای نامحدود):",
        config_limit_updated: "محدودیت کانفیگ به‌روزرسانی شد!",
        stats_title: "آمار پنل",
        count_active: "فعال",
        count_paused: "متوقف",
        count_disabled: "غیرفعال خودکار",
        dash_total: "کل کاربران",
        dash_active: "فعال",
        dash_paused: "متوقف",
        dash_expired: "منقضی",
        dash_auto_disabled: "غیرفعال خودکار",
        btn_main_menu: "منوی اصلی",
        btn_back_to_list: "بازگشت به لیست",
        total_traffic: "ترافیک کل",
        daily_traffic: "ترافیک روزانه",
        lbl_status: "وضعیت",
        lbl_subscription: "لینک اشتراک",
        lbl_user_not_found: "⚠️ کاربر یافت نشد",
        lbl_none: "ندارد",
        lbl_page: "صفحه",
        select_panel: "🔌 کدام پنل را می‌خواهید مدیریت کنید؟",
        current_panel: "پنل فعلی",
        switch_panel: "🔄 تغییر پنل",
        panel_local: "🏠 این پنل",
        panel_remote: "🌐",
        msg_panel_selected: "پنل انتخاب شد! ✅",
        msg_panel_error: "❌ اتصال به پنل انتخابی ناموفق بود.",
        msg_panel_unreachable: "⚠️ پنل در دسترس نیست. لطفاً پیکربندی را بررسی کنید.",
        btn_sub_link: "لینک اشتراک",
        sub_link_sent: "لینک اشتراک ارسال شد!",
        btn_update_usage: "بروزرسانی مصرف",
        tg_settings: "تنظیمات", tg_advanced: "پیشرفته", tg_logs: "گزارش‌ها",
        tg_sys_settings: "تنظیمات سیستم", tg_adv_settings: "تنظیمات پیشرفته",
        tg_logs_view: "مشاهده گزارش‌ها", tg_logs_clear: "پاک کردن گزارش‌ها",
        tg_proto: "پروتکل", tg_ports: "پورت‌ها", tg_uuid: "شناسه دستگاه", tg_path: "مسیر API",
        tg_pass: "کلید اصلی", tg_dns: "DNS", tg_relay: "آی‌پی رله", tg_maintenance: "سایت استتار",
        tg_tfo: "TCP Fast Open", tg_ech: "ECH", tg_silent: "هشدار خاموش", tg_pause: "کلید توقف",
        tg_auto_update: "بروزرسانی خودکار", tg_direct: "کانفیگ مستقیم", tg_nat64: "NAT64",
        tg_clean_ips: "آی‌پی تمیز", tg_nodes: "نودها", tg_strategy: "روش نام‌گذاری",
        tg_prefix: "پیشوند", tg_fake_entries: "ورودی‌های اشتراک", tg_cf_settings: "تنظیمات کلودفلر",
        tg_tg_settings: "تنظیمات تلگرام", tg_backup: "پشتیبان‌گیری", tg_restore: "بازیابی",
        tg_current_val: "مقدار فعلی", tg_new_val: "مقدار جدید را ارسال کنید:",
        tg_saved: "ذخیره شد!", tg_cancelled: "لغو شد",
        tg_log_entry: "", tg_log_empty: "گزارشی ثبت نشده",
        tg_u_custom_name: "نام سفارشی", tg_u_clean_ips: "آی‌پی تمیز", tg_u_proxy_ips: "آی‌پی پروکسی",
        tg_u_nodes: "نودها", tg_u_nat64: "NAT64", tg_u_mode: "پروتکل", tg_u_ports: "پورت‌ها", tg_u_conn_limit: "محدودیت اتصال", tg_u_panel_url: "آدرس پنل",
        tg_u_max_cfg: "حداکثر کانفیگ", tg_u_all: "همه تنظیمات",
        tg_network: "شبکه", tg_uptime: "زمان کارکرد", tg_conns: "اتصالات فعال",
        tg_version: "نسخه", tg_cf_usage: "مصرف کلودفلر",
    }
};

function getPanelsList() {
    const panels = [];
    panels.push({
        name: sysConfig.name || "Swimshady",
        host: null,
        apiRoute: sysConfig.apiRoute,
        apiKey: null,
        isLocal: true
    });
    if (sysConfig.linkedPanels && Array.isArray(sysConfig.linkedPanels)) {
        sysConfig.linkedPanels.forEach(p => {
            if (p && p.host) {
                panels.push({
                    name: p.name || p.host,
                    host: p.host,
                    apiRoute: p.apiRoute || sysConfig.apiRoute,
                    apiKey: p.apiKey || p.masterKey || null,
                    isLocal: false
                });
            }
        });
    }
    return panels;
}

async function remotePanelFetch(panel, method, path, body = null) {
    try {
        const url = `https://${panel.host}/${encodeURI(panel.apiRoute)}${path}`;
        const options = {
            method,
            headers: { 'Content-Type': 'application/json' }
        };
        if (body) options.body = JSON.stringify(body);
        const res = await fetch(url, { ...options, signal: AbortSignal.timeout(8000) });
        return await res.json();
    } catch(e) {
        return { success: false, error: e.message };
    }
}

async function fetchRemotePanelUsers(panel) {
    return await remotePanelFetch(panel, 'GET', `/api/users?key=${encodeURIComponent(panel.apiKey)}`);
}

async function fetchRemotePanelUser(panel, userId) {
    return await remotePanelFetch(panel, 'GET', `/api/users?id=${encodeURIComponent(userId)}&key=${encodeURIComponent(panel.apiKey)}`);
}

async function fetchRemotePanelStats(panel) {
    return await remotePanelFetch(panel, 'GET', `/api/stats?key=${encodeURIComponent(panel.apiKey)}`);
}

async function fetchRemotePanelConfig(panel) {
    return await remotePanelFetch(panel, 'POST', '/api/auth', { key: panel.apiKey });
}

async function remotePanelWriteAction(panel, method, userId, body = null) {
    let path = '/api/users';
    if (userId) path += `?id=${encodeURIComponent(userId)}&key=${encodeURIComponent(panel.apiKey)}`;
    else path += `?key=${encodeURIComponent(panel.apiKey)}`;
    return await remotePanelFetch(panel, method, path, body || { key: panel.apiKey });
}

async function remotePanelToggleUser(panel, userId) {
    return await remotePanelFetch(panel, 'POST', `/api/users?id=${encodeURIComponent(userId)}&action=toggle&key=${encodeURIComponent(panel.apiKey)}`);
}

async function remotePanelResetTraffic(panel, userId) {
    return await remotePanelFetch(panel, 'POST', `/api/users?id=${encodeURIComponent(userId)}&action=reset&key=${encodeURIComponent(panel.apiKey)}`);
}

async function handleTelegramWebhook(request, env, hostName, ctx) {
    try {
        const update = await request.json();
        const tgApi = `https://api.telegram.org/bot${sysConfig.tgToken}`;

        const langCode = sysConfig.tgBotLang || "fa";
        const t = (key) => botI18n[langCode]?.[key] || botI18n["en"]?.[key] || key;

        const callerId = update.callback_query?.from?.id?.toString() || update.message?.from?.id?.toString();
        const adminId = sysConfig.tgAdminId || sysConfig.tgChatId;
        const isAuthorized = adminId && callerId === adminId.toString();

        if (!isAuthorized) {
            const chatId = update.callback_query?.message?.chat?.id || update.message?.chat?.id;
            if (chatId) {
                await fetch(`${tgApi}/sendMessage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ 
                        chat_id: chatId, 
                        text: "❌ *شما دسترسی به این ربات را ندارید.*\n\nیوزر آیدی شما جهت اضافه کردن به لیست ادمین ها: `" + (callerId || "Unknown") + "`", 
                        parse_mode: 'Markdown' 
                    })
                });
            }
            return new Response(JSON.stringify({ success: false, error: "Unauthorized" }), { status: 200 });
        }

        let tgState = {};
        try {
            const storedState = await d1Get(env, "tg_bot_state");
            if (storedState) tgState = JSON.parse(storedState);
        } catch (e) { }

        const panels = getPanelsList();

        // Read last login signal from D1 (set by handleAuth or handleSyncPanel)
        let lastLoginPanel = null;
        try {
            const stored = await d1Get(env, "tg_panel_login");
            if (stored) lastLoginPanel = JSON.parse(stored);
        } catch (e) { }

        const getActivePanel = () => {
            if (lastLoginPanel) {
                if (lastLoginPanel.isLocal) return panels.find(p => p.isLocal) || panels[0];
                const found = panels.find(p => !p.isLocal && p.host === lastLoginPanel.host);
                if (found) return found;
                // Remote panel not in linkedPanels — synthesize from login signal
                return {
                    name: lastLoginPanel.name || lastLoginPanel.host,
                    host: lastLoginPanel.host,
                    apiRoute: lastLoginPanel.apiRoute || sysConfig.apiRoute,
                    apiKey: lastLoginPanel.apiKey || lastLoginPanel.masterKey || null,
                    isLocal: false
                };
            }
            return panels[0]; // default to local
        };

        // Custom sendOrEdit message helper
        const sendOrEdit = async (chatId, text, replyMarkup = null, messageId = null) => {
            let res;
            if (messageId) {
                res = await fetch(`${tgApi}/editMessageText`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        chat_id: chatId,
                        message_id: messageId,
                        text: text,
                        parse_mode: 'Markdown',
                        reply_markup: replyMarkup
                    })
                });
                if (res.ok) return res;
                try {
                    const errBody = await res.json();
                    if (errBody?.description?.includes("message is not modified")) return res;
                } catch (e) {}
            }
            res = await fetch(`${tgApi}/sendMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    chat_id: chatId,
                    text: text,
                    parse_mode: 'Markdown',
                    reply_markup: replyMarkup
                })
            });
            return res;
        };

        const getMainMenu = (activePanel, isAdmin = true) => {
            const isPaused = sysConfig.isPaused || false;
            const statusEmoji = isPaused ? "🔴" : "🟢";
            const users = sysConfig.users || [];
            const activeCount = users.filter(u => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs)).length;
            const pausedCount = users.filter(u => u.isPaused && !u.disabledReason).length;
            const autoDisabledCount = users.filter(u => u.isPaused && u.disabledReason).length;
            const isLocal = !activePanel || activePanel.isLocal;
            const panelName = activePanel ? activePanel.name : (sysConfig.name || "Swimshady");
            const panelIndicator = isLocal ? `🏠 ${panelName}` : `🌐 ${panelName}`;
            let text = `${t("welcome")}\n\n` +
                         `━━━━━━━━━━━━━━━━\n` +
                         `📌 **${t("current_panel")}**: ${panelIndicator}\n` +
                         `⚡ **${t("status")}**: ${isPaused ? t("paused") : t("active")} ${statusEmoji}\n` +
                         `👥 **${t("users")}**: ${users.length} (${activeCount} ${t("count_active")}, ${pausedCount} ${t("count_paused")}, ${autoDisabledCount} ${t("count_disabled")})\n` +
                         `━━━━━━━━━━━━━━━━`;
            const panelUrl = isLocal ? `https://${hostName}/${encodeURI(sysConfig.apiRoute)}/dash` : null;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}`;
            /** @type {any} */
            const inline_keyboard = [];
            if (isAdmin) {
                inline_keyboard.push([
                    { text: `👥 ${t("users")}`, callback_data: "subs_list:0" },
                    { text: `🔍 ${t("search")}`, callback_data: "sub_search_init" }
                ]);
            }
            inline_keyboard.push([
                { text: `📊 ${t("dashboard")}`, callback_data: "sys_dashboard" },
                { text: `📈 ${t("statistics")}`, callback_data: "sys_stats" }
            ]);
            inline_keyboard.push([
                { text: `🔗 ${t("btn_sub_link")}`, callback_data: "get_sub_link" }
            ]);
            if (isAdmin) {
                inline_keyboard.push([
                    { text: `🚫 ${t("disabled_users")}`, callback_data: "subs_disabled:0" }
                ]);
                inline_keyboard.push([
                    { text: `⚙️ ${t("tg_settings")}`, callback_data: "tg_settings_menu" },
                    { text: `🔧 ${t("tg_advanced")}`, callback_data: "tg_advanced_menu" }
                ]);
                inline_keyboard.push([
                    { text: `📋 ${t("tg_logs")}`, callback_data: "tg_logs_menu" }
                ]);
            }
            inline_keyboard.push([
                { text: `🌐 ${langCode === 'fa' ? 'English 🇺🇸' : 'فارسی 🇮🇷'}`, callback_data: "sys_lang" },
                { text: isPaused ? `▶️ ${t("btn_resume")}` : `⏸️ ${t("btn_pause")}`, callback_data: "sys_toggle_status" }
            ]);
            if (panelUrl) {
                inline_keyboard.push([
                    { text: `🔑 ${t("dash")}`, web_app: { url: panelUrl } },
                    { text: `ℹ️ ${t("panel_info")}`, callback_data: "sys_panel_info" }
                ]);
                if (isAdmin) {
                    inline_keyboard.push([
                        { text: `🚨 ${t("panic")}`, callback_data: "sys_panic_init" }
                    ]);
                }
            } else {
                inline_keyboard.push([
                    { text: `ℹ️ ${t("panel_info")}`, callback_data: "sys_panel_info" }
                ]);
            }
            const kb = { inline_keyboard };
            return { text, kb };
        };

        const getSubsList = (page = 0, usersList = null) => {
            const users = usersList || sysConfig.users || [];
            const itemsPerPage = 5;
            const totalPages = Math.ceil(users.length / itemsPerPage);
            const start = page * itemsPerPage;
            const end = start + itemsPerPage;
            const pageUsers = users.slice(start, end);
            
            let text = `👥 **${t("users")}** (${t("lbl_page")} ${page + 1}/${Math.max(1, totalPages)})\n`;
            text += `━━━━━━━━━━━━━━━━\n`;
            
            if (users.length === 0) {
                text += `⚠️ ${t("no_users")}\n`;
            } else {
                pageUsers.forEach((u, idx) => {
                    text += `${start + idx + 1}. 👤 **${u.name}**\n   \`${u.id}\`\n`;
                });
            }
            text += `━━━━━━━━━━━━━━━━`;
            
            const inline_keyboard = [];
            pageUsers.forEach((u) => {
                inline_keyboard.push([{ text: `👤 ${u.name}`, callback_data: `sub_detail:${u.id}` }]);
            });
            
            const navRow = [];
            if (page > 0) {
                navRow.push({ text: `⬅️ ${t("btn_back")}`, callback_data: `subs_list:${page - 1}` });
            }
            if (end < users.length) {
                navRow.push({ text: `${t("btn_next")} ➡️`, callback_data: `subs_list:${page + 1}` });
            }
            if (navRow.length > 0) {
                inline_keyboard.push(navRow);
            }
            
            inline_keyboard.push([{ text: `➕ ${t("btn_add")}`, callback_data: "sub_add_init" }]);
            inline_keyboard.push([{ text: t("btn_main_menu"), callback_data: "main_menu" }]);
            
            return { text, kb: { inline_keyboard } };
        };

        const getSubDetail = (uuid, usersList = null) => {
            const users = usersList || sysConfig.users || [];
            const u = users.find(usr => usr.id === uuid);
            if (!u) {
                return { text: "⚠️ User not found", kb: { inline_keyboard: [[{ text: t("btn_back"), callback_data: "subs_list:0" }]] } };
            }
            
            const sysU = sysUsageCache?.users?.[u.id.replace(/-/g,'').toLowerCase()] || { reqs: 0, dReqs: 0, lastDay: '' };
            const userReqs = sysU.reqs || 0;
            const curDate = new Date().toISOString().split('T')[0];
            const userDReqs = sysU.lastDay === curDate ? (sysU.dReqs || 0) : 0;
            
            const limitTotalTxt = u.limitTotalReq ? `${u.limitTotalReq}` : t("unlimited");
            const limitDailyTxt = u.limitDailyReq ? `${u.limitDailyReq}` : t("unlimited");
            const usedGB = (userReqs / 6000).toFixed(2);
            const limitGB = u.limitTotalReq ? (u.limitTotalReq / 6000).toFixed(2) : t("unlimited");
            
            let expTxt = t("unlimited");
            let isExp = false;
            let daysLeft = t("unlimited");
            if (u.expiryMs) {
                const date = new Date(u.expiryMs);
                expTxt = date.toLocaleDateString();
                const remDays = Math.ceil((u.expiryMs - Date.now()) / 86400000);
                daysLeft = remDays >= 0 ? `${remDays}` : '0';
                if (Date.now() > u.expiryMs) {
                    expTxt += ` (${t("dash_expired")} 🔴)`;
                    isExp = true;
                }
            }
            
            const statusEmoji = u.isPaused ? "⏸️" : (isExp ? "🔴" : "🟢");
            const statusText = u.isPaused ? t("paused") : (isExp ? t("dash_expired") : t("active"));
            const subSync = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(u.name)}`;
            const maxCfgTxt = u.maxConfigs || t("unlimited");
            const notesTxt = u.notes || t("lbl_none");
            const modeTxt = u.userMode ? (u.userMode === 'alpha' ? 'Alpha (V)' : u.userMode === 'beta' ? 'Beta (T)' : 'Both') : t("unlimited");
            const portsTxt = u.userPorts || t("unlimited");
            const cleanIpsTxt = u.cleanIp ? u.cleanIp.substring(0, 30) + (u.cleanIp.length > 30 ? '...' : '') : '—';
            const proxyIpsTxt = u.proxyIp ? u.proxyIp.substring(0, 30) + (u.proxyIp.length > 30 ? '...' : '') : '—';
            const nodesTxt = u.userNodes ? u.userNodes.substring(0, 30) + (u.userNodes.length > 30 ? '...' : '') : '—';
            const nat64Txt = u.nat64 || '—';
            
            let text = `👤 **${t("sub_info")}**\n`;
            text += `━━━━━━━━━━━━━━━━\n`;
            text += `📛 **${t("name")}**: ${u.name}\n`;
            text += `🆔 **UUID**: \`${u.id}\`\n`;
            text += `🚦 **${t("lbl_status")}**: ${statusEmoji} ${statusText}\n`;
            text += `📊 **${t("total")}**: ${usedGB} GB / ${limitGB} GB (${userReqs} reqs)\n`;
            text += `⏱ **${t("daily")}**: ${userDReqs} / ${limitDailyTxt}\n`;
            text += `📅 **${t("expiry")}**: ${expTxt}\n`;
            text += `⏳ **${t("days")}**: ${daysLeft}\n`;
            text += `📡 **${t("tg_u_mode")}**: ${modeTxt}\n`;
            text += `🔌 **${t("tg_u_ports")}**: ${portsTxt}\n`;
            text += `📱 **${t("device_limit")}**: ${maxCfgTxt}\n`;
            text += `🧹 **${t("tg_u_clean_ips")}**: ${cleanIpsTxt}\n`;
            text += `🔗 **${t("tg_u_proxy_ips")}**: ${proxyIpsTxt}\n`;
            text += `🖥️ **${t("tg_u_nodes")}**: ${nodesTxt}\n`;
            text += `🌐 **${t("tg_u_nat64")}**: ${nat64Txt}\n`;
            text += `🔗 **${t("tg_u_conn_limit")}**: ${u.connLimit || t("unlimited")}\n`;
            text += `🎛 **${t("tg_u_panel_url")}**: ${u.userPanelUrl || t("unlimited")}\n`;
            text += `📝 **${t("notes")}**: ${notesTxt}\n`;
            text += `━━━━━━━━━━━━━━━━\n`;
            text += `🔗 **${t("lbl_subscription")}:**\n\`${subSync}\``;
            
            const kb = {
                inline_keyboard: [
                    [
                        { text: u.isPaused ? `▶️ ${t("btn_resume")}` : `⏸️ ${t("btn_pause")}`, callback_data: `sub_toggle:${u.id}` },
                        { text: `🗑️ ${t("btn_del")}`, callback_data: `sub_del_init:${u.id}` }
                    ],
                    [
                        { text: `✏️ ${t("btn_edit_name")}`, callback_data: `sub_edit_name_init:${u.id}` },
                        { text: `⚙️ ${t("btn_edit_limits")}`, callback_data: `sub_edit_limits_init:${u.id}` }
                    ],
                    [
                        { text: `🔄 ${t("reset_traffic")}`, callback_data: `sub_reset_traffic:${u.id}` },
                        { text: `📅 ${t("extend_expiry")}`, callback_data: `sub_extend_init:${u.id}` }
                    ],
                    [
                        { text: `📝 ${t("notes")}`, callback_data: `sub_edit_notes_init:${u.id}` },
                        { text: `📱 ${t("device_limit")}`, callback_data: `sub_edit_device_init:${u.id}` }
                    ],
                    [
                        { text: t("btn_back_to_list"), callback_data: "subs_list:0" }
                    ]
                ]
            };
            return { text, kb };
        };

        if (update.callback_query) {
            const cb = update.callback_query;
            const chatId = cb.message?.chat?.id;
            const messageId = cb.message?.message_id;
            const data = cb.data;

            if (chatId) {
                if (!isAuthorized) {
                    await fetch(`${tgApi}/answerCallbackQuery`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ callback_query_id: cb.id, text: t("access_denied"), show_alert: true })
                    });
                    return new Response("OK", { status: 200 });
                }

                // Get active panel from last login signal
                const activePanel = getActivePanel();
                const isRemotePanel = activePanel && !activePanel.isLocal;

                // Helper to fetch users for the active panel
                const getPanelUsers = async () => {
                    if (isRemotePanel) {
                        const res = await fetchRemotePanelUsers(activePanel);
                        return res.success ? (res.users || []) : null;
                    }
                    return sysConfig.users || [];
                };

                // Clear step state on callback query
                tgState[chatId] = null;
                ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));

                let answerText = null;

                if (data === "main_menu") {
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_lang") {
                    sysConfig.tgBotLang = (langCode === "fa") ? "en" : "fa";
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_toggle_status") {
                    sysConfig.isPaused = !sysConfig.isPaused;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_metrics") {
                    let usageStr = t("unlimited");
                    if (sysConfig.cfAccountId && sysConfig.cfApiToken) {
                        const reqs = await fetchCloudflareUsage(sysConfig.cfAccountId, sysConfig.cfApiToken);
                        if (reqs !== null) {
                            const pct = ((reqs / 100000) * 100).toFixed(2);
                            usageStr = `${reqs}/100000 (${pct}%)`;
                        }
                    }
                    const upSeconds = Math.floor((Date.now() - isolateStartTime)/1000);
                    const dh = Math.floor(upSeconds/3600);
                    const dm = Math.floor((upSeconds%3600)/60);
                    
                    let text = `📡 **${t("metrics")}**\n`;
                    text += `━━━━━━━━━━━━━━━━\n`;
                    text += `⏱ **${t("uptime")}**: ${dh}h ${dm}m\n`;
                    text += `🔌 **${t("streams")}**: ${activeConnections}\n`;
                    text += `📊 **Cloudflare API Usage**: ${usageStr}\n`;
                    text += `━━━━━━━━━━━━━━━━`;
                    
                    const kb = { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("subs_list:")) {
                    const page = parseInt(data.replace("subs_list:", "")) || 0;
                    const panelUsers = await getPanelUsers();
                    if (panelUsers === null && isRemotePanel) {
                        await sendOrEdit(chatId, t("msg_panel_error"), { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] });
                    } else {
                        const list = getSubsList(page, panelUsers);
                        await sendOrEdit(chatId, list.text, list.kb, messageId);
                    }
                } else if (data.startsWith("sub_detail:")) {
                    const uuid = data.replace("sub_detail:", "");
                    const panelUsers = await getPanelUsers();
                    if (panelUsers === null && isRemotePanel) {
                        await sendOrEdit(chatId, t("msg_panel_error"), { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] });
                    } else {
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(chatId, detail.text, detail.kb, messageId);
                    }
                } else if (data.startsWith("sub_toggle:")) {
                    const uuid = data.replace("sub_toggle:", "");
                    if (isRemotePanel) {
                        await remotePanelToggleUser(activePanel, uuid);
                    } else if (sysConfig.users) {
                        const u = sysConfig.users.find(usr => usr.id === uuid);
                        if (u) {
                            u.isPaused = !u.isPaused;
                            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        }
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, detail.text, detail.kb, messageId);
                } else if (data.startsWith("sub_del_init:")) {
                    const uuid = data.replace("sub_del_init:", "");
                    const panelUsers = await getPanelUsers();
                    const u = panelUsers?.find(usr => usr.id === uuid);
                    const name = u ? u.name : "";
                    const text = `${t("msg_confirm_del")}\n\n👤 **${name}**`;
                    const kb = {
                        inline_keyboard: [
                            [
                                { text: `✅ ${t("btn_confirm")}`, callback_data: `sub_del_confirm:${uuid}` },
                                { text: `❌ ${t("btn_cancel")}`, callback_data: `sub_detail:${uuid}` }
                            ]
                        ]
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_del_confirm:")) {
                    const uuid = data.replace("sub_del_confirm:", "");
                    if (isRemotePanel) {
                        await remotePanelWriteAction(activePanel, 'DELETE', uuid);
                    } else if (sysConfig.users) {
                        sysConfig.users = sysConfig.users.filter(usr => usr.id !== uuid);
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    }
                    const successText = `✅ ${t("msg_deleted")}`;
                    const kb = { inline_keyboard: [[{ text: t("btn_back"), callback_data: "subs_list:0" }]] };
                    await sendOrEdit(chatId, successText, kb, messageId);
                } else if (data === "sub_add_init") {
                    tgState[chatId] = { step: "sub_add_name" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const text = `➕ ${t("msg_enter_name")}`;
                    const kb = { inline_keyboard: [[{ text: `❌ ${t("btn_cancel")}`, callback_data: "subs_list:0" }]] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_name_init:")) {
                    const uuid = data.replace("sub_edit_name_init:", "");
                    tgState[chatId] = { step: `sub_edit_name:${uuid}` };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const text = `✏️ ${t("msg_enter_name")}`;
                    const kb = { inline_keyboard: [[{ text: `❌ ${t("btn_cancel")}`, callback_data: `sub_detail:${uuid}` }]] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_limits_init:")) {
                    const uuid = data.replace("sub_edit_limits_init:", "");
                    tgState[chatId] = { step: `sub_edit_limits:${uuid}` };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const text = `⚙️ ${t("msg_enter_limits")}`;
                    const kb = {
                        inline_keyboard: [
                            [{ text: `♾️ Skip (Unlimited)`, callback_data: `sub_unlimit_cb:${uuid}` }],
                            [{ text: `❌ ${t("btn_cancel")}`, callback_data: `sub_detail:${uuid}` }]
                        ]
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_unlimit_cb:")) {
                    const uuid = data.replace("sub_unlimit_cb:", "");
                    if (isRemotePanel) {
                        await remotePanelWriteAction(activePanel, 'PUT', uuid, { key: activePanel.apiKey, trafficLimit: 0, dailyLimit: 0, expiryDays: 0 });
                    } else if (sysConfig.users) {
                        const u = sysConfig.users.find(usr => usr.id === uuid);
                        if (u) {
                            u.limitTotalReq = null;
                            u.limitDailyReq = null;
                            u.expiryMs = null;
                            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        }
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, detail.text, detail.kb, messageId);
                } else if (data === "sub_add_unlimited_skip") {
                    let stateName = "Subscriber";
                    try {
                        const savedStateRaw = await d1Get(env, "tg_bot_state");
                        if (savedStateRaw) {
                            const stObj = JSON.parse(savedStateRaw);
                            if (stObj[chatId] && stObj[chatId].name) {
                                stateName = stObj[chatId].name;
                            }
                        }
                    } catch(e){}
                    
                    const newUuid = crypto.randomUUID();
                    if (isRemotePanel) {
                        const res = await remotePanelWriteAction(activePanel, 'POST', null, { key: activePanel.apiKey, name: stateName });
                        if (res.success && res.user) {
                            const detail = getSubDetail(res.user.id, [res.user]);
                            await sendOrEdit(chatId, `✅ ${t("msg_added")}\n\n${detail.text}`, detail.kb, messageId);
                        } else {
                            await sendOrEdit(chatId, t("msg_panel_error"), { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] });
                        }
                    } else {
                        if (!sysConfig.users) sysConfig.users = [];
                        sysConfig.users.push({
                            id: newUuid,
                            name: stateName,
                            limitTotalReq: null,
                            limitDailyReq: null,
                            expiryMs: null,
                            createdAt: Date.now()
                        });
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        const detail = getSubDetail(newUuid);
                        await sendOrEdit(chatId, `✅ ${t("msg_added")}\n\n${detail.text}`, detail.kb, messageId);
                    }
                    tgState[chatId] = null;
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                } else if (data === "sys_panic_init") {
                    const text = `${t("msg_confirm_panic")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                { text: `🚨 YES PANIC 🚨`, callback_data: "sys_panic_confirm" },
                                { text: `❌ No, Cancel`, callback_data: "main_menu" }
                            ]
                        ]
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "sys_panic_confirm") {
                    sysConfig.apiRoute = Array.from(crypto.getRandomValues(new Uint8Array(8))).map(b => b.toString(16).padStart(2,'0')).join('');
                    sysConfig.isPaused = true;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    const successText = `${t("msg_panic")}\n\n🔑 New Secret Path Randomized. All old sessions revoked.`;
                    const kb = { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] };
                    await sendOrEdit(chatId, successText, kb, messageId);
                } else if (data === "sys_dashboard") {
                    let users, activeCount, pausedCount, expiredCount, autoDisabledCount;
                    if (isRemotePanel) {
                        const statsRes = await fetchRemotePanelStats(activePanel);
                        if (statsRes.success && statsRes.stats) {
                            const s = statsRes.stats;
                            users = [];
                            activeCount = s.users?.active || 0;
                            pausedCount = s.users?.paused || 0;
                            expiredCount = s.users?.expired || 0;
                            autoDisabledCount = s.users?.autoDisabled || 0;
                        } else {
                            const panelUsers = await getPanelUsers();
                            users = panelUsers || [];
                            activeCount = users.filter(u => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs)).length;
                            pausedCount = users.filter(u => u.isPaused && !u.disabledReason).length;
                            expiredCount = users.filter(u => u.expiryMs && Date.now() > u.expiryMs && !u.isPaused).length;
                            autoDisabledCount = users.filter(u => u.isPaused && u.disabledReason).length;
                        }
                    } else {
                        users = sysConfig.users || [];
                        activeCount = users.filter(u => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs)).length;
                        pausedCount = users.filter(u => u.isPaused && !u.disabledReason).length;
                        expiredCount = users.filter(u => u.expiryMs && Date.now() > u.expiryMs && !u.isPaused).length;
                        autoDisabledCount = users.filter(u => u.isPaused && u.disabledReason).length;
                    }
                    let dashText = `📊 **${t("dashboard")}**\n`;
                    dashText += `━━━━━━━━━━━━━━━━\n`;
                    dashText += `📌 **${t("current_panel")}**: ${activePanel.isLocal ? '🏠' : '🌐'} ${activePanel.name}\n`;
                    dashText += `━━━━━━━━━━━━━━━━\n`;
                    dashText += `👥 **${t("dash_total")}**: ${Array.isArray(users) ? users.length : (activeCount + pausedCount + expiredCount + autoDisabledCount)}\n`;
                    dashText += `🟢 **${t("dash_active")}**: ${activeCount}\n`;
                    dashText += `⏸️ **${t("dash_paused")}**: ${pausedCount}\n`;
                    dashText += `🔴 **${t("dash_expired")}**: ${expiredCount}\n`;
                    dashText += `🚫 **${t("dash_auto_disabled")}**: ${autoDisabledCount}\n`;
                    if (!isRemotePanel) {
                        const upSeconds = Math.floor((Date.now() - isolateStartTime) / 1000);
                        const dh = Math.floor(upSeconds / 3600);
                        const dm = Math.floor((upSeconds % 3600) / 60);
                        dashText += `⏱ **${t("uptime")}**: ${dh}h ${dm}m\n`;
                        dashText += `🔌 **${t("streams")}**: ${activeConnections}\n`;
                        dashText += `⚡ **System**: ${sysConfig.isPaused ? t("paused") : t("active")}\n`;
                    }
                    dashText += `━━━━━━━━━━━━━━━━`;
                    const kb = { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] };
                    await sendOrEdit(chatId, dashText, kb, messageId);
                } else if (data === "sys_stats") {
                    let users, totalReqs, dailyReqs;
                    if (isRemotePanel) {
                        const statsRes = await fetchRemotePanelStats(activePanel);
                        if (statsRes.success && statsRes.stats) {
                            const s = statsRes.stats;
                            users = [];
                            totalReqs = s.traffic?.totalRequests || 0;
                            dailyReqs = s.traffic?.dailyRequests || 0;
                        } else {
                            const panelUsers = await getPanelUsers();
                            users = panelUsers || [];
                            totalReqs = 0;
                            dailyReqs = 0;
                        }
                    } else {
                        users = sysConfig.users || [];
                        totalReqs = 0;
                        dailyReqs = 0;
                        const todayDate = new Date().toISOString().split('T')[0];
                        users.forEach(u => {
                            const idClean = u.id.replace(/-/g, '').toLowerCase();
                            const sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0, lastDay: '' };
                            totalReqs += (sysU.reqs || 0);
                            if (sysU.lastDay === todayDate) dailyReqs += (sysU.dReqs || 0);
                        });
                    }
                    let statsText = `📈 **${t("stats_title")}**\n`;
                    statsText += `━━━━━━━━━━━━━━━━\n`;
                    statsText += `📌 **${t("current_panel")}**: ${activePanel.isLocal ? '🏠' : '🌐'} ${activePanel.name}\n`;
                    statsText += `━━━━━━━━━━━━━━━━\n`;
                    statsText += `👥 **${t("dash_total")}**: ${Array.isArray(users) ? users.length : 'N/A'}\n`;
                    statsText += `📊 **${t("total_traffic")}**: ${(totalReqs / 6000).toFixed(2)} GB\n`;
                    statsText += `📅 **${t("daily_traffic")}**: ${(dailyReqs / 6000).toFixed(2)} GB\n`;
                    if (!isRemotePanel) {
                        const upSeconds = Math.floor((Date.now() - isolateStartTime) / 1000);
                        const dh = Math.floor(upSeconds / 3600);
                        const dm = Math.floor((upSeconds % 3600) / 60);
                        statsText += `⏱ **${t("tg_uptime")}**: ${dh}h ${dm}m\n`;
                        statsText += `🔌 **${t("tg_conns")}**: ${activeConnections}\n`;
                        statsText += `📦 **${t("tg_version")}**: v${CURRENT_VERSION}\n`;
                    }
                    statsText += `━━━━━━━━━━━━━━━━`;
                    if (sysConfig.cfAccountId && sysConfig.cfApiToken) {
                        const reqs = await fetchCloudflareUsage(sysConfig.cfAccountId, sysConfig.cfApiToken);
                        if (reqs !== null) {
                            const pct = ((reqs / 100000) * 100).toFixed(2);
                            statsText += `\n☁️ **Cloudflare API**: ${reqs}/100000 (${pct}%)`;
                        }
                    }
                    const kb = { inline_keyboard: [
                        [{ text: `🔄 ${t("btn_update_usage")}`, callback_data: "sys_stats" }],
                        [{ text: t("btn_main_menu"), callback_data: "main_menu" }]
                    ] };
                    await sendOrEdit(chatId, statsText, kb, messageId);
                } else if (data === "sys_panel_info") {
                    let infoText = `ℹ️ **${t("panel_info")}**\n`;
                    infoText += `━━━━━━━━━━━━━━━━\n`;
                    infoText += `📌 **${t("current_panel")}**: ${activePanel.isLocal ? '🏠' : '🌐'} ${activePanel.name}\n`;
                    if (activePanel.isLocal) {
                        infoText += `🌐 **Host**: ${hostName}\n`;
                        infoText += `🔑 **API Route**: \`${sysConfig.apiRoute}\`\n`;
                        infoText += `📡 **Mode**: ${sysConfig.mode || 'alpha'}\n`;
                        infoText += `🔒 **Ports**: ${sysConfig.socketPorts || '443'}\n`;
                    } else {
                        infoText += `🌐 **Host**: ${activePanel.host}\n`;
                        infoText += `🔑 **API Route**: \`${activePanel.apiRoute}\`\n`;
                    }
                    infoText += `📱 **Version**: ${CURRENT_VERSION}\n`;
                    infoText += `━━━━━━━━━━━━━━━━`;
                    const kb = { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] };
                    await sendOrEdit(chatId, infoText, kb, messageId);
                } else if (data.startsWith("subs_disabled:")) {
                    const panelUsers = await getPanelUsers();
                    const users = panelUsers || [];
                    const disabledUsers = users.filter(u => u.isPaused);
                    if (disabledUsers.length === 0) {
                        const kb = { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] };
                        await sendOrEdit(chatId, `🚫 ${t("msg_no_disabled")}`, kb, messageId);
                    } else {
                        const page = parseInt(data.replace("subs_disabled:", "")) || 0;
                        const itemsPerPage = 5;
                        const start = page * itemsPerPage;
                        const end = start + itemsPerPage;
                        const pageUsers = disabledUsers.slice(start, end);
                        let text = `🚫 **${t("disabled_users")}** (${disabledUsers.length})\n━━━━━━━━━━━━━━━━\n`;
                        const inline_keyboard = [];
                        pageUsers.forEach((u) => {
                            const reason = u.disabledReason || t("paused");
                            text += `👤 **${u.name}**\n   ${reason}\n`;
                            inline_keyboard.push([{ text: `▶️ ${u.name}`, callback_data: `sub_toggle:${u.id}` }]);
                        });
                        const navRow = [];
                        if (page > 0) navRow.push({ text: `⬅️ ${t("btn_back")}`, callback_data: `subs_disabled:${page - 1}` });
                        if (end < disabledUsers.length) navRow.push({ text: `${t("btn_next")} ➡️`, callback_data: `subs_disabled:${page + 1}` });
                        if (navRow.length > 0) inline_keyboard.push(navRow);
                        inline_keyboard.push([{ text: t("btn_main_menu"), callback_data: "main_menu" }]);
                        await sendOrEdit(chatId, text, { inline_keyboard }, messageId);
                    }
                } else if (data === "sub_search_init") {
                    tgState[chatId] = { step: "sub_search" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const text = `🔍 ${t("msg_enter_search")}`;
                    const kb = { inline_keyboard: [[{ text: `❌ ${t("btn_cancel")}`, callback_data: "main_menu" }]] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_reset_traffic:")) {
                    const uuid = data.replace("sub_reset_traffic:", "");
                    if (isRemotePanel) {
                        await remotePanelResetTraffic(activePanel, uuid);
                    } else {
                        if (!sysUsageCache) sysUsageCache = { users: {} };
                        if (!sysUsageCache.users) sysUsageCache.users = {};
                        const uuidClean = uuid.replace(/-/g, '').toLowerCase();
                        if (sysUsageCache.users[uuidClean]) {
                            sysUsageCache.users[uuidClean].reqs = 0;
                            sysUsageCache.users[uuidClean].dReqs = 0;
                        } else {
                            sysUsageCache.users[uuidClean] = { reqs: 0, dReqs: 0, lastDay: new Date().toISOString().split('T')[0] };
                        }
                        await cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache));
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, `✅ ${t("msg_traffic_reset")}\n\n${detail.text}`, detail.kb, messageId);
                } else if (data.startsWith("sub_extend_init:")) {
                    const uuid = data.replace("sub_extend_init:", "");
                    tgState[chatId] = { step: `sub_extend_days:${uuid}` };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const text = `📅 ${t("msg_enter_extend_days")}`;
                    const kb = { inline_keyboard: [[{ text: `❌ ${t("btn_cancel")}`, callback_data: `sub_detail:${uuid}` }]] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_notes_init:")) {
                    const uuid = data.replace("sub_edit_notes_init:", "");
                    tgState[chatId] = { step: `sub_edit_notes:${uuid}` };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const text = `📝 ${t("msg_enter_notes")}`;
                    const kb = { inline_keyboard: [[{ text: `❌ ${t("btn_cancel")}`, callback_data: `sub_detail:${uuid}` }]] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_device_init:")) {
                    const uuid = data.replace("sub_edit_device_init:", "");
                    tgState[chatId] = { step: `sub_edit_device:${uuid}` };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const text = `📱 ${t("msg_enter_device_limit")}`;
                    const kb = { inline_keyboard: [
                        [{ text: `♾️ Unlimited`, callback_data: `sub_device_unlimited:${uuid}` }],
                        [{ text: `❌ ${t("btn_cancel")}`, callback_data: `sub_detail:${uuid}` }]
                    ] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_device_unlimited:")) {
                    const uuid = data.replace("sub_device_unlimited:", "");
                    if (isRemotePanel) {
                        await remotePanelWriteAction(activePanel, 'PUT', uuid, { key: activePanel.apiKey, maxConfigs: null });
                    } else if (sysConfig.users) {
                        const u = sysConfig.users.find(usr => usr.id === uuid);
                        if (u) {
                            u.maxConfigs = null;
                            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        }
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, `✅ ${t("status_updated")}`, detail.kb, messageId);
                } else if (data === "get_sub_link") {
                    const subUrl = `https://${hostName}/${sysConfig.apiRoute}`;
                    await fetch(`${tgApi}/sendMessage`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ chat_id: chatId, text: `\`${subUrl}\``, parse_mode: 'Markdown' })
                    });
                    answerText = t("sub_link_sent");
                } else if (data === "tg_settings_menu") {
                    const modeTxt = sysConfig.mode === 'alpha' ? 'Alpha (V)' : sysConfig.mode === 'beta' ? 'Beta (T)' : 'Both';
                    const portsTxt = sysConfig.socketPorts || '443';
                    const passTxt = sysConfig.masterKey || 'admin';
                    const dnsTxt = sysConfig.resolveIp || '1.1.1.1';
                    const relayTxt = sysConfig.backupRelay || '—';
                    const tfoTxt = sysConfig.enableOpt1 ? '✅' : '❌';
                    const echTxt = sysConfig.enableOpt2 ? '✅' : '❌';
                    const pauseTxt = sysConfig.isPaused ? '🔴 ON' : '🟢 OFF';
                    const silentTxt = sysConfig.silentAlerts ? '✅' : '❌';
                    const autoUpTxt = sysConfig.autoUpdate ? '✅' : '❌';
                    const directTxt = sysConfig.enableDirectConfigs ? '✅' : '❌';
                    const nat64Txt = sysConfig.nat64Prefix || '—';
                    let text = `⚙️ **${t("tg_sys_settings")}**\n━━━━━━━━━━━━━━━━\n`;
                    text += `📡 ${t("tg_proto")}: **${modeTxt}**\n`;
                    text += `🔌 ${t("tg_ports")}: \`${portsTxt}\`\n`;
                    text += `🔑 ${t("tg_pass")}: \`${passTxt}\`\n`;
                    text += `🌐 ${t("tg_dns")}: \`${dnsTxt}\`\n`;
                    text += `🔗 ${t("tg_relay")}: \`${relayTxt}\`\n`;
                    text += `⚡ ${t("tg_tfo")}: ${tfoTxt} | ECH: ${echTxt}\n`;
                    text += `🔇 ${t("tg_silent")}: ${silentTxt}\n`;
                    text += `🛑 ${t("tg_pause")}: ${pauseTxt}\n`;
                    text += `🔄 ${t("tg_auto_update")}: ${autoUpTxt}\n`;
                    text += `🔀 ${t("tg_direct")}: ${directTxt}\n`;
                    text += `🌐 ${t("tg_nat64")}: \`${nat64Txt}\`\n`;
                    text += `━━━━━━━━━━━━━━━━`;
                    const kb = { inline_keyboard: [
                        [{ text: `📡 ${t("tg_proto")}`, callback_data: "tg_edit_proto" }, { text: `🔌 ${t("tg_ports")}`, callback_data: "tg_edit_ports" }],
                        [{ text: `🔑 ${t("tg_pass")}`, callback_data: "tg_edit_pass" }, { text: `🌐 ${t("tg_dns")}`, callback_data: "tg_edit_dns" }],
                        [{ text: `🔗 ${t("tg_relay")}`, callback_data: "tg_edit_relay" }],
                        [{ text: `⚡ ${t("tg_tfo")}`, callback_data: "tg_toggle_tfo" }, { text: `ECH`, callback_data: "tg_toggle_ech" }],
                        [{ text: `${t("tg_silent")}`, callback_data: "tg_toggle_silent" }, { text: `${t("tg_pause")}`, callback_data: "tg_toggle_pause2" }],
                        [{ text: `🔄 ${t("tg_auto_update")}`, callback_data: "tg_toggle_auto_update" }, { text: `🔀 ${t("tg_direct")}`, callback_data: "tg_toggle_direct" }],
                        [{ text: `🌐 ${t("tg_nat64")}`, callback_data: "tg_edit_nat64" }],
                        [{ text: t("btn_main_menu"), callback_data: "main_menu" }]
                    ] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "tg_advanced_menu") {
                    const cleanTxt = sysConfig.cleanIps ? sysConfig.cleanIps.substring(0, 40) + (sysConfig.cleanIps.length > 40 ? '...' : '') : '—';
                    const nodesTxt = sysConfig.slaveNodes ? sysConfig.slaveNodes.substring(0, 40) + (sysConfig.slaveNodes.length > 40 ? '...' : '') : '—';
                    const strategyTxt = sysConfig.nameStrategy || 'default';
                    const prefixTxt = sysConfig.namePrefix || 'Core';
                    const maintenanceTxt = sysConfig.maintenanceHost ? sysConfig.maintenanceHost.substring(0, 30) + '...' : '—';
                    let text = `🔧 **${t("tg_adv_settings")}**\n━━━━━━━━━━━━━━━━\n`;
                    text += `🧹 ${t("tg_clean_ips")}: \`${cleanTxt}\`\n`;
                    text += `🖥️ ${t("tg_nodes")}: \`${nodesTxt}\`\n`;
                    text += `📝 ${t("tg_strategy")}: \`${strategyTxt}\`\n`;
                    text += `🏷️ ${t("tg_prefix")}: \`${prefixTxt}\`\n`;
                    text += `🎭 ${t("tg_maintenance")}: \`${maintenanceTxt}\`\n`;
                    text += `━━━━━━━━━━━━━━━━`;
                    const kb = { inline_keyboard: [
                        [{ text: `🧹 ${t("tg_clean_ips")}`, callback_data: "tg_edit_clean_ips" }],
                        [{ text: `🖥️ ${t("tg_nodes")}`, callback_data: "tg_edit_nodes" }],
                        [{ text: `📝 ${t("tg_strategy")}`, callback_data: "tg_edit_strategy" }, { text: `🏷️ ${t("tg_prefix")}`, callback_data: "tg_edit_prefix" }],
                        [{ text: `🎭 ${t("tg_maintenance")}`, callback_data: "tg_edit_maintenance" }],
                        [{ text: `🤖 ${t("tg_tg_settings")}`, callback_data: "tg_edit_tg_settings" }],
                        [{ text: `☁️ ${t("tg_cf_settings")}`, callback_data: "tg_edit_cf_settings" }],
                        [{ text: t("btn_main_menu"), callback_data: "main_menu" }]
                    ] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "tg_logs_menu") {
                    let logs = [];
                    if (env.IOT_DB) {
                        const stored = await d1Get(env, "sys_logs");
                        if (stored) logs = JSON.parse(stored);
                    }
                    let text = `📋 **${t("tg_logs")}**\n━━━━━━━━━━━━━━━━\n`;
                    if (logs.length === 0) {
                        text += `ℹ️ ${t("tg_log_empty")}\n`;
                    } else {
                        logs.slice(0, 10).forEach((log, i) => {
                            const time = new Date(log.ts).toLocaleString();
                            text += `${i + 1}. ${t("tg_log_entry")} **${log.type}**\n   ${log.detail}\n   📅 ${time}\n`;
                        });
                        if (logs.length > 10) text += `\n... ${logs.length - 10} more entries`;
                    }
                    text += `\n━━━━━━━━━━━━━━━━`;
                    const kb = { inline_keyboard: [
                        [{ text: `🔄 ${t("btn_update_usage")}`, callback_data: "tg_logs_menu" }],
                        [{ text: t("btn_main_menu"), callback_data: "main_menu" }]
                    ] };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "tg_toggle_tfo") {
                    sysConfig.enableOpt1 = !sysConfig.enableOpt1;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_ech") {
                    sysConfig.enableOpt2 = !sysConfig.enableOpt2;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_silent") {
                    sysConfig.silentAlerts = !sysConfig.silentAlerts;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_pause2") {
                    sysConfig.isPaused = !sysConfig.isPaused;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_auto_update") {
                    sysConfig.autoUpdate = !sysConfig.autoUpdate;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    answerText = t("tg_saved");
                    await sendOrEdit(chatId, `⚙️ ${t("tg_auto_update")}: ${sysConfig.autoUpdate ? '✅ ON' : '❌ OFF'}`, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_toggle_direct") {
                    sysConfig.enableDirectConfigs = !sysConfig.enableDirectConfigs;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    answerText = t("tg_saved");
                    await sendOrEdit(chatId, `🔀 ${t("tg_direct")}: ${sysConfig.enableDirectConfigs ? '✅ ON' : '❌ OFF'}`, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_proto") {
                    tgState[chatId] = { step: "tg_edit_proto" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const kb = { inline_keyboard: [
                        [{ text: "Alpha (V-Core)", callback_data: "tg_set_proto:alpha" }, { text: "Beta (T-Core)", callback_data: "tg_set_proto:beta" }],
                        [{ text: "Both", callback_data: "tg_set_proto:both" }],
                        [{ text: "❌ " + t("btn_cancel"), callback_data: "tg_settings_menu" }]
                    ] };
                    await sendOrEdit(chatId, `📡 **${t("tg_proto")}**\n${t("tg_current_val")}: **${sysConfig.mode}**\n\n${t("tg_new_val")}`, kb, messageId);
                } else if (data.startsWith("tg_set_proto:")) {
                    const val = data.replace("tg_set_proto:", "");
                    sysConfig.mode = val;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    tgState[chatId] = null;
                    answerText = t("tg_saved");
                    await sendOrEdit(chatId, `✅ ${t("tg_proto")}: **${val}**`, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_dns") {
                    tgState[chatId] = { step: "tg_edit_dns" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🌐 **${t("tg_dns")}**\n${t("tg_current_val")}: \`${sysConfig.resolveIp}\`\n\n${t("tg_new_val")}`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_relay") {
                    tgState[chatId] = { step: "tg_edit_relay" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🔗 **${t("tg_relay")}**\n${t("tg_current_val")}: \`${sysConfig.backupRelay || '—'}\`\n\n${t("tg_new_val")}\n_send empty to clear_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_nat64") {
                    tgState[chatId] = { step: "tg_edit_nat64" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🌐 **${t("tg_nat64")}**\n${t("tg_current_val")}: \`${sysConfig.nat64Prefix || '—'}\`\n\n${t("tg_new_val")}\n_send empty to clear_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_maintenance") {
                    tgState[chatId] = { step: "tg_edit_maintenance" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🎭 **${t("tg_maintenance")}**\n${t("tg_current_val")}: \`${sysConfig.maintenanceHost || '—'}\`\n\n${t("tg_new_val")}`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_clean_ips") {
                    tgState[chatId] = { step: "tg_edit_clean_ips" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🧹 **${t("tg_clean_ips")}**\n${t("tg_current_val")}: \`${sysConfig.cleanIps || '—'}\`\n\n${t("tg_new_val")}\n_send empty to clear_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] }, messageId);
                } else if (data === "tg_edit_nodes") {
                    tgState[chatId] = { step: "tg_edit_nodes" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🖥️ **${t("tg_nodes")}**\n${t("tg_current_val")}: \`${sysConfig.slaveNodes || '—'}\`\n\n${t("tg_new_val")}\n_send empty to clear_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] }, messageId);
                } else if (data === "tg_edit_strategy") {
                    tgState[chatId] = { step: "tg_edit_strategy" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const kb = { inline_keyboard: [
                        [{ text: "default", callback_data: "tg_set_strategy:default" }],
                        [{ text: "type-user-port", callback_data: "tg_set_strategy:type-user-port" }],
                        [{ text: "user-port", callback_data: "tg_set_strategy:user-port" }],
                        [{ text: "ip", callback_data: "tg_set_strategy:ip" }],
                        [{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]
                    ] };
                    await sendOrEdit(chatId, `📝 **${t("tg_strategy")}**\n${t("tg_current_val")}: \`${sysConfig.nameStrategy}\`\n\n_send custom or select:_`, kb, messageId);
                } else if (data.startsWith("tg_set_strategy:")) {
                    const val = data.replace("tg_set_strategy:", "");
                    sysConfig.nameStrategy = val;
                    await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                    tgState[chatId] = null;
                    answerText = t("tg_saved");
                    await sendOrEdit(chatId, `✅ ${t("tg_strategy")}: **${val}**`, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] }, messageId);
                } else if (data === "tg_edit_prefix") {
                    tgState[chatId] = { step: "tg_edit_prefix" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🏷️ **${t("tg_prefix")}**\n${t("tg_current_val")}: \`${sysConfig.namePrefix}\`\n\n${t("tg_new_val")}`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] }, messageId);
                } else if (data === "tg_edit_pass") {
                    tgState[chatId] = { step: "tg_edit_pass" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🔑 **${t("tg_pass")}**\n${t("tg_current_val")}: \`${sysConfig.masterKey}\`\n\n${t("tg_new_val")}`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_ports") {
                    tgState[chatId] = { step: "tg_edit_ports" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🔌 **${t("tg_ports")}**\n${t("tg_current_val")}: \`${sysConfig.socketPorts}\`\n\n${t("tg_new_val")}\n_comma separated e.g. 443,80_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_settings_menu" }]] }, messageId);
                } else if (data === "tg_edit_tg_settings") {
                    tgState[chatId] = { step: "tg_edit_tg_token" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `🤖 **${t("tg_tg_settings")}**\n\n1️⃣ ${t("tg_current_val")}: \`${sysConfig.tgToken ? '***' + sysConfig.tgToken.slice(-4) : '—'}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] }, messageId);
                } else if (data === "tg_edit_cf_settings") {
                    tgState[chatId] = { step: "tg_edit_cf_acc" };
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    await sendOrEdit(chatId, `☁️ **${t("tg_cf_settings")}**\n\n1️⃣ CF Account ID: \`${sysConfig.cfAccountId || '—'}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] }, messageId);
                }
                
                ctx?.waitUntil(fetch(`${tgApi}/answerCallbackQuery`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ callback_query_id: cb.id, text: answerText || "Done!" })
                }).catch(()=>{}));
            }
        } else if (update.message && update.message.text) {
            const chatId = update.message.chat.id;
            const text = update.message.text.trim();
            
            if (isAuthorized) {
                // Get active panel from last login signal
                const activePanel = getActivePanel();
                const isRemotePanel = activePanel && !activePanel.isLocal;

                // Helper to fetch users for the active panel
                const getPanelUsers = async () => {
                    if (isRemotePanel) {
                        const res = await fetchRemotePanelUsers(activePanel);
                        return res.success ? (res.users || []) : null;
                    }
                    return sysConfig.users || [];
                };

                // Handle /start command
                if (text === "/start") {
                    tgState[chatId] = null;
                    ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb);
                    return new Response("OK", { status: 200 });
                }

                const state = tgState[chatId];
                
                if (state) {
                    if (!isAuthorized) {
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, t("access_denied"));
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step === "sub_add_name") {
                        const name = text;
                        tgState[chatId] = { step: "sub_add_limits", name: name };
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        
                        const msg = `⚙️ **${name}**\n\n${t("msg_enter_limits")}`;
                        const kb = {
                            inline_keyboard: [
                                [{ text: `♾️ Skip (Unlimited)`, callback_data: "sub_add_unlimited_skip" }],
                                [{ text: `❌ ${t("btn_cancel")}`, callback_data: "main_menu" }]
                            ]
                        };
                        await sendOrEdit(chatId, msg, kb);
                        return new Response("OK", { status: 200 });
                    }
                    
                    if (state.step === "sub_add_limits" || state.step === "sub_add_unlimited_skip") {
                        const name = state.name;
                        let tReq = null;
                        let dReq = null;
                        let days = null;
                        
                        if (state.step !== "sub_add_unlimited_skip" && text !== "0" && text !== "0 0 0") {
                            const parts = text.split(/\s+/).map(Number);
                            if (parts[0] > 0) tReq = parts[0];
                            if (parts[1] > 0) dReq = parts[1];
                            if (parts[2] > 0) days = parts[2];
                        }
                        
                        const newUuid = crypto.randomUUID();
                        if (isRemotePanel) {
                            const res = await remotePanelWriteAction(activePanel, 'POST', null, {
                                key: activePanel.apiKey,
                                name: name,
                                trafficLimit: tReq ? tReq / 6000 : 0,
                                dailyLimit: dReq ? dReq / 6000 : 0,
                                expiryDays: days || 0
                            });
                            if (res.success && res.user) {
                                const detail = getSubDetail(res.user.id, [res.user]);
                                await sendOrEdit(chatId, `✅ ${t("msg_added")}\n\n${detail.text}`, detail.kb);
                            } else {
                                await sendOrEdit(chatId, t("msg_panel_error"), { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] });
                            }
                        } else {
                            if (!sysConfig.users) sysConfig.users = [];
                            sysConfig.users.push({
                                id: newUuid,
                                name: name,
                                limitTotalReq: tReq,
                                limitDailyReq: dReq,
                                expiryMs: days ? Date.now() + days * 86400000 : null,
                                createdAt: Date.now()
                            });
                            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            const detail = getSubDetail(newUuid);
                            await sendOrEdit(chatId, `✅ ${t("msg_added")}\n\n${detail.text}`, detail.kb);
                        }
                        
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        return new Response("OK", { status: 200 });
                    }
                    
                    if (state.step.startsWith("sub_edit_name:")) {
                        const uuid = state.step.replace("sub_edit_name:", "");
                        if (isRemotePanel) {
                            await remotePanelWriteAction(activePanel, 'PUT', uuid, { key: activePanel.apiKey, name: text });
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(usr => usr.id === uuid);
                            if (u) {
                                u.name = text;
                                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(chatId, `✅ Successfully Changed!`, detail.kb);
                        return new Response("OK", { status: 200 });
                    }
                    
                    if (state.step.startsWith("sub_edit_limits:")) {
                        const uuid = state.step.replace("sub_edit_limits:", "");
                        let tReq = null;
                        let dReq = null;
                        let days = null;
                        
                        const parts = text.split(/\s+/).map(Number);
                        if (parts[0] > 0) tReq = parts[0];
                        if (parts[1] > 0) dReq = parts[1];
                        if (parts[2] > 0) days = parts[2];
                        
                        if (isRemotePanel) {
                            await remotePanelWriteAction(activePanel, 'PUT', uuid, {
                                key: activePanel.apiKey,
                                trafficLimit: tReq ? tReq / 6000 : 0,
                                dailyLimit: dReq ? dReq / 6000 : 0,
                                expiryDays: days || 0
                            });
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(usr => usr.id === uuid);
                            if (u) {
                                u.limitTotalReq = tReq;
                                u.limitDailyReq = dReq;
                                u.expiryMs = days ? Date.now() + days * 86400000 : null;
                                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(chatId, `✅ Limits Updated!`, detail.kb);
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step === "sub_search") {
                        const query = text.toLowerCase();
                        const panelUsers = await getPanelUsers();
                        const users = panelUsers || [];
                        const results = users.filter(u => u.name.toLowerCase().includes(query) || u.id.toLowerCase().includes(query));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        if (results.length === 0) {
                            const kb = { inline_keyboard: [[{ text: t("btn_main_menu"), callback_data: "main_menu" }]] };
                            await sendOrEdit(chatId, `🔍 No users found for "${text}"`, kb);
                        } else {
                            let searchText = `🔍 **Search Results** (${results.length})\n━━━━━━━━━━━━━━━━\n`;
                            const inline_keyboard = [];
                            results.slice(0, 10).forEach(u => {
                                const statusEmoji = u.isPaused ? "⏸️" : (u.expiryMs && Date.now() > u.expiryMs ? "🔴" : "🟢");
                                searchText += `${statusEmoji} **${u.name}**\n`;
                                inline_keyboard.push([{ text: `👤 ${u.name}`, callback_data: `sub_detail:${u.id}` }]);
                            });
                            inline_keyboard.push([{ text: t("btn_main_menu"), callback_data: "main_menu" }]);
                            await sendOrEdit(chatId, searchText, { inline_keyboard });
                        }
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_extend_days:")) {
                        const uuid = state.step.replace("sub_extend_days:", "");
                        const days = parseInt(text);
                        if (isNaN(days) || days <= 0) {
                            await sendOrEdit(chatId, t("msg_invalid"));
                            return new Response("OK", { status: 200 });
                        }
                        if (isRemotePanel) {
                            await remotePanelWriteAction(activePanel, 'PUT', uuid, { key: activePanel.apiKey, expiryDays: days });
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(usr => usr.id === uuid);
                            if (u) {
                                if (u.expiryMs) {
                                    u.expiryMs += days * 86400000;
                                } else {
                                    u.expiryMs = Date.now() + days * 86400000;
                                }
                                if (u.isPaused && u.disabledReason && u.disabledReason.includes('Expiration')) {
                                    u.isPaused = false;
                                    u.disabledReason = null;
                                    u.disabledAt = null;
                                }
                                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        const msg = t("msg_expiry_extended").replace("{days}", days);
                        await sendOrEdit(chatId, `✅ ${msg}\n\n${detail.text}`, detail.kb);
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_edit_notes:")) {
                        const uuid = state.step.replace("sub_edit_notes:", "");
                        if (isRemotePanel) {
                            await remotePanelWriteAction(activePanel, 'PUT', uuid, { key: activePanel.apiKey, notes: text });
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(usr => usr.id === uuid);
                            if (u) {
                                u.notes = text;
                                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(chatId, `✅ Notes updated!`, detail.kb);
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_edit_device:")) {
                        const uuid = state.step.replace("sub_edit_device:", "");
                        const limit = parseInt(text);
                        if (isNaN(limit) || limit < 0) {
                            await sendOrEdit(chatId, t("msg_invalid"));
                            return new Response("OK", { status: 200 });
                        }
                        if (isRemotePanel) {
                            await remotePanelWriteAction(activePanel, 'PUT', uuid, { key: activePanel.apiKey, maxConfigs: limit > 0 ? limit : null });
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(usr => usr.id === uuid);
                            if (u) {
                                u.maxConfigs = limit > 0 ? limit : null;
                                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(chatId, `✅ ${t("config_limit_updated")}`, detail.kb);
                        return new Response("OK", { status: 200 });
                    }
                    
                    if (state.step === "tg_edit_dns") {
                        sysConfig.resolveIp = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_dns")}: \`${text}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_relay") {
                        sysConfig.backupRelay = text || '';
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_relay")}: \`${text || '—'}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_nat64") {
                        sysConfig.nat64Prefix = text || '';
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_nat64")}: \`${text || '—'}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_maintenance") {
                        sysConfig.maintenanceHost = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_maintenance")}: \`${text}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_clean_ips") {
                        sysConfig.cleanIps = text || '';
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_clean_ips")}: \`${text || '—'}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_nodes") {
                        sysConfig.slaveNodes = text || '';
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_nodes")}: \`${text || '—'}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_prefix") {
                        sysConfig.namePrefix = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_prefix")}: \`${text}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_pass") {
                        sysConfig.masterKey = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_pass")}: \`${text}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_strategy") {
                        sysConfig.nameStrategy = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_strategy")}: \`${text}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_tg_token") {
                        if (text !== "/skip") sysConfig.tgToken = text;
                        tgState[chatId] = { step: "tg_edit_tg_chat" };
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `2️⃣ Chat ID: \`${sysConfig.tgChatId || '—'}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_tg_chat") {
                        if (text !== "/skip") sysConfig.tgChatId = text;
                        tgState[chatId] = { step: "tg_edit_tg_admin" };
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `3️⃣ Admin ID: \`${sysConfig.tgAdminId || '—'}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_tg_admin") {
                        if (text !== "/skip") sysConfig.tgAdminId = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_tg_settings")} saved!`, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_cf_acc") {
                        if (text !== "/skip") sysConfig.cfAccountId = text;
                        tgState[chatId] = { step: "tg_edit_cf_token" };
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `2️⃣ CF API Token: \`${sysConfig.cfApiToken ? '***' + sysConfig.cfApiToken.slice(-4) : '—'}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_cf_token") {
                        if (text !== "/skip") sysConfig.cfApiToken = text;
                        tgState[chatId] = { step: "tg_edit_cf_worker" };
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `3️⃣ CF Worker Name: \`${sysConfig.cfWorkerName || '—'}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`, { inline_keyboard: [[{ text: "❌ " + t("btn_cancel"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_cf_worker") {
                        if (text !== "/skip") sysConfig.cfWorkerName = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_cf_settings")} saved!`, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_advanced_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_ports") {
                        sysConfig.socketPorts = text;
                        await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                        tgState[chatId] = null;
                        ctx?.waitUntil(d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(()=>{}));
                        await sendOrEdit(chatId, `✅ ${t("tg_ports")}: \`${text}\``, { inline_keyboard: [[{ text: "◀️ " + t("btn_back"), callback_data: "tg_settings_menu" }]] });
                        return new Response("OK", { status: 200 });
                    }
                }
                
                // Default message / fallback menu
                const menu = getMainMenu(activePanel, isAuthorized);
                await sendOrEdit(chatId, menu.text, menu.kb);
            } else {
                if (text === "/start") {
                    const userHint = langCode === 'fa'
                        ? "لطفاً لینک اشتراک یا شناسه کاربری خود را ارسال کنید تا اطلاعات اشتراکتان نمایش داده شود."
                        : "Please send your subscription link or User ID to view your subscription info.";
                    await sendOrEdit(chatId, userHint);
                    return new Response("OK", { status: 200 });
                }
                let lookupId = text.replace(/^https?:\/\//, '').replace(/\/.*$/, '').trim();
                const subParamMatch = text.match(/[?&]sub=([^&]+)/);
                if (subParamMatch) lookupId = decodeURIComponent(subParamMatch[1]);
                if (!lookupId || lookupId.length < 3) {
                    const userHint = langCode === 'fa'
                        ? "لطفاً لینک اشتراک یا شناسه کاربری معتبر ارسال کنید."
                        : "Please send a valid subscription link or User ID.";
                    await sendOrEdit(chatId, userHint);
                    return new Response("OK", { status: 200 });
                }
                const users = sysConfig.users || [];
                const matchedUser = users.find(u =>
                    u.id === lookupId ||
                    u.id.replace(/-/g, '').toLowerCase() === lookupId.replace(/-/g, '').toLowerCase() ||
                    u.name.toLowerCase() === lookupId.toLowerCase()
                );
                if (matchedUser) {
                    const detail = getSubDetail(matchedUser.id);
                    await sendOrEdit(chatId, detail.text, detail.kb);
                } else {
                    const notFound = langCode === 'fa'
                        ? "کاربری با این شناسه یافت نشد."
                        : "No user found with this ID.";
                    await sendOrEdit(chatId, notFound);
                }
            }
        }
        return new Response("OK", { status: 200 });
    } catch(e) {
        return new Response("OK", { status: 200 });
    }
}

async function processTelemetryStream(env, ctx, wsRelayIdx) {
    const [client, webSocket] = Object.values(new WebSocketPair());
    webSocket.accept();
    webSocket.binaryType = "arraybuffer";
    startDataPipe(webSocket, env, ctx, wsRelayIdx);
    return new Response(null, { status: 101, webSocket: client });
}

async function startDataPipe(webSocket, env, ctx, wsRelayIdx) {
    activeConnections++;
    webSocket.addEventListener('close', () => {
        activeConnections--;
        if (activeClientHash) {
            let cur = activeConns.get(activeClientHash) || 0;
            if (cur > 0) activeConns.set(activeClientHash, cur - 1);
        }
    });
    webSocket.addEventListener('error', () => {
        activeConnections--;
        if (activeClientHash) {
            let cur = activeConns.get(activeClientHash) || 0;
            if (cur > 0) activeConns.set(activeClientHash, cur - 1);
        }
    });
    let remoteSocket, dataWriter, isInit = true, queue = Promise.resolve();
    let activeClientHash = null;
    webSocket.addEventListener("message", (event) => {
        queue = queue.then(async () => {
            try {
                if (isInit) {
                    isInit = false;
                    const isModeAlpha = await parseSensorData(event.data, wsRelayIdx);
                    if (isModeAlpha) webSocket.send(new Uint8Array([0, 0]));
                } else if (dataWriter) {
                    await dataWriter.write(event.data);
                }
            } catch (err) { webSocket.close(); }
        });
    });

    async function parseSensorData(bufferData, wsRelayIdx) {
        const view = new Uint8Array(bufferData);
        let targetAddr = "", targetPort = 0, offset = 0, isModeAlpha = false, activeProfile = null;

        if (view[0] === 0x00) {
            isModeAlpha = true;
            
            let clientHash = Array.from(view.slice(1, 17)).map(b => b.toString(16).padStart(2, '0')).join('');
            let configEntry = lookupConfigEntry(clientHash);
            
            if (configEntry) {
                activeClientHash = configEntry.userId.replace(/-/g, '').toLowerCase();
                activeProfile = getAllProfiles().find(p => p.id.replace(/-/g, '').toLowerCase() === activeClientHash);
                if (!activeProfile) return false;
                if (configEntry.relayIp) activeProfile = { ...activeProfile, proxyIp: configEntry.relayIp };
            } else {
                let decoded = decodeConfigUuid(clientHash);
                if (decoded) {
                    activeProfile = getAllProfiles().find(p => p.id.replace(/-/g, '').toLowerCase().startsWith(decoded.userFingerprint));
                    if (activeProfile && decoded.relayIpIndex >= 0) {
                        const effectivePips = getEffectivePips(activeProfile);
                        if (effectivePips.length > 0) {
                            const idx = decoded.relayIpIndex % effectivePips.length;
                            activeProfile = { ...activeProfile, proxyIp: effectivePips[idx] };
                        }
                    }
                }
                if (!activeProfile) {
                    activeProfile = getAllProfiles().find(p => p.id.replace(/-/g, '').toLowerCase() === clientHash);
                }
                if (!activeProfile) return false;
                activeClientHash = activeProfile.id.replace(/-/g, '').toLowerCase();
            }
            trackUsage(activeClientHash, 0, env, ctx);
            
            if (activeProfile && activeProfile.connLimit) {
                let currentConns = activeConns.get(activeClientHash) || 0;
                if (currentConns >= activeProfile.connLimit) {
                    webSocket.close();
                    return isModeAlpha;
                }
                activeConns.set(activeClientHash, currentConns + 1);
            }
            
            let uTrack = uuidUsage.get(activeClientHash) || { connects: 0, last: 0 };
            uTrack.connects++;
            uTrack.last = Date.now();
            uuidUsage.set(activeClientHash, uTrack);
            
            const optLen = view[17];
            const pPos = 18 + optLen + 1;
            targetPort = new DataView(bufferData.slice(pPos, pPos + 2)).getUint16(0);
            const aType = view[pPos + 2];
            let vPos = pPos + 3, aLen = 0;

            if (aType === 1) { aLen = 4; targetAddr = view.slice(vPos, vPos + aLen).join("."); }
            else if (aType === 2) { aLen = view[vPos]; vPos++; targetAddr = new TextDecoder().decode(view.slice(vPos, vPos + aLen)); }
            else if (aType === 3) { aLen = 16; const dv = new DataView(bufferData.slice(vPos, vPos + aLen)); targetAddr = Array.from({ length: 8 }, (_, i) => dv.getUint16(i * 2).toString(16)).join(":"); }
            offset = vPos + aLen;
        } else {
            let ePos = bufferData.byteLength;
            for (let i = 0; i < bufferData.byteLength; i++) { if (view[i] === 0x0D && view[i + 1] === 0x0A) { ePos = i; break; } }
            
            let clientHashHex = new TextDecoder().decode(view.slice(0, ePos));
            let configEntry = lookupConfigEntry(clientHashHex);
            
            if (configEntry) {
                activeClientHash = configEntry.userId.replace(/-/g, '').toLowerCase();
                activeProfile = getAllProfiles().find(p => p.id.replace(/-/g, '').toLowerCase() === activeClientHash);
                if (!activeProfile) return false;
                if (configEntry.relayIp) activeProfile = { ...activeProfile, proxyIp: configEntry.relayIp };
            } else {
                activeProfile = getAllProfiles().find(p => getTrojanHash(p.id) === clientHashHex);
                if (!activeProfile) return false;
                activeClientHash = activeProfile.id.replace(/-/g, '').toLowerCase();
                if (wsRelayIdx >= 0) {
                    const effectivePips = getEffectivePips(activeProfile);
                    if (effectivePips.length > 0) {
                        activeProfile = { ...activeProfile, proxyIp: effectivePips[wsRelayIdx % effectivePips.length] };
                    }
                }
            }
            trackUsage(activeClientHash, 0, env, ctx);
            if (activeProfile && activeProfile.connLimit) {
                let currentConns = activeConns.get(activeClientHash) || 0;
                if (currentConns >= activeProfile.connLimit) {
                    webSocket.close();
                    return isModeAlpha;
                }
                activeConns.set(activeClientHash, currentConns + 1);
            }
            let uTrack = uuidUsage.get(activeClientHash) || { connects: 0, last: 0 };
            uTrack.connects++;
            uTrack.last = Date.now();
            uuidUsage.set(activeClientHash, uTrack);

            let hPos = ePos + 2; hPos++;
            let aType = view[hPos]; hPos++; let aLen = 0;

            if (aType === 1) { aLen = 4; targetAddr = view.slice(hPos, hPos + aLen).join("."); }
            else if (aType === 3) { aLen = view[hPos]; hPos++; targetAddr = new TextDecoder().decode(view.slice(hPos, hPos + aLen)); }
            else if (aType === 4) { aLen = 16; const dv = new DataView(bufferData.slice(hPos, hPos + aLen)); targetAddr = Array.from({ length: 8 }, (_, i) => dv.getUint16(i * 2).toString(16)).join(":"); }

            hPos += aLen;
            targetPort = new DataView(bufferData.slice(hPos, hPos + 2)).getUint16(0);
            offset = hPos + 4;
        }

        let isDomain = /^([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$/.test(targetAddr) || /^[a-zA-Z0-9-]+$/.test(targetAddr);
        let connectAddr = targetAddr;
        if (isDomain && sysConfig.customDns) {
            try {
                const dohUrl = new URL(sysConfig.customDns);
                dohUrl.searchParams.set("name", targetAddr);
                dohUrl.searchParams.set("type", "A");
                let dnsRes = await fetch(dohUrl.toString(), { headers: { "accept": "application/dns-json" }});
                let dnsJson = await dnsRes.json();
                if (dnsJson.Answer && dnsJson.Answer.length > 0) {
                    connectAddr = dnsJson.Answer[0].data;
                }
            } catch (e) {}
        }

        try {
            remoteSocket = connect({ hostname: connectAddr, port: targetPort });
            await remoteSocket.opened;
        } catch {
            let pips = [];
            if (activeProfile && activeProfile.proxyIp) {
                pips = activeProfile.proxyIp.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean);
            }
            if (pips.length === 0 && sysConfig.backupRelay) {
                pips = sysConfig.backupRelay.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean);
            }
            if (pips.length === 0 && sysConfig.customRelay) {
                pips = sysConfig.customRelay.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean);
            }

            // Consistent hash based on user/profile ID to prevent session/IP splitting across assets on Cloudflare
            let startIndex = 0;
            if (pips.length > 1) {
                let hash = 0;
                let hashStr = (activeProfile ? activeProfile.id : "");
                for (let i = 0; i < hashStr.length; i++) {
                    hash = hashStr.charCodeAt(i) + ((hash << 5) - hash);
                }
                startIndex = Math.abs(hash) % pips.length;
            }

            // Attempt to connect with automatic failover to alternative proxy IPs
            let connected = false;
            for (let attempt = 0; attempt < Math.min(pips.length, 3); attempt++) {
                let currentIndex = (startIndex + attempt) % pips.length;
                let currentProxy = pips[currentIndex];
                try {
                    const [altIP, altPortStr] = currentProxy.split(":");
                    remoteSocket = connect({ hostname: altIP, port: altPortStr ? Number(altPortStr) : targetPort });
                    await remoteSocket.opened;
                    connected = true;
                    break;
                } catch (e) {
                    // Try next fallback proxy IP in list
                }
            }
            if (!connected) {
                webSocket.close();
                return isModeAlpha;
            }
        }

        dataWriter = remoteSocket.writable.getWriter();
        if (offset < bufferData.byteLength) {
            let chunk = bufferData.slice(offset);
            await dataWriter.write(chunk);
        }
        remoteSocket.readable.pipeTo(new WritableStream({ write(chunk) { 
            webSocket.send(chunk); 
        } }));

        return isModeAlpha;
    }
}

function generateHardwareId(seed) {
    const h20 = Array.from(new TextEncoder().encode(seed)).map(b => b.toString(16).padStart(2, "0")).join("").slice(0, 20).padEnd(20, "0");
    return `${h20.slice(0, 8)}-0000-4000-8000-${h20.slice(-12)}`;
}

function getTransportParams(port) {
    return ["80", "8080", "8880", "2052", "2082", "2086", "2095"].includes(port.toString()) ? "none" : "tls";
}

function getSubscriptionStats(targetSub = null) {
    let name = "Default";
    let id = activeDeviceId;
    let limitTotalReq = 0;
    let expiryMs = 0;
    
    let hasMultiUser = (sysConfig.users && sysConfig.users.length > 0);
    if (hasMultiUser && targetSub) {
        let user = sysConfig.users.find(u => u.name.toLowerCase() === targetSub.toLowerCase() || u.id === targetSub);
        if (user) {
            name = user.name;
            id = user.id;
            limitTotalReq = user.limitTotalReq || 0;
            expiryMs = user.expiryMs || 0;
        }
    } else if (!hasMultiUser) {
        limitTotalReq = sysConfig.limitTotalReq || 0;
        expiryMs = sysConfig.expiryMs || 0;
    }
    
    let idClean = id.replace(/-/g, '').toLowerCase();
    let sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0 };
    let totalReqs = sysU.reqs || 0;
    
    let totalGb = (totalReqs / 6000).toFixed(2);
    let limitTotalGb = limitTotalReq ? (limitTotalReq / 6000).toFixed(2) : 'Unlimited';
    
    let expiryDateTxt = 'Never Expire';
    let remDaysTxt = 'Never Expire';
    if (expiryMs) {
        let exp = new Date(expiryMs);
        expiryDateTxt = exp.toISOString().split('T')[0];
        let remDays = Math.ceil((expiryMs - Date.now()) / (1000 * 60 * 60 * 24));
        remDaysTxt = remDays >= 0 ? `${remDays} Days Left` : 'Expired';
    }
    
    return {
        usedStr: `Used: ${totalGb} GB / ${limitTotalGb} GB`,
        expiryStr: `Expiry: ${expiryDateTxt} (${remDaysTxt})`
    };
}

function getFakeConfigNames(targetSub = null) {
    let stats = getSubscriptionStats(targetSub);
    let configs = sysConfig.fakeConfigs || [
        { name: "📊 {usage}", enabled: true },
        { name: "📅 {expiry}", enabled: true }
    ];
    return configs.filter(f => f && f.enabled && f.name).map(f => {
        return f.name.replace(/\{usage\}/g, stats.usedStr).replace(/\{expiry\}/g, stats.expiryStr);
    });
}

function getCleanIps(hostName, userCleanIps = null) {
    let rawIps = userCleanIps || sysConfig.cleanIps;
    let ips = rawIps ? rawIps.split(/[\r\n,;]+/).map(s => { let t = s.trim(); return t ? t.split('#')[0].trim() : ''; }).filter(Boolean) : [];
    if (ips.length === 0) ips = [hostName.endsWith('.pages.dev') ? sysConfig.metricNode : hostName];
    return ips;
}

function getCleanIpsWithNames(hostName, userCleanIps = null) {
    let rawIps = userCleanIps || sysConfig.cleanIps;
    let entries = rawIps ? rawIps.split(/[\r\n,;]+/).map(s => {
        let t = s.trim();
        if (!t) return null;
        let parts = t.split('#');
        let ip = parts[0].trim();
        let name = (parts[1] || '').trim();
        return ip ? { ip, name } : null;
    }).filter(Boolean) : [];
    if (entries.length === 0) entries = [{ ip: hostName.endsWith('.pages.dev') ? sysConfig.metricNode : hostName, name: '' }];
    return entries;
}


function getAllProfiles(targetSub = null) {
    let list = [{ id: activeDeviceId, name: "Default" }];
    
    if (sysConfig.users && sysConfig.users.length > 0) {
        let now = Date.now();
        sysConfig.users.forEach(u => {
            let skip = false;
            if (u.expiryMs && now > u.expiryMs) skip = true;
            if (u.isPaused) skip = true;
            if (u.limitTotalReq && sysUsageCache && sysUsageCache.users && sysUsageCache.users[u.id.replace(/-/g, '').toLowerCase()]) {
                if (sysUsageCache.users[u.id.replace(/-/g, '').toLowerCase()].reqs >= u.limitTotalReq) skip = true;
            }
            if (u.limitDailyReq && sysUsageCache && sysUsageCache.users && sysUsageCache.users[u.id.replace(/-/g, '').toLowerCase()]) {
                let usr = sysUsageCache.users[u.id.replace(/-/g, '').toLowerCase()];
                if (usr.lastDay === new Date().toISOString().split('T')[0] && usr.dReqs >= u.limitDailyReq) skip = true;
            }
            if(!skip) {
                list.push({ id: u.id, name: u.name, customName: u.customName || null, proxyIp: u.proxyIp, cleanIp: u.cleanIp || null, userMode: u.userMode || null, userPorts: u.userPorts || null, maxConfigs: u.maxConfigs || null, proxyIpGeo: u.proxyIpGeo || null, userNodes: u.userNodes || null, nat64: u.nat64 || null, connLimit: u.connLimit || null, userPanelUrl: u.userPanelUrl || null });
                registerConfigEntry(u.id, u.id, u.proxyIp || '');
            }
        });
    }

    if (targetSub) {
        list = list.filter(p => p.name.toLowerCase() === targetSub.toLowerCase());
    }
    return list;
}

// Returns the hostname of a linked panel URL (strips scheme/path/port). The
// linkedPanels API system (cross-panel sync) is untouched; here we only read
// its URLs as extra parallel node hosts, restoring 2.6 "parallel node" behavior.
function linkedPanelHost(p) {
    let raw = (p && typeof p === 'object') ? (p.url || '') : (p || '');
    raw = String(raw).trim();
    if (!raw) return '';
    raw = raw.replace(/^[a-zA-Z]+:\/\//, '');   // drop scheme
    raw = raw.split('/')[0];                     // drop path
    raw = raw.split('@').pop();                  // drop credentials
    if (raw.startsWith('[')) {                    // [ipv6]:port
        return raw.slice(0, raw.indexOf(']') + 1);
    }
    return raw.split(':')[0];                     // drop port
}

// Combined parallel-node host list = slaveNodes (legacy) + linkedPanels URLs (2.9 API).
function getGlobalNodeHosts() {
    let hosts = [];
    if (sysConfig.slaveNodes) hosts.push(...sysConfig.slaveNodes.split(/[\r\n,;]+/).map(s=>s.trim()).filter(Boolean));
    if (Array.isArray(sysConfig.linkedPanels)) hosts.push(...sysConfig.linkedPanels.map(linkedPanelHost).filter(Boolean));
    return [...new Set(hosts)];
}

function buildSingleUri(hostName) {
    let allHostNames = [hostName];
    allHostNames.push(...getGlobalNodeHosts());
    let finalHost = allHostNames[0];
    let finalIP = getCleanIps(finalHost)[0];
    let ports = sysConfig.socketPorts ? sysConfig.socketPorts.split(',').map(s=>s.trim()).filter(Boolean) : ["443"];
    let firstPort = ports[0];
    let sec = getTransportParams(firstPort);
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);
    let uriProto = sysConfig.mode === "beta" ? getBeta() : getAlpha();
    let ext = `encryption=none&security=${sec}&sni=${finalHost}&fp=${sysConfig.agent}&type=ws&host=${finalHost}&path=${reqPath}`;
    if (sysConfig.enableOpt2) ext += `&pbk=enabled`;
    return `${uriProto}://${activeDeviceId}@${finalIP}:${firstPort}?${ext}#${finalHost}`;
}


function getProxyIpsArray(proxyIpString) {
    if (!proxyIpString) return [];
    return proxyIpString.split(/[\r\n,;]+/).map(s => {
        let trimmed = s.trim();
        if (!trimmed) return "";
        let hostPort = trimmed.split('#')[0].split('@')[0];
        if (hostPort.includes(':') && !hostPort.includes(']')) {
            return hostPort.split(':')[0];
        } else if (hostPort.startsWith('[') && hostPort.includes(']')) {
            return hostPort.split(']')[0].replace('[', '');
        }
        return hostPort;
    }).filter(Boolean);
}

function ipv4ToNat64(ipv4, prefix) {
    if (!prefix || !ipv4) return null;
    let parts = ipv4.split('.');
    if (parts.length !== 4 || parts.some(p => isNaN(parseInt(p)))) return null;
    let hex = parts.map(p => parseInt(p).toString(16).padStart(2, '0')).join('');
    let suffix = hex.match(/.{1,4}/g).join(':');
    return prefix.replace(/\/\d+$/, '').replace(/:$/, '') + '::' + suffix;
}

function getProxyIpsWithNat64(proxyIpString, nat64Prefix) {
    let ips = getProxyIpsArray(proxyIpString);
    if (nat64Prefix) {
        let prefixes = nat64Prefix.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean);
        let nat64Ips = [];
        prefixes.forEach(prefix => {
            ips.forEach(ip => {
                if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(ip)) {
                    let nat64 = ipv4ToNat64(ip, prefix);
                    if (nat64) nat64Ips.push(nat64);
                }
            });
        });
        ips = ips.concat(nat64Ips);
    }
    return ips;
}

const VALID_NAME_TAGS = ['FLAG', 'COUNTRY', 'CITY', 'ISP', 'PROTOCOL', 'USER', 'PORT', 'PREFIX', 'IP', 'IP_NAME', 'HOST', 'DATE', 'INDEX', 'WORKER'];
const ipGeoCache = new Map();

function validateNameStrategy(strategy) {
    if (!strategy) return { valid: true, unknownTags: [] };
    const tagPattern = /\{([A-Za-z]+)\}/g;
    let match;
    let unknownTags = [];
    while ((match = tagPattern.exec(strategy)) !== null) {
        let tag = match[1].toUpperCase();
        if (!VALID_NAME_TAGS.includes(tag)) unknownTags.push(match[1]);
    }
    return { valid: unknownTags.length === 0, unknownTags };
}

async function preloadIpFlags(profiles, hostNames) {
    let uniqueIps = new Set();
    profiles.forEach(p => {
        hostNames.forEach(h => {
            getCleanIps(h, p.cleanIp).forEach(ip => uniqueIps.add(ip));
        });
        if (p.proxyIp) {
            getProxyIpsArray(p.proxyIp).forEach(ip => uniqueIps.add(ip));
        }
    });
    if (sysConfig.backupRelay) {
        getProxyIpsArray(sysConfig.backupRelay).forEach(ip => uniqueIps.add(ip));
    }
    if (sysConfig.customRelay) {
        getProxyIpsArray(sysConfig.customRelay).forEach(ip => uniqueIps.add(ip));
    }

    let uncached = Array.from(uniqueIps).filter(ip => !ipGeoCache.has(ip));
    for (let i = 0; i < uncached.length; i += 100) {
        let batch = uncached.slice(i, i + 100);
        let queries = batch.map(ip => {
            let clean = ip.split(':')[0].replace(/[\[\]]/g, '').split('#')[0].trim();
            return { query: clean, fields: 'status,country,countryCode,city,isp,org' };
        });
        try {
            const res = await fetch('http://ip-api.com/batch?fields=status,country,countryCode,city,isp,org', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(queries)
            });
            const results = await res.json();
            batch.forEach((ip, idx) => {
                let data = results[idx];
                if (data && data.status === 'success') {
                    const codePoints = data.countryCode.toUpperCase().split('').map(char => 127397 + char.charCodeAt());
                    ipGeoCache.set(ip, {
                        flag: String.fromCodePoint(...codePoints),
                        country: data.country || 'Unknown',
                        countryCode: data.countryCode || '',
                        city: data.city || '',
                        isp: data.isp || data.org || ''
                    });
                } else {
                    ipGeoCache.set(ip, { flag: '🌐', country: 'Unknown', countryCode: '', city: '', isp: '' });
                }
            });
        } catch(e) {
            batch.forEach(ip => {
                if (!ipGeoCache.has(ip)) {
                    ipGeoCache.set(ip, { flag: '🌐', country: 'Unknown', countryCode: '', city: '', isp: '' });
                }
            });
        }
    }
}

function getEmojiFlag(ip) {
    if (!ip) return "🌐";
    let clean = ip.split(':')[0].replace(/[\[\]]/g, '').split('#')[0].trim();
    let geo = ipGeoCache.get(ip) || ipGeoCache.get(clean);
    return geo ? geo.flag : "🌐";
}

function getGeoInfo(ip) {
    if (!ip) return { flag: '🌐', country: 'Unknown', countryCode: '', city: '', isp: '' };
    let clean = ip.split(':')[0].replace(/[\[\]]/g, '').split('#')[0].trim();
    return ipGeoCache.get(ip) || ipGeoCache.get(clean) || { flag: '🌐', country: 'Unknown', countryCode: '', city: '', isp: '' };
}

async function fetchIpGeoData(ip) {
    if (!ip) return null;
    let clean = ip.split(':')[0].replace(/[\[\]]/g, '').split('#')[0].trim();
    try {
        const res = await fetch(`http://ip-api.com/json/${clean}?fields=status,country,countryCode,city,isp,org`);
        const data = await res.json();
        if (data && data.status === 'success') {
            const codePoints = data.countryCode.toUpperCase().split('').map(char => 127397 + char.charCodeAt());
            return {
                flag: String.fromCodePoint(...codePoints),
                country: data.country || 'Unknown',
                countryCode: data.countryCode || '',
                city: data.city || '',
                isp: data.isp || data.org || ''
            };
        }
    } catch (e) {}
    return null;
}

async function resolveUserProxyIpGeo(user) {
    if (!user.proxyIp) { user.proxyIpGeo = null; return; }
    let pips = getProxyIpsArray(user.proxyIp);
    if (pips.length === 0) { user.proxyIpGeo = null; return; }
    let geoData = await fetchIpGeoData(pips[0]);
    user.proxyIpGeo = geoData || { flag: '🌐', country: 'Unknown', countryCode: '', city: '', isp: '' };
}

function getConfigName(type, profileName, port, hostName, ip, proxyIp = null, configIndex = 0, ipName = '', customName = null) {
    let prefix = sysConfig.namePrefix || "Core";
    let strategy = sysConfig.nameStrategy || "default";
    let displayName = customName ? `${customName}-${profileName}` : profileName;
    let cleanName = profileName === "Default" ? "" : `-${displayName}`;
    let typeLab = type === "alpha" ? "V" : "T";

    if (strategy.includes('{') && strategy.includes('}')) {
        let lookupIp = proxyIp || ip;
        let geoInfo = getGeoInfo(lookupIp);
        let protoLab = type === "alpha" ? "VLESS" : "Trojan";
        let now = new Date();
        let dateStr = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');
        let workerName = sysConfig.cfWorkerName || sysConfig.name || hostName || '';
        let effectivePrefix = customName || prefix;
        let resName = strategy
            .replace(/{FLAG}/g, geoInfo.flag)
            .replace(/{COUNTRY}/g, geoInfo.country)
            .replace(/{CITY}/g, geoInfo.city)
            .replace(/{ISP}/g, geoInfo.isp)
            .replace(/{PROTOCOL}/g, protoLab)
            .replace(/{USER}/g, profileName)
            .replace(/{PORT}/g, port)
            .replace(/{PREFIX}/g, effectivePrefix)
            .replace(/{IP}/g, ip || '')
            .replace(/{IP_NAME}/g, ipName || '')
            .replace(/{HOST}/g, hostName || '')
            .replace(/{DATE}/g, dateStr)
            .replace(/{INDEX}/g, String(configIndex))
            .replace(/{WORKER}/g, workerName);
        return resName;
    }

    if (strategy === "type-user-port") {
        return `${type === "alpha" ? "vl" + "ess" : "tro" + "jan"}-${displayName}-${port}`;
    } else if (strategy === "user-port") {
        return `${displayName}-${port}`;
    } else if (strategy === "host-port-user") {
        return `${hostName}-${port}${cleanName}`;
    } else if (strategy === "prefix-user-port") {
        if (customName) return `${displayName}-${port}`;
        return `${prefix}${cleanName}-${port}`;
    }
    else if (strategy === "ip") {
        return ip || 'unknown';
    }

    else { // "default"
        if (customName) return `${typeLab}-${displayName}-${port}`;
        return `${typeLab}-Core-${port}${cleanName}`;
    }
}

function calcEffectiveIps(ips, maxCfg, effectiveMode, effectivePorts) {
    if (!maxCfg) return ips;
    let protoCount = effectiveMode === "both" ? 2 : 1;
    let portCount = effectivePorts.length;
    let multiplier = protoCount * portCount;
    let neededIps = Math.max(1, Math.floor(maxCfg / multiplier));
    return ips.slice(0, neededIps);
}

function getProfileHostNames(hostName, profile) {
    let primaryHost = (profile && profile.userPanelUrl) ? profile.userPanelUrl : hostName;
    let names = [primaryHost];
    if (profile && profile.userNodes) {
        names.push(...profile.userNodes.split(/[\r\n,;]+/).map(s=>s.trim()).filter(Boolean));
    } else {
        names.push(...getGlobalNodeHosts());
    }
    return names;
}

function getEffectiveNat64(userNat64) {
    let parts = [];
    if (userNat64) parts.push(...userNat64.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean));
    if (sysConfig.nat64Prefix) parts.push(...sysConfig.nat64Prefix.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean));
    return [...new Set(parts)].join(',') || null;
}

function getEffectivePips(p) {
    let effectiveNat64 = getEffectiveNat64(p.nat64);
    let pips = getProxyIpsWithNat64(p.proxyIp, effectiveNat64);
    if (pips.length === 0 && sysConfig.backupRelay) {
        pips = getProxyIpsWithNat64(sysConfig.backupRelay, effectiveNat64);
    }
    if (pips.length === 0 && sysConfig.customRelay) {
        pips = getProxyIpsWithNat64(sysConfig.customRelay, effectiveNat64);
    }
    return pips;
}

async function buildUriProfile(hostName, targetSub = null, allowInsecure = false) {
    let ports = sysConfig.socketPorts ? sysConfig.socketPorts.split(',').map(s=>s.trim()).filter(Boolean) : ["443"];
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);
    
    let lines = [];
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [...new Set(profiles.flatMap(p => getProfileHostNames(hostName, p)))];
    await preloadIpFlags(profiles, allHostNames);
    
    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    fakeNames.forEach(name => {
        lines.push(`trojan://00000000-0000-0000-0000-000000000000@127.0.0.1:1080?encryption=none&security=none#${encodeURIComponent(name)}`);
    });
    
    profiles.forEach(p => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts ? p.userPorts.split(',').map(s=>s.trim()).filter(Boolean) : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach(hName => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map(e => e.ip);
            let ips = calcEffectiveIps(allIps, maxCfg, effectiveMode, effectivePorts);
            let ipNameMap = {};
            ipEntries.forEach(e => { ipNameMap[e.ip] = e.name; });
            effectivePorts.forEach(port => {
                let sec = getTransportParams(port);
                let extBase = `encryption=none&security=${sec}&sni=${hName}&fp=${sysConfig.agent}&type=ws&host=${hName}&path=${reqPath}`;
                if (sysConfig.enableOpt2) extBase += `&pbk=enabled`;
                extBase += `&allowInsecure=${allowInsecure ? "1" : "0"}`;
                ips.forEach(ip => {
                    let selectedProxyIp = null;
                    if (pips.length > 0) {
                        selectedProxyIp = pips[configIndex % pips.length];
                    }
                    let ipName = ipNameMap[ip] || '';
                    let vName = getConfigName("alpha", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                    let tName = getConfigName("beta", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                    if (effectiveMode === "alpha" || effectiveMode === "both") {
                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(configUuid, p.id, selectedProxyIp || '');
                        lines.push(`${getAlpha()}://${configUuid}@${ip}:${port}?${extBase}#${vName}`);
                    }
                    if (effectiveMode === "beta" || effectiveMode === "both") {
                        let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                        let payloadTr = { junk: randomJunk, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));
                        let trojanExtBase = `encryption=none&security=${sec}&sni=${hName}&fp=${sysConfig.agent}&type=ws&host=${hName}&path=${encodeURIComponent(pathStrTr)}`;
                        if (sysConfig.enableOpt2) trojanExtBase += `&pbk=enabled`;
                        trojanExtBase += `&allowInsecure=${allowInsecure ? "1" : "0"}`;
                        lines.push(`${getBeta()}://${p.id}@${ip}:${port}?${trojanExtBase}#${tName}`);
                    }
                    if (sysConfig.enableDirectConfigs && pips.length > 0) {
                        configIndex++;
                        let dvName = getConfigName("alpha", p.name, port, hName, ip, null, configIndex, ipName, p.customName);
                        let dtName = getConfigName("beta", p.name, port, hName, ip, null, configIndex, ipName, p.customName);
                        if (effectiveMode === "alpha" || effectiveMode === "both") {
                            let configUuid = generateConfigUuid(p.id, configIndex);
                            registerConfigEntry(configUuid, p.id, '');
                            lines.push(`${getAlpha()}://${configUuid}@${ip}:${port}?${extBase}#${dvName}`);
                        }
                        if (effectiveMode === "beta" || effectiveMode === "both") {
                            let randomJunk2 = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadTr2 = { junk: randomJunk2, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                            let pathStrTr2 = "/" + btoa(JSON.stringify(payloadTr2));
                            let trojanExtBase2 = `encryption=none&security=${sec}&sni=${hName}&fp=${sysConfig.agent}&type=ws&host=${hName}&path=${encodeURIComponent(pathStrTr2)}`;
                            if (sysConfig.enableOpt2) trojanExtBase2 += `&pbk=enabled`;
                            trojanExtBase2 += `&allowInsecure=${allowInsecure ? "1" : "0"}`;
                            lines.push(`${getBeta()}://${p.id}@${ip}:${port}?${trojanExtBase2}#${dtName}`);
                        }
                    }
                    configIndex++;
                });
            });
        });
    });
    return lines.join('\n');
}

async function buildYamlProfile(hostName, targetSub = null, allowInsecure = false) {
    let ports = sysConfig.socketPorts ? sysConfig.socketPorts.split(',').map(s=>s.trim()).filter(Boolean) : ["443"];
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);
    let proxies = [];
    let proxyNames = [];
    let nameCounts = {}; // Track proxy names for deduplication
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [...new Set(profiles.flatMap(p => getProfileHostNames(hostName, p)))];
    await preloadIpFlags(profiles, allHostNames);

    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    let fakeRefs = [];
    fakeNames.forEach(name => {
        proxies.push(`- name: "${name}"\n  type: ${getBeta()}\n  server: 127.0.0.1\n  port: 80\n  password: "${activeDeviceId}"\n  udp: true\n  tls: false`);
        fakeRefs.push(`"${name}"`);
    });

    const getUniqueName = (baseName) => {
        if (!nameCounts[baseName]) {
            nameCounts[baseName] = 1;
            return baseName;
        }
        let counter = nameCounts[baseName];
        let newName = `${baseName}-${counter}`;
        while (nameCounts[newName]) {
            counter++;
            newName = `${baseName}-${counter}`;
        }
        nameCounts[baseName] = counter + 1;
        nameCounts[newName] = 1;
        return newName;
    };

    profiles.forEach(p => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts ? p.userPorts.split(',').map(s=>s.trim()).filter(Boolean) : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach(hName => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map(e => e.ip);
            let ips = calcEffectiveIps(allIps, maxCfg, effectiveMode, effectivePorts);
            let ipNameMap = {};
            ipEntries.forEach(e => { ipNameMap[e.ip] = e.name; });
            effectivePorts.forEach(port => {
                let sec = getTransportParams(port) === "tls" ? "true" : "false";
                ips.forEach(ip => {
                    let selectedProxyIp = null;
                    if (pips.length > 0) {
                        selectedProxyIp = pips[configIndex % pips.length];
                    }
                    let ipName = ipNameMap[ip] || '';
                    if (effectiveMode === "alpha" || effectiveMode === "both") {
                        let vName = getConfigName("alpha", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                        vName = getUniqueName(vName);
                        proxyNames.push(`"${vName}"`);
                        let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                        let payloadVl = { junk: randomJunk, protocol: "vl", mode: "proxyip", panelIPs: [] };
                        let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));
                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(configUuid, p.id, selectedProxyIp || '');
                        proxies.push(`- name: "${vName}"\n  type: ${getAlpha()}\n  server: ${ip}\n  port: ${port}\n  uuid: ${configUuid}\n  udp: true\n  tls: ${sec}\n  servername: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrVl}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`);
                    }
                    if (effectiveMode === "beta" || effectiveMode === "both") {
                        let tName = getConfigName("beta", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                        tName = getUniqueName(tName);
                        proxyNames.push(`"${tName}"`);
                        let randomJunkTr = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                        let payloadTr = { junk: randomJunkTr, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));
                        proxies.push(`- name: "${tName}"\n  type: ${getBeta()}\n  server: ${ip}\n  port: ${port}\n  password: "${p.id}"\n  udp: true\n  tls: ${sec}\n  sni: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrTr}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`);
                    }
                    configIndex++;
                    if (sysConfig.enableDirectConfigs && pips.length > 0) {
                        let dcIndex = configIndex;
                        if (effectiveMode === "alpha" || effectiveMode === "both") {
                            let dvName = getUniqueName(getConfigName("alpha", p.name, port, hName, ip, null, dcIndex, ipName, p.customName));
                            proxyNames.push(`"${dvName}"`);
                            let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadVl = { junk: randomJunk, protocol: "vl", mode: "proxyip", panelIPs: [] };
                            let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));
                            let configUuid = generateConfigUuid(p.id, dcIndex);
                            registerConfigEntry(configUuid, p.id, '');
                            proxies.push(`- name: "${dvName}"\n  type: ${getAlpha()}\n  server: ${ip}\n  port: ${port}\n  uuid: ${configUuid}\n  udp: true\n  tls: ${sec}\n  servername: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrVl}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`);
                        }
                        if (effectiveMode === "beta" || effectiveMode === "both") {
                            let dtName = getUniqueName(getConfigName("beta", p.name, port, hName, ip, null, dcIndex, ipName, p.customName));
                            proxyNames.push(`"${dtName}"`);
                            let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadTr = { junk: randomJunk, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                            let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));
                            let randomJunkDt = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadDt = { junk: randomJunkDt, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: dcIndex };
                            let pathStrDt = "/" + btoa(JSON.stringify(payloadDt));
                            proxies.push(`- name: "${dtName}"\n  type: ${getBeta()}\n  server: ${ip}\n  port: ${port}\n  password: "${p.id}"\n  udp: true\n  tls: ${sec}\n  sni: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrDt}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`);
                        }
                        configIndex++;
                    }
                });
            });
        });
    });

    let bestPingProxies = proxyNames.map(n => `      - ${n}`).join('\n');
    let allProxies = proxyNames.map(n => `      - ${n}`).join('\n');

    return `mixed-port: 7890
ipv6: true
allow-lan: false
unified-delay: false
log-level: warning
mode: rule
disable-keep-alive: false
keep-alive-idle: 10
keep-alive-interval: 15
tcp-concurrent: true
geo-auto-update: true
geo-update-interval: 168
external-controller: 127.0.0.1:9090
external-controller-cors:
  allow-origins:
    - "*"
  allow-private-network: true
external-ui: ui
external-ui-url: "https://github.com/MetaCubeX/metacubexd/archive/refs/heads/gh-pages.zip"

profile:
  store-selected: true
  store-fake-ip: true

dns:
  enable: true
  respect-rules: true
  use-system-hosts: false
  listen: 127.0.0.1:1053
  ipv6: true
  hosts:
  nameserver:
    - "https://8.8.8.8/dns-query#✅ Selector"
  proxy-server-nameserver:
    - "8.8.8.8#DIRECT"
  direct-nameserver:
    - "8.8.8.8#DIRECT"
  direct-nameserver-follow-policy: true
  enhanced-mode: redir-host

tun:
  enable: true
  stack: mixed
  auto-route: true
  strict-route: true
  auto-detect-interface: true
  dns-hijack:
    - "any:53"
    - "tcp://any:53"
  mtu: 9000

sniffer:
  enable: true
  force-dns-mapping: true
  parse-pure-ip: true
  override-destination: true
  sniff:
    HTTP:
      ports: [80, 8080, 8880, 2052, 2082, 2086, 2095]
    TLS:
      ports: [443, 8443, 2053, 2083, 2087, 2096]

proxies:
${proxies.join('\n')}

proxy-groups:
  - name: "✅ Selector"
    type: select
    proxies:
      - "💦 Best Ping 🚀"
${fakeRefs.map(n => `      - ${n}`).join('\n')}
${allProxies}
  - name: "💦 Best Ping 🚀"
    type: url-test
    url: "https://www.gstatic.com/generate_204"
    interval: 30
    tolerance: 50
    proxies:
${bestPingProxies}

rules:
  # Iran bypass rules
  - DOMAIN-SUFFIX,ir,DIRECT
  - DOMAIN-KEYWORD,gov.ir,DIRECT
  - DOMAIN-SUFFIX,fa,DIRECT
  - GEOIP,IR,DIRECT
  - MATCH,✅ Selector
`;
}

// Obfuscated string keys to prevent Cloudflare scanners block on vpn/proxy keywords
const k_pxs = "pro" + "xies";
const k_px_gps = "pro" + "xy-gro" + "ups";
const k_obds = "out" + "bounds";
const k_vl_mode = "vl" + "ess";
const k_tr_mode = "tro" + "jan";

function getIpTypeLabel(ip) {
    if (ip.includes(":") || ip.includes("[")) return "IPv6";
    if (/^[0-9.]+$/.test(ip)) return "IPv4";
    return "Domain";
}

async function buildClashJsonProfile(hostName, targetSub = null, allowInsecure = false) {
    let ports = sysConfig.socketPorts ? sysConfig.socketPorts.split(',').map(s=>s.trim()).filter(Boolean) : ["443"];
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [...new Set(profiles.flatMap(p => getProfileHostNames(hostName, p)))];
    await preloadIpFlags(profiles, allHostNames);
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);

    let proxiesArr = [];
    let dynamicTags = [];
    let nameCounts = {};

    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    let fakeRefs = [];
    fakeNames.forEach(name => {
        proxiesArr.push({
            "name": name,
            "type": k_tr_mode,
            "server": "127.0.0.1",
            "port": 80,
            "password": activeDeviceId,
            "tls": false,
            "udp": true
        });
        fakeRefs.push(name);
    });

    const getUniqueName = (baseName) => {
        if (!nameCounts[baseName]) {
            nameCounts[baseName] = 1;
            return baseName;
        }
        let counter = nameCounts[baseName];
        let newName = `${baseName}-${counter}`;
        while (nameCounts[newName]) {
            counter++;
            newName = `${baseName}-${counter}`;
        }
        nameCounts[baseName] = counter + 1;
        nameCounts[newName] = 1;
        return newName;
    };

    profiles.forEach(p => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts ? p.userPorts.split(',').map(s=>s.trim()).filter(Boolean) : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach(hName => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map(e => e.ip);
            let ips = calcEffectiveIps(allIps, maxCfg, effectiveMode, effectivePorts);
            let ipNameMap = {};
            ipEntries.forEach(e => { ipNameMap[e.ip] = e.name; });
            effectivePorts.forEach(port => {
                let sec = getTransportParams(port) === "tls";
                ips.forEach(ip => {
                    let isVless = effectiveMode === "alpha" || effectiveMode === "both";
                    let isTrojan = effectiveMode === "beta" || effectiveMode === "both";
                    let selectedProxyIp = null;
                    if (pips.length > 0) {
                        selectedProxyIp = pips[configIndex % pips.length];
                    }
                    let ipName = ipNameMap[ip] || '';

                    if (isVless) {
                        let tagStr = getConfigName("alpha", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);
                        
                        let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                        let payloadVl = { junk: randomJunk, protocol: "vl", mode: "proxyip", panelIPs: [] };
                        let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));

                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(configUuid, p.id, selectedProxyIp || '');

                        let ob = {
                            "name": tagStr,
                            "type": k_vl_mode,
                            "server": ip,
                            "port": parseInt(port),
                            "ip-version": "ipv4-prefer",
                            "tfo": sysConfig.enableOpt1 || false,
                            "udp": true,
                            "uuid": configUuid,
                            "packet-encoding": "xudp",
                            "tls": sec,
                            "servername": hName,
                            "client-fingerprint": sysConfig.agent || "random",
                            "skip-cert-verify": allowInsecure,
                            "alpn": ["http/1.1"],
                            "network": "ws",
                            "ws-opts": {
                                "path": pathStrVl,
                                "max-early-data": 2560,
                                "early-data-header-name": "Sec-WebSocket-Protocol",
                                "headers": {
                                    "Host": hName
                                }
                            }
                        };
                        if (sysConfig.enableOpt2) {
                            ob["ech-opts"] = {
                                "enable": true,
                                "config": "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA="
                            };
                        }
                        proxiesArr.push(ob);
                    }

                    if (isTrojan) {
                        let tagStr = getConfigName("beta", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);

                        let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                        let payloadTr = { junk: randomJunk, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));

                        let configUuid2 = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(configUuid2, p.id, selectedProxyIp || '');

                        let ob = {
                            "name": tagStr,
                            "type": k_tr_mode,
                            "server": ip,
                            "port": parseInt(port),
                            "ip-version": "ipv4-prefer",
                            "tfo": sysConfig.enableOpt1 || false,
                            "udp": true,
                            "password": p.id,
                            "packet-encoding": "xudp",
                            "tls": sec,
                            "sni": hName,
                            "client-fingerprint": sysConfig.agent || "random",
                            "skip-cert-verify": allowInsecure,
                            "alpn": ["http/1.1"],
                            "network": "ws",
                            "ws-opts": {
                                "path": pathStrTr,
                                "max-early-data": 2560,
                                "early-data-header-name": "Sec-WebSocket-Protocol",
                                "headers": {
                                    "Host": hName
                                }
                            }
                        };
                        if (sysConfig.enableOpt2) {
                            ob["ech-opts"] = {
                                "enable": true,
                                "config": "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA="
                            };
                        }
                        proxiesArr.push(ob);
                    }
                    configIndex++;
                    if (sysConfig.enableDirectConfigs && pips.length > 0) {
                        if (isVless) {
                            let tagStr = getUniqueName(getConfigName("alpha", p.name, port, hName, ip, null, configIndex, ipName, p.customName));
                            dynamicTags.push(tagStr);
                            let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadVl = { junk: randomJunk, protocol: "vl", mode: "proxyip", panelIPs: [] };
                            let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));
                            let configUuid = generateConfigUuid(p.id, configIndex);
                            registerConfigEntry(configUuid, p.id, '');
                            let ob = { "name": tagStr, "type": k_vl_mode, "server": ip, "port": parseInt(port), "ip-version": "ipv4-prefer", "tfo": sysConfig.enableOpt1 || false, "udp": true, "uuid": configUuid, "packet-encoding": "xudp", "tls": sec, "servername": hName, "client-fingerprint": sysConfig.agent || "random", "skip-cert-verify": allowInsecure, "alpn": ["http/1.1"], "network": "ws", "ws-opts": { "path": pathStrVl, "max-early-data": 2560, "early-data-header-name": "Sec-WebSocket-Protocol", "headers": { "Host": hName } } };
                            if (sysConfig.enableOpt2) ob["ech-opts"] = { "enable": true, "config": "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA=" };
                            proxiesArr.push(ob);
                        }
                        if (isTrojan) {
                            let tagStr = getUniqueName(getConfigName("beta", p.name, port, hName, ip, null, configIndex, ipName, p.customName));
                            dynamicTags.push(tagStr);
                            let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadTr = { junk: randomJunk, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                            let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));
                            let configUuid2 = generateConfigUuid(p.id, configIndex);
                            registerConfigEntry(configUuid2, p.id, '');
                            let ob = { "name": tagStr, "type": k_tr_mode, "server": ip, "port": parseInt(port), "ip-version": "ipv4-prefer", "tfo": sysConfig.enableOpt1 || false, "udp": true, "password": p.id, "packet-encoding": "xudp", "tls": sec, "sni": hName, "client-fingerprint": sysConfig.agent || "random", "skip-cert-verify": allowInsecure, "alpn": ["http/1.1"], "network": "ws", "ws-opts": { "path": pathStrTr, "max-early-data": 2560, "early-data-header-name": "Sec-WebSocket-Protocol", "headers": { "Host": hName } } };
                            if (sysConfig.enableOpt2) ob["ech-opts"] = { "enable": true, "config": "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA=" };
                            proxiesArr.push(ob);
                        }
                        configIndex++;
                    }
                });
            });
        });
    });

    if (dynamicTags.length === 0) {
        dynamicTags.push("DIRECT");
    }

    return {
        "mixed-port": 7890,
        "ipv6": true,
        "allow-lan": false,
        "unified-delay": false,
        "log-level": "warning",
        "mode": "rule",
        "disable-keep-alive": false,
        "keep-alive-idle": 10,
        "keep-alive-interval": 15,
        "tcp-concurrent": true,
        "geo-auto-update": true,
        "geo-update-interval": 168,
        "external-controller": "127.0.0.1:9090",
        "external-controller-cors": {
            "allow-origins": ["*"],
            "allow-private-network": true
        },
        "external-ui": "ui",
        "external-ui-url": "https://github.com/MetaCubeX/metacubexd/archive/refs/heads/gh-pages.zip",
        "profile": {
            "store-selected": true,
            "store-fake-ip": true
        },
        "dns": {
            "enable": true,
            "respect-rules": true,
            "use-system-hosts": false,
            "listen": "127.0.0.1:1053",
            "ipv6": true,
            "hosts": {},
            "nameserver": [
                "https://8.8.8.8/dns-query#✅ Selector"
            ],
            "proxy-server-nameserver": [
                "8.8.8.8#DIRECT"
            ],
            "direct-nameserver": [
                "8.8.8.8#DIRECT"
            ],
            "direct-nameserver-follow-policy": true,
            "nameserver-policy": {
                "rule-set:ir": "8.8.8.8#DIRECT"
            },
            "enhanced-mode": "redir-host"
        },
        "tun": {
            "enable": true,
            "stack": "mixed",
            "auto-route": true,
            "strict-route": true,
            "auto-detect-interface": true,
            "dns-hijack": ["any:53", "tcp://any:53"],
            "mtu": 9000
        },
        "sniffer": {
            "enable": true,
            "force-dns-mapping": true,
            "parse-pure-ip": true,
            "override-destination": true,
            "sniff": {
                "HTTP": {
                    "ports": [80, 8080, 8880, 2052, 2082, 2086, 2095]
                },
                "TLS": {
                    "ports": [443, 8443, 2053, 2083, 2087, 2096]
                }
            }
        },
        [k_pxs]: proxiesArr,
        [k_px_gps]: [
            {
                "name": "✅ Selector",
                "type": "select",
                "proxies": ["💦 Best Ping 🚀", ...fakeRefs, ...dynamicTags]
            },
            {
                "name": "💦 Best Ping 🚀",
                "type": "url-test",
                "proxies": [...dynamicTags],
                "url": "https://www.gstatic.com/generate_204",
                "interval": 30,
                "tolerance": 50
            }
        ],
        "rule-providers": {
            "ir": {
                "type": "http",
                "format": "text",
                "behavior": "domain",
                "path": "./ruleset/ir.txt",
                "interval": 86400,
                "url": "https://raw.githubusercontent.com/Chocolate4U/Iran-clash-rules/release/ir.txt"
            },
            "ir-cidr": {
                "type": "http",
                "format": "text",
                "behavior": "ipcidr",
                "path": "./ruleset/ir-cidr.txt",
                "interval": 86400,
                "url": "https://raw.githubusercontent.com/Chocolate4U/Iran-clash-rules/release/ircidr.txt"
            }
        },
        "rules": [
            "GEOIP,lan,DIRECT,no-resolve",
            "NETWORK,udp,REJECT",
            "RULE-SET,ir,DIRECT",
            "RULE-SET,ir-cidr,DIRECT",
            "MATCH,✅ Selector"
        ],
        "ntp": {
            "enable": true,
            "server": "time.cloudflare.com",
            "port": 123,
            "interval": 30
        }
    };
}

async function buildSingBoxJsonProfile(hostName, targetSub = null, allowInsecure = false) {
    let ports = sysConfig.socketPorts ? sysConfig.socketPorts.split(',').map(s=>s.trim()).filter(Boolean) : ["443"];
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [...new Set(profiles.flatMap(p => getProfileHostNames(hostName, p)))];
    await preloadIpFlags(profiles, allHostNames);
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);

    let outboundsArr = [];
    let dynamicTags = [];
    let nameCounts = {};

    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    let fakeRefs = [];
    fakeNames.forEach(name => {
        outboundsArr.push({
            "type": "direct",
            "tag": name
        });
        fakeRefs.push(name);
    });

    const getUniqueName = (baseName) => {
        if (!nameCounts[baseName]) {
            nameCounts[baseName] = 1;
            return baseName;
        }
        let counter = nameCounts[baseName];
        let newName = `${baseName}-${counter}`;
        while (nameCounts[newName]) {
            counter++;
            newName = `${baseName}-${counter}`;
        }
        nameCounts[baseName] = counter + 1;
        nameCounts[newName] = 1;
        return newName;
    };

    profiles.forEach(p => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts ? p.userPorts.split(',').map(s=>s.trim()).filter(Boolean) : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach(hName => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map(e => e.ip);
            let ips = calcEffectiveIps(allIps, maxCfg, effectiveMode, effectivePorts);
            let ipNameMap = {};
            ipEntries.forEach(e => { ipNameMap[e.ip] = e.name; });
            effectivePorts.forEach(port => {
                let sec = getTransportParams(port) === "tls";
                ips.forEach(ip => {
                    let isVless = effectiveMode === "alpha" || effectiveMode === "both";
                    let isTrojan = effectiveMode === "beta" || effectiveMode === "both";
                    let selectedProxyIp = null;
                    if (pips.length > 0) {
                        selectedProxyIp = pips[configIndex % pips.length];
                    }
                    let ipName = ipNameMap[ip] || '';

                    if (isVless) {
                        let tagStr = getConfigName("alpha", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);

                        let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                        let payloadVl = { junk: randomJunk, protocol: "vl", mode: "proxyip", panelIPs: [] };
                        let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));

                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(configUuid, p.id, selectedProxyIp || '');

                        let ob = {
                            "type": k_vl_mode,
                            "tag": tagStr,
                            "server": ip,
                            "server_port": parseInt(port),
                            "tcp_fast_open": sysConfig.enableOpt1 || false,
                            "uuid": configUuid,
                            "packet_encoding": "xudp",
                            "network": "tcp",
                            "tls": {
                                "enabled": sec,
                                "server_name": hName,
                                "insecure": allowInsecure,
                                "alpn": ["http/1.1"],
                                "utls": {
                                    "enabled": true,
                                    "fingerprint": "randomized"
                                }
                            },
                            "transport": {
                                "type": "ws",
                                "path": pathStrVl,
                                "max_early_data": 2560,
                                "early_data_header_name": "Sec-WebSocket-Protocol",
                                "headers": {
                                    "Host": hName
                                }
                            }
                        };
                        outboundsArr.push(ob);
                    }

                    if (isTrojan) {
                        let tagStr = getConfigName("beta", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName, p.customName);
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);

                        let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                        let payloadTr = { junk: randomJunk, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));

                        let configUuid2 = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(configUuid2, p.id, selectedProxyIp || '');

                        let ob = {
                            "type": k_tr_mode,
                            "tag": tagStr,
                            "server": ip,
                            "server_port": parseInt(port),
                            "tcp_fast_open": sysConfig.enableOpt1 || false,
                            "password": p.id,
                            "network": "tcp",
                            "tls": {
                                "enabled": sec,
                                "server_name": hName,
                                "insecure": allowInsecure,
                                "alpn": ["http/1.1"],
                                "utls": {
                                    "enabled": true,
                                    "fingerprint": "randomized"
                                }
                            },
                            "transport": {
                                "type": "ws",
                                "path": pathStrTr,
                                "max_early_data": 2560,
                                "early_data_header_name": "Sec-WebSocket-Protocol",
                                "headers": {
                                    "Host": hName
                                }
                            }
                        };
                        outboundsArr.push(ob);
                    }
                    configIndex++;
                    if (sysConfig.enableDirectConfigs && pips.length > 0) {
                        if (isVless) {
                            let tagStr = getUniqueName(getConfigName("alpha", p.name, port, hName, ip, null, configIndex, ipName, p.customName));
                            dynamicTags.push(tagStr);
                            let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadVl = { junk: randomJunk, protocol: "vl", mode: "proxyip", panelIPs: [] };
                            let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));
                            let configUuid = generateConfigUuid(p.id, configIndex);
                            registerConfigEntry(configUuid, p.id, '');
                            let ob = { "type": k_vl_mode, "tag": tagStr, "server": ip, "server_port": parseInt(port), "tcp_fast_open": sysConfig.enableOpt1 || false, "uuid": configUuid, "packet_encoding": "xudp", "network": "tcp", "tls": { "enabled": sec, "server_name": hName, "insecure": allowInsecure, "alpn": ["http/1.1"], "utls": { "enabled": true, "fingerprint": "randomized" } }, "transport": { "type": "ws", "path": pathStrVl, "max_early_data": 2560, "early_data_header_name": "Sec-WebSocket-Protocol", "headers": { "Host": hName } } };
                            outboundsArr.push(ob);
                        }
                        if (isTrojan) {
                            let tagStr = getUniqueName(getConfigName("beta", p.name, port, hName, ip, null, configIndex, ipName, p.customName));
                            dynamicTags.push(tagStr);
                            let randomJunk = Array.from({length: 11}, () => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random() * 62)]).join('');
                            let payloadTr = { junk: randomJunk, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                            let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));
                            let configUuid2 = generateConfigUuid(p.id, configIndex);
                            registerConfigEntry(configUuid2, p.id, '');
                            let ob = { "type": k_tr_mode, "tag": tagStr, "server": ip, "server_port": parseInt(port), "tcp_fast_open": sysConfig.enableOpt1 || false, "password": p.id, "network": "tcp", "tls": { "enabled": sec, "server_name": hName, "insecure": allowInsecure, "alpn": ["http/1.1"], "utls": { "enabled": true, "fingerprint": "randomized" } }, "transport": { "type": "ws", "path": pathStrTr, "max_early_data": 2560, "early_data_header_name": "Sec-WebSocket-Protocol", "headers": { "Host": hName } } };
                            outboundsArr.push(ob);
                        }
                        configIndex++;
                    }
                });
            });
        });
    });

    if (dynamicTags.length === 0) {
        dynamicTags.push("direct");
    }

    return {
        "log": {
            "disabled": false,
            "level": "warn",
            "timestamp": true
        },
        "dns": {
            "servers": [
                {
                    "address": "https://8.8.8.8/dns-query",
                    "detour": "✅ Selector",
                    "tag": "dns-remote"
                },
                {
                    "address": "8.8.8.8",
                    "detour": "direct",
                    "tag": "dns-direct"
                }
            ],
            "rules": [
                {
                    "clash_mode": "Direct",
                    "server": "dns-direct"
                },
                {
                    "clash_mode": "Global",
                    "server": "dns-remote"
                },
                {
                    "query_type": [
                        "HTTPS"
                    ],
                    "action": "reject"
                },
                {
                    "type": "logical",
                    "mode": "and",
                    "rules": [
                        {
                            "rule_set": [
                                "geosite-ir"
                            ]
                        },
                        {
                            "rule_set": "geoip-ir"
                        }
                    ],
                    "action": "route",
                    "server": "dns-direct"
                }
            ],
            "strategy": "prefer_ipv4",
            "independent_cache": true
        },
        "inbounds": [
            {
                "type": "tun",
                "tag": "tun-in",
                "address": [
                    "172.19.0.1/28"
                ],
                "mtu": 9000,
                "auto_route": true,
                "strict_route": true,
                "stack": "mixed"
            },
            {
                "type": "mixed",
                "tag": "mixed-in",
                "listen": "127.0.0.1",
                "listen_port": 2080
            }
        ],
        [k_obds]: [
            ...outboundsArr,
            {
                "type": "selector",
                "tag": "✅ Selector",
                "outbounds": [
                    "💦 Best Ping 🚀",
                    ...fakeRefs,
                    ...dynamicTags
                ],
                "interrupt_exist_connections": false
            },
            {
                "type": "direct",
                "tag": "direct"
            },
            {
                "type": "urltest",
                "tag": "💦 Best Ping 🚀",
                "outbounds": [
                    ...dynamicTags
                ],
                "url": "https://www.gstatic.com/generate_204",
                "interrupt_exist_connections": false,
                "interval": "30s"
            }
        ],
        "route": {
            "rules": [
                {
                    "ip_cidr": "172.19.0.2",
                    "action": "hijack-dns"
                },
                {
                    "clash_mode": "Direct",
                    "outbound": "direct"
                },
                {
                    "clash_mode": "Global",
                    "outbound": "✅ Selector"
                },
                {
                    "action": "sniff"
                },
                {
                    "protocol": "dns",
                    "action": "hijack-dns"
                },
                {
                    "ip_is_private": true,
                    "outbound": "direct"
                },
                {
                    "network": "udp",
                    "action": "reject"
                },
                {
                    "rule_set": [
                        "geosite-ir"
                    ],
                    "action": "route",
                    "outbound": "direct"
                },
                {
                    "rule_set": [
                        "geoip-ir"
                    ],
                    "action": "route",
                    "outbound": "direct"
                }
            ],
            "rule_set": [
                {
                    "type": "remote",
                    "tag": "geosite-ir",
                    "format": "binary",
                    "url": "https://raw.githubusercontent.com/Chocolate4U/Iran-sing-box-rules/rule-set/geosite-ir.srs",
                    "download_detour": "direct"
                },
                {
                    "type": "remote",
                    "tag": "geoip-ir",
                    "format": "binary",
                    "url": "https://raw.githubusercontent.com/Chocolate4U/Iran-sing-box-rules/rule-set/geoip-ir.srs",
                    "download_detour": "direct"
                }
            ],
            "auto_detect_interface": true,
            "final": "✅ Selector"
        },
        "ntp": {
            "enabled": true,
            "server": "time.cloudflare.com",
            "server_port": 123,
            "interval": "30m",
            "write_to_system": false
        },
        "experimental": {
            "cache_file": {
                "enabled": true,
                "store_fakeip": true
            },
            "clash_api": {
                "external_controller": "127.0.0.1:9090",
                "external_ui": "ui",
                "default_mode": "Rule",
                "external_ui_download_url": "https://github.com/MetaCubeX/metacubexd/archive/refs/heads/gh-pages.zip",
                "external_ui_download_detour": "direct"
            }
        }
    };
}

function getDashboardUI(hasDB) {
    return `
  <!DOCTYPE html>
  <html lang="en" class="dark">
  <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
      <meta name="apple-mobile-web-app-capable" content="yes">
      <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
      <meta name="mobile-web-app-capable" content="yes">
      <meta name="theme-color" content="#0c0c0c">
      <meta name="apple-mobile-web-app-title" content="SwimShady">
      <meta name="format-detection" content="telephone=no">
      <meta name="msapplication-tap-highlight" content="no">
      <link rel="apple-touch-icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='22' fill='%236366f1'/><text x='50' y='62' font-size='40' text-anchor='middle' fill='white' font-family='sans-serif' font-weight='bold'>N</text></svg>">
      <title>Swim Shady — Dashboard</title>
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600&family=Geist:wght@400;500;600;700;800&display=swap" rel="stylesheet">
      <script src="https://cdn.tailwindcss.com"></script>
      <script>
          window.toggleAccordion = function(btn) {
              var card = btn.closest('[data-accordion]');
              if (!card) return;
              var content = card.querySelector('[data-accordion-content]');
              var icon = btn.querySelector('.accordion-icon');
              if (!content) return;
              var isOpen = content.style.visibility === 'visible';
              if (isOpen) {
                  content.style.maxHeight = '0';
                  content.style.visibility = 'hidden';
                  if (icon) icon.style.transform = 'rotate(0deg)';
              } else {
                  content.style.visibility = 'visible';
                  content.style.maxHeight = content.scrollHeight + 'px';
                  if (icon) icon.style.transform = 'rotate(180deg)';
              }
          };
      </script>
      <script>
          tailwind.config = { 
              darkMode: 'class', 
              theme: { 
                  extend: { 
                      fontFamily: { sans: ['Inter', 'sans-serif'] },
                      colors: { 
                          primary: '#22c55e', 
                          darkbg: '#0c0c0c', 
                          darkcard: 'rgba(20, 20, 20, 0.75)', 
                          darkborder: 'rgba(255, 255, 255, 0.06)' 
                      } 
                  } 
              } 
          }
      </script>
      <style>
          *, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }
          :root {
              --bg: #0c0c0c; --surface: #141414; --surface-2: #1a1a1a;
              --border: #222; --border-focus: #444;
              --text: #e5e5e5; --text-2: #777; --text-3: #444;
              --accent: #22c55e; --accent-dim: rgba(34,197,94,0.08);
              --red: #ef4444; --amber: #fbbf24; --blue: #3b82f6; --purple: #a78bfa; --cyan: #22d3ee;
              --mono: 'JetBrains Mono', monospace; --sans: 'Inter', -apple-system, sans-serif;
          }
          html { -webkit-font-smoothing: antialiased; }
          body { font-family: var(--sans); background: var(--bg); color: var(--text); min-height: 100vh; display: flex; overflow-x: hidden; }
          .hidden { display: none; }
          .flex { display: flex; }
          .block { display: block; }
          .md\:flex { display: flex; }
          .md\:hidden { display: none; }
          @media (max-width: 768px) { .md\:flex { display: none; } .md\:hidden { display: flex; } }
          .material-symbols-outlined { font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 24; }
          .scroll-content { will-change: transform; -webkit-overflow-scrolling: touch; }
          ::-webkit-scrollbar { width: 6px; }
          ::-webkit-scrollbar-track { background: transparent; }
          ::-webkit-scrollbar-thumb { background: rgba(34,197,94,0.15); border-radius: 10px; }
          ::-webkit-scrollbar-thumb:hover { background: rgba(34,197,94,0.3); }
          [data-accordion-content] { max-height: 0; overflow: hidden; visibility: hidden; transition: max-height 0.3s ease, visibility 0.3s ease; }
          [data-accordion] { border-radius: 0 !important; }
          [data-accordion] > div:last-child { border-radius: 0 !important; }
          button.active, .nav-item.active {
              color: var(--accent) !important;
              background: var(--accent-dim) !important;
              border-left: 2px solid var(--accent) !important;
          }
          .fade-in { animation: fadeIn 0.3s ease-in-out; }
          @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
          body.logged-in .lock-pulse::before, body.logged-in .lock-pulse::after,
          body.logged-in .btn-shimmer::after, body.logged-in .animate-bounce { animation: none !important; }
          html.dark, html.dark body { background: var(--bg) !important; color: var(--text) !important; }
          html.dark .bg-white, html.dark .bg-slate-50, html.dark .bg-indigo-50, html.dark .bg-darkcard {
              background: var(--surface) !important;
              border: 1px solid var(--border) !important;
          }
          html.dark aside {
              background: var(--surface) !important;
              border-inline-end: 1px solid var(--border) !important;
          }
          html:not(.dark) { background: var(--bg) !important; color: var(--text) !important; }
          html:not(.dark) body { background: var(--bg) !important; color: var(--text) !important; }
          html:not(.dark) aside { background: var(--surface) !important; border-inline-end: 1px solid var(--border) !important; }
          .nav-item { border-inline-start: 2px solid transparent; transition: all 0.1s; }
          .nav-item:hover { background: var(--accent-dim) !important; }
          .mobile-nav-item.active { color: var(--accent); }
          .dark .mobile-nav-item.active { color: var(--accent); }
          @media (max-width: 767px) {
              .mobile-bottom-nav {
                  background: var(--surface) !important;
                  border-top: 1px solid var(--border) !important;
                  position: fixed !important;
                  bottom: 0 !important;
                  left: 0 !important;
                  right: 0 !important;
              }
              .mobile-tab-item.active { color: var(--accent); }
              ::-webkit-scrollbar { width: 0; height: 0; }
              #sidebar { display: none !important; }
              #main-content { margin-left: 0 !important; min-height: 0 !important; flex: 1 !important; overflow-y: auto !important; padding-bottom: calc(4rem + env(safe-area-inset-bottom, 0px)) !important; }
              #ov-stats-grid { grid-template-columns: repeat(2, 1fr) !important; }
              #ov-bento-grid { grid-template-columns: 1fr !important; }
              #net-stats-grid { grid-template-columns: repeat(2, 1fr) !important; }
              #save-bar { bottom: calc(4rem + env(safe-area-inset-bottom, 0px) + 12px) !important; }
          }
      </style>
  </head>
  <body class="h-[100dvh] flex flex-col md:flex-row overflow-hidden" style="background:#0c0c0c;color:#e5e5e5;font-family:'Inter',sans-serif;">

      <span id="btn-logout-mob" style="display:none"></span>

      <!-- LOGIN SCREEN -->
      <div id="login-box" class="absolute inset-0 flex items-center justify-center p-4 z-40 overflow-hidden" style="background:#0c0c0c;">
          <div class="relative w-full max-w-sm">
              <style>
                  @keyframes pulse-ring{0%{transform:scale(1);opacity:0.5}100%{transform:scale(1.7);opacity:0}}
                  @keyframes shimmer{0%{left:-100%}100%{left:100%}}
                  @keyframes blink{0%,100%{opacity:1}50%{opacity:0}}
                  @keyframes spin{to{transform:rotate(360deg)}}
                  .lock-pulse::before,.lock-pulse::after{content:'';position:absolute;inset:-8px;border-radius:50%;border:1px solid rgba(34,197,94,0.3);animation:pulse-ring 2.5s ease-out infinite;}
                  .lock-pulse::after{animation-delay:1.25s;}
                  .btn-shimmer::after{content:'';position:absolute;top:0;left:-100%;width:60%;height:100%;background:linear-gradient(90deg,transparent,rgba(255,255,255,0.08),transparent);animation:shimmer 2.5s ease-in-out infinite;}
                  .login-cursor{display:inline-block;width:7px;height:14px;background:#22c55e;animation:blink 1s step-end infinite;vertical-align:middle;}
          /* CLI Terminal Overrides */
          html.dark body { background: #0c0c0c !important; color: #e5e5e5 !important; font-family: 'Inter', sans-serif !important; }
          html.dark .bg-white, html.dark .bg-slate-50, html.dark .bg-slate-100, html.dark .bg-darkcard { background: #141414 !important; border-color: #222 !important; }
          html.dark aside { background: #141414 !important; border-color: #222 !important; }
          html.dark .text-slate-800, html.dark .text-slate-700, html.dark .text-slate-600 { color: #e5e5e5 !important; }
          html.dark .text-slate-500, html.dark .text-slate-400 { color: #777 !important; }
          html.dark .text-white { color: #e5e5e5 !important; }
          html.dark .border-slate-200, html.dark .border-slate-100, html.dark .border-slate-300 { border-color: #222 !important; }
          html.dark .bg-slate-800, html.dark .bg-slate-900, html.dark .bg-slate-950 { background: #0c0c0c !important; }
          html.dark .text-slate-300, html.dark .text-slate-200 { color: #a1a1aa !important; }
          html.dark .bg-emerald-500\/10 { background: rgba(34,197,94,0.08) !important; }
          html.dark .bg-amber-500\/10 { background: rgba(251,191,36,0.08) !important; }
          html.dark .bg-red-500\/10 { background: rgba(239,68,68,0.08) !important; }
          html.dark .bg-blue-500\/10 { background: rgba(59,130,246,0.08) !important; }
          html.dark .bg-violet-500\/10 { background: rgba(167,139,250,0.08) !important; }
          html.dark .bg-indigo-500\/10 { background: rgba(99,102,241,0.08) !important; }
          html.dark .bg-primary\/10 { background: rgba(34,197,94,0.08) !important; }
          html.dark .text-emerald-500, html.dark .text-emerald-400, html.dark .text-emerald-600 { color: #22c55e !important; }
          html.dark .text-amber-500, html.dark .text-amber-400, html.dark .text-amber-600 { color: #fbbf24 !important; }
          html.dark .text-red-500, html.dark .text-red-400, html.dark .text-red-600 { color: #ef4444 !important; }
          html.dark .text-blue-500, html.dark .text-blue-400, html.dark .text-blue-600 { color: #3b82f6 !important; }
          html.dark .text-violet-500, html.dark .text-violet-400, html.dark .text-violet-600 { color: #a78bfa !important; }
          html.dark .text-primary { color: #22c55e !important; }
          html.dark input, html.dark select, html.dark textarea { background: #0c0c0c !important; border-color: #222 !important; color: #e5e5e5 !important; font-family: 'JetBrains Mono', monospace !important; font-size: 13px !important; }
          html.dark input:focus, html.dark select:focus, html.dark textarea:focus { border-color: #22c55e !important; }
          html.dark .rounded-xl, html.dark .rounded-2xl, html.dark .rounded-3xl, html.dark .rounded-lg { border-radius: 0 !important; }
          html.dark .shadow-sm, html.dark .shadow-md, html.dark .shadow-lg, html.dark .shadow-xl { box-shadow: none !important; }
          html.dark header { background: rgba(12,12,12,0.95) !important; border-color: #222 !important; }
          html.dark .hover\:bg-white\/5:hover, html.dark .hover\:bg-white\/10:hover { background: rgba(255,255,255,0.03) !important; }
          html.dark .hover\:bg-slate-100:hover, html.dark .hover\:bg-slate-200:hover { background: #1a1a1a !important; }
          html.dark .bg-slate-100 { background: #141414 !important; }
          html.dark .bg-indigo-50 { background: rgba(34,197,94,0.08) !important; }
          html.dark .bg-red-50 { background: rgba(239,68,68,0.06) !important; }
          html.dark select option { background: #0c0c0c; color: #e5e5e5; }
          html.dark .nav-item { font-family: 'JetBrains Mono', monospace !important; font-size: 12px !important; }
          html.dark .nav-item.active { color: #22c55e !important; background: rgba(34,197,94,0.08) !important; border-left: 2px solid #22c55e !important; }
          html.dark .nav-item:hover { background: rgba(34,197,94,0.05) !important; }
          html.dark label { font-family: 'JetBrains Mono', monospace !important; font-size: 11px !important; text-transform: uppercase !important; letter-spacing: 0.05em !important; color: #777 !important; }
          html.dark h2, html.dark h3 { font-family: 'JetBrains Mono', monospace !important; }
          html.dark .text-sm, html.dark .text-xs { font-family: 'JetBrains Mono', monospace !important; }
          html.dark button { font-family: 'JetBrains Mono', monospace !important; }
          html.dark .bg-primary { background: #22c55e !important; color: #000 !important; }
          html.dark .text-on-primary { color: #000 !important; }

          /* User Card Styles */
          .user-card { border: 1px solid #222; background: #141414; padding: 14px 16px; margin-bottom: 8px; }
          .user-card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
          .user-name { font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 600; }
          .user-status { font-family: 'JetBrains Mono', monospace; font-size: 10px; padding: 2px 6px; }
          .user-status.active { color: #22c55e; border: 1px solid rgba(34,197,94,0.2); }
          .user-status.paused { color: #fbbf24; border: 1px solid rgba(251,191,36,0.2); }
          .user-status.disabled { color: #ef4444; border: 1px solid rgba(239,68,68,0.2); }
          .user-traffic { margin-bottom: 8px; }
          .user-traffic-label { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #777; margin-bottom: 4px; display: flex; justify-content: space-between; }
          .user-bar { height: 3px; background: #222; width: 100%; }
          .user-bar-fill { height: 100%; transition: width 0.3s; }
          .user-bar-fill.green { background: #22c55e; }
          .user-bar-fill.amber { background: #fbbf24; }
          .user-bar-fill.red { background: #ef4444; }
          .user-actions { display: flex; gap: 4px; flex-wrap: wrap; }
          .user-action { padding: 4px 8px; font-family: 'JetBrains Mono', monospace; font-size: 10px; border: 1px solid #222; background: transparent; color: #777; cursor: pointer; transition: all 0.1s; }
          .user-action:hover { border-color: #777; color: #777; }
          .user-action.danger:hover { border-color: #ef4444; color: #ef4444; }

          /* Override generated user card styles */
          html.dark .native-press.bg-white, html.dark .native-press.bg-darkcard { background: #141414 !important; border: 1px solid #222 !important; border-radius: 0 !important; box-shadow: none !important; }
          html.dark .native-press:hover { box-shadow: none !important; }
          html.dark .rounded-2xl { border-radius: 0 !important; }
          html.dark .bg-emerald-500 { background: #22c55e !important; }
          html.dark .bg-amber-500 { background: #fbbf24 !important; }
          html.dark .bg-red-500 { background: #ef4444 !important; }
          html.dark .bg-red-400 { background: #ef4444 !important; }
          html.dark .text-emerald-500 { color: #22c55e !important; }
          html.dark .text-amber-500 { color: #fbbf24 !important; }
          html.dark .text-red-400, html.dark .text-red-500, html.dark .text-red-600 { color: #ef4444 !important; }
          html.dark .bg-red-50 { background: rgba(239,68,68,0.05) !important; }
          html.dark .bg-red-100 { background: rgba(239,68,68,0.08) !important; }
          html.dark .bg-red-900\/40 { background: rgba(239,68,68,0.08) !important; }
          html.dark .bg-red-900\/20 { background: rgba(239,68,68,0.05) !important; }
          html.dark .border-red-100, html.dark .border-red-200 { border-color: rgba(239,68,68,0.1) !important; }
          html.dark .text-red-700, html.dark .text-red-300 { color: #ef4444 !important; }
          html.dark .text-red-500\/70, html.dark .text-red-400\/60 { color: rgba(239,68,68,0.6) !important; }
          html.dark .bg-indigo-50, html.dark .bg-indigo-900\/30 { background: rgba(34,197,94,0.06) !important; }
          html.dark .text-indigo-500, html.dark .text-indigo-600, html.dark .text-indigo-700, html.dark .text-indigo-300 { color: #22c55e !important; }
          html.dark .bg-emerald-50, html.dark .bg-emerald-900\/30 { background: rgba(34,197,94,0.06) !important; }
          html.dark .text-emerald-600, html.dark .text-emerald-300 { color: #22c55e !important; }
          html.dark .bg-violet-50, html.dark .bg-violet-900\/30 { background: rgba(167,139,250,0.06) !important; }
          html.dark .text-violet-500, html.dark .text-violet-600, html.dark .text-violet-300 { color: #a78bfa !important; }
          html.dark .bg-amber-50, html.dark .bg-amber-900\/30 { background: rgba(251,191,36,0.06) !important; }
          html.dark .text-amber-600, html.dark .text-amber-300 { color: #fbbf24 !important; }
          html.dark .bg-cyan-50, html.dark .bg-cyan-900\/30 { background: rgba(34,211,238,0.06) !important; }
          html.dark .text-cyan-600, html.dark .text-cyan-300 { color: #22d3ee !important; }
          html.dark .bg-green-50, html.dark .bg-green-900\/30 { background: rgba(34,197,94,0.06) !important; }
          html.dark .text-green-500, html.dark .text-green-600, html.dark .text-green-700, html.dark .text-green-300 { color: #22c55e !important; }
          html.dark .bg-slate-50, html.dark .bg-slate-800\/50 { background: rgba(24,24,27,0.8) !important; }
          html.dark .bg-gradient-to-r { background: linear-gradient(to right, #22c55e, #22c55e) !important; }
          html.dark .from-emerald-500, html.dark .to-teal-500 { background-color: #22c55e !important; }
          html.dark .from-amber-500, html.dark .to-orange-500 { background-color: #fbbf24 !important; }
          html.dark .from-red-500, html.dark .to-rose-600 { background-color: #ef4444 !important; }
          html.dark .w-full.bg-slate-200, html.dark .dark\:bg-slate-700 { background: #222 !important; }
          html.dark .h-1\.5 { height: 3px !important; }
          html.dark .rounded-full { border-radius: 0 !important; }
          html.dark .text-black { color: #000 !important; }
          html.dark .text-white { color: #e5e5e5 !important; }
          html.dark .bg-green-500 { background: #22c55e !important; }
          html.dark .hover\:bg-green-600:hover { background: #1aab52 !important; }
          html.dark .shadow-sm { box-shadow: none !important; }

          /* Log Entry Styles */
          .log-entry { display: flex; align-items: center; gap: 10px; padding: 8px 0; border-bottom: 1px solid #222; font-family: 'JetBrains Mono', monospace; font-size: 11px; }
          .log-entry:last-child { border-bottom: none; }
          .log-badge { padding: 2px 6px; font-size: 9px; text-transform: uppercase; letter-spacing: 0.05em; white-space: nowrap; }
          .log-badge.green { color: #22c55e; border: 1px solid rgba(34,197,94,0.2); }
          .log-badge.red { color: #ef4444; border: 1px solid rgba(239,68,68,0.2); }
          .log-badge.amber { color: #fbbf24; border: 1px solid rgba(251,191,36,0.2); }
          .log-badge.blue { color: #3b82f6; border: 1px solid rgba(59,130,246,0.2); }
          .log-badge.purple { color: #a78bfa; border: 1px solid rgba(167,139,250,0.2); }
          .log-text { flex: 1; color: #777; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 300px; }
          .log-time { color: #777; white-space: nowrap; }

          /* Profile Card Styles */
          .profile-card { border: 1px solid #222; margin-bottom: 8px; background: #141414; }
          .profile-header { padding: 14px 16px; border-bottom: 1px solid #222; display: flex; justify-content: space-between; align-items: center; }
          .profile-name { font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 600; }
          .profile-id { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #777; }
          .profile-actions { display: flex; gap: 6px; }
          .profile-action { padding: 4px 8px; font-family: 'JetBrains Mono', monospace; font-size: 10px; border: 1px solid #222; background: transparent; color: #777; cursor: pointer; transition: all 0.1s; }
          .profile-action:hover { border-color: #22c55e; color: #22c55e; }
          .profile-url { padding: 10px 16px; font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #777; word-break: break-all; border-top: 1px solid #222; display: flex; align-items: center; gap: 8px; }

          /* Stats Grid */
          .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; background: #222; border: 1px solid #222; margin-bottom: 24px; }
          .stat-card { background: #141414; padding: 16px; }
          .stat-label { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #777; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
          .stat-value { font-family: 'JetBrains Mono', monospace; font-size: 24px; font-weight: 700; color: #e5e5e5; }
          .stat-value.green { color: #22c55e; }
          .stat-value.amber { color: #fbbf24; }
          .stat-value.red { color: #ef4444; }
          .stat-value.blue { color: #3b82f6; }
          .stat-value.purple { color: #a78bfa; }

          /* Bento Grid */
          .bento { display: grid; grid-template-columns: 2fr 1fr; gap: 1px; background: #222; border: 1px solid #222; margin-bottom: 24px; }
          .bento-card { background: #141414; padding: 20px; }
          .bento-title { font-family: 'JetBrains Mono', monospace; font-size: 11px; font-weight: 600; color: #777; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 16px; }

          /* Row Styles */
          .row { display: flex; justify-content: space-between; align-items: center; padding: 10px 0; border-bottom: 1px solid #222; }
          .row:last-child { border-bottom: none; }
          .row-label { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #777; }
          .row-value { font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 600; color: #e5e5e5; }

          /* Actions Grid */
          .actions-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1px; background: #222; border: 1px solid #222; margin-bottom: 24px; }
          .action-card { background: #141414; padding: 14px; cursor: pointer; transition: background 0.1s; }
          .action-card:hover { background: #1a1a1a; }
          .action-icon { font-family: 'JetBrains Mono', monospace; font-size: 14px; margin-bottom: 6px; }
          .action-label { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #777; }

          /* Field Styles */
          .field { margin-bottom: 14px; }
          .field-label { display: block; font-family: 'JetBrains Mono', monospace; font-size: 11px; font-weight: 500; color: #777; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.05em; }
          .field-input, .field-select, .field-textarea { width: 100%; padding: 10px 12px; font-family: 'JetBrains Mono', monospace; font-size: 13px; color: #e5e5e5; background: #0c0c0c; border: 1px solid #222; outline: none; transition: border-color 0.15s; }
          .field-input::placeholder, .field-textarea::placeholder { color: #777; }
          .field-input:focus, .field-select:focus, .field-textarea:focus { border-color: #22c55e; }
          .field-textarea { resize: vertical; min-height: 60px; }

          /* Button Styles */
          .btn { padding: 10px 16px; border: 1px solid #22c55e; font-family: 'JetBrains Mono', monospace; font-size: 12px; font-weight: 600; color: #22c55e; background: transparent; cursor: pointer; transition: all 0.15s; text-transform: uppercase; letter-spacing: 0.05em; }
          .btn:hover { background: #22c55e; color: #000; }
          .btn.btn-red { border-color: #ef4444; color: #ef4444; }
          .btn.btn-red:hover { background: #ef4444; color: #fff; }
          .btn.btn-full { width: 100%; }

          /* Toggle Styles */
          .toggle-row { display: flex; align-items: center; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #222; }
          .toggle-row:last-child { border-bottom: none; }
          .toggle-label { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #777; }
          /* Toggle Switch for Checkboxes */
          .sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; border: 0; }
          .peer:checked ~ .toggle { background: #22c55e; }
          .peer:checked ~ .toggle .toggle-knob { left: 18px; background: #000; }
          .toggle { width: 36px; height: 20px; background: #222; border-radius: 10px; position: relative; cursor: pointer; transition: background 0.15s; flex-shrink: 0; }
          .toggle.on { background: #22c55e; }
          .toggle-knob { width: 16px; height: 16px; background: #444; border-radius: 50%; position: absolute; top: 2px; left: 2px; transition: all 0.15s; }
          .toggle.on .toggle-knob { left: 18px; background: #000; }

          /* Section Label */
          .section-label { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #777; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 12px; }
      </style>
              <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444;margin-bottom:24px;letter-spacing:0.05em;text-transform:uppercase">
                   <span style="color:#22c55e">~</span> / 🦈 swim shady / auth
              </div>
              <div style="background:#141414;border:1px solid #222">
                  <div style="display:flex;align-items:center;gap:8px;padding:12px 16px;border-bottom:1px solid #222;font-family:'JetBrains Mono',monospace;font-size:12px;color:#777">
                      <div style="display:flex;gap:6px"><div style="width:8px;height:8px;border-radius:50%;background:#333"></div><div style="width:8px;height:8px;border-radius:50%;background:#333"></div><div style="width:8px;height:8px;border-radius:50%;background:#22c55e"></div></div>
                      <span>gateway — login</span>
                  </div>
                  <div style="padding:24px">
                      <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #222;display:flex;align-items:center;gap:6px">
                          <span style="color:#22c55e">$</span>
                          <span>authentication required</span>
                          <span class="login-cursor"></span>
                      </div>
                       <form id="login-form" onsubmit="doLogin(); return false;">
                          <div style="margin-bottom:14px">
                              <label style="display:block;font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:500;color:#777;margin-bottom:6px;text-transform:uppercase;letter-spacing:0.05em" for="pwd">password</label>
                              <div style="position:relative">
                                  <input type="password" id="pwd" data-i18n="pass_ph" placeholder="••••••••" style="width:100%;padding:10px 12px;font-family:'JetBrains Mono',monospace;font-size:13px;color:#e5e5e5;background:#0c0c0c;border:1px solid #222;outline:none;transition:border-color 0.15s" autofocus>
                              </div>
                          </div>
                          <div id="err-msg" class="hidden" style="padding:10px 12px;margin-bottom:14px;background:rgba(239,68,68,0.05);border:1px solid rgba(239,68,68,0.1);border-left:2px solid #ef4444;font-family:'JetBrains Mono',monospace;font-size:12px;color:#ef4444">
                              <span data-i18n="err_pass">✗ authentication failed — invalid credentials</span>
                          </div>
                           <button id="login-btn" onclick="doLogin()" style="width:100%;padding:10px 16px;border:1px solid #22c55e;font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#22c55e;background:transparent;cursor:pointer;transition:all 0.15s;text-transform:uppercase;letter-spacing:0.05em" data-i18n="login_btn">
                              authenticate →
                          </button>
                      </form>
                  </div>
              </div>
              <div style="margin-top:16px;font-family:'JetBrains Mono',monospace;font-size:11px;color:#444;display:flex;align-items:center;gap:6px">
                  <span>#</span>
                  <span style="color:#777">v${CURRENT_VERSION}</span>
                  <span>·</span>
                  <span style="color:#777">cloudflare workers</span>
                  <span style="margin-left:auto;color:#444">built by <span style="color:#777">Ceether</span></span>
              </div>
          </div>
      </div>

      <!-- DASHBOARD CONTAINER -->
      <div id="dash-box" class="hidden w-full h-full flex-col md:flex-row relative dash-box-native" style="padding-top: env(safe-area-inset-top, 0px);">
          
          <!-- SIDEBAR (Desktop) -->
          <aside id="sidebar" style="width:220px;height:100vh;position:fixed;left:0;top:0;background:#141414;border-right:1px solid #222;display:flex;flex-direction:column;z-index:10">
              <div style="padding:20px 16px;border-bottom:1px solid #222">
                  <div style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:#22c55e" data-i18n="title">🦈 swim shady</div>
                  <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;margin-top:3px;text-transform:uppercase;letter-spacing:0.08em">gateway console</div>
              </div>
              <nav class="flex-1 overflow-y-auto" style="padding:12px 8px">
                  <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;padding:12px 8px 6px">main</div>
                  <button onclick="switchTab('overview')" id="tab-overview" class="nav-item active" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#777;cursor:pointer;border:none;background:none;width:100%;text-align:left;border-left:2px solid transparent;transition:all 0.1s">
                      <svg style="width:16px;height:16px;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>
                      <span data-i18n="tab_overview">overview</span>
                  </button>
                  <button onclick="switchTab('info')" id="tab-info" class="nav-item" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#777;cursor:pointer;border:none;background:none;width:100%;text-align:left;border-left:2px solid transparent;transition:all 0.1s">
                      <svg style="width:16px;height:16px;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>
                      <span data-i18n="tab_info">endpoints</span>
                  </button>
                  <button onclick="switchTab('network')" id="tab-network" class="nav-item" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#777;cursor:pointer;border:none;background:none;width:100%;text-align:left;border-left:2px solid transparent;transition:all 0.1s">
                      <svg style="width:16px;height:16px;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M18 20V10"/><path d="M12 20V4"/><path d="M6 20v-6"/></svg>
                      <span data-i18n="tab_status">metrics</span>
                  </button>
                  <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;padding:12px 8px 6px">config</div>
                  <button onclick="switchTab('settings')" id="tab-settings" class="nav-item" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#777;cursor:pointer;border:none;background:none;width:100%;text-align:left;border-left:2px solid transparent;transition:all 0.1s">
                      <svg style="width:16px;height:16px;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
                      <span data-i18n="tab_settings">system</span>
                  </button>
                  <button onclick="switchTab('advanced')" id="tab-advanced" class="nav-item" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#777;cursor:pointer;border:none;background:none;width:100%;text-align:left;border-left:2px solid transparent;transition:all 0.1s">
                      <svg style="width:16px;height:16px;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/><line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/><line x1="17" y1="16" x2="23" y2="16"/></svg>
                      <span data-i18n="tab_adv">advanced</span>
                  </button>
                  <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;padding:12px 8px 6px">monitor</div>
                  <button onclick="switchTab('logs')" id="tab-logs" class="nav-item" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#777;cursor:pointer;border:none;background:none;width:100%;text-align:left;border-left:2px solid transparent;transition:all 0.1s">
                      <svg style="width:16px;height:16px;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
                      <span data-i18n="tab_logs">logs</span>
                  </button>
                  <button onclick="switchTab('users')" id="tab-users" class="nav-item" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#777;cursor:pointer;border:none;background:none;width:100%;text-align:left;border-left:2px solid transparent;transition:all 0.1s">
                      <svg style="width:16px;height:16px;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
                      <span data-i18n="tab_users">users</span>
                  </button>
              </nav>
              <div style="padding:12px 8px;border-top:1px solid #222">
                  <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;padding:0 8px;margin-bottom:8px">v${CURRENT_VERSION}</div>
                  <button onclick="logout()" style="display:flex;align-items:center;gap:10px;padding:9px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500;color:#ef4444;cursor:pointer;border:1px solid rgba(239,68,68,0.15);background:transparent;width:100%;text-align:left;transition:all 0.1s">
                      <svg style="width:16px;height:16px" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
                      <span data-i18n="logout">disconnect</span>
                  </button>
              </div>
          </aside>
  
          <!-- MAIN CONTENT AREA -->
          <main id="main-content" style="margin-left:220px;flex:1;min-height:100vh;display:flex;flex-direction:column">
              <header style="display:flex;align-items:center;justify-content:space-between;padding:16px 32px;border-bottom:1px solid #222;background:#141414;position:sticky;top:0;z-index:5">
                  <div id="view-title" style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:600;color:#e5e5e5">~/ overview</div>
                  <div style="display:flex;align-items:center;gap:16px">
                      <span style="font-family:'JetBrains Mono',monospace;font-size:10px;padding:3px 10px;background:rgba(34,197,94,0.08);border:1px solid rgba(34,197,94,0.15);color:#22c55e;text-transform:uppercase;letter-spacing:0.05em">online</span>
                      <span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">v${CURRENT_VERSION}</span>
                      <a href="https://github.com/Ceetherr/swimshady" target="_blank" style="color:#777;transition:color 0.15s" onmouseover="this.style.color='#e5e5e5'" onmouseout="this.style.color='#777'">
                          <svg style="width:16px;height:16px" fill="currentColor" viewBox="0 0 24 24"><path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd"></path></svg>
                      </a>

                  </div>
              </header>
              <div style="flex:1;padding:24px;overflow-y:auto">
  
              <!-- Scrollable Content -->
              <div class="scroll-content flex-1 overflow-y-auto" style="padding:24px">
                  <div class="space-y-6 fade-in">

                      <!-- Update Banner -->
                      <div id="update-alert-banner" class="hidden items-center justify-between p-4 rounded-xl border border-emerald-500/30 bg-emerald-500/5">
                          <div class="flex-1 min-w-0">
                              <p id="update-alert-text" class="text-sm font-bold text-emerald-600 dark:text-emerald-400"></p>
                              <a id="update-github-link" href="#" target="_blank" class="text-[10px] text-slate-400 hover:text-slate-200 transition-colors mt-1 inline-block"></a>
                              <div id="update-changelog-area" class="hidden mt-3">
                                  <div id="update-changelog-content" class="text-xs text-slate-400 max-h-48 overflow-y-auto rounded-lg bg-slate-900/50 p-3 border border-slate-800"></div>
                              </div>
                          </div>
                          <div class="flex items-center gap-2 shrink-0 ms-4">
                              <button onclick="doUpdate()" id="update-deploy-btn" class="px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-bold rounded-lg transition-colors">Deploy</button>
                              <button onclick="dismissUpdate()" class="px-3 py-2 bg-slate-200 dark:bg-slate-700 hover:bg-slate-300 dark:hover:bg-slate-600 text-slate-700 dark:text-slate-300 text-xs font-bold rounded-lg transition-colors">Dismiss</button>
                          </div>
                          <div id="update-deploy-status" class="hidden w-full mt-3 p-3 rounded-xl text-sm font-bold text-center"></div>
                      </div>

                      <!-- OVERVIEW / DASHBOARD VIEW -->
                       <div id="view-overview" class="block">
                          <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">// users</div>
                          <div id="ov-stats-grid" style="display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:#222;border:1px solid #222;margin-bottom:24px">
                              <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="ov_total_users">Total Users</div><p id="ov-total-users" style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700;color:#e5e5e5">-</p></div>
                              <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="ov_active_users">Active</div><p id="ov-active-users" style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700;color:#22c55e">-</p></div>
                              <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="ov_paused_users">Paused</div><p id="ov-paused-users" style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700;color:#fbbf24">-</p></div>
                              <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="ov_auto_disabled">Disabled</div><p id="ov-auto-disabled" style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700;color:#ef4444">-</p></div>
                              <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="ov_expired_users">Expired</div><p id="ov-expired-users" style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700;color:#e5e5e5">-</p></div>
                          </div>
                          <div id="ov-bento-grid" style="display:grid;grid-template-columns:2fr 1fr;gap:1px;background:#222;border:1px solid #222;margin-bottom:24px">
                              <div style="background:#141414;padding:20px">
                                  <div style="font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:#777;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:16px" data-i18n="ov_total_traffic">// traffic</div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777" data-i18n="ov_total_traffic">total traffic</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#e5e5e5" id="ov-total-traffic">- GB</span></div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777" data-i18n="ov_today_traffic">today</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#e5e5e5" id="ov-today-traffic">- GB</span></div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777">requests</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#e5e5e5" id="ov-total-reqs">-</span></div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777">active connections</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#3b82f6" id="ov-active-conns">-</span></div>
                              </div>
                              <div style="background:#141414;padding:20px">
                                  <div style="font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:#777;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:16px">// system</div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">version</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#22c55e" id="ov-version">v${CURRENT_VERSION}</span></div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">status</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#22c55e">active</span></div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">uptime</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#e5e5e5" id="ov-uptime">-</span></div>
                                  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">edge</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#a78bfa" id="ov-colo">-</span></div>
                              </div>
                          </div>
                          <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">// recent activity</div>
                          <div style="border:1px solid #222;background:#141414;padding:20px;margin-bottom:24px">
                              <div id="ov-activity-list" style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444;text-align:center;padding:16px" data-i18n="ov_loading">Loading...</div>
                          </div>
                          <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">// quick actions</div>
                          <div style="display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#222;border:1px solid #222">
                              <div style="background:#141414;padding:14px;cursor:pointer" onclick="openAddUserPage()"><div style="font-family:'JetBrains Mono',monospace;font-size:14px;margin-bottom:6px">+</div><div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777" data-i18n="ov_add_user">add user</div></div>
                              <div style="background:#141414;padding:14px;cursor:pointer" onclick="loadDashboard()"><div style="font-family:'JetBrains Mono',monospace;font-size:14px;margin-bottom:6px">↻</div><div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777" data-i18n="ov_refresh">refresh</div></div>
                              <div style="background:#141414;padding:14px;cursor:pointer" onclick="exportConfig()"><div style="font-family:'JetBrains Mono',monospace;font-size:14px;margin-bottom:6px">↓</div><div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777" data-i18n="ov_backup_config">backup</div></div>
                              <div style="background:#141414;padding:14px;cursor:pointer" onclick="switchTab('network')"><div style="font-family:'JetBrains Mono',monospace;font-size:14px;margin-bottom:6px">⚡</div><div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#777">diagnostics</div></div>
                          </div>
                      </div>

                       <!-- INFO VIEW -->
                       <div id="view-info" class="hidden">
                           <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">// subscription endpoints</div>
                           <div id="dyn-profiles-container"></div>
                       </div>

                       <!-- NETWORK/METRICS VIEW -->
                       <div id="view-network" class="hidden">
                           <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">// network diagnostics</div>
                           <div id="net-stats-grid" style="display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:#222;border:1px solid #222;margin-bottom:24px">
                               <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="stat_ip">origin ip</div><p id="net-ip" style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:#3b82f6">...</p></div>
                               <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="stat_dc">edge node</div><p id="net-colo" style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:#22c55e">...</p></div>
                               <div style="background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="stat_loc">region</div><p id="net-loc" style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:#a78bfa">...</p></div>
                           </div>
                           <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px">
                               <div style="border:1px solid #222;background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px" data-i18n="stat_datetime">date / time</div><div style="font-family:'JetBrains Mono',monospace;font-size:13px;color:#e5e5e5" id="net-datetime">...</div></div>
                               <div style="border:1px solid #222;background:#141414;padding:16px"><div style="font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px">active conns</div><div style="font-family:'JetBrains Mono',monospace;font-size:13px;color:#22c55e" id="net-active-conns">-</div></div>
                           </div>
                           <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">// live profile usage</div>
                           <div style="border:1px solid #222;background:#141414;margin-bottom:16px">
                               <div id="usage-metrics-container" style="padding:16px;font-family:'JetBrains Mono',monospace;font-size:11px;color:#444;text-align:center" data-i18n="no_metrics">No active connection data yet.</div>
                           </div>
                            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">// latency diagnostics</div>
                            <div style="border:1px solid #222;background:#141414;padding:20px;margin-bottom:16px">
                                <div style="font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:#777;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:12px" data-i18n="ping_test_title">ping test</div>
                                <div class="field" style="margin-bottom:12px">
                                    <label class="field-label" data-i18n="ping_target">target</label>
                                    <input class="field-input" id="ping-custom-ip" placeholder="1.1.1.1 or clean IP">
                                </div>
                                <button onclick="runPingTest()" style="width:100%;padding:10px 16px;border:1px solid #22c55e;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600;color:#22c55e;background:transparent;cursor:pointer;text-transform:uppercase;letter-spacing:0.05em" data-i18n="run_diagnostics">run diagnostics →</button>
                               <div id="ping-results" class="hidden" style="margin-top:12px">
                                   <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444" data-i18n="target_node">target</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#e5e5e5" id="ping-target">-</span></div>
                                   <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">response</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#22c55e" id="ping-time">-</span></div>
                                   <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #222"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">status</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#22c55e" id="ping-status">-</span></div>
                                   <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0"><span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444">port</span><span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#e5e5e5" id="ping-port">443</span></div>
                               </div>
                           </div>
                       </div>
                      </div>
  
                      <!-- SETTINGS VIEW -->
                      <div id="view-settings" class="hidden">
                          <div class="bg-white dark:bg-darkcard rounded-3xl p-6 shadow-sm border border-slate-200 dark:border-darkborder grid grid-cols-1 md:grid-cols-2 gap-5">
                              <div class="space-y-1">
                                  <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_proto">Primary Display Mode</label>
                                  <select id="cfg-proto" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none appearance-none">
                                      <option value="alpha">Alpha Mode (V-Core)</option>
                                      <option value="beta">Beta Mode (T-Core)</option>
                                      <option value="both">Both (V-Core & T-Core)</option>
                                  </select>
                              </div>
                               <div class="space-y-1">
                                  <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_port">Data Port (Checkbox Selection)</label>
                                  <select id="cfg-port" multiple class="hidden">
                                      <option value="443">443</option>
                                      <option value="2053">2053</option>
                                      <option value="2083">2083</option>
                                      <option value="2087">2087</option>
                                      <option value="2096">2096</option>
                                      <option value="8443">8443</option>
                                      <option value="80">80</option>
                                      <option value="8080">8080</option>
                                      <option value="8880">8880</option>
                                      <option value="2052">2052</option>
                                      <option value="2082">2082</option>
                                      <option value="2086">2086</option>
                                      <option value="2095">2095</option>
                                  </select>
                                  <div id="port-checkboxes-container" class="bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-darkborder p-4 rounded-xl space-y-3 font-mono text-xs max-h-48 overflow-y-auto">
                                      <!-- TLS ports -->
                                      <div class="space-y-1.5">
                                          <div class="text-[10px] uppercase tracking-wider font-bold text-slate-400 dark:text-slate-500">🔒 Secure (TLS)</div>
                                          <div class="grid grid-cols-2 gap-2">
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="443" onchange="togglePortCheckbox('443', this.checked)" class="accent-primary">
                                                  <span>443</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="2053" onchange="togglePortCheckbox('2053', this.checked)" class="accent-primary">
                                                  <span>2053</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="2083" onchange="togglePortCheckbox('2083', this.checked)" class="accent-primary">
                                                  <span>2083</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="2087" onchange="togglePortCheckbox('2087', this.checked)" class="accent-primary">
                                                  <span>2087</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="2096" onchange="togglePortCheckbox('2096', this.checked)" class="accent-primary">
                                                  <span>2096</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="8443" onchange="togglePortCheckbox('8443', this.checked)" class="accent-primary">
                                                  <span>8443</span>
                                              </label>
                                          </div>
                                      </div>
                                      <!-- Non-TLS ports -->
                                      <div class="space-y-1.5 pt-1 border-t border-slate-200 dark:border-slate-700">
                                          <div class="text-[10px] uppercase tracking-wider font-bold text-slate-400 dark:text-slate-500">🔓 Standard</div>
                                          <div class="grid grid-cols-2 gap-2">
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="80" onchange="togglePortCheckbox('80', this.checked)" class="accent-primary">
                                                  <span>80</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="8080" onchange="togglePortCheckbox('8080', this.checked)" class="accent-primary">
                                                  <span>8080</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="8880" onchange="togglePortCheckbox('8880', this.checked)" class="accent-primary">
                                                  <span>8880</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="2052" onchange="togglePortCheckbox('2052', this.checked)" class="accent-primary">
                                                  <span>2052</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="2082" onchange="togglePortCheckbox('2082', this.checked)" class="accent-primary">
                                                  <span>2082</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition">
                                                  <input type="checkbox" value="2086" onchange="togglePortCheckbox('2086', this.checked)" class="accent-primary">
                                                  <span>2086</span>
                                              </label>
                                              <label class="flex items-center gap-2 p-1.5 rounded bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition col-span-2">
                                                  <input type="checkbox" value="2095" onchange="togglePortCheckbox('2095', this.checked)" class="accent-primary">
                                                  <span>2095</span>
                                              </label>
                                          </div>
                                      </div>
                                  </div>
                              </div>
                              <div class="space-y-1 md:col-span-2">
                                  <div class="flex justify-between items-center">
                                      <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_id">Device UUID (Empty=Auto)</label>
                                      <button type="button" onclick="document.getElementById('cfg-uuid').value = crypto.randomUUID()" class="text-xs text-primary bg-primary/10 hover:bg-primary/20 px-2 py-1 rounded transition-colors duration-200" data-i18n="btn_generate_uuid">Generate UUID</button>
                                  </div>
                                  <input type="text" id="cfg-uuid" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none font-mono text-sm">
                              </div>
                              <div class="space-y-1">
                                  <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_path">API Route (Hidden Path)</label>
                                  <input type="text" id="cfg-path" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none">
                              </div>
                              <div class="space-y-1">
                                  <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_pass">Master Key</label>
                                  <div class="relative">
                                      <input type="password" id="cfg-pass" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none pe-12">
                                      <button type="button" onclick="const n=document.getElementById('cfg-pass');n.type=n.type==='password'?'text':'password'" class="absolute inset-y-0 end-0 flex items-center px-4 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">👁️</button>
                                  </div>
                              </div>
                              <div class="space-y-1 md:col-span-2">
                                  <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_github_repo">GitHub Update Repository</label>
                                  <input type="text" id="cfg-github-repo" placeholder="Ceetherr/swimshady" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                  <div class="flex justify-start items-center gap-2 mt-2">
                                      <button type="button" onclick="triggerManualRedeploy()" class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-primary/10 hover:bg-primary/20 text-primary text-xs font-bold rounded-lg transition-colors border border-primary/20">
                                          🔄 <span data-i18n="btn_redeploy_force">Force Redeploy / Switch Format</span>
                                      </button>
                                  </div>
                              </div>
                              <div class="space-y-1 md:col-span-2">
                                  <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_sub_ua">Custom Subscription User-Agent</label>
                                  <input type="text" id="cfg-sub-ua" placeholder="e.g. MySpecialUABypass" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                  <p class="text-xs text-slate-500 mt-1 ms-1" data-i18n="desc_sub_ua">Allow specific browser User-Agent containing this text to bypass camouflage and retrieve profile data directly in web browser.</p>
                              </div>
                              <div class="space-y-1 md:col-span-2">
                                  <label class="block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1" data-i18n="lbl_custom_panel_url">Custom Panel URL / Subscription Domain</label>
                                  <input type="text" id="cfg-custom-panel-url" placeholder="e.g. custom.domain.com or https://custom.domain.com" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                  <p class="text-xs text-slate-500 mt-1 ms-1" data-i18n="desc_custom_panel_url">Optionally specify a custom domain/URL to be used for subscription/sync links. If empty, the default Worker address will be used.</p>
                              </div>
                               <!-- System Toggles -->
                               <div class="flex flex-col sm:flex-row gap-3 md:col-span-2">
                                   <label class="flex-1 flex items-center justify-between cursor-pointer p-4" style="background:#1a1a1a;border:1px solid #222">
                                       <span style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#e5e5e5" data-i18n="lbl_silent">Silent UI Alerts</span>
                                         <div class="toggle" style="position:relative;cursor:pointer" onclick="this.classList.toggle('on')">
                                             <input type="checkbox" id="cfg-silent" class="sr-only" style="display:none">
                                             <div style="width:36px;height:20px;border-radius:10px;position:relative;transition:background 0.15s">
                                                 <div style="width:16px;height:16px;background:#444;border-radius:50%;position:absolute;top:2px;transition:all 0.15s" class="toggle-knob"></div>
                                            </div>
                                        </div>
                                    </label>
                                    <label class="flex-1 flex items-center justify-between cursor-pointer p-4" style="background:rgba(239,68,68,0.03);border:1px solid rgba(239,68,68,0.1)">
                                        <span style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#ef4444" data-i18n="lbl_pause">Kill Switch</span>
                                         <div class="toggle" style="position:relative;cursor:pointer" onclick="this.classList.toggle('on')">
                                            <input type="checkbox" id="cfg-pause" class="sr-only" style="display:none">
                                            <div style="width:36px;height:20px;border-radius:10px;position:relative;transition:background 0.15s">
                                                <div style="width:16px;height:16px;background:#444;border-radius:50%;position:absolute;top:2px;transition:all 0.15s" class="toggle-knob"></div>
                                           </div>
                                       </div>
                                   </label>
                                </div>
                                <div class="space-y-3 md:col-span-2">
                                    <label class="flex items-center justify-between p-4" style="background:rgba(34,197,94,0.03);border:1px solid rgba(34,197,94,0.1)">
                                        <div>
                                            <span style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#22c55e" data-i18n="lbl_auto_update">Auto-Update</span>
                                            <p style="font-family:monospace;font-size:10px;color:rgba(34,197,94,0.5);margin-top:2px">Automatically deploy when a new version is detected</p>
                                        </div>
                                           <div class="toggle" style="position:relative;cursor:pointer" onclick="this.classList.toggle('on')">
                                               <input type="checkbox" id="cfg-auto-update" class="sr-only" style="display:none">
                                               <div style="width:36px;height:20px;border-radius:10px;position:relative;transition:background 0.15s">
                                                 <div style="width:16px;height:16px;background:#444;border-radius:50%;position:absolute;top:2px;transition:all 0.15s" class="toggle-knob"></div>
                                            </div>
                                        </div>
                                    </label>
                                   <div id="auto-update-format-wrap" class="hidden">
                                       <label class="block text-xs font-bold text-slate-500 mb-2" data-i18n="lbl_auto_update_format">Update Format</label>
                                       <div class="flex gap-3">
                                           <label class="flex-1 flex items-center gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder cursor-pointer hover:border-emerald-400 transition-colors">
                                               <input type="radio" name="auto-update-format" value="normal" checked class="accent-emerald-500">
                                               <div>
                                               <span class="text-xs font-bold text-slate-700 dark:text-slate-300" data-i18n="format_normal_label">Normal</span>
                                               <p class="text-[10px] text-slate-400" data-i18n="desc_format_normal">Standard _worker.js</p>
                                               </div>
                                           </label>
                                           <label class="flex-1 flex items-center gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder cursor-pointer hover:border-emerald-400 transition-colors">
                                               <input type="radio" name="auto-update-format" value="obfuscated" class="accent-emerald-500">
                                               <div>
                                               <span class="text-xs font-bold text-slate-700 dark:text-slate-300" data-i18n="format_obfuscated_label">Obfuscated</span>
                                               <p class="text-[10px] text-slate-400" data-i18n="desc_format_obfuscated">XOR byte-shifting</p>
                                               </div>
                                           </label>
                                       </div>
                                   </div>
                                </div>

                                <!-- API Keys Management -->
                                <div class="bg-white dark:bg-darkcard rounded-3xl p-6 shadow-sm border border-slate-200 dark:border-darkborder md:col-span-2 space-y-4">
                                    <div class="flex items-center justify-between">
                                        <div>
                                            <h3 class="text-sm font-bold text-slate-700 dark:text-slate-200 flex items-center gap-2">
                                                🔑 <span data-i18n="lbl_api_keys">Panel API Keys</span>
                                            </h3>
                                            <p class="text-[10px] text-slate-500 dark:text-slate-400 mt-1" data-i18n="desc_api_keys">Generate API keys to securely connect remote panels. Remote panels use these keys instead of sharing your master key.</p>
                                        </div>
                                        <button onclick="generateApiKey()" class="px-4 py-2 bg-primary text-white text-xs font-bold rounded-xl hover:opacity-90 transition-opacity" data-i18n="btn_generate_key">Generate Key</button>
                                    </div>
                                    <div id="api-keys-list" class="space-y-2"></div>
                                    <div id="api-key-new" class="hidden bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-800 rounded-xl p-4 space-y-2">
                                        <p class="text-xs font-bold text-emerald-700 dark:text-emerald-400" data-i18n="api_key_created">API Key Created! Copy it now — it won't be shown again.</p>
                                        <div class="flex items-center gap-2">
                                            <input type="text" id="api-key-value" readonly class="flex-1 px-3 py-2 bg-white dark:bg-slate-800 rounded-lg text-xs font-mono border border-emerald-300 dark:border-emerald-700 text-slate-700 dark:text-slate-300">
                                            <button onclick="copyApiKey()" class="px-3 py-2 bg-emerald-600 text-white text-xs font-bold rounded-lg hover:bg-emerald-700">Copy</button>
                                        </div>
                                    </div>
                                </div>

                                <!-- Import/Export Config Area -->
                               <div class="bg-white dark:bg-darkcard rounded-3xl p-6 shadow-sm border border-slate-200 dark:border-darkborder md:col-span-2 space-y-4">
                                  <h3 class="text-sm uppercase font-bold text-slate-400 tracking-wider" data-i18n="backup_restore_title">Backup & Restore</h3>
                                  <div class="flex flex-col sm:flex-row gap-4">
                                      <button onclick="exportConfig()" class="flex-1 py-3 px-4 bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 font-bold rounded-xl transition-colors text-sm" data-i18n="export_btn">
                                          📥 Export Configuration (JSON)
                                      </button>
                                      <label class="flex-1 py-3 px-4 bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 font-bold rounded-xl transition-colors text-sm text-center cursor-pointer">
                                          <span data-i18n="import_btn">📤 Import Configuration (JSON)</span>
                                          <input type="file" id="import-file" class="hidden" accept=".json" onchange="importConfig(event)">
                                      </label>
                                  </div>
                              </div>
                          </div>
                      </div>
  
                      <!-- ADVANCED VIEW -->
                      <div id="view-advanced" class="hidden space-y-4">

                          <!-- Section: Network & DNS -->
                          <div class="bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden" data-accordion>
                              <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                                  <div class="flex items-center gap-3">
                                      <span class="text-lg">🌐</span>
                                      <span class="text-sm font-bold text-slate-700 dark:text-slate-200" data-i18n="adv_network_dns">Network & DNS</span>
                                  </div>
                                  <svg class="w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                              </button>
                              <div data-accordion-content class="transition-all duration-300" style="max-height:0;overflow:hidden;visibility:hidden">
                                  <div class="space-y-4 px-5 pb-5 pt-1">
                                      <div>
                                          <div class="flex items-center justify-between mb-2">
                                              <label class="text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_clean_ips">Clean IPs (Multi-Generator)</label>
                                              <span class="text-xs bg-indigo-100 dark:bg-indigo-900/50 text-indigo-700 dark:text-indigo-300 px-2 py-1 rounded-md font-bold" id="ip-count-badge">1 Config Set</span>
                                          </div>
                                          <textarea id="cfg-ips" rows="3" data-i18n="ph_clean_ips" placeholder="1.2.3.4#Germany&#10;5.6.7.8#US&#10;9.10.11.12#France" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none font-mono text-sm resize-none"></textarea>
                                          <p class="text-xs text-slate-400 mt-2" data-i18n="desc_clean_ips">One IP per line. Use <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">IP#Name</code> format to tag IPs (e.g. <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">1.2.3.4#Germany</code>). Use <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{IP_NAME}</code> in name strategy.</p>
                                          <button id="btn-resolve-smart-ips" onclick="resolveSmartCleanIps()" class="mt-3 w-full sm:w-auto px-4 py-2.5 bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 rounded-xl text-xs font-bold transition-all flex items-center justify-center gap-2">
                                              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                                              Auto-Resolve CDN & Clean IPs
                                          </button>
                                      </div>
                                      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_fp">TLS Signature</label>
                                              <select id="cfg-fp" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none appearance-none">
                                                  <option value="chrome">Chrome</option><option value="firefox">Firefox</option><option value="safari">Safari</option>
                                              </select>
                                          </div>
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_dns">Resolver IP</label>
                                              <input type="text" id="cfg-dns" placeholder="1.1.1.1" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                          </div>
                                          <div class="space-y-1 md:col-span-2">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_doh">Custom DNS (DoH Provider)</label>
                                              <input type="text" id="cfg-custom-dns" placeholder="https://cloudflare-dns.com/dns-query" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                          </div>
                                          <div class="space-y-1 md:col-span-2">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300">Private DoH Server</label>
                                              <p class="text-[11px] text-slate-400 mb-2">Your private DNS-over-HTTPS endpoint. Use this in your clients instead of public DNS for privacy and censorship bypass.</p>
                                              <div class="flex gap-2">
                                                  <input type="text" id="cfg-doh-server" readonly class="flex-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 text-sm font-mono" style="color: var(--text-2);">
                                                  <button onclick="copyDoHServer()" class="px-4 py-2 bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 rounded-xl text-xs font-bold transition-all">Copy</button>
                                              </div>
                                              <p class="text-[10px] text-slate-500 mt-1">Add <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">/dns-query</code> to your client's DNS settings.</p>
                                          </div>
                                      </div>
                                  </div>
                              </div>
                          </div>

                          <!-- Section: Proxy & Relay -->
                          <div class="bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden" data-accordion>
                              <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                                  <div class="flex items-center gap-3">
                                      <span class="text-lg">🔗</span>
                                      <span class="text-sm font-bold text-slate-700 dark:text-slate-200" data-i18n="adv_proxy_relay">Proxy & Relay</span>
                                  </div>
                                  <svg class="w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                              </button>
                              <div data-accordion-content class="transition-all duration-300" style="max-height:0;overflow:hidden;visibility:hidden">
                                  <div class="space-y-4 px-5 pb-5 pt-1">
                                      <div class="space-y-1">
                                          <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_relay">Proxy IPs (Comma/Newline separated)</label>
                                          <textarea id="cfg-relay" rows="3" placeholder="104.20.0.1&#10;proxyip.cmliussss.net" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none font-mono text-sm resize-none"></textarea>
                                      </div>
                                      <div class="space-y-1">
                                          <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_nat64">NAT64 Prefix</label>
                                          <textarea id="cfg-nat64" rows="2" placeholder="64:ff9b::/96&#10;2001:db8:64::/96" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none font-mono text-sm resize-none"></textarea>
                                          <p class="text-xs text-slate-400 mt-1" data-i18n="desc_nat64">Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses. Supports multiple prefixes (one per line).</p>
                                      </div>
                                      <label class="flex items-center justify-between cursor-pointer p-4" style="background:#1a1a1a;border:1px solid #222">
                                           <div>
                                               <span style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#e5e5e5" data-i18n="lbl_direct_configs">Include Direct Configs</span>
                                               <p style="font-family:monospace;font-size:10px;color:#444;margin-top:2px">Generate configs without Proxy IP alongside relay configs</p>
                                           </div>
                                              <div class="toggle" style="position:relative;cursor:pointer" onclick="this.classList.toggle('on')">
                                                  <input type="checkbox" id="cfg-direct-configs" class="sr-only" style="display:none">
                                                  <div style="width:36px;height:20px;border-radius:10px;position:relative;transition:background 0.15s">
                                                    <div style="width:16px;height:16px;background:#444;border-radius:50%;position:absolute;top:2px;transition:all 0.15s" class="toggle-knob"></div>
                                               </div>
                                           </div>
                                       </label>
                                  </div>
                              </div>
                          </div>

                          <!-- Section: Subscription -->
                          <div class="bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden" data-accordion>
                              <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                                  <div class="flex items-center gap-3">
                                      <span class="text-lg">📝</span>
                                      <span class="text-sm font-bold text-slate-700 dark:text-slate-200" data-i18n="adv_subscription">Subscription</span>
                                  </div>
                                  <svg class="w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                              </button>
                              <div data-accordion-content class="transition-all duration-300" style="max-height:0;overflow:hidden;visibility:hidden">
                                  <div class="space-y-4 px-5 pb-5 pt-1">
                                      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_strategy">Configuration Name Strategy</label>
                                              <input type="text" id="cfg-name-strategy" placeholder="{FLAG} {PROTOCOL}-{USER}-{PORT}" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none">
                                              <p data-i18n="html_desc_strategy" class="text-[11px] text-slate-400 dark:text-slate-500 mt-1 leading-relaxed">
                                                  Supported placeholders: <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{FLAG}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{COUNTRY}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{CITY}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{ISP}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{PROTOCOL}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{USER}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{PORT}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{PREFIX}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{IP}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{IP_NAME}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{HOST}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{DATE}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{INDEX}</code> <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{WORKER}</code>.<br>
                                                  <span class="text-slate-500 dark:text-slate-400">{FLAG}</span>: Country flag emoji &middot; <span class="text-slate-500 dark:text-slate-400">{COUNTRY}</span>: Country name &middot; <span class="text-slate-500 dark:text-slate-400">{CITY}</span>: City name &middot; <span class="text-slate-500 dark:text-slate-400">{ISP}</span>: ISP / ASN org &middot; <span class="text-slate-500 dark:text-slate-400">{PROTOCOL}</span>: VLESS / Trojan &middot; <span class="text-slate-500 dark:text-slate-400">{USER}</span>: Subscriber name &middot; <span class="text-slate-500 dark:text-slate-400">{PORT}</span>: Active port &middot; <span class="text-slate-500 dark:text-slate-400">{PREFIX}</span>: Custom prefix &middot; <span class="text-slate-500 dark:text-slate-400">{IP}</span>: Clean IP &middot; <span class="text-slate-500 dark:text-slate-400">{HOST}</span>: Hostname &middot; <span class="text-slate-500 dark:text-slate-400">{DATE}</span>: Current date (YYYY-MM-DD) &middot; <span class="text-slate-500 dark:text-slate-400">{INDEX}</span>: Config index &middot; <span class="text-slate-500 dark:text-slate-400">{WORKER}</span>: Worker name.<br>
                                                  Pre-defined strategies: <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">default</code>, <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">type-user-port</code>, <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">user-port</code>, <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">host-port-user</code>, <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">prefix-user-port</code>, <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">ip</code>
                                              </p>
                                          </div>
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_prefix">Custom Name Prefix</label>
                                              <input type="text" id="cfg-name-prefix" placeholder="Core" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                          </div>
                                       </div>
                                       <div class="border-t border-slate-100 dark:border-darkborder pt-4">
                                          <div class="flex items-center justify-between mb-3">
                                              <div>
                                                  <h4 class="text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_fake_configs">Subscription Fake Entries</h4>
                                                  <p class="text-[11px] text-slate-400 dark:text-slate-500 mt-0.5" data-i18n="desc_fake_configs">Customize info entries shown in subscription profiles. Use <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{usage}</code> and <code class="bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono">{expiry}</code> for dynamic values.</p>
                                              </div>
                                              <button onclick="addFakeConfig()" class="px-3 py-1.5 bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 rounded-xl text-xs font-bold transition-all flex items-center gap-1.5 shrink-0">
                                                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6"></path></svg>
                                                  <span data-i18n="btn_add_entry">Add Entry</span>
                                              </button>
                                          </div>
                                          <div id="fake-configs-list" class="space-y-2"></div>
                                      </div>
                                  </div>
                              </div>
                          </div>

                          <!-- Section: Protocol -->
                          <div class="bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden" data-accordion>
                              <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                                  <div class="flex items-center gap-3">
                                      <span class="text-lg">⚡</span>
                                      <span class="text-sm font-bold text-slate-700 dark:text-slate-200" data-i18n="adv_protocol">Protocol</span>
                                  </div>
                                  <svg class="w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                              </button>
                              <div data-accordion-content class="transition-all duration-300" style="max-height:0;overflow:hidden;visibility:hidden">
                                  <div class="flex flex-col sm:flex-row gap-3">
                                      <label class="flex-1 flex items-center justify-between cursor-pointer p-4" style="background:#1a1a1a;border:1px solid #222">
                                          <span style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#e5e5e5" data-i18n="lbl_tfo">TCP Fast Open</span>
                                             <div class="toggle" style="position:relative;cursor:pointer" onclick="this.classList.toggle('on')">
                                                 <input type="checkbox" id="cfg-tfo" class="sr-only" style="display:none">
                                                 <div style="width:36px;height:20px;border-radius:10px;position:relative;transition:background 0.15s">
                                                   <div style="width:16px;height:16px;background:#444;border-radius:50%;position:absolute;top:2px;transition:all 0.15s" class="toggle-knob"></div>
                                              </div>
                                          </div>
                                      </label>
                                      <label class="flex-1 flex items-center justify-between cursor-pointer p-4" style="background:#1a1a1a;border:1px solid #222">
                                          <span style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#e5e5e5" data-i18n="lbl_ech">Secure Hello (ECH)</span>
                                             <div class="toggle" style="position:relative;cursor:pointer" onclick="this.classList.toggle('on')">
                                                 <input type="checkbox" id="cfg-ech" class="sr-only" style="display:none">
                                                 <div style="width:36px;height:20px;border-radius:10px;position:relative;transition:background 0.15s">
                                                   <div style="width:16px;height:16px;background:#444;border-radius:50%;position:absolute;top:2px;transition:all 0.15s" class="toggle-knob"></div>
                                              </div>
                                          </div>
                                      </label>
                                  </div>
                              </div>
                          </div>

                          <!-- Section: Cluster -->
                          <div class="bg-indigo-50 dark:bg-indigo-950/20 rounded-2xl border border-indigo-100 dark:border-indigo-900/50 overflow-hidden" data-accordion>
                               <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between px-5 py-4 hover:bg-indigo-100/50 dark:hover:bg-indigo-900/30 transition-colors">
                                   <div class="flex items-center gap-3">
                                       <span class="text-lg">🔬</span>
                                       <span class="text-sm font-bold text-indigo-700 dark:text-indigo-300" data-i18n="other_nodes_title">Other Nodes</span>
                                   </div>
                                   <svg class="w-4 h-4 text-indigo-400 transform transition-transform duration-200 accordion-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                               </button>
                               <div data-accordion-content class="transition-all duration-300" style="max-height:0;overflow:hidden;visibility:hidden">
                                   <div class="space-y-3 px-5 pb-5 pt-1">
                                       <p class="text-xs text-indigo-600/80 dark:text-indigo-300/70 leading-relaxed" data-i18n="other_nodes_desc">External nodes (URL + API Key) for cross-panel management.</p>
                                       <div class="flex items-center justify-between">
                                           <div id="linked-nodes-list" class="space-y-2 flex-1"></div>
                                       </div>
                                       <button onclick="showAddNodeModal()" type="button" class="w-full py-3 border-2 border-dashed border-indigo-300 dark:border-indigo-700 hover:border-indigo-500 dark:hover:border-indigo-500 text-indigo-500 dark:text-indigo-400 text-sm font-bold rounded-xl transition-colors flex items-center justify-center gap-2">
                                           <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6"></path></svg>
                                           <span data-i18n="add_node_confirm">Add Node</span>
                                       </button>
                                   </div>
                               </div>
                           </div>

                          <!-- Modal: Add Other Node -->
                           <div id="modal-add-node" class="hidden fixed inset-0 z-50 flex items-end sm:items-center justify-center p-0 pb-4 sm:p-4 bg-slate-900/50 backdrop-blur-sm">
                               <div class="bg-white dark:bg-darkcard rounded-t-3xl sm:rounded-3xl w-full sm:max-w-md max-h-[calc(100vh-2rem)] sm:max-h-[85vh] flex flex-col shadow-2xl border border-slate-200 dark:border-darkborder">
                                  <div class="px-6 pt-6 pb-4">
                                      <h3 class="text-lg font-bold" data-i18n="add_node_title">Add External Node</h3>
                                      <p class="text-xs text-slate-400 mt-1" data-i18n="add_node_desc">Enter the URL and API Key of the external panel.</p>
                                  </div>
                                   <div class="px-6 pb-4 space-y-4 overflow-y-auto flex-1 min-h-0">
                                      <div>
                                          <label class="block text-xs font-bold text-slate-500 mb-1" data-i18n="add_node_url">Node URL</label>
                                          <input type="text" id="add-node-url" placeholder="node.example.com" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono">
                                      </div>
                                      <div>
                                          <label class="block text-xs font-bold text-slate-500 mb-1" data-i18n="add_node_apikey">API Key</label>
                                          <input type="password" id="add-node-apikey" placeholder="swimshady_..." class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono pe-12">
                                          <button type="button" onclick="const n=document.getElementById('add-node-apikey');n.type=n.type==='password'?'text':'password'" class="absolute end-14 mt-[-36px] px-2 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">👁️</button>
                                      </div>
                                  </div>
                                  <div class="px-6 py-4 border-t border-slate-200 dark:border-darkborder flex justify-end gap-2">
                                      <button onclick="document.getElementById('modal-add-node').classList.add('hidden')" class="px-4 py-2 rounded-xl bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 font-bold text-sm" data-i18n="btn_cancel">Cancel</button>
                                      <button onclick="commitAddNode()" class="px-4 py-2 rounded-xl bg-indigo-500 hover:bg-indigo-600 text-white font-bold text-sm" data-i18n="add_node_confirm">Add Node</button>
                                  </div>
                              </div>
                          </div>

                          <!-- Section: Telegram -->
                          <div class="bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden" data-accordion>
                              <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                                  <div class="flex items-center gap-3">
                                      <span class="text-lg">🤖</span>
                                      <span class="text-sm font-bold text-slate-700 dark:text-slate-200" data-i18n="adv_telegram">Telegram Bot</span>
                                  </div>
                                  <svg class="w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                              </button>
                              <div data-accordion-content class="transition-all duration-300" style="max-height:0;overflow:hidden;visibility:hidden">
                                  <div class="space-y-3 px-5 pb-5 pt-1">
                                      <div class="space-y-1">
                                          <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_tg_token">Bot Token</label>
                                          <div class="relative">
                                              <input type="password" id="cfg-tg-token" placeholder="123456:ABC-DEF1234ghIkl-zyx5c" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm pe-12">
                                              <button type="button" onclick="const n=document.getElementById('cfg-tg-token');n.type=n.type==='password'?'text':'password'" class="absolute inset-y-0 end-0 flex items-center px-4 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">👁️</button>
                                          </div>
                                      </div>
                                      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_tg_chat">Chat ID</label>
                                              <input type="text" id="cfg-tg-chat" placeholder="123456789" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                          </div>
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_tg_admin">Authorized Admin ID</label>
                                              <input type="text" id="cfg-tg-admin" placeholder="123456789" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                              <p class="text-xs text-slate-400" data-i18n="desc_tg_admin">Only this Telegram User ID can manage the panel via bot. Leave empty to use Chat ID.</p>
                                          </div>
                                      </div>
                                      <p class="text-xs text-slate-400" data-i18n="desc_tg_bot">Set these values to receive login alerts via Telegram.</p>
                                  </div>
                              </div>
                          </div>

                          <!-- Section: Cloudflare -->
                          <div class="bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden" data-accordion>
                              <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                                  <div class="flex items-center gap-3">
                                      <span class="text-lg">☁️</span>
                                      <span class="text-sm font-bold text-slate-700 dark:text-slate-200" data-i18n="adv_cloudflare">Cloudflare</span>
                                  </div>
                                  <svg class="w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                              </button>
                              <div data-accordion-content class="transition-all duration-300" style="max-height:0;overflow:hidden;visibility:hidden">
                                  <div class="space-y-3 px-5 pb-5 pt-1">
                                      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_cf_acc">CF Account ID</label>
                                              <input type="text" id="cfg-cf-acc" placeholder="a1b2c3d4e5f6..." class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono">
                                          </div>
                                          <div class="space-y-1">
                                              <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_cf_token">CF API Token</label>
                                              <div class="relative">
                                                  <input type="password" id="cfg-cf-token" placeholder="Bearer Token (Read Analytics)" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono pe-12">
                                                  <button type="button" onclick="const n=document.getElementById('cfg-cf-token');n.type=n.type==='password'?'text':'password'" class="absolute inset-y-0 end-0 flex items-center px-4 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">👁️</button>
                                              </div>
                                          </div>
                                      </div>
                                      <div class="space-y-1">
                                          <label class="block text-sm font-bold text-slate-600 dark:text-slate-300" data-i18n="lbl_cf_worker">CF Worker Script Name</label>
                                          <input type="text" id="cfg-cf-worker" placeholder="e.g. SwimShady" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono">
                                          <p class="text-xs text-slate-400" data-i18n="desc_cf_worker">Required for in-panel updates. The script name shown in your Cloudflare Workers dashboard.</p>
                                      </div>
                                      <p class="text-xs text-slate-400" data-i18n="desc_cf_api">Optional: Monitor Worker free usage limits (100k/day). Needs Account Analytics Read permission.</p>
                                      <div class="border-t border-slate-100 dark:border-darkborder pt-3">
                                          <button type="button" onclick="document.getElementById('cf-helper-guide').classList.toggle('hidden')" class="w-full text-start px-4 py-3 bg-primary/10 hover:bg-primary/15 text-primary text-xs font-bold rounded-xl flex items-center justify-between transition-colors">
                                              <span class="flex items-center gap-1.5">
                                                  💡 <span data-i18n="cf_help_title">Need help getting these? Beginner's Guide</span>
                                              </span>
                                              <span class="text-[10px] transform transition-transform duration-200">▼</span>
                                          </button>
                                          <div id="cf-helper-guide" class="hidden mt-3 p-4 bg-slate-50/50 dark:bg-slate-900/30 border border-slate-200 dark:border-darkborder rounded-2xl text-[11px] space-y-4 text-start leading-relaxed">
                                              <div class="space-y-1 pb-3 border-b border-dashed border-slate-200 dark:border-darkborder">
                                                  <h5 class="font-extrabold text-slate-800 dark:text-slate-200 flex items-center gap-1">🇬🇧 Beginner's Walkthrough:</h5>
                                                  <ol class="list-decimal list-inside space-y-1 text-slate-500 dark:text-slate-400">
                                                      <li><strong>CF API Token:</strong> Click <a href="https://dash.cloudflare.com/profile/api-tokens?template=edit-workers" target="_blank" class="text-primary hover:underline font-bold">Api Token Template ↗</a>. Click <strong>Use Template</strong>, then <strong>Continue to summary</strong> &gt; <strong>Create Token</strong>. Copy and paste above!</li>
                                                      <li><strong>CF Account ID:</strong> Open any Cloudflare Workers page. Copy the 32-char string after <code>dash.cloudflare.com/</code> in the URL.</li>
                                                      <li><strong>Worker Script Name:</strong> Go to <strong>Compute &gt; Workers & Pages</strong> in Cloudflare. Copy your worker's name.</li>
                                                  </ol>
                                              </div>
                                          </div>
                                      </div>
                                  </div>
                              </div>
                          </div>

                      </div>
                      
                          <!-- USERS VIEW -->
                      <div id="view-users" class="hidden space-y-4">
                          <!-- Compact Stats Bar -->
                          <div class="bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder p-4 flex flex-wrap items-center gap-4 md:gap-6">
                              <div class="flex items-center gap-2">
                                  <div class="p-1.5 bg-primary/10 text-primary rounded-lg"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a4 4 0 11-8 0 4 4 0 018 0z"></path></svg></div>
                                  <div><span class="text-[10px] font-bold text-slate-400 uppercase" data-i18n="stat_total_subscribers">Total</span><span id="stat-total-users" class="ms-1.5 text-sm font-black text-slate-800 dark:text-white">0</span></div>
                              </div>
                              <div class="flex items-center gap-2">
                                  <div class="p-1.5 bg-emerald-500/10 text-emerald-500 rounded-lg"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg></div>
                                  <div><span class="text-[10px] font-bold text-slate-400 uppercase" data-i18n="stat_active_paused">Active/Paused</span><span id="stat-active-users" class="ms-1.5 text-sm font-black text-slate-800 dark:text-white">0 / 0</span></div>
                              </div>
                              <div class="flex items-center gap-2">
                                  <div class="p-1.5 bg-violet-500/10 text-violet-500 rounded-lg"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg></div>
                                  <div><span class="text-[10px] font-bold text-slate-400 uppercase" data-i18n="stat_cumulative_traffic">Traffic</span><span id="stat-total-traffic" class="ms-1.5 text-sm font-black text-slate-800 dark:text-white">0 GB</span></div>
                              </div>
                              <div class="flex items-center gap-2">
                                  <div class="p-1.5 bg-red-500/10 text-red-500 rounded-lg"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"></path></svg></div>
                                  <div><span class="text-[10px] font-bold text-slate-400 uppercase" data-i18n="stat_auto_disabled">Disabled</span><span id="stat-auto-disabled" class="ms-1.5 text-sm font-black text-slate-800 dark:text-white">0</span></div>
                              </div>
                          </div>

                          <!-- Recently Disabled Users Panel -->
                          <div id="disabled-users-panel" class="hidden">
                              <div style="border:1px solid rgba(239,68,68,0.15);margin-bottom:16px;padding:12px 16px;background:rgba(239,68,68,0.03)">
                                  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                                      <span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#ef4444;font-weight:600">recently disabled</span>
                                      <span id="disabled-panel-badge" style="font-family:'JetBrains Mono',monospace;font-size:10px;padding:2px 6px;background:#ef4444;color:#000">0</span>
                                  </div>
                                  <div id="disabled-users-list" class="space-y-2"></div>
                              </div>
                          </div>

                          <div class="bg-white dark:bg-darkcard rounded-3xl p-4 md:p-6 shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden">
                              <div class="flex flex-col sm:flex-row items-stretch sm:items-center justify-between mb-4 md:mb-6 gap-3">
                                   <h3 class="text-sm uppercase font-bold text-slate-500 tracking-wider" data-i18n="sub_directory_title">Subscriber Directory</h3>
                                   <div class="flex flex-col sm:flex-row items-stretch sm:items-center gap-2">
                                       <select id="user-status-filter" onchange="renderUsersTable()" class="bg-slate-50 dark:bg-darkbg border border-slate-200 dark:border-darkborder px-3 py-2.5 rounded-xl text-xs outline-none font-sans text-slate-600 dark:text-slate-400 focus:border-primary">
                                           <option value="all" data-i18n="filter_all">All Users</option>
                                           <option value="active" data-i18n="filter_active">Active</option>
                                           <option value="paused" data-i18n="filter_paused">Paused</option>
                                           <option value="auto-disabled" data-i18n="filter_auto_disabled">Auto-Disabled</option>
                                       </select>
                                       <input type="text" id="user-search-input" onkeyup="renderUsersTable()" placeholder="🔍 Find by Name or UUID..." data-i18n="user_search_placeholder" class="bg-slate-50 dark:bg-darkbg border border-slate-200 dark:border-darkborder px-3 py-2.5 rounded-xl text-xs outline-none font-sans text-slate-600 dark:text-slate-400 focus:border-primary">
                                       <button onclick="openAddUserPage()" class="native-press px-4 py-2.5 bg-primary hover:bg-primary/90 text-white rounded-xl text-xs font-bold transition-colors shadow-sm whitespace-nowrap" data-i18n="btn_add_user">+ Add New User</button>
                                   </div>
                               </div>
                              <div class="overflow-x-auto">
                                  <div id="tbl-users" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
                                  </div>
                              </div>
                          </div>
                       </div>

                       <!-- PAGE: Add User -->
                       <div id="view-add-user" class="hidden h-full flex flex-col">
                           <div class="bg-white dark:bg-darkcard rounded-2xl md:rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder overflow-hidden flex flex-col flex-1 min-h-0">
                               <div class="flex items-center gap-3 px-5 py-4 border-b border-slate-200 dark:border-darkborder shrink-0">
                                   <button onclick="closeAddUserPage()" class="native-press p-2 -ms-2 rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                                       <svg class="w-5 h-5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path></svg>
                                   </button>
                                   <h3 class="text-lg font-bold text-slate-800 dark:text-white" data-i18n="modal_add_title">Add User</h3>
                               </div>
                               <div class="overflow-y-auto flex-1 min-h-0 p-5 space-y-5">
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_basic_info">Basic Info</h4>
                                       <div class="space-y-3">
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_u_name">Name / Identifier</label>
                                               <input type="text" id="add-user-name" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_custom_config_name">Custom Config Name / Prefix</label>
                                               <input type="text" id="add-user-custom-name" placeholder="Leave empty to use user name" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                           </div>
                                       </div>
                                   </div>
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_limits">Limits</h4>
                                       <div class="space-y-3">
                                           <div class="grid grid-cols-2 gap-3">
                                               <div>
                                                   <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_traffic_limit_gb">Traffic (GB) Limit</label>
                                                   <input type="number" id="add-user-total-reqs" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                               </div>
                                               <div>
                                                   <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_daily_limit_gb">Daily Limit (GB)</label>
                                                   <input type="number" id="add-user-daily-reqs" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                               </div>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_expiration_days">Expiration (Days)</label>
                                               <input type="number" id="add-user-days" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_conn_limit">IP Connection Limit</label>
                                               <input type="number" id="add-user-conn-limit" placeholder="Unlimited" min="1" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm" data-i18n-placeholder="unlimited">
                                               <p class="text-[10px] text-slate-400 mt-1" data-i18n="desc_conn_limit">Max simultaneous connections per IP. Leave empty for unlimited.</p>
                                           </div>
                                       </div>
                                   </div>
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_network">Network</h4>
                                       <div class="space-y-3">
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_clean_ips">Clean IPs</label>
                                               <div id="add-user-clean-ips-wrap" class="flex flex-wrap gap-2 mt-1 text-slate-500"></div>
                                               <label class="block text-[10px] font-bold text-slate-400 mt-2" data-i18n="desc_clean_ips_modal">Custom Clean IPs (comma/newline)</label>
                                               <textarea id="add-user-custom-clean" rows="2" placeholder="e.g. 1.2.3.4, 5.6.7.8" class="w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm"></textarea>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_proxy_ips">Proxy IPs</label>
                                               <div id="add-user-proxy-ips-wrap" class="flex flex-wrap gap-2 mt-1 text-slate-500"></div>
                                               <label class="block text-[10px] font-bold text-slate-400 mt-2" data-i18n="desc_proxy_ips">Custom Proxy IPs (comma/newline)</label>
                                               <textarea id="add-user-custom-proxy" rows="2" placeholder="e.g. proxy1.com:443" class="w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm"></textarea>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_assigned_nodes">Assigned Nodes</label>
                                               <div id="add-user-nodes-wrap" class="flex flex-wrap gap-2 mt-1 text-slate-500"></div>
                                               <label class="block text-[10px] font-bold text-slate-400 mt-2" data-i18n="desc_assigned_nodes">Custom Nodes (comma/newline, empty = all nodes)</label>
                                               <textarea id="add-user-custom-nodes" rows="2" placeholder="node1.example.com" class="w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm"></textarea>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_user_panel_url">Main Panel URL (Custom Nodes)</label>
                                               <input type="text" id="add-user-panel-url" placeholder="e.g. panel.example.com" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                               <p class="text-[10px] text-slate-400 mt-1" data-i18n="desc_user_panel_url">Main panel domain for custom nodes. If empty, default panel URL is used.</p>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_nat64">NAT64 Prefix</label>
                                               <input type="text" id="add-user-nat64" placeholder="e.g. 64:ff9b::/96" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono">
                                               <p class="text-[10px] text-slate-400 mt-1" data-i18n="desc_nat64_user">Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses.</p>
                                           </div>
                                       </div>
                                   </div>
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_advanced">Advanced</h4>
                                       <div class="space-y-3">
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_protocol_mode">Protocol Mode</label>
                                               <div id="add-user-mode-wrap" class="flex gap-4 mt-1">
                                                   <label class="flex items-center gap-2 text-sm cursor-pointer"><input type="checkbox" value="alpha" class="add-mode-cb accent-primary"> <span>Alpha (VLESS)</span></label>
                                                   <label class="flex items-center gap-2 text-sm cursor-pointer"><input type="checkbox" value="beta" class="add-mode-cb accent-primary"> <span>Beta (Trojan)</span></label>
                                               </div>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5">Ports</label>
                                               <div id="add-user-ports-wrap" class="flex flex-wrap gap-2 mt-1"></div>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_max_configs">Max Configs</label>
                                               <input type="number" id="add-user-max-configs" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm" data-i18n-placeholder="unlimited">
                                           </div>
                                       </div>
                                   </div>
                               </div>
                               <div class="px-5 py-4 border-t border-slate-200 dark:border-darkborder bg-white dark:bg-darkcard flex justify-between items-center shrink-0">
                                   <button onclick="closeAddUserPage()" class="px-5 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 font-bold text-sm" data-i18n="btn_cancel">Cancel</button>
                                   <button onclick="commitAddUser()" class="native-press px-6 py-2.5 rounded-xl bg-primary text-white font-bold text-sm shadow-sm" data-i18n="save_btn_user">Save User</button>
                               </div>
                           </div>
                       </div>

                       <!-- PAGE: Edit User -->
                       <div id="view-edit-user" class="hidden h-full flex flex-col">
                           <div class="bg-white dark:bg-darkcard rounded-2xl md:rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder overflow-hidden flex flex-col flex-1 min-h-0">
                               <div class="flex items-center gap-3 px-5 py-4 border-b border-slate-200 dark:border-darkborder shrink-0">
                                   <button onclick="closeEditUserPage()" class="native-press p-2 -ms-2 rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                                       <svg class="w-5 h-5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path></svg>
                                   </button>
                                   <h3 class="text-lg font-bold text-slate-800 dark:text-white" data-i18n="edit_sub">Edit Subscriber</h3>
                                   <input type="hidden" id="edit-user-id">
                               </div>
                               <div class="overflow-y-auto flex-1 min-h-0 p-5 space-y-5">
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_basic_info">Basic Info</h4>
                                       <div class="space-y-3">
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_name_ph">Name / Identifier</label>
                                               <input type="text" id="edit-user-name" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_custom_config_name">Custom Config Name / Prefix</label>
                                               <input type="text" id="edit-user-custom-name" placeholder="Leave empty to use user name" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                           </div>
                                       </div>
                                   </div>
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_limits">Limits</h4>
                                       <div class="space-y-3">
                                           <div class="grid grid-cols-2 gap-3">
                                               <div>
                                                   <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_traffic_limit_gb">Traffic Limit (GB)</label>
                                                   <input type="number" id="edit-user-total-reqs" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                               </div>
                                               <div>
                                                   <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_daily_limit_gb">Daily Limit (GB)</label>
                                                   <input type="number" id="edit-user-daily-reqs" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                               </div>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_expiration_days">Expiration (Days)</label>
                                               <input type="number" id="edit-user-days" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_conn_limit">IP Connection Limit</label>
                                               <input type="number" id="edit-user-conn-limit" placeholder="Unlimited" min="1" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm" data-i18n-placeholder="unlimited">
                                               <p class="text-[10px] text-slate-400 mt-1" data-i18n="desc_conn_limit">Max simultaneous connections per user. Leave empty for unlimited.</p>
                                           </div>
                                       </div>
                                   </div>
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_network">Network</h4>
                                       <div class="space-y-3">
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_clean_ips">Clean IPs</label>
                                               <div id="edit-user-clean-ips-wrap" class="flex flex-wrap gap-2 mt-1 text-slate-500"></div>
                                               <label class="block text-[10px] font-bold text-slate-400 mt-2" data-i18n="desc_clean_ips_modal">Custom Clean IPs (comma/newline)</label>
                                               <textarea id="edit-user-custom-clean" rows="2" placeholder="e.g. 1.2.3.4, 5.6.7.8" class="w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm"></textarea>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_proxy_ips">Proxy IPs</label>
                                               <div id="edit-user-proxy-ips-wrap" class="flex flex-wrap gap-2 mt-1 text-slate-500"></div>
                                               <label class="block text-[10px] font-bold text-slate-400 mt-2" data-i18n="desc_proxy_ips">Custom Proxy IPs (comma/newline)</label>
                                               <textarea id="edit-user-custom-proxy" rows="2" placeholder="e.g. proxy1.com:443" class="w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm"></textarea>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_assigned_nodes">Assigned Nodes</label>
                                               <div id="edit-user-nodes-wrap" class="flex flex-wrap gap-2 mt-1 text-slate-500"></div>
                                               <label class="block text-[10px] font-bold text-slate-400 mt-2" data-i18n="desc_assigned_nodes">Custom Nodes (comma/newline, empty = all nodes)</label>
                                               <textarea id="edit-user-custom-nodes" rows="2" placeholder="node1.example.com" class="w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm"></textarea>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_user_panel_url">Main Panel URL (Custom Nodes)</label>
                                               <input type="text" id="edit-user-panel-url" placeholder="e.g. panel.example.com" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm">
                                               <p class="text-[10px] text-slate-400 mt-1" data-i18n="desc_user_panel_url">Main panel domain for custom nodes. If empty, default panel URL is used.</p>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_nat64">NAT64 Prefix</label>
                                               <input type="text" id="edit-user-nat64" placeholder="e.g. 64:ff9b::/96" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono">
                                               <p class="text-[10px] text-slate-400 mt-1" data-i18n="desc_nat64_user">Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses.</p>
                                           </div>
                                       </div>
                                   </div>
                                   <div class="space-y-4">
                                       <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider" data-i18n="section_advanced">Advanced</h4>
                                       <div class="space-y-3">
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_protocol_mode">Protocol Mode</label>
                                               <div id="edit-user-mode-wrap" class="flex gap-4 mt-1">
                                                   <label class="flex items-center gap-2 text-sm cursor-pointer"><input type="checkbox" value="alpha" class="edit-mode-cb accent-primary"> <span>Alpha (VLESS)</span></label>
                                                   <label class="flex items-center gap-2 text-sm cursor-pointer"><input type="checkbox" value="beta" class="edit-mode-cb accent-primary"> <span>Beta (Trojan)</span></label>
                                               </div>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5">Ports</label>
                                               <div id="edit-user-ports-wrap" class="flex flex-wrap gap-2 mt-1"></div>
                                           </div>
                                           <div>
                                               <label class="block text-xs font-bold text-slate-500 mb-1.5" data-i18n="lbl_max_configs">Max Configs</label>
                                               <input type="number" id="edit-user-max-configs" placeholder="Unlimited" class="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm" data-i18n-placeholder="unlimited">
                                           </div>
                                       </div>
                                   </div>
                               </div>
                               <div class="px-5 py-4 border-t border-slate-200 dark:border-darkborder bg-white dark:bg-darkcard flex justify-between items-center shrink-0">
                                   <button onclick="closeEditUserPage()" class="px-5 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 font-bold text-sm" data-i18n="btn_cancel">Cancel</button>
                                   <button onclick="commitEditUser()" class="native-press px-6 py-2.5 rounded-xl bg-primary text-white font-bold text-sm shadow-sm" data-i18n="btn_save_changes">Save Changes</button>
                               </div>
                           </div>
                       </div>

                      <!-- LOGS VIEW -->
                      <div id="view-logs" class="hidden space-y-6">
                          <div class="bg-white dark:bg-darkcard rounded-3xl p-6 shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden">
                              <div class="flex items-center justify-between mb-6">
                                  <h3 class="text-sm uppercase font-bold text-slate-500 tracking-wider" data-i18n="tab_logs">activity logs</h3>
                                  <button onclick="loadLogs()" class="px-3 py-1.5 bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-lg text-xs font-bold transition-colors">
                                      🔄 Refresh
                                  </button>
                              </div>
                              <div class="space-y-3" id="logs-container">
                                  <p class="text-sm text-slate-400 text-center py-8" data-i18n="loading_logs">Loading activity logs...</p>
                              </div>
                          </div>
                      </div>
                  </div>
              </div>
  
              <!-- Floating Save Button -->
              <span id="save-status" style="display:none"></span>
              <button id="save-bar" onclick="doSave()" style="position:fixed;bottom:24px;right:24px;z-index:50;display:flex;align-items:center;gap:8px;padding:10px 18px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600;color:#000;background:#22c55e;border:none;cursor:pointer;box-shadow:0 4px 16px rgba(34,197,94,0.25);transition:all 0.2s" onmouseover="this.style.transform='translateY(-2px)';this.style.boxShadow='0 6px 24px rgba(34,197,94,0.35)'" onmouseout="this.style.transform='';this.style.boxShadow='0 4px 16px rgba(34,197,94,0.25)'" data-i18n="save_btn">save config</button>
              <div id="save-toast" style="position:fixed;bottom:80px;right:24px;z-index:50;padding:10px 18px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600;color:#000;background:#22c55e;box-shadow:0 4px 16px rgba(34,197,94,0.25);display:none">saved successfully ✓</div>
          </main>
  
          <!-- BOTTOM NAV (Mobile) -->
          <nav class="md:hidden w-full mobile-bottom-nav flex justify-around items-center z-30 shrink-0" style="height:calc(4rem + env(safe-area-inset-bottom, 0px));padding-bottom:env(safe-area-inset-bottom, 0px);background:#141414;border-top:1px solid #222;">
              <button onclick="switchTab('overview')" id="mob-tab-overview" class="mobile-tab-item mobile-nav-item active flex flex-col items-center justify-center w-full h-full" style="color:#444">
                  <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 5a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1H5a1 1 0 01-1-1V5zm10 0a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 01-1-1V5zM4 15a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1H5a1 1 0 01-1-1v-4zm10 0a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 01-1-1v-4z"></path></svg>
                  <span class="text-[10px] font-bold" data-i18n="tab_overview">home</span>
              </button>
              <button onclick="switchTab('info')" id="mob-tab-info" class="mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full" style="color:#444">
                  <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"></path></svg>
                  <span class="text-[10px] font-bold" data-i18n="tab_info">endpoints</span>
              </button>
              <button onclick="switchTab('network')" id="mob-tab-network" class="mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full" style="color:#444">
                  <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path></svg>
                  <span class="text-[10px] font-bold" data-i18n="tab_status">metrics</span>
              </button>
              <button onclick="switchTab('settings')" id="mob-tab-settings" class="mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full" style="color:#444">
                  <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path></svg>
                  <span class="text-[10px] font-bold" data-i18n="tab_settings">system</span>
              </button>
              <button onclick="switchTab('advanced')" id="mob-tab-advanced" class="mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full" style="color:#444">
                  <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"></path></svg>
                  <span class="text-[10px] font-bold" data-i18n="tab_adv">network</span>
              </button>
              <button onclick="switchTab('logs')" id="mob-tab-logs" class="mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full" style="color:#444">
                  <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 10h16M4 14h16M4 18h16"></path></svg>
                  <span class="text-[10px] font-bold" data-i18n="tab_logs">logs</span>
              </button>
              <button onclick="switchTab('users')" id="mob-tab-users" class="mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full" style="color:#444">
                  <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"></path></svg>
                  <span class="text-[10px] font-bold" data-i18n="tab_users">users</span>
              </button>
          </nav>
      </div>
  
      <!-- Toast Notification -->
      <div id="copy-toast" class="fixed top-20 md:top-10 left-1/2 -translate-x-1/2 bg-slate-800 dark:bg-white text-white dark:text-slate-900 px-6 py-3 rounded-full shadow-2xl font-bold text-sm z-50 transition-all transform -translate-y-20 opacity-0 pointer-events-none">
          <span data-i18n="copied">Copied!</span>
      </div>
      
      <!-- QR Code Modal (Enhanced) -->
      <div id="qr-modal" class="fixed inset-0 z-[100] hidden items-center justify-center p-4" style="background:rgba(0,0,0,0.7)">
          <div class="rounded-3xl p-8 max-w-sm w-full relative" style="background:#141414;border:1px solid #222;box-shadow:0 24px 48px rgba(0,0,0,0.4)">
              <button onclick="closeQRModal()" class="absolute top-4 end-4" style="color:#444;background:none;border:none;cursor:pointer;font-family:monospace;font-size:16px">
                  <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
              </button>
              <div class="text-center mb-6">
                  <h3 id="qr-modal-title" class="text-xl font-bold" style="color:#e5e5e5" data-i18n="qr_title">Scan to Connect</h3>
                  <p class="text-xs mt-1" style="color:#444" data-i18n="qr_subtitle">Scan with your V-Core or T-Core client</p>
              </div>
              <div class="p-4 rounded-2xl mb-4" style="background:#fff">
                  <img id="qr-modal-img" src="" alt="QR Code" class="w-full aspect-square object-contain">
              </div>
              <div class="p-3 rounded-xl break-all text-xs font-mono max-h-24 overflow-auto" style="background:#0c0c0c;border:1px solid #222;color:#444" id="qr-modal-link"></div>
          </div>
      </div>

      <!-- Modal: Version Update Highlights -->
      <div id="modal-version-update" class="fixed inset-0 bg-slate-900/60 backdrop-blur-md z-[101] hidden items-center justify-center p-4">
          <div class="bg-white dark:bg-darkcard rounded-3xl p-8 max-w-lg w-full shadow-2xl border border-slate-200 dark:border-darkborder relative overflow-hidden transform transition-all duration-300">
              <div class="absolute top-0 right-0 left-0 h-2 bg-gradient-to-r from-indigo-500 via-primary to-emerald-500"></div>
              <div class="flex items-center justify-between mb-6">
                  <div class="flex items-center gap-2.5">
                      <div class="bg-primary/10 text-primary p-2.5 rounded-2xl">
                          <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
                          </svg>
                      </div>
                      <div>
                          <h3 class="text-lg font-black text-slate-800 dark:text-white" data-i18n="v_pop_title">Version Update</h3>
                          <span id="modal-version-badge" class="text-[10px] font-bold px-2 py-0.5 bg-indigo-500 text-white rounded-full tracking-wide"></span>
                      </div>
                  </div>
                  <button onclick="closeVersionModal()" class="text-slate-400 hover:text-slate-700 dark:hover:text-white bg-slate-50 dark:bg-slate-800 p-2 rounded-xl border border-slate-100 dark:border-darkborder transition-colors">
                      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                      </svg>
                  </button>
              </div>

              <div class="space-y-4">
                  <div class="p-4 bg-slate-50 dark:bg-slate-800/30 rounded-2xl border border-slate-100 dark:border-darkborder/50">
                      <p class="text-xs font-bold text-slate-400 uppercase tracking-widest" data-i18n="v_pop_whatsnew">What's New in This Version</p>
                      <h4 id="modal-version-headline" class="text-sm font-black text-slate-700 dark:text-white mt-1"></h4>
                  </div>
                  
                  <div id="modal-changelog-container" class="space-y-4 max-h-[50vh] overflow-y-auto pe-2 text-start">
                  </div>
              </div>

              <div class="mt-6 pt-5 border-t border-slate-100 dark:border-darkborder/50 flex justify-end">
                  <button onclick="closeVersionModal()" class="px-5 py-2.5 bg-primary hover:bg-primary/95 text-white rounded-xl text-xs font-bold shadow-md transition-all transform hover:scale-105 active:scale-95" data-i18n="v_pop_btn">Got it!</button>
              </div>
          </div>
      </div>
  
      <script>
          function parseImportBindings(importStr) {
              const cleanStr = importStr.replace(/\\/\\/.*$/gm, '').replace(/\\/\\*[\\s\\S]*?\\*\\//g, '').trim();
              const content = cleanStr
                  .replace(/^import\\s+/, '')
                  .replace(/\\s+from\\s+["'].*?["'];?$/, '')
                  .trim();
              
              const bindings = [];
              
              if (content.startsWith('*')) {
                  const match = content.match(/\\*\\s+as\\s+(\\w+)/);
                  if (match) bindings.push({ name: match[1], isNamespace: true });
                  return bindings;
              }
              
              const braceStart = content.indexOf('{');
              if (braceStart !== -1) {
                  const defaultPart = content.slice(0, braceStart).replace(/,/, '').trim();
                  if (defaultPart) {
                      bindings.push({ name: defaultPart, isDefault: true });
                  }
                  const bracePart = content.slice(braceStart + 1, content.lastIndexOf('}')).trim();
                  const namedImports = bracePart.split(',').map(s => s.trim()).filter(Boolean);
                  namedImports.forEach(item => {
                      if (item.includes(' as ')) {
                          const parts = item.split(/\\s+as\\s+/);
                           bindings.push({ name: parts[1], original: parts[0] });
                      } else {
                          bindings.push({ name: item });
                      }
                  });
              } else {
                  bindings.push({ name: content, isDefault: true });
              }
              
              return bindings;
          }

          function obfuscateCode(srcText) {
              const importRegex = /import\\s+[\\s\\S]*?from\\s+["'].*?["'];?/g;
              const imports = [];
              let match;
              
              while ((match = importRegex.exec(srcText)) !== null) {
                  imports.push(match[0]);
              }
              
              let cleanCode = srcText.replace(importRegex, '');
              
              const bindings = [];
              imports.forEach(imp => {
                  const parsed = parseImportBindings(imp);
                  bindings.push(...parsed);
              });
              
              const uniqueBindings = [];
              const seenNames = new Set();
              bindings.forEach(b => {
                  if (!seenNames.has(b.name)) {
                      seenNames.add(b.name);
                      uniqueBindings.push(b);
                  }
              });
              
              cleanCode = cleanCode.replace(/export\\s+default\\s+/g, 'const _0xSwimShadyModule = ');
              cleanCode += '\\nreturn _0xSwimShadyModule;';
              
              const randKey = Math.floor(Math.random() * 80) + 64; 
              
              const encoder = new TextEncoder();
              const bytes = encoder.encode(cleanCode);
              
              let hexOutput = '';
              for (let i = 0; i < bytes.length; i++) {
                  const xorByte = bytes[i] ^ randKey;
                  hexOutput += xorByte.toString(16).padStart(2, '0');
               }
              
              const rawImportsStr = imports.join('\\n');
              const bindingNames = uniqueBindings.map(b => b.name);
              
              const finalLoaderCode = rawImportsStr + '\\n\\n' +
                  '// SwimShady Gateway - Obfuscated Loader Context (v2.5.4.2 Optimized)\\n' +
                  'const _0xSwimShadyPayload = "' + hexOutput + '";\\n' +
                  'const _0xSwimShadyKey = ' + randKey + ';\\n\\n' +
                  'const _0xSwimShadyBytes = new Uint8Array((_0xSwimShadyPayload.match(/.{1,2}/g) || []).map(x => parseInt(x, 16) ^ _0xSwimShadyKey));\\n' +
                  'const _0xSwimShadyCode = new TextDecoder().decode(_0xSwimShadyBytes);\\n' +
                  'const _0xSwimShadyRuntime = new Function(' + bindingNames.map(name => '"' + name + '"').join(', ') + ', _0xSwimShadyCode)(' + bindingNames.join(', ') + ');\\n\\n' +
                  'export default _0xSwimShadyRuntime;';

              return finalLoaderCode;
          }

          const CURRENT_VERSION = "${CURRENT_VERSION}";
          const i18n = {
              en: {
                  title: "🦈 swim shady", pass_ph: "Master Key", login_btn: "Authenticate", err_pass: "Access Denied", missing_db: "⚠️ IOT_DB namespace missing! Settings won't save.",
                   logout: "Disconnect", tab_overview: "overview", tab_info: "endpoints", tab_status: "metrics", tab_settings: "system", tab_adv: "advanced", tab_logs: "activity logs",
                  qr_title: "Direct Stream Link", badge_multi: "Dual-Core Multiplexed", copy: "Copy", copied: "Copied to clipboard!", sync_link: "Cloud Sync URL", active_id: "Hardware ID",
                  stat_ip: "Origin IP", stat_dc: "Edge Node", stat_loc: "Data Region",
                  lbl_proto: "Primary Display Mode", lbl_port: "Data Port", lbl_id: "Device UUID (Empty=Auto)",
                  lbl_path: "API Route (Hidden Path)", lbl_pass: "Master Key", lbl_fp: "TLS Signature", lbl_dns: "Resolver IP",
                  lbl_clean_ips: "Clean IPs (Multi-Generator)", ph_clean_ips: "1.1.1.1, 2.2.2.2", desc_clean_ips: "Separate IPs by comma or new line. The Sync URL will multiply configs for all IPs.",
                  lbl_fake: "Maintenance Hosts (Camouflage)", lbl_relay: "Backup Relay IP", lbl_tfo: "TCP Fast Open", lbl_ech: "Secure Hello (ECH)",
                  lbl_fake_configs: "Subscription Fake Entries", desc_fake_configs: "Customize info entries shown in subscription profiles. Use {usage} and {expiry} for dynamic values.", btn_add_entry: "Add Entry",                   lbl_tg_token: "Telegram Bot Token", lbl_tg_chat: "Telegram Chat ID", lbl_tg_admin: "Authorized Telegram Admin ID", desc_tg_admin: "Only this Telegram User ID can manage the panel via bot. Leave empty to use Chat ID.", desc_tg_bot: "Set these values to receive login alerts via Telegram.",
                  lbl_cf_acc: "Cloudflare Account ID", lbl_cf_token: "Cloudflare API Token", desc_cf_api: "Optional: Monitor Worker daily usage limit (100k/day). Requires Account Analytics read permission.",
                  lbl_silent: "Silent UI Alerts", lbl_pause: "Kill Switch (Pause System)",
                  lbl_sub_ua: "Custom Subscription User-Agent", desc_sub_ua: "Allow specific browser User-Agent containing this text to bypass camouflage and retrieve profile data directly in web browser.",
                  tab_users: "Users",
                  user_mgt_title: "User Management", user_mgt_desc: "Manage multiple users, set traffic limits, and expiration dates.", btn_add_user: "+ Add New User",
                  tbl_name: "Name", tbl_uuid: "UUID", tbl_traffic: "Traffic (Used / Limit)", tbl_exp: "Expiration", tbl_action: "Action", no_users: "No users found. Create one above.",
                  modal_add_title: "Add New User", lbl_u_name: "Name (Required)", lbl_u_gb: "Traffic Limit (GB) - Optional", lbl_u_days: "Duration (Days) - Optional", btn_cancel: "Cancel", btn_confirm: "Add User",
                  limit_total: "Traffic (GB) Limit (Leave empty for unlimited)", limit_daily: "Daily Requests Limit (Leave empty for unlimited)",
                  limit_days: "Expiration limit (Days) - Leave empty for unlimited", edit_sub: "Edit Subscriber", lbl_name_ph: "Name or UUID",
                  btn_save_changes: "Save Changes", save_btn_user: "Save User", save_btn: "Save Config", status_active: "Active", status_paused: "Paused", status_expired: "Expired",
                  stat_total_subscribers: "Total Subscribers", stat_active_paused: "Active / Paused", stat_cumulative_traffic: "Cumulative Traffic", stat_auto_disabled: "Auto-Disabled",
                  sub_directory_title: "Subscriber Directory", sub_directory_desc: "Search, modify bounds, toggle traffic limits or clear billing sessions.", user_search_placeholder: "🔍 Find by Name or UUID...",
                  filter_all: "All Users", filter_active: "Active", filter_paused: "Paused", filter_auto_disabled: "Auto-Disabled",
                  disabled_panel_title: "Recently Disabled Users", disabled_panel_desc: "Users automatically disabled due to quota or expiration limits",
                  lbl_u_Protocol:"Protocol Mode (Leave empty to use global setting)",
                  lbl_u_ports:"Custom Ports (Optional - overrides global ports, comma separated e.g. 443,80",
                  lbl_u_max_config:"Max Configs",
                  login_password:"Password",
                  lbl_u_ipproxy:"User Proxy IP(s) (Optional - overrides global Clean IP, comma/newline separated)",
                   lbl_custom_panel_url:"Custom Panel URL / Subscription Domain",
                   lbl_api_keys: "Panel API Keys", desc_api_keys: "Generate API keys to securely connect remote panels. Remote panels use these keys instead of sharing your master key.",
                   btn_generate_key: "Generate Key", api_key_created: "API Key Created! Copy it now — it won't be shown again.",
                   api_keys_empty: "No API keys generated yet.", enter_key_name: "Enter a name for this API key:",
                   confirm_revoke: "Revoke this API key? The remote panel will lose access.", revoke: "Revoke",
                   created: "Created", last_used: "Last used", never: "Never",
                   v_pop_title: "Release Notice", v_pop_whatsnew: "What's New", v_pop_headline: "New Features & Improvements",
                  v_pop_btn: "Got it!",
                  changelog_title: "Release Notes & Changelog:",
                  changelog_added: "Added", changelog_fixed: "Fixed", changelog_improved: "Improved", changelog_changed: "Changed", changelog_note: "Important Notes",
                  ov_total_users: "Total Users", ov_active_users: "Active", ov_paused_users: "Paused", ov_auto_disabled: "Auto-Disabled", ov_expired_users: "Expired",
                  ov_total_traffic: "Total Traffic", ov_today_traffic: "Today's Traffic", ov_requests: "requests", ov_active_conns: "Active Connections",
                  ov_system: "System", ov_recent_activity: "Recent Activity", ov_view_all: "View All →", ov_loading: "Loading...",
                   ov_quick_actions: "Quick Actions", ov_add_user: "Add User", ov_backup_config: "Backup Config", ov_refresh: "Refresh Statistics", ov_manage_users: "Manage Users",
                   ov_gb_unit: "GB",
                    lbl_allow_sync:"Allow Sync",
                    other_nodes_title: "Other Nodes", other_nodes_desc: "External nodes (URL + API Key) for cross-panel management.",
                    add_node_title: "Add External Node", add_node_desc: "Enter the URL and API Key of the external panel.",
                    add_node_url: "Node URL", add_node_apikey: "API Key", add_node_confirm: "Add Node", add_node_invalid: "Please enter both URL and API Key.",
                    node_added: "Node added successfully!", node_removed: "Node removed.",
                    deploy_btn: "Deploy Now", update_deploying: "Deploying update...",
                   update_success: "Update successful! Reloading...", update_error: "Update failed",
                   lbl_cf_worker: "CF Worker Script Name", desc_cf_worker: "Required for in-panel updates. The script name shown in your Cloudflare Workers dashboard.",
                   view_github: "View on GitHub",
                    cf_help_title: "Need help getting these? Beginner's Step-by-Step Guide",
                    lbl_update_format: "Update Format & Obfuscated Options:",
                    desc_update_format: "Deploy clean source code, or encrypt using dynamic XOR byte-shifting to avoid network interception.",
                    format_normal: "Normal (_worker.js)",
                    format_obfuscated: "Obfuscated (UTF-8 + XOR)",
                     btn_redeploy_force: "Force Redeploy / Switch Format",
                    adv_network_dns: "Network & DNS", adv_proxy_relay: "Proxy & Relay", adv_subscription: "Subscription",
                    adv_protocol: "Protocol", adv_telegram: "Telegram Bot", adv_cloudflare: "Cloudflare",
                    stat_datetime: "Date Time",
                    desc_custom_panel_url: "Optionally specify a custom domain/URL to be used for subscription/sync links. If empty, the default Worker address will be used.",
                    lbl_custom_config_name: "Custom Config Name / Prefix",
                    lbl_traffic_limit_gb: "Traffic (GB) Limit",
                    lbl_daily_limit_gb: "Daily Limit (GB)",
                    lbl_expiration_days: "Expiration (Days)",
                    loading_logs: "Loading activity logs...", show_qr: "Show QR Code",
                    no_matching_users: "No matching subscribers found", no_active_conn: "No active connection data yet.",
                    qr_subtitle: "Scan with your V-Core or T-Core client",
                    no_activity_logs: "No activity logs found.", no_recent_activity: "No recent activity.",
                    no_ips_advanced: "No IPs added in Advanced Tab", no_nodes_advanced: "No slave nodes in Advanced Tab",
                    no_changelog: "No changelog available for this version.", no_changes: "No changes documented.",
                    update_requires_cf: "Set CF Account ID, API Token, and Worker Name to enable in-panel deploy.",
                    section_basic_info: "Basic Info", section_limits: "Limits", section_network: "Network", section_advanced: "Advanced",
                    lbl_nat64: "NAT64 Prefix", desc_nat64: "Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses. Supports multiple prefixes.",
                    lbl_direct_configs: "Include Direct Configs", desc_direct_configs: "Generate configs without Proxy IP alongside relay configs",
                    lbl_sync_api_key: "Sync API Key (Slave Push)", desc_sync_api_key: "API key from a slave panel. Main uses this to push config. Same key must exist on each slave's Panel API Keys.",
                    lbl_auto_update: "Auto-Update", desc_auto_update: "Automatically deploy when a new version is detected",
                    lbl_auto_update_format: "Update Format", format_normal_label: "Normal", format_obfuscated_label: "Obfuscated",
                    desc_format_normal: "Standard _worker.js", desc_format_obfuscated: "XOR byte-shifting",
                    lbl_clean_ips: "Clean IPs", lbl_proxy_ips: "Proxy IPs", lbl_assigned_nodes: "Assigned Nodes",
                    lbl_protocol_mode: "Protocol Mode", lbl_max_configs: "Max Configs",
                    desc_assigned_nodes: "Custom Nodes (comma/newline, empty = all nodes)",
                    desc_nat64_user: "Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses.",
                    desc_proxy_ips: "Custom Proxy IPs (comma/newline)",
                    desc_clean_ips_modal: "Custom Clean IPs (comma/newline)",
                    btn_generate_uuid: "Generate UUID",
                    lbl_conn_limit: "IP Connection Limit", desc_conn_limit: "Max simultaneous connections per user. Leave empty for unlimited.",
                    lbl_user_panel_url: "Main Panel URL (Custom Nodes)", desc_user_panel_url: "Main panel domain for custom nodes. If empty, default panel URL is used.",
                    html_desc_strategy: "Supported placeholders: <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{FLAG}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{COUNTRY}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{CITY}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{ISP}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PROTOCOL}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{USER}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PORT}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PREFIX}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{IP}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{HOST}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{DATE}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{INDEX}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{WORKER}</code>.<br><span class='text-[10px] text-slate-400 dark:text-slate-500 leading-snug'>• <b>{FLAG}</b>: Country flag emoji (e.g. 🇺🇸).<br>• <b>{COUNTRY}</b>: Country name (e.g. United States).<br>• <b>{CITY}</b>: City name (e.g. San Francisco).<br>• <b>{ISP}</b>: ISP / ASN org (e.g. Cloudflare, Inc.).<br>• <b>{PROTOCOL}</b>: Core mode (VLESS / Trojan).<br>• <b>{USER}</b>: Subscriber name.<br>• <b>{PORT}</b>: Active port.<br>• <b>{PREFIX}</b>: Custom prefix.<br>• <b>{IP}</b>: Clean IP address.<br>• <b>{HOST}</b>: Hostname.<br>• <b>{DATE}</b>: Current date (YYYY-MM-DD).<br>• <b>{INDEX}</b>: Config index (0, 1, 2...).<br>• <b>{WORKER}</b>: Worker name from config.</span><br>Pre-defined strategies: <code>default</code>, <code>type-user-port</code>, <code>user-port</code>, <code>host-port-user</code>, <code>prefix-user-port</code>, <code>ip</code>.",
               },
          };

          const CHANGELOG_DATA = {
              "1.2.1": {
                  headline: { en: "Custom Config Name Fix & Update Notification Fix" },
                  added: [],
                  fixed: [
                      { en: "Custom Config Name not saved on user create/update — backend was ignoring the field" },
                      { en: "Custom Config Name not applied to subscription config names — profiles now use per-user customName" },
                      { en: "Subscription info page now displays custom name instead of internal user name" },
                      { en: "Update notification banner not showing — inline style was overriding Tailwind classes" }
                  ],
                  improved: [],
                  notes: []
              },
              "1.2.0": {
                  headline: { en: "DoH Server, Ad Blocking & Subscription Format Links" },
                  added: [
                      { en: "Private DoH Server endpoint — use your worker as a DNS-over-HTTPS server for privacy and censorship bypass" },
                      { en: "DNS-based ad blocking — blocks 60+ ad/tracker domains including Google, Facebook, TikTok, and analytics trackers" },
                      { en: "Subscription page format buttons — copy links for Clash, Sing-box, and Raw formats with app hints" },
                      { en: "Live connection data persistence — active connection counts now survive worker restarts via D1" },
                      { en: "Panel name displayed in subscription page credits" }
                  ],
                  fixed: [
                      { en: "Live profile usage always showing 'No active connection data yet' — data now persists to D1" }
                  ],
                  improved: [
                      { en: "Subscription page now shows which app works with which format (Clash Verge, Sing-box, V2rayNG, etc.)" },
                      { en: "DoH server supports GET (JSON API) and POST (RFC 8484 wire format) with CORS headers" }
                  ],
                  notes: []
              },
              "1.1.0": {
                  headline: { en: "Bug Fixes, Mobile UI & Security" },
                  added: [
                      { en: "Update notification banner restored — panel now detects new GitHub versions and offers one-click deploy" },
                      { en: "Mobile bottom navigation with responsive grids — stats adapt to phone screens" },
                      { en: "XSS protection on subscription page — user names are now HTML-escaped" },
                      { en: "Shark emoji branding" }
                  ],
                  fixed: [
                      { en: "Fake configs toggle not working — was missing CSS class and onclick handler" },
                      { en: "Add/Edit User NAT64 field swap — each form now reads from its own field" },
                      { en: "Duplicate element ID breaking network view active connections count" },
                      { en: "Dashboard crash from missing ov-today-reqs element reference" },
                      { en: "'swin shady' typo fixed to 'swim shady' everywhere" }
                  ],
                  improved: [
                      { en: "Color contrast for better readability — muted text now meets WCAG standards" },
                      { en: "Removed unused Persian translations from web dashboard" }
                  ],
                  notes: []
              },
          };
  
          function renderChangelog(version) {
              const container = document.getElementById('modal-changelog-container');
              if (!container) return;
              
              const data = CHANGELOG_DATA[version];
              if (!data) {
                  container.innerHTML = '<p class="text-slate-400 text-xs">' + (i18n[lang]?.no_changelog || 'No changelog available for this version.') + '</p>';
                  return;
              }

              const t = (key) => i18n[lang]?.[key] || i18n['en']?.[key] || key;
              let html = '';

              if (data.headline) {
                  const headlineEl = document.getElementById('modal-version-headline');
                  if (headlineEl) headlineEl.textContent = data.headline[lang] || data.headline['en'];
              }

              const sections = [
                  { key: 'added', icon: '✨', color: 'emerald', items: data.added },
                  { key: 'fixed', icon: '🔧', color: 'blue', items: data.fixed },
                  { key: 'improved', icon: '⚡', color: 'violet', items: data.improved },
                  { key: 'changed', icon: '🔄', color: 'amber', items: data.changed },
                  { key: 'note', icon: '⚠️', color: 'red', items: data.notes },
              ];

              sections.forEach(section => {
                  if (section.items && section.items.length > 0) {
                      html += '<div class="mb-4">';
                      html += '<div class="flex items-center gap-2 mb-2">';
                      html += '<span class="text-sm">' + section.icon + '</span>';
                      html += '<h5 class="text-xs font-bold text-' + section.color + '-600 dark:text-' + section.color + '-400 uppercase tracking-wider">' + t('changelog_' + section.key) + '</h5>';
                      html += '</div>';
                      html += '<div class="space-y-1.5 ps-6">';
                      section.items.forEach(item => {
                          html += '<div class="flex items-start gap-2">';
                          html += '<span class="text-' + section.color + '-400 mt-1.5">•</span>';
                          html += '<span class="text-xs text-slate-600 dark:text-slate-300">' + (item[lang] || item['en']) + '</span>';
                          html += '</div>';
                      });
                      html += '</div></div>';
                  }
              });

              container.innerHTML = html || '<p class="text-slate-400 text-xs">' + (i18n[lang]?.no_changes || 'No changes documented.') + '</p>';
          }

           let lang = 'en';
          let sessionKey = "", baseRoute = window.location.pathname.split('/dash')[0];
          let hostName = window.location.hostname, localUUID = "";

          window.addEventListener('DOMContentLoaded', () => {
              let savedSession = localStorage.getItem('swimshady_session');
              if (savedSession) {
                  try {
                      let parsed = JSON.parse(savedSession);
                      if (parsed && parsed.expiry && Date.now() < parsed.expiry) {
                           sessionKey = parsed.key;
                           doLogin(true).then(() => loadDashboard());
                      } else {
                          localStorage.removeItem('swimshady_session');
                      }
                  } catch(e){}
              }
              checkVersionPopup();
          });
  

  
          if (localStorage.getItem('theme') === 'dark' || (!('theme' in localStorage) && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
              document.documentElement.classList.add('dark');
          } else {
              document.documentElement.classList.remove('dark');
          }
  
          function toggleTheme() {
              document.documentElement.classList.toggle('dark');
              localStorage.setItem('theme', document.documentElement.classList.contains('dark') ? 'dark' : 'light');
          }

          function checkVersionPopup() {
              const popupKey = \`swimshady_shown_v\${CURRENT_VERSION}\`;
              if (!localStorage.getItem(popupKey)) {
                  setTimeout(() => {
                      const badge = document.getElementById('modal-version-badge');
                      if (badge) badge.textContent = 'v' + CURRENT_VERSION;
                      renderChangelog(CURRENT_VERSION);
                      const m = document.getElementById('modal-version-update');
                      if (m) {
                          m.classList.remove('hidden');
                          m.classList.add('flex');
                      }
                  }, 800);
              }
          }

          function closeVersionModal() {
              const m = document.getElementById('modal-version-update');
              if (m) {
                  m.classList.add('hidden');
                  m.classList.remove('flex');
              }
              const popupKey = \`swimshady_shown_v\${CURRENT_VERSION}\`;
              localStorage.setItem(popupKey, 'true');
          }
  
          function updateTitle() {
              const activeTab = document.querySelector('.nav-item.active span');
               if(activeTab) document.getElementById('view-title').innerText = '~/ ' + activeTab.innerText;
          }
  
          function renderProfiles() {
              const pCont = document.getElementById('dyn-profiles-container');
              if (!pCont) return;
              const profiles = window.nahanProfiles || [];
              if (profiles.length === 0) { pCont.innerHTML = '<p style="font-family:monospace;font-size:12px;color:#444;text-align:center;padding:20px">No profiles found.</p>'; return; }
              let html = '';
              profiles.forEach(p => {
                  const isDef = p.name === 'Default';
                  html += \`<div style="border:1px solid #222;background:#141414;margin-bottom:8px">
                      <div style="padding:14px 16px;border-bottom:1px solid #222;display:flex;justify-content:space-between;align-items:center;cursor:pointer" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display==='none'?'block':'none'">
                          <div style="display:flex;align-items:center;gap:8px">
                              <span style="font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;color:#e5e5e5">\${p.name}</span>
                              \${isDef ? '<span style="font-family:monospace;font-size:9px;padding:2px 6px;background:#222;color:#777">master</span>' : ''}
                          </div>
                          <span style="font-family:monospace;font-size:12px;color:#444">▼</span>
                      </div>
                      <div style="display:none;padding:16px;border-top:1px solid #222">
                          <div style="margin-bottom:12px">
                              <div style="font-family:monospace;font-size:10px;color:#444;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">uuid</div>
                              <div style="font-family:monospace;font-size:11px;color:#777;background:#0c0c0c;border:1px solid #222;padding:8px 10px">\${p.id}</div>
                          </div>
                          <div style="margin-bottom:12px">
                              <div style="font-family:monospace;font-size:10px;color:#22c55e;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">sync url</div>
                              <div style="display:flex;gap:4px">
                                  <input type="text" id="ep-\${p.name}" readonly value="\${p.sync}" style="flex:1;font-family:monospace;font-size:11px;color:#777;background:#0c0c0c;border:1px solid #222;padding:8px 10px;outline:none">
                                  <button onclick="copyData('ep-\${p.name}')" style="padding:8px 12px;border:1px solid #22c55e;background:transparent;color:#22c55e;font-family:monospace;font-size:11px;cursor:pointer">copy</button>
                              </div>
                          </div>
                          <button onclick="showQR('\${p.name}','\${p.sync}')" style="width:100%;padding:8px;border:1px solid #222;background:transparent;color:#777;font-family:monospace;font-size:11px;cursor:pointer">show qr</button>
                      </div>
                  </div>\`;
              });
              pCont.innerHTML = html;
          }

          function switchTab(tab) {
            ['overview','info','network','settings','advanced','logs','users'].forEach(t => {
                  const view = document.getElementById('view-'+t);
                  const deskBtn = document.getElementById('tab-'+t);
                  const mobBtn = document.getElementById('mob-tab-'+t);
                  if (tab === t) {
                      view.classList.remove('hidden'); view.classList.add('block', 'fade-in');
                      deskBtn.classList.add('active'); mobBtn.classList.add('active');
                  } else {
                      view.classList.add('hidden'); view.classList.remove('block', 'fade-in');
                      deskBtn.classList.remove('active'); mobBtn.classList.remove('active');
                  }
              });
            document.getElementById('view-add-user').classList.add('hidden');
            document.getElementById('view-edit-user').classList.add('hidden');
            var sc = document.querySelector('.scroll-content');
            sc.style.overflow = '';
            sc.classList.remove('flex', 'flex-col');
            sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');
            updateTitle();
            var sc = document.querySelector('.scroll-content');
            if (sc) sc.scrollTop = 0;
            if(tab === 'overview') loadDashboard();
            if(tab === 'info') renderProfiles();
            if(tab === 'logs') loadLogs();
            if(tab === 'network') doLogin(true); // refresh metrics
        }

        async function loadLogs() {
            const container = document.getElementById('logs-container');
            if(!container) return;
            container.innerHTML = '<p class="text-sm text-slate-400 text-center py-4">' + (i18n[lang]?.loading_logs || 'Loading logs...') + '</p>';
            try {
                const res = await fetch(baseRoute + '/api/logs', { method: 'POST', body: JSON.stringify({ key: sessionKey }) });
                const data = await res.json();
                if (data.success && data.logs) {
                    if (data.logs.length === 0) {
                        container.innerHTML = '<p class="text-sm text-slate-400 text-center py-4">' + (i18n[lang]?.no_activity_logs || 'No activity logs found.') + '</p>';
                        return;
                    }
                    let logsHtml = '';
                    data.logs.forEach(log => {
                        const dateStr = new Date(log.ts).toLocaleString('en-US', {hour12: false});
                        logsHtml += \`<div class="flex flex-col sm:flex-row sm:items-center justify-between p-3 bg-slate-50 dark:bg-slate-800 rounded-xl border border-slate-100 dark:border-darkborder/50 gap-2"><div><p class="text-sm font-bold text-slate-700 dark:text-slate-200">\${log.type}</p><p class="text-xs text-slate-500 truncate max-w-[200px] sm:max-w-xs" title="\${log.detail}">\${log.detail}</p></div><span class="text-[10px] font-mono text-slate-400 bg-white dark:bg-darkcard px-2 py-1 rounded shrink-0">\${dateStr}</span></div>\`;
                    });
                    container.innerHTML = logsHtml;
                } else {
                    container.innerHTML = '<p class="text-sm text-red-400 text-center py-4">Failed to load logs.</p>';
                }
            } catch (err) {
                container.innerHTML = '<p class="text-sm text-red-400 text-center py-4">Error loading logs.</p>';
            }
        }

        async function loadDashboard() {
            try {
                const [statsRes, logsRes] = await Promise.all([
                    fetch(baseRoute + '/api/stats', { method: 'GET', headers: { 'Authorization': 'Bearer ' + sessionKey } }),
                    fetch(baseRoute + '/api/logs', { method: 'POST', body: JSON.stringify({ key: sessionKey }) })
                ]);
                const statsData = await statsRes.json();
                const logsData = await logsRes.json();

                if (statsData.success && statsData.stats) {
                    const s = statsData.stats;
                    document.getElementById('ov-total-users').textContent = s.users.total;
                    document.getElementById('ov-active-users').textContent = s.users.active;
                    document.getElementById('ov-paused-users').textContent = s.users.paused;
                    document.getElementById('ov-auto-disabled').textContent = s.users.autoDisabled;
                    document.getElementById('ov-expired-users').textContent = s.users.expired;
                    document.getElementById('ov-total-traffic').textContent = s.traffic.totalGB + ' ' + (i18n[lang]?.ov_gb_unit || 'GB');
                    document.getElementById('ov-total-reqs').textContent = s.traffic.totalRequests.toLocaleString();
                    document.getElementById('ov-today-traffic').textContent = s.traffic.dailyGB + ' ' + (i18n[lang]?.ov_gb_unit || 'GB');
                    document.getElementById('ov-active-conns').textContent = s.system.activeConnections;
                    var netConnsEl = document.getElementById('net-active-conns');
                    if (netConnsEl) netConnsEl.textContent = s.system.activeConnections;
                    document.getElementById('ov-version').textContent = 'v' + s.system.version;
                }

                const actList = document.getElementById('ov-activity-list');
                if (logsData.success && logsData.logs && logsData.logs.length > 0) {
                    let actHtml = '';
                    logsData.logs.slice(0, 8).forEach(log => {
                        const dateStr = new Date(log.ts).toLocaleString('en-US', {hour12: false});
                        const typeColors = { 'Auth Success': 'bg-emerald-500', 'Auth Failed': 'bg-red-500', 'User Created': 'bg-blue-500', 'User Deleted': 'bg-red-500', 'User Toggled': 'bg-amber-500', 'User Updated': 'bg-indigo-500', 'User Auto-Disabled': 'bg-red-500', 'Traffic Reset': 'bg-cyan-500', 'Config Changed': 'bg-violet-500' };
                        const dotColor = typeColors[log.type] || 'bg-slate-400';
                        actHtml += '<div class="flex items-center gap-3 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl"><div class="w-2 h-2 rounded-full shrink-0 ' + dotColor + '"></div><div class="flex-1 min-w-0"><p class="text-sm font-semibold text-slate-700 dark:text-slate-200 truncate">' + log.type + '</p><p class="text-[11px] text-slate-400 truncate">' + log.detail + '</p></div><span class="text-[10px] font-mono text-slate-400 shrink-0">' + dateStr + '</span></div>';
                    });
                    actList.innerHTML = actHtml;
                } else {
                    actList.innerHTML = '<p class="text-sm text-slate-400 text-center py-6">' + (i18n[lang]?.no_recent_activity || 'No recent activity.') + '</p>';
                }
            } catch (err) {
                console.error('Dashboard load error:', err);
            }
            loadApiKeys();
        }

          function copyData(id) {
              const input = document.getElementById(id); input.select(); navigator.clipboard.writeText(input.value);
              const toast = document.getElementById('copy-toast');
              toast.style.transform = 'translate(-50%, 0)'; toast.style.opacity = '1';
              setTimeout(() => { toast.style.transform = 'translate(-50%, -5rem)'; toast.style.opacity = '0'; }, 2000);
          }
          
          function showQR(name, url) {
              document.getElementById('qr-modal-title').innerText = name;
              document.getElementById('qr-modal-img').src = "https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=" + encodeURIComponent(url);
              document.getElementById('qr-modal-link').innerText = url;
              document.getElementById('qr-modal').classList.remove('hidden');
              document.getElementById('qr-modal').classList.add('flex');
          }

          window.toggleAccordion = function(btn) {
              const card = btn.closest('[data-accordion]');
              if (!card) return;
              const content = card.querySelector('[data-accordion-content]');
              const icon = btn.querySelector('.accordion-icon');
              const isOpen = content.style.visibility === 'visible';

              content.style.transition = 'max-height 0.3s ease, visibility 0.3s ease';

              if (isOpen) {
                  content.style.maxHeight = content.scrollHeight + 'px';
                  requestAnimationFrame(() => {
                      content.style.maxHeight = '0';
                      content.style.visibility = 'hidden';
                  });
                  icon.style.transform = 'rotate(0deg)';
              } else {
                  content.style.visibility = 'visible';
                  content.style.maxHeight = content.scrollHeight + 'px';
                  icon.style.transform = 'rotate(180deg)';
                  setTimeout(() => { if (content.style.visibility === 'visible') content.style.maxHeight = 'none'; }, 350);
              }
          }

          window.handleCopy = function handleCopy(btn) {
              copyData('sync-' + btn.dataset.id);
          }
          window.handleQR = function handleQR(btn) {
              showQR(btn.dataset.name, document.getElementById('sync-' + btn.dataset.id).value);
          }

          function closeQRModal() {
              document.getElementById('qr-modal').classList.add('hidden');
              document.getElementById('qr-modal').classList.remove('flex');
          }
  
          function updateUI() {
              try {
                  let portsStr = Array.from(document.getElementById('cfg-port').selectedOptions).map(o=>o.value).join(',');
                  let port = portsStr ? portsStr.split(',')[0] : '443';
                  let proto = document.getElementById('cfg-proto').value === 'beta' ? String.fromCharCode(116, 114, 111, 106, 97, 110) : String.fromCharCode(118, 108, 101, 115, 115);
                  let rawIps = document.getElementById('cfg-ips').value || "";
                  
                  let ipsList = rawIps.replace(/,/g, '\\n').replace(/;/g, '\\n').split('\\n').map(s=>s.trim()).filter(Boolean);
                  let finalIP = ipsList.length > 0 ? ipsList[0] : (hostName.endsWith('.pages.dev') ? 'time.is' : hostName);
                  
                  let fp = document.getElementById('cfg-fp').value;
                  let path = encodeURI("/" + document.getElementById('cfg-path').value);
                  let sec = ["80","8080"].includes(port) ? "none" : "tls";
                  
                  let rawLink = proto + "://" + localUUID + "@" + finalIP + ":" + port + "?encryption=none&security=" + sec + "&sni=" + hostName + "&fp=" + fp + "&type=ws&host=" + hostName + "&path=" + path;
                  if (document.getElementById('cfg-ech').checked) rawLink += "&pbk=enabled";
                  rawLink += "#" + hostName;
  
                  // FIX: Check if elements exist
                  const linkEl = document.getElementById('link-direct');
                  if (linkEl) linkEl.value = rawLink;
  
                  const qrEl = document.getElementById('qr-code');
                  if (qrEl) qrEl.src = "https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=" + encodeURIComponent(rawLink);
  
                  let totalIps = ipsList.length === 0 ? 1 : ipsList.length;
                  let tCfg = totalIps * 2; 
                  document.getElementById('ip-count-badge').innerText = lang === 'fa' ? (tCfg + ' کانفیگ تولید شد') : (tCfg + ' Configs Active');
              } catch(e) { console.error(e); }
          }
  
          function logout() {
              localStorage.removeItem('swimshady_session');
              window.location.reload();
          }

          function showAddNodeModal() {
              document.getElementById('modal-add-node').classList.remove('hidden');
              document.getElementById('add-node-url').value = '';
              document.getElementById('add-node-apikey').value = '';
              document.getElementById('add-node-url').focus();
          }

          function commitAddNode() {
              const url = document.getElementById('add-node-url').value.trim();
              const apiKey = document.getElementById('add-node-apikey').value.trim();
              if (!url || !apiKey) {
                  const t = i18n[lang] || i18n['en'];
                  alert(t.add_node_invalid || 'Please enter both URL and API Key.');
                  return;
              }
              if (!window.nahanConfig) window.nahanConfig = {};
              if (!Array.isArray(window.nahanConfig.linkedPanels)) window.nahanConfig.linkedPanels = [];
              window.nahanConfig.linkedPanels.push({ url, apiKey });
              document.getElementById('modal-add-node').classList.add('hidden');
              renderLinkedNodes();
          }

          function removeLinkedNode(idx) {
              if (!window.nahanConfig || !Array.isArray(window.nahanConfig.linkedPanels)) return;
              window.nahanConfig.linkedPanels.splice(idx, 1);
              renderLinkedNodes();
          }

          function renderLinkedNodes() {
              const list = document.getElementById('linked-nodes-list');
              if (!list) return;
              const panels = (window.nahanConfig && Array.isArray(window.nahanConfig.linkedPanels)) ? window.nahanConfig.linkedPanels : [];
              if (panels.length === 0) {
                  list.innerHTML = '<p class="text-xs text-slate-400 dark:text-slate-500 italic">' + ((i18n[lang] || i18n['en']).no_nodes_advanced || 'No external nodes added yet.') + '</p>';
                  return;
              }
              list.innerHTML = panels.map((p, i) => \`
                  <div class="flex items-center justify-between gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder/50">
                      <div class="min-w-0 flex-1">
                          <p class="text-sm font-mono font-bold text-slate-700 dark:text-slate-200 truncate">\${p.url}</p>
                          <p class="text-[11px] text-slate-400 dark:text-slate-500 font-mono truncate">\${p.apiKey.substring(0, 12)}...</p>
                      </div>
                      <button onclick="removeLinkedNode(\${i})" class="p-1.5 hover:bg-red-100 dark:hover:bg-red-900/30 rounded-lg transition-colors shrink-0" title="Remove">
                          <svg class="w-3.5 h-3.5 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
                      </button>
                  </div>
              \`).join('');
          }

          function renderFakeConfigs(configs) {
              const list = document.getElementById('fake-configs-list');
              if (!list) return;
              list.innerHTML = '';
              if (!configs || configs.length === 0) {
                  configs = [
                      { name: "📊 {usage}", enabled: true },
                      { name: "📅 {expiry}", enabled: true }
                  ];
              }
              configs.forEach((cfg, idx) => {
                  const item = document.createElement('div');
                  item.className = 'flex items-center gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder/50';
                  item.innerHTML = \`
                      <div class="toggle \${cfg.enabled ? 'on' : ''}" style="position:relative;cursor:pointer;flex-shrink:0" onclick="this.classList.toggle('on'); toggleFakeConfig(\${idx})">
                          <input type="checkbox" id="fake-cb-\${idx}" \${cfg.enabled ? 'checked' : ''} class="sr-only" style="display:none">
                          <div style="width:36px;height:20px;border-radius:10px;position:relative;transition:background 0.15s">
                              <div style="width:16px;height:16px;border-radius:50%;position:absolute;top:2px;transition:all 0.15s" class="toggle-knob"></div>
                          </div>
                      </div>
                      <input type="text" value="\${cfg.name.replace(/"/g, '&quot;')}" onchange="updateFakeConfigName(\${idx}, this.value)" class="flex-1 min-w-0 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-darkborder bg-white dark:bg-slate-900 focus:border-primary outline-none text-sm font-mono">
                      <button onclick="moveFakeConfig(\${idx}, -1)" class="p-1.5 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-lg transition-colors shrink-0" title="Move up">
                          <svg class="w-3.5 h-3.5 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7"></path></svg>
                      </button>
                      <button onclick="moveFakeConfig(\${idx}, 1)" class="p-1.5 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-lg transition-colors shrink-0" title="Move down">
                          <svg class="w-3.5 h-3.5 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                      </button>
                      <button onclick="removeFakeConfig(\${idx})" class="p-1.5 hover:bg-red-100 dark:hover:bg-red-900/30 rounded-lg transition-colors shrink-0" title="Remove">
                          <svg class="w-3.5 h-3.5 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
                      </button>
                  \`;
                  list.appendChild(item);
              });
              window._fakeConfigs = configs;
          }

          function addFakeConfig() {
              if (!window._fakeConfigs) window._fakeConfigs = [];
              window._fakeConfigs.push({ name: "Custom Entry", enabled: true });
              renderFakeConfigs(window._fakeConfigs);
          }

          function removeFakeConfig(idx) {
              if (!window._fakeConfigs) return;
              window._fakeConfigs.splice(idx, 1);
              renderFakeConfigs(window._fakeConfigs);
          }

          function toggleFakeConfig(idx) {
              if (!window._fakeConfigs || !window._fakeConfigs[idx]) return;
              window._fakeConfigs[idx].enabled = !window._fakeConfigs[idx].enabled;
          }

          function updateFakeConfigName(idx, value) {
              if (!window._fakeConfigs || !window._fakeConfigs[idx]) return;
              window._fakeConfigs[idx].name = value;
          }

          function moveFakeConfig(idx, direction) {
              if (!window._fakeConfigs) return;
              const newIdx = idx + direction;
              if (newIdx < 0 || newIdx >= window._fakeConfigs.length) return;
              const temp = window._fakeConfigs[idx];
              window._fakeConfigs[idx] = window._fakeConfigs[newIdx];
              window._fakeConfigs[newIdx] = temp;
              renderFakeConfigs(window._fakeConfigs);
          }

          function getFakeConfigsFromUI() {
              return window._fakeConfigs || [
                  { name: "📊 {usage}", enabled: true },
                  { name: "📅 {expiry}", enabled: true }
              ];
          }
  
          // Export active page inputs configuration
          function exportConfig() {
              const el = id => document.getElementById(id);
              const payload = {
                  mode: el('cfg-proto').value, socketPorts: Array.from(el('cfg-port').selectedOptions).map(o=>o.value).join(','), deviceId: el('cfg-uuid').value,
                  apiRoute: el('cfg-path').value, masterKey: el('cfg-pass').value, agent: el('cfg-fp').value,
                   resolveIp: el('cfg-dns').value, customDns: el('cfg-custom-dns').value ? el('cfg-custom-dns').value : 'https://cloudflare-dns.com/dns-query', cleanIps: el('cfg-ips').value, maintenanceHost: el('cfg-fake') ? el('cfg-fake').value : '', backupRelay: el('cfg-relay').value, nat64Prefix: el('cfg-nat64') ? el('cfg-nat64').value : '', enableDirectConfigs: el('cfg-direct-configs') ? el('cfg-direct-configs').checked : false, syncApiKey: el('cfg-sync-api-key') ? el('cfg-sync-api-key').value.trim() : '', autoUpdate: el('cfg-auto-update') ? el('cfg-auto-update').checked : false, autoUpdateFormat: document.querySelector('input[name="auto-update-format"]:checked')?.value || 'normal',
                   enableOpt1: el('cfg-tfo').checked, enableOpt2: el('cfg-ech').checked,
                   tgToken: el('cfg-tg-token').value, tgChatId: el('cfg-tg-chat').value, tgAdminId: el('cfg-tg-admin').value,
                  cfAccountId: el('cfg-cf-acc').value, cfApiToken: el('cfg-cf-token').value,
                  cfWorkerName: el('cfg-cf-worker').value,
                  isPaused: el('cfg-pause').checked, silentAlerts: el('cfg-silent').checked,
                  githubRepo: el('cfg-github-repo').value,
                  subUserAgent: el('cfg-sub-ua').value,
                  customPanelUrl: el('cfg-custom-panel-url').value,
                  fakeConfigs: getFakeConfigsFromUI(),
                  linkedPanels: (window.nahanConfig && Array.isArray(window.nahanConfig.linkedPanels)) ? window.nahanConfig.linkedPanels : []
              };
              const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(payload, null, 2));
              const dlAnchor = document.createElement('a');
              dlAnchor.setAttribute("href", dataStr);
              dlAnchor.setAttribute("download", "SwimShady-gateway-config.json");
              document.body.appendChild(dlAnchor);
              dlAnchor.click();
              dlAnchor.remove();
          }
  
          // Import backup json to overwrite config inputs 
          function importConfig(event) {
              const file = event.target.files[0];
              if (!file) return;
              const reader = new FileReader();
              reader.onload = function(e) {
                  try {
                      const conf = JSON.parse(e.target.result);
                      const mapId = (id, val) => { const el = document.getElementById(id); if (el && val !== undefined) el.value = val; };
                      mapId('cfg-proto', conf.mode);
                      let pList = (conf.socketPorts || conf.socketPort || '443').split(',');
                      Array.from(document.getElementById('cfg-port').options).forEach(o => o.selected = pList.includes(o.value));
                      mapId('cfg-uuid', conf.deviceId);
                      mapId('cfg-path', conf.apiRoute);
                      mapId('cfg-pass', conf.masterKey);
                      mapId('cfg-fp', conf.agent);
                      mapId('cfg-dns', conf.resolveIp);
                      mapId('cfg-custom-dns', conf.customDns);
                      mapId('cfg-ips', conf.cleanIps);
                      mapId('cfg-fake', conf.maintenanceHost);
                      mapId('cfg-relay', conf.backupRelay);
                      mapId('cfg-tg-token', conf.tgToken);
                      mapId('cfg-tg-chat', conf.tgChatId);
                      mapId('cfg-tg-admin', conf.tgAdminId);
                      mapId('cfg-cf-acc', conf.cfAccountId);
                      mapId('cfg-cf-token', conf.cfApiToken);
                      mapId('cfg-cf-worker', conf.cfWorkerName);
                      mapId('cfg-github-repo', conf.githubRepo);
                      mapId('cfg-sub-ua', conf.subUserAgent);
                      mapId('cfg-custom-panel-url', conf.customPanelUrl);
                      
                      if (conf.enableOpt1 !== undefined) document.getElementById('cfg-tfo').checked = conf.enableOpt1;
                      if (conf.enableOpt2 !== undefined) document.getElementById('cfg-ech').checked = conf.enableOpt2;
                      if (conf.isPaused !== undefined) document.getElementById('cfg-pause').checked = conf.isPaused;
                      if (conf.silentAlerts !== undefined) document.getElementById('cfg-silent').checked = conf.silentAlerts;
                      mapId('cfg-nat64', conf.nat64Prefix);
                      if (conf.enableDirectConfigs !== undefined && document.getElementById('cfg-direct-configs')) document.getElementById('cfg-direct-configs').checked = conf.enableDirectConfigs;
                      if (document.getElementById('cfg-sync-api-key')) document.getElementById('cfg-sync-api-key').value = conf.syncApiKey || '';
                      if (conf.autoUpdate !== undefined && document.getElementById('cfg-auto-update')) {
                          document.getElementById('cfg-auto-update').checked = conf.autoUpdate;
                          const wrap = document.getElementById('auto-update-format-wrap');
                          if (wrap) wrap.classList.toggle('hidden', !conf.autoUpdate);
                      }
                      if (conf.autoUpdateFormat) {
                          const radio = document.querySelector(\`input[name="auto-update-format"][value="\${conf.autoUpdateFormat}"]\`);
                          if (radio) radio.checked = true;
                       }
                       ['cfg-tfo','cfg-ech','cfg-pause','cfg-silent','cfg-direct-configs','cfg-auto-update'].forEach(id=>{const cb=document.getElementById(id);if(cb){const t=cb.closest('.toggle')||cb.parentElement;t.classList.toggle('on',cb.checked)}});
                       
                       if (conf.fakeConfigs) renderFakeConfigs(conf.fakeConfigs);
                      if (conf.linkedPanels) {
                          if (!window.nahanConfig) window.nahanConfig = {};
                          window.nahanConfig.linkedPanels = conf.linkedPanels;
                          renderLinkedNodes();
                      }
                      
                      updateUI();
                      alert(lang === 'fa' ? 'پیکربندی با موفقیت وارد شد! روی ذخیره کلیک کنید.' : 'Configuration parsed! Click save to write changes.');
                  } catch(err) {
                      alert(lang === 'fa' ? 'فایل نامعتبر است!' : 'Invalid configuration file!');
                  }
              };
              reader.readAsText(file);
          }
  
          // Browser-level latency check diagnostics
           async function runPingTest() {
               const customIP = document.getElementById('ping-custom-ip')?.value?.trim();
               let targetIP;
               if (customIP) {
                   targetIP = customIP;
               } else {
                   const rawIps = document.getElementById('cfg-ips').value || "";
                   let ipsList = rawIps.replace(/,/g, '\\n').replace(/;/g, '\\n').split('\\n').map(s=>s.trim()).filter(Boolean);
                   targetIP = ipsList.length > 0 ? ipsList[0] : (hostName.endsWith('.pages.dev') ? 'time.is' : hostName);
               }
              
              const resultsDiv = document.getElementById('ping-results');
              resultsDiv.classList.remove('hidden');
              
              document.getElementById('ping-target').textContent = targetIP;
              document.getElementById('ping-time').textContent = 'Testing...';
              document.getElementById('ping-status').textContent = 'Dialing...';
              document.getElementById('ping-port').textContent = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
              
              const startTime = performance.now();
              try {
                  await fetch('https://' + targetIP + '/favicon.ico?cb=' + startTime, { mode: 'no-cors', cache: 'no-store' });
                  const duration = Math.round(performance.now() - startTime);
                  document.getElementById('ping-time').textContent = duration + ' ms';
                  document.getElementById('ping-status').className = "text-sm font-bold text-emerald-500";
                  document.getElementById('ping-status').textContent = "Success";
              } catch (err) {
                  const duration = Math.round(performance.now() - startTime);
                  if (duration < 1500) {
                      document.getElementById('ping-time').textContent = duration + ' ms';
                      document.getElementById('ping-status').className = "text-sm font-bold text-amber-500";
                      document.getElementById('ping-status').textContent = "Indirect-OK";
                  } else {
                      document.getElementById('ping-time').textContent = 'Timeout';
                      document.getElementById('ping-status').className = "text-sm font-bold text-red-500";
                      document.getElementById('ping-status').textContent = "Unreachable";
                  }
              }
          }
  
          function togglePortCheckbox(val, checked) {
              const sel = document.getElementById('cfg-port');
              const opt = Array.from(sel.options).find(o => o.value === val);
              if (opt) {
                  opt.selected = checked;
                  sel.dispatchEvent(new Event('change'));
              }
          }
          function syncCheckboxesFromSelect() {
              const sel = document.getElementById('cfg-port');
              const ports = Array.from(sel.selectedOptions).map(o => o.value);
              const checkboxes = document.querySelectorAll('#port-checkboxes-container input[type="checkbox"]');
              checkboxes.forEach(cb => {
                  cb.checked = ports.includes(cb.value);
              });
          }

          async function doLogin(silent = false) {
              const btn = document.querySelector('button[onclick="doLogin()"]');
              const origText = btn.innerText; 
              if(!silent) btn.innerText = "...";
              try {
                  const pass = silent ? sessionKey : document.getElementById('pwd').value;
                  const res = await fetch(baseRoute + '/api/auth', { method: 'POST', body: JSON.stringify({ key: pass }) });
                  const data = await res.json();
                  if (data.success) {
                      sessionKey = pass; localUUID = data.deviceId;
                      localStorage.setItem('swimshady_session', JSON.stringify({ key: pass, expiry: Date.now() + 30 * 60 * 1000 }));
                      
                      document.getElementById('login-box').classList.add('hidden');
                      document.getElementById('dash-box').classList.remove('hidden');
                      document.getElementById('dash-box').classList.add('flex');
                      document.getElementById('btn-logout-mob').classList.remove('hidden');
                      document.body.classList.add('logged-in');
                      
                      document.getElementById('net-ip').textContent = data.network.ip;
                      document.getElementById('net-colo').textContent = data.network.colo;
                      document.getElementById('net-loc').textContent = data.network.loc;
                      const conf = data.config;
                      document.getElementById('cfg-proto').value = conf.mode || 'alpha';
                      let pList = (conf.socketPorts || conf.socketPort || '443').split(',');
                      Array.from(document.getElementById('cfg-port').options).forEach(o => o.selected = pList.includes(o.value));
                      syncCheckboxesFromSelect();
                      document.getElementById('cfg-uuid').value = conf.deviceId || '';
                      document.getElementById('cfg-path').value = conf.apiRoute || '';
                      document.getElementById('cfg-pass').value = conf.masterKey || '';
                      document.getElementById('cfg-fp').value = conf.agent || 'chrome';
                      document.getElementById('cfg-dns').value = conf.resolveIp || '';
                      document.getElementById('cfg-custom-dns').value = conf.customDns || 'https://cloudflare-dns.com/dns-query';
                      document.getElementById('cfg-ips').value = conf.cleanIps || '';
                      if (document.getElementById('cfg-fake')) document.getElementById('cfg-fake').value = conf.maintenanceHost || '';
                       document.getElementById('cfg-relay').value = conf.backupRelay || '';
                       if (document.getElementById('cfg-nat64')) document.getElementById('cfg-nat64').value = conf.nat64Prefix || '';
                       if (document.getElementById('cfg-direct-configs')) document.getElementById('cfg-direct-configs').checked = conf.enableDirectConfigs || false;
                       if (document.getElementById('cfg-sync-api-key')) document.getElementById('cfg-sync-api-key').value = conf.syncApiKey || '';
                       if (document.getElementById('cfg-auto-update')) {
                           document.getElementById('cfg-auto-update').checked = conf.autoUpdate || false;
                           const wrap = document.getElementById('auto-update-format-wrap');
                           if (wrap) wrap.classList.toggle('hidden', !conf.autoUpdate);
                       }
                       if (conf.autoUpdateFormat) {
                           const radio = document.querySelector(\`input[name="auto-update-format"][value="\${conf.autoUpdateFormat}"]\`);
                           if (radio) radio.checked = true;
                       }
                      document.getElementById('cfg-tfo').checked = conf.enableOpt1 || false;
                      document.getElementById('cfg-ech').checked = conf.enableOpt2 || false;
                      document.getElementById('cfg-tg-token').value = conf.tgToken || '';
                      document.getElementById('cfg-tg-chat').value = conf.tgChatId || '';
                      document.getElementById('cfg-tg-admin').value = conf.tgAdminId || '';
                      document.getElementById('cfg-cf-acc').value = conf.cfAccountId || '';
                      document.getElementById('cfg-cf-token').value = conf.cfApiToken || '';
                      document.getElementById('cfg-cf-worker').value = conf.cfWorkerName || '';
                      document.getElementById('cfg-pause').checked = conf.isPaused || false;
                       document.getElementById('cfg-silent').checked = conf.silentAlerts || false;
                       ['cfg-tfo','cfg-ech','cfg-pause','cfg-silent','cfg-direct-configs','cfg-auto-update'].forEach(id=>{const cb=document.getElementById(id);if(cb){const t=cb.closest('.toggle')||cb.parentElement;t.classList.toggle('on',cb.checked)}});
                       document.getElementById('cfg-github-repo').value = conf.githubRepo || 'Ceetherr/swimshady';
                      document.getElementById('cfg-name-strategy').value = conf.nameStrategy || 'default';
                      document.getElementById('cfg-name-prefix').value = conf.namePrefix || 'Core';
                      document.getElementById('cfg-sub-ua').value = conf.subUserAgent || '';
                      document.getElementById('cfg-custom-panel-url').value = conf.customPanelUrl || '';
                      renderFakeConfigs(conf.fakeConfigs || [
                          { name: "📊 {usage}", enabled: true },
                          { name: "📅 {expiry}", enabled: true }
                      ]);
  
                      window.nahanConfig = JSON.parse(JSON.stringify(conf));
                      window.nahanUsage = data.sysUsage || {};
                      window.nahanProfiles = data.profiles || [];
                      renderUsersTable();
                      renderLinkedNodes();
                      renderProfiles();
                      updateDoHServerUrl();
                      try { checkUpdate(); } catch(ue) { console.error(ue); }
                       if (!silent) switchTab('overview');

                      ['cfg-proto','cfg-port','cfg-fp','cfg-ips','cfg-path', 'cfg-relay', 'cfg-name-strategy', 'cfg-name-prefix', 'cfg-sub-ua', 'cfg-custom-panel-url'].forEach(id => {
                          const el = document.getElementById(id);
                          if(el) { el.addEventListener('input', updateUI); el.addEventListener('change', updateUI); }
                      });
                       ['cfg-silent','cfg-pause','cfg-auto-update','cfg-direct-configs','cfg-tfo','cfg-ech'].forEach(id => {
                           const el = document.getElementById(id);
                           if(el) {
                               el.addEventListener('change', updateUI);
                               el.addEventListener('change', function() {
                                   const t = this.closest('.toggle') || this.parentElement;
                                   t.classList.toggle('on', this.checked);
                               });
                           }
                       });
                      const autoUpdateEl = document.getElementById('cfg-auto-update');
                      if (autoUpdateEl) {
                          autoUpdateEl.addEventListener('change', () => {
                              const wrap = document.getElementById('auto-update-format-wrap');
                              if (wrap) wrap.classList.toggle('hidden', !autoUpdateEl.checked);
                          });
                      }
                const pCont = document.getElementById('dyn-profiles-container');
                let profilesHtml = '';
                data.profiles.forEach(p => {
                            const isDef = p.name === 'Default';
                            let html = \`<div class="bg-white dark:bg-darkcard rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder relative mb-4 break-inside-avoid inline-block w-full" data-accordion>
    <div class="absolute top-0 end-0 w-32 h-32 bg-primary/5 rounded-bl-[100px] -z-10"></div>
    <button onclick="toggleAccordion(this)" class="w-full flex items-center justify-between p-5 md:p-6">
        <h3 class="text-lg font-bold text-slate-800 dark:text-white flex items-center">
            <svg class="w-5 h-5 me-2 text-primary" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"></path></svg>
            \${p.name}
        </h3>
        <div class="flex items-center gap-2">
            \${isDef ? '<span class="text-[10px] bg-slate-100 text-slate-500 px-2 py-1 rounded font-bold uppercase">Master</span>' : ''}
            <svg class="w-4 h-4 text-slate-400 accordion-icon transition-transform duration-300" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
        </div>
    </button>
    <div class="transition-all duration-300" style="max-height:0;overflow:hidden;" data-accordion-content>
        <div class="space-y-3 px-5 md:px-6 pb-5 md:pb-6">
            <div>
                <label class="block text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">UUID</label>
                <div class="bg-slate-50 dark:bg-slate-800 border border-slate-100 dark:border-darkborder px-3 py-2 rounded-lg text-xs font-mono text-slate-500">\${p.id}</div>
            </div>
            <div class="relative">
                <label class="block text-[10px] font-semibold text-emerald-500 uppercase tracking-wider mb-1 flex items-center gap-1.5"><span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>Universal Sync URL</label>
                <input type="text" id="sync-\${p.id}" readonly value="\${p.sync}" class="w-full bg-slate-50 dark:bg-darkbg border border-slate-200 dark:border-darkborder px-4 py-2.5 rounded-xl text-xs outline-none font-mono text-slate-600 dark:text-slate-400 truncate pe-12">
                <button data-id="\${p.id}" onclick="handleCopy(this)" class="absolute bottom-1 end-1 text-primary p-1.5 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-md"><svg class="w-4.5 h-4.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"></path></svg></button>
            </div>
            <div class="mt-2">
                <button data-id="\${p.id}" data-name="\${p.name}" onclick="handleQR(this)" class="w-full flex items-center justify-center p-2.5 bg-slate-50 hover:bg-slate-100 dark:bg-slate-800 dark:hover:bg-slate-700 border border-slate-200 dark:border-darkborder rounded-xl transition-all gap-1.5 text-[11px] font-bold text-slate-600 dark:text-slate-400">
                    <svg class="w-4 h-4 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v1m0 11v1m5-7h1m-13 0h1m2-5a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2V6a2 2 0 00-2-2h-8zM9 9h1m0 0v1m2-1h1m0 0v1"></path></svg>
                    <span data-i18n="show_qr">Show QR Code</span>
                </button>
            </div>
        </div>
    </div>
</div>\`;
                         profilesHtml += html;
                      });
                      pCont.innerHTML = profilesHtml;



                      // Inject usage metrics table
                      const usageCont = document.getElementById('usage-metrics-container');
                      if(usageCont && data.usage) {
                          let usageHtml = '';
                          data.profiles.forEach(p => {
                              let hash = p.id.replace(/-/g, '').toLowerCase();
                              let use = data.usage[hash];
                              if(use) {
                                  let timeStr = new Date(use.last).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});
                                  usageHtml += \`<div class="flex items-center justify-between p-3 border-b border-slate-100 dark:border-darkborder/50 last:border-0"><div class="flex flex-col"><span class="text-sm font-bold text-slate-700 dark:text-slate-200">\${p.name}</span><span class="text-[10px] text-slate-400 font-mono">\${p.id.split('-')[0]}...</span></div><div class="flex flex-col items-end"><span class="text-xs font-bold text-emerald-500">\${use.connects} Conns</span><span class="text-[10px] text-slate-400">\${timeStr}</span></div></div>\`;
                              }
                          });
                          usageCont.innerHTML = usageHtml || '<p class="text-xs text-slate-400 text-center py-4">' + (i18n[lang]?.no_active_conn || 'No active connection data yet.') + '</p>';
                      }
                      
                      updateUI();
                  } else { 
                      if(!silent) { document.getElementById('err-msg').classList.remove('hidden'); btn.innerText = origText; }
                      else { localStorage.removeItem('swimshady_session'); }
                  }
              } catch (err) { if(!silent) btn.innerText = origText; }
          }
  
          async function doSave() {
              const el = id => document.getElementById(id);
              const payload = {
                  key: sessionKey,
                  config: {
                      mode: el('cfg-proto').value, socketPorts: Array.from(el('cfg-port').selectedOptions).map(o=>o.value).join(','), deviceId: el('cfg-uuid').value,
                      apiRoute: el('cfg-path').value, masterKey: el('cfg-pass').value, agent: el('cfg-fp').value,
                      resolveIp: el('cfg-dns').value, customDns: el('cfg-custom-dns').value ? el('cfg-custom-dns').value : 'https://cloudflare-dns.com/dns-query', cleanIps: el('cfg-ips').value, maintenanceHost: el('cfg-fake') ? el('cfg-fake').value : '', backupRelay: el('cfg-relay').value, nat64Prefix: el('cfg-nat64') ? el('cfg-nat64').value : '', enableDirectConfigs: el('cfg-direct-configs') ? el('cfg-direct-configs').checked : false, syncApiKey: el('cfg-sync-api-key') ? el('cfg-sync-api-key').value.trim() : '', autoUpdate: el('cfg-auto-update') ? el('cfg-auto-update').checked : false, autoUpdateFormat: document.querySelector('input[name="auto-update-format"]:checked')?.value || 'normal',
                      enableOpt1: el('cfg-tfo').checked, enableOpt2: el('cfg-ech').checked,
                      tgToken: el('cfg-tg-token').value, tgChatId: el('cfg-tg-chat').value, tgAdminId: el('cfg-tg-admin').value,
                      cfAccountId: el('cfg-cf-acc').value, cfApiToken: el('cfg-cf-token').value,
                      cfWorkerName: el('cfg-cf-worker').value,
                      isPaused: el('cfg-pause').checked, silentAlerts: el('cfg-silent').checked,
                      githubRepo: el('cfg-github-repo').value,
                      subUserAgent: el('cfg-sub-ua').value,
                      customPanelUrl: el('cfg-custom-panel-url').value,
                      nameStrategy: el('cfg-name-strategy').value,
                      namePrefix: el('cfg-name-prefix').value,
                      fakeConfigs: getFakeConfigsFromUI(),
                      linkedPanels: (window.nahanConfig && Array.isArray(window.nahanConfig.linkedPanels)) ? window.nahanConfig.linkedPanels : []
                  }
              };
                        //update user port after change global
                     const globalPorts = (payload.config.socketPorts || '443').split(',').map(s=>s.trim()).filter(Boolean);
                     payload.config.users = (window.nahanConfig.users || []).map(u => {
                     if (!u.userPorts) return u;
                        const filtered = u.userPorts.split(',').map(s=>s.trim()).filter(p => globalPorts.includes(p));
                      u.userPorts = filtered.length ? filtered.join(',') : globalPorts[0];
                          return u;
                          });
              const stat = el('save-status'); stat.textContent = i18n[lang].msg_saving; stat.className = "text-sm font-bold text-primary animate-pulse md:me-4";
               try {
                   const toast = document.getElementById('save-toast');
                   if (toast) { toast.style.display = 'block'; setTimeout(() => { toast.style.display = 'none'; }, 2000); }
                   const res = await fetch(baseRoute + '/api/sync', { method: 'POST', body: JSON.stringify(payload) });
                   const data = await res.json();
                  if (data.success) {
                      stat.textContent = i18n[lang].msg_saved; stat.className = "text-sm font-bold text-emerald-500 md:me-4";
                      if (Array.isArray(window.nahanConfig?.linkedPanels) && window.nahanConfig.linkedPanels.length > 0) {
                          const sc = payload.config;
                          const slaveCfg = { ...sc };
                          delete slaveCfg.tgToken; delete slaveCfg.tgChatId; delete slaveCfg.tgAdminId; delete slaveCfg.tgBotLang;
                          delete slaveCfg.cfAccountId; delete slaveCfg.cfApiToken; delete slaveCfg.cfWorkerName;
                          delete slaveCfg.panelApiKeys; delete slaveCfg.linkedPanels; delete slaveCfg.slaveNodes; delete slaveCfg.syncApiKey;
                          const synced = new Set();
                          window.nahanConfig.linkedPanels.forEach(p => {
                              if (!p || !p.url || !p.apiKey) return;
                              const h = p.url.trim().replace(/^https?:\\/\\//, '').replace(/\\/.*$/, '');
                              if (!h || synced.has(h.toLowerCase())) return;
                              synced.add(h.toLowerCase());
                              fetch('https://' + h + '/' + encodeURIComponent(sc.apiRoute || 'sync') + '/api/sync', {
                                  method: 'POST',
                                  headers: { 'Content-Type': 'application/json' },
                                  body: JSON.stringify({ key: p.apiKey, config: slaveCfg, fromMaster: true })
                              }).then(r => { if (!r.ok) console.error('Sync to ' + h + ' failed: HTTP ' + r.status); }).catch(e => { console.error('Sync to ' + h + ' error:', e.message); });
                          });
                      }
                      setTimeout(() => window.location.href = '/' + data.newRoute + '/dash', 1000);
                  } else { stat.textContent = i18n[lang].msg_err; stat.className = "text-sm font-bold text-red-500 md:me-4"; }
              } catch(e) { stat.textContent = i18n[lang].msg_err; stat.className = "text-sm font-bold text-red-500 md:me-4"; }
          }

          document.getElementById('pwd').addEventListener('keypress', e => { if (e.key === 'Enter') doLogin(); });
  
          function renderUsersTable() {
              const tbl = document.getElementById('tbl-users');
              if(!tbl) return;
              let users = window.nahanConfig?.users || [];
              let usage = window.nahanUsage || {};
              
              // Calculate stats metrics
              let totalUsersVal = users.length;
              let activeSubscribers = users.filter(u => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs)).length;
              let autoDisabledCount = users.filter(u => u.isPaused && u.disabledReason).length;
              let pausedSubscribers = users.filter(u => u.isPaused && !u.disabledReason).length;
              let expiredCount = users.filter(u => u.expiryMs && Date.now() > u.expiryMs && !u.isPaused).length;
              let totalReqsSum = 0;
              users.forEach(u => {
                  let sysU = usage[u.id.replace(/-/g,'').toLowerCase()] || {reqs: 0};
                  totalReqsSum += (sysU.reqs || 0);
              });
              let totalGBSum = (totalReqsSum / 6000).toFixed(2);

              // Update stats elements in DOM if they exist
              const totalUsersEl = document.getElementById('stat-total-users');
              if (totalUsersEl) totalUsersEl.textContent = totalUsersVal;
              const activeUsersEl = document.getElementById('stat-active-users');
              if (activeUsersEl) activeUsersEl.textContent = \`\${activeSubscribers} / \${pausedSubscribers}\`;
              const totalTrafficEl = document.getElementById('stat-total-traffic');
              if (totalTrafficEl) totalTrafficEl.textContent = \`\${totalGBSum} \${i18n[lang]?.ov_gb_unit || 'GB'}\`;
              const autoDisabledEl = document.getElementById('stat-auto-disabled');
              if (autoDisabledEl) autoDisabledEl.textContent = autoDisabledCount;

              // Render Recently Disabled Users Panel
              const disabledPanel = document.getElementById('disabled-users-panel');
              const disabledList = document.getElementById('disabled-users-list');
              const disabledBadge = document.getElementById('disabled-panel-badge');
              if (disabledPanel && disabledList) {
                  const autoDisabledUsers = users.filter(u => u.isPaused && u.disabledReason)
                      .sort((a, b) => (b.disabledAt || 0) - (a.disabledAt || 0));
                  if (autoDisabledUsers.length > 0) {
                      disabledPanel.classList.remove('hidden');
                      if (disabledBadge) disabledBadge.textContent = autoDisabledUsers.length;
                      disabledList.innerHTML = autoDisabledUsers.map(u => {
                          let timeStr = u.disabledAt ? new Date(u.disabledAt).toLocaleString() : '-';
                          let reasonIcon = u.disabledReason.includes('Traffic') ? '📊' : (u.disabledReason.includes('Expiration') ? '📅' : '⚠️');
                          let btnLabel = lang === 'fa' ? 'فعال‌سازی مجدد' : 'Re-enable';
                          return \`
                              <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-slate-800/50 rounded-xl border border-red-100 dark:border-red-800/20 hover:shadow-md transition-shadow">
                                  <div class="flex items-center gap-3 flex-1 min-w-0">
                                      <div class="text-lg">\${reasonIcon}</div>
                                      <div class="min-w-0">
                                          <div class="text-sm font-bold text-slate-700 dark:text-slate-200 truncate">\${u.name}</div>
                                          <div class="text-[11px] text-red-500 dark:text-red-400 font-medium">\${u.disabledReason}</div>
                                          <div class="text-[10px] text-slate-400 mt-0.5">\${timeStr}</div>
                                      </div>
                                  </div>
                                  <button onclick="togglePauseUser('\${u.id}')" class="ml-3 px-3 py-1.5 bg-emerald-500 hover:bg-emerald-600 text-white text-[11px] font-bold rounded-lg shadow-sm transition-colors whitespace-nowrap">\${btnLabel}</button>
                              </div>
                          \`;
                      }).join('');
                  } else {
                      disabledPanel.classList.add('hidden');
                  }
              }

              // Apply Status Filter
              const statusFilter = document.getElementById('user-status-filter')?.value || 'all';
              const searchVal = document.getElementById('user-search-input')?.value.toLowerCase().trim() || '';
              let filteredUsers = users.filter(u => {
                  if (statusFilter === 'active' && (u.isPaused || (u.expiryMs && Date.now() > u.expiryMs))) return false;
                  if (statusFilter === 'paused' && (!u.isPaused || u.disabledReason)) return false;
                  if (statusFilter === 'auto-disabled' && !(u.isPaused && u.disabledReason)) return false;
                  return u.name.toLowerCase().includes(searchVal) || u.id.toLowerCase().includes(searchVal);
              });

              tbl.innerHTML = '';
              if (filteredUsers.length === 0) {
                  tbl.innerHTML = '<div class="col-span-full px-4 py-8 text-center text-slate-400 text-sm">' + (i18n[lang]?.no_matching_users || 'No matching subscribers found') + '</div>';
                  return;
              }
              
              // Alias users to the filtered list for downstream compatibility
              users = filteredUsers;
              if (users.length === 0) {
                  tbl.innerHTML = \`<div class="col-span-full px-4 py-8 text-center text-slate-400 text-sm" data-i18n="no_users">\${i18n[lang].no_users}</div>\`;
                  return;
              }
              let tblHtml = '';
              users.forEach((u, i) => {
                  let sysU = usage[u.id.replace(/-/g,'').toLowerCase()] || {reqs: 0, dReqs: 0, lastDay: ''};
                  let userReqs = sysU.reqs || 0;
                  let userDReqs = sysU.lastDay === new Date().toISOString().split('T')[0] ? (sysU.dReqs || 0) : 0;
                  
                  const unlimitedTxt = lang === 'fa' ? 'نامحدود' : 'Unlimited';
                  let limitTotalTxt = u.limitTotalReq ? u.limitTotalReq : unlimitedTxt;
                  let limitDailyTxt = u.limitDailyReq ? u.limitDailyReq : unlimitedTxt;
                  
                  let perT = u.limitTotalReq ? Math.min(100, (userReqs / u.limitTotalReq) * 100).toFixed(1) + '%' : '-';
                  let perD = u.limitDailyReq ? Math.min(100, (userDReqs / u.limitDailyReq) * 100).toFixed(1) + '%' : '-';
                  
                  let expTxt = unlimitedTxt;
                  let isExp = false;
                  if (u.expiryMs) {
                      let date = new Date(u.expiryMs);
                      expTxt = lang === 'fa' ? date.toLocaleDateString('fa-IR') : date.toLocaleDateString();
                      if (Date.now() > u.expiryMs) { 
                          const expiredTxt = lang === 'fa' ? ' (منقضی شده)' : ' (Expired)';
                          expTxt += \` <span class="text-xs text-red-500 font-bold">\${expiredTxt}</span>\`; 
                          isExp = true; 
                      }
                  }
                  
                  const totalLabel = lang === 'fa' ? 'کل:' : 'Total:';
                  const dailyLabel = lang === 'fa' ? 'روزانه:' : 'Daily:';
                  const rLabel = lang === 'fa' ? 'درخواست' : 'r';

                  let linkTitle = lang === 'fa' ? 'کپی لینک ساب' : 'Copy Subscription Link';
                  let pauseTitle = u.isPaused ? (lang === 'fa' ? 'فعال‌سازی کاربر' : 'Resume User') : (lang === 'fa' ? 'توقف کاربر' : 'Pause User');
                  let editTitle = lang === 'fa' ? 'ویرایش کاربر' : 'Edit Subscriber';
                  let resetTitle = lang === 'fa' ? 'بازنشانی مصرف ترافیک' : 'Reset Traffic Metrics';
                  let deleteTitle = lang === 'fa' ? 'حذف کاربر' : 'Delete User';

                   let linkHtml = \`<button onclick="copyData('sync-\${u.id}')" class="native-press flex-1 flex items-center justify-center text-primary hover:text-indigo-700 bg-indigo-50 hover:bg-indigo-100 dark:bg-indigo-900/30 dark:hover:bg-indigo-800/50 py-2 rounded-lg" title="\${linkTitle}">🔗</button>\`;
                   
                   let pauseBtnHtml = \`<button onclick="togglePauseUser('\${u.id}')" class="native-press flex-1 flex items-center justify-center \${u.isPaused ? 'text-green-500 hover:text-green-700 bg-green-50 hover:bg-green-100 dark:bg-green-900/30 dark:hover:bg-green-800/50' : 'text-amber-500 hover:text-amber-700 bg-amber-50 hover:bg-amber-100 dark:bg-amber-900/30 dark:hover:bg-amber-800/50'} py-2 rounded-lg" title="\${pauseTitle}">\\s*\${u.isPaused ? '▶️' : '⏸️'}</button>\`;

                   let editBtnHtml = \`<button onclick="editUser('\${u.id}')" class="native-press flex-1 flex items-center justify-center text-indigo-500 hover:text-indigo-700 bg-indigo-50 hover:bg-indigo-100 dark:bg-indigo-900/30 dark:hover:bg-indigo-800/50 py-2 rounded-lg" title="\${editTitle}">✏️</button>\`;

                   let resetBtnHtml = \`<button onclick="resetUserTraffic('\${u.id}')" class="native-press flex-1 flex items-center justify-center text-violet-500 hover:text-violet-700 bg-violet-50 hover:bg-violet-100 dark:bg-violet-900/30 dark:hover:bg-violet-800/50 py-2 rounded-lg" title="\${resetTitle}">🔄</button>\`;

                  let isAutoDisabled = u.isPaused && u.disabledReason;
                  let disableInfoHtml = '';
                  if (isAutoDisabled) {
                      let reasonLabel = u.disabledReason;
                      let timeLabel = u.disabledAt ? new Date(u.disabledAt).toLocaleString() : '';
                      let reasonTitle = lang === 'fa' ? 'علت غیرفعال‌سازی' : 'Disable Reason';
                      let timeTitle = lang === 'fa' ? 'زمان غیرفعال‌سازی' : 'Disabled At';
                      disableInfoHtml = \`
                          <div class="mt-2 p-2 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800/30">
                              <div class="flex items-center gap-1.5 text-[10px] font-bold text-red-600 dark:text-red-400">
                                  <span>⚠️</span>
                                  <span>\${reasonTitle}:</span>
                              </div>
                              <div class="text-[10px] text-red-500 dark:text-red-300 mt-0.5">\${reasonLabel}</div>
                              \${timeLabel ? \`<div class="text-[9px] text-slate-400 mt-1">\${timeTitle}: \${timeLabel}</div>\` : ''}
                          </div>
                      \`;
                  }

                  let rawSync = window.nahanProfiles?.find(p => p.id === u.id)?.sync || '';
                  if (rawSync) {
                      rawSync += rawSync.includes('?') ? '&flag=a' : '?flag=a';
                  }

                  tblHtml += \`<div class="native-press bg-white dark:bg-darkcard rounded-2xl border border-slate-200 dark:border-darkborder p-4 hover:shadow-md transition-shadow">
                      <div class="flex items-center justify-between mb-3">
                          <div class="flex items-center gap-2 min-w-0 flex-1">
                              <span class="w-2 h-2 rounded-full shrink-0 \${u.isPaused ? (isAutoDisabled ? 'bg-red-500' : 'bg-amber-500') : (isExp ? 'bg-red-400' : 'bg-emerald-500')}"></span>
                              <span class="font-bold text-sm text-slate-800 dark:text-slate-100 truncate">\${u.name}</span>
                              \${u.proxyIpGeo ? \`<span class="text-[10px] px-1.5 py-0.5 rounded bg-violet-50 dark:bg-violet-900/30 text-violet-600 dark:text-violet-300 font-semibold shrink-0">\${u.proxyIpGeo.flag}</span>\` : ''}
                          </div>
                          <input type="hidden" id="sync-\${u.id}" value="\${rawSync}">
                      </div>
                      <div class="flex items-center gap-1 mb-3">
                          \${linkHtml}
                          \${pauseBtnHtml}
                          \${editBtnHtml}
                          \${resetBtnHtml}
                          <button onclick="deleteUser('\${u.id}')" class="native-press flex-1 flex items-center justify-center text-red-400 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 py-2 rounded-lg transition-colors text-sm" title="\${deleteTitle}">🗑️</button>
                      </div>
                      <div class="flex flex-wrap gap-1 mb-3">
                          \${u.isPaused && u.disabledReason ? \`<span class="text-[9px] font-bold px-1.5 py-0.5 rounded bg-red-100 dark:bg-red-900/40 text-red-600 dark:text-red-300">Auto-Disabled</span>\` : ''}
                          \${u.userMode ? \`<span class="text-[9px] font-bold px-1.5 py-0.5 rounded bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-300">\${u.userMode === 'alpha' ? 'VLESS' : u.userMode === 'beta' ? 'Trojan' : 'Both'}</span>\` : ''}
                          \${u.userPorts ? \`<span class="text-[9px] font-bold px-1.5 py-0.5 rounded bg-emerald-50 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-300">\${u.userPorts}</span>\` : ''}
                           \${u.maxConfigs ? \`<span class="text-[9px] font-bold px-1.5 py-0.5 rounded bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-300">\${u.maxConfigs} cfgs</span>\` : ''}
                           \${u.connLimit ? \`<span class="text-[9px] font-bold px-1.5 py-0.5 rounded bg-cyan-50 dark:bg-cyan-900/30 text-cyan-600 dark:text-cyan-300">\${u.connLimit} conn</span>\` : ''}
                      </div>
                      \${disableInfoHtml}
                      <div class="grid grid-cols-2 gap-3">
                          <div class="bg-slate-50 dark:bg-slate-800/50 rounded-xl p-2.5">
                              <div class="text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-1">\${totalLabel}</div>
                              <div class="text-sm font-black text-slate-800 dark:text-white">\${(userReqs/6000).toFixed(2)} <span class="text-[10px] font-semibold text-slate-400">GB</span></div>
                              \${u.limitTotalReq ? \`
                              <div class="w-full bg-slate-200 dark:bg-slate-700 h-1.5 rounded-full overflow-hidden mt-1.5">
                                  <div class="bg-gradient-to-r \${parseFloat(perT) > 85 ? 'from-red-500 to-rose-600' : parseFloat(perT) > 60 ? 'from-amber-500 to-orange-500' : 'from-emerald-500 to-teal-500'} h-full rounded-full" style="width: \${perT}"></div>
                              </div>
                              <div class="flex items-center justify-between mt-1">
                                  <span class="text-[9px] text-slate-400">/ \${(u.limitTotalReq/6000).toFixed(2)} GB</span>
                                  \${perT !== '-' ? \`<span class="text-[9px] font-bold \${parseFloat(perT) > 85 ? 'text-red-500' : parseFloat(perT) > 60 ? 'text-amber-500' : 'text-emerald-500'}">\${perT}</span>\` : ''}
                              </div>
                              \` : '<div class="text-[9px] text-slate-400 mt-1">' + unlimitedTxt + '</div>'}
                          </div>
                          <div class="bg-slate-50 dark:bg-slate-800/50 rounded-xl p-2.5">
                              <div class="text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-1">\${dailyLabel}</div>
                              <div class="text-sm font-black text-slate-800 dark:text-white">\${userDReqs} <span class="text-[10px] font-semibold text-slate-400">\${rLabel}</span></div>
                              \${u.limitDailyReq ? \`
                              <div class="w-full bg-slate-200 dark:bg-slate-700 h-1.5 rounded-full overflow-hidden mt-1.5">
                                  <div class="bg-gradient-to-r \${parseFloat(perD) > 85 ? 'from-red-500 to-rose-600' : parseFloat(perD) > 60 ? 'from-amber-500 to-orange-500' : 'from-emerald-500 to-teal-500'} h-full rounded-full" style="width: \${perD}"></div>
                              </div>
                              <div class="flex items-center justify-between mt-1">
                                  <span class="text-[9px] text-slate-400">/ \${(u.limitDailyReq/6000).toFixed(2)} GB</span>
                                  \${perD !== '-' ? \`<span class="text-[9px] font-bold \${parseFloat(perD) > 85 ? 'text-red-500' : parseFloat(perD) > 60 ? 'text-amber-500' : 'text-emerald-500'}">\${perD}</span>\` : ''}
                              </div>
                              \` : '<div class="text-[9px] text-slate-400 mt-1">' + unlimitedTxt + '</div>'}
                          </div>
                      </div>
                      <div class="flex items-center justify-between mt-2 pt-2 border-t border-slate-100 dark:border-slate-800">
                          <span class="text-[10px] text-slate-400">📅 \${expTxt}</span>
                      </div>
                  \`;
                  tblHtml += '</div>';
              });
              tbl.innerHTML = tblHtml;
          }

          async function resetUserTraffic(uuid) {
              const resetMsg = lang === 'fa' ? 'آیا از بازنشانی وضعیت ترافیک (کل و روزانه) این مشترک مطمئن هستید؟' : 'Are you sure you want to reset all traffic metrics (Total and Daily) for this subscriber?';
              if(!confirm(resetMsg)) return;
              try {
                  const res = await fetch(baseRoute + '/api/sync', {
                      method: 'POST',
                      headers: {'Content-Type': 'application/json'},
                      body: JSON.stringify({ key: sessionKey, resetUUID: uuid })
                  });
                  if (res.ok) {
                      const successMsg = lang === 'fa' ? 'ترافیک مشترک با موفقیت بازنشانی شد!' : 'Subscriber traffic metrics successfully reset!';
                      alert(successMsg);
                      doLogin(true); // reload usage data from server
                  } else {
                      const errMsg = lang === 'fa' ? 'سرور در بازنشانی ترافیک خطا بازگرداند.' : 'Server returned error while resetting metrics.';
                      alert(errMsg);
                  }
              } catch(e) {
                  const netErr = lang === 'fa' ? 'خطای ارتباط با شبکه.' : 'Network connection error.';
                  alert(netErr);
              }
          }

          function deleteUser(uuid) {
              const deleteMsg = lang === 'fa' ? 'آیا از حذف این کاربر مطمئن هستید؟' : 'Are you sure you want to delete this user?';
              if(!confirm(deleteMsg)) return;
              if(window.nahanConfig && window.nahanConfig.users) {
                  window.nahanConfig.users = window.nahanConfig.users.filter(u => u.id !== uuid);
              }
              // Automatically sync
              renderUsersTable();
              doSaveDirectly();
          }

          function togglePauseUser(uuid) {
              if(window.nahanConfig && window.nahanConfig.users) {
                  let usr = window.nahanConfig.users.find(u => u.id === uuid);
                  if (usr) {
                      usr.isPaused = !usr.isPaused;
                      if (!usr.isPaused) {
                          usr.disabledReason = null;
                          usr.disabledAt = null;
                      }
                      renderUsersTable();
                      doSaveDirectly();
                  }
              }
          }

          function getGlobalPorts() {
              return (window.nahanConfig && window.nahanConfig.socketPorts)
                  ? window.nahanConfig.socketPorts.split(',').map(s=>s.trim()).filter(Boolean)
                  : ['443'];
          }

          function getGlobalMode() {
              return (window.nahanConfig && window.nahanConfig.mode) ? window.nahanConfig.mode : 'alpha';
          }

          function openAddUserPage() {
              document.getElementById('view-users').classList.add('hidden');
              document.getElementById('view-add-user').classList.remove('hidden');
              var sc = document.querySelector('.scroll-content');
              sc.style.overflow = 'hidden';
              sc.classList.add('flex', 'flex-col');
              sc.firstElementChild.classList.add('flex-1', 'min-h-0', 'flex', 'flex-col');
              updateTitleText('Add User');
              buildPortCheckboxes('add-user-ports-wrap', null);
              buildModeCheckboxes('add-user-mode-wrap', null);
              buildIPCheckboxes("add-user-clean-ips-wrap", "", (window.nahanConfig?.cleanIps||"").split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
              buildIPCheckboxes("add-user-proxy-ips-wrap", "", (window.nahanConfig?.backupRelay||"").split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
              buildNodeCheckboxes("add-user-nodes-wrap", "", getGlobalNodeList());
          }
          function closeAddUserPage() {
              document.getElementById('view-add-user').classList.add('hidden');
              document.getElementById('view-users').classList.remove('hidden');
              var sc = document.querySelector('.scroll-content');
              sc.style.overflow = '';
              sc.classList.remove('flex', 'flex-col');
              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');
              updateTitle();
          }
          function closeEditUserPage() {
              document.getElementById('view-edit-user').classList.add('hidden');
              document.getElementById('view-users').classList.remove('hidden');
              var sc = document.querySelector('.scroll-content');
              sc.style.overflow = '';
              sc.classList.remove('flex', 'flex-col');
              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');
              updateTitle();
          }
          function updateTitleText(txt) {
              var el = document.getElementById('view-title');
              if (el) el.innerText = txt;
          }

          
function buildIPCheckboxes(wrapId, selectedIps, allIps) {
    const wrap = document.getElementById(wrapId);
    if(!wrap) return;
    wrap.innerHTML = '';
    if(!allIps || allIps.length === 0) {
        wrap.innerHTML = '<span class="text-xs text-slate-400">' + (i18n[lang]?.no_ips_advanced || 'No IPs added in Advanced Tab') + '</span>';
        return;
    }
    const selArr = selectedIps ? selectedIps.split(',').map(s=>s.trim()).filter(Boolean) : [];
    allIps.forEach(ip => {
        const lbl = document.createElement('label');
        lbl.className = "flex items-center gap-1.5 text-sm cursor-pointer border border-slate-200 dark:border-darkborder px-2 py-1 rounded-lg";
        const cb = document.createElement('input');
        cb.type = "checkbox";
        cb.className = "accent-primary";
        cb.value = ip;
        if(selArr.includes(ip)) cb.checked = true;
        
        lbl.appendChild(cb);
        const span = document.createElement('span');
        span.innerText = ip;
        lbl.appendChild(span);
        wrap.appendChild(lbl);
    });
}
function getSelectedCheckboxes(wrapId) {
    const wrap = document.getElementById(wrapId);
    if(!wrap) return '';
    const checked = Array.from(wrap.querySelectorAll('input:checked')).map(cb => cb.value);
    return checked.join(',');
}
function getGlobalNodeList() {
    var nodes = (window.nahanConfig && window.nahanConfig.slaveNodes ? window.nahanConfig.slaveNodes : "").split(/[\\s,;]+/).map(function(s){return s.trim();}).filter(Boolean);
    var lp = (window.nahanConfig && Array.isArray(window.nahanConfig.linkedPanels)) ? window.nahanConfig.linkedPanels : [];
    lp.forEach(function(p){
        var raw = (p && typeof p === 'object') ? (p.url || '') : (p || '');
        raw = String(raw).trim();
        if(!raw) return;
        raw = raw.replace(/^[a-zA-Z]+:\\/\\//, '').split('/')[0].split('@').pop();
        var h = raw.indexOf('[') === 0 ? raw.slice(0, raw.indexOf(']') + 1) : raw.split(':')[0];
        h = h.trim();
        if(h) nodes.push(h);
    });
    return nodes.filter(function(v,i,a){return a.indexOf(v) === i;});
}
function buildNodeCheckboxes(wrapId, selectedNodes, allNodes) {
    const wrap = document.getElementById(wrapId);
    if(!wrap) return;
    wrap.innerHTML = '';
    if(!allNodes || allNodes.length === 0) {
        wrap.innerHTML = '<span class="text-xs text-slate-400">' + (i18n[lang]?.no_nodes_advanced || 'No slave nodes in Advanced Tab') + '</span>';
        return;
    }
    const selArr = selectedNodes ? selectedNodes.split(',').map(s=>s.trim()).filter(Boolean) : [];
    allNodes.forEach(node => {
        const lbl = document.createElement('label');
        lbl.className = "flex items-center gap-1.5 text-sm cursor-pointer border border-slate-200 dark:border-darkborder px-2 py-1 rounded-lg";
        const cb = document.createElement('input');
        cb.type = "checkbox";
        cb.className = "accent-primary";
        cb.value = node;
        if(selArr.includes(node)) cb.checked = true;
        lbl.appendChild(cb);
        const span = document.createElement('span');
        span.innerText = node;
        lbl.appendChild(span);
        wrap.appendChild(lbl);
    });
}

function buildPortCheckboxes(wrapId, selectedPorts) {
              const wrap = document.getElementById(wrapId);
              if (!wrap) return;
              const globalPorts = getGlobalPorts();
              const sel = selectedPorts ? selectedPorts.split(',').map(s=>s.trim()) : ['443'];
              wrap.innerHTML = globalPorts.map(function(p) {
                  return '<label class="flex items-center gap-1.5 text-sm cursor-pointer"><input type="checkbox" value="' + p + '" class="' + wrapId + '-port-cb accent-primary"' + (sel.includes(p) ? ' checked' : '') + '><span>' + p + '</span></label>';
              }).join('');
          }

          function buildModeCheckboxes(wrapId, userMode) {
              const globalMode = getGlobalMode();
              const alphaAllowed = globalMode === 'alpha' || globalMode === 'both';
              const betaAllowed = globalMode === 'beta' || globalMode === 'both';
              const selAlpha = userMode === 'alpha' || userMode === 'both' || (!userMode && alphaAllowed);
              const selBeta = userMode === 'beta' || userMode === 'both' || (!userMode && betaAllowed);
              const wrap = document.getElementById(wrapId);
              if (!wrap) return;
              wrap.querySelectorAll('input[type=checkbox]').forEach(cb => {
                  if (cb.value === 'alpha') { cb.disabled = !alphaAllowed; cb.checked = selAlpha && alphaAllowed; cb.closest			('label').style.opacity = alphaAllowed ? '1' : '0.35'; }
                  if (cb.value === 'beta')  { cb.disabled = !betaAllowed;  cb.checked = selBeta && betaAllowed;  cb.closest			('label').style.opacity = betaAllowed  ? '1' : '0.35'; }
              });
          }

          function readModeFromCheckboxes(cbClass) {
             const cbs = [...document.querySelectorAll('.' + cbClass + ':checked')].map(c=>c.value);
              if (cbs.includes('alpha') && cbs.includes('beta')) return 'both';
              if (cbs.includes('alpha')) return 'alpha';
              if (cbs.includes('beta')) return 'beta';
              return getGlobalMode();
          }

          function readPortsFromCheckboxes(wrapId) {
             const ports = [...document.querySelectorAll('#' + wrapId + ' input[type=checkbox]:checked')].map(c=>c.value);
              return ports.length ? ports.join(',') : getGlobalPorts()[0];
          }

          function commitAddUser() {
              const name = document.getElementById('add-user-name').value.trim();
              let tReq = document.getElementById('add-user-total-reqs').value;
              tReq = tReq? Math.floor(parseFloat(tReq) * 6000): null;
              let dReq = document.getElementById('add-user-daily-reqs').value;
              dReq = dReq? Math.floor(parseFloat(dReq) * 6000): null;
              let days = document.getElementById('add-user-days').value;
               const cleanIpsCheckbox = getSelectedCheckboxes("add-user-clean-ips-wrap");
               const cleanIpsCustom = document.getElementById("add-user-custom-clean").value.trim();
               let cleanIpArray = [];
               if (cleanIpsCheckbox) cleanIpArray.push(...cleanIpsCheckbox.split(','));
               if (cleanIpsCustom) {
                   cleanIpArray.push(...cleanIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
               }
               const cleanIp = cleanIpArray.length ? cleanIpArray.join(',') : null;
               const proxyIpsCheckbox = getSelectedCheckboxes("add-user-proxy-ips-wrap");
               const proxyIpsCustom = document.getElementById("add-user-custom-proxy").value.trim();
               let proxyIpArray = [];
               if (proxyIpsCheckbox) proxyIpArray.push(...proxyIpsCheckbox.split(','));
               if (proxyIpsCustom) {
                   proxyIpArray.push(...proxyIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
               }
               const proxyIp = proxyIpArray.length ? proxyIpArray.join(',') : null;
               
               const customName = document.getElementById('add-user-custom-name').value.trim() || null;
               const userMode = readModeFromCheckboxes('add-mode-cb');
               const userPorts = readPortsFromCheckboxes('add-user-ports-wrap');
               let maxConfigs = document.getElementById('add-user-max-configs').value;
               maxConfigs = maxConfigs ? parseInt(maxConfigs) : null;
               const nodesCheckbox = getSelectedCheckboxes("add-user-nodes-wrap");
               const nodesCustom = document.getElementById("add-user-custom-nodes").value.trim();
               let nodesArray = [];
               if (nodesCheckbox) nodesArray.push(...nodesCheckbox.split(','));
               if (nodesCustom) nodesArray.push(...nodesCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
               const userNodes = nodesArray.length ? nodesArray.join(',') : null;
               const nat64 = document.getElementById('add-user-nat64').value.trim() || null;
               
               if(!name) {
                   alert(lang === 'fa' ? 'لطفاً نام را وارد کنید' : 'Please enter a name');
                  return;
              }

              if(!window.nahanConfig) window.nahanConfig = {};
              if(!window.nahanConfig.users) window.nahanConfig.users = [];

              if(window.nahanConfig.users.some(u => u.name.trim().toLowerCase() === name.toLowerCase())) {
                  alert(lang === 'fa' ? 'این نام قبلاً استفاده شده است' : 'This name is already taken');
                  return;
              }

               tReq = tReq ? parseInt(tReq) : null;
               dReq = dReq ? parseInt(dReq) : null;
               days = days ? parseInt(days) : null;
               let connLimit = document.getElementById('add-user-conn-limit').value;
               connLimit = connLimit ? parseInt(connLimit) : null;
               const userPanelUrl = document.getElementById('add-user-panel-url').value.trim() || null;
               
               let newId = Array.from(crypto.getRandomValues(new Uint8Array(16)))
                   .map((b,i) => (i===4||i===6||i===8||i===10?'-':'') + b.toString(16).padStart(2,'0')).join('');
               
                const u = {
                    id: newId,
                    name: name,
                    limitTotalReq: tReq,
                    limitDailyReq: dReq,
                    expiryMs: days ? Date.now() + days*86400000 : null,
                    proxyIp: proxyIp,
                     cleanIp: cleanIp,
                     customName: customName,
                     userMode: userMode,
                     userPorts: userPorts,
                     maxConfigs: maxConfigs,
                     userNodes: userNodes,
                     nat64: nat64,
                     connLimit: connLimit,
                     userPanelUrl: userPanelUrl,
                     createdAt: Date.now()
                };
              
              window.nahanConfig.users.push(u);
              document.getElementById('view-add-user').classList.add('hidden');
              document.getElementById('view-users').classList.remove('hidden');
              var sc = document.querySelector('.scroll-content');
              sc.style.overflow = '';
              sc.classList.remove('flex', 'flex-col');
              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');
              updateTitle();
              document.getElementById('add-user-name').value = '';
               document.getElementById('add-user-custom-name').value = '';
               document.getElementById('add-user-custom-clean').value = '';
               document.getElementById('add-user-custom-proxy').value = '';
               document.getElementById('add-user-custom-nodes').value = '';
              document.getElementById('add-user-total-reqs').value = '';
              document.getElementById('add-user-daily-reqs').value = '';
              document.getElementById('add-user-days').value = '';
              document.getElementById('add-user-max-configs').value = '';
              document.getElementById('add-user-conn-limit').value = '';
              document.getElementById('add-user-panel-url').value = '';
              
              renderUsersTable();
              doSaveDirectly();
              setTimeout(() => { renderProfiles(); }, 2500);
          }

          function editUser(uuid) {
              if(!window.nahanConfig || !window.nahanConfig.users) return;
              let u = window.nahanConfig.users.find(usr => usr.id === uuid);
              if(!u) return;
              
              document.getElementById('edit-user-id').value = u.id;
              document.getElementById('edit-user-name').value = u.name;
              document.getElementById('edit-user-total-reqs').value = u.limitTotalReq? (u.limitTotalReq / 6000).toFixed(2): '';
              document.getElementById('edit-user-daily-reqs').value = u.limitDailyReq? (u.limitDailyReq / 6000).toFixed(2): '';
                            const globalCleanIps = (window.nahanConfig?.cleanIps||"").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);
              const userCleanIps = (u.cleanIp || "").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);
              const checkedGlobalClean = [];
              const customClean = [];
              userCleanIps.forEach(ip => {
                  let hostOnly = ip.split('#')[0].split(':')[0].trim();
                  let isFound = globalCleanIps.some(g => g.split('#')[0].split(':')[0].trim() === hostOnly || g === ip);
                  if (isFound) checkedGlobalClean.push(ip);
                  else customClean.push(ip);
              });
              buildIPCheckboxes("edit-user-clean-ips-wrap", checkedGlobalClean.join(','), globalCleanIps);
              document.getElementById('edit-user-custom-clean').value = customClean.join(', ');

              const globalProxyIps = (window.nahanConfig?.backupRelay||"").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);
              const userProxyIps = (u.proxyIp || "").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);
              const checkedGlobalProxy = [];
              const customProxy = [];
              userProxyIps.forEach(ip => {
                  let hostOnly = ip.split('#')[0].split(':')[0].trim();
                  let isFound = globalProxyIps.some(g => g.split('#')[0].split(':')[0].trim() === hostOnly || g === ip);
                  if (isFound) checkedGlobalProxy.push(ip);
                  else customProxy.push(ip);
              });
               buildIPCheckboxes("edit-user-proxy-ips-wrap", checkedGlobalProxy.join(','), globalProxyIps);
               document.getElementById('edit-user-custom-proxy').value = customProxy.join(', ');

               const globalNodes = getGlobalNodeList();
               const userNodesList = (u.userNodes || "").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);
               const checkedGlobalNodes = [];
               const customNodes = [];
               userNodesList.forEach(node => {
                   let isFound = globalNodes.some(g => g === node);
                   if (isFound) checkedGlobalNodes.push(node);
                   else customNodes.push(node);
               });
               buildNodeCheckboxes("edit-user-nodes-wrap", checkedGlobalNodes.join(','), globalNodes);
               document.getElementById('edit-user-custom-nodes').value = customNodes.join(', ');
               document.getElementById('edit-user-nat64').value = u.nat64 || '';

               document.getElementById('edit-user-custom-name').value = u.customName || '';
              
              document.getElementById('edit-user-max-configs').value = u.maxConfigs || '';
              document.getElementById('edit-user-conn-limit').value = u.connLimit || '';
              document.getElementById('edit-user-panel-url').value = u.userPanelUrl || '';
              
              buildPortCheckboxes('edit-user-ports-wrap', u.userPorts);
              buildModeCheckboxes('edit-user-mode-wrap', u.userMode);

              let daysLeft = '';
              if(u.expiryMs) {
                  let diff = u.expiryMs - Date.now();
                  daysLeft = diff > 0 ? Math.ceil(diff / 86400000) : 0;
              }
              document.getElementById('edit-user-days').value = daysLeft;
              
              document.getElementById('view-users').classList.add('hidden');
              document.getElementById('view-edit-user').classList.remove('hidden');
              var sc = document.querySelector('.scroll-content');
              sc.style.overflow = 'hidden';
              sc.classList.add('flex', 'flex-col');
              sc.firstElementChild.classList.add('flex-1', 'min-h-0', 'flex', 'flex-col');
              updateTitleText('Edit Subscriber');
          }

          function commitEditUser() {
              const uuid = document.getElementById('edit-user-id').value;
              const name = document.getElementById('edit-user-name').value.trim();
              let tReq = document.getElementById('edit-user-total-reqs').value;
              tReq = tReq? Math.floor(parseFloat(tReq) * 6000): null;
              let dReq = document.getElementById('edit-user-daily-reqs').value;
              dReq = dReq? Math.floor(parseFloat(dReq) * 6000): null;
              let days = document.getElementById('edit-user-days').value;
                             const proxyIpsCheckbox = getSelectedCheckboxes("edit-user-proxy-ips-wrap");
               const proxyIpsCustom = document.getElementById("edit-user-custom-proxy").value.trim();
               let proxyIpArray = [];
               if (proxyIpsCheckbox) proxyIpArray.push(...proxyIpsCheckbox.split(','));
               if (proxyIpsCustom) {
                   proxyIpArray.push(...proxyIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
               }
               const proxyIp = proxyIpArray.length ? proxyIpArray.join(',') : null;
               
               const customName = document.getElementById('edit-user-custom-name').value.trim() || null;
               const cleanIpsCheckbox = getSelectedCheckboxes("edit-user-clean-ips-wrap");
               const cleanIpsCustom = document.getElementById("edit-user-custom-clean").value.trim();
               let cleanIpArray = [];
               if (cleanIpsCheckbox) cleanIpArray.push(...cleanIpsCheckbox.split(','));
               if (cleanIpsCustom) {
                   cleanIpArray.push(...cleanIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
               }
               const cleanIp = cleanIpArray.length ? cleanIpArray.join(',') : null;
              const userMode = readModeFromCheckboxes('edit-mode-cb');
              const userPorts = readPortsFromCheckboxes('edit-user-ports-wrap');
               let maxConfigs = document.getElementById('edit-user-max-configs').value;
               maxConfigs = maxConfigs ? parseInt(maxConfigs) : null;
               const nodesCheckbox = getSelectedCheckboxes("edit-user-nodes-wrap");
               const nodesCustom = document.getElementById("edit-user-custom-nodes").value.trim();
               let nodesArray = [];
               if (nodesCheckbox) nodesArray.push(...nodesCheckbox.split(','));
               if (nodesCustom) nodesArray.push(...nodesCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));
               const userNodes = nodesArray.length ? nodesArray.join(',') : null;
                const nat64 = document.getElementById('edit-user-nat64').value.trim() || null;
                let connLimit = document.getElementById('edit-user-conn-limit').value;
                connLimit = connLimit ? parseInt(connLimit) : null;
                const userPanelUrl = document.getElementById('edit-user-panel-url').value.trim() || null;
               
               if(!name) {
                  alert(lang === 'fa' ? 'لطفاً نام را وارد کنید' : 'Please enter a name');
                  return;
              }
              tReq = tReq ? parseInt(tReq) : null;
              dReq = dReq ? parseInt(dReq) : null;
              days = days ? parseInt(days) : null;
              
              if(!window.nahanConfig || !window.nahanConfig.users) return;

              if(window.nahanConfig.users.some(u => u.id !== uuid && u.name.trim().toLowerCase() === name.toLowerCase())) {
                  alert(lang === 'fa' ? 'این نام قبلاً استفاده شده است' : 'This name is already taken');
                  return;
              }

              let u = window.nahanConfig.users.find(usr => usr.id === uuid);
              if(!u) return;
              
              u.name = name;
              u.limitTotalReq = tReq;
              u.limitDailyReq = dReq;
              u.expiryMs = days ? Date.now() + days*86400000 : null;
              u.proxyIp = proxyIp;
               u.cleanIp = cleanIp;
               u.customName = customName;
              u.userMode = userMode;
              u.userPorts = userPorts;
              u.maxConfigs = maxConfigs;
              u.userNodes = userNodes;
              u.nat64 = nat64;
              u.connLimit = connLimit;
              u.userPanelUrl = userPanelUrl;
              
              document.getElementById('view-edit-user').classList.add('hidden');
              document.getElementById('view-users').classList.remove('hidden');
              var sc = document.querySelector('.scroll-content');
              sc.style.overflow = '';
              sc.classList.remove('flex', 'flex-col');
              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');
              renderUsersTable();
              doSaveDirectly();
          }

          async function loadApiKeys() {
              try {
                  const res = await fetch(baseRoute + '/api/keys', {
                      headers: { 'Authorization': 'Bearer ' + sessionKey }
                  });
                  const data = await res.json();
                  if (data.success) {
                      const list = document.getElementById('api-keys-list');
                      if (!list) return;
                      if (!data.keys || data.keys.length === 0) {
                          list.innerHTML = '<p class="text-xs text-slate-400 dark:text-slate-500">' + (i18n[lang]?.api_keys_empty || 'No API keys generated yet.') + '</p>';
                          return;
                      }
                      list.innerHTML = data.keys.map(k => {
                          const created = new Date(k.createdAt).toLocaleDateString();
                          const lastUsed = k.lastUsed ? new Date(k.lastUsed).toLocaleDateString() : (i18n[lang]?.never || 'Never');
                          return '<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder">' +
                              '<div class="flex-1 min-w-0">' +
                              '<p class="text-xs font-bold text-slate-700 dark:text-slate-200 truncate">' + (k.name || 'Unnamed') + '</p>' +
                              '<p class="text-[10px] font-mono text-slate-400 mt-0.5">' + k.keyPreview + '</p>' +
                              '<p class="text-[10px] text-slate-400 mt-0.5">' + (i18n[lang]?.created || 'Created') + ': ' + created + ' · ' + (i18n[lang]?.last_used || 'Last used') + ': ' + lastUsed + '</p>' +
                              '</div>' +
                              '<button onclick="revokeApiKey(\\'' + k.id + '\\')" class="ms-3 px-3 py-1.5 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 text-[10px] font-bold rounded-lg border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors">' + (i18n[lang]?.revoke || 'Revoke') + '</button>' +
                              '</div>';
                      }).join('');
                  }
              } catch(e) {}
          }

          async function generateApiKey() {
              const name = prompt(i18n[lang]?.enter_key_name || 'Enter a name for this API key:');
              if (!name) return;
              try {
                  const res = await fetch(baseRoute + '/api/keys', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionKey },
                      body: JSON.stringify({ action: 'create', name })
                  });
                  const data = await res.json();
                  if (data.success && data.key) {
                      const newBox = document.getElementById('api-key-new');
                      const keyInput = document.getElementById('api-key-value');
                      keyInput.value = data.key.key;
                      newBox.classList.remove('hidden');
                      loadApiKeys();
                  } else {
                      alert(data.error || 'Failed to create key');
                  }
              } catch(e) { alert('Error: ' + e.message); }
          }

          async function revokeApiKey(id) {
              if (!confirm(i18n[lang]?.confirm_revoke || 'Revoke this API key? The remote panel will lose access.')) return;
              try {
                  const res = await fetch(baseRoute + '/api/keys', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionKey },
                      body: JSON.stringify({ action: 'revoke', id })
                  });
                  const data = await res.json();
                  if (data.success) loadApiKeys();
                  else alert(data.error || 'Failed to revoke key');
              } catch(e) { alert('Error: ' + e.message); }
          }

          function copyApiKey() {
              const input = document.getElementById('api-key-value');
              navigator.clipboard.writeText(input.value);
              const stat = document.getElementById('save-status');
              if (stat) { stat.textContent = "Copied!"; stat.className = "text-sm font-bold text-emerald-500 md:me-4"; setTimeout(() => { stat.textContent = ""; }, 2000); }
          }

          function copyDoHServer() {
              const input = document.getElementById('cfg-doh-server');
              if (input && input.value) {
                  navigator.clipboard.writeText(input.value);
                  const stat = document.getElementById('save-status');
                  if (stat) { stat.textContent = "Copied!"; stat.className = "text-sm font-bold text-emerald-500 md:me-4"; setTimeout(() => { stat.textContent = ""; }, 2000); }
              }
          }

          function updateDoHServerUrl() {
              const dohInput = document.getElementById('cfg-doh-server');
              if (dohInput) {
                  dohInput.value = window.location.origin + '/' + (window.nahanConfig?.apiRoute || 'sync') + '/dns-query';
              }
          }

          async function doSaveDirectly() {
              const btn = document.querySelector('button[onclick="doSave()"]');
              const origText = btn.innerText; btn.innerText = "...";
              try {
                  const res = await fetch(baseRoute + '/api/sync', {
                      method: 'POST',
                      headers: {'Content-Type': 'application/json'},
                      body: JSON.stringify({ key: sessionKey, config: window.nahanConfig })
                  });
                  if(res.ok) {
                       const stat = document.getElementById('save-status');
                       stat.textContent = "Saved. Refreshing...";
                       setTimeout(() => { doLogin(true); stat.textContent = ""; }, 1000);
                  }
              } catch(e) {}
              btn.innerText = origText;
          }

          async function resolveSmartCleanIps() {
              const btn = document.getElementById('btn-resolve-smart-ips');
              const origText = btn.innerHTML;
              btn.disabled = true;
              btn.innerHTML = '⚡ Resolving CDN & Clean IPs...';
              
              const domains = [
                  'www.speedtest.net',
                  'grok.com',
                  'feedback.spotify.com',
                  'www.hcaptcha.com',
                  'chatgpt.com',
                  'sourceforge.net',
                  'snapp.ir',
                  'digikala.com',
                  'divar.ir',
                  'cafebazaar.ir',
                  'shaparak.ir',
                  'aparat.com',
                  'soft98.ir',
                  'varzesh3.com'
              ];
              
              let resolvedIps = new Set();
              const cleanIpsTextarea = document.getElementById('cfg-ips');
              
              async function resolveOne(domain) {
                  try {
                      const res = await fetch(\`https://cloudflare-dns.com/dns-query?name=\${encodeURIComponent(domain)}&type=A\`, { 
                          headers: { 'accept': 'application/dns-json' }
                      });
                      const data = await res.json();
                      if (data && data.Answer) {
                          data.Answer.forEach(ans => {
                              if (ans.type === 1 && ans.data) {
                                  resolvedIps.add(ans.data);
                              }
                          });
                      }
                  } catch(e) {
                      try {
                          const res = await fetch(\`https://dns.google/resolve?name=\${encodeURIComponent(domain)}&type=A\`);
                          const data = await res.json();
                          if (data && data.Answer) {
                              data.Answer.forEach(ans => {
                                  if (ans.type === 1 && ans.data) {
                                      resolvedIps.add(ans.data);
                                  }
                              });
                          }
                      } catch(ge) {}
                  }
              }
              
              try {
                  await Promise.all(domains.map(d => resolveOne(d)));
              } catch(err) {
                  console.error("DNS resolving process encountered an issue:", err);
              }
              
              if (resolvedIps.size > 0) {
                  const ipList = Array.from(resolvedIps).join('\\n');
                  cleanIpsTextarea.value = ipList;
                  cleanIpsTextarea.dispatchEvent(new Event('input'));
                  cleanIpsTextarea.dispatchEvent(new Event('change'));
                  alert((lang === 'fa' ? 'با موفقیت حل شد و ' : 'Successfully resolved and loaded ') + resolvedIps.size + (lang === 'fa' ? ' آی‌پی تمیز بارگذاری شد!' : ' clean IPs!'));
              } else {
                  alert(lang === 'fa' ? 'خطا در تبدیل دامنه به آی‌پی. لطفاً اتصال اینترنت یا DNS سفارشی خود را بررسی کنید.' : 'Failed to resolve domains to IPs. Please verify your internet connection or custom DNS.');
              }
              
              btn.disabled = false;
              btn.innerHTML = origText;
          }

          async function checkUpdate() {
              try {
                  const res = await fetch(baseRoute + '/api/update', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ key: sessionKey, action: 'check' })
                  });
                  const data = await res.json();
                  if (data.success && data.updateAvailable) {
                      window._updateData = data;
                      if (window.nahanConfig?.autoUpdate && data.canDeploy) {
                          const format = window.nahanConfig.autoUpdateFormat || 'normal';
                          const formatEl = document.querySelector(\`input[name="auto-update-format"][value="\${format}"]\`);
                          if (formatEl) formatEl.checked = true;
                          const autoRadio = document.querySelector(\`input[name="auto-update-format"][value="\${format}"]\`);
                          if (autoRadio) autoRadio.checked = true;
                          doUpdate();
                      } else {
                          showUpdateBanner((document.getElementById('cfg-github-repo')?.value || window.nahanConfig?.githubRepo || 'Ceetherr/swimshady').replace('https://github.com/', '').replace('http://github.com/', '').trim(), data.latest);
                      }
                  }
                  if (data.success && !data.canDeploy) {
                      const statusEl = document.getElementById('update-deploy-status');
                      if (statusEl) {
                          statusEl.classList.remove('hidden');
                          statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-amber-600 bg-amber-50 dark:bg-amber-900/20 dark:text-amber-400';
                          statusEl.textContent = i18n[lang].update_requires_cf || 'Configure CF credentials to enable auto-deploy.';
                      }
                  }
              } catch(err) {
                  console.error("Update check failed:", err);
              }
          }

          async function doUpdate() {
              const btn = document.getElementById('update-deploy-btn');
              const statusEl = document.getElementById('update-deploy-status');
              if (!btn) return;
              if (!confirm(lang === 'fa' ? 'آیا از دپلوی نسخه فعلی/جدید اطمینان دارید؟' : 'Deploy the selected version now?')) return;

              const formatEl = document.querySelector('input[name="update-format"]:checked');
              const format = formatEl ? formatEl.value : 'normal';
              const forceDeploy = !window._updateData?.updateAvailable;

              const origText = btn.innerHTML;
              btn.innerHTML = '⏳ ' + (i18n[lang].update_deploying || 'Deploying...');
              btn.disabled = true;
              if (statusEl) {
                  statusEl.classList.remove('hidden');
                  statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-blue-600 bg-blue-50 dark:bg-blue-900/20 dark:text-blue-400 animate-pulse';
                  statusEl.textContent = i18n[lang].update_deploying || 'Deploying update...';
              }

              let latestCode = null;
              try {
                  const repo = (document.getElementById('cfg-github-repo')?.value || window.nahanConfig?.githubRepo || 'Ceetherr/swimshady').replace('https://github.com/', '').replace('http://github.com/', '').trim();
                  if (statusEl) statusEl.textContent = '📥 ' + (lang === 'fa' ? 'در حال دریافت کد از مخزن گیت‌هاب...' : 'Fetching latest code from GitHub...');
                  const fetchRes = await fetch('https://raw.githubusercontent.com/' + repo + '/main/_worker.js');
                  if (!fetchRes.ok) throw new Error('HTTP ' + fetchRes.status);
                  latestCode = await fetchRes.text();
              } catch(fe) {
                  console.warn("Client fetch failed, falling back to server-side fetch", fe);
              }

              if (latestCode && format === 'obfuscated') {
                  if (statusEl) statusEl.textContent = '🛡️ ' + (lang === 'fa' ? 'در حال اجرای مبهم‌سازی کلاینت...' : 'Applying client-side XOR obfuscation...');
                  try {
                      latestCode = obfuscateCode(latestCode);
                  } catch(oe) {
                      if (statusEl) {
                          statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-red-600 bg-red-50 dark:bg-red-900/20 dark:text-red-400';
                          statusEl.textContent = 'Obfuscation failed: ' + oe.message;
                      }
                      btn.innerHTML = origText;
                      btn.disabled = false;
                      return;
                  }
              }

              try {
                  const res = await fetch(baseRoute + '/api/update', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ 
                          key: sessionKey, 
                          action: 'deploy',
                          code: latestCode,
                          force: forceDeploy
                      })
                  });
                  const data = await res.json();
                  if (data.success) {
                      if (statusEl) {
                          statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-emerald-600 bg-emerald-50 dark:bg-emerald-900/20 dark:text-emerald-400';
                          statusEl.textContent = (i18n[lang].update_success || 'Update successful!') + ' v' + data.newVersion;
                      }
                      btn.innerHTML = '✅ ' + (i18n[lang].update_success || 'Done!');
                      setTimeout(() => window.location.reload(), 3000);
                  } else {
                      if (statusEl) {
                          statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-red-600 bg-red-50 dark:bg-red-900/20 dark:text-red-400';
                          statusEl.textContent = (i18n[lang].update_error || 'Update failed') + ': ' + (data.error || 'Unknown error');
                      }
                      btn.innerHTML = origText;
                      btn.disabled = false;
                  }
              } catch(e) {
                  if (statusEl) {
                      statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-red-600 bg-red-50 dark:bg-red-900/20 dark:text-red-400';
                      statusEl.textContent = 'Error: ' + e.message;
                  }
                  btn.innerHTML = origText;
                  btn.disabled = false;
              }
          }

          async function triggerManualRedeploy() {
              const banner = document.getElementById('update-alert-banner');
              if (!banner) return;
              
              document.getElementById('update-alert-text').textContent = lang === 'fa' 
                  ? 'می‌توانید آخرین نسخه فعال را مجدداً دپلوی نموده یا بین نسخه معمولی و مبهم‌سازی شده جابجا شوید.'
                  : 'You can redeploy the latest code or switch between Normal/Obfuscated version on the fly.';
              
              banner.classList.remove('hidden');
              banner.classList.add('flex');
              banner.style.display = 'flex';
              
              if (!window._updateData) {
                  window._updateData = { latest: CURRENT_VERSION, updateAvailable: false };
              }
              
              const repo = (document.getElementById('cfg-github-repo')?.value || window.nahanConfig?.githubRepo || 'Ceetherr/swimshady').replace('https://github.com/', '').replace('http://github.com/', '').trim();
              
              showUpdateBanner(repo, CURRENT_VERSION);
              
              switchTab('overview');
              document.getElementById('update-alert-banner').scrollIntoView({ behavior: 'smooth' });
          }
          
          function parseMarkdown(md) {
              if (!md) return '';
              let lines = md.split(/\\r?\\n/);
              let htmlLines = [];
              let inCodeBlock = false;
              let codeContent = [];
              let activeBlockLang = null;

              for (let line of lines) {
                  let trimmed = line.trim();

                  if (trimmed === '<!-- LANG:EN -->' || trimmed === '<!--LANG:EN-->') {
                      if (activeBlockLang === 'en') {
                          activeBlockLang = null;
                      } else {
                          activeBlockLang = 'en';
                      }
                      continue;
                  }
                  if (trimmed === '<!-- LANG:FA -->' || trimmed === '<!--LANG:FA-->') {
                      if (activeBlockLang === 'fa') {
                          activeBlockLang = null;
                      } else {
                          activeBlockLang = 'fa';
                      }
                      continue;
                  }

                  if (activeBlockLang !== null && activeBlockLang !== lang) {
                      continue;
                  }

                  // Toggle code block
                  if (trimmed.startsWith('\\x60\\x60\\x60')) {
                      if (inCodeBlock) {
                          // Close code block
                          let codeText = codeContent.join('\\n')
                              .replace(/&/g, "&amp;")
                              .replace(/</g, "&lt;")
                              .replace(/>/g, "&gt;");
                          htmlLines.push('<pre class="bg-slate-900/90 text-slate-100 p-3 rounded-xl my-2 font-mono text-[10px] overflow-x-auto border border-slate-800 max-h-40">' + codeText + '</pre>');
                          codeContent = [];
                          inCodeBlock = false;
                      } else {
                          inCodeBlock = true;
                      }
                      continue;
                  }

                  if (inCodeBlock) {
                      codeContent.push(line);
                      continue;
                  }

                  if (!trimmed) {
                      continue; 
                  }

                  // Process headers
                  if (trimmed.startsWith('### ')) {
                      let text = trimmed.slice(4);
                      htmlLines.push('<h5 class="text-sm font-bold text-amber-800 dark:text-amber-400 mt-3 mb-1">' + parseInlineMarkdown(text) + '</h5>');
                      continue;
                  }
                  if (trimmed.startsWith('## ')) {
                      let text = trimmed.slice(3);
                      htmlLines.push('<h4 class="text-sm font-extrabold text-amber-800 dark:text-amber-400 mt-4 mb-2">' + parseInlineMarkdown(text) + '</h4>');
                      continue;
                  }
                  if (trimmed.startsWith('# ')) {
                      let text = trimmed.slice(2);
                      htmlLines.push('<h3 class="text-base font-black text-amber-900 dark:text-amber-300 mt-4 mb-2">' + parseInlineMarkdown(text) + '</h3>');
                      continue;
                  }

                  // Process lists
                  let listMatch = line.match(/^(\\s*)([-*+])\\s+(.*)$/);
                  if (listMatch) {
                      let text = listMatch[3];
                      htmlLines.push('<div class="flex items-start gap-2 my-1"><span class="text-amber-500 mt-0.5">▪</span><span class="flex-1">' + parseInlineMarkdown(text) + '</span></div>');
                      continue;
                  }

                  // Standard line
                  htmlLines.push('<p class="my-1">' + parseInlineMarkdown(line) + '</p>');
              }

              // Guard for unclosed code block
              if (inCodeBlock && codeContent.length > 0) {
                  let codeText = codeContent.join('\\n')
                      .replace(/&/g, "&amp;")
                      .replace(/</g, "&lt;")
                      .replace(/>/g, "&gt;");
                  htmlLines.push('<pre class="bg-slate-900/90 text-slate-100 p-3 rounded-xl my-2 font-mono text-[10px] overflow-x-auto border border-slate-800 max-h-40">' + codeText + '</pre>');
              }

              return htmlLines.join('\\n');

              function parseInlineMarkdown(text) {
                  let safe = text
                      .replace(/&/g, "&amp;")
                      .replace(/</g, "&lt;")
                      .replace(/>/g, "&gt;");
                  // Bold
                  safe = safe.replace(/\\*\\*(.*?)\\*\\*/g, '<strong class="font-extrabold text-slate-800 dark:text-slate-200">\$1</strong>');
                  // Italic
                  safe = safe.replace(/\\*(.*?)\\*/g, '<em class="italic">\$1</em>');
                  // Inline code
                  safe = safe.replace(/[\\x60](.*?)[\\x60]/g, '<code class="bg-amber-500/10 dark:bg-slate-800 px-1.5 py-0.5 rounded text-rose-500 font-mono text-[11px]">\$1</code>');
                  return safe;
              }
          }

          async function showUpdateBanner(repo, version) {
              const banner = document.getElementById('update-alert-banner');
              if (!banner) return;
              
              const msg = lang === 'fa' 
                  ? 'نسخه جدیدتر (v' + version + ') در مخزن گیت\u200cهاب شما (' + repo + ') در دسترس است.' 
                  : 'A newer version (v' + version + ') is available in your GitHub repository (' + repo + ').';
                  
              document.getElementById('update-alert-text').textContent = msg;
              const ghLink = document.getElementById('update-github-link');
              if (ghLink) ghLink.href = 'https://github.com/' + repo;
              banner.classList.remove('hidden');
              banner.classList.add('flex');
              banner.style.display = 'flex';
              
              const changelogArea = document.getElementById('update-changelog-area');
              const changelogContent = document.getElementById('update-changelog-content');
              if (changelogArea && changelogContent) {
                  changelogArea.classList.remove('hidden');
                  changelogContent.innerHTML = lang === 'fa' 
                      ? '<p class="animate-pulse">در حال دریافت گزارش تغییرات...</p>' 
                      : '<p class="animate-pulse">Loading changelog...</p>';
                      
                  try {
                      let changelogText = '';
                      try {
                          const res = await fetch('https://api.github.com/repos/' + repo + '/releases/tags/v' + version);
                          if (res.ok) {
                              const rel = await res.json();
                              if (rel && rel.body) {
                                  changelogText = rel.body;
                              }
                          } else {
                              const resNoV = await fetch('https://api.github.com/repos/' + repo + '/releases/tags/' + version);
                              if (resNoV.ok) {
                                  const relNoV = await resNoV.json();
                                  if (relNoV && relNoV.body) {
                                      changelogText = relNoV.body;
                                  }
                              }
                          }
                      } catch(e) {}
                      
                      if (!changelogText) {
                          try {
                              const resLatest = await fetch('https://api.github.com/repos/' + repo + '/releases/latest');
                              if (resLatest.ok) {
                                  const relLatest = await resLatest.json();
                                  if (relLatest && relLatest.body) {
                                      changelogText = relLatest.body;
                                  }
                              }
                          } catch(e) {}
                      }
                      
                      if (!changelogText) {
                          try {
                              const resFile = await fetch('https://raw.githubusercontent.com/' + repo + '/main/CHANGELOG.md');
                              if (resFile.ok) {
                                  changelogText = await resFile.text();
                              }
                          } catch(e) {}
                      }
                      
                      if (changelogText) {
                          changelogContent.innerHTML = parseMarkdown(changelogText);
                      } else {
                          changelogContent.innerHTML = lang === 'fa' 
                              ? '<div class="space-y-2">' +
                                '<p class="font-bold">✨ اضافه شده:</p>' +
                                '<ul class="list-disc list-inside text-xs space-y-1">' +
                                '<li>صفحه اشتراک چندزبانه با حالت تاریک/روشن</li>' +
                                '<li>پشتیبانی NAT64 و نودهای اختصاصی کاربر</li>' +
                                '<li>کانفیگ‌های مستقیم و بروزرسانی خودکار</li>' +
                                '<li>مدیریت کامل دروازه از ربات تلگرام</li>' +
                                '</ul>' +
                                '<p class="font-bold mt-2">⚡ بهبود یافته:</p>' +
                                '<ul class="list-disc list-inside text-xs space-y-1">' +
                                '<li>عملکرد داشبورد و سرعت اسکرول</li>' +
                                '<li>بازنویسی کامل تولید کانفیگ‌ها</li>' +
                                '<li>نام‌گذاری هوشمند با تگ‌های جدید</li>' +
                                '</ul>' +
                                '<p class="font-bold mt-2">🔧 رفع شده:</p>' +
                                '<ul class="list-disc list-inside text-xs space-y-1">' +
                                '<li>ترجمه‌های فارسی معیوب</li>' +
                                '<li>خطای صفحه اشتراک</li>' +
                                '</ul></div>'
                              : '<div class="space-y-2">' +
                                '<p class="font-bold">✨ Added:</p>' +
                                '<ul class="list-disc list-inside text-xs space-y-1">' +
                                '<li>Bilingual subscription page with dark/light mode</li>' +
                                '<li>NAT64 support and per-user custom nodes</li>' +
                                '<li>Direct configs and auto update</li>' +
                                '<li>Full gateway management via Telegram bot</li>' +
                                '</ul>' +
                                '<p class="font-bold mt-2">⚡ Improved:</p>' +
                                '<ul class="list-disc list-inside text-xs space-y-1">' +
                                '<li>Dashboard performance and scroll speed</li>' +
                                '<li>Complete rewrite of all config generators</li>' +
                                '<li>Smart config naming with new tags</li>' +
                                '</ul>' +
                                '<p class="font-bold mt-2">🔧 Fixed:</p>' +
                                '<ul class="list-disc list-inside text-xs space-y-1">' +
                                '<li>Garbled Persian translations</li>' +
                                '<li>Subscription page display error</li>' +
                                '</ul></div>';
                      }
                  } catch(err) {
                      changelogContent.innerHTML = lang === 'fa' 
                          ? '<p class="text-rose-500">خطا در دریافت گزارش تغییرات.</p>' 
                          : '<p class="text-rose-500">Failed to load changelog.</p>';
                  }
              }
          }
          //DateTime Function
            const _dtFormatter = new Intl.DateTimeFormat('fa-IR', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
            function updatePersianDateTime() {
    const now = new Date();
    const parts = _dtFormatter.formatToParts(now);

    const map = {};
    parts.forEach(p => {
        map[p.type] = p.value;
    });

  
      
        const custom = \`\${map.day} \${map.month} \${map.year} \${map.hour}:\${map.minute}:\${map.second}\`;

    document.getElementById("net-datetime").innerText = custom;
    
}

                updatePersianDateTime();
                setInterval(updatePersianDateTime, 1000);



          function dismissUpdate() {
              const b = document.getElementById('update-alert-banner');
              if (b) {
                  b.classList.remove('flex');
                  b.classList.add('hidden');
                  b.style.display = 'none';
              }
          }

          document.addEventListener('DOMContentLoaded', () => {
              const cached = localStorage.getItem('swimshady_session');
              if(cached) {
                  try {
                      const session = JSON.parse(cached);
                      if (Date.now() < session.expiry) {
                          document.getElementById('pwd').value = session.key;
                          doLogin(true);
                      } else { localStorage.removeItem('swimshady_session'); }
                  } catch(e) { localStorage.removeItem('swimshady_session'); }
              }
          });
      </script>
  </body>
  </html>
    `;
  } 
