const { Command } = require('commander');
const crypto = require('node:crypto');
const express = require('express');
const { Readable, PassThrough } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { Agent, ProxyAgent, setGlobalDispatcher } = require('undici');
const health = require('./health-monitor');

const NodeCache = require('node-cache');

setGlobalDispatcher(new Agent({
    connect: { timeout: 30000 },
    headersTimeout: 30000,
    bodyTimeout: 0,
    // Un lecteur HLS ouvre plusieurs connexions en parallèle (playlist +
    // plusieurs segments à la fois, surtout avec le préchargement en arrière-
    // plan). La limite par défaut (10/origine) peut mettre des requêtes en
    // file d'attente et créer des micro-pauses. On l'augmente.
    connections: 64,
    pipelining: 1,
}));

const RETRYABLE_STATUS_CODES = new Set([403, 408, 429, 500, 502, 503, 504]);

// --- Cache serveur des segments HLS (.ts/.aac/...) -------------------------
// Objectif : le serveur (connexion stable, ex. US) récupère les segments
// AVANT que le client ne les demande, et les garde en mémoire quelques
// secondes. Ainsi, si la connexion du client est faible/instable, la requête
// du lecteur est servie instantanément depuis la RAM du serveur au lieu
// d'attendre un aller-retour vers Vavoo à chaque segment.
const SEGMENT_CACHE_TTL_SECONDS = Number(process.env.SEGMENT_CACHE_TTL_SECONDS || 30);
const SEGMENT_CACHE_MAX_BYTES = Number(process.env.SEGMENT_CACHE_MAX_MB || 200) * 1024 * 1024;
const SEGMENT_PREFETCH_COUNT = Number(process.env.SEGMENT_PREFETCH_COUNT || 3);

const segmentCache = new Map(); // url -> { buffer, contentType, storedAt, size }
const inFlightFetches = new Map(); // url -> Promise<{buffer, contentType}>
let segmentCacheBytes = 0;

function isSegmentUrl(upstreamUrl) {
    const pathname = new URL(upstreamUrl).pathname.toLowerCase();
    return /\.(ts|aac|mp4|m4s|key)$/.test(pathname);
}

function evictSegmentCacheIfNeeded() {
    if (segmentCacheBytes <= SEGMENT_CACHE_MAX_BYTES) return;
    const entries = [...segmentCache.entries()].sort((a, b) => a[1].storedAt - b[1].storedAt);
    while (segmentCacheBytes > SEGMENT_CACHE_MAX_BYTES && entries.length) {
        const [url, entry] = entries.shift();
        segmentCache.delete(url);
        segmentCacheBytes -= entry.size;
    }
}

function pruneExpiredSegments() {
    const cutoff = Date.now() - SEGMENT_CACHE_TTL_SECONDS * 1000;
    for (const [url, entry] of segmentCache.entries()) {
        if (entry.storedAt < cutoff) {
            segmentCache.delete(url);
            segmentCacheBytes -= entry.size;
        }
    }
}

setInterval(pruneExpiredSegments, 10000).unref();

async function fetchSegmentBuffer(upstreamUrl, streamHeaders) {
    const cached = segmentCache.get(upstreamUrl);
    if (cached) return cached;

    const inFlight = inFlightFetches.get(upstreamUrl);
    if (inFlight) return inFlight;

    const fetchPromise = (async function () {
        const upstream = await fetchWithRetry(upstreamUrl, { headers: streamHeaders });
        if (!upstream.ok || !upstream.body) {
            throw new Error(`upstream returned HTTP ${upstream.status}`);
        }
        const arrayBuffer = await upstream.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        const entry = {
            buffer,
            contentType: upstream.headers.get('content-type') || 'video/mp2t',
            storedAt: Date.now(),
            size: buffer.length,
        };
        segmentCache.set(upstreamUrl, entry);
        segmentCacheBytes += entry.size;
        evictSegmentCacheIfNeeded();
        return entry;
    })();

    inFlightFetches.set(upstreamUrl, fetchPromise);
    try {
        return await fetchPromise;
    } finally {
        inFlightFetches.delete(upstreamUrl);
    }
}

const PREFETCH_STAGGER_MS = Number(process.env.PREFETCH_STAGGER_MS || 150);
const PREFETCH_FAILURE_COOLDOWN_MS = Number(process.env.PREFETCH_FAILURE_COOLDOWN_MS || 20000);
const recentPrefetchFailures = new Map(); // url -> timestamp du dernier échec

function isInFailureCooldown(url) {
    const failedAt = recentPrefetchFailures.get(url);
    if (!failedAt) return false;
    if (Date.now() - failedAt > PREFETCH_FAILURE_COOLDOWN_MS) {
        recentPrefetchFailures.delete(url);
        return false;
    }
    return true;
}

async function prefetchSegmentsInBackground(segmentUrls, streamHeaders) {
    const toPrefetch = segmentUrls
        .slice(0, SEGMENT_PREFETCH_COUNT)
        .filter((url) => !segmentCache.has(url) && !isInFailureCooldown(url));

    // Requêtes espacées dans le temps (pas toutes en parallèle) pour éviter
    // de "marteler" un flux source instable/partagé entre plusieurs chaînes,
    // ce qui peut déclencher un throttling (403/429) côté Vavoo.
    for (const url of toPrefetch) {
        fetchSegmentBuffer(url, streamHeaders).catch((error) => {
            recentPrefetchFailures.set(url, Date.now());
            console.log(`[prefetch] failed for ${describeUpstreamUrl(url)}: ${error.message}`);
        });
        if (toPrefetch.length > 1) {
            await new Promise((resolve) => setTimeout(resolve, PREFETCH_STAGGER_MS));
        }
    }
}

// Cache-miss "à la volée" : les octets sont envoyés au client dès qu'ils
// arrivent (aucun délai de premier octet ajouté), pendant qu'une copie est
// accumulée en parallèle pour remplir le cache et accélérer les prochaines
// requêtes sur ce même segment (autre lecteur, retry, etc.).
async function streamSegmentAndFillCache(res, upstream, upstreamUrl, controllerSignal) {
    res.setHeader('Content-Type', upstream.headers.get('content-type') || 'video/mp2t');
    const contentLength = upstream.headers.get('content-length');
    if (contentLength) res.setHeader('Content-Length', contentLength);
    res.setHeader('Accept-Ranges', 'bytes');
    res.status(upstream.status);

    const nodeStream = Readable.fromWeb(upstream.body);
    const cacheSink = new PassThrough();
    const chunks = [];
    let cacheBytes = 0;

    cacheSink.on('data', function (chunk) {
        chunks.push(chunk);
        cacheBytes += chunk.length;
    });
    cacheSink.on('end', function () {
        if (controllerSignal.aborted) return; // ne pas mettre en cache un flux interrompu
        const buffer = Buffer.concat(chunks, cacheBytes);
        const entry = {
            buffer,
            contentType: upstream.headers.get('content-type') || 'video/mp2t',
            storedAt: Date.now(),
            size: buffer.length,
        };
        segmentCache.set(upstreamUrl, entry);
        segmentCacheBytes += entry.size;
        evictSegmentCacheIfNeeded();
    });
    cacheSink.on('error', function () { /* échec de mise en cache : sans impact pour le client */ });

    nodeStream.pipe(cacheSink);
    await pipeline(nodeStream, res);
}

function parseUrlList(raw) {
    return String(raw || '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);
}

const classicProxyUrls = parseUrlList(process.env.OUTBOUND_PROXY_URLS || process.env.OUTBOUND_PROXY_URL);
const relayProxyUrls = parseUrlList(process.env.RELAY_PROXY_URLS);
const relaySecret = process.env.RELAY_SECRET || '';
const disableDirectFallback = /^(1|true|yes)$/i.test(process.env.DISABLE_DIRECT_FALLBACK || '');

const outboundRoutes = [];

classicProxyUrls.forEach(function (proxyUrl, index) {
    const dispatcher = new ProxyAgent(proxyUrl);
    outboundRoutes.push({
        name: `proxy#${index + 1}`,
        buildFetch(targetUrl, baseOptions) {
            return [targetUrl, { ...baseOptions, dispatcher }];
        }
    });
});

relayProxyUrls.forEach(function (relayUrl, index) {
    outboundRoutes.push({
        name: `relay#${index + 1}`,
        buildFetch(targetUrl, baseOptions) {
            const separator = relayUrl.includes('?') ? '&' : '?';
            const relayTarget = `${relayUrl}${separator}url=${encodeURIComponent(targetUrl)}`;
            const headers = { ...(baseOptions.headers || {}) };
            if (relaySecret) {
                headers['x-relay-secret'] = relaySecret;
            }
            return [relayTarget, { ...baseOptions, headers }];
        }
    });
});

if (outboundRoutes.length === 0 || !disableDirectFallback) {
    outboundRoutes.push({
        name: 'direct',
        buildFetch(targetUrl, baseOptions) {
            return [targetUrl, baseOptions];
        }
    });
}

const program = new Command();

program
    .name('vavoo-iptv-stream-proxy')
    .description('Local proxy for Vavoo IPTV streams')
    .option('--http-host <host>', 'Local HTTP host for displayed URLs', '127.0.0.1')
    .option('--http-port <port>', 'Local HTTP port', '8888')
    .option('--vavoo-language <language>', 'Language sent to Vavoo APIs, e.g. de or optional en', 'de')
    .option('--vavoo-region <region>', 'Region sent to Vavoo APIs, default US for a broad catalog, optional DE which tends to prefilter strongly toward Germany', 'US')
    .option('--vavoo-url-list <selection>', 'URL list to use: primary, fallback, both', 'both')
    .option('--redirect', 'Redirect VAVOO user agents directly to resolved upstream URLs instead of proxying them', false)
    .parse(process.argv);

const options = program.opts();

function getBaseSites(selection) {
    const normalized = String(selection || 'both').trim().toLowerCase();
    if (normalized === 'primary') return ['https://vavoo.to'];
    if (normalized === 'fallback') return ['https://kool.to'];
    return ['https://vavoo.to', 'https://kool.to'];
}

const app = express();

app.use(function (req, res, next) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
    res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
    if (req.method === 'OPTIONS') {
        res.sendStatus(204);
        return;
    }
    next();
});

const FAMILY_USERS = {
    'papa': 'change-moi-1',
    'famille': 'change-moi-2',
};

app.use(function (req, res, next) {
    const header = req.headers.authorization || '';
    const [scheme, encoded] = header.split(' ');
    if (scheme === 'Basic' && encoded) {
        const [user, pass] = Buffer.from(encoded, 'base64').toString().split(':');
        if (FAMILY_USERS[user] && FAMILY_USERS[user] === pass) {
            return next();
        }
    }
    res.setHeader('WWW-Authenticate', 'Basic realm="Vavoo Proxy"');
    res.status(401).send('Authentification requise');
});

const httpHost = options.httpHost;
const port = Number(process.env.PORT || options.httpPort);
const currentLanguage = options.vavooLanguage;
const currentRegion = options.vavooRegion;
const vavooUrlList = options.vavooUrlList;
const redirect = Boolean(options.redirect);
const baseSites = getBaseSites(vavooUrlList);

const cache = new NodeCache();
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const CHANNELS_CACHE_KEY = 'vavoo_channels';
const SIGNATURE_CACHE_KEY = 'vavoo_addon_sig';
const COUNTRY_SEPARATORS = ['➾', '⟾', '->', '→', '»', '›'];
const PING_URLS = ['https://www.vavoo.tv/api/app/ping'];

function getLocalBaseUrl() {
    return `http://${httpHost}:${port}`;
}

// --- Identifiants intégrés dans les URLs publiques générées ---
// Puisque le lecteur vidéo (hls.js) n'envoie pas d'en-tête Authorization,
// on encode "user:pass@" directement dans l'origine publique utilisée pour
// construire /stream/:id et /hls-proxy?url=... . Ainsi CHAQUE requête faite
// par le lecteur (playlist ET segments) porte déjà les identifiants.
const PLAYER_AUTH_USER = process.env.PLAYER_AUTH_USER || 'papa';
const PLAYER_AUTH_PASS = process.env.PLAYER_AUTH_PASS || FAMILY_USERS[PLAYER_AUTH_USER] || 'change-moi-1';

function getPublicOrigin(req) {
    const forwardedHost = req.headers['x-forwarded-host'];
    const forwardedProto = req.headers['x-forwarded-proto'];
    const host = forwardedHost ? forwardedHost.split(',')[0].trim() : req.headers.host;
    const isLocal = /^(127\.0\.0\.1|localhost)(:\d+)?$/.test(host || '');
    let proto = forwardedProto ? forwardedProto.split(',')[0].trim() : req.protocol;
    if (!isLocal) {
        proto = 'https';
    }
    const userInfo = `${encodeURIComponent(PLAYER_AUTH_USER)}:${encodeURIComponent(PLAYER_AUTH_PASS)}@`;
    return `${proto}://${userInfo}${host}`;
}

function buildHomePage() {
    const baseUrl = getLocalBaseUrl();
    const allM3u = `${baseUrl}/channels.m3u8`;
    const germanyM3u = `${baseUrl}/channels.m3u8?country=Germany`;
    const italyM3u = `${baseUrl}/channels.m3u8?country=Italy`;
    const franceM3u = `${baseUrl}/channels.m3u8?country=France`;
    const spainM3u = `${baseUrl}/channels.m3u8?country=Spain`;
    const ukM3u = `${baseUrl}/channels.m3u8?country=${encodeURIComponent('United Kingdom')}`;
    const countriesUrl = `${baseUrl}/countries`;

    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Vavoo Proxy</title>
  <style>
    :root { color-scheme: dark; --bg: #111111; --text: #f3f3f3; --muted: #b8b8b8; --link: #8fd3ff; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: sans-serif; background: var(--bg); color: var(--text); }
    main { max-width: 760px; margin: 0 auto; padding: 24px 18px 40px; }
    h1 { margin: 0 0 10px; font-size: 28px; }
    p { margin: 0 0 18px; color: var(--muted); }
    ul { margin: 0; padding-left: 20px; }
    li { margin: 10px 0; }
    a { color: var(--link); word-break: break-all; }
    code { color: var(--text); }
  </style>
</head>
<body>
  <main>
    <h1>Vavoo Proxy</h1>
    <p>Local entry points for playlists and stream playback.</p>
    <ul>
      <li><a href="${baseUrl}/">${baseUrl}/</a></li>
      <li><a href="${allM3u}">${allM3u}</a></li>
      <li><a href="${germanyM3u}">${germanyM3u}</a></li>
      <li><a href="${italyM3u}">${italyM3u}</a></li>
      <li><a href="${franceM3u}">${franceM3u}</a></li>
      <li><a href="${spainM3u}">${spainM3u}</a></li>
      <li><a href="${ukM3u}">${ukM3u}</a></li>
      <li><a href="${countriesUrl}">${countriesUrl}</a></li>
    </ul>
  </main>
</body>
</html>`;
}

function normalize(value) {
    return String(value || '').trim().toLowerCase();
}

function normalizeChannelIdPart(value) {
    return normalize(value).replace(/\s+/g, ' ');
}

function getStableChannelId(name, country) {
    const seed = [normalizeChannelIdPart(country), normalizeChannelIdPart(name)].join('|');
    return crypto.createHash('sha1').update(seed).digest('hex').slice(0, 22);
}

function extractCountry(group) {
    const rawGroup = String(group || '').trim();
    if (!rawGroup) return 'default';
    for (const separator of COUNTRY_SEPARATORS) {
        if (rawGroup.includes(separator)) {
            return rawGroup.split(separator)[0].trim() || 'default';
        }
    }
    return rawGroup;
}

function getCatalogHeaders(signature) {
    return {
        'content-type': 'application/json; charset=utf-8',
        'mediahubmx-signature': signature,
        'user-agent': 'MediaHubMX/2',
        'accept': '*/*',
        'Accept-Language': currentLanguage,
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'close',
    };
}

function getStreamHeaders(req) {
    const headers = { 'User-Agent': 'VAVOO/2.6', 'Connection': 'close' };
    if (req.headers.range) headers.Range = req.headers.range;
    return headers;
}

function getProxiedUpstreamUrl(req, upstreamUrl) {
    return `${getPublicOrigin(req)}/hls-proxy?url=${encodeURIComponent(upstreamUrl)}`;
}

function setPlaylistHeaders(res) {
    res.type('application/vnd.apple.mpegurl');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
}

async function sendHlsMasterPlaylist(req, res, streamUrl) {
    setPlaylistHeaders(res);
    try {
        const upstream = await fetchWithRetry(streamUrl, { headers: getStreamHeaders(req) });
        if (!upstream.ok) throw new Error(`upstream returned HTTP ${upstream.status}`);
        const playlist = await upstream.text();
        const hasVariants = /#EXT-X-STREAM-INF/i.test(playlist);
        if (hasVariants) {
            const rewritten = rewriteM3u8Playlist(req, streamUrl, playlist);
            res.send(rewritten);
            return;
        }
    } catch (error) {
        console.log(`[master playlist] fallback to single variant: ${error.message}`);
    }
    res.send([
        '#EXTM3U',
        '#EXT-X-VERSION:3',
        '#EXT-X-STREAM-INF:BANDWIDTH=8000000',
        getProxiedUpstreamUrl(req, streamUrl)
    ].join('\n') + '\n');
}

function isM3u8Url(upstreamUrl) {
    return new URL(upstreamUrl).pathname.toLowerCase().endsWith('.m3u8');
}

function isM3u8Response(upstreamUrl, contentType) {
    return String(contentType || '').toLowerCase().includes('mpegurl')
        || String(contentType || '').toLowerCase().includes('application/vnd.apple')
        || isM3u8Url(upstreamUrl);
}

function shouldRewritePlaylistUri(uri) {
    const trimmed = String(uri || '').trim();
    if (!trimmed) return false;
    return !/^(data|urn|skd):/i.test(trimmed);
}

function rewritePlaylistUri(req, baseUrl, uri) {
    if (!shouldRewritePlaylistUri(uri)) return uri;
    return getProxiedUpstreamUrl(req, new URL(uri, baseUrl).toString());
}

// Si la source propose plusieurs débits (playlist maître HLS avec
// #EXT-X-STREAM-INF), on choisit automatiquement la variante la plus légère
// pour les clients en connexion faible — sans transcoder, donc sans coût CPU.
// Activé via /stream/:id?quality=low (le client passe ce paramètre quand il
// détecte une connexion lente, ou par défaut si tu préfères la sécurité).
function isMasterPlaylist(playlist) {
    return /#EXT-X-STREAM-INF/.test(playlist);
}

function pickLowestBandwidthVariant(baseUrl, playlist) {
    const lines = String(playlist).split(/\r?\n/);
    let best = null;
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line.startsWith('#EXT-X-STREAM-INF')) continue;
        const match = line.match(/BANDWIDTH=(\d+)/);
        const bandwidth = match ? Number(match[1]) : Infinity;
        const uriLine = (lines[i + 1] || '').trim();
        if (!uriLine || uriLine.startsWith('#')) continue;
        if (!best || bandwidth < best.bandwidth) {
            try {
                best = { bandwidth, url: new URL(uriLine, baseUrl).toString() };
            } catch (error) { /* URI malformée, on ignore cette variante */ }
        }
    }
    return best ? best.url : null;
}

function rewriteM3u8Playlist(req, upstreamUrl, playlist) {
    return String(playlist)
        .split(/\r?\n/)
        .map(function (line) {
            const trimmed = line.trim();
            if (!trimmed) return line;
            if (trimmed.startsWith('#')) {
                return line.replace(/URI="([^"]+)"/g, function (match, uri) {
                    return `URI="${rewritePlaylistUri(req, upstreamUrl, uri)}"`;
                });
            }
            return rewritePlaylistUri(req, upstreamUrl, trimmed);
        })
        .join('\n');
}

function extractSegmentUrls(baseUrl, playlist) {
    const urls = [];
    for (const rawLine of String(playlist).split(/\r?\n/)) {
        const trimmed = rawLine.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        if (!shouldRewritePlaylistUri(trimmed)) continue;
        try {
            urls.push(new URL(trimmed, baseUrl).toString());
        } catch (error) {
            // ignore malformed lines
        }
    }
    return urls;
}

function getPlaylistDebugInfo(playlist) {
    const lines = String(playlist).split(/\r?\n/);
    const sequenceLine = lines.find((line) => line.startsWith('#EXT-X-MEDIA-SEQUENCE:'));
    const sequence = sequenceLine ? sequenceLine.split(':')[1] : 'n/a';
    const segments = lines.filter((line) => line.trim() && !line.trim().startsWith('#')).length;
    return { sequence, segments };
}

function describeUpstreamUrl(upstreamUrl) {
    const url = new URL(upstreamUrl);
    return `${url.hostname}${url.pathname}`;
}

function setUpstreamHeaders(res, upstream) {
    const contentType = upstream.headers.get('content-type');
    if (contentType) res.setHeader('Content-Type', contentType);
    const contentLength = upstream.headers.get('content-length');
    if (contentLength) res.setHeader('Content-Length', contentLength);
    const acceptRanges = upstream.headers.get('accept-ranges');
    if (acceptRanges) res.setHeader('Accept-Ranges', acceptRanges);
    const contentRange = upstream.headers.get('content-range');
    if (contentRange) res.setHeader('Content-Range', contentRange);
}

function getPingPayload() {
    const currentTimestamp = Date.now();
    return {
        reason: 'app-focus', locale: currentLanguage, theme: 'dark',
        metadata: {
            device: { type: 'desktop', uniqueId: `node-${currentTimestamp}` },
            os: { name: 'linux', version: 'Linux', abis: ['x64'], host: 'node' },
            app: { platform: 'electron' },
            version: { package: 'tv.vavoo.app', binary: '3.1.8', js: '3.1.8' }
        },
        appFocusTime: 0, playerActive: false, playDuration: 0, devMode: false,
        hasAddon: true, castConnected: false, package: 'tv.vavoo.app', version: '3.1.8',
        process: 'app', firstAppStart: currentTimestamp, lastAppStart: currentTimestamp,
        ipLocation: null, adblockEnabled: true,
        proxy: { supported: ['ss'], engine: 'Mu', enabled: false, autoServer: true },
        iap: { supported: false }
    };
}

async function requestJson(options) {
    const response = await fetch(options.url, {
        method: options.method || 'GET',
        headers: options.headers,
        body: options.body ? JSON.stringify(options.body) : undefined,
        // 8s au lieu de 30s : si vavoo.to traîne, on bascule vite sur
        // kool.to plutôt que de bloquer le clic de l'utilisateur ~30s.
        signal: AbortSignal.timeout(options.timeout || 8000),
    });
    const body = await response.json();
    if (!response.ok) {
        const error = new Error(`HTTP ${response.status} for ${options.url}`);
        error.statusCode = response.status;
        error.body = body;
        throw error;
    }
    return body;
}

async function getAddonSignature() {
    const cached = cache.get(SIGNATURE_CACHE_KEY);
    if (cached) return cached;
    const payload = getPingPayload();
    for (const url of PING_URLS) {
        try {
            const body = await requestJson({ method: 'POST', url, body: payload });
            const signature = body?.addonSig;
            if (signature) {
                cache.set(SIGNATURE_CACHE_KEY, signature, 300);
                return signature;
            }
        } catch (error) {
            console.log(`[vavoo] addonSig request failed for ${url}: ${error.message}`);
        }
    }
    throw new Error('Unable to obtain addonSig');
}

function mapCatalogItem(item) {
    const name = item.name || 'Unknown Channel';
    const country = extractCountry(item.group);
    return {
        id: getStableChannelId(name, country),
        url: item.url, name, logo: item.logo || '', group: item.group || '', country
    };
}

async function loadCatalogFromBase(baseUrl, signature) {
    const catalogUrl = `${baseUrl.replace(/\/$/, '')}/mediahubmx-catalog.json`;
    const headers = getCatalogHeaders(signature);
    const channels = [];
    let cursor = null;
    while (true) {
        const body = await requestJson({
            method: 'POST', url: catalogUrl, headers,
            body: {
                language: currentLanguage, region: currentRegion, catalogId: 'iptv', id: 'iptv',
                adult: false, search: '', sort: '', filter: {}, cursor, clientVersion: '3.0.2'
            }
        });
        const items = Array.isArray(body?.items) ? body.items : [];
        for (const item of items) {
            if (item?.type === 'iptv' && item?.url) channels.push(mapCatalogItem(item));
        }
        if (!body?.nextCursor) break;
        cursor = body.nextCursor;
    }
    return channels;
}

async function getChannels(forceRefresh = false) {
    if (forceRefresh) cache.del(CHANNELS_CACHE_KEY);
    const cached = cache.get(CHANNELS_CACHE_KEY);
    if (cached) return cached;
    const signature = await getAddonSignature();
    for (const baseUrl of baseSites) {
        try {
            const channels = await loadCatalogFromBase(baseUrl, signature);
            cache.set(CHANNELS_CACHE_KEY, channels, 300);
            console.log(`[vavoo] channels loaded from ${baseUrl}: ${channels.length}`);
            return channels;
        } catch (error) {
            console.log(`[vavoo] catalog load failed for ${baseUrl}: ${error.message}`);
        }
    }
    throw new Error('Unable to load channel catalog');
}

async function getChannelsByCountry(country) {
    const channels = await getChannels();
    return channels.filter((channel) => normalize(channel.country) === normalize(country));
}

async function getCountries() {
    const channels = await getChannels();
    return [...new Set(
        channels.map((channel) => channel.country).filter((country) => country && normalize(country) !== 'default')
    )].sort((left, right) => left.localeCompare(right));
}

async function findChannelById(id) {
    const channels = await getChannels();
    return channels.find((channel) => String(channel.id) === String(id));
}

function normalizeStreamId(id) {
    return String(id || '').split('|')[0];
}

// Petit cache court : si l'utilisateur re-clique vite sur la même chaîne
// (ou si le player recharge la playlist), on évite de re-résoudre à chaque
// fois — gain de temps direct au clic.
const RESOLVE_CACHE_TTL_SECONDS = 20;

async function resolveStreamUrl(channel) {
    const cacheKey = `resolve_${channel.id}`;
    const cached = cache.get(cacheKey);
    if (cached) return cached;

    const signature = await getAddonSignature();

    // On interroge vavoo.to ET kool.to EN PARALLÈLE : le premier des deux
    // qui répond gagne. Si l'un des deux miroirs est lent/instable ce
    // jour-là, on n'attend plus après lui — fini le blocage ~30s côté
    // client sur certaines chaînes.
    const orderedSites = health.orderByHealth(baseSites);
    const allQuarantined = orderedSites.every((site) => health.isQuarantined(site));
    const attempts = orderedSites.map(async (baseUrl) => {
        if (!allQuarantined && health.isQuarantined(baseUrl)) {
            throw new Error(`${baseUrl} en quarantaine (échecs récents)`);
        }
        const resolveUrl = `${baseUrl.replace(/\/$/, '')}/mediahubmx-resolve.json`;
        try {
            const body = await requestJson({
                method: 'POST', url: resolveUrl, headers: getCatalogHeaders(signature),
                body: { language: currentLanguage, region: currentRegion, url: channel.url, clientVersion: '3.0.2' }
            });
            const resolvedUrl = (Array.isArray(body) && body[0]?.url) || body?.url || body?.streamUrl;
            if (!resolvedUrl) throw new Error('réponse sans url');
            health.reportSuccess(baseUrl);
            return resolvedUrl;
        } catch (error) {
            health.reportFailure(baseUrl);
            const causeInfo = error.cause ? ` | cause: ${error.cause.code || error.cause.message || error.cause}` : '';
            console.log(`[vavoo] resolve failed for ${baseUrl}: ${error.message}${causeInfo}`);
            throw error;
        }
    });

    try {
        const resolvedUrl = await Promise.any(attempts);
        cache.set(cacheKey, resolvedUrl, RESOLVE_CACHE_TTL_SECONDS);
        return resolvedUrl;
    } catch (error) {
        throw new Error(`Unable to resolve stream for channel ${channel.name}`);
    }
}

async function proxyStream(req, res, streamUrl, channelName) {
    const connId = `${req.socket.remoteAddress}`;
    const controller = new AbortController();
    // req.on (pas req.socket.on) : listener propre à CETTE requête, retiré
    // automatiquement à la fin — évite l'accumulation de listeners quand le
    // socket TCP est réutilisé (keep-alive) pour plusieurs requêtes.
    req.on('close', function () {
        console.log(`[${connId}] connection closed`);
        controller.abort();
    });
    try {
        const upstream = await fetchWithRetry(streamUrl, { signal: controller.signal, headers: getStreamHeaders(req) });
        if (!upstream.ok || !upstream.body) throw new Error(`upstream returned HTTP ${upstream.status}`);
        const contentType = upstream.headers.get('content-type');
        if (isM3u8Response(streamUrl, contentType)) {
            const playlist = await upstream.text();
            // Connexion faible : si la source propose plusieurs débits, on
            // suit automatiquement la variante la plus légère (zéro coût CPU,
            // juste du choix d'URL) au lieu du flux "auto" qui démarre
            // souvent en haute qualité.
            if (req.query.quality === 'low' && isMasterPlaylist(playlist)) {
                const lightUrl = pickLowestBandwidthVariant(streamUrl, playlist);
                if (lightUrl) {
                    console.log(`[${connId}] variante basse qualité choisie pour "${channelName}"`);
                    return proxyStream(req, res, lightUrl, channelName);
                }
            }
            const rewrittenPlaylist = rewriteM3u8Playlist(req, streamUrl, playlist);
            setPlaylistHeaders(res);
            res.send(rewrittenPlaylist);
            return;
        }
        setUpstreamHeaders(res, upstream);
        console.log(`[${connId}] starting stream proxy "${channelName}"`);
        await pipeline(Readable.fromWeb(upstream.body), res);
    } catch (error) {
        if (controller.signal.aborted) {
            console.log(`[${connId}] stream ended "${channelName}"`);
            return;
        }
        console.log(`[${connId}] stream error "${channelName}": ${error.message}`);
        if (!res.headersSent) res.status(400).send(`stream error: ${error.message}`);
    }
}

async function fetchWithRetry(url, baseOptions, retriesPerRoute = 2) {
    let lastError;
    const routeNames = outboundRoutes.map((r) => r.name);
    const allRoutesQuarantined = routeNames.every((name) => health.isQuarantined(name));
    const orderedRoutes = outboundRoutes.slice().sort((a, b) => {
        const qa = (!allRoutesQuarantined && health.isQuarantined(a.name)) ? 1 : 0;
        const qb = (!allRoutesQuarantined && health.isQuarantined(b.name)) ? 1 : 0;
        return qa - qb;
    });
    for (const route of orderedRoutes) {
        if (!allRoutesQuarantined && health.isQuarantined(route.name) && orderedRoutes.length > 1) {
            console.log(`[hls-proxy] route "${route.name}" ignorée (quarantaine)`);
            continue;
        }
        for (let attempt = 1; attempt <= retriesPerRoute + 1; attempt++) {
            if (baseOptions.signal?.aborted) throw lastError || new Error('aborted');
            try {
                const [fetchUrl, fetchOptions] = route.buildFetch(url, baseOptions);
                const response = await fetch(fetchUrl, fetchOptions);
                if (!RETRYABLE_STATUS_CODES.has(response.status)) {
                    health.reportSuccess(route.name);
                    return response;
                }
                console.log(`[hls-proxy] ${route.name} attempt ${attempt}/${retriesPerRoute + 1} got HTTP ${response.status} for ${describeUpstreamUrl(url)}`);
                lastError = new Error(`upstream returned HTTP ${response.status}`);
                if (attempt <= retriesPerRoute) await new Promise((resolve) => setTimeout(resolve, 400));
            } catch (error) {
                lastError = error;
                if (baseOptions.signal?.aborted) throw error;
                const causeInfo = error.cause ? ` | cause: ${error.cause.code || error.cause.message || error.cause}` : '';
                console.log(`[hls-proxy] ${route.name} attempt ${attempt}/${retriesPerRoute + 1} network error for ${describeUpstreamUrl(url)}: ${error.message}${causeInfo}`);
                if (attempt <= retriesPerRoute) await new Promise((resolve) => setTimeout(resolve, 400));
            }
        }
        health.reportFailure(route.name);
        console.log(`[hls-proxy] route "${route.name}" exhausted, trying next route`);
    }
    throw lastError || new Error('all routes exhausted');
}

async function proxyUpstreamUrl(req, res, upstreamUrl) {
    const connId = `${req.socket.remoteAddress}`;
    const controller = new AbortController();
    const upstreamLabel = describeUpstreamUrl(upstreamUrl);
    const streamHeaders = getStreamHeaders(req);
    req.on('close', function () { controller.abort(); }); // req, pas req.socket : évite le leak de listeners sur socket keep-alive

    // --- Segment déjà en cache serveur : réponse instantanée, sans attendre Vavoo ---
    const hasRangeRequest = Boolean(req.headers.range);
    if (!hasRangeRequest && isSegmentUrl(upstreamUrl)) {
        const cached = segmentCache.get(upstreamUrl);
        if (cached) {
            res.setHeader('Content-Type', cached.contentType);
            res.setHeader('Content-Length', cached.size);
            res.setHeader('Accept-Ranges', 'bytes');
            console.log(`[${connId}] hls asset (cache) "${upstreamLabel}" size=${cached.size}`);
            res.status(200).send(cached.buffer);
            return;
        }
        // Pas en cache : on stream en direct vers le client SANS attendre le
        // téléchargement complet (aucune latence ajoutée), tout en remplissant
        // le cache en parallèle pour les requêtes suivantes.
        try {
            const upstream = await fetchWithRetry(upstreamUrl, { signal: controller.signal, headers: streamHeaders });
            if (!upstream.ok || !upstream.body) throw new Error(`upstream returned HTTP ${upstream.status}`);
            console.log(`[${connId}] hls asset (live+cache) "${upstreamLabel}"`);
            await streamSegmentAndFillCache(res, upstream, upstreamUrl, controller.signal);
            return;
        } catch (error) {
            if (controller.signal.aborted) {
                console.log(`[${connId}] hls proxy ended "${upstreamLabel}"`);
                return;
            }
            console.log(`[${connId}] hls asset live-stream failed "${upstreamLabel}": ${error.message}`);
            if (res.headersSent) return;
            // on continue vers le chemin de repli générique ci-dessous
        }
    }

    try {
        const upstream = await fetchWithRetry(upstreamUrl, { signal: controller.signal, headers: streamHeaders });
        if (!upstream.ok || !upstream.body) throw new Error(`upstream returned HTTP ${upstream.status}`);
        const contentType = upstream.headers.get('content-type');
        if (isM3u8Response(upstreamUrl, contentType)) {
            const playlist = await upstream.text();
            const rewrittenPlaylist = rewriteM3u8Playlist(req, upstreamUrl, playlist);
            const debugInfo = getPlaylistDebugInfo(playlist);
            console.log(`[${connId}] hls playlist "${upstreamLabel}" status=${upstream.status} sequence=${debugInfo.sequence} entries=${debugInfo.segments}`);
            setPlaylistHeaders(res);
            res.send(rewrittenPlaylist);
            // Pré-charge en arrière-plan les prochains segments annoncés par la playlist,
            // pour que les requêtes suivantes du lecteur soient servies depuis le cache.
            const segmentUrls = extractSegmentUrls(upstreamUrl, playlist).filter((url) => isSegmentUrl(url));
            if (segmentUrls.length) prefetchSegmentsInBackground(segmentUrls, streamHeaders);
            return;
        }
        setUpstreamHeaders(res, upstream);
        res.status(upstream.status);
        console.log(`[${connId}] hls asset "${upstreamLabel}" status=${upstream.status} type="${contentType || 'unknown'}"`);
        await pipeline(Readable.fromWeb(upstream.body), res);
    } catch (error) {
        if (controller.signal.aborted) {
            console.log(`[${connId}] hls proxy ended "${upstreamLabel}"`);
            return;
        }
        const causeInfo = error.cause ? ` | cause: ${error.cause.code || error.cause.message || error.cause}` : '';
        console.log(`[${connId}] hls proxy error "${upstreamLabel}": ${error.message}${causeInfo}`);
        if (!res.headersSent) res.status(400).send(`upstream proxy error: ${error.message}`);
    }
}

app.get('/', function (req, res) {
    res.type('html').send(buildHomePage());
});

app.get('/health-debug', function (req, res) {
    res.json(health.getDebugSnapshot());
});

app.get('/countries', async function (req, res) {
    try {
        res.json(await getCountries());
    } catch (error) {
        console.log('[vavoo] countries error', error.message);
        res.status(500).send(error.message);
    }
});

app.get('/channels.m3u8', async function (req, res) {
    try {
        const country = req.query.country;
        const channels = country ? await getChannelsByCountry(country) : await getChannels();
        const output = ['#EXTM3U'];
        for (const channel of channels) {
            output.push(`#EXTINF:-1 tvg-name="${channel.name}" group-title="${channel.country}" tvg-logo="${channel.logo}" tvg-id="${channel.name}",${channel.name}`);
            output.push('#EXTVLCOPT:http-user-agent=VAVOO/2.6');
            output.push('#EXTVLCOPT:no-ssl-verify');
            output.push(`${getPublicOrigin(req)}/stream/${encodeURIComponent(channel.id)}`);
        }
        setPlaylistHeaders(res);
        res.send(output.join('\n'));
    } catch (error) {
        console.log('[vavoo] channels.m3u8 error', error.message);
        res.status(500).send(error.message);
    }
});


app.get('/hls-proxy', async function (req, res) {
    const upstreamUrl = req.query.url;
    const connId = `${req.socket.remoteAddress}`;
    if (!upstreamUrl) {
        console.log(`[${connId}] hls proxy error: missing url`);
        res.status(400).send('missing url');
        return;
    }
    try {
        const parsedUrl = new URL(upstreamUrl);
        if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
            console.log(`[${connId}] hls proxy error "${upstreamUrl}": unsupported protocol`);
            res.status(400).send('unsupported upstream protocol');
            return;
        }
        console.log(`[${connId}] hls proxy opened "${describeUpstreamUrl(parsedUrl.toString())}"`);
        await proxyUpstreamUrl(req, res, parsedUrl.toString());
    } catch (error) {
        console.log(`[${connId}] hls proxy error: invalid upstream url: ${error.message}`);
        res.status(400).send(`invalid upstream url: ${error.message}`);
    }
});

// --- Lecture "instantanée basse qualité" via transcodage serveur ---------
// /live/:id/playlist.m3u8 : démarre (ou rejoint) le transcodeur de la
// chaîne, attend que les premiers segments existent, puis sert la playlist.
// Le player (hls.js côté client) n'a plus qu'à lire des segments minuscules
// déjà prêts → plus d'attente réseau perceptible au clic.

app.get('/stream/:id', async function (req, res) {
    const connId = `${req.socket.remoteAddress}`;
    const userAgent = req.headers['user-agent'] ?? 'unknown';
    try {
        console.log(`[${connId}] connection opened: "${userAgent}"`);
        const channelId = normalizeStreamId(req.params.id);
        const channel = await findChannelById(channelId);
        if (!channel) {
            res.status(404).send(`unknown channel: ${channelId}`);
            return;
        }
        const streamUrl = await resolveStreamUrl(channel);
        console.log(`[${connId}] resolved "${channel.name}": ${streamUrl}`);
        if (redirect && userAgent.toLowerCase().includes('vavoo')) {
            res.redirect(streamUrl);
            return;
        }
        if (isM3u8Url(streamUrl)) {
            console.log(`[${connId}] hls master playlist "${channel.name}"`);
            await sendHlsMasterPlaylist(req, res, streamUrl);
            return;
        }
        await proxyStream(req, res, streamUrl, channel.name);
    } catch (error) {
        console.log(`[${connId}] playback error for channel "${req.params.id}":`, error.message);
        if (error.body) console.log(`[${connId}] upstream error body:`, JSON.stringify(error.body));
        if (error.stack) console.log(error.stack);
        res.status(500).send(error.message);
    }
});


app.listen(port, '0.0.0.0', () => {
    const baseUrl = getLocalBaseUrl();
    console.log(`Listening on ${baseUrl}/`);
    console.log(`M3U: ${baseUrl}/channels.m3u8`);
    console.log(`Example filtered M3U: ${baseUrl}/channels.m3u8?country=Germany`);
    console.log(`Countries: ${baseUrl}/countries`);

    // ── Préchauffage au démarrage ─────────────────────────────────────
    // Sur Render, chaque redémarrage (déploiement, réveil après mise en
    // veille) repart avec un cache vide. Sans ça, le TOUT PREMIER clic
    // client après un redémarrage paie le coût complet de charger le
    // catalogue + la signature Vavoo (plusieurs secondes), en plus de la
    // résolution du flux. On le fait une fois ici, en arrière-plan, pour
    // que ce coût soit payé par le serveur au démarrage et non par le
    // premier utilisateur qui clique.
    getChannels()
        .then(async (channels) => {
            console.log(`[warmup] catalogue préchauffé (${channels.length} chaînes)`);
            // Optionnel : préchauffe aussi la RÉSOLUTION des chaînes les plus
            // regardées, si tu renseignes leurs IDs dans la variable
            // d'environnement WARMUP_CHANNEL_IDS (séparés par des virgules).
            // Sans ça, chaque ID listé ici bénéficie d'un premier clic
            // vraiment instantané même juste après un redémarrage.
            const warmupIds = parseUrlList(process.env.WARMUP_CHANNEL_IDS);
            for (const id of warmupIds) {
                try {
                    const channel = await findChannelById(id);
                    if (!channel) { console.log(`[warmup] chaîne inconnue: ${id}`); continue; }
                    await resolveStreamUrl(channel);
                    console.log(`[warmup] résolution préchauffée: ${channel.name}`);
                } catch (error) {
                    console.log(`[warmup] échec résolution ${id}:`, error.message);
                }
            }
        })
        .catch((error) => console.log('[warmup] échec préchauffage catalogue:', error.message));
});
