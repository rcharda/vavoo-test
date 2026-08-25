// health-monitor.js — Évite que le serveur s'acharne sur un miroir
// (vavoo.to / kool.to) ou une route de sortie (proxy/relay/direct) devenue
// lente ou en panne. Sans ça, chaque requête re-tente la route morte et
// paie le timeout à chaque fois → micro-coupures répétées côté client.
//
// Principe : après plusieurs échecs consécutifs sur une route, on la met en
// "quarantaine" (skip) pendant COOLDOWN_MS, et on réessaie une seule requête
// "sonde" à l'expiration pour voir si elle est revenue.

const FAILURE_THRESHOLD = 3;      // échecs consécutifs avant quarantaine
const COOLDOWN_MS = 30000;        // durée de la quarantaine
const SUCCESS_RESET_AFTER = 1;    // 1 succès suffit à réhabiliter une route

const state = new Map(); // routeKey -> { consecutiveFailures, quarantinedUntil }

function getState(routeKey) {
    let entry = state.get(routeKey);
    if (!entry) {
        entry = { consecutiveFailures: 0, quarantinedUntil: 0 };
        state.set(routeKey, entry);
    }
    return entry;
}

/** true si la route doit être évitée pour l'instant. */
function isQuarantined(routeKey) {
    const entry = getState(routeKey);
    if (entry.quarantinedUntil === 0) return false;
    if (Date.now() >= entry.quarantinedUntil) return false; // cooldown écoulé → on retente
    return true;
}

function reportSuccess(routeKey) {
    const entry = getState(routeKey);
    entry.consecutiveFailures = 0;
    entry.quarantinedUntil = 0;
}

function reportFailure(routeKey) {
    const entry = getState(routeKey);
    entry.consecutiveFailures += 1;
    if (entry.consecutiveFailures >= FAILURE_THRESHOLD) {
        entry.quarantinedUntil = Date.now() + COOLDOWN_MS;
        console.log(`[health] route "${routeKey}" mise en quarantaine ${COOLDOWN_MS / 1000}s après ${entry.consecutiveFailures} échecs`);
    }
}

/** Trie une liste de clés de route pour tenter les saines avant les suspectes. */
function orderByHealth(routeKeys) {
    return [...routeKeys].sort((a, b) => {
        const qa = isQuarantined(a) ? 1 : 0;
        const qb = isQuarantined(b) ? 1 : 0;
        return qa - qb;
    });
}

function getDebugSnapshot() {
    const snapshot = {};
    for (const [key, entry] of state.entries()) {
        snapshot[key] = {
            consecutiveFailures: entry.consecutiveFailures,
            quarantined: isQuarantined(key),
            quarantinedUntil: entry.quarantinedUntil ? new Date(entry.quarantinedUntil).toISOString() : null,
        };
    }
    return snapshot;
}

module.exports = { isQuarantined, reportSuccess, reportFailure, orderByHealth, getDebugSnapshot };
