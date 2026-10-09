"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DurableEvictionCache = exports.EvictionCause = void 0;
exports.buildEvictionReason = buildEvictionReason;
const duration_1 = require("../../duration");
var EvictionCause;
(function (EvictionCause) {
    EvictionCause["TTL_EXCEEDED"] = "ttl_exceeded";
    EvictionCause["CAPACITY_PRESSURE"] = "capacity_pressure";
    EvictionCause["WORKER_SHUTDOWN"] = "worker_shutdown";
})(EvictionCause || (exports.EvictionCause = EvictionCause = {}));
class DurableEvictionCache {
    constructor() {
        this._runs = new Map();
    }
    registerRun(key, taskRunExternalId, invocationCount, now, evictionPolicy) {
        this._runs.set(key, {
            key,
            taskRunExternalId,
            invocationCount,
            evictionPolicy,
            registeredAt: now,
            waitingSince: undefined,
            waitKind: undefined,
            waitResourceId: undefined,
            _waitCount: 0,
            evictionReason: undefined,
        });
    }
    unregisterRun(key) {
        this._runs.delete(key);
    }
    get(key) {
        return this._runs.get(key);
    }
    getAllWaiting() {
        return [...this._runs.values()].filter((r) => r._waitCount > 0);
    }
    findKeyByTaskRunExternalId(taskRunExternalId) {
        for (const [key, rec] of this._runs) {
            if (rec.taskRunExternalId === taskRunExternalId)
                return key;
        }
        return undefined;
    }
    markWaiting(key, now, waitKind, resourceId) {
        const rec = this._runs.get(key);
        if (!rec)
            return;
        rec._waitCount += 1;
        if (rec._waitCount === 1) {
            rec.waitingSince = now;
        }
        rec.waitKind = waitKind;
        rec.waitResourceId = resourceId;
    }
    markActive(key) {
        const rec = this._runs.get(key);
        if (!rec)
            return;
        rec._waitCount = Math.max(0, rec._waitCount - 1);
        if (rec._waitCount === 0) {
            rec.waitingSince = undefined;
            rec.waitKind = undefined;
            rec.waitResourceId = undefined;
        }
    }
    selectEvictionCandidate(now, durableSlots, reserveSlots, minWaitForCapacityEvictionMs) {
        const waiting = [...this._runs.values()].filter((r) => r._waitCount > 0 && r.evictionPolicy !== undefined);
        if (waiting.length === 0)
            return undefined;
        const ttlEligible = waiting.filter((r) => {
            var _a;
            const ttl = (_a = r.evictionPolicy) === null || _a === void 0 ? void 0 : _a.ttl;
            if (!ttl || !r.waitingSince)
                return false;
            return now - r.waitingSince >= (0, duration_1.durationToMs)(ttl);
        });
        if (ttlEligible.length > 0) {
            ttlEligible.sort((a, b) => {
                var _a, _b, _c, _d, _e, _f;
                return ((_b = (_a = a.evictionPolicy) === null || _a === void 0 ? void 0 : _a.priority) !== null && _b !== void 0 ? _b : 0) - ((_d = (_c = b.evictionPolicy) === null || _c === void 0 ? void 0 : _c.priority) !== null && _d !== void 0 ? _d : 0) ||
                    ((_e = a.waitingSince) !== null && _e !== void 0 ? _e : now) - ((_f = b.waitingSince) !== null && _f !== void 0 ? _f : now);
            });
            const [chosen] = ttlEligible;
            chosen.evictionReason = buildEvictionReason(EvictionCause.TTL_EXCEEDED, chosen);
            return chosen.key;
        }
        if (!this._hasCapacityPressure(durableSlots, reserveSlots, waiting.length)) {
            return undefined;
        }
        const capacityCandidates = waiting.filter((r) => {
            var _a;
            return ((_a = r.evictionPolicy) === null || _a === void 0 ? void 0 : _a.allowCapacityEviction) !== false &&
                r.waitingSince !== undefined &&
                now - r.waitingSince >= minWaitForCapacityEvictionMs;
        });
        if (capacityCandidates.length === 0)
            return undefined;
        capacityCandidates.sort((a, b) => {
            var _a, _b, _c, _d, _e, _f;
            return ((_b = (_a = a.evictionPolicy) === null || _a === void 0 ? void 0 : _a.priority) !== null && _b !== void 0 ? _b : 0) - ((_d = (_c = b.evictionPolicy) === null || _c === void 0 ? void 0 : _c.priority) !== null && _d !== void 0 ? _d : 0) ||
                ((_e = a.waitingSince) !== null && _e !== void 0 ? _e : now) - ((_f = b.waitingSince) !== null && _f !== void 0 ? _f : now);
        });
        const [chosen] = capacityCandidates;
        chosen.evictionReason = buildEvictionReason(EvictionCause.CAPACITY_PRESSURE, chosen);
        return chosen.key;
    }
    _hasCapacityPressure(durableSlots, reserveSlots, waitingCount) {
        if (durableSlots <= 0)
            return false;
        const maxWaiting = durableSlots - reserveSlots;
        if (maxWaiting <= 0)
            return false;
        return waitingCount >= maxWaiting;
    }
}
exports.DurableEvictionCache = DurableEvictionCache;
function buildEvictionReason(cause, rec) {
    var _a;
    let waitDesc = rec.waitKind || 'unknown';
    if (rec.waitResourceId) {
        waitDesc = `${waitDesc}(${rec.waitResourceId})`;
    }
    switch (cause) {
        case EvictionCause.TTL_EXCEEDED: {
            const ttlStr = ((_a = rec.evictionPolicy) === null || _a === void 0 ? void 0 : _a.ttl) ? ` (${rec.evictionPolicy.ttl})` : '';
            return `Wait TTL${ttlStr} exceeded while waiting on ${waitDesc}`;
        }
        case EvictionCause.CAPACITY_PRESSURE:
            return `Worker at capacity while waiting on ${waitDesc}`;
        case EvictionCause.WORKER_SHUTDOWN:
            return `Worker shutdown while waiting on ${waitDesc}`;
        default: {
            const _exhaustive = cause;
            throw new Error(`Unknown eviction cause: ${_exhaustive}`);
        }
    }
}
