"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DurableEvictionManager = exports.DEFAULT_DURABLE_EVICTION_CONFIG = void 0;
const eviction_cache_1 = require("./eviction-cache");
const hatchet_error_1 = require("../../../../util/errors/hatchet-error");
exports.DEFAULT_DURABLE_EVICTION_CONFIG = {
    checkIntervalMs: 1000,
    reserveSlots: 0,
    minWaitForCapacityEvictionMs: 10000,
};
class DurableEvictionManager {
    constructor(opts) {
        this._ticking = false;
        this._durableSlots = opts.durableSlots;
        this._cancelLocal = opts.cancelLocal;
        this._requestEvictionWithAck = opts.requestEvictionWithAck;
        this._config = Object.assign(Object.assign({}, exports.DEFAULT_DURABLE_EVICTION_CONFIG), opts.config);
        this._cache = opts.cache || new eviction_cache_1.DurableEvictionCache();
        this._logger = opts.logger;
    }
    get cache() {
        return this._cache;
    }
    start() {
        if (this._timer)
            return;
        this._timer = setInterval(() => this._tickSafe(), this._config.checkIntervalMs);
    }
    stop() {
        if (this._timer) {
            clearInterval(this._timer);
            this._timer = undefined;
        }
    }
    registerRun(key, taskRunExternalId, invocationCount, evictionPolicy) {
        this._cache.registerRun(key, taskRunExternalId, invocationCount, Date.now(), evictionPolicy);
    }
    unregisterRun(key) {
        this._cache.unregisterRun(key);
    }
    markWaiting(key, waitKind, resourceId) {
        this._cache.markWaiting(key, Date.now(), waitKind, resourceId);
    }
    markActive(key) {
        this._cache.markActive(key);
    }
    // The invocation count pins the teardown to the invocation that was
    // actually evicted: between requesting eviction and the server's ack, the
    // server may restore and re-dispatch the next invocation, which registers
    // under the same action key and must not be torn down.
    _evictRun(key, invocationCount) {
        const current = this._cache.get(key);
        if (current && current.invocationCount !== invocationCount)
            return;
        this._cancelLocal(key, invocationCount);
        this.unregisterRun(key);
    }
    _tickSafe() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._ticking)
                return;
            this._ticking = true;
            try {
                yield this._tick();
            }
            catch (err) {
                this._logger.error(`DurableEvictionManager: error in eviction loop: ${(0, hatchet_error_1.getErrorMessage)(err)}`);
            }
            finally {
                this._ticking = false;
            }
        });
    }
    _tick() {
        return __awaiter(this, void 0, void 0, function* () {
            const evictedThisTick = new Set();
            while (true) {
                const key = this._cache.selectEvictionCandidate(Date.now(), this._durableSlots, this._config.reserveSlots, this._config.minWaitForCapacityEvictionMs);
                if (!key)
                    return;
                if (evictedThisTick.has(key))
                    return;
                evictedThisTick.add(key);
                const rec = this._cache.get(key);
                if (!rec || !rec.evictionPolicy)
                    continue;
                this._logger.debug(`DurableEvictionManager: evicting task_run_external_id=${rec.taskRunExternalId} ` +
                    `wait_kind=${rec.waitKind} resource_id=${rec.waitResourceId}`);
                yield this._requestEvictionWithAck(key, rec);
                this._evictRun(key, rec.invocationCount);
            }
        });
    }
    handleServerEviction(taskRunExternalId, invocationCount) {
        const key = this._cache.findKeyByTaskRunExternalId(taskRunExternalId);
        if (!key)
            return;
        const rec = this._cache.get(key);
        if (rec && rec.invocationCount !== invocationCount)
            return;
        this._logger.info(`DurableEvictionManager: server-initiated eviction for task_run_external_id=${taskRunExternalId} invocation_count=${invocationCount}`);
        this._evictRun(key, invocationCount);
    }
    evictAllWaiting() {
        return __awaiter(this, void 0, void 0, function* () {
            this.stop();
            const waiting = this._cache.getAllWaiting();
            let evicted = 0;
            for (const rec of waiting) {
                rec.evictionReason = (0, eviction_cache_1.buildEvictionReason)(eviction_cache_1.EvictionCause.WORKER_SHUTDOWN, rec);
                this._logger.debug(`DurableEvictionManager: shutdown-evicting task_run_external_id=${rec.taskRunExternalId} ` +
                    `wait_kind=${rec.waitKind}`);
                try {
                    yield this._requestEvictionWithAck(rec.key, rec);
                }
                catch (err) {
                    this._logger.error(`DurableEvictionManager: failed to send eviction for ` +
                        `task_run_external_id=${rec.taskRunExternalId}: ${(0, hatchet_error_1.getErrorMessage)(err)}`);
                }
                // Always cancel locally even if the server ACK failed, so the
                // future settles and exitGracefully doesn't hang.
                // This will get resolved by the reassignment of the task.
                this._evictRun(rec.key, rec.invocationCount);
                evicted++;
            }
            return evicted;
        });
    }
}
exports.DurableEvictionManager = DurableEvictionManager;
