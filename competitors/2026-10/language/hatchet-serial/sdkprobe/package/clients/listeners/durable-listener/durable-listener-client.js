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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncGenerator = (this && this.__asyncGenerator) || function (thisArg, _arguments, generator) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var g = generator.apply(thisArg, _arguments || []), i, q = [];
    return i = Object.create((typeof AsyncIterator === "function" ? AsyncIterator : Object).prototype), verb("next"), verb("throw"), verb("return", awaitReturn), i[Symbol.asyncIterator] = function () { return this; }, i;
    function awaitReturn(f) { return function (v) { return Promise.resolve(v).then(f, reject); }; }
    function verb(n, f) { if (g[n]) { i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; if (f) i[n] = f(i[n]); } }
    function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
    function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
    function fulfill(value) { resume("next", value); }
    function reject(value) { resume("throw", value); }
    function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LegacyDurableEventStreamable = exports.DurableListenerClient = void 0;
const events_1 = require("events");
const abort_controller_x_1 = require("abort-controller-x");
const hatchet_error_1 = require("../../../util/errors/hatchet-error");
const dispatcher_1 = require("../../../protoc/v1/dispatcher");
const non_determinism_error_1 = require("../../../util/errors/non-determinism-error");
const task_run_terminated_error_1 = require("../../../util/errors/task-run-terminated-error");
const abort_error_1 = require("../../../util/abort-error");
const abort_signal_1 = require("../../../util/abort-signal");
const sleep_1 = __importDefault(require("../../../util/sleep"));
const listener_severity_1 = require("../../dispatcher/listener-severity");
class TTLMap {
    constructor(ttlMs) {
        this.ttlMs = ttlMs;
        this.cache = new Map();
        this.timer = setInterval(() => this.evict(), ttlMs);
    }
    set(key, value) {
        this.cache.set(key, { value, expiresAt: Date.now() + this.ttlMs });
    }
    get(key) {
        var _a;
        return (_a = this.cache.get(key)) === null || _a === void 0 ? void 0 : _a.value;
    }
    get size() {
        return this.cache.size;
    }
    has(key) {
        return this.cache.has(key);
    }
    delete(key) {
        return this.cache.delete(key);
    }
    keys() {
        return this.cache.keys();
    }
    pop(key) {
        const entry = this.cache.get(key);
        if (entry) {
            this.cache.delete(key);
            return entry.value;
        }
        return undefined;
    }
    clear() {
        this.cache.clear();
    }
    destroy() {
        clearInterval(this.timer);
        this.cache.clear();
    }
    evict() {
        const now = Date.now();
        for (const [key, entry] of this.cache) {
            if (entry.expiresAt <= now) {
                this.cache.delete(key);
            }
        }
    }
}
const DEFAULT_RECONNECT_INTERVAL = 3000;
const EVICTION_ACK_TIMEOUT_MS = 30000;
const WORKER_STATUS_RESEND_INTERVAL_MS = 5000;
const WORKER_STATUS_RESEND_MIN_PENDING_MS = 2000;
const WORKER_STATUS_MAX_ENTRIES_PER_REQUEST = 10000;
const RUN_CHILDREN_MAX_CHILDREN_PER_REQUEST = 100;
function eventLogEntryResultFromProto(proto) {
    var _a, _b, _c, _d;
    let payload;
    if (!proto.isFailure && proto.payload && proto.payload.length > 0) {
        payload = JSON.parse(new TextDecoder().decode(proto.payload));
    }
    return {
        durableTaskExternalId: (_b = (_a = proto.ref) === null || _a === void 0 ? void 0 : _a.durableTaskExternalId) !== null && _b !== void 0 ? _b : '',
        nodeId: (_d = (_c = proto.ref) === null || _c === void 0 ? void 0 : _c.nodeId) !== null && _d !== void 0 ? _d : 0,
        payload,
        isFailure: proto.isFailure,
        errorMessage: proto.errorMessage,
    };
}
function awaitedCompletedEntryFromCallbackKey(key) {
    const [durableTaskExternalId, invocationCount, branchId, nodeId] = key.split(':');
    return {
        durableTaskExternalId,
        invocationCount: parseInt(invocationCount, 10),
        branchId: parseInt(branchId, 10),
        nodeId: parseInt(nodeId, 10),
    };
}
function ackKey(taskExtId, invocationCount) {
    return `${taskExtId}:${invocationCount}`;
}
function completionOrderKey(taskExtId, invocationCount) {
    return `${taskExtId}:${invocationCount}`;
}
function callbackKey(taskExtId, invocationCount, branchId, nodeId) {
    return `${taskExtId}:${invocationCount}:${branchId}:${nodeId}`;
}
function evictionKey(taskExtId, invocationCount) {
    return `${taskExtId}:${invocationCount}`;
}
function deferred() {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
}
class DurableListenerClient {
    constructor(config, channel, factory) {
        this._running = false;
        this._requestQueue = [];
        this._pendingEventAcks = new Map();
        this._pendingCallbacks = new Map();
        // Completions held in server delivery order until their waiters can
        // consume them without overtaking an earlier-delivered completion.
        // The TTL only garbage-collects queues for invocations that died
        // without consuming everything; the server stall-evicts long before.
        this._orderedCompletions = new TTLMap(300000);
        this._pendingEvictionAcks = new Map();
        this._newlyWaitingKeys = new Set();
        this._waitingSince = new Map();
        this._workerStatusSendScheduled = false;
        this._consecutiveFailures = 0;
        this._drainsInProgress = new Set();
        this._drainRerunRequests = new Set();
        this.config = config;
        this.client = factory.create(dispatcher_1.V1DispatcherDefinition, channel);
        this.logger = config.logger(`DurableListener`, config.log_level);
    }
    get workerId() {
        return this._workerId;
    }
    start(workerId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._startLock) {
                yield this._startLock;
                return;
            }
            this._startLock = this._doStart(workerId);
            yield this._startLock;
        });
    }
    _doStart(workerId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._running)
                return;
            this._workerId = workerId;
            this._running = true;
            this._consecutiveFailures = 0;
            yield this._connect();
            this._startStatusPolling();
        });
    }
    ensureStarted(workerId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this._running) {
                yield this.start(workerId);
            }
        });
    }
    stop() {
        return __awaiter(this, void 0, void 0, function* () {
            this._running = false;
            this._startLock = undefined;
            if (this._statusInterval) {
                clearInterval(this._statusInterval);
                this._statusInterval = undefined;
            }
            if (this._receiveAbort) {
                this._receiveAbort.abort();
            }
            this._failPendingAcks(new Error('DurableListener stopped'));
            this._orderedCompletions.destroy();
        });
    }
    _connect() {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            this.logger.info('durable event listener connecting...');
            (_a = this._receiveAbort) === null || _a === void 0 ? void 0 : _a.abort();
            this._requestQueue = [];
            this._receiveAbort = new AbortController();
            this._enqueueRequest({
                registerWorker: { workerId: this._workerId },
            });
            this._enqueueFullWorkerStatus();
            void this._streamLoop();
            this.logger.info('durable event listener connected');
        });
    }
    _streamLoop() {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, e_1, _b, _c;
            var _d;
            while (this._running) {
                try {
                    const stream = this.client.durableTask(this._requestIterator(), {
                        signal: (_d = this._receiveAbort) === null || _d === void 0 ? void 0 : _d.signal,
                    });
                    let receivedResponse = false;
                    try {
                        for (var _e = true, stream_1 = (e_1 = void 0, __asyncValues(stream)), stream_1_1; stream_1_1 = yield stream_1.next(), _a = stream_1_1.done, !_a; _e = true) {
                            _c = stream_1_1.value;
                            _e = false;
                            const response = _c;
                            if (!receivedResponse) {
                                receivedResponse = true;
                                this._consecutiveFailures = 0;
                            }
                            this._handleResponse(response);
                        }
                    }
                    catch (e_1_1) { e_1 = { error: e_1_1 }; }
                    finally {
                        try {
                            if (!_e && !_a && (_b = stream_1.return)) yield _b.call(stream_1);
                        }
                        finally { if (e_1) throw e_1.error; }
                    }
                    if (this._running) {
                        this._consecutiveFailures += 1;
                        const message = `durable event listener disconnected (EOF), reconnecting in ${DEFAULT_RECONNECT_INTERVAL}ms...`;
                        const eofSeverity = (0, listener_severity_1.classifyListenerFailure)(undefined, this._consecutiveFailures);
                        if (eofSeverity !== 'silent') {
                            this.logger[eofSeverity](message);
                        }
                        this._failPendingAcks(new Error('durable stream disconnected'));
                        yield (0, sleep_1.default)(DEFAULT_RECONNECT_INTERVAL);
                        yield this._connect();
                        return;
                    }
                }
                catch (e) {
                    if ((0, abort_controller_x_1.isAbortError)(e)) {
                        this.logger.debug('durable event listener aborted');
                        return;
                    }
                    this._consecutiveFailures += 1;
                    const message = `error in durable event listener: ${(0, hatchet_error_1.getErrorMessage)(e)}`;
                    const errSeverity = (0, listener_severity_1.classifyListenerFailure)(e, this._consecutiveFailures);
                    if (errSeverity !== 'silent') {
                        this.logger[errSeverity](message);
                    }
                    if (this._running) {
                        this._failPendingAcks(new Error(`durable stream error: ${(0, hatchet_error_1.getErrorMessage)(e)}`));
                        yield (0, sleep_1.default)(DEFAULT_RECONNECT_INTERVAL);
                        yield this._connect();
                        return;
                    }
                }
            }
        });
    }
    _requestIterator() {
        return __asyncGenerator(this, arguments, function* _requestIterator_1() {
            const queue = this._requestQueue;
            const { signal } = this._receiveAbort;
            while (this._running && !signal.aborted) {
                while (queue.length > 0) {
                    yield yield __await(queue.shift());
                }
                yield __await(new Promise((resolve) => {
                    this._requestNotify = resolve;
                    signal.addEventListener('abort', () => resolve(), { once: true });
                }));
                this._requestNotify = undefined;
            }
        });
    }
    _enqueueRequest(request) {
        this._requestQueue.push(request);
        if (this._requestNotify) {
            this._requestNotify();
        }
    }
    _startStatusPolling() {
        if (this._statusInterval) {
            clearInterval(this._statusInterval);
        }
        this._statusInterval = setInterval(() => {
            this._enqueueWorkerStatusForLongPendingWaiters();
        }, WORKER_STATUS_RESEND_INTERVAL_MS);
    }
    _registerWaitingKey(key) {
        this._waitingSince.set(key, Date.now());
        this._newlyWaitingKeys.add(key);
    }
    // Waiters registered in the same turn of the event loop share one request,
    // instead of each sending its own.
    _scheduleWorkerStatusSend() {
        if (this._workerStatusSendScheduled)
            return;
        this._workerStatusSendScheduled = true;
        setImmediate(() => this._enqueueWorkerStatusForNewlyWaitingKeys());
    }
    _enqueueWorkerStatusForNewlyWaitingKeys() {
        this._workerStatusSendScheduled = false;
        const keys = [...this._newlyWaitingKeys].filter((key) => this._pendingCallbacks.has(key));
        this._newlyWaitingKeys.clear();
        this._enqueueWorkerStatusFor(keys);
    }
    _enqueueWorkerStatusForLongPendingWaiters() {
        const registeredBefore = Date.now() - WORKER_STATUS_RESEND_MIN_PENDING_MS;
        const keys = [];
        for (const [key, waitingSince] of this._waitingSince) {
            if (!this._pendingCallbacks.has(key)) {
                this._waitingSince.delete(key);
            }
            else if (waitingSince <= registeredBefore) {
                keys.push(key);
            }
        }
        this._enqueueWorkerStatusFor(keys);
    }
    _enqueueFullWorkerStatus() {
        this._newlyWaitingKeys.clear();
        this._enqueueWorkerStatusFor([...this._pendingCallbacks.keys()]);
    }
    _enqueueWorkerStatusFor(keys, maxEntriesPerRequest = WORKER_STATUS_MAX_ENTRIES_PER_REQUEST) {
        if (!this._workerId)
            return;
        for (let start = 0; start < keys.length; start += maxEntriesPerRequest) {
            this._enqueueRequest({
                workerStatus: {
                    workerId: this._workerId,
                    waitingEntries: keys
                        .slice(start, start + maxEntriesPerRequest)
                        .map(awaitedCompletedEntryFromCallbackKey),
                },
            });
        }
    }
    _failPendingAcks(exc) {
        for (const d of this._pendingEventAcks.values()) {
            d.reject(exc);
        }
        this._pendingEventAcks.clear();
        for (const d of this._pendingEvictionAcks.values()) {
            d.reject(exc);
        }
        this._pendingEvictionAcks.clear();
    }
    _failAllPending(exc) {
        this._failPendingAcks(exc);
        for (const d of this._pendingCallbacks.values()) {
            d.reject(exc);
        }
        this._pendingCallbacks.clear();
        this._orderedCompletions.clear();
    }
    _handleResponse(response) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y, _z, _0, _1, _2, _3, _4, _5;
        if (response.registerWorker) {
            // registration acknowledged
        }
        else if (response.triggerRunsAck) {
            const ack = response.triggerRunsAck;
            const key = ackKey(ack.durableTaskExternalId, ack.invocationCount);
            const pending = this._pendingEventAcks.get(key);
            if (pending) {
                pending.resolve({
                    ackType: 'run',
                    invocationCount: ack.invocationCount,
                    durableTaskExternalId: ack.durableTaskExternalId,
                    runEntries: (ack.runEntries || []).map((e) => ({
                        nodeId: e.nodeId,
                        branchId: e.branchId,
                        workflowRunExternalId: e.workflowRunExternalId,
                    })),
                });
                this._pendingEventAcks.delete(key);
            }
        }
        else if (response.memoAck) {
            const ack = response.memoAck;
            const { ref } = ack;
            const key = ackKey((_a = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _a !== void 0 ? _a : '', (_b = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _b !== void 0 ? _b : 0);
            const pending = this._pendingEventAcks.get(key);
            if (pending) {
                pending.resolve({
                    ackType: 'memo',
                    invocationCount: (_c = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _c !== void 0 ? _c : 0,
                    durableTaskExternalId: (_d = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _d !== void 0 ? _d : '',
                    branchId: (_e = ref === null || ref === void 0 ? void 0 : ref.branchId) !== null && _e !== void 0 ? _e : 0,
                    nodeId: (_f = ref === null || ref === void 0 ? void 0 : ref.nodeId) !== null && _f !== void 0 ? _f : 0,
                    memoAlreadyExisted: ack.memoAlreadyExisted,
                    memoResultPayload: ack.memoResultPayload,
                });
                this._pendingEventAcks.delete(key);
            }
        }
        else if (response.waitForAck) {
            const ack = response.waitForAck;
            const { ref } = ack;
            const key = ackKey((_g = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _g !== void 0 ? _g : '', (_h = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _h !== void 0 ? _h : 0);
            const pending = this._pendingEventAcks.get(key);
            if (pending) {
                pending.resolve({
                    ackType: 'waitFor',
                    invocationCount: (_j = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _j !== void 0 ? _j : 0,
                    durableTaskExternalId: (_k = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _k !== void 0 ? _k : '',
                    branchId: (_l = ref === null || ref === void 0 ? void 0 : ref.branchId) !== null && _l !== void 0 ? _l : 0,
                    nodeId: (_m = ref === null || ref === void 0 ? void 0 : ref.nodeId) !== null && _m !== void 0 ? _m : 0,
                });
                this._pendingEventAcks.delete(key);
            }
        }
        else if (response.entryCompleted) {
            const completed = response.entryCompleted;
            const { ref } = completed;
            const key = callbackKey((_o = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _o !== void 0 ? _o : '', (_p = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _p !== void 0 ? _p : 0, (_q = ref === null || ref === void 0 ? void 0 : ref.branchId) !== null && _q !== void 0 ? _q : 0, (_r = ref === null || ref === void 0 ? void 0 : ref.nodeId) !== null && _r !== void 0 ? _r : 0);
            const result = eventLogEntryResultFromProto(completed);
            const orderKey = completionOrderKey((_s = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _s !== void 0 ? _s : '', (_t = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _t !== void 0 ? _t : 0);
            const queue = (_u = this._orderedCompletions.get(orderKey)) !== null && _u !== void 0 ? _u : {
                pending: [],
                pendingKeys: new Set(),
                delivered: new Set(),
            };
            if (!queue.delivered.has(key)) {
                queue.delivered.add(key);
                queue.pendingKeys.add(key);
                queue.pending.push({ key, result });
            }
            else if (!queue.pendingKeys.has(key)) {
                // Re-delivery of a completion that already drained (reconnect,
                // worker-status re-send, or a repeated wait on a node deduped by
                // child key). Its satisfied order was released before anything
                // still queued, so handing it straight to a waiter keeps order.
                const redelivered = this._pendingCallbacks.get(key);
                if (redelivered) {
                    this._pendingCallbacks.delete(key);
                    redelivered.resolve(result);
                }
            }
            this._orderedCompletions.set(orderKey, queue);
            this._drainOrderedCompletions(orderKey);
        }
        else if (response.evictionAck) {
            const ack = response.evictionAck;
            const key = evictionKey(ack.durableTaskExternalId, ack.invocationCount);
            const pending = this._pendingEvictionAcks.get(key);
            if (pending) {
                pending.resolve();
                this._pendingEvictionAcks.delete(key);
            }
        }
        else if (response.serverEvict) {
            const evict = response.serverEvict;
            this.logger.info(`received server eviction notification for task ${evict.durableTaskExternalId} ` +
                `invocation ${evict.invocationCount}: ${evict.reason}`);
            // onServerEvict aborts the run first so waiters settle as aborted
            // (eviction) rather than with cleanup's generic rejection.
            if (this.onServerEvict) {
                this.onServerEvict(evict.durableTaskExternalId, evict.invocationCount);
            }
            this.cleanupTaskState(evict.durableTaskExternalId, evict.invocationCount);
        }
        else if (response.error) {
            const { error } = response;
            const { ref } = error;
            let exc;
            if (error.errorType === dispatcher_1.DurableTaskErrorType.DURABLE_TASK_ERROR_TYPE_NONDETERMINISM) {
                exc = new non_determinism_error_1.NonDeterminismError((_v = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _v !== void 0 ? _v : '', (_w = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _w !== void 0 ? _w : 0, (_x = ref === null || ref === void 0 ? void 0 : ref.nodeId) !== null && _x !== void 0 ? _x : 0, error.errorMessage);
            }
            else {
                exc = new Error(`Unspecified durable task error: ${error.errorMessage} (type: ${error.errorType})`);
            }
            const eAckKey = ackKey((_y = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _y !== void 0 ? _y : '', (_z = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _z !== void 0 ? _z : 0);
            const pendingAck = this._pendingEventAcks.get(eAckKey);
            if (pendingAck) {
                pendingAck.reject(exc);
                this._pendingEventAcks.delete(eAckKey);
            }
            const eCbKey = callbackKey((_0 = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _0 !== void 0 ? _0 : '', (_1 = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _1 !== void 0 ? _1 : 0, (_2 = ref === null || ref === void 0 ? void 0 : ref.branchId) !== null && _2 !== void 0 ? _2 : 0, (_3 = ref === null || ref === void 0 ? void 0 : ref.nodeId) !== null && _3 !== void 0 ? _3 : 0);
            const pendingCb = this._pendingCallbacks.get(eCbKey);
            if (pendingCb) {
                pendingCb.reject(exc);
                this._pendingCallbacks.delete(eCbKey);
            }
            const eEvKey = evictionKey((_4 = ref === null || ref === void 0 ? void 0 : ref.durableTaskExternalId) !== null && _4 !== void 0 ? _4 : '', (_5 = ref === null || ref === void 0 ? void 0 : ref.invocationCount) !== null && _5 !== void 0 ? _5 : 0);
            const pendingEv = this._pendingEvictionAcks.get(eEvKey);
            if (pendingEv) {
                pendingEv.reject(exc);
                this._pendingEvictionAcks.delete(eEvKey);
            }
        }
    }
    sendEvent(durableTaskExternalId, invocationCount, event) {
        return __awaiter(this, void 0, void 0, function* () {
            let request;
            switch (event.kind) {
                case 'runChildren':
                    return this._sendRunChildrenEvent(durableTaskExternalId, invocationCount, event);
                case 'waitFor': {
                    const waitForReq = {
                        invocationCount,
                        durableTaskExternalId,
                        waitForConditions: event.waitForConditions,
                        label: event.label,
                    };
                    request = { waitFor: waitForReq };
                    break;
                }
                case 'memo': {
                    const memoReq = {
                        invocationCount,
                        durableTaskExternalId,
                        key: event.memoKey,
                        payload: event.payload,
                    };
                    request = { memo: memoReq };
                    break;
                }
                default: {
                    const _ = event;
                    throw new Error(`Unknown durable task send event: ${_}`);
                }
            }
            return this._sendRequestAndAwaitAck(durableTaskExternalId, invocationCount, request);
        });
    }
    _sendRequestAndAwaitAck(durableTaskExternalId, invocationCount, request) {
        const d = deferred();
        this._pendingEventAcks.set(ackKey(durableTaskExternalId, invocationCount), d);
        this._enqueueRequest(request);
        return d.promise;
    }
    _sendRunChildrenEvent(durableTaskExternalId, invocationCount, event) {
        return __awaiter(this, void 0, void 0, function* () {
            const runEntries = [];
            for (let start = 0; start < event.triggerOpts.length; start += RUN_CHILDREN_MAX_CHILDREN_PER_REQUEST) {
                const triggerRunsReq = {
                    invocationCount,
                    durableTaskExternalId,
                    triggerOpts: event.triggerOpts.slice(start, start + RUN_CHILDREN_MAX_CHILDREN_PER_REQUEST),
                };
                const ack = yield this._sendRequestAndAwaitAck(durableTaskExternalId, invocationCount, {
                    triggerRuns: triggerRunsReq,
                });
                if (ack.ackType !== 'run') {
                    throw new Error(`Expected a run ack for a trigger runs request, got ${ack.ackType}`);
                }
                runEntries.push(...ack.runEntries);
            }
            return { ackType: 'run', invocationCount, durableTaskExternalId, runEntries };
        });
    }
    // Hands queued completions to their waiters strictly in delivery order.
    // Stops at the first completion whose waiter has not registered yet:
    // releasing a later completion first would resume its coroutine ahead of
    // the recorded order and diverge the re-emitted event sequence.
    _drainOrderedCompletions(orderKey) {
        if (this._drainsInProgress.has(orderKey)) {
            this._drainRerunRequests.add(orderKey);
            return;
        }
        this._drainsInProgress.add(orderKey);
        void this._drainLoop(orderKey).finally(() => {
            this._drainsInProgress.delete(orderKey);
            if (this._drainRerunRequests.delete(orderKey)) {
                this._drainOrderedCompletions(orderKey);
            }
        });
    }
    _drainLoop(orderKey) {
        return __awaiter(this, void 0, void 0, function* () {
            let released = false;
            while (true) {
                if (released) {
                    // A macrotask boundary between releases lets the just-resumed waiter
                    // run to its next suspension point (emitting its next durable event)
                    // before the following completion is released. Resolving several
                    // deferreds back-to-back would instead resume them in the order their
                    // continuations happened to be attached, not in release order.
                    yield new Promise((resolve) => {
                        setImmediate(resolve);
                    });
                }
                const queue = this._orderedCompletions.get(orderKey);
                if (!queue || queue.pending.length === 0)
                    return;
                const [head] = queue.pending;
                const waiter = this._pendingCallbacks.get(head.key);
                if (!waiter)
                    return;
                queue.pending.shift();
                queue.pendingKeys.delete(head.key);
                this._pendingCallbacks.delete(head.key);
                waiter.resolve(head.result);
                released = true;
            }
        });
    }
    // A memo's value arrives in its ack and its completion is never awaited,
    // but the server still delivers that completion on replay, in recorded
    // order. Register a waiter that nothing blocks on so the drain can hand the
    // completion through instead of stalling the queue at it forever.
    consumeCallbackWithoutBlocking(durableTaskExternalId, invocationCount, branchId, nodeId) {
        const key = callbackKey(durableTaskExternalId, invocationCount, branchId, nodeId);
        if (this._pendingCallbacks.has(key))
            return;
        const d = deferred();
        d.promise.catch(() => { });
        this._pendingCallbacks.set(key, d);
        this._registerWaitingKey(key);
        this._drainOrderedCompletions(completionOrderKey(durableTaskExternalId, invocationCount));
    }
    waitForCallback(durableTaskExternalId, invocationCount, branchId, nodeId, opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const signal = opts === null || opts === void 0 ? void 0 : opts.signal;
            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                return Promise.reject((0, abort_error_1.createAbortError)('Operation cancelled by AbortSignal'));
            }
            const key = callbackKey(durableTaskExternalId, invocationCount, branchId, nodeId);
            let d = this._pendingCallbacks.get(key);
            if (!d) {
                d = deferred();
                // A deferred can outlive its waiters (eviction aborts the caller while
                // the entry stays registered); a later cleanupTaskState rejection must
                // not crash the process as an unhandled rejection.
                d.promise.catch(() => { });
                this._pendingCallbacks.set(key, d);
                this._registerWaitingKey(key);
                this._drainOrderedCompletions(completionOrderKey(durableTaskExternalId, invocationCount));
                if (this._pendingCallbacks.has(key)) {
                    this._scheduleWorkerStatusSend();
                }
            }
            if (!signal) {
                return d.promise;
            }
            return new Promise((resolve, reject) => {
                let settled = false;
                const onAbort = () => {
                    if (settled)
                        return;
                    settled = true;
                    reject((0, abort_error_1.createAbortError)('Operation cancelled by AbortSignal'));
                };
                (0, abort_signal_1.bindAbortSignalHandler)(signal, onAbort);
                d.promise.then((value) => {
                    if (settled)
                        return;
                    settled = true;
                    signal.removeEventListener('abort', onAbort);
                    resolve(value);
                }, (err) => {
                    if (settled)
                        return;
                    settled = true;
                    signal.removeEventListener('abort', onAbort);
                    reject(err);
                });
            });
        });
    }
    cleanupTaskState(durableTaskExternalId, invocationCount) {
        // Rejecting with TaskRunTerminatedError marks any coroutine still blocked
        // on this invocation's state as evicted rather than failed, matching
        // Python's future cancellation on cleanup.
        const evicted = () => new task_run_terminated_error_1.TaskRunTerminatedError('evicted', 'task state cleaned up');
        for (const [k, d] of this._pendingCallbacks) {
            const parts = k.split(':');
            if (parts[0] === durableTaskExternalId && parseInt(parts[1], 10) <= invocationCount) {
                d.reject(evicted());
                this._pendingCallbacks.delete(k);
            }
        }
        for (const [k, d] of this._pendingEventAcks) {
            const parts = k.split(':');
            if (parts[0] === durableTaskExternalId && parseInt(parts[1], 10) <= invocationCount) {
                d.reject(evicted());
                this._pendingEventAcks.delete(k);
            }
        }
        for (const k of this._orderedCompletions.keys()) {
            const parts = k.split(':');
            if (parts[0] === durableTaskExternalId && parseInt(parts[1], 10) <= invocationCount) {
                this._orderedCompletions.delete(k);
            }
        }
    }
    sendEvictInvocation(durableTaskExternalId, invocationCount, reason) {
        return __awaiter(this, void 0, void 0, function* () {
            const key = evictionKey(durableTaskExternalId, invocationCount);
            const pending = this._pendingEvictionAcks.get(key);
            if (pending) {
                return pending.promise;
            }
            const d = deferred();
            this._pendingEvictionAcks.set(key, d);
            const req = {
                invocationCount,
                durableTaskExternalId,
                reason,
            };
            this._enqueueRequest({ evictInvocation: req });
            const timeout = setTimeout(() => {
                d.reject(new Error(`Eviction ack timed out after ${EVICTION_ACK_TIMEOUT_MS}ms for task ${durableTaskExternalId} invocation ${invocationCount}`));
            }, EVICTION_ACK_TIMEOUT_MS);
            try {
                yield d.promise;
            }
            finally {
                clearTimeout(timeout);
                if (this._pendingEvictionAcks.get(key) === d) {
                    this._pendingEvictionAcks.delete(key);
                }
            }
        });
    }
    sendMemoCompletedNotification(durableTaskExternalId, nodeId, branchId, invocationCount, memoKey, memoResultPayload) {
        return __awaiter(this, void 0, void 0, function* () {
            const ref = {
                durableTaskExternalId,
                invocationCount,
                branchId,
                nodeId,
            };
            const req = {
                ref,
                payload: memoResultPayload !== null && memoResultPayload !== void 0 ? memoResultPayload : new Uint8Array(),
                memoKey,
            };
            this._enqueueRequest({ completeMemo: req });
        });
    }
    /**
     * @deprecated Legacy backward-compat: uses the old unary RegisterDurableEvent RPC.
     */
    registerDurableEvent(request) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.client.registerDurableEvent({
                taskId: request.taskId,
                signalKey: request.signalKey,
                conditions: {
                    sleepConditions: request.sleepConditions,
                    userEventConditions: request.userEventConditions,
                },
            });
        });
    }
    /**
     * @deprecated Legacy backward-compat: uses the old streaming ListenForDurableEvent RPC.
     */
    subscribe(request) {
        if (!this._legacyPooledListener) {
            this._legacyPooledListener = new LegacyPooledListener(this);
        }
        return this._legacyPooledListener.subscribe(request);
    }
    /**
     * @deprecated Legacy backward-compat: subscribes and waits for a single result.
     */
    result(request, opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const subscriber = this.subscribe(request);
            return subscriber.get({ signal: opts === null || opts === void 0 ? void 0 : opts.signal });
        });
    }
}
exports.DurableListenerClient = DurableListenerClient;
/**
 * @deprecated Legacy support for the old streaming ListenForDurableEvent RPC.
 */
class LegacyDurableEventStreamable {
    constructor(onCleanup) {
        this.responseEmitter = new events_1.EventEmitter();
        this._onCleanup = onCleanup;
    }
    get(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const signal = opts === null || opts === void 0 ? void 0 : opts.signal;
            return new Promise((resolve, reject) => {
                let cleanedUp = false;
                const cleanup = () => {
                    if (cleanedUp)
                        return;
                    cleanedUp = true;
                    this.responseEmitter.removeListener('response', onResponse);
                    if (signal) {
                        signal.removeEventListener('abort', onAbort);
                    }
                    this._onCleanup();
                };
                const onResponse = (event) => {
                    cleanup();
                    resolve(event);
                };
                const onAbort = () => {
                    cleanup();
                    reject((0, abort_error_1.createAbortError)('Operation cancelled by AbortSignal'));
                };
                if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                    onAbort();
                    return;
                }
                this.responseEmitter.once('response', onResponse);
                if (signal) {
                    (0, abort_signal_1.bindAbortSignalHandler)(signal, onAbort);
                }
            });
        });
    }
}
exports.LegacyDurableEventStreamable = LegacyDurableEventStreamable;
/**
 * @deprecated Legacy pooled listener for old ListenForDurableEvent streaming RPC.
 */
class LegacyPooledListener {
    constructor(client) {
        this.requestEmitter = new events_1.EventEmitter();
        this.signal = new AbortController();
        this.subscribers = {};
        this.taskSignalKeyToSubscriptionIds = {};
        this.subscriptionCounter = 0;
        this.currRequester = 0;
        this.client = client;
        this.init();
    }
    init() {
        return __awaiter(this, arguments, void 0, function* (retries = 0) {
            var _a, e_2, _b, _c;
            const MAX_RETRY_INTERVAL = 5000;
            const BASE_RETRY_INTERVAL = 100;
            const MAX_RETRY_COUNT = 5;
            if (retries > 0) {
                const backoffTime = Math.min(BASE_RETRY_INTERVAL * 2 ** (retries - 1), MAX_RETRY_INTERVAL);
                yield (0, sleep_1.default)(backoffTime);
            }
            if (retries > MAX_RETRY_COUNT)
                return;
            try {
                this.signal = new AbortController();
                this.currRequester++;
                this.listener = this.client.client.listenForDurableEvent(this.request(), {
                    signal: this.signal.signal,
                });
                try {
                    for (var _d = true, _e = __asyncValues(this.listener), _f; _f = yield _e.next(), _a = _f.done, !_a; _d = true) {
                        _c = _f.value;
                        _d = false;
                        const event = _c;
                        const subscriptionKey = `${event.taskId}|${event.signalKey}`;
                        const subscriptionIds = this.taskSignalKeyToSubscriptionIds[subscriptionKey] || [];
                        for (const subId of subscriptionIds) {
                            const emitter = this.subscribers[subId];
                            if (emitter) {
                                emitter.responseEmitter.emit('response', event);
                                this.cleanupSubscription(subId);
                            }
                        }
                    }
                }
                catch (e_2_1) { e_2 = { error: e_2_1 }; }
                finally {
                    try {
                        if (!_d && !_a && (_b = _e.return)) yield _b.call(_e);
                    }
                    finally { if (e_2) throw e_2.error; }
                }
            }
            catch (e) {
                if ((0, abort_controller_x_1.isAbortError)(e))
                    return;
            }
            finally {
                if (Object.keys(this.subscribers).length > 0) {
                    this.init(retries + 1);
                }
            }
        });
    }
    cleanupSubscription(subscriptionId) {
        var _a;
        const emitter = this.subscribers[subscriptionId];
        if (!emitter)
            return;
        const key = (_a = Object.entries(this.taskSignalKeyToSubscriptionIds).find(([, ids]) => ids.includes(subscriptionId))) === null || _a === void 0 ? void 0 : _a[0];
        delete this.subscribers[subscriptionId];
        if (key && this.taskSignalKeyToSubscriptionIds[key]) {
            this.taskSignalKeyToSubscriptionIds[key] = this.taskSignalKeyToSubscriptionIds[key].filter((id) => id !== subscriptionId);
            if (this.taskSignalKeyToSubscriptionIds[key].length === 0) {
                delete this.taskSignalKeyToSubscriptionIds[key];
            }
        }
    }
    subscribe(request) {
        const subscriptionId = (this.subscriptionCounter++).toString();
        const subscriber = new LegacyDurableEventStreamable(() => this.cleanupSubscription(subscriptionId));
        this.subscribers[subscriptionId] = subscriber;
        const key = `${request.taskId}|${request.signalKey}`;
        if (!this.taskSignalKeyToSubscriptionIds[key]) {
            this.taskSignalKeyToSubscriptionIds[key] = [];
        }
        this.taskSignalKeyToSubscriptionIds[key].push(subscriptionId);
        this.requestEmitter.emit('subscribe', request);
        return subscriber;
    }
    request() {
        return __asyncGenerator(this, arguments, function* request_1() {
            var _a, e_3, _b, _c;
            const { currRequester } = this;
            const existing = new Set();
            for (const key in this.taskSignalKeyToSubscriptionIds) {
                if (this.taskSignalKeyToSubscriptionIds[key].length > 0) {
                    const [taskId, signalKey] = key.split('|');
                    existing.add(key);
                    yield yield __await({ taskId, signalKey });
                }
            }
            try {
                for (var _d = true, _e = __asyncValues((0, events_1.on)(this.requestEmitter, 'subscribe')), _f; _f = yield __await(_e.next()), _a = _f.done, !_a; _d = true) {
                    _c = _f.value;
                    _d = false;
                    const e = _c;
                    if (currRequester !== this.currRequester)
                        break;
                    const request = e[0];
                    const key = `${request.taskId}|${request.signalKey}`;
                    if (!existing.has(key)) {
                        existing.add(key);
                        yield yield __await(request);
                    }
                }
            }
            catch (e_3_1) { e_3 = { error: e_3_1 }; }
            finally {
                try {
                    if (!_d && !_a && (_b = _e.return)) yield __await(_b.call(_e));
                }
                finally { if (e_3) throw e_3.error; }
            }
        });
    }
}
