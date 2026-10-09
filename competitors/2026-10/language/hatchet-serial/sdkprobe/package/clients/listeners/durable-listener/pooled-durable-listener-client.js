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
exports.DurableEventGrpcPooledListener = exports.DurableEventStreamable = void 0;
const events_1 = require("events");
const abort_controller_x_1 = require("abort-controller-x");
const hatchet_error_1 = require("../../../util/errors/hatchet-error");
const sleep_1 = __importDefault(require("../../../util/sleep"));
const abort_error_1 = require("../../../util/abort-error");
const abort_signal_1 = require("../../../util/abort-signal");
class DurableEventStreamable {
    constructor(listener, taskId, signalKey, subscriptionId, onCleanup) {
        this.responseEmitter = new events_1.EventEmitter();
        this.listener = listener;
        this.taskId = taskId;
        this.signalKey = signalKey;
        this.subscriptionId = subscriptionId;
        this.onCleanup = onCleanup;
    }
    get(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const signal = opts === null || opts === void 0 ? void 0 : opts.signal;
            return new Promise((resolve, reject) => {
                let cleanedUp = false;
                const cleanup = () => {
                    if (cleanedUp) {
                        return;
                    }
                    cleanedUp = true;
                    this.responseEmitter.removeListener('response', onResponse);
                    if (signal) {
                        signal.removeEventListener('abort', onAbort);
                    }
                    this.onCleanup();
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
exports.DurableEventStreamable = DurableEventStreamable;
class DurableEventGrpcPooledListener {
    constructor(client, onFinish) {
        this.requestEmitter = new events_1.EventEmitter();
        this.signal = new AbortController();
        this.subscribers = {};
        this.taskSignalKeyToSubscriptionIds = {};
        this.onFinish = () => { };
        this.subscriptionCounter = 0;
        this.currRequester = 0;
        this.DEFAULT_INTERRUPT_INTERVAL = 15 * 60 * 1000; // 15 minutes in milliseconds
        this.client = client;
        this.init();
        this.onFinish = onFinish;
        this.scheduleInterrupt();
    }
    scheduleInterrupt() {
        setTimeout(() => {
            if (this.signal) {
                this.signal.abort();
                this.init(0);
            }
            this.scheduleInterrupt();
        }, this.DEFAULT_INTERRUPT_INTERVAL);
    }
    init() {
        return __awaiter(this, arguments, void 0, function* (retries = 0) {
            var _a, e_1, _b, _c;
            let retryCount = retries;
            const MAX_RETRY_INTERVAL = 5000; // 5 seconds in milliseconds
            const BASE_RETRY_INTERVAL = 100; // 0.1 seconds in milliseconds
            const MAX_RETRY_COUNT = 5;
            if (retries > 0) {
                const backoffTime = Math.min(BASE_RETRY_INTERVAL * 2 ** (retries - 1), MAX_RETRY_INTERVAL);
                this.client.logger.info(`Retrying in ... ${backoffTime / 1000} seconds`);
                yield (0, sleep_1.default)(backoffTime);
            }
            if (retries > MAX_RETRY_COUNT) {
                this.client.logger.error('Max retry count exceeded for durable event listener');
                return;
            }
            try {
                this.client.logger.debug('Initializing durable-event-listener');
                this.signal = new AbortController();
                this.currRequester++;
                this.listener = this.client.client.listenForDurableEvent(this.request(), {
                    signal: this.signal.signal,
                });
                if (retries > 0) {
                    setTimeout(() => this.replayRequests(), 100);
                }
                try {
                    for (var _d = true, _e = __asyncValues(this.listener), _f; _f = yield _e.next(), _a = _f.done, !_a; _d = true) {
                        _c = _f.value;
                        _d = false;
                        const event = _c;
                        retryCount = 0;
                        const subscriptionKey = keyHelper(event.taskId, event.signalKey);
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
                catch (e_1_1) { e_1 = { error: e_1_1 }; }
                finally {
                    try {
                        if (!_d && !_a && (_b = _e.return)) yield _b.call(_e);
                    }
                    finally { if (e_1) throw e_1.error; }
                }
                this.client.logger.debug('Durable event listener finished');
            }
            catch (e) {
                if ((0, abort_controller_x_1.isAbortError)(e)) {
                    this.client.logger.debug('Durable event listener aborted');
                    return;
                }
                this.client.logger.error(`Error in durable-event-listener: ${(0, hatchet_error_1.getErrorMessage)(e)}`);
            }
            finally {
                const subscriberCount = Object.keys(this.subscribers).length;
                if (subscriberCount > 0) {
                    this.client.logger.debug(`Durable event listener loop exited with ${subscriberCount} subscribers`);
                    this.client.logger.debug(`Restarting durable event listener retry ${retryCount + 1}`);
                    this.init(retryCount + 1);
                }
            }
        });
    }
    cleanupSubscription(subscriptionId) {
        const emitter = this.subscribers[subscriptionId];
        if (!emitter) {
            return;
        }
        const subscriptionKey = keyHelper(emitter.taskId, emitter.signalKey);
        delete this.subscribers[subscriptionId];
        // Remove from the mapping
        if (this.taskSignalKeyToSubscriptionIds[subscriptionKey]) {
            this.taskSignalKeyToSubscriptionIds[subscriptionKey] = this.taskSignalKeyToSubscriptionIds[subscriptionKey].filter((id) => id !== subscriptionId);
            if (this.taskSignalKeyToSubscriptionIds[subscriptionKey].length === 0) {
                delete this.taskSignalKeyToSubscriptionIds[subscriptionKey];
            }
        }
    }
    subscribe(request) {
        const { taskId, signalKey } = request;
        if (!this.listener) {
            throw new Error('listener not initialized');
        }
        const subscriptionId = (this.subscriptionCounter++).toString();
        const subscriber = new DurableEventStreamable(this.listener, taskId, signalKey, subscriptionId, () => this.cleanupSubscription(subscriptionId));
        this.subscribers[subscriptionId] = subscriber;
        const key = keyHelper(taskId, signalKey);
        if (!this.taskSignalKeyToSubscriptionIds[key]) {
            this.taskSignalKeyToSubscriptionIds[key] = [];
        }
        this.taskSignalKeyToSubscriptionIds[key].push(subscriptionId);
        this.requestEmitter.emit('subscribe', { taskId, signalKey });
        return subscriber;
    }
    result(request, opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const subscriber = this.subscribe(request);
            const event = yield subscriber.get({ signal: opts === null || opts === void 0 ? void 0 : opts.signal });
            return event;
        });
    }
    registerDurableEvent(request) {
        return __awaiter(this, void 0, void 0, function* () {
            const conditions = {
                sleepConditions: request.sleepConditions,
                userEventConditions: request.userEventConditions,
            };
            const registerRequest = {
                taskId: request.taskId,
                signalKey: request.signalKey,
                conditions,
            };
            return this.client.client.registerDurableEvent(registerRequest);
        });
    }
    replayRequests() {
        const subscriptionEntries = Object.entries(this.taskSignalKeyToSubscriptionIds);
        this.client.logger.debug(`Replaying ${subscriptionEntries.length} requests...`);
        for (const [key] of subscriptionEntries) {
            const [taskId, signalKey] = key.split('|');
            this.requestEmitter.emit('subscribe', { taskId, signalKey });
        }
    }
    request() {
        return __asyncGenerator(this, arguments, function* request_1() {
            var _a, e_2, _b, _c;
            const { currRequester } = this;
            // Replay existing subscriptions
            const existingSubscriptions = new Set();
            for (const key in this.taskSignalKeyToSubscriptionIds) {
                if (this.taskSignalKeyToSubscriptionIds[key].length > 0) {
                    const [taskId, signalKey] = key.split('|');
                    existingSubscriptions.add(key);
                    yield yield __await({ taskId, signalKey });
                }
            }
            try {
                for (var _d = true, _e = __asyncValues((0, events_1.on)(this.requestEmitter, 'subscribe')), _f; _f = yield __await(_e.next()), _a = _f.done, !_a; _d = true) {
                    _c = _f.value;
                    _d = false;
                    const e = _c;
                    // Stop if this requester is outdated
                    if (currRequester !== this.currRequester) {
                        break;
                    }
                    const request = e[0];
                    const key = keyHelper(request.taskId, request.signalKey);
                    // Only send unique subscriptions
                    if (!existingSubscriptions.has(key)) {
                        existingSubscriptions.add(key);
                        yield yield __await(request);
                    }
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (!_d && !_a && (_b = _e.return)) yield __await(_b.call(_e));
                }
                finally { if (e_2) throw e_2.error; }
            }
        });
    }
}
exports.DurableEventGrpcPooledListener = DurableEventGrpcPooledListener;
const keyHelper = (taskId, signalKey) => `${taskId}|${signalKey}`;
