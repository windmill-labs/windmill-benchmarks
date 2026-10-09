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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RunGrpcPooledListener = exports.Streamable = void 0;
const events_1 = require("events");
const dispatcher_1 = require("../../../protoc/dispatcher");
const abort_controller_x_1 = require("abort-controller-x");
const hatchet_error_1 = require("../../../util/errors/hatchet-error");
const sleep_1 = __importDefault(require("../../../util/sleep"));
const abort_error_1 = require("../../../util/abort-error");
const abort_signal_1 = require("../../../util/abort-signal");
class Streamable {
    constructor(listener, id, onCleanup) {
        this.cleanedUp = false;
        this.responseEmitter = new events_1.EventEmitter();
        this.listener = listener;
        this.id = id;
        this.onCleanup = onCleanup;
    }
    cleanupOnce() {
        if (this.cleanedUp) {
            return;
        }
        this.cleanedUp = true;
        this.onCleanup();
    }
    get(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const signal = opts === null || opts === void 0 ? void 0 : opts.signal;
            return new Promise((resolve, reject) => {
                const cleanupListeners = () => {
                    this.responseEmitter.removeListener('response', onResponse);
                    if (signal) {
                        signal.removeEventListener('abort', onAbort);
                    }
                };
                const onResponse = (event) => {
                    cleanupListeners();
                    resolve(event);
                };
                const onAbort = () => {
                    cleanupListeners();
                    this.cleanupOnce();
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
    stream(opts) {
        return __asyncGenerator(this, arguments, function* stream_1() {
            while (true) {
                const event = yield __await(this.get(opts));
                yield yield __await(event);
                if (event.eventType === dispatcher_1.WorkflowRunEventType.WORKFLOW_RUN_EVENT_TYPE_FINISHED) {
                    this.cleanupOnce();
                    break;
                }
            }
        });
    }
}
exports.Streamable = Streamable;
class RunGrpcPooledListener {
    constructor(client, onFinish) {
        this.requestEmitter = new events_1.EventEmitter();
        this.signal = new AbortController();
        this.subscribers = {};
        this.onFinish = () => { };
        this.client = client;
        this.init();
        this.onFinish = onFinish;
    }
    init() {
        return __awaiter(this, arguments, void 0, function* (retries = 0) {
            var _a, e_1, _b, _c;
            let retryCount = retries;
            const MAX_RETRY_INTERVAL = 5000; // 5 seconds in milliseconds
            const BASE_RETRY_INTERVAL = 100; // 0.1 seconds in milliseconds
            if (retries > 0) {
                const backoffTime = Math.min(BASE_RETRY_INTERVAL * 2 ** (retries - 1), MAX_RETRY_INTERVAL);
                this.client.logger.info(`Retrying in ... ${backoffTime / 1000} seconds`);
                yield (0, sleep_1.default)(backoffTime);
            }
            try {
                this.client.logger.debug('Initializing child-listener');
                this.signal = new AbortController();
                this.listener = this.client.client.subscribeToWorkflowRuns(this.request(), {
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
                        const emitter = this.subscribers[event.workflowRunId];
                        if (emitter) {
                            emitter.responseEmitter.emit('response', event);
                            if (event.eventType === dispatcher_1.WorkflowRunEventType.WORKFLOW_RUN_EVENT_TYPE_FINISHED) {
                                delete this.subscribers[event.workflowRunId];
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
                this.client.logger.debug('Child listener finished');
            }
            catch (e) {
                if ((0, abort_controller_x_1.isAbortError)(e)) {
                    this.client.logger.debug('Child Listener aborted');
                    return;
                }
                this.client.logger.error(`Error in child-listener: ${(0, hatchet_error_1.getErrorMessage)(e)}`);
            }
            finally {
                // it is possible the server hangs up early,
                // restart the listener if we still have subscribers
                this.client.logger.debug(`Child listener loop exited with ${Object.keys(this.subscribers).length} subscribers`);
                this.client.logger.debug(`Restarting child listener retry ${retryCount + 1}`);
                this.init(retryCount + 1);
            }
        });
    }
    subscribe(request) {
        if (!this.listener) {
            throw new Error('listener not initialized');
        }
        this.subscribers[request.workflowRunId] = new Streamable(this.listener, request.workflowRunId, () => {
            delete this.subscribers[request.workflowRunId];
        });
        this.requestEmitter.emit('subscribe', request);
        return this.subscribers[request.workflowRunId];
    }
    replayRequests() {
        const subs = Object.values(this.subscribers);
        this.client.logger.debug(`Replaying ${subs.length} requests...`);
        for (const subscriber of subs) {
            this.requestEmitter.emit('subscribe', { workflowRunId: subscriber.id });
        }
    }
    request() {
        return __asyncGenerator(this, arguments, function* request_1() {
            var _a, e_2, _b, _c;
            try {
                for (var _d = true, _e = __asyncValues((0, events_1.on)(this.requestEmitter, 'subscribe')), _f; _f = yield __await(_e.next()), _a = _f.done, !_a; _d = true) {
                    _c = _f.value;
                    _d = false;
                    const e = _c;
                    yield yield __await(e[0]);
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
exports.RunGrpcPooledListener = RunGrpcPooledListener;
