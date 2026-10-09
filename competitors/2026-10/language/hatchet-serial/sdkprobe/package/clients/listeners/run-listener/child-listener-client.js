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
exports.RunListenerClient = exports.RunEventListener = exports.RunEventType = void 0;
const nice_grpc_1 = require("nice-grpc");
const grpc_error_1 = require("../../../util/grpc-error");
const events_1 = require("events");
const dispatcher_1 = require("../../../protoc/dispatcher");
const hatchet_error_1 = __importDefault(require("../../../util/errors/hatchet-error"));
const sleep_1 = __importDefault(require("../../../util/sleep"));
const pooled_child_listener_client_1 = require("./pooled-child-listener-client");
const DEFAULT_EVENT_LISTENER_RETRY_INTERVAL = 5; // seconds
const DEFAULT_EVENT_LISTENER_RETRY_COUNT = 5;
var RunEventType;
(function (RunEventType) {
    RunEventType["STEP_RUN_EVENT_TYPE_STARTED"] = "STEP_RUN_EVENT_TYPE_STARTED";
    RunEventType["STEP_RUN_EVENT_TYPE_COMPLETED"] = "STEP_RUN_EVENT_TYPE_COMPLETED";
    RunEventType["STEP_RUN_EVENT_TYPE_FAILED"] = "STEP_RUN_EVENT_TYPE_FAILED";
    RunEventType["STEP_RUN_EVENT_TYPE_CANCELLED"] = "STEP_RUN_EVENT_TYPE_CANCELLED";
    RunEventType["STEP_RUN_EVENT_TYPE_TIMED_OUT"] = "STEP_RUN_EVENT_TYPE_TIMED_OUT";
    RunEventType["STEP_RUN_EVENT_TYPE_STREAM"] = "STEP_RUN_EVENT_TYPE_STREAM";
    RunEventType["WORKFLOW_RUN_EVENT_TYPE_STARTED"] = "WORKFLOW_RUN_EVENT_TYPE_STARTED";
    RunEventType["WORKFLOW_RUN_EVENT_TYPE_COMPLETED"] = "WORKFLOW_RUN_EVENT_TYPE_COMPLETED";
    RunEventType["WORKFLOW_RUN_EVENT_TYPE_FAILED"] = "WORKFLOW_RUN_EVENT_TYPE_FAILED";
    RunEventType["WORKFLOW_RUN_EVENT_TYPE_CANCELLED"] = "WORKFLOW_RUN_EVENT_TYPE_CANCELLED";
    RunEventType["WORKFLOW_RUN_EVENT_TYPE_TIMED_OUT"] = "WORKFLOW_RUN_EVENT_TYPE_TIMED_OUT";
})(RunEventType || (exports.RunEventType = RunEventType = {}));
const stepEventTypeMap = {
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_STARTED]: RunEventType.STEP_RUN_EVENT_TYPE_STARTED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_COMPLETED]: RunEventType.STEP_RUN_EVENT_TYPE_COMPLETED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_FAILED]: RunEventType.STEP_RUN_EVENT_TYPE_FAILED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_CANCELLED]: RunEventType.STEP_RUN_EVENT_TYPE_CANCELLED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_TIMED_OUT]: RunEventType.STEP_RUN_EVENT_TYPE_TIMED_OUT,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_STREAM]: RunEventType.STEP_RUN_EVENT_TYPE_STREAM,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_UNKNOWN]: undefined,
    [dispatcher_1.ResourceEventType.UNRECOGNIZED]: undefined,
};
const workflowEventTypeMap = {
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_STARTED]: RunEventType.WORKFLOW_RUN_EVENT_TYPE_STARTED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_COMPLETED]: RunEventType.WORKFLOW_RUN_EVENT_TYPE_COMPLETED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_FAILED]: RunEventType.WORKFLOW_RUN_EVENT_TYPE_FAILED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_CANCELLED]: RunEventType.WORKFLOW_RUN_EVENT_TYPE_CANCELLED,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_TIMED_OUT]: RunEventType.WORKFLOW_RUN_EVENT_TYPE_TIMED_OUT,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_UNKNOWN]: undefined,
    [dispatcher_1.ResourceEventType.RESOURCE_EVENT_TYPE_STREAM]: undefined,
    [dispatcher_1.ResourceEventType.UNRECOGNIZED]: undefined,
};
const resourceTypeMap = {
    [dispatcher_1.ResourceType.RESOURCE_TYPE_STEP_RUN]: stepEventTypeMap,
    [dispatcher_1.ResourceType.RESOURCE_TYPE_WORKFLOW_RUN]: workflowEventTypeMap,
    [dispatcher_1.ResourceType.RESOURCE_TYPE_UNKNOWN]: undefined,
    [dispatcher_1.ResourceType.UNRECOGNIZED]: undefined,
};
class RunEventListener {
    constructor(client) {
        this.q = [];
        this.eventEmitter = new events_1.EventEmitter();
        this.client = client;
    }
    static forRunId(workflowRunId, client) {
        const listener = new RunEventListener(client);
        listener.listenForRunId(workflowRunId);
        return listener;
    }
    static forAdditionalMeta(key, value, client) {
        const listener = new RunEventListener(client);
        listener.listenForAdditionalMeta(key, value);
        return listener;
    }
    emit(event) {
        this.q.push(event);
        this.eventEmitter.emit('event');
    }
    listenForRunId(workflowRunId) {
        return __awaiter(this, void 0, void 0, function* () {
            const listenerFactory = () => this.client.subscribeToWorkflowEvents({
                workflowRunId,
            });
            return this.listenLoop(listenerFactory);
        });
    }
    listenForAdditionalMeta(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            const listenerFactory = () => this.client.subscribeToWorkflowEvents({
                additionalMetaKey: key,
                additionalMetaValue: value,
            });
            return this.listenLoop(listenerFactory);
        });
    }
    listenLoop(listenerFactory) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, e_1, _b, _c;
            var _d;
            let listener = listenerFactory();
            while (true) {
                try {
                    try {
                        for (var _e = true, listener_1 = (e_1 = void 0, __asyncValues(listener)), listener_1_1; listener_1_1 = yield listener_1.next(), _a = listener_1_1.done, !_a; _e = true) {
                            _c = listener_1_1.value;
                            _e = false;
                            const workflowEvent = _c;
                            const eventType = (_d = resourceTypeMap[workflowEvent.resourceType]) === null || _d === void 0 ? void 0 : _d[workflowEvent.eventType];
                            if (eventType) {
                                this.emit({
                                    type: eventType,
                                    payload: workflowEvent.eventPayload,
                                    resourceId: workflowEvent.resourceId,
                                    workflowRunId: workflowEvent.workflowRunId,
                                });
                            }
                        }
                    }
                    catch (e_1_1) { e_1 = { error: e_1_1 }; }
                    finally {
                        try {
                            if (!_e && !_a && (_b = listener_1.return)) yield _b.call(listener_1);
                        }
                        finally { if (e_1) throw e_1.error; }
                    }
                    this.eventEmitter.emit('complete');
                    return;
                }
                catch (e) {
                    if ((0, grpc_error_1.getGrpcErrorCode)(e) === nice_grpc_1.Status.CANCELLED) {
                        this.eventEmitter.emit('complete');
                        return;
                    }
                    if ((0, grpc_error_1.getGrpcErrorCode)(e) === nice_grpc_1.Status.UNAVAILABLE) {
                        listener = yield this.retrySubscribe(listenerFactory);
                    }
                    else {
                        throw e;
                    }
                }
            }
        });
    }
    retrySubscribe(listenerFactory) {
        return __awaiter(this, void 0, void 0, function* () {
            let retries = 0;
            while (retries < DEFAULT_EVENT_LISTENER_RETRY_COUNT) {
                try {
                    yield (0, sleep_1.default)(DEFAULT_EVENT_LISTENER_RETRY_INTERVAL);
                    return listenerFactory();
                }
                catch (_a) {
                    retries += 1;
                }
            }
            throw new hatchet_error_1.default(`Could not subscribe to the worker after ${DEFAULT_EVENT_LISTENER_RETRY_COUNT} retries`);
        });
    }
    stream() {
        return __asyncGenerator(this, arguments, function* stream_1() {
            var _a, e_2, _b, _c;
            let completed = false;
            this.eventEmitter.once('complete', () => {
                completed = true;
                this.eventEmitter.emit('event');
            });
            try {
                for (var _d = true, _e = __asyncValues((0, events_1.on)(this.eventEmitter, 'event')), _f; _f = yield __await(_e.next()), _a = _f.done, !_a; _d = true) {
                    _c = _f.value;
                    _d = false;
                    const _ = _c;
                    while (this.q.length > 0) {
                        const r = this.q.shift();
                        if (r) {
                            yield yield __await(r);
                        }
                    }
                    if (completed && this.q.length === 0) {
                        break;
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
exports.RunEventListener = RunEventListener;
class RunListenerClient {
    constructor(config, channel, factory, api) {
        this.config = config;
        this.client = factory.create(dispatcher_1.DispatcherDefinition, channel);
        this.logger = config.logger(`Listener`, config.log_level);
        this.api = api;
    }
    get(workflowRunId) {
        if (!this.pooledListener) {
            this.pooledListener = new pooled_child_listener_client_1.RunGrpcPooledListener(this, () => {
                this.pooledListener = undefined;
            });
        }
        return this.pooledListener.subscribe({
            workflowRunId,
        });
    }
    stream(workflowRunId) {
        return __awaiter(this, void 0, void 0, function* () {
            const listener = RunEventListener.forRunId(workflowRunId, this.client);
            return listener.stream();
        });
    }
    streamByRunId(workflowRunId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.stream(workflowRunId);
        });
    }
    streamByAdditionalMeta(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            const listener = RunEventListener.forAdditionalMeta(key, value, this.client);
            return listener.stream();
        });
    }
}
exports.RunListenerClient = RunListenerClient;
