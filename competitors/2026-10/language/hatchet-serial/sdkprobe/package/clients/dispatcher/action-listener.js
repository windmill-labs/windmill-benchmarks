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
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
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
exports.ActionListener = exports.workflowNameFromAction = exports.createActionId = exports.createAction = void 0;
const nice_grpc_1 = require("nice-grpc");
const grpc_error_1 = require("../../util/grpc-error");
const abort_controller_x_1 = require("abort-controller-x");
const sleep_1 = __importDefault(require("../../util/sleep"));
const hatchet_error_1 = __importStar(require("../../util/errors/hatchet-error"));
const heartbeat_controller_1 = require("./heartbeat/heartbeat-controller");
const action_1 = require("./action");
const listener_severity_1 = require("./listener-severity");
const DEFAULT_ACTION_LISTENER_RETRY_INTERVAL = 5000; // milliseconds
const DEFAULT_ACTION_LISTENER_RETRY_COUNT = 20;
var ListenStrategy;
(function (ListenStrategy) {
    ListenStrategy[ListenStrategy["LISTEN_STRATEGY_V1"] = 1] = "LISTEN_STRATEGY_V1";
    ListenStrategy[ListenStrategy["LISTEN_STRATEGY_V2"] = 2] = "LISTEN_STRATEGY_V2";
})(ListenStrategy || (ListenStrategy = {}));
var action_2 = require("./action");
Object.defineProperty(exports, "createAction", { enumerable: true, get: function () { return action_2.createAction; } });
Object.defineProperty(exports, "createActionId", { enumerable: true, get: function () { return action_2.createActionId; } });
Object.defineProperty(exports, "workflowNameFromAction", { enumerable: true, get: function () { return action_2.workflowNameFromAction; } });
class ActionListener {
    constructor(client, workerId, retryInterval = DEFAULT_ACTION_LISTENER_RETRY_INTERVAL, retryCount = DEFAULT_ACTION_LISTENER_RETRY_COUNT) {
        this.lastConnectionAttempt = 0;
        this.retries = 0;
        this.retryInterval = DEFAULT_ACTION_LISTENER_RETRY_INTERVAL;
        this.retryCount = DEFAULT_ACTION_LISTENER_RETRY_COUNT;
        this.done = false;
        this.listenStrategy = ListenStrategy.LISTEN_STRATEGY_V2;
        this.actions = () => (function gen(client) {
            return __asyncGenerator(this, arguments, function* gen_1() {
                var _a, e_1, _b, _c;
                while (true) {
                    if (client.done) {
                        break;
                    }
                    try {
                        const listenClient = yield __await(client.getListenClient());
                        try {
                            for (var _d = true, listenClient_1 = (e_1 = void 0, __asyncValues(listenClient)), listenClient_1_1; listenClient_1_1 = yield __await(listenClient_1.next()), _a = listenClient_1_1.done, !_a; _d = true) {
                                _c = listenClient_1_1.value;
                                _d = false;
                                const assignedAction = _c;
                                yield yield __await((0, action_1.createAction)(assignedAction));
                            }
                        }
                        catch (e_1_1) { e_1 = { error: e_1_1 }; }
                        finally {
                            try {
                                if (!_d && !_a && (_b = listenClient_1.return)) yield __await(_b.call(listenClient_1));
                            }
                            finally { if (e_1) throw e_1.error; }
                        }
                    }
                    catch (e) {
                        // If the stream was aborted (e.g., during worker shutdown), exit gracefully
                        if ((0, abort_controller_x_1.isAbortError)(e)) {
                            client.logger.info('Listener aborted, exiting generator');
                            break;
                        }
                        client.logger.info('Listener error');
                        // if this is a HatchetError, we should throw this error
                        if (e instanceof hatchet_error_1.default) {
                            throw e;
                        }
                        if ((yield __await(client.getListenStrategy())) === ListenStrategy.LISTEN_STRATEGY_V2 &&
                            (0, grpc_error_1.getGrpcErrorCode)(e) === nice_grpc_1.Status.UNIMPLEMENTED) {
                            client.setListenStrategy(ListenStrategy.LISTEN_STRATEGY_V1);
                        }
                        client.incrementRetries();
                        const message = `Listener encountered an error: ${(0, hatchet_error_1.getErrorMessage)(e)}`;
                        const severity = (0, listener_severity_1.classifyListenerFailure)(e, client.retries);
                        if (severity !== 'silent') {
                            client.logger[severity](message);
                        }
                        if (client.retries > 1) {
                            client.logger.info(`Retrying in ${client.retryInterval}ms...`);
                            yield __await((0, sleep_1.default)(client.retryInterval));
                        }
                        else {
                            client.logger.info(`Retrying`);
                        }
                    }
                }
            });
        })(this);
        this.config = client.config;
        this.client = client.client;
        this.workerId = workerId;
        this.logger = client.config.logger(`ActionListener`, this.config.log_level);
        this.retryInterval = retryInterval;
        this.retryCount = retryCount;
        this.heartbeat = new heartbeat_controller_1.Heartbeat(client, workerId);
    }
    setListenStrategy(strategy) {
        return __awaiter(this, void 0, void 0, function* () {
            this.listenStrategy = strategy;
        });
    }
    getListenStrategy() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.listenStrategy;
        });
    }
    incrementRetries() {
        return __awaiter(this, void 0, void 0, function* () {
            this.retries += 1;
        });
    }
    getListenClient() {
        return __awaiter(this, void 0, void 0, function* () {
            const currentTime = Math.floor(Date.now());
            // attempt to account for the time it takes to establish the listener
            if (currentTime - this.lastConnectionAttempt > this.retryInterval * 4) {
                this.retries = 0;
            }
            this.lastConnectionAttempt = currentTime;
            if (this.retries > DEFAULT_ACTION_LISTENER_RETRY_COUNT) {
                throw new hatchet_error_1.default(`Could not subscribe to the worker after ${DEFAULT_ACTION_LISTENER_RETRY_COUNT} retries`);
            }
            this.logger.info(`Connecting to Hatchet to establish listener for actions... ${this.retries}/${DEFAULT_ACTION_LISTENER_RETRY_COUNT} (last attempt: ${this.lastConnectionAttempt})`);
            if (this.retries >= 1) {
                yield (0, sleep_1.default)(DEFAULT_ACTION_LISTENER_RETRY_INTERVAL);
            }
            try {
                // Create a new AbortController for this connection
                this.abortController = new AbortController();
                if (this.listenStrategy === ListenStrategy.LISTEN_STRATEGY_V1) {
                    const result = this.client.listen({
                        workerId: this.workerId,
                    }, {
                        signal: this.abortController.signal,
                    });
                    this.logger.green('Connection established using LISTEN_STRATEGY_V1');
                    return result;
                }
                const res = this.client.listenV2({
                    workerId: this.workerId,
                }, {
                    signal: this.abortController.signal,
                });
                yield this.heartbeat.start();
                this.logger.green('Connection established using LISTEN_STRATEGY_V2');
                return res;
            }
            catch (e) {
                this.retries += 1;
                const message = `Attempt ${this.retries}: Failed to connect, retrying...`;
                const severity = (0, listener_severity_1.classifyListenerFailure)(e, this.retries);
                if (severity !== 'silent') {
                    this.logger[severity](message);
                }
                if ((0, grpc_error_1.getGrpcErrorCode)(e) === nice_grpc_1.Status.UNAVAILABLE) {
                    // Connection lost, reset heartbeat interval and retry connection
                    this.heartbeat.stop();
                    return this.getListenClient();
                }
                throw e;
            }
        });
    }
    stopStream() {
        this.done = true;
        // Abort the gRPC stream to immediately cancel the generator
        if (this.abortController) {
            this.abortController.abort('Worker stopping');
        }
    }
    unregister() {
        return __awaiter(this, void 0, void 0, function* () {
            this.stopStream();
            this.heartbeat.stop();
            try {
                return yield this.client.unsubscribe({
                    workerId: this.workerId,
                });
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e, {
                    defaultMessage: 'Failed to unsubscribe',
                    prefix: 'Failed to unsubscribe: ',
                });
            }
        });
    }
}
exports.ActionListener = ActionListener;
