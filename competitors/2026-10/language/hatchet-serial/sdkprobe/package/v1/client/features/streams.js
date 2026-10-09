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
exports.StreamsClient = void 0;
const crypto_1 = require("crypto");
const axios_1 = require("axios");
const nice_grpc_1 = require("nice-grpc");
const grpc_helpers_1 = require("../../../util/grpc-helpers");
const grpc_error_1 = require("../../../util/grpc-error");
const sleep_1 = __importDefault(require("../../../util/sleep"));
const streams_1 = require("../../../protoc/v1/streams");
// the server stored nothing, so the seq is unused
function publishRejectedBeforeStoring(err) {
    const code = (0, grpc_error_1.getGrpcErrorCode)(err);
    return (code === nice_grpc_1.Status.INVALID_ARGUMENT ||
        code === nice_grpc_1.Status.RESOURCE_EXHAUSTED ||
        code === nice_grpc_1.Status.UNAUTHENTICATED ||
        code === nice_grpc_1.Status.PERMISSION_DENIED);
}
// retrying a subscribe that failed with these can't succeed
const permanentSubscribeErrors = new Set([
    nice_grpc_1.Status.INVALID_ARGUMENT,
    nice_grpc_1.Status.PERMISSION_DENIED,
    nice_grpc_1.Status.UNAUTHENTICATED,
    nice_grpc_1.Status.OUT_OF_RANGE,
    nice_grpc_1.Status.FAILED_PRECONDITION,
    nice_grpc_1.Status.NOT_FOUND,
]);
const resubscribeMinDelayMs = 250;
const resubscribeMaxDelayMs = 10000;
// the server's limit for a payload sent over gRPC (MaxStreamMessagePayloadBytes)
const maxInlinePayloadBytes = 4 * 1024 * 1024 - 5 * 1024;
// the server's limit for an uploaded payload (MaxStreamUploadedPayloadBytes)
const maxUploadedPayloadBytes = 64 * 1024 * 1024;
const payloadRequestAttempts = 3;
function httpStatus(err) {
    var _a;
    return (0, axios_1.isAxiosError)(err) ? (_a = err.response) === null || _a === void 0 ? void 0 : _a.status : undefined;
}
// a payload fetch that got no response, a 5xx or a 429 may succeed later
function isTemporaryHttpError(err) {
    if (!(0, axios_1.isAxiosError)(err)) {
        return false;
    }
    const status = httpStatus(err);
    return status === undefined || status >= 500 || status === 429;
}
function shouldResubscribe(err, attempt) {
    if (isTemporaryHttpError(err)) {
        return true;
    }
    const code = (0, grpc_error_1.getGrpcErrorCode)(err);
    if (code === undefined || permanentSubscribeErrors.has(code)) {
        return false;
    }
    // proxies answer 404 while an engine restarts, but on a first attempt it means no streams support
    if (code === nice_grpc_1.Status.UNIMPLEMENTED) {
        return attempt > 0;
    }
    return true;
}
/**
 * StreamsClient publishes to and reads from durable topics. Unlike
 * `runs.subscribeToStream`, topics are stored, independent of any run, and
 * readers can resume from a cursor.
 */
class StreamsClient {
    constructor(client) {
        // per (namespace, topic); seq only advances after a publish succeeds
        this.producers = new Map();
        // serializes publishes per (namespace, topic) so two never share a seq; the
        // stored promise always resolves, so one failure doesn't block later calls
        this.publishChains = new Map();
        this._config = client.config;
        this._client = client;
    }
    get grpc() {
        if (!this._grpc) {
            const { client } = (0, grpc_helpers_1.createGrpcClient)(this._config, streams_1.V1StreamsDefinition);
            this._grpc = client;
        }
        return this._grpc;
    }
    // retries a 5xx or network failure; a 4xx won't succeed on retry
    static withRetries(request) {
        return __awaiter(this, void 0, void 0, function* () {
            for (let attempt = 1;; attempt += 1) {
                try {
                    return yield request();
                }
                catch (err) {
                    const status = httpStatus(err);
                    if ((status !== undefined && status < 500) || attempt >= payloadRequestAttempts) {
                        throw err;
                    }
                }
            }
        });
    }
    uploadPayload(payload) {
        return __awaiter(this, void 0, void 0, function* () {
            const res = yield StreamsClient.withRetries(() => this._client.api.v1StreamPayloadUpload(this._config.tenant_id, {
                payload: new Blob([payload]),
            }));
            return res.data.ref;
        });
    }
    fetchPayload(payloadRef, signal) {
        return __awaiter(this, void 0, void 0, function* () {
            const res = yield StreamsClient.withRetries(() => this._client.api.v1StreamPayloadGet(this._config.tenant_id, { ref: payloadRef }, { format: 'arraybuffer', signal }));
            return new Uint8Array(res.data);
        });
    }
    publishOrdered(key_1, namespace_1, topic_1, body_1) {
        return __awaiter(this, arguments, void 0, function* (key, namespace, topic, body, retryGap = true) {
            let producer = this.producers.get(key);
            if (!producer) {
                producer = { producerId: (0, crypto_1.randomUUID)(), seq: 0 };
                this.producers.set(key, producer);
            }
            try {
                yield this.grpc.publish(Object.assign(Object.assign({ namespace,
                    topic }, body), { producerId: producer.producerId, producerSeq: producer.seq }));
            }
            catch (err) {
                // it may still land, and a different payload under its seq would be dropped as a duplicate
                if (!publishRejectedBeforeStoring(err)) {
                    this.producers.set(key, { producerId: (0, crypto_1.randomUUID)(), seq: 0 });
                }
                // a gap stored nothing (e.g. the watermark passed cursor retention), so resend as the new producer
                if (retryGap && (0, grpc_error_1.getGrpcErrorCode)(err) === nice_grpc_1.Status.FAILED_PRECONDITION) {
                    return this.publishOrdered(key, namespace, topic, body, false);
                }
                throw err;
            }
            producer.seq += 1;
        });
    }
    /**
     * Resolves once the message is stored. A client's messages to a topic are
     * delivered in the order publish() was called.
     * @param topic - the topic to publish to
     * @param message - the message payload
     * @param options - optional namespace override
     */
    publish(topic, message, options) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            const payload = typeof message === 'string' ? new TextEncoder().encode(message) : message;
            const namespace = (_a = options === null || options === void 0 ? void 0 : options.namespace) !== null && _a !== void 0 ? _a : '';
            const key = `${namespace}\u0000${topic}`;
            if (payload.byteLength > maxUploadedPayloadBytes) {
                throw new Error(`payload exceeds maximum size of ${maxUploadedPayloadBytes} bytes`);
            }
            // uploaded before taking its place in the topic's order, so uploads overlap
            const uploaded = payload.byteLength > maxInlinePayloadBytes ? this.uploadPayload(payload) : undefined;
            // awaited in order below; this only keeps an early failure from reporting as unhandled
            uploaded === null || uploaded === void 0 ? void 0 : uploaded.catch(() => { });
            const previous = (_b = this.publishChains.get(key)) !== null && _b !== void 0 ? _b : Promise.resolve();
            const publishPromise = previous.then(() => __awaiter(this, void 0, void 0, function* () {
                return this.publishOrdered(key, namespace, topic, uploaded ? { payloadRef: yield uploaded } : { payload });
            }));
            this.publishChains.set(key, publishPromise.catch(() => { }));
            return publishPromise;
        });
    }
    /**
     * Describes topic's retained messages. Rejects with a NOT_FOUND gRPC error
     * for a topic nothing was published to.
     * @param topic - the topic to describe
     * @param options - optional namespace override
     */
    topicMetadata(topic, options) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            const res = yield this.grpc.getTopicMetadata({ namespace: (_a = options === null || options === void 0 ? void 0 : options.namespace) !== null && _a !== void 0 ? _a : '', topic });
            return {
                namespace: res.namespace,
                topic: res.topic,
                tenantId: res.tenantId,
                messageCount: res.messageCount,
                latestCursor: res.latestCursor,
                lastPublishedAt: res.lastPublishedAt,
            };
        });
    }
    /**
     * Yields topic's messages after options.cursor, or from the oldest retained.
     * If the connection drops, it resubscribes from the last message yielded, so
     * nothing is skipped or repeated. Ends when the server hangs up an idle
     * subscription, options.signal aborts, or the caller stops iterating; throws
     * on errors a retry can't fix, such as an expired cursor.
     * @param topic - the topic to read from
     * @param options - optional namespace override, resume cursor, and abort signal
     */
    events(topic, options) {
        return __asyncGenerator(this, arguments, function* events_1() {
            var _a, e_1, _b, _c;
            var _d;
            const namespace = (_d = options === null || options === void 0 ? void 0 : options.namespace) !== null && _d !== void 0 ? _d : '';
            const signal = options === null || options === void 0 ? void 0 : options.signal;
            let { cursor } = options !== null && options !== void 0 ? options : {};
            let failures = 0;
            for (let attempt = 0;; attempt += 1) {
                try {
                    const stream = this.grpc.subscribe({ namespace, topic, cursor }, { signal });
                    try {
                        for (var _e = true, stream_1 = (e_1 = void 0, __asyncValues(stream)), stream_1_1; stream_1_1 = yield __await(stream_1.next()), _a = stream_1_1.done, !_a; _e = true) {
                            _c = stream_1_1.value;
                            _e = false;
                            const msg = _c;
                            if (msg.hangup) {
                                return yield __await(void 0);
                            }
                            for (const entry of msg.entries) {
                                const payload = entry.payloadRef
                                    ? yield __await(this.fetchPayload(entry.payloadRef, signal))
                                    : entry.payload;
                                ({ cursor } = entry);
                                // reset only on delivery, so a payload fetch failing on every reconnect still backs off
                                failures = 0;
                                yield yield __await({
                                    payload,
                                    cursor: entry.cursor,
                                    createdAt: entry.createdAt,
                                });
                            }
                        }
                    }
                    catch (e_1_1) { e_1 = { error: e_1_1 }; }
                    finally {
                        try {
                            if (!_e && !_a && (_b = stream_1.return)) yield __await(_b.call(stream_1));
                        }
                        finally { if (e_1) throw e_1.error; }
                    }
                    // ending without a hangup means the engine went away, e.g. it is shutting down
                }
                catch (err) {
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        return yield __await(void 0);
                    }
                    if (!shouldResubscribe(err, attempt)) {
                        throw err;
                    }
                }
                const delayMs = Math.min(resubscribeMinDelayMs * 2 ** failures, resubscribeMaxDelayMs);
                failures += 1;
                try {
                    yield __await((0, sleep_1.default)(delayMs * (0.5 + Math.random() / 2), signal));
                }
                catch (_f) {
                    return yield __await(void 0);
                }
            }
        });
    }
}
exports.StreamsClient = StreamsClient;
