"use strict";
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncDelegator = (this && this.__asyncDelegator) || function (o) {
    var i, p;
    return i = {}, verb("next"), verb("throw", function (e) { throw e; }), verb("return"), i[Symbol.iterator] = function () { return this; }, i;
    function verb(n, f) { i[n] = o[n] ? function (v) { return (p = !p) ? { value: __await(o[n](v)), done: false } : f ? f(v) : v; } : f; }
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.createGrpcClient = exports.addTokenMiddleware = exports.channelFactory = void 0;
const nice_grpc_1 = require("nice-grpc");
const nice_grpc_common_1 = require("nice-grpc-common");
const config_loader_1 = require("./config-loader");
const channelFactory = (config, credentials) => {
    var _a, _b;
    return (0, nice_grpc_1.createChannel)(config.host_port, credentials, {
        'grpc.ssl_target_name_override': config.tls_config.server_name,
        'grpc.keepalive_timeout_ms': 60 * 1000,
        'grpc.client_idle_timeout_ms': 60 * 1000,
        // Send keepalive pings every 10 seconds, default is 2 hours.
        'grpc.keepalive_time_ms': 10 * 1000,
        // Allow keepalive pings when there are no gRPC calls.
        'grpc.keepalive_permit_without_calls': 1,
        // Enable gzip compression for all calls on this channel
        'grpc.default_compression_algorithm': 2, // 2 = Gzip compression
        'grpc.max_send_message_length': (_a = config.grpc_max_send_message_length) !== null && _a !== void 0 ? _a : 4 * 1024 * 1024,
        'grpc.max_receive_message_length': (_b = config.grpc_max_recv_message_length) !== null && _b !== void 0 ? _b : 4 * 1024 * 1024,
    });
};
exports.channelFactory = channelFactory;
const addTokenMiddleware = (token) => function _(call, options) {
    return __asyncGenerator(this, arguments, function* _1() {
        var _a, e_1, _b, _c;
        const optionsWithAuth = Object.assign(Object.assign({}, options), { metadata: new nice_grpc_common_1.Metadata({ authorization: `bearer ${token}` }) });
        if (!call.responseStream) {
            const response = yield __await(yield* __asyncDelegator(__asyncValues(call.next(call.request, optionsWithAuth))));
            return yield __await(response);
        }
        try {
            for (var _d = true, _e = __asyncValues(call.next(call.request, optionsWithAuth)), _f; _f = yield __await(_e.next()), _a = _f.done, !_a; _d = true) {
                _c = _f.value;
                _d = false;
                const response = _c;
                yield yield __await(response);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (!_d && !_a && (_b = _e.return)) yield __await(_b.call(_e));
            }
            finally { if (e_1) throw e_1.error; }
        }
        return yield __await(undefined);
    });
};
exports.addTokenMiddleware = addTokenMiddleware;
const createGrpcClient = (config, serviceDefinition) => {
    const credentials = config_loader_1.ConfigLoader.createCredentials(config.tls_config);
    const clientFactory = (0, nice_grpc_1.createClientFactory)().use((0, exports.addTokenMiddleware)(config.token));
    const channel = (0, exports.channelFactory)(config, credentials);
    return {
        factory: clientFactory,
        channel,
        client: clientFactory.create(serviceDefinition, channel),
    };
};
exports.createGrpcClient = createGrpcClient;
