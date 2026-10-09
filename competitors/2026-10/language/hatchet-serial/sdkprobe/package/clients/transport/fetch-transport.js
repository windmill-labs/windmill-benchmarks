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
exports.resolveServerUrl = resolveServerUrl;
exports.createFetchTransport = createFetchTransport;
exports.limitMessageSizes = limitMessageSizes;
const connect_1 = require("@connectrpc/connect");
const connect_web_1 = require("@connectrpc/connect-web");
const grpc_target_1 = require("./grpc-target");
const transport_1 = require("./transport");
/**
 * Resolves the engine's base URL from a `serverUrl` or from `hostPort` plus the TLS
 * strategy, the two ways a fetch-based client can be pointed at an engine. `hostPort` takes
 * the gRPC target forms the Node client takes (`host:port`, `dns:///host:port`, `ipv4:`,
 * `ipv6:`), so a token or config shared with a Node client works unchanged. A `serverUrl` is
 * checked against the rules its documentation states, so a plaintext URL cannot slip past
 * the TLS default.
 */
function resolveServerUrl(options) {
    var _a, _b;
    const strategy = (_b = (_a = options.tls) === null || _a === void 0 ? void 0 : _a.strategy) !== null && _b !== void 0 ? _b : 'tls';
    if (options.serverUrl) {
        return validateServerUrl(options.serverUrl, strategy);
    }
    if (options.hostPort) {
        const scheme = strategy === 'none' ? 'http' : 'https';
        return (0, grpc_target_1.grpcTargetBaseUrl)((0, grpc_target_1.parseGrpcTarget)(options.hostPort), scheme);
    }
    throw new Error('a fetch transport needs a serverUrl or a hostPort');
}
/*
 * The messages name the field and the rule only: a URL can carry a password, and the token
 * is never part of it, so neither is echoed.
 */
function validateServerUrl(serverUrl, strategy) {
    let url;
    try {
        url = new URL(serverUrl);
    }
    catch (_a) {
        throw new Error('serverUrl must be an absolute http:// or https:// URL');
    }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
        throw new Error('serverUrl must be an absolute http:// or https:// URL');
    }
    if (url.username || url.password) {
        throw new Error('serverUrl must not carry a username or password; the client authenticates with its token');
    }
    if (serverUrl.includes('?') || serverUrl.includes('#')) {
        throw new Error('serverUrl must not carry a query string or fragment; the RPC path is appended to it');
    }
    if (url.protocol === 'http:' && strategy !== 'none') {
        throw new Error("serverUrl is http:// but tls.strategy is 'tls' (the default), which would send the token in plaintext; set tls: { strategy: 'none' } to reach an engine over plain HTTP");
    }
    if (url.protocol === 'https:' && strategy === 'none') {
        throw new Error("serverUrl is https:// but tls.strategy is 'none'; use tls: { strategy: 'tls' } or an http:// serverUrl");
    }
    return serverUrl.replace(/\/+$/, '');
}
/**
 * The transport for runtimes that only have `fetch` (Cloudflare Workers, Vercel Functions,
 * Deno, Bun, browsers). It must not depend on the HTTP version, since the engine answers unary
 * calls over whichever of HTTP/1.1 and HTTP/2 the runtime negotiates, and it enforces the Node
 * transport's message size limits itself because connect-web has none.
 */
function createFetchTransport(options) {
    var _a, _b, _c;
    const send = (_a = options.fetch) !== null && _a !== void 0 ? _a : ((input, init) => globalThis.fetch(input, init));
    const limits = {
        receive: (_b = options.maxReceiveMessageBytes) !== null && _b !== void 0 ? _b : transport_1.DEFAULT_MAX_MESSAGE_BYTES,
        send: (_c = options.maxSendMessageBytes) !== null && _c !== void 0 ? _c : transport_1.DEFAULT_MAX_MESSAGE_BYTES,
    };
    return (0, connect_web_1.createConnectTransport)({
        baseUrl: resolveServerUrl(options),
        useBinaryFormat: true,
        interceptors: [(0, transport_1.createAuthInterceptor)(options.token)],
        fetch: limitMessageSizes(send, limits),
    });
}
/**
 * Wraps a `fetch` with the message size limits the Node transport enforces through Connect's
 * `readMaxBytes` and `writeMaxBytes`, which the fetch transport has no option for. A request
 * body over the send limit is refused before anything is sent, and a response body that grows
 * past the receive limit is cancelled as it streams, counted after the runtime's HTTP
 * decompression so a small compressed body cannot expand past it; the limit covers Connect
 * error bodies as well as messages. Both surface as a `ConnectError` with
 * `Code.ResourceExhausted`, as they do on the Node transport.
 */
function limitMessageSizes(send, limits) {
    return (input, init) => __awaiter(this, void 0, void 0, function* () {
        const size = bodySize(init === null || init === void 0 ? void 0 : init.body);
        if (size > limits.send) {
            throw new connect_1.ConnectError(`message size ${size} is larger than configured maxSendMessageBytes ${limits.send}`, connect_1.Code.ResourceExhausted);
        }
        const response = yield send(input, init);
        if (!response.body) {
            return response;
        }
        return new Response(limitBody(response.body, limits.receive), {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
        });
    });
}
/** The size of the bodies Connect sends; a body of another kind is not measured. */
function bodySize(body) {
    if (typeof body === 'string') {
        return new TextEncoder().encode(body).byteLength;
    }
    if (body instanceof ArrayBuffer || ArrayBuffer.isView(body)) {
        return body.byteLength;
    }
    if (body instanceof Blob) {
        return body.size;
    }
    return 0;
}
function limitBody(body, max) {
    const reader = body.getReader();
    let received = 0;
    return new ReadableStream({
        pull(controller) {
            return __awaiter(this, void 0, void 0, function* () {
                const { done, value } = yield reader.read();
                if (done) {
                    controller.close();
                    return;
                }
                received += value.byteLength;
                if (received > max) {
                    const error = new connect_1.ConnectError(`response body is larger than configured maxReceiveMessageBytes ${max}`, connect_1.Code.ResourceExhausted);
                    yield reader.cancel(error).catch(() => undefined);
                    controller.error(error);
                    return;
                }
                controller.enqueue(value);
            });
        },
        cancel(reason) {
            return reader.cancel(reason);
        },
    });
}
