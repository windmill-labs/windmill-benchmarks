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
exports.createTsProtoClient = createTsProtoClient;
exports.toConnectCallOptions = toConnectCallOptions;
exports.metadataToHeaders = metadataToHeaders;
exports.headersToMetadata = headersToMetadata;
const connect_1 = require("@connectrpc/connect");
const abort_controller_x_1 = require("abort-controller-x");
const nice_grpc_common_1 = require("nice-grpc-common");
const message_bridge_1 = require("./message-bridge");
/**
 * Builds the client a `ts-proto` service definition describes, with every method served by a
 * Connect client for the matching protobuf-es service on `transport`. The result has the exact
 * surface `nice-grpc`'s `ClientFactory.create(definition, channel)` produced: every RPC of the
 * service as `(request, options?) => Promise<response>` with the SDK's `ts-proto` message types,
 * so a caller holding one of the public client properties sees no difference.
 *
 * Both bindings are generated from the same proto, so a method's client name is the same in
 * each and the protobuf-es descriptor can be looked up by it.
 */
function createTsProtoClient(definition, service, transport) {
    const connectClient = (0, connect_1.createClient)(service, transport);
    const client = {};
    for (const [key, method] of Object.entries(definition.methods)) {
        const desc = service.method[key];
        if (!desc) {
            throw new Error(`${definition.fullName}.${method.name} has no protobuf-es descriptor; regenerate src/protoc-es`);
        }
        if (method.requestStream || method.responseStream) {
            client[key] = () => {
                throw new Error(`${definition.fullName}.${method.name} is a streaming RPC, which the unary transport does not serve`);
            };
            continue;
        }
        const requestCodec = method.requestType;
        const responseCodec = method.responseType;
        client[key] = (request, options) => unaryCall(() => connectClient[key]((0, message_bridge_1.toProtobufEs)(desc.input, requestCodec, request), toConnectCallOptions(options)), responseCodec, desc.output, options);
    }
    return client;
}
/**
 * Runs one unary call with `nice-grpc`'s cancellation and deadline semantics: a signal that
 * is already aborted rejects before anything is sent, an abort during the call rejects with the
 * signal's reason (an `AbortError`) rather than the transport's cancellation error, and a
 * deadline that has already passed rejects with `DEADLINE_EXCEEDED` without sending.
 */
function unaryCall(send, responseCodec, responseSchema, options) {
    return __awaiter(this, void 0, void 0, function* () {
        const signal = options === null || options === void 0 ? void 0 : options.signal;
        if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
            throw abortReason(signal);
        }
        if ((options === null || options === void 0 ? void 0 : options.deadline) !== undefined && remainingMs(options.deadline) <= 0) {
            throw new connect_1.ConnectError('the operation timed out', connect_1.Code.DeadlineExceeded);
        }
        let response;
        try {
            response = yield send();
        }
        catch (e) {
            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                throw abortReason(signal);
            }
            throw e;
        }
        return (0, message_bridge_1.fromProtobufEs)(responseCodec, responseSchema, response);
    });
}
function abortReason(signal) {
    var _a;
    return (_a = signal.reason) !== null && _a !== void 0 ? _a : new abort_controller_x_1.AbortError();
}
function remainingMs(deadline) {
    const at = deadline instanceof Date ? deadline.getTime() : deadline;
    return at - Date.now();
}
/**
 * Translates `nice-grpc` call options to Connect's: the signal passes through, an absolute
 * deadline becomes the remaining time, request metadata becomes headers and the header and
 * trailer callbacks receive `Metadata` built from the response headers.
 */
function toConnectCallOptions(options) {
    if (!options) {
        return undefined;
    }
    const { signal, deadline, metadata, onHeader, onTrailer } = options;
    const connect = {};
    if (signal) {
        connect.signal = signal;
    }
    if (deadline !== undefined) {
        connect.timeoutMs = Math.max(1, remainingMs(deadline));
    }
    if (metadata) {
        connect.headers = metadataToHeaders(metadata);
    }
    if (onHeader) {
        connect.onHeader = (headers) => onHeader(headersToMetadata(headers));
    }
    if (onTrailer) {
        connect.onTrailer = (headers) => onTrailer(headersToMetadata(headers));
    }
    return connect;
}
/*
 * Binary metadata is base64 on the wire. These use the standard `btoa`/`atob` rather than
 * `Buffer`, since the fetch transport runs where `Buffer` does not exist.
 */
function bytesToBase64(bytes) {
    let binary = '';
    for (const byte of bytes) {
        binary += String.fromCharCode(byte);
    }
    return btoa(binary);
}
function base64ToBytes(base64) {
    return Uint8Array.from(atob(base64), (char) => char.charCodeAt(0));
}
/**
 * gRPC metadata as HTTP headers: binary values (keys ending in `-bin`) are base64 encoded,
 * which is how they travel on the wire in every gRPC implementation.
 */
function metadataToHeaders(metadata) {
    const headers = new Headers();
    for (const [key, values] of metadata) {
        for (const value of values) {
            headers.append(key, typeof value === 'string' ? value : bytesToBase64(value));
        }
    }
    return headers;
}
/**
 * Response headers as gRPC metadata, the inverse of `metadataToHeaders`. `Headers` joins
 * repeated values with a comma, so a binary header is split back into its values before each
 * is decoded.
 */
function headersToMetadata(headers) {
    const metadata = (0, nice_grpc_common_1.Metadata)();
    headers.forEach((value, key) => {
        if (key.endsWith('-bin')) {
            metadata.set(key, value.split(',').map((part) => base64ToBytes(part.trim())));
        }
        else {
            metadata.set(key, value);
        }
    });
    return metadata;
}
