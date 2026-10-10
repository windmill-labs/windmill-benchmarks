"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SESSION_OPTIONS = exports.MAX_MESSAGE_BYTES = void 0;
exports.tlsSessionOptions = tlsSessionOptions;
exports.createNodeTransport = createNodeTransport;
exports.nodeTransportOptions = nodeTransportOptions;
const fs_1 = require("fs");
const connect_node_1 = require("@connectrpc/connect-node");
const grpc_target_1 = require("./grpc-target");
const transport_1 = require("./transport");
/**
 * The largest message size Connect can enforce (a 32-bit length prefix). The config schema
 * accepts any positive integer, as grpc-js does, so a larger configured limit is clamped to it
 * rather than refused at the first call.
 */
exports.MAX_MESSAGE_BYTES = 0xffffffff;
function messageLimit(configured) {
    return Math.min(configured !== null && configured !== void 0 ? configured : transport_1.DEFAULT_MAX_MESSAGE_BYTES, exports.MAX_MESSAGE_BYTES);
}
/**
 * The connection settings the `nice-grpc` channel uses (`grpc.keepalive_time_ms`,
 * `grpc.keepalive_timeout_ms`, `grpc.keepalive_permit_without_calls` and
 * `grpc.client_idle_timeout_ms` in `util/grpc-helpers.ts`), so a unary connection is kept alive,
 * given up on and idled exactly as a streaming one.
 */
exports.SESSION_OPTIONS = {
    pingIntervalMs: 10 * 1000,
    pingTimeoutMs: 60 * 1000,
    pingIdleConnection: true,
    idleConnectionTimeoutMs: 60 * 1000,
};
/**
 * Builds the TLS session options for the engine connection from the client's `tls_config`:
 * `none` means plaintext, `tls` verifies the server against the trusted roots, and `mtls` (the
 * default when no strategy is set) additionally presents `cert_file` and `key_file`.
 * `server_name` overrides the hostname used for SNI and certificate verification, which is what
 * lets a client reach an engine through an address its certificate does not name.
 *
 * The roots and cipher suites follow grpc-js, which the streaming channel uses: `ca_file`, else
 * the bundle `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` names, else the platform roots; and
 * `GRPC_SSL_CIPHER_SUITES` when set.
 */
function tlsSessionOptions(tls) {
    // grpc-js verifies the peer regardless of NODE_TLS_REJECT_UNAUTHORIZED, which is Node's
    // process-wide switch; set explicitly, the switch cannot weaken this connection either.
    const options = { rejectUnauthorized: true };
    const rootsFile = tls.ca_file || process.env.GRPC_DEFAULT_SSL_ROOTS_FILE_PATH;
    if (rootsFile) {
        options.ca = (0, fs_1.readFileSync)(rootsFile);
    }
    const ciphers = process.env.GRPC_SSL_CIPHER_SUITES;
    if (ciphers) {
        options.ciphers = ciphers;
    }
    if (tls.tls_strategy !== 'tls') {
        // grpc-js refuses half a client identity with these messages rather than connecting without
        // one, which an endpoint that does not require client certificates would silently accept.
        if (tls.key_file && !tls.cert_file) {
            throw new Error('Private key must be given with accompanying certificate chain');
        }
        if (tls.cert_file && !tls.key_file) {
            throw new Error('Certificate chain must be given with accompanying private key');
        }
        if (tls.key_file && tls.cert_file) {
            options.key = (0, fs_1.readFileSync)(tls.key_file);
            options.cert = (0, fs_1.readFileSync)(tls.cert_file);
        }
    }
    if (tls.server_name) {
        options.servername = tls.server_name;
    }
    return options;
}
/**
 * Creates the Node transport: the gRPC protocol over HTTP/2 (`createGrpcTransport` speaks
 * nothing else), the same wire the SDK's `nice-grpc` clients speak, so an engine sees no
 * difference between the two. The bearer token from the config is attached to every call and
 * the connection is kept alive with pings the way the `nice-grpc` channel is.
 *
 * The transport is built on the first call so that constructing a client reads no TLS files;
 * a client whose unary RPCs are never used never touches them.
 */
function createNodeTransport(config) {
    let transport;
    const get = () => {
        if (!transport) {
            transport = (0, connect_node_1.createGrpcTransport)(nodeTransportOptions(config));
        }
        return transport;
    };
    return {
        unary: (...args) => get().unary(...args),
        stream: (...args) => get().stream(...args),
    };
}
/**
 * The options `createNodeTransport` builds its transport from, derived from the client config.
 * The session manager is built here from `nodeOptions` and the session settings; Connect takes
 * the connection from it and leaves those transport-level copies as the record of what it uses.
 */
function nodeTransportOptions(config) {
    const insecure = config.tls_config.tls_strategy === 'none';
    const target = (0, grpc_target_1.parseGrpcTarget)(config.host_port);
    const baseUrl = (0, grpc_target_1.grpcTargetBaseUrl)(target, insecure ? 'http' : 'https');
    const nodeOptions = insecure ? undefined : tlsSessionOptions(config.tls_config);
    return Object.assign({ baseUrl,
        nodeOptions, sessionManager: createSessionManager(baseUrl, target.authority, nodeOptions), interceptors: [(0, transport_1.createAuthInterceptor)(config.token)], sendCompression: connect_node_1.compressionGzip, readMaxBytes: messageLimit(config.grpc_max_recv_message_length), writeMaxBytes: messageLimit(config.grpc_max_send_message_length) }, exports.SESSION_OPTIONS);
}
/**
 * The HTTP/2 session for the transport, sending `:authority` as grpc-js does: the target as
 * written, so `server_name` changes only SNI and certificate verification. Node otherwise
 * derives the authority from `servername` when it is set, which would also change the virtual
 * host a proxy in front of the engine routes on.
 */
function createSessionManager(baseUrl, authority, nodeOptions) {
    const manager = new connect_node_1.Http2SessionManager(baseUrl, exports.SESSION_OPTIONS, nodeOptions);
    return {
        authority: manager.authority,
        request: (method, path, headers, options) => manager.request(method, path, Object.assign({ ':authority': authority }, headers), options),
        notifyResponseByteRead: (stream) => manager.notifyResponseByteRead(stream),
    };
}
