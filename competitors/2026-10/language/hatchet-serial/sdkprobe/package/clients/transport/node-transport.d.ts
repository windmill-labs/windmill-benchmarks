import type { SecureClientSessionOptions } from 'http2';
import { type GrpcTransportOptions } from '@connectrpc/connect-node';
import type { ClientConfig } from '../hatchet-client/client-config';
import { type Transport } from './transport';
/**
 * The largest message size Connect can enforce (a 32-bit length prefix). The config schema
 * accepts any positive integer, as grpc-js does, so a larger configured limit is clamped to it
 * rather than refused at the first call.
 */
export declare const MAX_MESSAGE_BYTES = 4294967295;
/**
 * The connection settings the `nice-grpc` channel uses (`grpc.keepalive_time_ms`,
 * `grpc.keepalive_timeout_ms`, `grpc.keepalive_permit_without_calls` and
 * `grpc.client_idle_timeout_ms` in `util/grpc-helpers.ts`), so a unary connection is kept alive,
 * given up on and idled exactly as a streaming one.
 */
export declare const SESSION_OPTIONS: {
    readonly pingIntervalMs: number;
    readonly pingTimeoutMs: number;
    readonly pingIdleConnection: true;
    readonly idleConnectionTimeoutMs: number;
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
export declare function tlsSessionOptions(tls: ClientConfig['tls_config']): SecureClientSessionOptions;
/**
 * Creates the Node transport: the gRPC protocol over HTTP/2 (`createGrpcTransport` speaks
 * nothing else), the same wire the SDK's `nice-grpc` clients speak, so an engine sees no
 * difference between the two. The bearer token from the config is attached to every call and
 * the connection is kept alive with pings the way the `nice-grpc` channel is.
 *
 * The transport is built on the first call so that constructing a client reads no TLS files;
 * a client whose unary RPCs are never used never touches them.
 */
export declare function createNodeTransport(config: ClientConfig): Transport;
/**
 * The options `createNodeTransport` builds its transport from, derived from the client config.
 * The session manager is built here from `nodeOptions` and the session settings; Connect takes
 * the connection from it and leaves those transport-level copies as the record of what it uses.
 */
export declare function nodeTransportOptions(config: ClientConfig): GrpcTransportOptions;
