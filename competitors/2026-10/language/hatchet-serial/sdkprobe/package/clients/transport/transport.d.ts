import type { Interceptor, Transport } from '@connectrpc/connect';
/**
 * The seam every Connect-backed client calls through. A transport carries the protocol
 * (gRPC over HTTP/2 in Node, Connect over fetch elsewhere), the connection settings and the
 * interceptors that apply to every call.
 */
export type { Transport };
/**
 * The largest message either transport accepts or sends unless configured otherwise: the
 * grpc-js default the `grpc_max_recv_message_length` and `grpc_max_send_message_length`
 * settings override.
 */
export declare const DEFAULT_MAX_MESSAGE_BYTES: number;
/**
 * Sets the bearer token the engine authenticates every call with. The transport applies it to
 * each request, so callers never pass credentials per call. A token that cannot be carried in a
 * header is refused here with a fixed message, so the token's value never reaches a log or an
 * error.
 */
export declare function createAuthInterceptor(token: string): Interceptor;
