/**
 * gRPC codes that typically indicate a transient connectivity problem
 * (server unreachable/restarting) rather than an application-level error.
 */
export declare const CONNECTION_ERROR_CODES: number[];
export declare function isConnectionError(code: number | undefined): boolean;
/**
 * Returns true if `e` is the DOMException thrown when an AbortSignal (e.g. from
 * AbortSignal.timeout()) fires. This rejects the underlying call directly rather than
 * surfacing as a gRPC status code, so it needs to be classified as transient separately
 * from isConnectionError.
 */
export declare function isTimeoutOrAbortError(e: unknown): boolean;
/**
 * Returns the gRPC status code from an unknown value (e.g. from a catch block).
 * Used for checking Status.CANCELLED, Status.UNAVAILABLE, etc.
 */
export declare function getGrpcErrorCode(e: unknown): number | undefined;
/**
 * Returns the gRPC status message from an unknown value (e.g. from a catch block): the
 * `details` string of a `nice-grpc` or `@grpc/grpc-js` error, or the `rawMessage` of a
 * `ConnectError`, which carries the same server-provided text without the code prefix.
 */
export declare function getGrpcErrorDetails(e: unknown): string | undefined;
