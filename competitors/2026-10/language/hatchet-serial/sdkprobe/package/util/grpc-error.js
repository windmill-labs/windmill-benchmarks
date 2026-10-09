"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CONNECTION_ERROR_CODES = void 0;
exports.isConnectionError = isConnectionError;
exports.isTimeoutOrAbortError = isTimeoutOrAbortError;
exports.getGrpcErrorCode = getGrpcErrorCode;
exports.getGrpcErrorDetails = getGrpcErrorDetails;
const nice_grpc_1 = require("nice-grpc");
/**
 * gRPC codes that typically indicate a transient connectivity problem
 * (server unreachable/restarting) rather than an application-level error.
 */
exports.CONNECTION_ERROR_CODES = [nice_grpc_1.Status.UNAVAILABLE, nice_grpc_1.Status.FAILED_PRECONDITION];
function isConnectionError(code) {
    return code !== undefined && exports.CONNECTION_ERROR_CODES.includes(code);
}
/**
 * Returns true if `e` is the DOMException thrown when an AbortSignal (e.g. from
 * AbortSignal.timeout()) fires. This rejects the underlying call directly rather than
 * surfacing as a gRPC status code, so it needs to be classified as transient separately
 * from isConnectionError.
 */
function isTimeoutOrAbortError(e) {
    return e instanceof DOMException && (e.name === 'TimeoutError' || e.name === 'AbortError');
}
/**
 * Returns the gRPC status code from an unknown value (e.g. from a catch block).
 * Used for checking Status.CANCELLED, Status.UNAVAILABLE, etc.
 */
function getGrpcErrorCode(e) {
    if (e != null && typeof e === 'object' && 'code' in e) {
        const { code } = e;
        return typeof code === 'number' ? code : undefined;
    }
    return undefined;
}
/**
 * Returns the gRPC status message from an unknown value (e.g. from a catch block): the
 * `details` string of a `nice-grpc` or `@grpc/grpc-js` error, or the `rawMessage` of a
 * `ConnectError`, which carries the same server-provided text without the code prefix.
 */
function getGrpcErrorDetails(e) {
    if (e != null && typeof e === 'object') {
        if ('rawMessage' in e) {
            const { rawMessage } = e;
            if (typeof rawMessage === 'string')
                return rawMessage;
        }
        if ('details' in e) {
            const { details } = e;
            return typeof details === 'string' ? details : undefined;
        }
    }
    return undefined;
}
