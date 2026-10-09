"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MAX_MISSED_HEARTBEATS = void 0;
exports.classifyHeartbeatFailure = classifyHeartbeatFailure;
const grpc_error_1 = require("../../../util/grpc-error");
const failure_severity_1 = require("../../../util/failure-severity");
exports.MAX_MISSED_HEARTBEATS = 3;
// determines whether to immediately error log or wait for additional errors
function classifyHeartbeatFailure(e, missedHeartbeats) {
    const code = (0, grpc_error_1.getGrpcErrorCode)(e);
    const isTransient = (0, grpc_error_1.isConnectionError)(code) || (0, grpc_error_1.isTimeoutOrAbortError)(e);
    return (0, failure_severity_1.classifyRepeatedFailure)(isTransient, missedHeartbeats, exports.MAX_MISSED_HEARTBEATS);
}
