"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MAX_TRANSIENT_LISTENER_RETRIES = void 0;
exports.classifyListenerFailure = classifyListenerFailure;
const grpc_error_1 = require("../../util/grpc-error");
const failure_severity_1 = require("../../util/failure-severity");
exports.MAX_TRANSIENT_LISTENER_RETRIES = 3;
function classifyListenerFailure(e, retries) {
    const isTransient = e === undefined || (0, grpc_error_1.isConnectionError)((0, grpc_error_1.getGrpcErrorCode)(e));
    return (0, failure_severity_1.classifyRepeatedFailure)(isTransient, retries, exports.MAX_TRANSIENT_LISTENER_RETRIES);
}
