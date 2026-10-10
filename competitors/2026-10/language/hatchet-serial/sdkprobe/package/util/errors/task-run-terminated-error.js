"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaskRunTerminatedError = void 0;
exports.isTaskRunTerminatedError = isTaskRunTerminatedError;
class TaskRunTerminatedError extends Error {
    constructor(reason, message) {
        super(message !== null && message !== void 0 ? message : reason);
        this.name = 'TaskRunTerminatedError';
        this.reason = reason;
    }
}
exports.TaskRunTerminatedError = TaskRunTerminatedError;
function isTaskRunTerminatedError(err) {
    return err instanceof TaskRunTerminatedError;
}
