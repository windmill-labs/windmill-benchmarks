"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CancellationReason = void 0;
const task_run_terminated_error_1 = require("../errors/task-run-terminated-error");
/** Canonical reasons when cancelling a HatchetPromise (e.g. worker shutdown). */
var CancellationReason;
(function (CancellationReason) {
    CancellationReason["CANCELLED_BY_WORKER"] = "Cancelled by worker";
    CancellationReason["EVICTED_BY_WORKER"] = "Evicted by worker";
})(CancellationReason || (exports.CancellationReason = CancellationReason = {}));
const reasonToTermination = {
    [CancellationReason.CANCELLED_BY_WORKER]: 'cancelled',
    [CancellationReason.EVICTED_BY_WORKER]: 'evicted',
};
class HatchetPromise {
    constructor(promise) {
        this.cancel = (_reason) => { };
        this.inner = Promise.resolve(promise);
        this.promise = new Promise((resolve, reject) => {
            this.cancel = (reason) => {
                const termination = reason ? reasonToTermination[reason] : 'cancelled';
                reject(new task_run_terminated_error_1.TaskRunTerminatedError(termination, reason));
            };
            this.inner.then(resolve).catch(reject);
        });
    }
}
exports.default = HatchetPromise;
