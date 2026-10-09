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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WorkflowRunRef = exports.MAX_POLL_INTERVAL_MS = exports.INITIAL_POLL_INTERVAL_MS = void 0;
const hatchet_error_1 = __importDefault(require("../util/errors/hatchet-error"));
const abort_error_1 = require("../util/abort-error");
const data_contracts_1 = require("../clients/rest/generated/data-contracts");
const run_detail_1 = require("./run-detail");
/** The first wait between two `GetRunDetails` polls. */
exports.INITIAL_POLL_INTERVAL_MS = 250;
/** The longest wait between two polls; the interval doubles until it gets here. */
exports.MAX_POLL_INTERVAL_MS = 5000;
/**
 * The signal one poll is issued with: it fires when the caller's signal fires or when the
 * deadline arrives, whichever is first. `AbortSignal.any` and `AbortSignal.timeout` are
 * missing on some runtimes the core entry targets, so a plain controller, timer and
 * forwarded abort are used everywhere; the timer is also one a test's fake clock controls.
 */
function pollSignal(signal, remainingMs) {
    const controller = new AbortController();
    let timedOut = false;
    const forwardAbort = () => controller.abort(signal === null || signal === void 0 ? void 0 : signal.reason);
    signal === null || signal === void 0 ? void 0 : signal.addEventListener('abort', forwardAbort, { once: true });
    const timer = remainingMs === undefined
        ? undefined
        : setTimeout(() => {
            timedOut = true;
            controller.abort();
        }, remainingMs);
    return {
        signal: controller.signal,
        timedOut: () => timedOut,
        dispose: () => {
            signal === null || signal === void 0 ? void 0 : signal.removeEventListener('abort', forwardAbort);
            if (timer !== undefined)
                clearTimeout(timer);
        },
    };
}
function wait(ms, signal) {
    return new Promise((resolve) => {
        if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
            resolve();
            return;
        }
        const timer = setTimeout(() => {
            signal === null || signal === void 0 ? void 0 : signal.removeEventListener('abort', onAbort);
            resolve();
        }, ms);
        const onAbort = () => {
            clearTimeout(timer);
            resolve();
        };
        signal === null || signal === void 0 ? void 0 : signal.addEventListener('abort', onAbort, { once: true });
    });
}
/**
 * A reference to a triggered run. `result()` polls `GetRunDetails` until the run is done,
 * which is the one way to wait for a run that works on every platform the core client runs
 * on: no stream is held open, so a Worker request or a browser tab can wait on it.
 */
class WorkflowRunRef {
    constructor(workflowRunId, runs, parentWorkflowRunId, standaloneTaskName, defaultSignal) {
        this.runs = runs;
        this.workflowRunId = workflowRunId;
        this.parentWorkflowRunId = parentWorkflowRunId;
        this._standaloneTaskName = standaloneTaskName;
        this.defaultSignal = defaultSignal;
    }
    get runId() {
        return this.getWorkflowRunId();
    }
    getWorkflowRunId() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.workflowRunId;
        });
    }
    /** The run's result; the same as `result()` with no options. */
    get output() {
        return this.result();
    }
    /** The run's current state. */
    details() {
        return this.runs.getDetails(this.workflowRunId);
    }
    /**
     * Waits for the run to finish and returns its output: the standalone task's output for a
     * task, or the outputs keyed by task name for a workflow.
     *
     * The wait polls with backoff, starting at 250 ms and capped at 5 s with jitter. It is
     * unbounded unless `timeoutMs` is set (rejects with a `HatchetError`) or `signal` fires
     * (rejects with an `AbortError`). Both bound the poll in flight as well as the waits
     * between polls, so a stalled `GetRunDetails` cannot hold the result past the deadline.
     *
     * A failed or cancelled run rejects the way the Node client's `result()` does: with the
     * array of the tasks' error messages when any task reported one. When no task reported an
     * error the run resolves with the tasks' outputs whatever its status, as it does on the
     * Node client, since that is the shape an explicit cancellation leaves. A run with no
     * tasks at all rejects with an `Error` naming the run's status.
     */
    result() {
        return __awaiter(this, arguments, void 0, function* (options = {}) {
            var _a;
            const signal = (_a = options.signal) !== null && _a !== void 0 ? _a : this.defaultSignal;
            const startedAt = Date.now();
            const deadline = options.timeoutMs !== undefined ? startedAt + options.timeoutMs : undefined;
            let interval = exports.INITIAL_POLL_INTERVAL_MS;
            const abortError = () => (0, abort_error_1.createAbortError)(`waiting for run ${this.workflowRunId} was aborted`);
            const timeoutError = () => new hatchet_error_1.default(`timed out after ${options.timeoutMs} ms waiting for run ${this.workflowRunId}`);
            for (;;) {
                if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                    throw abortError();
                }
                const remaining = deadline !== undefined ? deadline - Date.now() : undefined;
                if (remaining !== undefined && remaining <= 0) {
                    throw timeoutError();
                }
                // Whatever the transport rejected with, an abort by the caller's signal surfaces as the
                // AbortError the check above throws and one by the deadline as the same HatchetError;
                // the deadline also goes to the call so the transport applies a Connect timeout.
                const poll = pollSignal(signal, remaining);
                let detail;
                try {
                    detail = yield this.runs.getDetails(this.workflowRunId, { signal: poll.signal, deadline });
                }
                catch (e) {
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw abortError();
                    }
                    if (poll.timedOut() || (deadline !== undefined && Date.now() >= deadline)) {
                        throw timeoutError();
                    }
                    throw e;
                }
                finally {
                    poll.dispose();
                }
                if (detail.done) {
                    return this.resolveResult(detail);
                }
                const now = Date.now();
                if (deadline !== undefined && now >= deadline) {
                    throw timeoutError();
                }
                // Jitter of up to 20% keeps many waiters from polling in lockstep; the cap holds for
                // the jittered wait too.
                let delay = Math.min(Math.round(interval * (1 + Math.random() * 0.2)), exports.MAX_POLL_INTERVAL_MS);
                if (deadline !== undefined) {
                    delay = Math.min(delay, Math.max(0, deadline - now));
                }
                yield wait(delay, signal);
                interval = Math.min(interval * 2, exports.MAX_POLL_INTERVAL_MS);
            }
        });
    }
    resolveResult(detail) {
        if (detail.status === data_contracts_1.V1TaskStatus.COMPLETED) {
            return Promise.resolve(this.outputs(detail));
        }
        const tasks = Object.values(detail.taskRuns);
        const errors = tasks
            .map((task) => task.error)
            .filter((error) => error !== undefined && error !== '');
        if (errors.length > 0) {
            return Promise.reject(errors);
        }
        // A task with no error under a terminal status is what an explicit cancellation
        // (`ctx.cancel()` on a batch member, for one) leaves behind; the Node client resolves it
        // with the task's output rather than failing the wait, and this keeps that contract.
        if (tasks.length > 0) {
            return Promise.resolve(this.outputs(detail));
        }
        const outcome = detail.status === data_contracts_1.V1TaskStatus.CANCELLED ? 'was cancelled' : 'failed';
        return Promise.reject(new Error(`run ${this.workflowRunId} ${outcome}`));
    }
    /**
     * The outputs keyed by task name, or the standalone task's own output, each read the way
     * the Node client reads them; a task output that is not JSON throws its `SyntaxError`.
     */
    outputs(detail) {
        const outputs = {};
        for (const task of Object.values(detail.taskRuns)) {
            outputs[task.readableId] = (0, run_detail_1.parseTaskOutput)(task);
        }
        if (this._standaloneTaskName) {
            return outputs[this._standaloneTaskName];
        }
        return outputs;
    }
    toJSON() {
        return __awaiter(this, void 0, void 0, function* () {
            return JSON.stringify({ workflowRunId: this.workflowRunId });
        });
    }
    cancel() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.runs.cancel({ ids: [this.workflowRunId] });
        });
    }
    replay() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.runs.replay({ ids: [this.workflowRunId] });
        });
    }
}
exports.WorkflowRunRef = WorkflowRunRef;
