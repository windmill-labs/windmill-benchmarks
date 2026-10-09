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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DedupeViolationErr = void 0;
const nice_grpc_1 = require("nice-grpc");
const grpc_error_1 = require("./grpc-error");
const dispatcher_1 = require("../protoc/dispatcher");
class DedupeViolationErr extends Error {
    constructor(message) {
        super(message);
        this.name = 'DedupeViolationErr';
    }
}
exports.DedupeViolationErr = DedupeViolationErr;
function getWorkflowRunId(workflowRunId) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        if (typeof workflowRunId === 'string') {
            return workflowRunId;
        }
        if (workflowRunId instanceof Promise) {
            try {
                const resolved = yield workflowRunId;
                if (typeof resolved === 'string') {
                    return resolved;
                }
                return resolved.workflowRunId;
            }
            catch (e) {
                if ((0, grpc_error_1.getGrpcErrorCode)(e) === nice_grpc_1.Status.ALREADY_EXISTS) {
                    throw new DedupeViolationErr((_a = (0, grpc_error_1.getGrpcErrorDetails)(e)) !== null && _a !== void 0 ? _a : '');
                }
                throw e;
            }
        }
        throw new Error('Invalid workflowRunId: must be a string or a promise');
    });
}
class WorkflowRunRef {
    constructor(workflowRunId, client, runsClient, parentWorkflowRunId, standaloneTaskName, defaultSignal) {
        this.workflowRunId = workflowRunId;
        this.parentWorkflowRunId = parentWorkflowRunId;
        this.client = client;
        this.runs = runsClient;
        this._standaloneTaskName = standaloneTaskName;
        this.defaultSignal = defaultSignal;
    }
    // TODO docstrings
    get runId() {
        return this.getWorkflowRunId();
    }
    // @deprecated use runId
    getWorkflowRunId() {
        return __awaiter(this, void 0, void 0, function* () {
            return getWorkflowRunId(this.workflowRunId);
        });
    }
    stream() {
        return __awaiter(this, void 0, void 0, function* () {
            const workflowRunId = yield getWorkflowRunId(this.workflowRunId);
            return this.client.stream(workflowRunId);
        });
    }
    get output() {
        return this.result();
    }
    /**
     * @alias output
     * @deprecated use output
     */
    result() {
        return __awaiter(this, void 0, void 0, function* () {
            const workflowRunId = yield getWorkflowRunId(this.workflowRunId);
            const streamable = yield this.client.get(workflowRunId);
            return new Promise((resolve, reject) => {
                (() => __awaiter(this, void 0, void 0, function* () {
                    var _a, e_1, _b, _c;
                    var _d, _e, _f;
                    const signal = this.defaultSignal;
                    try {
                        for (var _g = true, _h = __asyncValues(streamable.stream({ signal })), _j; _j = yield _h.next(), _a = _j.done, !_a; _g = true) {
                            _c = _j.value;
                            _g = false;
                            const event = _c;
                            if (event.eventType === dispatcher_1.WorkflowRunEventType.WORKFLOW_RUN_EVENT_TYPE_FINISHED) {
                                // A present-but-empty error field is ambiguous (e.g. a batch member that was
                                // explicitly cancelled via ctx.cancel() and resolved with no further error);
                                // only a non-empty message is treated as a genuine failure here.
                                if (event.results.some((r) => r.error !== undefined && r.error !== '')) {
                                    const errors = event.results
                                        .filter((r) => r.error !== undefined && r.error !== '')
                                        .map((r) => r.error);
                                    reject(errors);
                                    return;
                                }
                                if (event.results.length === 0) {
                                    const data = yield this.client.api.workflowRunGetShape(this.client.config.tenant_id, event.workflowRunId);
                                    const mostRecentJobRun = (_d = data.data.jobRuns) === null || _d === void 0 ? void 0 : _d[0];
                                    if (!mostRecentJobRun) {
                                        reject(new Error('No job runs found'));
                                        return;
                                    }
                                    // A task run that fails before ever being dispatched (e.g. a batch group-key
                                    // CEL expression that fails to parse) never gets a StepRunResult in the
                                    // WORKFLOW_RUN_EVENT_TYPE_FINISHED event, so `event.results` above is empty.
                                    // Check step statuses here too, or this codepath would resolve as if the run
                                    // had succeeded.
                                    const failedStepRun = (_e = mostRecentJobRun.stepRuns) === null || _e === void 0 ? void 0 : _e.find((stepRun) => stepRun.status === 'FAILED' || stepRun.status === 'CANCELLED');
                                    if (failedStepRun) {
                                        reject(new Error(failedStepRun.error || failedStepRun.cancelledReason || 'task failed'));
                                        return;
                                    }
                                    const outputs = {};
                                    (_f = mostRecentJobRun.stepRuns) === null || _f === void 0 ? void 0 : _f.forEach((stepRun) => {
                                        var _a, _b;
                                        const readable = (_b = (_a = mostRecentJobRun.job) === null || _a === void 0 ? void 0 : _a.steps) === null || _b === void 0 ? void 0 : _b.find((step) => step.metadata.id === stepRun.stepId);
                                        const readableStepName = `${readable === null || readable === void 0 ? void 0 : readable.readableId}`;
                                        try {
                                            outputs[readableStepName] = JSON.parse(stepRun.output || '{}');
                                        }
                                        catch (_c) {
                                            outputs[readableStepName] = stepRun.output;
                                        }
                                    });
                                    if (!this._standaloneTaskName) {
                                        resolve(outputs);
                                        return;
                                    }
                                    resolve(outputs[this._standaloneTaskName]);
                                    return;
                                }
                                const result = event.results.reduce((acc, r) => (Object.assign(Object.assign({}, acc), { [r.taskName]: JSON.parse(r.output || '{}') })), {});
                                if (!this._standaloneTaskName) {
                                    resolve(result);
                                    return;
                                }
                                resolve(result[this._standaloneTaskName]);
                                return;
                            }
                        }
                    }
                    catch (e_1_1) { e_1 = { error: e_1_1 }; }
                    finally {
                        try {
                            if (!_g && !_a && (_b = _h.return)) yield _b.call(_h);
                        }
                        finally { if (e_1) throw e_1.error; }
                    }
                }))().catch(reject);
            });
        });
    }
    toJSON() {
        return __awaiter(this, void 0, void 0, function* () {
            return JSON.stringify({
                workflowRunId: yield this.workflowRunId,
            });
        });
    }
    cancel() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.runs) {
                throw new Error('cancel is a v1 only feature, please upgrade your sdk');
            }
            const workflowRunId = yield getWorkflowRunId(this.workflowRunId);
            yield this.runs.cancel({
                ids: [workflowRunId],
            });
        });
    }
    replay() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.runs) {
                throw new Error('replay is a v1 only feature, please upgrade your sdk');
            }
            const workflowRunId = yield getWorkflowRunId(this.workflowRunId);
            yield this.runs.replay({
                ids: [workflowRunId],
            });
        });
    }
}
exports.default = WorkflowRunRef;
