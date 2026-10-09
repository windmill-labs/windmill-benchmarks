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
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __asyncGenerator = (this && this.__asyncGenerator) || function (thisArg, _arguments, generator) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var g = generator.apply(thisArg, _arguments || []), i, q = [];
    return i = Object.create((typeof AsyncIterator === "function" ? AsyncIterator : Object).prototype), verb("next"), verb("throw"), verb("return", awaitReturn), i[Symbol.asyncIterator] = function () { return this; }, i;
    function awaitReturn(f) { return function (v) { return Promise.resolve(v).then(f, reject); }; }
    function verb(n, f) { if (g[n]) { i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; if (f) i[n] = f(i[n]); } }
    function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
    function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
    function fulfill(value) { resume("next", value); }
    function reject(value) { resume("throw", value); }
    function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RunsClient = exports.runStatusToJSON = void 0;
const workflow_run_ref_1 = __importDefault(require("../../../util/workflow-run-ref"));
const child_listener_client_1 = require("../../../clients/listeners/run-listener/child-listener-client");
const rpc_1 = require("../../../clients/admin/rpc");
const run_detail_1 = require("../../../core/run-detail");
Object.defineProperty(exports, "runStatusToJSON", { enumerable: true, get: function () { return run_detail_1.runStatusToJSON; } });
/**
 * The runs client is a client for interacting with task and workflow runs within Hatchet.
 */
class RunsClient {
    constructor(client) {
        this.api = client.api;
        this.tenantId = client.tenantId;
        this.workflows = client.workflows;
        this.listener = client._listener;
        this._transport = client.transport;
    }
    /** The v1 admin RPCs on the client's Connect transport, the same path the core client uses. */
    get adminRpc() {
        if (!this._adminRpc) {
            this._adminRpc = (0, rpc_1.createV1AdminRpc)(this._transport);
        }
        return this._adminRpc;
    }
    /**
     * Gets a task or workflow run by its ID.
     * @param run - The ID of the run to get.
     * @returns A promise that resolves to the run.
     */
    get(run) {
        return __awaiter(this, void 0, void 0, function* () {
            const runId = typeof run === 'string' ? run : yield run.getWorkflowRunId();
            const { data } = yield this.api.v1WorkflowRunGet(runId);
            return data;
        });
    }
    /**
     * Gets the status of a task or workflow run by its ID.
     * @param run - The ID of the run to get the status of.
     * @returns A promise that resolves to the status of the run.
     */
    get_status(run) {
        return __awaiter(this, void 0, void 0, function* () {
            const runId = typeof run === 'string' ? run : yield run.getWorkflowRunId();
            const { data } = yield this.api.v1WorkflowRunGetStatus(runId);
            return data;
        });
    }
    /**
     * Gets run details
     * @param run - The workflow run ID (string) or a WorkflowRunRef.
     * @returns A promise resolving to GetRunDetailsResponse with task run statuses and outputs.
     */
    getDetails(run) {
        return __awaiter(this, void 0, void 0, function* () {
            const runId = typeof run === 'string' ? run : yield run.getWorkflowRunId();
            return (0, run_detail_1.toRunDetail)(yield this.adminRpc.getRunDetails({ externalId: runId }));
        });
    }
    /**
     * Lists all task and workflow runs for the current tenant.
     * @param opts - The options for the list operation.
     * @returns A promise that resolves to the list of runs.
     */
    list(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const normalizedOpts = (opts === null || opts === void 0 ? void 0 : opts.parentTaskExternalId) && !(opts === null || opts === void 0 ? void 0 : opts.parentTaskRunExternalId)
                ? Object.assign(Object.assign({}, opts), { parentTaskRunExternalId: opts.parentTaskExternalId }) : opts;
            const { data } = yield this.api.v1WorkflowRunList(this.tenantId, Object.assign({}, (yield this.prepareListFilter(normalizedOpts || {}))));
            return data;
        });
    }
    /**
     * Cancels a task or workflow run by its ID.
     * @param opts - The options for the cancel operation.
     * @returns A promise that resolves to the cancelled run.
     */
    cancel(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const filter = yield this.prepareFilter(opts.filters || {});
            return this.api.v1TaskCancel(this.tenantId, {
                externalIds: opts.ids,
                filter: !opts.ids ? filter : undefined,
            });
        });
    }
    /**
     * Replays a task or workflow run by its ID.
     * @param opts - The options for the replay operation.
     * @returns A promise that resolves to the replayed run.
     */
    replay(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const filter = yield this.prepareFilter(opts.filters || {});
            return this.api.v1TaskReplay(this.tenantId, {
                externalIds: opts.ids,
                filter: !opts.ids ? filter : undefined,
            });
        });
    }
    prepareFilter(_a) {
        return __awaiter(this, arguments, void 0, function* ({ since, until, statuses, workflowNames, additionalMetadata, }) {
            const am = Object.entries(additionalMetadata || {}).map(([key, value]) => `${key}:${value}`);
            return {
                // default to 1 hour ago
                since: since ? since.toISOString() : new Date(Date.now() - 1000 * 60 * 60).toISOString(),
                until: until === null || until === void 0 ? void 0 : until.toISOString(),
                statuses,
                workflowIds: yield Promise.all((workflowNames === null || workflowNames === void 0 ? void 0 : workflowNames.map((name) => __awaiter(this, void 0, void 0, function* () { return (yield this.workflows.get(name)).metadata.id; }))) || []),
                additionalMetadata: am,
            };
        });
    }
    prepareListFilter(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            const am = Object.entries(opts.additionalMetadata || {}).map(([key, value]) => `${key}:${value}`);
            return {
                offset: opts.offset,
                limit: opts.limit,
                // default to 1 hour ago
                since: opts.since
                    ? opts.since.toISOString()
                    : new Date(Date.now() - 1000 * 60 * 60).toISOString(),
                until: (_a = opts.until) === null || _a === void 0 ? void 0 : _a.toISOString(),
                statuses: opts.statuses,
                worker_id: opts.workerId,
                workflow_ids: yield Promise.all(((_b = opts.workflowNames) === null || _b === void 0 ? void 0 : _b.map((name) => __awaiter(this, void 0, void 0, function* () { return (yield this.workflows.get(name)).metadata.id; }))) || []),
                additional_metadata: am,
                only_tasks: opts.onlyTasks || false,
                parent_task_external_id: opts.parentTaskRunExternalId,
                triggering_event_external_id: opts.triggeringEventExternalId,
                include_payloads: opts.includePayloads,
            };
        });
    }
    /**
     * Restore an evicted durable task so it can resume execution.
     * @param taskExternalId - The external ID of the evicted task.
     */
    restoreTask(taskExternalId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.api.v1TaskRestore(taskExternalId);
        });
    }
    /**
     * Fork (reset) a durable task from a specific node, triggering re-execution from that point.
     * @param taskExternalId - The external ID of the durable task to reset.
     * @param nodeId - The node ID to replay from.
     */
    branchDurableTask(taskExternalId_1, nodeId_1) {
        return __awaiter(this, arguments, void 0, function* (taskExternalId, nodeId, branchId = 0) {
            return this.api.v1DurableTaskBranch(this.tenantId, {
                taskExternalId,
                nodeId,
                branchId,
            });
        });
    }
    /**
     * Resolve the task external ID for a workflow run. For runs with multiple tasks,
     * returns the first task's external ID.
     * @param workflowRunId - The workflow run ID to look up.
     * @returns The task external ID.
     */
    getTaskExternalId(workflowRunId) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            const run = yield this.get(workflowRunId);
            const tasks = run === null || run === void 0 ? void 0 : run.tasks;
            if (Array.isArray(tasks) && tasks.length > 0 && ((_a = tasks[0]) === null || _a === void 0 ? void 0 : _a.taskExternalId)) {
                return tasks[0].taskExternalId;
            }
            throw new Error(`Could not find task external ID for workflow run ${workflowRunId}`);
        });
    }
    runRef(id) {
        return new workflow_run_ref_1.default(id, this.listener, this);
    }
    /**
     * Subscribes to a stream of events for a task or workflow run by its ID.
     * @param workflowRunId - The ID of the run to subscribe to.
     * @returns A promise that resolves to the stream of events.
     */
    subscribeToStream(workflowRunId) {
        return __asyncGenerator(this, arguments, function* subscribeToStream_1() {
            var _a, e_1, _b, _c;
            const ref = this.runRef(workflowRunId);
            const stream = yield __await(ref.stream());
            try {
                for (var _d = true, stream_1 = __asyncValues(stream), stream_1_1; stream_1_1 = yield __await(stream_1.next()), _a = stream_1_1.done, !_a; _d = true) {
                    _c = stream_1_1.value;
                    _d = false;
                    const event = _c;
                    if (event.type === child_listener_client_1.RunEventType.STEP_RUN_EVENT_TYPE_STREAM) {
                        yield yield __await(event.payload);
                    }
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (!_d && !_a && (_b = stream_1.return)) yield __await(_b.call(stream_1));
                }
                finally { if (e_1) throw e_1.error; }
            }
        });
    }
}
exports.RunsClient = RunsClient;
