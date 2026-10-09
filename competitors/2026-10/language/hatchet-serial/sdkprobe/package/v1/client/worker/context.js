"use strict";
/**
 * The Hatchet Context class provides helper methods and useful data to tasks at runtime. It is passed as the second argument to all tasks and durable tasks.
 *
 * There are two types of context classes you'll encounter:
 *
 * - Context - The standard context for regular tasks with methods for logging, task output retrieval, cancellation, and more.
 * - DurableContext - An extended context for durable tasks that includes additional methods for durable execution.
 * @module Context
 */
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
exports.DurableContext = exports.Context = exports.ContextWorker = void 0;
exports.computeMemoKey = computeMemoKey;
const declaration_1 = require("../../declaration");
const hatchet_error_1 = __importDefault(require("../../../util/errors/hatchet-error"));
const action_1 = require("../../../clients/dispatcher/action");
const parse_1 = require("../../../util/parse");
const conditions_1 = require("../../conditions");
const transformer_1 = require("../../conditions/transformer");
const condition_1 = require("../../../protoc/v1/shared/condition");
const apply_namespace_1 = require("../../../util/apply-namespace");
const abort_error_1 = require("../../../util/abort-error");
const parent_run_context_vars_1 = require("../../parent-run-context-vars");
const duration_1 = require("../duration");
const engine_version_1 = require("./engine-version");
const pre_eviction_1 = require("./deprecated/pre-eviction");
const runtime_1 = require("./runtime");
const worker_runtime_1 = require("./worker-runtime");
/**
 * ContextWorker is a user-friendly view of the worker running the task, from the context of a run.
 */
class ContextWorker {
    constructor(runtime) {
        this.runtime = runtime;
    }
    /**
     * Gets the ID of the worker.
     * @returns The ID of the worker.
     */
    id() {
        return this.runtime.workerId();
    }
    /**
     * Checks if the worker has a registered workflow.
     * @param workflowName - The name of the workflow to check.
     * @returns True if the workflow is registered, otherwise false.
     */
    hasWorkflow(workflowName) {
        return this.runtime.hasWorkflow(workflowName);
    }
    /**
     * Gets the current state of the worker labels.
     * @returns The labels of the worker.
     */
    labels() {
        return this.runtime.workerLabels();
    }
    /**
     * Upserts the a set of labels on the worker.
     * @param labels - The labels to upsert.
     * @returns A promise that resolves when the labels have been upserted.
     */
    upsertLabels(labels) {
        return this.runtime.upsertWorkerLabels(labels);
    }
}
exports.ContextWorker = ContextWorker;
function resolveRuntime(runtimeOrClient, worker) {
    if ((0, runtime_1.isContextRuntime)(runtimeOrClient)) {
        return runtimeOrClient;
    }
    return (0, worker_runtime_1.createWorkerContextRuntime)(runtimeOrClient, worker);
}
class Context {
    nextChildIndex(n = 1) {
        var _a;
        const ctx = parent_run_context_vars_1.parentRunContextManager.getContext();
        const idx = (_a = ctx === null || ctx === void 0 ? void 0 : ctx.childIndex) !== null && _a !== void 0 ? _a : this.spawnIndex;
        parent_run_context_vars_1.parentRunContextManager.incrementChildIndex(n);
        this.spawnIndex = idx + n;
        return idx;
    }
    constructor(action, runtimeOrClient, worker) {
        // @deprecated use ctx.abortController instead
        this.controller = new AbortController();
        this.overridesData = {};
        /** @deprecated Kept for backward compat; prefer {@link nextChildIndex}. */
        this.spawnIndex = 0;
        this.streamIndex = 0;
        try {
            const runtime = resolveRuntime(runtimeOrClient, worker);
            const data = (0, parse_1.parseJSON)(action.actionPayload);
            this.data = data;
            this.action = action;
            this.runtime = runtime;
            this.v1 = runtime.client;
            this.worker = new ContextWorker(runtime);
            this._logger = runtime.logger(`Context Logger`);
            // if this is a getGroupKeyRunId, the data is the workflow input
            if (action.getGroupKeyRunId !== '') {
                this.input = data;
            }
            else {
                this.input = data.input;
            }
            this.overridesData = data.overrides || {};
        }
        catch (e) {
            throw new hatchet_error_1.default(`Could not parse payload: ${e.message}`);
        }
    }
    get abortController() {
        return this.controller;
    }
    get cancelled() {
        return this.controller.signal.aborted;
    }
    throwIfCancelled() {
        if (this.abortController.signal.aborted) {
            throw (0, abort_error_1.createAbortError)('Operation cancelled by AbortSignal');
        }
    }
    /**
     * Helper for broad `catch` blocks so cancellation isn't accidentally swallowed.
     *
     * Example:
     * ```ts
     * try { ... } catch (e) { ctx.rethrowIfCancelled(e); ... }
     * ```
     */
    rethrowIfCancelled(err) {
        (0, abort_error_1.rethrowIfAborted)(err);
    }
    cancel() {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            if (this.action.batchId) {
                // Batch tasks share one context across every buffered member, so there is no single
                // task-run id to cancel, so cancel every member of the batch instead. `this.data` is
                // the raw batch-items map for a START_BATCH action, keyed by each member's
                // task-run external id.
                const memberIds = Object.keys((_a = this.data) !== null && _a !== void 0 ? _a : {});
                yield this.runtime.cancelBatch({
                    workerId: (_b = this.worker.id()) !== null && _b !== void 0 ? _b : '',
                    jobId: this.action.jobId,
                    actionId: this.action.actionId,
                    batchId: this.action.batchId,
                    memberIds,
                });
            }
            else {
                yield this.runtime.cancelRun(this.action.taskRunExternalId);
            }
            // optimistically abort the run
            this.controller.abort();
        });
    }
    /**
     * Retrieves the output of a parent task.
     * @param parentTask - The a CreateTaskOpts or string of the parent task name.
     * @returns The output of the specified parent task.
     * @throws An error if the task output is not found.
     */
    parentOutput(parentTask) {
        return __awaiter(this, void 0, void 0, function* () {
            // NOTE: parentOutput is async since we plan on potentially making this a cacheable server call
            if (typeof parentTask !== 'string') {
                return this.parentOutput(parentTask.name);
            }
            if (!this.data.parents) {
                throw new hatchet_error_1.default('Parent task outputs not found');
            }
            if (!this.data.parents[parentTask]) {
                throw new hatchet_error_1.default(`Output for parent task '${parentTask}' not found`);
            }
            return this.data.parents[parentTask];
        });
    }
    /**
     * Returns errors from any task runs in the workflow.
     * @returns A record mapping task names to error messages.
     * @throws A warning if no errors are found (this method should be used in on-failure tasks).
     * @deprecated use ctx.errors() instead
     * @hidden
     */
    stepRunErrors() {
        return this.errors();
    }
    /**
     * Returns errors from any task runs in the workflow.
     * @returns A record mapping task names to error messages.
     * @throws A warning if no errors are found (this method should be used in on-failure tasks).
     */
    errors() {
        const errors = this.data.step_run_errors || {};
        if (Object.keys(errors).length === 0) {
            this._logger.error('No run errors found. `ctx.errors` is intended to be run in an on-failure task, and will only work on engine versions more recent than v0.53.10');
        }
        return errors;
    }
    /**
     * Gets the dag conditional triggers for the current workflow run.
     * @returns The triggers for the current workflow.
     */
    triggers() {
        return this.data.triggers;
    }
    /**
     * Gets the payload from the filter that matched when triggering the event.
     * @returns The payload.
     */
    filterPayload() {
        var _a;
        return ((_a = this.data.triggers) === null || _a === void 0 ? void 0 : _a.filter_payload) || {};
    }
    /**
     * Determines if the workflow was triggered by an event.
     * @returns True if the workflow was triggered by an event, otherwise false.
     */
    triggeredByEvent() {
        var _a;
        return ((_a = this.data) === null || _a === void 0 ? void 0 : _a.triggered_by) === 'event';
    }
    /**
     * Gets the name of the current workflow.
     * @returns The name of the workflow.
     * @deprecated This method returns the task name. Use {@link workflowNameV1} for the workflow
     * name or {@link taskName} for the task name.
     */
    workflowName() {
        return this.action.jobName;
    }
    /**
     * Gets the name of the current workflow.
     * @returns The name of the workflow.
     */
    workflowNameV1() {
        return (0, action_1.workflowNameFromAction)(this.action);
    }
    /**
     * Gets the user data associated with the workflow.
     * @returns The user data.
     */
    userData() {
        var _a;
        return (_a = this.data) === null || _a === void 0 ? void 0 : _a.user_data;
    }
    /**
     * Gets the name of the current running task.
     * @returns The name of the task.
     */
    taskName() {
        return this.action.taskName;
    }
    /**
     * Gets the ID of the current workflow run.
     * @returns The workflow run ID.
     */
    workflowRunId() {
        return this.action.workflowRunId;
    }
    /**
     * Gets the workflow ID of the currently running workflow.
     * @returns The workflow id.
     */
    workflowId() {
        return this.action.workflowId;
    }
    /**
     * Gets the workflow version ID of the currently running workflow.
     * @returns The workflow version ID.
     */
    workflowVersionId() {
        return this.action.workflowVersionId;
    }
    /**
     * Gets the ID of the current task run.
     * @returns The task run ID.
     */
    taskRunExternalId() {
        return this.action.taskRunExternalId;
    }
    /**
     * Gets the ID of the current task run.
     * @returns The task run ID.
     * @deprecated use taskRunExternalId() instead
     * @hidden
     */
    taskRunId() {
        return this.taskRunExternalId();
    }
    /**
     * Gets the number of times the current task has been retried.
     * @returns The retry count.
     */
    retryCount() {
        return this.action.retryCount;
    }
    /**
     * Logs a message from the current task.
     * @param message - The message to log.
     * @param level - The log level (optional).
     * @deprecated use ctx.logger.infoger.info, ctx.logger.infoger.debug, ctx.logger.infoger.warn, ctx.logger.infoger.error, ctx.logger.infoger.trace instead
     * @hidden
     */
    log(message, level, extra) {
        const { taskRunExternalId } = this.action;
        if (!taskRunExternalId) {
            // log a warning
            this._logger.warn('cannot log from context without stepRunId');
            return Promise.resolve();
        }
        const logger = this.runtime.logger('ctx');
        const contextExtra = Object.assign({ workflowRunId: this.action.workflowRunId, taskRunExternalId: this.action.taskRunExternalId, retryCount: this.action.retryCount, workflowName: this.workflowNameV1() }, extra === null || extra === void 0 ? void 0 : extra.extra);
        const promises = [];
        if (!level || level === 'INFO') {
            promises.push(logger.info(message, contextExtra));
        }
        else if (level === 'DEBUG') {
            promises.push(logger.debug(message, contextExtra));
        }
        else if (level === 'WARN') {
            promises.push(logger.warn(message, extra === null || extra === void 0 ? void 0 : extra.error, contextExtra));
        }
        else if (level === 'ERROR') {
            promises.push(logger.error(message, extra === null || extra === void 0 ? void 0 : extra.error, contextExtra));
        }
        // FIXME: this is a hack to get around the fact that the log level is not typed
        promises.push(this.runtime.putLog(taskRunExternalId, message, level, this.retryCount(), extra === null || extra === void 0 ? void 0 : extra.extra));
        return Promise.all(promises);
    }
    get logger() {
        return {
            info: (message, extra) => {
                return this.log(message, 'INFO', { extra });
            },
            debug: (message, extra) => {
                return this.log(message, 'DEBUG', { extra });
            },
            warn: (message, extra) => {
                return this.log(message, 'WARN', extra);
            },
            error: (message, extra) => {
                return this.log(message, 'ERROR', extra);
            },
            util: (key, message, extra) => {
                const logger = this.runtime.logger('ctx');
                if (!logger.util) {
                    return Promise.resolve();
                }
                return logger.util(key, message, extra === null || extra === void 0 ? void 0 : extra.extra);
            },
        };
    }
    /**
     * Refreshes the timeout for the current task.
     * @param incrementBy - The interval by which to increment the timeout.
     * The interval should be specified in the format of '10s' for 10 seconds, '1m' for 1 minute, or '1d' for 1 day.
     */
    refreshTimeout(incrementBy) {
        return __awaiter(this, void 0, void 0, function* () {
            const { taskRunExternalId } = this.action;
            if (!taskRunExternalId) {
                // log a warning
                this._logger.warn('cannot refresh timeout from context without stepRunId');
                return;
            }
            yield this.runtime.refreshTimeout(taskRunExternalId, (0, duration_1.durationToString)(incrementBy));
        });
    }
    /**
     * Releases a worker slot for a task run such that the worker can pick up another task.
     * Note: this is an advanced feature that may lead to unexpected behavior if used incorrectly.
     * @returns A promise that resolves when the slot has been released.
     */
    releaseSlot() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.runtime.releaseSlot(this.action.taskRunExternalId);
        });
    }
    /**
     * Streams data from the current task run.
     * @param data - The data to stream (string or binary).
     * @returns A promise that resolves when the data has been streamed.
     */
    putStream(data) {
        return __awaiter(this, void 0, void 0, function* () {
            const { taskRunExternalId } = this.action;
            if (!taskRunExternalId) {
                // log a warning
                this._logger.warn('cannot log from context without stepRunId');
                return;
            }
            const index = this._incrementStreamIndex();
            yield this.runtime.putStream(taskRunExternalId, data, index);
        });
    }
    spawnOptions(workflow, options) {
        this.throwIfCancelled();
        let workflowName;
        if (typeof workflow === 'string') {
            workflowName = workflow;
        }
        else {
            workflowName = workflow.name;
        }
        const opts = options || {};
        const { sticky } = opts;
        if (sticky && !this.worker.hasWorkflow(workflowName)) {
            throw new hatchet_error_1.default(`Cannot run with sticky: workflow ${workflowName} is not registered on the worker`);
        }
        const { workflowRunId, taskRunExternalId } = this.action;
        const childIndex = this.nextChildIndex();
        const finalOpts = Object.assign(Object.assign({}, opts), { parentId: workflowRunId, parentTaskRunExternalId: taskRunExternalId, childIndex, childKey: options === null || options === void 0 ? void 0 : options.key, desiredWorkerId: sticky ? this.worker.id() : undefined, _standaloneTaskName: workflow instanceof declaration_1.TaskWorkflowDeclaration ? workflow._standalone_task_name : undefined });
        return { workflowName, opts: finalOpts };
    }
    spawn(workflow, input, options) {
        const { workflowName, opts } = this.spawnOptions(workflow, options);
        return this.runtime.runWorkflow(workflowName, input, opts);
    }
    spawnBulk(children) {
        this.throwIfCancelled();
        const workflows = children.map((child) => {
            const { workflowName, opts } = this.spawnOptions(child.workflow, child.options);
            return { workflowName, input: child.input, options: opts };
        });
        return this.runtime.runWorkflows(workflows);
    }
    /**
     * Runs multiple children workflows in parallel without waiting for their results.
     * @param children - An array of  objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of workflow run references to the enqueued runs.
     */
    bulkRunNoWaitChildren(children) {
        return __awaiter(this, void 0, void 0, function* () {
            const refs = yield this.spawnBulk(children);
            refs.forEach((ref) => {
                ref.defaultSignal = this.abortController.signal;
            });
            return refs;
        });
    }
    /**
     * Runs multiple children workflows in parallel and waits for all results.
     * @param children - An array of objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of results from the children workflows.
     */
    bulkRunChildren(children) {
        return __awaiter(this, void 0, void 0, function* () {
            const runs = yield this.bulkRunNoWaitChildren(children);
            return Promise.all(runs.map((run) => run.output));
        });
    }
    /**
     * Runs a new workflow and waits for its result.
     *
     * @param workflow - The workflow to run (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - An options object containing key, sticky, priority, and additionalMetadata.
     * @returns The result of the workflow.
     */
    runChild(workflow, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const run = yield this.spawn(workflow, input, options);
            // Ensure waiting for the child result aborts when this task is cancelled.
            run.defaultSignal = this.abortController.signal;
            return run.output;
        });
    }
    /**
     * Enqueues a new workflow without waiting for its result.
     *
     * @param workflow - The workflow to enqueue (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - An options object containing key, sticky, priority, and additionalMetadata.
     * @returns A reference to the spawned workflow run.
     */
    runNoWaitChild(workflow, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const ref = yield this.spawn(workflow, input, options);
            ref.defaultSignal = this.abortController.signal;
            return ref;
        });
    }
    /**
     * Retrieves additional metadata associated with the current workflow run.
     * @returns A record of metadata key-value pairs.
     */
    additionalMetadata() {
        if (!this.action.additionalMetadata) {
            return {};
        }
        // parse the additional metadata
        const res = (0, parse_1.parseJSON)(this.action.additionalMetadata);
        return res;
    }
    /**
     * Gets the index of this workflow if it was spawned as part of a bulk operation.
     * @returns The child index number, or undefined if not set.
     */
    childIndex() {
        return this.action.childWorkflowIndex;
    }
    /**
     * Gets the key associated with this workflow if it was spawned as a child workflow.
     * @returns The child key, or undefined if not set.
     */
    childKey() {
        return this.action.childWorkflowKey;
    }
    /**
     * Gets the ID of the parent workflow run if this workflow was spawned as a child.
     * @returns The parent workflow run ID, or undefined if not a child workflow.
     */
    parentWorkflowRunId() {
        return this.action.parentWorkflowRunId;
    }
    priority() {
        switch (this.action.priority) {
            case 1:
                return declaration_1.Priority.LOW;
            case 2:
                return declaration_1.Priority.MEDIUM;
            case 3:
                return declaration_1.Priority.HIGH;
            default:
                return undefined;
        }
    }
    /**
     * Gets the external ID of the event that triggered this workflow run, if any.
     * @returns The triggering event external ID, or undefined if the workflow was not triggered by an event.
     */
    triggeringEventId() {
        return this.action.triggeringEventExternalId;
    }
    /**
     * Gets the key of the event that triggered this workflow run, if any.
     * @returns The triggering event key, or undefined if the workflow was not triggered by an event.
     */
    triggeringEventKey() {
        return this.action.triggeringEventKey;
    }
    // FIXME: drop these at some point soon
    /**
     * Get the output of a task.
     * @param task - The name of the task to get the output for.
     * @returns The output of the task.
     * @throws An error if the task output is not found.
     * @deprecated use ctx.parentOutput instead
     * @hidden
     */
    stepOutput(step) {
        if (!this.data.parents) {
            throw new hatchet_error_1.default('Parent task outputs not found');
        }
        if (!this.data.parents[step]) {
            throw new hatchet_error_1.default(`Output for parent task '${step}' not found`);
        }
        return this.data.parents[step];
    }
    /**
     * Gets the input data for the current workflow.
     * @returns The input data for the workflow.
     * @deprecated use task input parameter instead
     * @hidden
     */
    workflowInput() {
        return this.input;
    }
    /**
     * Gets the name of the current task.
     * @returns The name of the task.
     * @deprecated use ctx.taskName instead
     * @hidden
     */
    stepName() {
        return this.taskName();
    }
    /**
     * Spawns multiple workflows.
     *
     * @param workflows - An array of objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of references to the spawned workflow runs.
     * @deprecated Use bulkRunNoWaitChildren or bulkRunChildren instead.
     * @hidden
     */
    spawnWorkflows(workflows) {
        return __awaiter(this, void 0, void 0, function* () {
            this.throwIfCancelled();
            const { workflowRunId, taskRunExternalId } = this.action;
            const workflowRuns = workflows.map(({ workflow, input, options }) => {
                let workflowName;
                if (typeof workflow === 'string') {
                    workflowName = workflow;
                }
                else {
                    workflowName = workflow.name;
                }
                const name = (0, apply_namespace_1.applyNamespace)(workflowName, this.runtime.namespace).toLowerCase();
                const opts = options || {};
                const { sticky } = opts;
                if (sticky && !this.worker.hasWorkflow(name)) {
                    throw new hatchet_error_1.default(`Cannot run with sticky: workflow ${name} is not registered on the worker`);
                }
                // `signal` must never be sent over the wire.
                const optsWithoutSignal = Object.assign({}, opts);
                delete optsWithoutSignal.signal;
                const childIndex = this.nextChildIndex();
                const resp = {
                    workflowName: name,
                    input,
                    options: Object.assign(Object.assign({}, optsWithoutSignal), { parentId: workflowRunId, parentTaskRunExternalId: taskRunExternalId, childIndex, desiredWorkerId: sticky ? this.worker.id() : undefined }),
                };
                return resp;
            });
            try {
                const batchSize = 10;
                let resp = [];
                for (let i = 0; i < workflowRuns.length; i += batchSize) {
                    const batch = workflowRuns.slice(i, i + batchSize);
                    const batchResp = yield this.runtime.runWorkflows(batch);
                    resp = resp.concat(batchResp);
                }
                const res = [];
                resp.forEach((ref, index) => {
                    const wf = workflows[index].workflow;
                    if (wf instanceof declaration_1.TaskWorkflowDeclaration) {
                        ref._standaloneTaskName = wf._standalone_task_name;
                    }
                    res.push(ref);
                });
                return resp;
            }
            catch (e) {
                throw new hatchet_error_1.default(e.message);
            }
        });
    }
    /**
     * Spawns a new workflow.
     *
     * @param workflow - The workflow to spawn (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - Additional options for spawning the workflow.
     * @returns A reference to the spawned workflow run.
     * @deprecated Use runChild or runNoWaitChild instead.
     * @hidden
     */
    spawnWorkflow(workflow, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            this.throwIfCancelled();
            const { workflowRunId, taskRunExternalId } = this.action;
            const workflowName = typeof workflow === 'string' ? workflow : workflow.name;
            const name = (0, apply_namespace_1.applyNamespace)(workflowName, this.runtime.namespace).toLowerCase();
            const opts = options || {};
            const { sticky } = opts;
            if (sticky && !this.worker.hasWorkflow(name)) {
                throw new hatchet_error_1.default(`cannot run with sticky: workflow ${name} is not registered on the worker`);
            }
            try {
                const childIndex = this.nextChildIndex();
                const resp = yield this.runtime.runWorkflow(name, input, Object.assign({ parentId: workflowRunId, parentTaskRunExternalId: taskRunExternalId, childIndex, desiredWorkerId: sticky ? this.worker.id() : undefined }, opts));
                if (workflow instanceof declaration_1.TaskWorkflowDeclaration) {
                    resp._standaloneTaskName = workflow._standalone_task_name;
                }
                return resp;
            }
            catch (e) {
                throw new hatchet_error_1.default(e.message);
            }
        });
    }
    _incrementStreamIndex() {
        const index = this.streamIndex;
        this.streamIndex += 1;
        return index;
    }
}
exports.Context = Context;
/**
 * DurableContext provides helper methods and useful data to durable tasks at runtime.
 * It extends the Context class and includes additional methods for durable execution like sleepFor and waitFor.
 */
class DurableContext extends Context {
    _serializeSendEvent(send) {
        // Re-check cancellation when the queued send actually executes: once this
        // invocation is aborted (evicted), a queued send must not reach the
        // server, where it could race the eviction and append to the event log
        // out of recorded order. Python gets this for free from task
        // cancellation killing coroutines queued on the send lock.
        const runSend = () => {
            this.throwIfCancelled();
            return send();
        };
        const result = this._sendEventLock.then(runSend, runSend);
        this._sendEventLock = result.then(() => undefined, () => undefined);
        return result;
    }
    constructor(action, runtimeOrClient, workerOrTransport, listenerOrOptions, evictionManager, engineVersion) {
        // The base constructor tells a runtime apart from a client itself, so the worker
        // argument is ignored on the runtime path.
        super(action, runtimeOrClient, workerOrTransport);
        this._waitKey = 0;
        // Serializes sendEvent calls from concurrent coroutines in this invocation.
        // The listener keys pending acks by (task, invocation), so overlapping sends
        // would overwrite each other's ack and cross-wire branch/node assignments.
        this._sendEventLock = Promise.resolve();
        if ((0, runtime_1.isContextRuntime)(runtimeOrClient)) {
            const options = listenerOrOptions;
            this._durableListener = workerOrTransport;
            this._evictionManager = options === null || options === void 0 ? void 0 : options.evictionManager;
            this._engineVersion = options === null || options === void 0 ? void 0 : options.engineVersion;
        }
        else {
            this._durableListener = listenerOrOptions;
            this._evictionManager = evictionManager;
            this._engineVersion = engineVersion;
        }
    }
    get supportsEviction() {
        return (0, engine_version_1.supportsEviction)(this._engineVersion);
    }
    /** The transport durable events travel over. */
    get durableListener() {
        return this._durableListener;
    }
    /**
     * The invocation count for the current durable task. Used for deduplication across replays.
     */
    get invocationCount() {
        var _a;
        return (_a = this.action.durableTaskInvocationCount) !== null && _a !== void 0 ? _a : 1;
    }
    get _actionKey() {
        return this.action.key;
    }
    withEvictionWait(waitKind, resourceId, fn) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            (_a = this._evictionManager) === null || _a === void 0 ? void 0 : _a.markWaiting(this._actionKey, waitKind, resourceId);
            try {
                return yield fn();
            }
            finally {
                (_b = this._evictionManager) === null || _b === void 0 ? void 0 : _b.markActive(this._actionKey);
            }
        });
    }
    sleepFor(duration, readableDataKeyOrOptions, label) {
        return __awaiter(this, void 0, void 0, function* () {
            const opts = typeof readableDataKeyOrOptions === 'string'
                ? { readableDataKey: readableDataKeyOrOptions, label }
                : (readableDataKeyOrOptions !== null && readableDataKeyOrOptions !== void 0 ? readableDataKeyOrOptions : {});
            const res = yield this.waitFor({ sleepFor: duration, readableDataKey: opts.readableDataKey }, opts.label);
            const matches = res['CREATE'] || {};
            const [firstMatch] = Object.values(matches);
            if (!firstMatch || firstMatch.length === 0) {
                return { durationMs: (0, duration_1.durationToMs)(duration) };
            }
            const [sleep] = firstMatch;
            const sleepDuration = sleep === null || sleep === void 0 ? void 0 : sleep.sleep_duration;
            if (sleepDuration) {
                const DURATION_RE = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/;
                const match = sleepDuration.match(DURATION_RE);
                if (match) {
                    const [, h, m, s] = match;
                    const ms = (parseInt(h !== null && h !== void 0 ? h : '0', 10) * 3600 + parseInt(m !== null && m !== void 0 ? m : '0', 10) * 60 + parseInt(s !== null && s !== void 0 ? s : '0', 10)) *
                        1000;
                    return { durationMs: ms };
                }
            }
            return { durationMs: (0, duration_1.durationToMs)(duration) };
        });
    }
    /**
     * Pauses execution until the specified conditions are met.
     * Conditions are "global" meaning they will wait in real time regardless of transient failures like worker restarts.
     * @param conditions - The conditions to wait for.
     * @returns A promise that resolves with the event that satisfied the conditions.
     */
    waitFor(conditions, label) {
        return __awaiter(this, void 0, void 0, function* () {
            this.throwIfCancelled();
            if (!this.supportsEviction) {
                return this._waitForPreEviction(conditions);
            }
            const rendered = (0, conditions_1.Render)(condition_1.Action.CREATE, conditions);
            const pbConditions = (0, transformer_1.conditionsToPb)(rendered, this.runtime.namespace);
            const ack = yield this._serializeSendEvent(() => this._durableListener.sendEvent(this.action.taskRunExternalId, this.invocationCount, {
                kind: 'waitFor',
                waitForConditions: {
                    sleepConditions: pbConditions.sleepConditions,
                    userEventConditions: pbConditions.userEventConditions,
                },
                label,
            }));
            const resourceId = rendered
                .map((c) => c.base.readableDataKey)
                .filter(Boolean)
                .join(',') || `node:${ack.nodeId}`;
            return this.withEvictionWait('waitFor', resourceId, () => __awaiter(this, void 0, void 0, function* () {
                const result = yield this._durableListener.waitForCallback(this.action.taskRunExternalId, this.invocationCount, ack.branchId, ack.nodeId, { signal: this.abortController.signal });
                return result.payload || {};
            }));
        });
    }
    waitForEvent(key, expression, payloadSchema, scope, lookbackWindow, label) {
        return __awaiter(this, void 0, void 0, function* () {
            const considerEventsSince = lookbackWindow
                ? new Date((yield this.now()).getTime() - (0, duration_1.durationToMs)(lookbackWindow)).toISOString()
                : undefined;
            const res = yield this.waitFor({
                eventKey: key,
                expression,
                scope,
                considerEventsSince,
            }, label);
            // The engine returns an object like:
            // {"CREATE": {"signal_key_1": [{"id": ..., "data": {...}}]}}
            // Since we have a single match, the list will only have one item.
            const matches = res['CREATE'] || {};
            const [firstMatch] = Object.values(matches);
            if (!firstMatch || firstMatch.length === 0) {
                if (payloadSchema) {
                    return payloadSchema.parse({});
                }
                return {};
            }
            const [rawPayload] = firstMatch;
            if (payloadSchema) {
                return payloadSchema.parse(rawPayload);
            }
            return rawPayload;
        });
    }
    /**
     * Durably sleep until a specific timestamp.
     * Uses the memoized `now()` to compute the remaining duration, then delegates to `sleepFor`.
     *
     * @param wakeAt - The timestamp to sleep until.
     * @returns A SleepResult containing the actual duration slept.
     */
    sleepUntil(wakeAt) {
        return __awaiter(this, void 0, void 0, function* () {
            const now = yield this.now();
            const remainingMs = wakeAt.getTime() - now.getTime();
            return this.sleepFor(`${Math.max(0, Math.ceil(remainingMs / 1000))}s`);
        });
    }
    /**
     * Get the current timestamp, memoized across replays. Returns the same Date on every replay of the same task run.
     * @returns The memoized current timestamp.
     */
    now() {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield this.memo(() => __awaiter(this, void 0, void 0, function* () {
                return { ts: new Date().toISOString() };
            }), ['now']);
            return new Date(result.ts);
        });
    }
    _waitForPreEviction(conditions) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!(0, pre_eviction_1.isLegacyDurableTransport)(this._durableListener)) {
                throw new hatchet_error_1.default(`Engine ${this._engineVersion || 'unknown'} does not support durable eviction and the durable transport has no legacy fallback. Upgrade the Hatchet engine.`);
            }
            const { result, nextWaitKey } = yield (0, pre_eviction_1.waitForPreEviction)(this._durableListener, this.action.taskRunExternalId, this._waitKey, conditions, this.runtime.namespace, this.abortController.signal);
            this._waitKey = nextWaitKey;
            return result;
        });
    }
    _buildTriggerOpts(workflow, input, options) {
        let workflowName;
        if (typeof workflow === 'string') {
            workflowName = workflow;
        }
        else {
            workflowName = workflow.name;
        }
        workflowName = (0, apply_namespace_1.applyNamespace)(workflowName, this.runtime.namespace).toLowerCase();
        const childIndex = this.nextChildIndex();
        const triggerOpts = {
            name: workflowName,
            input: JSON.stringify(input || {}),
            parentId: this.action.workflowRunId,
            parentTaskRunExternalId: this.action.taskRunExternalId,
            childIndex,
            childKey: options === null || options === void 0 ? void 0 : options.key,
            additionalMetadata: (options === null || options === void 0 ? void 0 : options.additionalMetadata)
                ? JSON.stringify(options.additionalMetadata)
                : undefined,
            desiredWorkerId: (options === null || options === void 0 ? void 0 : options.sticky) ? this.worker.id() : undefined,
            priority: options === null || options === void 0 ? void 0 : options.priority,
            desiredWorkerLabels: {},
        };
        return { workflowName, triggerOpts };
    }
    /**
     * Spawns a child workflow through the durable event log, waits for the child to complete.
     * @param workflow - The workflow to spawn.
     * @param input - The input data for the child workflow.
     * @param options - Options for spawning the child workflow.
     * @returns The result of the child workflow.
     */
    spawnChild(workflow, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.supportsEviction) {
                const { workflowName, opts } = this.spawnOptions(workflow, options);
                const ref = yield this.runtime.runWorkflow(workflowName, (input || {}), opts);
                ref.defaultSignal = this.abortController.signal;
                return ref.output;
            }
            const results = yield this.spawnChildren([
                { workflow, input: (input || {}), options },
            ]);
            return results[0];
        });
    }
    /**
     * Spawns multiple child workflows through the durable event log, waits for all to complete.
     * @param children - An array of objects containing the workflow, input, and options for each child.
     * @returns A list of results from the child workflows.
     */
    spawnChildren(children) {
        return __awaiter(this, void 0, void 0, function* () {
            this.throwIfCancelled();
            if (!this.supportsEviction) {
                const workflows = children.map((c) => {
                    const { workflowName, opts } = this.spawnOptions(c.workflow, c.options);
                    return { workflowName, input: c.input, options: opts };
                });
                const refs = yield this.runtime.runWorkflows(workflows);
                for (const r of refs) {
                    r.defaultSignal = this.abortController.signal;
                }
                return Promise.all(refs.map((r) => r.output));
            }
            const triggerOptsList = children.map((child) => {
                const { triggerOpts } = this._buildTriggerOpts(child.workflow, child.input, child.options);
                return triggerOpts;
            });
            const ack = yield this._serializeSendEvent(() => this._durableListener.sendEvent(this.action.taskRunExternalId, this.invocationCount, {
                kind: 'runChildren',
                triggerOpts: triggerOptsList,
            }));
            const results = yield Promise.all(ack.runEntries.map((entry) => this.withEvictionWait('runChild', `workflow:bulk-child`, () => __awaiter(this, void 0, void 0, function* () {
                const result = yield this._durableListener.waitForCallback(this.action.taskRunExternalId, this.invocationCount, entry.branchId, entry.nodeId, { signal: this.abortController.signal });
                if (result.isFailure) {
                    throw new hatchet_error_1.default(result.errorMessage || 'child task failed');
                }
                return (result.payload || {});
            }))));
            return results;
        });
    }
    /**
     * Memoize a function by storing its result in durable storage. Avoids recomputation on replay.
     *
     * @param fn - The async function to compute the value.
     * @param deps - Dependency values that form the memoization key.
     * @returns The memoized value, either from durable storage or freshly computed.
     */
    memo(fn, deps) {
        return __awaiter(this, void 0, void 0, function* () {
            this.throwIfCancelled();
            if (!this.supportsEviction) {
                return fn();
            }
            const memoKey = yield computeMemoKey(this.action.taskRunExternalId, deps);
            const ack = yield this._serializeSendEvent(() => this._durableListener.sendEvent(this.action.taskRunExternalId, this.invocationCount, {
                kind: 'memo',
                memoKey,
            }));
            this._durableListener.consumeCallbackWithoutBlocking(this.action.taskRunExternalId, this.invocationCount, ack.branchId, ack.nodeId);
            if (ack.memoAlreadyExisted && ack.memoResultPayload && ack.memoResultPayload.length > 0) {
                const serialized = new TextDecoder().decode(ack.memoResultPayload);
                return JSON.parse(serialized);
            }
            const result = yield fn();
            const serializedResult = new TextEncoder().encode(JSON.stringify(result));
            yield this._durableListener.sendMemoCompletedNotification(this.action.taskRunExternalId, ack.nodeId, ack.branchId, this.invocationCount, memoKey, serializedResult);
            return result;
        });
    }
}
exports.DurableContext = DurableContext;
/**
 * Derives the memo key for a task run and its dependency values: the SHA-256 of the task
 * run id followed by the JSON-serialised dependencies, over WebCrypto so it works in
 * every runtime. The bytes are identical to the previous Node `createHash` version, so
 * event logs recorded by older SDKs keep replaying.
 */
function computeMemoKey(taskRunExternalId, args) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        const { subtle } = (_a = globalThis.crypto) !== null && _a !== void 0 ? _a : {};
        if (!subtle) {
            throw new hatchet_error_1.default('WebCrypto is not available in this runtime. Durable tasks need globalThis.crypto.subtle (Node 20 or newer).');
        }
        const data = new TextEncoder().encode(taskRunExternalId + JSON.stringify(args));
        return new Uint8Array(yield subtle.digest('SHA-256', data));
    });
}
