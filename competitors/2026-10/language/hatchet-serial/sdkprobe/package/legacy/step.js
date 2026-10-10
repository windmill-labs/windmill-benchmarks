"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
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
exports.V0DurableContext = exports.V0Context = exports.V0ContextWorker = exports.CreateStepSchema = exports.DesiredWorkerLabelSchema = exports.CreateRateLimitSchema = void 0;
exports.mapRateLimit = mapRateLimit;
const hatchet_error_1 = __importDefault(require("../util/errors/hatchet-error"));
const z = __importStar(require("zod/v4"));
const parse_1 = require("../util/parse");
const workflows_1 = require("../protoc/workflows");
const v1_1 = require("../v1");
const task_1 = require("../v1/task");
const declaration_1 = require("../v1/declaration");
const conditions_1 = require("../v1/conditions");
const condition_1 = require("../protoc/v1/shared/condition");
const transformer_1 = require("../v1/conditions/transformer");
const duration_1 = require("../v1/client/duration");
const apply_namespace_1 = require("../util/apply-namespace");
exports.CreateRateLimitSchema = z.object({
    key: z.string().optional(),
    staticKey: z.string().optional(),
    dynamicKey: z.string().optional(),
    units: z.union([z.number().min(0), z.string()]),
    limit: z.union([z.number().min(1), z.string()]).optional(),
    duration: z.enum(workflows_1.RateLimitDuration).optional(),
});
exports.DesiredWorkerLabelSchema = z
    .union([
    z.string(),
    z.number().int(),
    z.object({
        value: z.union([z.string(), z.number()]),
        required: z.boolean().optional(),
        weight: z.number().int().optional(),
        // (optional) comparator for the label
        // if not provided, the default is EQUAL
        // desired COMPARATOR actual (i.e. desired > actual for GREATER_THAN)
        comparator: z.enum(task_1.WorkerLabelComparator).optional(),
    }),
])
    .optional();
exports.CreateStepSchema = z.object({
    name: z.string(),
    parents: z.array(z.string()).optional(),
    timeout: z.string().optional(),
    retries: z.number().optional(),
    rate_limits: z.array(exports.CreateRateLimitSchema).optional(),
    worker_labels: z
        .record(z.any(), z.lazy(() => exports.DesiredWorkerLabelSchema))
        .optional(),
    backoff: z
        .object({
        factor: z.number().optional(),
        maxSeconds: z.number().optional(),
    })
        .optional(),
});
class V0ContextWorker {
    constructor(worker) {
        this.worker = worker;
    }
    /**
     * Gets the ID of the worker.
     * @returns The ID of the worker.
     */
    id() {
        return this.worker.workerId;
    }
    /**
     * Checks if the worker has a registered workflow.
     * @param workflowName - The name of the workflow to check.
     * @returns True if the workflow is registered, otherwise false.
     */
    hasWorkflow(workflowName) {
        return !!this.worker.workflow_registry.find((workflow) => 'id' in workflow ? workflow.id === workflowName : workflow.name === workflowName);
    }
    /**
     * Gets the current state of the worker labels.
     * @returns The labels of the worker.
     */
    labels() {
        return this.worker.labels;
    }
    /**
     * Upserts the a set of labels on the worker.
     * @param labels - The labels to upsert.
     * @returns A promise that resolves when the labels have been upserted.
     */
    upsertLabels(labels) {
        return this.worker.upsertLabels(labels);
    }
}
exports.V0ContextWorker = V0ContextWorker;
class V0Context {
    constructor(action, client, worker) {
        // @deprecated use ctx.abortController instead
        this.controller = new AbortController();
        this.overridesData = {};
        this.spawnIndex = 0;
        try {
            const data = (0, parse_1.parseJSON)(action.actionPayload);
            this.data = data;
            this.action = action;
            this.v0 = client;
            this.worker = new V0ContextWorker(worker);
            this.logger = client.config.logger(`Context Logger`, client.config.log_level);
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
    /**
     * Retrieves the output of a parent task.
     * @param parentTask - The a CreateTaskOpts or string of the parent task name.
     * @returns The output of the specified parent task.
     * @throws An error if the task output is not found.
     */
    parentOutput(parentTask) {
        return __awaiter(this, void 0, void 0, function* () {
            // NOTE: parentOutput is async since we plan on potentially making this a cacheable server call
            if (typeof parentTask === 'string') {
                return this.stepOutput(parentTask);
            }
            return this.stepOutput(parentTask.name);
        });
    }
    /**
     * Get the output of a task.
     * @param task - The name of the task to get the output for.
     * @returns The output of the task.
     * @throws An error if the task output is not found.
     * @deprecated use ctx.parentOutput instead
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
     * Returns errors from any task runs in the workflow.
     * @returns A record mapping task names to error messages.
     * @throws A warning if no errors are found (this method should be used in on-failure tasks).
     * @deprecated use ctx.errors() instead
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
            this.logger.error('No run errors found. `ctx.errors` is intended to be run in an on-failure task, and will only work on engine versions more recent than v0.53.10');
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
     * Determines if the workflow was triggered by an event.
     * @returns True if the workflow was triggered by an event, otherwise false.
     */
    triggeredByEvent() {
        var _a;
        return ((_a = this.data) === null || _a === void 0 ? void 0 : _a.triggered_by) === 'event';
    }
    /**
     * Gets the input data for the current workflow.
     * @returns The input data for the workflow.
     * @deprecated use task input parameter instead
     */
    workflowInput() {
        return this.input;
    }
    /**
     * Gets the name of the current workflow.
     * @returns The name of the workflow.
     */
    workflowName() {
        return this.action.jobName;
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
     * Gets the name of the current task.
     * @returns The name of the task.
     * @deprecated use ctx.taskName instead
     */
    stepName() {
        return this.taskName();
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
     * Gets the ID of the current task run.
     * @returns The task run ID.
     */
    taskRunExternalId() {
        return this.action.taskRunExternalId;
    }
    /**
     * Gets the ID of the current task run.
     * @returns The task run ID.
     * @deprecated use taskRunExternalId instead
     */
    taskRunId() {
        return this.action.taskRunExternalId;
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
     */
    log(message, level) {
        const { taskRunExternalId } = this.action;
        if (!taskRunExternalId) {
            // log a warning
            this.logger.warn('cannot log from context without stepRunId');
            return;
        }
        this.v0.event.putLog(taskRunExternalId, message, level, this.retryCount());
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
                this.logger.warn('cannot refresh timeout from context without stepRunId');
                return;
            }
            yield this.v0.dispatcher.refreshTimeout((0, duration_1.durationToString)(incrementBy), taskRunExternalId);
        });
    }
    /**
     * Releases a worker slot for a task run such that the worker can pick up another task.
     * Note: this is an advanced feature that may lead to unexpected behavior if used incorrectly.
     * @returns A promise that resolves when the slot has been released.
     */
    releaseSlot() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.v0.dispatcher.client.releaseSlot({
                taskRunExternalId: this.action.taskRunExternalId,
            });
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
                this.logger.warn('cannot log from context without stepRunId');
                return;
            }
            yield this.v0.event.putStream(taskRunExternalId, data, undefined);
        });
    }
    /**
     * Runs multiple children workflows in parallel without waiting for their results.
     * @param children - An array of  objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of workflow run references to the enqueued runs.
     */
    bulkRunNoWaitChildren(children) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.spawnWorkflows(children);
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
     * Spawns multiple workflows.
     *
     * @param workflows - An array of objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of references to the spawned workflow runs.
     * @deprecated Use bulkRunNoWaitChildren or bulkRunChildren instead.
     */
    spawnWorkflows(workflows) {
        return __awaiter(this, void 0, void 0, function* () {
            const { workflowRunId, taskRunExternalId } = this.action;
            const workflowRuns = workflows.map(({ workflow, input, options }) => {
                let workflowName;
                if (typeof workflow === 'string') {
                    workflowName = workflow;
                }
                else {
                    workflowName = workflow.id;
                }
                const name = (0, apply_namespace_1.applyNamespace)(workflowName, this.v0.config.namespace);
                const opts = options || {};
                const { sticky } = opts;
                if (sticky && !this.worker.hasWorkflow(name)) {
                    throw new hatchet_error_1.default(`Cannot run with sticky: workflow ${name} is not registered on the worker`);
                }
                const resp = {
                    workflowName: name,
                    input,
                    options: Object.assign(Object.assign({}, opts), { parentId: workflowRunId, parentStepRunId: taskRunExternalId, childIndex: this.spawnIndex, desiredWorkerId: sticky ? this.worker.id() : undefined }),
                };
                this.spawnIndex += 1;
                return resp;
            });
            try {
                const batchSize = 100;
                let resp = [];
                for (let i = 0; i < workflowRuns.length; i += batchSize) {
                    const batch = workflowRuns.slice(i, i + batchSize);
                    const batchResp = yield this.v0.admin.runWorkflows(batch);
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
     * Runs a new workflow and waits for its result.
     *
     * @param workflow - The workflow to run (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - An options object containing key, sticky, priority, and additionalMetadata.
     * @returns The result of the workflow.
     */
    runChild(workflow, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const run = yield this.spawnWorkflow(workflow, input, options);
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
            return this.spawnWorkflow(workflow, input, options);
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
     */
    spawnWorkflow(workflow, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const { workflowRunId, taskRunExternalId } = this.action;
            const workflowName = typeof workflow === 'string' ? workflow : workflow.id;
            const name = (0, apply_namespace_1.applyNamespace)(workflowName, this.v0.config.namespace);
            const opts = options || {};
            const { sticky } = opts;
            if (sticky && !this.worker.hasWorkflow(name)) {
                throw new hatchet_error_1.default(`cannot run with sticky: workflow ${name} is not registered on the worker`);
            }
            try {
                const resp = yield this.v0.admin.runWorkflow(name, input, Object.assign({ parentId: workflowRunId, parentStepRunId: taskRunExternalId, childIndex: this.spawnIndex, desiredWorkerId: sticky ? this.worker.id() : undefined }, opts));
                this.spawnIndex += 1;
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
                return v1_1.Priority.LOW;
            case 2:
                return v1_1.Priority.MEDIUM;
            case 3:
                return v1_1.Priority.HIGH;
            default:
                return undefined;
        }
    }
}
exports.V0Context = V0Context;
class V0DurableContext extends V0Context {
    constructor() {
        super(...arguments);
        this.waitKey = 0;
    }
    /**
     * Pauses execution for the specified duration.
     * Duration is "global" meaning it will wait in real time regardless of transient failures like worker restarts.
     * @param duration - The duration to sleep for.
     * @returns A promise that resolves when the sleep duration has elapsed.
     */
    sleepFor(duration, readableDataKey) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.waitFor({ sleepFor: duration, readableDataKey });
        });
    }
    /**
     * Pauses execution until the specified conditions are met.
     * Conditions are "global" meaning they will wait in real time regardless of transient failures like worker restarts.
     * @param conditions - The conditions to wait for.
     * @returns A promise that resolves with the event that satisfied the conditions.
     */
    waitFor(conditions) {
        return __awaiter(this, void 0, void 0, function* () {
            const pbConditions = (0, transformer_1.conditionsToPb)((0, conditions_1.Render)(condition_1.Action.CREATE, conditions), this.v0.config.namespace);
            const key = `waitFor-${this.waitKey++}`;
            yield this.v0.durableListener.registerDurableEvent({
                taskId: this.action.taskRunExternalId,
                signalKey: key,
                sleepConditions: pbConditions.sleepConditions,
                userEventConditions: pbConditions.userEventConditions,
            });
            const listener = this.v0.durableListener.subscribe({
                taskId: this.action.taskRunExternalId,
                signalKey: key,
            });
            const event = yield listener.get();
            // Convert event.data from Uint8Array to string if needed
            const eventData = event.data instanceof Uint8Array ? new TextDecoder().decode(event.data) : event.data;
            const res = JSON.parse(eventData);
            return res.CREATE;
        });
    }
}
exports.V0DurableContext = V0DurableContext;
function mapRateLimit(limits) {
    if (!limits) {
        return [];
    }
    return limits.map((l) => {
        let key = l.staticKey;
        const keyExpression = l.dynamicKey;
        if (l.key !== undefined) {
            console.warn('key is deprecated and will be removed in a future release, please use staticKey instead');
            ({ key } = l);
        }
        if (keyExpression !== undefined) {
            if (key !== undefined) {
                throw new Error('Cannot have both static key and dynamic key set');
            }
            key = keyExpression;
            if (!validateCelExpression(keyExpression)) {
                throw new Error(`Invalid CEL expression: ${keyExpression}`);
            }
        }
        if (key === undefined) {
            throw new Error(`Invalid key`);
        }
        let units;
        let unitsExpression;
        if (typeof l.units === 'number') {
            ({ units } = l);
        }
        else {
            if (!validateCelExpression(l.units)) {
                throw new Error(`Invalid CEL expression: ${l.units}`);
            }
            unitsExpression = l.units;
        }
        let limitExpression;
        if (l.limit !== undefined) {
            if (typeof l.limit === 'number') {
                limitExpression = `${l.limit}`;
            }
            else {
                if (!validateCelExpression(l.limit)) {
                    throw new Error(`Invalid CEL expression: ${l.limit}`);
                }
                limitExpression = l.limit;
            }
        }
        if (keyExpression !== undefined && limitExpression === undefined) {
            throw new Error('CEL based keys requires limit to be set');
        }
        if (limitExpression === undefined) {
            limitExpression = `-1`;
        }
        return {
            key,
            keyExpr: keyExpression,
            units,
            unitsExpr: unitsExpression,
            limitValuesExpr: limitExpression,
            duration: l.duration,
        };
    });
}
// Helper function to validate CEL expressions
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function validateCelExpression(expr) {
    // This is a placeholder. In a real implementation, you'd need to use a CEL parser or validator.
    // For now, we'll just return true to mimic the behavior.
    return true;
}
