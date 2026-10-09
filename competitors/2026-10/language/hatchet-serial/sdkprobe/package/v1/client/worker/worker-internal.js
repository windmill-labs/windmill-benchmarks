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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.InternalWorker = exports.taskConcurrencyArr = exports.resolveScheduleTimeout = exports.resolveExecutionTimeout = exports.mapSlotRequestsPb = exports.mapRateLimitPb = exports.mapConcurrencyPb = exports.mapBatchConfigPb = exports.assertValidConcurrencyArr = void 0;
const hatchet_error_1 = __importDefault(require("../../../util/errors/hatchet-error"));
const task_run_terminated_error_1 = require("../../../util/errors/task-run-terminated-error");
const action_1 = require("../../../clients/dispatcher/action");
const dispatcher_1 = require("../../../protoc/dispatcher");
const hatchet_promise_1 = __importStar(require("../../../util/hatchet-promise/hatchet-promise"));
const logger_1 = require("../../../util/logger");
const task_1 = require("../../task");
const apply_namespace_1 = require("../../../util/apply-namespace");
const sleep_1 = __importDefault(require("../../../util/sleep"));
const abort_error_1 = require("../../../util/abort-error");
const context_1 = require("./context");
const parent_run_context_vars_1 = require("../../parent-run-context-vars");
const parent_run_context_storage_1 = require("../../parent-run-context-storage");
const workflow_proto_1 = require("./workflow-proto");
const health_server_1 = require("./health-server");
const eviction_manager_1 = require("./eviction/eviction-manager");
const eviction_policy_1 = require("./eviction/eviction-policy");
const engine_version_1 = require("./engine-version");
var workflow_proto_2 = require("./workflow-proto");
Object.defineProperty(exports, "assertValidConcurrencyArr", { enumerable: true, get: function () { return workflow_proto_2.assertValidConcurrencyArr; } });
Object.defineProperty(exports, "mapBatchConfigPb", { enumerable: true, get: function () { return workflow_proto_2.mapBatchConfigPb; } });
Object.defineProperty(exports, "mapConcurrencyPb", { enumerable: true, get: function () { return workflow_proto_2.mapConcurrencyPb; } });
Object.defineProperty(exports, "mapRateLimitPb", { enumerable: true, get: function () { return workflow_proto_2.mapRateLimitPb; } });
Object.defineProperty(exports, "mapSlotRequestsPb", { enumerable: true, get: function () { return workflow_proto_2.mapSlotRequestsPb; } });
Object.defineProperty(exports, "resolveExecutionTimeout", { enumerable: true, get: function () { return workflow_proto_2.resolveExecutionTimeout; } });
Object.defineProperty(exports, "resolveScheduleTimeout", { enumerable: true, get: function () { return workflow_proto_2.resolveScheduleTimeout; } });
Object.defineProperty(exports, "taskConcurrencyArr", { enumerable: true, get: function () { return workflow_proto_2.taskConcurrencyArr; } });
// Tasks read the parent run context across awaits; the worker is the only place that
// enters it, so it installs the AsyncLocalStorage store before any task can run.
(0, parent_run_context_storage_1.installAsyncLocalParentRunContext)();
class InternalWorker {
    constructor(client, options) {
        var _a, _b, _c, _d;
        this.durable_action_set = new Set();
        this.eviction_policies = new Map();
        this.workflow_registry = [];
        this.futures = {};
        this.contexts = {};
        this.registeredWorkflowPromises = [];
        this.labels = {};
        this.status = health_server_1.workerStatus.INITIALIZED;
        this.client = client;
        this.name = (0, apply_namespace_1.applyNamespace)(options.name, this.client.config.namespace);
        this.action_registry = {};
        this.slots = options.slots;
        this.durableSlots = options.durableSlots;
        this.slotConfig = options.slotConfig || {};
        this.labels = options.labels || {};
        this.enableHealthServer = (_b = (_a = client.config.healthcheck) === null || _a === void 0 ? void 0 : _a.enabled) !== null && _b !== void 0 ? _b : false;
        this.healthPort = (_d = (_c = client.config.healthcheck) === null || _c === void 0 ? void 0 : _c.port) !== null && _d !== void 0 ? _d : 8001;
        this.killing = false;
        this.handle_kill = options.handleKill === undefined ? true : options.handleKill;
        process.on('SIGTERM', () => this.exitGracefully(this.handle_kill));
        process.on('SIGINT', () => this.exitGracefully(this.handle_kill));
        this.logger = client.config.logger(`Worker/${this.name}`, this.client.config.log_level);
        if (this.enableHealthServer && this.healthPort) {
            this.initializeHealthServer();
        }
    }
    initializeHealthServer() {
        if (!this.healthPort) {
            this.logger.warn('Health server enabled but no port specified');
            return;
        }
        this.healthServer = new health_server_1.HealthServer(this.healthPort, () => this.status, this.name, () => this.getAvailableSlots(), () => this.getRegisteredActions(), () => this.getFilteredLabels(), this.logger);
    }
    getAvailableSlots() {
        // sum all the slots in the slot config
        const totalSlots = Object.values(this.slotConfig).reduce((acc, curr) => acc + curr, 0);
        const currentRuns = Object.keys(this.futures).length;
        return Math.max(0, totalSlots - currentRuns);
    }
    getRegisteredActions() {
        return Object.keys(this.action_registry);
    }
    getFilteredLabels() {
        const filtered = {};
        for (const [key, value] of Object.entries(this.labels)) {
            if (value !== undefined) {
                filtered[key] = value;
            }
        }
        return filtered;
    }
    setStatus(status) {
        this.status = status;
        this.logger.debug(`Worker status changed to: ${status}`);
    }
    registerDurableActions(workflow) {
        const newActions = workflow._durableTasks
            .filter((task) => !!task.fn)
            .reduce((acc, task) => {
            const actionId = (0, action_1.createActionId)((0, apply_namespace_1.applyNamespace)(workflow.name, this.client.config.namespace), task.name);
            acc[actionId] = (ctx) => task.fn(ctx.input, ctx);
            this.durable_action_set.add(actionId);
            this.eviction_policies.set(actionId, task.evictionPolicy !== undefined
                ? task.evictionPolicy
                : eviction_policy_1.DEFAULT_DURABLE_TASK_EVICTION_POLICY);
            return acc;
        }, {});
        this.action_registry = Object.assign(Object.assign({}, this.action_registry), newActions);
    }
    registerActions(workflow) {
        const newActions = workflow._tasks
            .filter((task) => !!task.fn)
            .reduce((acc, task) => {
            const actionId = (0, action_1.createActionId)(workflow.name, task.name);
            if (task.batch) {
                acc[actionId] = (ctx) => task.fn(decodeBatchItems(ctx.action.actionPayload), ctx);
            }
            else {
                acc[actionId] = (ctx) => task.fn(ctx.input, ctx);
            }
            return acc;
        }, {});
        const onFailureFn = workflow.onFailure
            ? typeof workflow.onFailure === 'function'
                ? workflow.onFailure
                : workflow.onFailure.fn
            : undefined;
        const onFailureAction = onFailureFn
            ? {
                [(0, workflow_proto_1.onFailureTaskName)(workflow)]: (ctx) => onFailureFn(ctx.input, ctx),
            }
            : {};
        this.action_registry = Object.assign(Object.assign(Object.assign({}, this.action_registry), newActions), onFailureAction);
    }
    registerWorkflow(initWorkflow_1) {
        return __awaiter(this, arguments, void 0, function* (initWorkflow, durable = false) {
            const { namespace } = this.client.config;
            const workflow = (0, workflow_proto_1.normalizeWorkflowDefinition)(initWorkflow, { namespace, durable });
            try {
                const registeredWorkflow = this.client.admin.putWorkflow((0, workflow_proto_1.workflowToProto)(workflow, { namespace, durable }));
                this.registeredWorkflowPromises.push(registeredWorkflow);
                yield registeredWorkflow;
                this.workflow_registry.push(workflow);
            }
            catch (e) {
                throw new hatchet_error_1.default(`Could not register workflow: ${e.message}`);
            }
            this.registerActions(workflow);
        });
    }
    ensureEvictionManager() {
        var _a, _b, _c;
        if (this.evictionManager)
            return this.evictionManager;
        const totalDurableSlots = (_c = (_b = (_a = this.slotConfig) === null || _a === void 0 ? void 0 : _a.durable) !== null && _b !== void 0 ? _b : this.durableSlots) !== null && _c !== void 0 ? _c : 0;
        this.evictionManager = new eviction_manager_1.DurableEvictionManager({
            durableSlots: totalDurableSlots,
            cancelLocal: (key, invocationCount) => {
                var _a;
                const err = new task_run_terminated_error_1.TaskRunTerminatedError('evicted');
                const ctx = this.contexts[key];
                // A newer invocation may already be registered under this key (the
                // server restored the run before the eviction ack arrived); it must
                // not be aborted for the old invocation's eviction.
                const ctxMatchesEvictedInvocation = ctx && ((_a = ctx.invocationCount) !== null && _a !== void 0 ? _a : 1) === invocationCount;
                if (ctx) {
                    // Abort before cleanup: waiters must settle as aborted (eviction),
                    // not with cleanup's generic rejection, which would surface as a
                    // task failure.
                    if (ctxMatchesEvictedInvocation && ctx.abortController) {
                        ctx.abortController.abort(err);
                    }
                    this.client.durableListener.cleanupTaskState(ctx.action.taskRunExternalId, invocationCount);
                }
                const future = this.futures[key];
                if (future && ctxMatchesEvictedInvocation) {
                    future.promise.catch(() => undefined);
                    future.cancel(hatchet_promise_1.CancellationReason.EVICTED_BY_WORKER);
                }
            },
            requestEvictionWithAck: (key, rec) => __awaiter(this, void 0, void 0, function* () {
                yield this.client.durableListener.sendEvictInvocation(rec.taskRunExternalId, rec.invocationCount, rec.evictionReason);
            }),
            logger: this.logger,
        });
        this.client.durableListener.onServerEvict = (durableTaskExternalId, invocationCount) => {
            var _a;
            (_a = this.evictionManager) === null || _a === void 0 ? void 0 : _a.handleServerEviction(durableTaskExternalId, invocationCount);
        };
        this.evictionManager.start();
        return this.evictionManager;
    }
    cleanupRun(key, attemptContext) {
        var _a;
        const ctx = attemptContext !== null && attemptContext !== void 0 ? attemptContext : this.contexts[key];
        if (ctx instanceof context_1.DurableContext) {
            this.client.durableListener.cleanupTaskState(ctx.action.taskRunExternalId, ctx.invocationCount);
        }
        // A restored invocation may have re-registered under this key while this
        // attempt was shutting down; the key-owned state now belongs to it.
        if (attemptContext && this.contexts[key] !== attemptContext)
            return;
        (_a = this.evictionManager) === null || _a === void 0 ? void 0 : _a.unregisterRun(key);
        delete this.futures[key];
        delete this.contexts[key];
    }
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchHandleStartStepRun.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    handleStartStepRun(action) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            const { actionId, taskRunExternalId, taskName } = action;
            const actionKey = action.key;
            try {
                const isDurable = this.durable_action_set.has(actionId);
                let context;
                if (isDurable) {
                    const { durableListener } = this.client;
                    let mgr;
                    if ((0, engine_version_1.supportsEviction)(this.engineVersion)) {
                        yield durableListener.ensureStarted(this.workerId || '');
                        mgr = this.ensureEvictionManager();
                        const evictionPolicy = this.eviction_policies.get(actionId);
                        mgr.registerRun(actionKey, taskRunExternalId, (_a = action.durableTaskInvocationCount) !== null && _a !== void 0 ? _a : 1, evictionPolicy);
                    }
                    context = new context_1.DurableContext(action, this.client, this, durableListener, mgr, this.engineVersion);
                }
                else {
                    context = new context_1.Context(action, this.client, this);
                }
                this.contexts[actionKey] = context;
                const step = this.action_registry[actionId];
                if (!step) {
                    this.logger.error(`Registered actions: '${Object.keys(this.action_registry).join(', ')}'`);
                    this.logger.error(`Could not find step '${actionId}'`);
                    this.cleanupRun(actionKey, context);
                    return;
                }
                const run = () => __awaiter(this, void 0, void 0, function* () {
                    const { middleware } = this.client.config;
                    if (middleware === null || middleware === void 0 ? void 0 : middleware.before) {
                        const hooks = Array.isArray(middleware.before) ? middleware.before : [middleware.before];
                        for (const hook of hooks) {
                            const extra = yield hook(context.input, context);
                            if (extra !== undefined) {
                                const merged = Object.assign(Object.assign({}, context.input), extra);
                                context.input = merged;
                                if (context.data && typeof context.data === 'object') {
                                    context.data.input = merged;
                                }
                            }
                        }
                    }
                    let result = yield parent_run_context_vars_1.parentRunContextManager.runWithContext({
                        parentId: action.workflowRunId,
                        parentTaskRunExternalId: taskRunExternalId,
                        childIndex: 0,
                        desiredWorkerId: this.workerId || '',
                        signal: context.abortController.signal,
                        durableContext: isDurable && context instanceof context_1.DurableContext ? context : undefined,
                    }, () => {
                        (0, abort_error_1.throwIfAborted)(context.abortController.signal);
                        return step(context);
                    });
                    if (middleware === null || middleware === void 0 ? void 0 : middleware.after) {
                        const hooks = Array.isArray(middleware.after) ? middleware.after : [middleware.after];
                        for (const hook of hooks) {
                            const extra = yield hook(result, context, context.input);
                            if (extra !== undefined) {
                                result = extra;
                            }
                        }
                    }
                    return result;
                });
                const success = (result) => __awaiter(this, void 0, void 0, function* () {
                    try {
                        if (context.cancelled) {
                            return;
                        }
                        this.logger.info((0, logger_1.taskRunLog)(taskName, taskRunExternalId, 'completed'));
                        const event = this.getStepActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_COMPLETED, false, result || null, action.retryCount);
                        yield this.client.dispatcher.sendStepActionEvent(event);
                    }
                    catch (actionEventError) {
                        this.logger.error(`Could not send completed action event: ${actionEventError.message || actionEventError}`);
                        const failureEvent = this.getStepActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_FAILED, false, actionEventError.message, action.retryCount);
                        try {
                            yield this.client.dispatcher.sendStepActionEvent(failureEvent);
                        }
                        catch (failureEventError) {
                            this.logger.error(`Could not send failed action event: ${failureEventError.message || failureEventError}`);
                        }
                        this.logger.error(`Could not send action event: ${actionEventError.message || actionEventError}`);
                    }
                    finally {
                        this.cleanupRun(actionKey, context);
                    }
                });
                const failure = (error) => __awaiter(this, void 0, void 0, function* () {
                    const shouldNotRetry = error instanceof task_1.NonRetryableError;
                    try {
                        if (context.cancelled) {
                            return;
                        }
                        // The run was evicted or cancelled by the worker, not failed by
                        // user code; the server already accounts for it.
                        if ((0, task_run_terminated_error_1.isTaskRunTerminatedError)(error)) {
                            return;
                        }
                        this.logger.error((0, logger_1.taskRunLog)(taskName, taskRunExternalId, `failed: ${error.message}`));
                        if (error.stack) {
                            this.logger.error(error.stack);
                        }
                        const event = this.getStepActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_FAILED, shouldNotRetry, {
                            message: error === null || error === void 0 ? void 0 : error.message,
                            stack: error === null || error === void 0 ? void 0 : error.stack,
                        }, action.retryCount);
                        yield this.client.dispatcher.sendStepActionEvent(event);
                    }
                    catch (e) {
                        this.logger.error(`Could not send action event: ${e.message}`);
                    }
                    finally {
                        this.cleanupRun(actionKey, context);
                    }
                });
                const future = new hatchet_promise_1.default((() => __awaiter(this, void 0, void 0, function* () {
                    let result;
                    try {
                        result = yield run();
                    }
                    catch (e) {
                        yield failure(e);
                        // Return error for OTel instrumentor to capture
                        return e;
                    }
                    // Postcheck: user code may swallow AbortError; don't report completion after cancellation.
                    // If we reached this point and the signal is aborted, the task likely caught/ignored cancellation.
                    if (context.abortController.signal.aborted) {
                        this.logger.warn(`Cancellation: task run ${taskRunExternalId} returned after cancellation was signaled. ` +
                            `This usually means an AbortError was caught and not propagated. ` +
                            `See https://docs.hatchet.run/home/cancellation`);
                        return;
                    }
                    (0, abort_error_1.throwIfAborted)(context.abortController.signal);
                    yield success(result);
                    return undefined;
                }))());
                this.futures[actionKey] = future;
                // Send the action event to the dispatcher
                const event = this.getStepActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_STARTED, false, undefined, action.retryCount);
                this.client.dispatcher.sendStepActionEvent(event).catch((e) => {
                    this.logger.error(`Could not send action event: ${e.message}`);
                });
                try {
                    return yield future.promise;
                }
                catch (e) {
                    if (!(0, task_run_terminated_error_1.isTaskRunTerminatedError)(e)) {
                        this.logger.error(`Could not wait for task run ${taskRunExternalId} to finish. ` +
                            `See https://docs.hatchet.run/home/cancellation for best practices on handling cancellation: `, e);
                    }
                }
                finally {
                    this.cleanupRun(actionKey, context);
                }
            }
            catch (e) {
                this.cleanupRun(actionKey);
                this.logger.error('Could not send action event (outer): ', e);
                return e instanceof Error ? e : new Error(String(e));
            }
        });
    }
    getStepActionEvent(action, eventType, shouldNotRetry, payload = '', retryCount = 0) {
        return {
            workerId: this.name,
            jobId: action.jobId,
            jobRunId: action.jobRunId,
            taskId: action.taskId,
            taskRunExternalId: action.taskRunExternalId,
            actionId: action.actionId,
            eventTimestamp: new Date(),
            eventType,
            eventPayload: JSON.stringify(payload),
            shouldNotRetry,
            retryCount,
        };
    }
    /**
     * Handles a START_BATCH action: invokes the registered batch task handler once with
     * every buffered item, then reports completion/failure back per-member (or, for
     * broadcast batch tasks, the same result for every member) via sendBatchActionEvent.
     */
    handleStartBatch(action) {
        return __awaiter(this, void 0, void 0, function* () {
            const { actionId, taskName } = action;
            const memberIds = batchMemberIds(action.actionPayload);
            this.client.dispatcher
                .sendBatchActionEvent(this.getBatchActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_STARTED, memberIds.map((id) => ({ taskRunExternalId: id, eventPayload: '' }))))
                .catch((e) => {
                this.logger.error(`Could not send batch started event: ${e.message}`);
            });
            const step = this.action_registry[actionId];
            if (!step) {
                this.logger.error(`Registered actions: '${Object.keys(this.action_registry).join(', ')}'`);
                this.logger.error(`Could not find step '${actionId}'`);
                return;
            }
            try {
                const context = new context_1.Context(action, this.client, this);
                const result = yield step(context);
                // If the handler cancelled the batch (ctx.cancel()), a CANCELLED batch event
                // covering every member was already sent from within cancel() itself. Don't also
                // send a COMPLETED event for the same members afterward — matches the Python SDK's
                // `if context.is_cancelled: return` guard in its batch runner.
                if (context.cancelled) {
                    return undefined;
                }
                yield this.sendBatchCompleted(action, result);
                return undefined;
            }
            catch (e) {
                this.logger.error((0, logger_1.taskRunLog)(taskName, actionId, `batch failed: ${e.message}`));
                if (e.stack) {
                    this.logger.error(e.stack);
                }
                yield this.sendBatchFailureForAll(action, memberIds, e);
                return e instanceof Error ? e : new Error(String(e));
            }
        });
    }
    /**
     * Reports the result of a successful batch handler invocation. result is expected to be
     * a Record<string, any> keyed by batch member id; each member's output is serialized
     * independently so that one member's serialization failure does not fail the whole
     * batch.
     */
    sendBatchCompleted(action, result) {
        return __awaiter(this, void 0, void 0, function* () {
            if (typeof result !== 'object' || result === null || Array.isArray(result)) {
                yield this.sendBatchFailureForAll(action, Object.keys(result !== null && result !== void 0 ? result : {}), new Error('batch task handler did not return a valid per-member result map'));
                return;
            }
            const completedItems = [];
            const failedItems = [];
            Object.entries(result).forEach(([id, output]) => {
                try {
                    completedItems.push({ taskRunExternalId: id, eventPayload: JSON.stringify(output) });
                }
                catch (e) {
                    failedItems.push({
                        taskRunExternalId: id,
                        eventPayload: JSON.stringify({ message: e.message }),
                    });
                }
            });
            if (completedItems.length > 0) {
                yield this.client.dispatcher.sendBatchActionEvent(this.getBatchActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_COMPLETED, completedItems));
            }
            if (failedItems.length > 0) {
                yield this.client.dispatcher.sendBatchActionEvent(this.getBatchActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_FAILED, failedItems));
            }
        });
    }
    /** Fails every member of the batch uniformly, e.g. when the handler itself throws. */
    sendBatchFailureForAll(action, memberIds, error) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = JSON.stringify({ message: error === null || error === void 0 ? void 0 : error.message, stack: error === null || error === void 0 ? void 0 : error.stack });
            const items = memberIds.map((id) => ({ taskRunExternalId: id, eventPayload: payload }));
            try {
                yield this.client.dispatcher.sendBatchActionEvent(this.getBatchActionEvent(action, dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_FAILED, items));
            }
            catch (e) {
                this.logger.error(`Could not send batch failed event: ${e.message}`);
            }
        });
    }
    getBatchActionEvent(action, eventType, items) {
        return {
            workerId: this.name,
            jobId: action.jobId,
            actionId: action.actionId,
            batchId: action.batchId,
            eventTimestamp: new Date(),
            eventType,
            items,
        };
    }
    getGroupKeyActionEvent(action, eventType, payload = '') {
        if (!action.getGroupKeyRunId) {
            throw new hatchet_error_1.default('No group key run id provided');
        }
        return {
            workerId: this.name,
            workflowRunId: action.workflowRunId,
            getGroupKeyRunId: action.getGroupKeyRunId,
            actionId: action.actionId,
            eventTimestamp: new Date(),
            eventType,
            eventPayload: JSON.stringify(payload),
        };
    }
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchHandleCancelStepRun.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    handleCancelStepRun(action) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b, _c;
            const { taskRunExternalId, taskName } = action;
            const actionKey = action.key;
            try {
                const future = this.futures[actionKey];
                const context = this.contexts[actionKey];
                const cancelErr = new task_run_terminated_error_1.TaskRunTerminatedError('cancelled', 'Cancelled by worker');
                if (context && context.abortController) {
                    context.abortController.abort(cancelErr);
                }
                if (future) {
                    const start = Date.now();
                    const warningThresholdMs = (_a = this.client.config.cancellation_warning_threshold) !== null && _a !== void 0 ? _a : 300;
                    const gracePeriodMs = (_b = this.client.config.cancellation_grace_period) !== null && _b !== void 0 ? _b : 1000;
                    const warningMs = Math.max(0, warningThresholdMs);
                    const graceMs = Math.max(0, gracePeriodMs);
                    // Ensure cancelling this future doesn't create an unhandled rejection in cases
                    // where the main action handler isn't currently awaiting `future.promise`.
                    future.promise.catch(() => undefined);
                    // Cancel the future (rejects the wrapper); user code must still cooperate with AbortSignal.
                    future.cancel(hatchet_promise_1.CancellationReason.CANCELLED_BY_WORKER);
                    // Track completion of the underlying work (not the cancelable wrapper).
                    // Ensure this promise never throws into our supervision flow.
                    const completion = ((_c = future.inner) !== null && _c !== void 0 ? _c : future.promise).catch(() => undefined);
                    // Wait until warning threshold, then log if still running.
                    if (warningMs > 0) {
                        const winner = yield Promise.race([
                            completion.then(() => 'done'),
                            (0, sleep_1.default)(warningMs).then(() => 'warn'),
                        ]);
                        if (winner === 'warn') {
                            const milliseconds = Date.now() - start;
                            this.logger.warn(`Cancellation: task run ${taskRunExternalId} has not cancelled after ${milliseconds}ms. Consider checking for blocking operations. ` +
                                `See https://docs.hatchet.run/home/cancellation`);
                        }
                    }
                    // Wait until grace period (total), then log if still running.
                    const elapsedMs = Date.now() - start;
                    const remainingMs = graceMs - elapsedMs;
                    const winner = yield Promise.race([
                        completion.then(() => 'done'),
                        (0, sleep_1.default)(Math.max(0, remainingMs)).then(() => 'grace'),
                    ]);
                    if (winner === 'done') {
                        this.logger.info((0, logger_1.taskRunLog)(taskName, taskRunExternalId, 'cancelled'));
                    }
                    else {
                        const totalElapsedMs = Date.now() - start;
                        this.logger.error(`Cancellation: task run ${taskRunExternalId} still running after cancellation grace period ` +
                            `${totalElapsedMs}ms.\n` +
                            `JavaScript cannot force-kill user code; see: https://docs.hatchet.run/home/cancellation`);
                    }
                }
            }
            catch (e) {
                this.logger.error(`Cancellation: error while supervising cancellation for task run ${taskRunExternalId}: ${(e === null || e === void 0 ? void 0 : e.message) || e}`);
            }
            finally {
                this.cleanupRun(actionKey);
            }
        });
    }
    stop() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.exitGracefully(false);
        });
    }
    exitGracefully(handleKill) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            // Signals and an explicit stop must await the same eviction and listener teardown.
            (_a = this.shutdownPromise) !== null && _a !== void 0 ? _a : (this.shutdownPromise = this.shutdown());
            yield this.shutdownPromise;
            if (handleKill) {
                this.logger.info('Exiting hatchet worker...');
                process.exit(0);
            }
        });
    }
    shutdown() {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            this.killing = true;
            this.setStatus(health_server_1.workerStatus.UNHEALTHY);
            this.logger.info('Starting to exit...');
            // Pause the worker on the server so it stops receiving new task assignments
            // before we evict waiting durable runs, mirroring Python's pause_task_assignment().
            if (this.workerId) {
                try {
                    yield this.client.workers.pause(this.workerId);
                }
                catch (e) {
                    this.logger.error(`Could not pause worker: ${e.message}`);
                }
            }
            if (this.evictionManager) {
                try {
                    const evicted = yield this.evictionManager.evictAllWaiting();
                    if (evicted > 0) {
                        this.logger.info(`Evicted ${evicted} waiting durable run(s) during shutdown`);
                    }
                }
                catch (e) {
                    this.logger.error(`Could not evict waiting runs: ${e.message}`);
                }
            }
            try {
                yield this.client.durableListener.stop();
            }
            catch (e) {
                this.logger.error(`Could not stop durable listener: ${e.message}`);
            }
            this.logger.info('Gracefully exiting hatchet worker, running tasks will attempt to finish...');
            // Awaits the underlying work rather than the cancelable wrapper, which rejects as soon as a
            // cancellation arrives while the task is still winding down.
            yield Promise.allSettled(Object.values(this.futures).map(({ inner }) => inner));
            this.logger.info('Successfully finished pending tasks.');
            // Unregistering closes the action stream and stops the heartbeat, so it has to wait until
            // the running tasks are done. Stopping the heartbeat earlier lets the engine treat the worker
            // as dead and reassign tasks that are about to complete, and closing the stream earlier makes
            // the engine reject every heartbeat sent while those tasks finish.
            try {
                yield ((_a = this.listener) === null || _a === void 0 ? void 0 : _a.unregister());
            }
            catch (e) {
                this.logger.error(`Could not unregister listener: ${e.message}`);
            }
            if (this.healthServer) {
                try {
                    yield this.healthServer.stop();
                }
                catch (e) {
                    this.logger.error(`Could not stop health server: ${e.message}`);
                }
            }
        });
    }
    /**
     * Creates an action listener by registering the worker with the dispatcher.
     * Override in subclasses to change registration behavior (e.g. legacy engines).
     */
    createListener() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.client.dispatcher.getActionListener({
                workerName: this.name,
                services: ['default'],
                actions: Object.keys(this.action_registry),
                slotConfig: this.slotConfig,
                labels: this.labels,
            });
        });
    }
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, e_1, _b, _c;
            if (this.killing) {
                return;
            }
            this.setStatus(health_server_1.workerStatus.STARTING);
            if (this.healthServer) {
                try {
                    yield this.healthServer.start();
                    if (this.killing) {
                        yield this.shutdownPromise;
                        yield this.healthServer.stop();
                        return;
                    }
                }
                catch (e) {
                    this.logger.error(`Could not start health server: ${e.message}`);
                    this.setStatus(health_server_1.workerStatus.UNHEALTHY);
                    return;
                }
            }
            // ensure all workflows are registered
            yield Promise.all(this.registeredWorkflowPromises);
            if (this.killing || Object.keys(this.action_registry).length === 0) {
                return;
            }
            try {
                const listener = yield this.createListener();
                if (this.killing) {
                    // Shutdown cannot clean up a listener that registration has not returned yet.
                    yield listener.unregister();
                    return;
                }
                this.listener = listener;
                this.workerId = this.listener.workerId;
                this.setStatus(health_server_1.workerStatus.HEALTHY);
                const generator = this.listener.actions();
                this.logger.info(`Worker ${this.name} listening for actions`);
                try {
                    for (var _d = true, generator_1 = __asyncValues(generator), generator_1_1; generator_1_1 = yield generator_1.next(), _a = generator_1_1.done, !_a; _d = true) {
                        _c = generator_1_1.value;
                        _d = false;
                        const action = _c;
                        const receivedType = (0, logger_1.actionMap)(action.actionType);
                        this.logger.info((0, logger_1.taskRunLog)(action.taskName, action.taskRunExternalId, `${receivedType}`));
                        void this.handleAction(action);
                    }
                }
                catch (e_1_1) { e_1 = { error: e_1_1 }; }
                finally {
                    try {
                        if (!_d && !_a && (_b = generator_1.return)) yield _b.call(generator_1);
                    }
                    finally { if (e_1) throw e_1.error; }
                }
            }
            catch (e) {
                this.setStatus(health_server_1.workerStatus.UNHEALTHY);
                if (this.killing) {
                    this.logger.info(`Exiting worker, ignoring error: ${e.message}`);
                    return;
                }
                this.logger.error(`Could not run worker: ${e.message}`);
                throw new hatchet_error_1.default(`Could not run worker: ${e.message}`);
            }
        });
    }
    handleAction(action) {
        return __awaiter(this, void 0, void 0, function* () {
            const type = (0, dispatcher_1.actionTypeFromJSON)(action.actionType) || dispatcher_1.ActionType.START_STEP_RUN;
            switch (type) {
                case dispatcher_1.ActionType.START_STEP_RUN:
                    return this.handleStartStepRun(action);
                case dispatcher_1.ActionType.START_BATCH:
                    return this.handleStartBatch(action);
                case dispatcher_1.ActionType.CANCEL_STEP_RUN:
                    return this.handleCancelStepRun(action);
                case dispatcher_1.ActionType.START_GET_GROUP_KEY:
                    this.logger.error(`Worker ${this.name} received unsupported action type START_GET_GROUP_KEY, please upgrade to V1...`);
                    return Promise.resolve();
                case dispatcher_1.ActionType.UNRECOGNIZED:
                    this.logger.error(`Worker ${this.name} received unrecognized action type ${action.actionType}`);
                    return Promise.resolve();
                default: {
                    const _ = type;
                    throw new Error(`Unhandled action type: ${_}`);
                }
            }
        });
    }
    upsertLabels(labels) {
        return __awaiter(this, void 0, void 0, function* () {
            this.labels = labels;
            if (!this.workerId) {
                this.logger.warn('Worker not registered.');
                return this.labels;
            }
            this.client.dispatcher.upsertWorkerLabels(this.workerId, labels);
            return this.labels;
        });
    }
}
exports.InternalWorker = InternalWorker;
/**
 * Decodes the buffered items of a batch task's START_BATCH action into a Record keyed by
 * each buffered item's task-run external id, mapping to that item's input. The wire shape
 * of actionPayload for a START_BATCH action is
 * `{ "<taskRunExternalId>": { "payload": { "input": {...}, ... }, "workflow_run_id": "..." }, ... }`,
 * distinct from the flat single-input shape used by START_STEP_RUN actions.
 */
function decodeBatchItems(actionPayload) {
    const parsed = parseBatchPayload(actionPayload);
    return Object.fromEntries(Object.entries(parsed).map(([id, item]) => { var _a, _b; return [id, (_b = (_a = item === null || item === void 0 ? void 0 : item.payload) === null || _a === void 0 ? void 0 : _a.input) !== null && _b !== void 0 ? _b : {}]; }));
}
function batchMemberIds(actionPayload) {
    return Object.keys(parseBatchPayload(actionPayload));
}
function parseBatchPayload(actionPayload) {
    if (!actionPayload) {
        return {};
    }
    try {
        const parsed = JSON.parse(actionPayload);
        return typeof parsed === 'object' && parsed !== null ? parsed : {};
    }
    catch (_a) {
        return {};
    }
}
