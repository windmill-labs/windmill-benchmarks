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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ON_SUCCESS_TASK_NAME = exports.ON_FAILURE_TASK_NAME = void 0;
exports.normalizeWorkflowDefinition = normalizeWorkflowDefinition;
exports.mapStickyStrategyPb = mapStickyStrategyPb;
exports.workflowToProto = workflowToProto;
exports.mapWorkerLabelPb = mapWorkerLabelPb;
exports.onFailureTaskName = onFailureTaskName;
exports.getLeaves = getLeaves;
exports.isLeafTask = isLeafTask;
exports.mapSlotRequestsPb = mapSlotRequestsPb;
exports.mapRateLimitPb = mapRateLimitPb;
exports.batchOf = batchOf;
exports.mapConcurrencyPb = mapConcurrencyPb;
exports.taskConcurrencyArr = taskConcurrencyArr;
exports.assertValidConcurrencyArr = assertValidConcurrencyArr;
exports.mapBatchConfigPb = mapBatchConfigPb;
exports.resolveExecutionTimeout = resolveExecutionTimeout;
exports.resolveScheduleTimeout = resolveScheduleTimeout;
/**
 * Turns a workflow declaration into the `CreateWorkflowVersionRequest` the engine
 * registers it under. Pure: nothing here talks to the engine or imports from Node, so
 * a serverless runtime can register the same declarations a worker does.
 * @module WorkflowProto
 */
const hatchet_error_1 = __importDefault(require("../../../util/errors/hatchet-error"));
const workflows_1 = require("../../../protoc/workflows");
const workflows_2 = require("../../../protoc/v1/workflows");
const transformer_1 = require("../../conditions/transformer");
const apply_namespace_1 = require("../../../util/apply-namespace");
const action_1 = require("../../../clients/dispatcher/action");
const z = __importStar(require("zod/v4"));
const duration_1 = require("../duration");
exports.ON_FAILURE_TASK_NAME = 'on-failure-task';
exports.ON_SUCCESS_TASK_NAME = 'on-success-task';
function definitionOf(definition) {
    return 'definition' in definition ? definition.definition : definition;
}
/**
 * Applies the namespace to a workflow definition and appends the on-success task, if the
 * workflow declares one, as a regular task whose parents are the leaves of the DAG.
 * Returns a new definition; the declaration is not mutated. Idempotent, so a definition
 * that was already normalized comes back unchanged.
 */
function normalizeWorkflowDefinition(definition, opts = {}) {
    var _a;
    const source = definitionOf(definition);
    // The client lowercases its namespace; doing the same here keeps this idempotent when
    // a caller passes a mixed-case namespace directly.
    const workflow = Object.assign(Object.assign({}, source), { name: (0, apply_namespace_1.applyNamespace)(source.name, (_a = opts.namespace) === null || _a === void 0 ? void 0 : _a.toLowerCase()).toLowerCase(), _tasks: [...source._tasks], _durableTasks: [...source._durableTasks] });
    const alreadyHasOnSuccess = workflow._tasks.some((task) => task.name === exports.ON_SUCCESS_TASK_NAME);
    const onSuccessTask = alreadyHasOnSuccess || opts.durable ? undefined : onSuccessTaskOf(workflow);
    if (onSuccessTask) {
        workflow._tasks.push(onSuccessTask);
    }
    return workflow;
}
function onSuccessTaskOf(workflow) {
    var _a, _b, _c, _d, _e, _f, _g;
    if (!workflow.onSuccess) {
        return undefined;
    }
    const parents = getLeaves([...workflow._tasks, ...workflow._durableTasks]);
    if (typeof workflow.onSuccess === 'function') {
        return {
            name: exports.ON_SUCCESS_TASK_NAME,
            fn: workflow.onSuccess,
            executionTimeout: '60s',
            parents,
            retries: 0,
            rateLimits: [],
            desiredWorkerLabels: undefined,
            concurrency: [],
        };
    }
    const onSuccess = workflow.onSuccess;
    return {
        name: exports.ON_SUCCESS_TASK_NAME,
        fn: onSuccess.fn,
        executionTimeout: onSuccess.executionTimeout || ((_a = workflow.taskDefaults) === null || _a === void 0 ? void 0 : _a.executionTimeout) || '60s',
        scheduleTimeout: onSuccess.scheduleTimeout || ((_b = workflow.taskDefaults) === null || _b === void 0 ? void 0 : _b.scheduleTimeout),
        parents,
        retries: onSuccess.retries || ((_c = workflow.taskDefaults) === null || _c === void 0 ? void 0 : _c.retries) || 0,
        rateLimits: onSuccess.rateLimits || ((_d = workflow.taskDefaults) === null || _d === void 0 ? void 0 : _d.rateLimits),
        desiredWorkerLabels: onSuccess.desiredWorkerLabels || ((_e = workflow.taskDefaults) === null || _e === void 0 ? void 0 : _e.workerLabels),
        concurrency: onSuccess.concurrency || ((_f = workflow.taskDefaults) === null || _f === void 0 ? void 0 : _f.concurrency),
        backoff: onSuccess.backoff || ((_g = workflow.taskDefaults) === null || _g === void 0 ? void 0 : _g.backoff),
    };
}
function onFailureTaskOf(workflow) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m;
    if (!workflow.onFailure) {
        return undefined;
    }
    if (typeof workflow.onFailure === 'function') {
        return {
            readableId: exports.ON_FAILURE_TASK_NAME,
            action: onFailureTaskName(workflow),
            timeout: '60s',
            inputs: '{}',
            parents: [],
            retries: 0,
            rateLimits: [],
            workerLabels: {},
            concurrency: [],
            isDurable: false,
            slotRequests: { default: 1 },
        };
    }
    const onFailure = workflow.onFailure;
    const scheduleTimeout = (_a = onFailure.scheduleTimeout) !== null && _a !== void 0 ? _a : (_b = workflow.taskDefaults) === null || _b === void 0 ? void 0 : _b.scheduleTimeout;
    return {
        readableId: exports.ON_FAILURE_TASK_NAME,
        action: onFailureTaskName(workflow),
        timeout: (0, duration_1.durationToString)(onFailure.executionTimeout || ((_c = workflow.taskDefaults) === null || _c === void 0 ? void 0 : _c.executionTimeout) || '60s'),
        scheduleTimeout: scheduleTimeout ? (0, duration_1.durationToString)(scheduleTimeout) : undefined,
        inputs: '{}',
        parents: [],
        retries: onFailure.retries || ((_d = workflow.taskDefaults) === null || _d === void 0 ? void 0 : _d.retries) || 0,
        rateLimits: mapRateLimitPb(onFailure.rateLimits || ((_e = workflow.taskDefaults) === null || _e === void 0 ? void 0 : _e.rateLimits)),
        workerLabels: mapWorkerLabelPb(onFailure.desiredWorkerLabels || ((_f = workflow.taskDefaults) === null || _f === void 0 ? void 0 : _f.workerLabels)),
        concurrency: [],
        backoffFactor: ((_g = onFailure.backoff) === null || _g === void 0 ? void 0 : _g.factor) || ((_j = (_h = workflow.taskDefaults) === null || _h === void 0 ? void 0 : _h.backoff) === null || _j === void 0 ? void 0 : _j.factor),
        backoffMaxSeconds: ((_k = onFailure.backoff) === null || _k === void 0 ? void 0 : _k.maxSeconds) || ((_m = (_l = workflow.taskDefaults) === null || _l === void 0 ? void 0 : _l.backoff) === null || _m === void 0 ? void 0 : _m.maxSeconds),
        isDurable: false,
        slotRequests: mapSlotRequestsPb(onFailure, false),
    };
}
function mapStickyStrategyPb(sticky) {
    // `workflow.sticky` is optional. When omitted, we don't set any sticky strategy.
    //
    // When provided, `workflow.sticky` is a v1 (non-protobuf) config which may also include
    // legacy protobuf enum values for backwards compatibility.
    if (sticky == null) {
        return undefined;
    }
    switch (sticky) {
        case 'soft':
        case 'SOFT':
        case 0:
            return workflows_1.StickyStrategy.SOFT;
        case 'hard':
        case 'HARD':
        case 1:
            return workflows_1.StickyStrategy.HARD;
        default:
            throw new hatchet_error_1.default(`Invalid sticky strategy: ${sticky}`);
    }
}
/**
 * Builds the registration request for a workflow. Accepts a declaration or its
 * definition; the namespace is applied and the on-success task appended the same way
 * `normalizeWorkflowDefinition` does, so callers may pass either the raw declaration or
 * an already normalized definition.
 *
 * Action ids are `<workflow>:<task>`, lowercased (see `createActionId`).
 */
function workflowToProto(definition, opts = {}) {
    var _a, _b;
    const workflow = normalizeWorkflowDefinition(definition, opts);
    const { namespace } = opts;
    const { concurrency } = workflow;
    const eventTriggers = [
        ...(workflow.onEvents || []).map((event) => (0, apply_namespace_1.applyNamespace)(event, namespace)),
        ...(workflow.on && 'event' in workflow.on && workflow.on.event
            ? Array.isArray(workflow.on.event)
                ? workflow.on.event.map((event) => (0, apply_namespace_1.applyNamespace)(event, namespace))
                : [(0, apply_namespace_1.applyNamespace)(workflow.on.event, namespace)]
            : []),
    ];
    const cronTriggers = [
        ...(workflow.onCrons || []),
        ...(workflow.on && 'cron' in workflow.on && workflow.on.cron
            ? Array.isArray(workflow.on.cron)
                ? workflow.on.cron
                : [workflow.on.cron]
            : []),
    ];
    const concurrencyArr = Array.isArray(concurrency) ? concurrency : [];
    const concurrencySolo = !Array.isArray(concurrency) ? concurrency : undefined;
    assertValidConcurrencyArr(concurrencyArr);
    assertValidConcurrencyArr(concurrencySolo ? [concurrencySolo] : undefined);
    // Convert Zod schema to JSON Schema if provided
    let inputJsonSchema;
    if (workflow.inputValidator) {
        const jsonSchema = z.toJSONSchema(workflow.inputValidator);
        inputJsonSchema = new TextEncoder().encode(JSON.stringify(jsonSchema));
    }
    const durableTaskSet = new Set(workflow._durableTasks);
    return {
        name: workflow.name,
        description: workflow.description || '',
        version: workflow.version || '',
        eventTriggers,
        cronTriggers,
        sticky: mapStickyStrategyPb(workflow.sticky),
        concurrencyArr: mapConcurrencyPb(concurrencyArr),
        onFailureTask: onFailureTaskOf(workflow),
        defaultPriority: workflow.defaultPriority,
        inputJsonSchema,
        tasks: [...workflow._tasks, ...workflow._durableTasks].map((task) => {
            var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l;
            return ({
                readableId: task.name,
                action: (0, action_1.createActionId)(workflow.name, task.name),
                timeout: resolveExecutionTimeout(task, workflow.taskDefaults),
                scheduleTimeout: resolveScheduleTimeout(task, workflow.taskDefaults),
                inputs: '{}',
                parents: (_b = (_a = task.parents) === null || _a === void 0 ? void 0 : _a.map((p) => p.name)) !== null && _b !== void 0 ? _b : [],
                userData: '{}',
                // Batch tasks buffer many concurrent runs into a single execution; per-item retry
                // semantics don't apply, so retries is always forced to 0.
                retries: batchOf(task) ? 0 : task.retries || ((_c = workflow.taskDefaults) === null || _c === void 0 ? void 0 : _c.retries) || 0,
                rateLimits: mapRateLimitPb(task.rateLimits || ((_d = workflow.taskDefaults) === null || _d === void 0 ? void 0 : _d.rateLimits)),
                workerLabels: mapWorkerLabelPb(task.desiredWorkerLabels || ((_e = workflow.taskDefaults) === null || _e === void 0 ? void 0 : _e.workerLabels)),
                backoffFactor: ((_f = task.backoff) === null || _f === void 0 ? void 0 : _f.factor) || ((_h = (_g = workflow.taskDefaults) === null || _g === void 0 ? void 0 : _g.backoff) === null || _h === void 0 ? void 0 : _h.factor),
                backoffMaxSeconds: ((_j = task.backoff) === null || _j === void 0 ? void 0 : _j.maxSeconds) || ((_l = (_k = workflow.taskDefaults) === null || _k === void 0 ? void 0 : _k.backoff) === null || _l === void 0 ? void 0 : _l.maxSeconds),
                conditions: (0, transformer_1.taskConditionsToPb)(task, namespace),
                isDurable: durableTaskSet.has(task),
                slotRequests: mapSlotRequestsPb(task, durableTaskSet.has(task)),
                batch: mapBatchConfigPb(batchOf(task)),
                concurrency: (() => {
                    const taskConcurrency = taskConcurrencyArr(task, workflow);
                    assertValidConcurrencyArr(taskConcurrency);
                    return mapConcurrencyPb(taskConcurrency);
                })(),
            });
        }),
        concurrency: concurrencySolo ? mapConcurrencyPb([concurrencySolo])[0] : undefined,
        defaultFilters: (_b = (_a = workflow.defaultFilters) === null || _a === void 0 ? void 0 : _a.map((f) => ({
            scope: f.scope,
            expression: f.expression,
            payload: f.payload ? new TextEncoder().encode(JSON.stringify(f.payload)) : undefined,
        }))) !== null && _b !== void 0 ? _b : [],
        idempotency: workflow.idempotency
            ? {
                expression: workflow.idempotency.expression,
                ttlMs: workflow.idempotency.strategy === 'status'
                    ? workflow.idempotency.fallbackTtlMs
                    : workflow.idempotency.ttlMs,
                method: workflow.idempotency.strategy === 'status'
                    ? workflows_2.IdempotencyMethod.STATUS
                    : workflows_2.IdempotencyMethod.TTL,
            }
            : undefined,
    };
}
function mapWorkerLabelPb(in_) {
    if (!in_) {
        return {};
    }
    return Object.entries(in_).reduce((acc, [key, label]) => {
        if (!label) {
            return Object.assign(Object.assign({}, acc), { [key]: {
                    strValue: undefined,
                    intValue: undefined,
                } });
        }
        if (typeof label === 'string') {
            return Object.assign(Object.assign({}, acc), { [key]: {
                    strValue: label,
                    intValue: undefined,
                } });
        }
        if (typeof label === 'number') {
            return Object.assign(Object.assign({}, acc), { [key]: {
                    strValue: undefined,
                    intValue: label,
                } });
        }
        return Object.assign(Object.assign({}, acc), { [key]: {
                strValue: typeof label.value === 'string' ? label.value : undefined,
                intValue: typeof label.value === 'number' ? label.value : undefined,
                required: label.required,
                weight: label.weight,
                comparator: label.comparator,
            } });
    }, {});
}
/** The action id of a workflow's on-failure task. */
function onFailureTaskName(workflow) {
    return (0, action_1.createActionId)(workflow.name, exports.ON_FAILURE_TASK_NAME);
}
function getLeaves(tasks) {
    return tasks.filter((task) => isLeafTask(task, tasks));
}
function isLeafTask(task, allTasks) {
    return !allTasks.some((t) => { var _a; return (_a = t.parents) === null || _a === void 0 ? void 0 : _a.some((p) => p.name === task.name); });
}
/** Durable tasks stay on the durable pool; slotCost applies only to the default pool. */
function mapSlotRequestsPb(task, isDurable) {
    if (task.slotRequests) {
        return task.slotRequests;
    }
    if (isDurable) {
        return { durable: 1 };
    }
    if (task.slotCost !== undefined) {
        if (!Number.isInteger(task.slotCost) || task.slotCost <= 0) {
            throw new Error(`slotCost must be a positive integer, got: ${task.slotCost}`);
        }
        return { default: task.slotCost };
    }
    return { default: 1 };
}
function mapRateLimitPb(limits) {
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
/** Batch tasks are only available on non-durable tasks; durable tasks never carry `batch`. */
function batchOf(task) {
    return 'batch' in task ? task.batch : undefined;
}
// mapConcurrencyPb maps SDK concurrency entries onto the proto shape; entries keep their
// declared order, which is the chain order.
function mapConcurrencyPb(entries) {
    return entries.map((c) => ({
        expression: c.expression,
        // a string maxRuns is a CEL expression; the static field then carries the default
        // of 1, which only governs slots created before the expression existed
        maxRuns: typeof c.maxRuns === 'string' ? 1 : c.maxRuns,
        limitStrategy: c.limitStrategy,
        name: c.name,
        isTenantScoped: c.isTenantScoped,
        maxRunsExpression: typeof c.maxRuns === 'string' ? c.maxRuns : undefined,
    }));
}
function taskConcurrencyArr(task, workflow) {
    var _a;
    if (task.concurrency) {
        return Array.isArray(task.concurrency) ? task.concurrency : [task.concurrency];
    }
    if ((_a = workflow.taskDefaults) === null || _a === void 0 ? void 0 : _a.concurrency) {
        return Array.isArray(workflow.taskDefaults.concurrency)
            ? workflow.taskDefaults.concurrency
            : [workflow.taskDefaults.concurrency];
    }
    return [];
}
function assertValidConcurrencyArr(concurrency) {
    concurrency === null || concurrency === void 0 ? void 0 : concurrency.forEach((c) => {
        if (typeof c.maxRuns === 'string') {
            if (!c.maxRuns.trim()) {
                throw new Error('concurrency.maxRuns expression must be non-empty');
            }
            return;
        }
        if (c.maxRuns !== undefined && (!Number.isInteger(c.maxRuns) || c.maxRuns <= 0)) {
            throw new Error(`concurrency.maxRuns must be a positive integer or a CEL expression, got: ${c.maxRuns}`);
        }
    });
}
function mapBatchConfigPb(batch) {
    if (!batch) {
        return undefined;
    }
    if (!Number.isInteger(batch.maxSize) || batch.maxSize <= 0) {
        throw new Error(`batch.maxSize must be a positive integer, got: ${batch.maxSize}`);
    }
    const batchMaxIntervalMs = batch.maxInterval !== undefined ? (0, duration_1.durationToMs)(batch.maxInterval) : undefined;
    if (batchMaxIntervalMs !== undefined && batchMaxIntervalMs <= 0) {
        throw new Error('batch.maxInterval must be positive when provided');
    }
    if (batch.groupMaxRuns !== undefined &&
        (!Number.isInteger(batch.groupMaxRuns) || batch.groupMaxRuns <= 0)) {
        throw new Error(`batch.groupMaxRuns must be a positive integer when provided, got: ${batch.groupMaxRuns}`);
    }
    return {
        batchMaxSize: batch.maxSize,
        batchMaxIntervalMs,
        batchGroupKey: batch.groupKey,
        batchGroupMaxRuns: batch.groupMaxRuns,
        broadcastOutput: batch.broadcastOutput,
    };
}
// Helper function to validate CEL expressions
function validateCelExpression(_expr) {
    // FIXME: this is a placeholder. In a real implementation, you'd need to use a CEL parser or validator.
    // For now, we'll just return true to mimic the behavior.
    return true;
}
function resolveExecutionTimeout(task, workflowDefaults) {
    return (0, duration_1.durationToString)(task.executionTimeout || task.timeout || (workflowDefaults === null || workflowDefaults === void 0 ? void 0 : workflowDefaults.executionTimeout) || '60s');
}
function resolveScheduleTimeout(task, workflowDefaults) {
    const value = task.scheduleTimeout || (workflowDefaults === null || workflowDefaults === void 0 ? void 0 : workflowDefaults.scheduleTimeout);
    return value ? (0, duration_1.durationToString)(value) : undefined;
}
