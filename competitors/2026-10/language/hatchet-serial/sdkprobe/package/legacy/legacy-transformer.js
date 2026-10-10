"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isLegacyWorkflow = isLegacyWorkflow;
exports.warnLegacyWorkflow = warnLegacyWorkflow;
exports.transformLegacyWorkflow = transformLegacyWorkflow;
exports.normalizeWorkflow = normalizeWorkflow;
exports.normalizeWorkflows = normalizeWorkflows;
exports.getWorkflowName = getWorkflowName;
const v0_deprecation_warning_1 = require("../util/v0-deprecation-warning");
const step_1 = require("./step");
const declaration_1 = require("../v1/declaration");
const LEGACY_WORKFLOW_WARNING = [
    '',
    '\x1b[33m╔══════════════════════════════════════════════════════════════════════════════╗',
    '║                                                                              ║',
    '║   ⚠  DEPRECATION WARNING: Legacy workflow format detected.                   ║',
    '║                                                                              ║',
    '║   Please migrate to the v1 SDK:                                              ║',
    '║   https://docs.hatchet.run/home/migration-guide-typescript                   ║',
    '║                                                                              ║',
    '╚══════════════════════════════════════════════════════════════════════════════╝\x1b[0m',
    '',
].join('\n');
/**
 * Type guard: returns true if the workflow is a legacy v0 Workflow (not a BaseWorkflowDeclaration).
 */
function isLegacyWorkflow(workflow) {
    return (workflow != null &&
        typeof workflow === 'object' &&
        !(workflow instanceof declaration_1.BaseWorkflowDeclaration) &&
        'id' in workflow &&
        'steps' in workflow &&
        Array.isArray(workflow.steps));
}
/**
 * Emits a deprecation warning for legacy workflow usage.
 */
let warnedLegacyWorkflow = false;
/**
 * Warns once per process that a v0 workflow is in use: the banner for the terminal and a
 * DeprecationWarning with a stable code that Node's warning flags and handlers can filter.
 */
function warnLegacyWorkflow() {
    if (warnedLegacyWorkflow) {
        return;
    }
    warnedLegacyWorkflow = true;
    console.warn(LEGACY_WORKFLOW_WARNING);
    (0, v0_deprecation_warning_1.emitV0RemovedWarning)('workflow');
}
/**
 * Transforms a legacy v0 Workflow into a v1 WorkflowDeclaration.
 *
 * The transformed declaration can be registered with a worker and executed
 * by the v1 runtime. Legacy step `run` functions are wrapped to receive
 * a V0Context for backwards compatibility.
 */
function transformLegacyWorkflow(workflow) {
    // Map concurrency
    let concurrency;
    if (workflow.concurrency) {
        if (workflow.concurrency.key) {
            console.warn('[hatchet] Legacy concurrency key functions are not supported in v1. ' +
                'Use CEL expressions instead: https://docs.hatchet.run/home/v1-sdk-improvements');
        }
        concurrency = {
            expression: workflow.concurrency.expression || workflow.concurrency.name,
            maxRuns: workflow.concurrency.maxRuns,
            limitStrategy: workflow.concurrency.limitStrategy,
        };
    }
    const opts = {
        name: workflow.id,
        description: workflow.description,
        version: workflow.version,
        sticky: workflow.sticky != null ? workflow.sticky : undefined,
        on: workflow.on ? workflow.on : undefined,
        concurrency,
        taskDefaults: {
            executionTimeout: workflow.timeout,
            scheduleTimeout: workflow.scheduleTimeout,
        },
    };
    const declaration = new declaration_1.WorkflowDeclaration(opts);
    // Build task lookup for parent resolution (parents are strings in legacy, objects in v1)
    const taskMap = {};
    for (const step of workflow.steps) {
        const taskOpts = legacyStepToTaskOpts(step, taskMap, workflow.timeout);
        taskMap[step.name] = taskOpts;
        declaration.definition._tasks.push(taskOpts);
    }
    // Handle onFailure
    if (workflow.onFailure) {
        declaration.definition.onFailure = {
            fn: wrapLegacyStepRun(workflow.onFailure),
            executionTimeout: (workflow.onFailure.timeout || workflow.timeout),
            retries: workflow.onFailure.retries,
            rateLimits: mapLegacyRateLimits(workflow.onFailure.rate_limits),
            desiredWorkerLabels: workflow.onFailure.worker_labels,
            backoff: workflow.onFailure.backoff,
        };
    }
    return declaration;
}
/**
 * Converts a legacy CreateStep to a v1 CreateWorkflowTaskOpts.
 */
function legacyStepToTaskOpts(step, taskMap, workflowTimeout) {
    var _a;
    return {
        name: step.name,
        fn: wrapLegacyStepRun(step),
        executionTimeout: (step.timeout || workflowTimeout),
        retries: step.retries,
        rateLimits: mapLegacyRateLimits(step.rate_limits),
        desiredWorkerLabels: step.worker_labels,
        backoff: step.backoff,
        parents: (_a = step.parents) === null || _a === void 0 ? void 0 : _a.map((name) => taskMap[name]).filter(Boolean),
    };
}
/**
 * Wraps a legacy step's `run(ctx: V0Context)` function into a v1-compatible
 * `fn(input, ctx: Context)` function by constructing a V0Context at runtime.
 */
function wrapLegacyStepRun(step) {
    return (input, ctx) => {
        // Access the V1Worker from the ContextWorker's private field.
        // This is intentionally accessing a private field for legacy compatibility.
        const v1Worker = ctx.worker.worker;
        const v0ctx = new step_1.V0Context(ctx.action, ctx.v1.v0, v1Worker);
        // Share the abort controller so cancellation propagates
        v0ctx.controller = ctx.controller;
        return step.run(v0ctx);
    };
}
/**
 * Maps legacy rate limits to v1 format.
 */
function mapLegacyRateLimits(limits) {
    if (!limits) {
        return undefined;
    }
    return limits.map((l) => ({
        staticKey: l.staticKey || l.key,
        dynamicKey: l.dynamicKey,
        units: l.units,
        limit: l.limit,
        duration: l.duration,
    }));
}
/**
 * Normalizes a workflow: if it is a legacy Workflow, emits a deprecation warning
 * and transforms it into a BaseWorkflowDeclaration. If already a v1 declaration,
 * returns it as-is.
 */
function normalizeWorkflow(workflow) {
    if (isLegacyWorkflow(workflow)) {
        warnLegacyWorkflow();
        return transformLegacyWorkflow(workflow);
    }
    return workflow;
}
/**
 * Normalizes an array of workflows, transforming any legacy workflows and
 * emitting deprecation warnings.
 */
function normalizeWorkflows(workflows) {
    return workflows.map(normalizeWorkflow);
}
/**
 * Extracts the workflow name from a workflow-like value.
 * Works for strings, BaseWorkflowDeclaration, WorkflowDefinition, and legacy Workflow.
 * Emits a deprecation warning if a legacy workflow is detected.
 */
function getWorkflowName(workflow) {
    if (typeof workflow === 'string') {
        return workflow;
    }
    if (workflow instanceof declaration_1.BaseWorkflowDeclaration) {
        return workflow.name;
    }
    if (isLegacyWorkflow(workflow)) {
        warnLegacyWorkflow();
        return workflow.id;
    }
    if ('name' in workflow) {
        return workflow.name;
    }
    throw new Error('Invalid workflow: must be a string, BaseWorkflowDeclaration, or legacy Workflow object');
}
