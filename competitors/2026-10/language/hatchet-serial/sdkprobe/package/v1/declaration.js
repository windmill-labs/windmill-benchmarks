"use strict";
/**
 * `Runnables` in the Hatchet TypeScript SDK are things that can be run, namely tasks and workflows. The two main types of runnables you'll encounter are:
 *
 * - `WorkflowDeclaration`, returned by `hatchet.workflow(...)`, which lets you define tasks and call `run()`, `schedule()`, `cron()`, etc.
 * - `TaskWorkflowDeclaration`, returned by `hatchet.task(...)`, which is a single standalone task that exposes the same execution helpers as a workflow.
 * @module Runnables
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
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaskWorkflowDeclaration = exports.WorkflowDeclaration = exports.BaseWorkflowDeclaration = exports.StickyStrategy = exports.Priority = void 0;
exports.CreateTaskWorkflow = CreateTaskWorkflow;
exports.CreateBatchTaskWorkflow = CreateBatchTaskWorkflow;
exports.CreateWorkflow = CreateWorkflow;
exports.CreateDurableTaskWorkflow = CreateDurableTaskWorkflow;
const hatchet_error_1 = __importDefault(require("../util/errors/hatchet-error"));
const abort_error_1 = require("../util/abort-error");
const parent_run_context_vars_1 = require("./parent-run-context-vars");
const UNBOUND_ERR = new Error('workflow unbound to hatchet client, hint: use client.run instead');
var Priority;
(function (Priority) {
    Priority[Priority["LOW"] = 1] = "LOW";
    Priority[Priority["MEDIUM"] = 2] = "MEDIUM";
    Priority[Priority["HIGH"] = 3] = "HIGH";
})(Priority || (exports.Priority = Priority = {}));
/**
 * Sticky strategy for workflow scheduling.
 *
 * Prefer using `StickyStrategy.SOFT` / `StickyStrategy.HARD` (v1, non-protobuf).
 * For backwards compatibility, the workflow/task `sticky` field also accepts legacy
 * protobuf enum values (`0`/`1`) and strings (`'SOFT'`/`'HARD'`).
 * @internal
 */
exports.StickyStrategy = {
    SOFT: 'soft',
    HARD: 'hard',
};
/**
 * Represents a workflow that can be executed by Hatchet.
 * @template I The input type for the workflow.
 * @template O The return type of the workflow.
 * @internal
 */
class BaseWorkflowDeclaration {
    /**
     * Creates a new workflow instance.
     * @param options The options for creating the workflow.
     * @param client Optional Hatchet client instance.
     * @internal
     */
    constructor(options, client) {
        this.definition = Object.assign(Object.assign({}, options), { _tasks: [], _durableTasks: [] });
        this.client = client;
    }
    runNoWait(input, options, _standaloneTaskName) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            const parentRunContext = parent_run_context_vars_1.parentRunContextManager.getContext();
            if (!parentRunContext && ((options === null || options === void 0 ? void 0 : options.childKey) || (options === null || options === void 0 ? void 0 : options.sticky))) {
                this.client.admin.logger.warn('ignoring childKey or sticky because run is not being spawned from a parent task');
            }
            const inheritedSignal = parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.signal;
            // Precheck: if we're being called from a cancelled parent task, do not enqueue more work.
            // The signal is inherited from the parent task's `ctx.abortController.signal`.
            (0, abort_error_1.throwIfAborted)(inheritedSignal, {
                isTrigger: true,
                context: (parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.parentTaskRunExternalId)
                    ? `task run ${parentRunContext.parentTaskRunExternalId}`
                    : undefined,
                warn: (message) => this.client.admin.logger.warn(message),
            });
            // Snapshot childIndex before incrementing (shared with Context.spawnIndex)
            // via parentRunContextManager so both spawning APIs produce unique indices.
            const baseChildIndex = parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.childIndex;
            const inputCount = Array.isArray(input) ? input.length : 1;
            parent_run_context_vars_1.parentRunContextManager.incrementChildIndex(inputCount);
            const runOpts = Object.assign(Object.assign({}, (options !== null && options !== void 0 ? options : {})), { parentId: parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.parentId, parentTaskRunExternalId: parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.parentTaskRunExternalId, childIndex: baseChildIndex, sticky: (options === null || options === void 0 ? void 0 : options.sticky) ? parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.desiredWorkerId : undefined, childKey: options === null || options === void 0 ? void 0 : options.childKey });
            if (Array.isArray(input)) {
                let resp = [];
                for (let i = 0; i < input.length; i += 500) {
                    const batch = input.slice(i, i + 500);
                    const batchResp = yield this.client.admin.runWorkflows(batch.map((inp, batchIdx) => ({
                        workflowName: this.definition.name,
                        input: inp,
                        options: Object.assign(Object.assign({}, runOpts), { childIndex: (baseChildIndex !== null && baseChildIndex !== void 0 ? baseChildIndex : 0) + i + batchIdx }),
                    })));
                    resp = resp.concat(batchResp);
                }
                const res = [];
                resp.forEach((ref, index) => {
                    const wf = input[index].workflow;
                    if (wf instanceof TaskWorkflowDeclaration) {
                        ref._standaloneTaskName = wf._standalone_task_name;
                    }
                    if (_standaloneTaskName) {
                        ref._standaloneTaskName = _standaloneTaskName;
                    }
                    // Ensure result subscriptions inherit cancellation if no signal is provided explicitly.
                    ref.defaultSignal = inheritedSignal;
                    res.push(ref);
                });
                return res;
            }
            const res = yield this.client.admin.runWorkflow(this.definition.name, input, runOpts);
            if (_standaloneTaskName) {
                res._standaloneTaskName = _standaloneTaskName;
            }
            res.defaultSignal = inheritedSignal;
            return res;
        });
    }
    /**
     * Synchronously trigger multiple workflow runs without waiting for completion.
     * @param runs A list of run requests containing input and per-run options.
     * @returns WorkflowRunRef values in the same order as input runs.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    runManyNoWait(runs, _standaloneTaskName) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            const parentRunContext = parent_run_context_vars_1.parentRunContextManager.getContext();
            const hasChildKeyOrSticky = runs.some((run) => { var _a, _b; return ((_a = run.opts) === null || _a === void 0 ? void 0 : _a.childKey) || ((_b = run.opts) === null || _b === void 0 ? void 0 : _b.sticky); });
            if (!parentRunContext && hasChildKeyOrSticky) {
                this.client.admin.logger.warn('ignoring childKey or sticky because run is not being spawned from a parent task');
            }
            const inheritedSignal = parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.signal;
            (0, abort_error_1.throwIfAborted)(inheritedSignal, {
                isTrigger: true,
                context: (parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.parentTaskRunExternalId)
                    ? `task run ${parentRunContext.parentTaskRunExternalId}`
                    : undefined,
                warn: (message) => this.client.admin.logger.warn(message),
            });
            parent_run_context_vars_1.parentRunContextManager.incrementChildIndex(runs.length);
            const baseOpts = {
                parentId: parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.parentId,
                parentTaskRunExternalId: parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.parentTaskRunExternalId,
                childIndex: parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.childIndex,
            };
            let resp = [];
            for (let i = 0; i < runs.length; i += 500) {
                const batch = runs.slice(i, i + 500);
                const batchResp = yield this.client.admin.runWorkflows(batch.map((run, batchIndex) => {
                    var _a, _b, _c;
                    const _d = (_a = run.opts) !== null && _a !== void 0 ? _a : {}, { sticky } = _d, restOpts = __rest(_d, ["sticky"]);
                    return {
                        workflowName: this.definition.name,
                        input: run.input,
                        options: Object.assign(Object.assign(Object.assign({}, baseOpts), restOpts), { childIndex: ((_b = baseOpts.childIndex) !== null && _b !== void 0 ? _b : 0) + i + batchIndex, desiredWorkerId: sticky ? parentRunContext === null || parentRunContext === void 0 ? void 0 : parentRunContext.desiredWorkerId : undefined, childKey: (_c = run.opts) === null || _c === void 0 ? void 0 : _c.childKey }),
                    };
                }));
                resp = resp.concat(batchResp);
            }
            resp.forEach((ref) => {
                if (_standaloneTaskName) {
                    ref._standaloneTaskName = _standaloneTaskName;
                }
                ref.defaultSignal = inheritedSignal;
            });
            return resp;
        });
    }
    runAndWait(input, options, _standaloneTaskName) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            // note: typescript is not smart enough to infer that input is an array
            return Array.isArray(input)
                ? this.run(input, options, _standaloneTaskName)
                : this.run(input, options, _standaloneTaskName);
        });
    }
    run(input, options, _standaloneTaskName) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            const durableCtx = (_a = parent_run_context_vars_1.parentRunContextManager.getContext()) === null || _a === void 0 ? void 0 : _a.durableContext;
            if (durableCtx) {
                if (Array.isArray(input)) {
                    return durableCtx.spawnChildren(input.map((inp) => ({ workflow: this, input: inp, options })));
                }
                return durableCtx.spawnChild(this, input, options);
            }
            if (Array.isArray(input)) {
                const refs = yield this.runNoWait(input, options, _standaloneTaskName);
                if (options === null || options === void 0 ? void 0 : options.returnExceptions) {
                    const settled = yield Promise.allSettled(refs.map((ref) => ref.result()));
                    return settled.map((s) => {
                        if (s.status === 'fulfilled') {
                            return s.value;
                        }
                        const { reason } = s;
                        if (reason instanceof Error) {
                            return reason;
                        }
                        return new Error(Array.isArray(reason) ? reason.join('; ') : String(reason));
                    });
                }
                return Promise.all(refs.map((ref) => ref.result()));
            }
            const res = yield this.runNoWait(input, options, _standaloneTaskName);
            return res.result();
        });
    }
    /**
     * Executes many workflow runs and waits for all results.
     * @param runs A list of run requests containing input and per-run options.
     * @returns Results in the same order as input runs.
     */
    runMany(runs, _standaloneTaskName) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            const durableCtx = (_a = parent_run_context_vars_1.parentRunContextManager.getContext()) === null || _a === void 0 ? void 0 : _a.durableContext;
            if (durableCtx) {
                return durableCtx.spawnChildren(runs.map((run) => ({
                    workflow: this,
                    input: run.input,
                    options: run.opts,
                })));
            }
            const refs = yield this.runManyNoWait(runs, _standaloneTaskName);
            const returnExceptions = runs.some((run) => { var _a; return (_a = run.opts) === null || _a === void 0 ? void 0 : _a.returnExceptions; });
            if (returnExceptions) {
                const settled = yield Promise.allSettled(refs.map((ref) => ref.result()));
                return settled.map((s) => {
                    if (s.status === 'fulfilled') {
                        return s.value;
                    }
                    const { reason } = s;
                    if (reason instanceof Error) {
                        return reason;
                    }
                    return new Error(Array.isArray(reason) ? reason.join('; ') : String(reason));
                });
            }
            return Promise.all(refs.map((ref) => ref.result()));
        });
    }
    /**
     * Schedules a workflow to run at a specific date and time in the future.
     * @param enqueueAt The date when the workflow should be triggered.
     * @param input The input data for the workflow.
     * @param options Optional configuration for this workflow run.
     * @returns A promise that resolves with the scheduled workflow details.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    schedule(enqueueAt, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            // If called from within a cancelled parent task, do not enqueue scheduled work.
            (0, abort_error_1.throwIfAborted)((_a = parent_run_context_vars_1.parentRunContextManager.getContext()) === null || _a === void 0 ? void 0 : _a.signal, {
                isTrigger: true,
                warn: (message) => this.client.admin.logger.warn(message),
            });
            const scheduled = this.client.scheduled.create(this.definition.name, Object.assign({ triggerAt: enqueueAt, input: input }, (options !== null && options !== void 0 ? options : {})));
            return scheduled;
        });
    }
    /**
     * Schedules a workflow to run after a specified delay.
     * @param duration The delay in seconds before the workflow should run.
     * @param input The input data for the workflow.
     * @param options Optional configuration for this workflow run.
     * @returns A promise that resolves with the scheduled workflow details.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    delay(duration, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const now = Date.now();
            const triggerAt = new Date(now + duration * 1000);
            return this.schedule(triggerAt, input, options);
        });
    }
    /**
     * Creates a cron schedule for the workflow.
     * @param name The name of the cron schedule.
     * @param expression The cron expression defining the schedule.
     * @param input The input data for the workflow.
     * @param options Optional configuration for this workflow run.
     * @returns A promise that resolves with the cron workflow details.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    cron(name, expression, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            // If called from within a cancelled parent task, do not enqueue cron work.
            (0, abort_error_1.throwIfAborted)((_a = parent_run_context_vars_1.parentRunContextManager.getContext()) === null || _a === void 0 ? void 0 : _a.signal, {
                isTrigger: true,
                warn: (message) => this.client.admin.logger.warn(message),
            });
            const cronDef = this.client.crons.create(this.definition.name, Object.assign(Object.assign({ expression, input: input }, (options !== null && options !== void 0 ? options : {})), { additionalMetadata: options === null || options === void 0 ? void 0 : options.additionalMetadata, name }));
            return cronDef;
        });
    }
    /**
     * Get metrics for the workflow.
     * @param opts Optional configuration for the metrics request.
     * @returns A promise that resolves with the workflow metrics.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    metrics(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            const workflow = yield this.client.workflows.get(this.definition.name);
            return this.client.metrics.getTaskStatusMetrics({
                since: (opts === null || opts === void 0 ? void 0 : opts.since) || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
                until: (opts === null || opts === void 0 ? void 0 : opts.until) || new Date().toISOString(),
                workflow_ids: [workflow.metadata.id],
            });
        });
    }
    /**
     * Get queue metrics for the workflow.
     * @param opts Optional configuration for the metrics request.
     * @returns A promise that resolves with the workflow metrics.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    taskStatusMetrics(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                throw UNBOUND_ERR;
            }
            const workflow = yield this.client.workflows.get(this.definition.name);
            return this.client.metrics.getTaskStatusMetrics({
                since: (opts === null || opts === void 0 ? void 0 : opts.since) || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
                until: (opts === null || opts === void 0 ? void 0 : opts.until) || new Date().toISOString(),
                workflow_ids: [workflow.metadata.id],
            });
        });
    }
    /**
     * Get the current state of the workflow.
     * @returns A promise that resolves with the workflow state.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    get() {
        if (!this.client) {
            throw UNBOUND_ERR;
        }
        return this.client.workflows.get(this);
    }
    /**
     * Pause the workflow. While paused, new runs of the workflow are queued but not started.
     * @param opts - The options for the pause operation.
     * @returns A promise that resolves to the updated workflow.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    pause(opts) {
        if (!this.client) {
            throw UNBOUND_ERR;
        }
        return this.client.workflows.pause(this, opts);
    }
    /**
     * Unpause the workflow.
     * @returns A promise that resolves to the updated workflow.
     * @throws Error if the workflow is not bound to a Hatchet client.
     */
    unpause() {
        if (!this.client) {
            throw UNBOUND_ERR;
        }
        return this.client.workflows.unpause(this);
    }
    /**
     * @deprecated use definition.name instead
     * @hidden
     */
    get id() {
        return this.definition.name;
    }
    /**
     * Get the friendly name of the workflow.
     * @returns The name of the workflow.
     */
    get name() {
        return this.definition.name;
    }
    mcpTool(sdk, ...args) {
        if (sdk === 'openai') {
            // eslint-disable-next-line @typescript-eslint/no-require-imports
            const { OpenAIToolFunc } = require('./agent/openai');
            return OpenAIToolFunc(this, ...args);
        }
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const { ClaudeToolFunc } = require('./agent/claude');
        return ClaudeToolFunc(this, ...args);
    }
}
exports.BaseWorkflowDeclaration = BaseWorkflowDeclaration;
/**
 * A Hatchet workflow, which lets you define tasks and perform actions on the workflow.
 *
 * Workflows in Hatchet represent coordinated units of work that can be triggered,
 * scheduled, or run on a cron schedule. Each workflow can contain multiple tasks
 * that can be arranged in dependencies (DAGs), with customized retry behavior,
 * timeouts, concurrency controls, and more.
 *
 * Example:
 * ```typescript
 * import { hatchet } from './hatchet-client';
 *
 * type MyInput = { name: string };
 *
 * const workflow = hatchet.workflow<MyInput>({
 *   name: 'my-workflow',
 * });
 *
 * workflow.task({
 *   name: 'greet',
 *   fn: async (input) => {
 *     return { message: `Hello, ${input.name}!` };
 *   },
 * });
 *
 * // Run the workflow
 * await workflow.run({ name: 'World' });
 * ```
 *
 * Workflows support various execution patterns, including:
 * - One-time execution with `run()` and `runNoWait()`
 * - Scheduled execution with `schedule()`
 * - Cron-based recurring execution with `cron()`
 * - Bulk execution by passing an array input to `run()` and `runNoWait()`
 * - Per-run bulk execution options with `runMany()` and `runManyNoWait()`
 *
 * Tasks within workflows can be defined with `workflow.task()` or
 * `workflow.durableTask()` and arranged into complex dependency patterns.
 */
class WorkflowDeclaration extends BaseWorkflowDeclaration {
    /**
     * Adds a task to the workflow.
     * The return type will be either the property on O that corresponds to the task name,
     * or if there is no matching property, the inferred return type of the function.
     * @template Name The literal string name of the task.
     * @template Fn The type of the task function.
     * @param options The task configuration options.
     * @returns The task options that were added.
     */
    task(options) {
        let typedOptions;
        if (options instanceof TaskWorkflowDeclaration) {
            typedOptions = options.taskDef;
        }
        else {
            typedOptions = options;
        }
        this.definition._tasks.push(typedOptions);
        return typedOptions;
    }
    /**
     * Adds a batch task to the workflow. Batch tasks buffer concurrent runs until Hatchet
     * flushes the batch (size reached or flush interval), then invoke the handler once with
     * all buffered inputs keyed by each run's task-run external id. The handler must return
     * a Record mapping each id to its output, or set `batch.broadcastOutput` to return the
     * same result to all callers. retries is always forced to 0 for batch tasks.
     *
     * Preview: batch tasks are in beta and may change in future releases.
     * @template Fn The type of the batch task function.
     * @param options The batch task configuration options.
     * @returns The task options that were added.
     */
    batchTask(options) {
        const { fn, batch } = options, rest = __rest(options, ["fn", "batch"]);
        const wrappedFn = wrapBatchFn(fn, !!batch.broadcastOutput);
        const typedOptions = Object.assign(Object.assign({}, rest), { fn: wrappedFn, batch });
        this.definition._tasks.push(typedOptions);
        return typedOptions;
    }
    /**
     * Adds an onFailure task to the workflow.
     * This will only run if any task in the workflow fails.
     * @template Name The literal string name of the task.
     * @template L The inferred return type of the task function.
     * @param options The task configuration options.
     * @returns The task options that were added.
     */
    onFailure(options) {
        var _a;
        let typedOptions;
        if (options instanceof TaskWorkflowDeclaration) {
            typedOptions = options.taskDef;
        }
        else {
            typedOptions = options;
        }
        if (this.definition.onFailure) {
            (_a = this.client) === null || _a === void 0 ? void 0 : _a.logger.warn(`onFailure task will override existing onFailure task`);
        }
        this.definition.onFailure = typedOptions;
        return typedOptions;
    }
    /**
     * Adds an onSuccess task to the workflow.
     * This will only run if all tasks in the workflow complete successfully.
     * @template Name The literal string name of the task.
     * @template L The inferred return type of the task function.
     * @param options The task configuration options.
     * @returns The task options that were added.
     */
    onSuccess(options) {
        var _a;
        let typedOptions;
        if (options instanceof TaskWorkflowDeclaration) {
            typedOptions = options.taskDef;
        }
        else {
            typedOptions = options;
        }
        if (this.definition.onSuccess) {
            (_a = this.client) === null || _a === void 0 ? void 0 : _a.logger.warn(`onSuccess task will override existing onSuccess task`);
        }
        this.definition.onSuccess = typedOptions;
        return typedOptions;
    }
    /**
     * Adds a durable task to the workflow.
     * The return type will be either the property on O that corresponds to the task name,
     * or if there is no matching property, the inferred return type of the function.
     * @template Name The literal string name of the task.
     * @template Fn The type of the task function.
     * @param options The task configuration options.
     * @returns The task options that were added.
     */
    durableTask(options) {
        const typedOptions = options;
        this.definition._durableTasks.push(typedOptions);
        return typedOptions;
    }
}
exports.WorkflowDeclaration = WorkflowDeclaration;
/**
 * A standalone task declaration that can be run like a workflow.
 *
 * `TaskWorkflowDeclaration` is returned by `hatchet.task(...)` and wraps a single
 * task definition while exposing the same execution helpers as workflows, such as
 * `run()`, `runNoWait()`, `schedule()`, and `cron()` (inherited from
 * `BaseWorkflowDeclaration`).
 *
 * Example:
 * ```typescript
 * const greet = hatchet.task<{ name: string }, { message: string }>({
 *   name: 'greet',
 *   fn: async (input) => ({ message: `Hello, ${input.name}!` }),
 * });
 *
 * await greet.run({ name: 'World' });
 * const ref = await greet.runNoWait({ name: 'World' });
 * ```
 *
 * @template I The input type for the standalone task.
 * @template O The output type returned by the standalone task.
 * @template GlobalInput - Global input type from the client, merged into all run/schedule/cron input signatures.
 * @template MiddlewareBefore - Extra fields added to the task fn input by pre-middleware hooks.
 * @template MiddlewareAfter - Extra fields merged into the task output by post-middleware hooks.
 */
class TaskWorkflowDeclaration extends BaseWorkflowDeclaration {
    constructor(options, client) {
        // strip out the concurrency options, because those do not need to be provided to the workflow options.
        // if they are, it will cause the concurrency strategy to have a parentId (from the workflow that doesn't really exist)
        // causing a fallback to the slow concurrency evaluation
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { concurrency } = options, workflowOpts = __rest(options, ["concurrency"]);
        super(Object.assign({}, workflowOpts), client);
        this._standalone_task_name = options.name;
        this.definition._tasks.push(Object.assign({}, options));
    }
    runAndWait(input, options) {
        const _super = Object.create(null, {
            runAndWait: { get: () => super.runAndWait }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return Array.isArray(input)
                ? _super.runAndWait.call(this, input, options, this._standalone_task_name)
                : _super.runAndWait.call(this, input, options, this._standalone_task_name);
        });
    }
    run(input, options) {
        const _super = Object.create(null, {
            run: { get: () => super.run }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return Array.isArray(input)
                ? _super.run.call(this, input, options, this._standalone_task_name)
                : _super.run.call(this, input, options, this._standalone_task_name);
        });
    }
    runNoWait(input, options) {
        const _super = Object.create(null, {
            runNoWait: { get: () => super.runNoWait }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return Array.isArray(input)
                ? _super.runNoWait.call(this, input, options, this._standalone_task_name)
                : _super.runNoWait.call(this, input, options, this._standalone_task_name);
        });
    }
    /**
     * Triggers many task runs and waits for all results.
     * @param runs - The list of inputs and optional per-run options.
     * @returns A promise that resolves with ordered task outputs.
     */
    runMany(runs) {
        const _super = Object.create(null, {
            runMany: { get: () => super.runMany }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return _super.runMany.call(this, runs, this._standalone_task_name);
        });
    }
    /**
     * Triggers many task runs without waiting for completion.
     * @param runs - The list of inputs and optional per-run options.
     * @returns WorkflowRunRef values for each scheduled run.
     */
    runManyNoWait(runs) {
        const _super = Object.create(null, {
            runManyNoWait: { get: () => super.runManyNoWait }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return _super.runManyNoWait.call(this, runs, this._standalone_task_name);
        });
    }
    /**
     * Schedules the task to run at a specific date and time.
     * @param enqueueAt - The date when the task should be triggered.
     * @param input - The input data for the task, including global input fields.
     * @param options - Optional configuration for this task run.
     * @returns A promise that resolves with the scheduled workflow details.
     */
    schedule(enqueueAt, input, options) {
        const _super = Object.create(null, {
            schedule: { get: () => super.schedule }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return _super.schedule.call(this, enqueueAt, input, options);
        });
    }
    /**
     * Schedules the task to run after a specified delay.
     * @param duration - The delay in seconds before the task should run.
     * @param input - The input data for the task, including global input fields.
     * @param options - Optional configuration for this task run.
     * @returns A promise that resolves with the scheduled workflow details.
     */
    delay(duration, input, options) {
        const _super = Object.create(null, {
            delay: { get: () => super.delay }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return _super.delay.call(this, duration, input, options);
        });
    }
    /**
     * Creates a cron schedule for the task.
     * @param name - The name of the cron schedule.
     * @param expression - The cron expression defining the schedule.
     * @param input - The input data for the task, including global input fields.
     * @param options - Optional configuration for this task run.
     * @returns A promise that resolves with the cron workflow details.
     */
    cron(name, expression, input, options) {
        const _super = Object.create(null, {
            cron: { get: () => super.cron }
        });
        return __awaiter(this, void 0, void 0, function* () {
            return _super.cron.call(this, name, expression, input, options);
        });
    }
    // Returns the underlying task definition for this declaration.
    get taskDef() {
        return this.definition._tasks[0];
    }
}
exports.TaskWorkflowDeclaration = TaskWorkflowDeclaration;
/**
 * Creates a new task workflow declaration with types inferred from the function parameter.
 * @template Fn The type of the task function
 * @param options The task configuration options.
 * @param client Optional Hatchet client instance.
 * @returns A new TaskWorkflowDeclaration with inferred types.
 */
function CreateTaskWorkflow(options, client) {
    return new TaskWorkflowDeclaration(options, client);
}
/**
 * Wraps a user-provided batch task handler so it always resolves to a Record keyed by
 * batch member id, regardless of whether the handler itself uses broadcastOutput. When
 * broadcastOutput is set, the handler's single return value is copied to every member id
 * present in the input. Otherwise, the handler's returned Record must have exactly the
 * same key set as the input; a mismatch throws, which fails every member of the batch
 * uniformly (matching the behavior of an uncaught error from the handler itself).
 */
function wrapBatchFn(fn, broadcastOutput) {
    return (input, ctx) => __awaiter(this, void 0, void 0, function* () {
        const result = yield fn(input, ctx);
        if (broadcastOutput) {
            return Object.fromEntries(Object.keys(input).map((id) => [id, result]));
        }
        if (typeof result !== 'object' || result === null || Array.isArray(result)) {
            throw new hatchet_error_1.default('batch task handler must return an object keyed by batch member id when broadcastOutput is false');
        }
        const inputKeys = Object.keys(input);
        const resultKeys = Object.keys(result);
        const missing = inputKeys.filter((k) => !resultKeys.includes(k));
        const extra = resultKeys.filter((k) => !inputKeys.includes(k));
        if (missing.length > 0 || extra.length > 0) {
            throw new hatchet_error_1.default(`batch task handler result keys do not match batch member ids (missing=${missing.join(', ')}, extra=${extra.join(', ')})`);
        }
        return result;
    });
}
function CreateBatchTaskWorkflow(options, client) {
    const { fn, batch } = options, rest = __rest(options, ["fn", "batch"]);
    const wrappedFn = wrapBatchFn(fn, !!batch.broadcastOutput);
    return new TaskWorkflowDeclaration(Object.assign(Object.assign({}, rest), { fn: wrappedFn, batch, retries: 0 }), client);
}
/**
 * Creates a new workflow instance.
 * @template I The input type for the workflow.
 * @template O The return type of the workflow.
 * @param options The options for creating the workflow. Optionally include a Zod schema
 *                via the `input` field to generate a JSON Schema for the backend.
 * @param client Optional Hatchet client instance.
 * @returns A new Workflow instance.
 */
function CreateWorkflow(options, client) {
    return new WorkflowDeclaration(options, client);
}
/**
 * Creates a new durable task workflow declaration with types inferred from the function parameter.
 * @template Fn The type of the durable task function
 * @param options The durable task configuration options.
 * @param client Optional Hatchet client instance.
 * @returns A new TaskWorkflowDeclaration with inferred types.
 */
function CreateDurableTaskWorkflow(options, client) {
    // Note: We're using TaskWorkflowDeclaration here since task and durableTask
    // share the same declaration structure but with different task types
    const taskWorkflow = new TaskWorkflowDeclaration(options, client);
    // Move the task from tasks to durableTasks
    if (taskWorkflow.definition._tasks.length > 0) {
        const [task] = taskWorkflow.definition._tasks;
        taskWorkflow.definition._tasks = [];
        taskWorkflow.definition._durableTasks.push(task);
    }
    return taskWorkflow;
}
