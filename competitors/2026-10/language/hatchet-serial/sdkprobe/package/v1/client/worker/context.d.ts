/**
 * The Hatchet Context class provides helper methods and useful data to tasks at runtime. It is passed as the second argument to all tasks and durable tasks.
 *
 * There are two types of context classes you'll encounter:
 *
 * - Context - The standard context for regular tasks with methods for logging, task output retrieval, cancellation, and more.
 * - DurableContext - An extended context for durable tasks that includes additional methods for durable execution.
 * @module Context
 */
import { Priority, RunOpts, TaskWorkflowDeclaration, BaseWorkflowDeclaration as WorkflowV1 } from '../../declaration';
import type { Action } from '../../../clients/dispatcher/action';
import { Logger, LogLevel } from '../../../util/logger';
import WorkflowRunRef from '../../../util/workflow-run-ref';
import { Conditions } from '../../conditions';
import { CreateWorkflowDurableTaskOpts, CreateWorkflowTaskOpts } from '../../task';
import { JsonObject, OutputType } from '../../types';
import type { HatchetClient } from '../client';
import type { NextStep } from '../../../legacy/step';
import type { DurableListenerClient } from '../../../clients/listeners/durable-listener/durable-listener-client';
import { z } from 'zod/v4';
import type { InternalWorker } from './worker-internal';
import { Duration } from '../duration';
import type { DurableEvictionManager } from './eviction/eviction-manager';
import { ContextRuntime, DurableContextOptions, DurableTransport, WorkerLabels } from './runtime';
type TriggerData = Record<string, Record<string, any>>;
type ChildRunOpts = RunOpts & {
    key?: string;
    sticky?: boolean;
};
export interface SleepResult {
    /** The sleep duration in milliseconds. */
    durationMs: number;
}
export interface SleepForOptions {
    /** Optional key used for condition result indexing. */
    readableDataKey?: string;
    /** Optional human-readable wait label shown in dashboard run details. */
    label?: string;
}
type LogExtra = {
    extra?: any;
    error?: Error;
};
interface ContextData<T, K> {
    input: T;
    triggers: TriggerData;
    parents: Record<string, any>;
    triggered_by: string;
    user_data: K;
    step_run_errors: Record<string, string>;
}
/**
 * ContextWorker is a user-friendly view of the worker running the task, from the context of a run.
 */
export declare class ContextWorker {
    private runtime;
    constructor(runtime: ContextRuntime);
    /**
     * Gets the ID of the worker.
     * @returns The ID of the worker.
     */
    id(): string | undefined;
    /**
     * Checks if the worker has a registered workflow.
     * @param workflowName - The name of the workflow to check.
     * @returns True if the workflow is registered, otherwise false.
     */
    hasWorkflow(workflowName: string): boolean;
    /**
     * Gets the current state of the worker labels.
     * @returns The labels of the worker.
     */
    labels(): WorkerLabels;
    /**
     * Upserts the a set of labels on the worker.
     * @param labels - The labels to upsert.
     * @returns A promise that resolves when the labels have been upserted.
     */
    upsertLabels(labels: WorkerLabels): Promise<WorkerLabels>;
}
export declare class Context<T, K = {}> {
    data: ContextData<T, K>;
    input: T;
    controller: AbortController;
    action: Action;
    /**
     * The Hatchet client of the worker running the task. Only set when the context was
     * created by a worker; a runtime without a client (see {@link ContextRuntime}) leaves
     * it undefined.
     */
    v1: HatchetClient;
    /** The runtime the context performs engine-facing operations through. */
    runtime: ContextRuntime;
    worker: ContextWorker;
    overridesData: Record<string, any>;
    _logger: Logger;
    /** @deprecated Kept for backward compat; prefer {@link nextChildIndex}. */
    spawnIndex: number;
    streamIndex: number;
    protected nextChildIndex(n?: number): number;
    /**
     * Creates a context on top of a runtime.
     * @param action - The action assigned to the task.
     * @param runtime - The runtime the context performs engine-facing operations through.
     */
    constructor(action: Action, runtime: ContextRuntime);
    /**
     * Creates a context for a task running on a worker.
     * @param action - The action assigned to the task.
     * @param v1 - The worker's Hatchet client.
     * @param worker - The worker running the task.
     */
    constructor(action: Action, v1: HatchetClient, worker: InternalWorker);
    get abortController(): AbortController;
    get cancelled(): boolean;
    protected throwIfCancelled(): void;
    /**
     * Helper for broad `catch` blocks so cancellation isn't accidentally swallowed.
     *
     * Example:
     * ```ts
     * try { ... } catch (e) { ctx.rethrowIfCancelled(e); ... }
     * ```
     */
    rethrowIfCancelled(err: unknown): void;
    cancel(): Promise<void>;
    /**
     * Retrieves the output of a parent task.
     * @param parentTask - The a CreateTaskOpts or string of the parent task name.
     * @returns The output of the specified parent task.
     * @throws An error if the task output is not found.
     */
    parentOutput<L extends OutputType>(parentTask: CreateWorkflowTaskOpts<any, L> | CreateWorkflowDurableTaskOpts<any, L> | string): Promise<L>;
    /**
     * Returns errors from any task runs in the workflow.
     * @returns A record mapping task names to error messages.
     * @throws A warning if no errors are found (this method should be used in on-failure tasks).
     * @deprecated use ctx.errors() instead
     * @hidden
     */
    stepRunErrors(): Record<string, string>;
    /**
     * Returns errors from any task runs in the workflow.
     * @returns A record mapping task names to error messages.
     * @throws A warning if no errors are found (this method should be used in on-failure tasks).
     */
    errors(): Record<string, string>;
    /**
     * Gets the dag conditional triggers for the current workflow run.
     * @returns The triggers for the current workflow.
     */
    triggers(): TriggerData;
    /**
     * Gets the payload from the filter that matched when triggering the event.
     * @returns The payload.
     */
    filterPayload(): Record<string, any>;
    /**
     * Determines if the workflow was triggered by an event.
     * @returns True if the workflow was triggered by an event, otherwise false.
     */
    triggeredByEvent(): boolean;
    /**
     * Gets the name of the current workflow.
     * @returns The name of the workflow.
     * @deprecated This method returns the task name. Use {@link workflowNameV1} for the workflow
     * name or {@link taskName} for the task name.
     */
    workflowName(): string;
    /**
     * Gets the name of the current workflow.
     * @returns The name of the workflow.
     */
    workflowNameV1(): string;
    /**
     * Gets the user data associated with the workflow.
     * @returns The user data.
     */
    userData(): K;
    /**
     * Gets the name of the current running task.
     * @returns The name of the task.
     */
    taskName(): string;
    /**
     * Gets the ID of the current workflow run.
     * @returns The workflow run ID.
     */
    workflowRunId(): string;
    /**
     * Gets the workflow ID of the currently running workflow.
     * @returns The workflow id.
     */
    workflowId(): string | undefined;
    /**
     * Gets the workflow version ID of the currently running workflow.
     * @returns The workflow version ID.
     */
    workflowVersionId(): string | undefined;
    /**
     * Gets the ID of the current task run.
     * @returns The task run ID.
     */
    taskRunExternalId(): string;
    /**
     * Gets the ID of the current task run.
     * @returns The task run ID.
     * @deprecated use taskRunExternalId() instead
     * @hidden
     */
    taskRunId(): string;
    /**
     * Gets the number of times the current task has been retried.
     * @returns The retry count.
     */
    retryCount(): number;
    /**
     * Logs a message from the current task.
     * @param message - The message to log.
     * @param level - The log level (optional).
     * @deprecated use ctx.logger.infoger.info, ctx.logger.infoger.debug, ctx.logger.infoger.warn, ctx.logger.infoger.error, ctx.logger.infoger.trace instead
     * @hidden
     */
    log(message: string, level?: LogLevel, extra?: LogExtra): Promise<void> | Promise<void[]>;
    get logger(): {
        info: (message: string, extra?: any) => Promise<void> | Promise<void[]>;
        debug: (message: string, extra?: any) => Promise<void> | Promise<void[]>;
        warn: (message: string, extra?: LogExtra) => Promise<void> | Promise<void[]>;
        error: (message: string, extra?: LogExtra) => Promise<void> | Promise<void[]>;
        util: (key: string, message: string, extra?: LogExtra) => void | Promise<void>;
    };
    /**
     * Refreshes the timeout for the current task.
     * @param incrementBy - The interval by which to increment the timeout.
     * The interval should be specified in the format of '10s' for 10 seconds, '1m' for 1 minute, or '1d' for 1 day.
     */
    refreshTimeout(incrementBy: Duration): Promise<void>;
    /**
     * Releases a worker slot for a task run such that the worker can pick up another task.
     * Note: this is an advanced feature that may lead to unexpected behavior if used incorrectly.
     * @returns A promise that resolves when the slot has been released.
     */
    releaseSlot(): Promise<void>;
    /**
     * Streams data from the current task run.
     * @param data - The data to stream (string or binary).
     * @returns A promise that resolves when the data has been streamed.
     */
    putStream(data: string | Uint8Array): Promise<void>;
    protected spawnOptions(workflow: string | WorkflowV1<any, any>, options?: ChildRunOpts): {
        workflowName: string;
        opts: {
            parentId: string;
            parentTaskRunExternalId: string;
            childIndex: number;
            childKey: string | undefined;
            desiredWorkerId: string | undefined;
            _standaloneTaskName: string | undefined;
            additionalMetadata?: {
                [x: string]: string;
            };
            priority?: Priority;
            sticky?: boolean;
            returnExceptions?: boolean;
            desiredWorkerLabels?: Record<string, {
                value: string | number;
                required?: boolean;
                weight?: number;
                comparator?: import("../../task").WorkerLabelComparator;
            }>;
            key?: string;
        };
    };
    private spawn;
    private spawnBulk;
    /**
     * Runs multiple children workflows in parallel without waiting for their results.
     * @param children - An array of  objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of workflow run references to the enqueued runs.
     */
    bulkRunNoWaitChildren<Q extends JsonObject = any, P extends JsonObject = any>(children: Array<{
        workflow: string | WorkflowV1<Q, P>;
        input: Q;
        options?: ChildRunOpts;
    }>): Promise<WorkflowRunRef<P>[]>;
    /**
     * Runs multiple children workflows in parallel and waits for all results.
     * @param children - An array of objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of results from the children workflows.
     */
    bulkRunChildren<Q extends JsonObject = any, P extends JsonObject = any>(children: Array<{
        workflow: string | WorkflowV1<Q, P>;
        input: Q;
        options?: ChildRunOpts;
    }>): Promise<P[]>;
    /**
     * Runs a new workflow and waits for its result.
     *
     * @param workflow - The workflow to run (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - An options object containing key, sticky, priority, and additionalMetadata.
     * @returns The result of the workflow.
     */
    runChild<Q extends JsonObject, P extends JsonObject>(workflow: string | WorkflowV1<Q, P> | TaskWorkflowDeclaration<Q, P>, input: Q, options?: ChildRunOpts): Promise<P>;
    /**
     * Enqueues a new workflow without waiting for its result.
     *
     * @param workflow - The workflow to enqueue (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - An options object containing key, sticky, priority, and additionalMetadata.
     * @returns A reference to the spawned workflow run.
     */
    runNoWaitChild<Q extends JsonObject, P extends JsonObject>(workflow: string | WorkflowV1<Q, P>, input: Q, options?: ChildRunOpts): Promise<WorkflowRunRef<P>>;
    /**
     * Retrieves additional metadata associated with the current workflow run.
     * @returns A record of metadata key-value pairs.
     */
    additionalMetadata(): Record<string, string>;
    /**
     * Gets the index of this workflow if it was spawned as part of a bulk operation.
     * @returns The child index number, or undefined if not set.
     */
    childIndex(): number | undefined;
    /**
     * Gets the key associated with this workflow if it was spawned as a child workflow.
     * @returns The child key, or undefined if not set.
     */
    childKey(): string | undefined;
    /**
     * Gets the ID of the parent workflow run if this workflow was spawned as a child.
     * @returns The parent workflow run ID, or undefined if not a child workflow.
     */
    parentWorkflowRunId(): string | undefined;
    priority(): Priority | undefined;
    /**
     * Gets the external ID of the event that triggered this workflow run, if any.
     * @returns The triggering event external ID, or undefined if the workflow was not triggered by an event.
     */
    triggeringEventId(): string | undefined;
    /**
     * Gets the key of the event that triggered this workflow run, if any.
     * @returns The triggering event key, or undefined if the workflow was not triggered by an event.
     */
    triggeringEventKey(): string | undefined;
    /**
     * Get the output of a task.
     * @param task - The name of the task to get the output for.
     * @returns The output of the task.
     * @throws An error if the task output is not found.
     * @deprecated use ctx.parentOutput instead
     * @hidden
     */
    stepOutput<L = NextStep>(step: string): L;
    /**
     * Gets the input data for the current workflow.
     * @returns The input data for the workflow.
     * @deprecated use task input parameter instead
     * @hidden
     */
    workflowInput(): T;
    /**
     * Gets the name of the current task.
     * @returns The name of the task.
     * @deprecated use ctx.taskName instead
     * @hidden
     */
    stepName(): string;
    /**
     * Spawns multiple workflows.
     *
     * @param workflows - An array of objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of references to the spawned workflow runs.
     * @deprecated Use bulkRunNoWaitChildren or bulkRunChildren instead.
     * @hidden
     */
    spawnWorkflows<Q extends JsonObject = any, P extends JsonObject = any>(workflows: Array<{
        workflow: string | WorkflowV1<Q, P>;
        input: Q;
        options?: ChildRunOpts;
    }>): Promise<WorkflowRunRef<P>[]>;
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
    spawnWorkflow<Q extends JsonObject, P extends JsonObject>(workflow: string | WorkflowV1<Q, P> | TaskWorkflowDeclaration<Q, P>, input: Q, options?: ChildRunOpts): Promise<WorkflowRunRef<P>>;
    _incrementStreamIndex(): number;
}
/**
 * DurableContext provides helper methods and useful data to durable tasks at runtime.
 * It extends the Context class and includes additional methods for durable execution like sleepFor and waitFor.
 */
export declare class DurableContext<T, K = {}> extends Context<T, K> {
    private _durableListener;
    private _evictionManager;
    private _engineVersion;
    private _waitKey;
    private _sendEventLock;
    private _serializeSendEvent;
    /**
     * Creates a durable context on top of a runtime and a durable transport.
     * @param action - The action assigned to the task.
     * @param runtime - The runtime the context performs engine-facing operations through.
     * @param transport - The transport durable events travel over.
     * @param options - Engine version and eviction settings.
     */
    constructor(action: Action, runtime: ContextRuntime, transport: DurableTransport, options?: DurableContextOptions & {
        evictionManager?: DurableEvictionManager;
    });
    /**
     * Creates a durable context for a task running on a worker.
     * @param action - The action assigned to the task.
     * @param v1 - The worker's Hatchet client.
     * @param worker - The worker running the task.
     * @param durableListener - The worker's durable listener.
     * @param evictionManager - The worker's eviction manager, when the engine supports eviction.
     * @param engineVersion - The engine version the worker is connected to.
     */
    constructor(action: Action, v1: HatchetClient, worker: InternalWorker, durableListener: DurableListenerClient, evictionManager?: DurableEvictionManager, engineVersion?: string);
    get supportsEviction(): boolean;
    /** The transport durable events travel over. */
    get durableListener(): DurableTransport;
    /**
     * The invocation count for the current durable task. Used for deduplication across replays.
     */
    get invocationCount(): number;
    private get _actionKey();
    private withEvictionWait;
    /**
     * Pauses execution for the specified duration.
     * Duration is "global" meaning it will wait in real time regardless of transient failures like worker restarts.
     * @param duration - The duration to sleep for.
     * @param readableDataKeyOrOptions - Optional readable key string or options object containing readableDataKey and label.
     * @param label - Optional wait label used when readableDataKey is passed as the second argument.
     * @returns A promise that resolves with a SleepResult when the sleep duration has elapsed.
     */
    sleepFor(duration: Duration, options?: SleepForOptions): Promise<SleepResult>;
    sleepFor(duration: Duration, readableDataKey?: string): Promise<SleepResult>;
    sleepFor(duration: Duration, readableDataKey?: string, label?: string): Promise<SleepResult>;
    /**
     * Pauses execution until the specified conditions are met.
     * Conditions are "global" meaning they will wait in real time regardless of transient failures like worker restarts.
     * @param conditions - The conditions to wait for.
     * @returns A promise that resolves with the event that satisfied the conditions.
     */
    waitFor(conditions: Conditions | Conditions[], label?: string): Promise<Record<string, any>>;
    /**
     * Lightweight wrapper for waiting for a user event. Allows for shorthand usage of
     * `ctx.waitFor` when specifying a user event condition.
     *
     * For more complicated conditions, use `ctx.waitFor` directly.
     *
     * @param key - The event key to wait for.
     * @param expression - An optional CEL expression to filter events.
     * @param payloadSchema - An optional Zod schema to validate and parse the event payload.
     * @returns The event payload, validated against the schema if provided.
     */
    waitForEvent(key: string, expression?: string, payloadSchema?: undefined, scope?: string, lookbackWindow?: Duration, label?: string): Promise<Record<string, any>>;
    waitForEvent<T extends z.ZodTypeAny>(key: string, expression?: string, payloadSchema?: T, scope?: string, lookbackWindow?: Duration, label?: string): Promise<z.infer<T>>;
    /**
     * Durably sleep until a specific timestamp.
     * Uses the memoized `now()` to compute the remaining duration, then delegates to `sleepFor`.
     *
     * @param wakeAt - The timestamp to sleep until.
     * @returns A SleepResult containing the actual duration slept.
     */
    sleepUntil(wakeAt: Date): Promise<SleepResult>;
    /**
     * Get the current timestamp, memoized across replays. Returns the same Date on every replay of the same task run.
     * @returns The memoized current timestamp.
     */
    now(): Promise<Date>;
    private _waitForPreEviction;
    private _buildTriggerOpts;
    /**
     * Spawns a child workflow through the durable event log, waits for the child to complete.
     * @param workflow - The workflow to spawn.
     * @param input - The input data for the child workflow.
     * @param options - Options for spawning the child workflow.
     * @returns The result of the child workflow.
     */
    spawnChild<Q extends JsonObject, P extends OutputType>(workflow: string | WorkflowV1<Q, P> | TaskWorkflowDeclaration<Q, P>, input?: Q, options?: ChildRunOpts): Promise<P>;
    /**
     * Spawns multiple child workflows through the durable event log, waits for all to complete.
     * @param children - An array of objects containing the workflow, input, and options for each child.
     * @returns A list of results from the child workflows.
     */
    spawnChildren<Q extends JsonObject, P extends OutputType>(children: Array<{
        workflow: string | WorkflowV1<Q, P> | TaskWorkflowDeclaration<Q, P>;
        input: Q;
        options?: ChildRunOpts;
    }>): Promise<P[]>;
    /**
     * Memoize a function by storing its result in durable storage. Avoids recomputation on replay.
     *
     * @param fn - The async function to compute the value.
     * @param deps - Dependency values that form the memoization key.
     * @returns The memoized value, either from durable storage or freshly computed.
     */
    private memo;
}
/**
 * Derives the memo key for a task run and its dependency values: the SHA-256 of the task
 * run id followed by the JSON-serialised dependencies, over WebCrypto so it works in
 * every runtime. The bytes are identical to the previous Node `createHash` version, so
 * event logs recorded by older SDKs keep replaying.
 */
export declare function computeMemoKey(taskRunExternalId: string, args: readonly unknown[]): Promise<Uint8Array>;
export {};
