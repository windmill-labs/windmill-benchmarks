import * as z from 'zod/v4';
import { JsonObject } from '@bufbuild/protobuf';
import { Workflow } from './workflow';
import { Action } from '../clients/dispatcher/action-listener';
import { LogLevel } from '../clients/event/event-client';
import { Logger } from '../util/logger';
import WorkflowRunRef from '../util/workflow-run-ref';
import { WorkerLabels } from '../clients/dispatcher/dispatcher-client';
import { CreateStepRateLimit, RateLimitDuration } from '../protoc/workflows';
import { CreateWorkflowTaskOpts, Priority } from '../v1';
import { WorkerLabelComparator } from '../v1/task';
import { RunOpts, TaskWorkflowDeclaration, BaseWorkflowDeclaration as WorkflowV1 } from '../v1/declaration';
import { Conditions } from '../v1/conditions';
import { Duration } from '../v1/client/duration';
import { JsonValue, OutputType } from '../v1/types';
import { InternalWorker } from '../v1/client/worker/worker-internal';
import { LegacyHatchetClient } from './legacy-client';
export declare const CreateRateLimitSchema: z.ZodObject<{
    key: z.ZodOptional<z.ZodString>;
    staticKey: z.ZodOptional<z.ZodString>;
    dynamicKey: z.ZodOptional<z.ZodString>;
    units: z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>;
    limit: z.ZodOptional<z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>>;
    duration: z.ZodOptional<z.ZodEnum<typeof RateLimitDuration>>;
}, z.core.$strip>;
export declare const DesiredWorkerLabelSchema: z.ZodOptional<z.ZodUnion<readonly [z.ZodString, z.ZodNumber, z.ZodObject<{
    value: z.ZodUnion<readonly [z.ZodString, z.ZodNumber]>;
    required: z.ZodOptional<z.ZodBoolean>;
    weight: z.ZodOptional<z.ZodNumber>;
    comparator: z.ZodOptional<z.ZodEnum<typeof WorkerLabelComparator>>;
}, z.core.$strip>]>>;
export declare const CreateStepSchema: z.ZodObject<{
    name: z.ZodString;
    parents: z.ZodOptional<z.ZodArray<z.ZodString>>;
    timeout: z.ZodOptional<z.ZodString>;
    retries: z.ZodOptional<z.ZodNumber>;
    rate_limits: z.ZodOptional<z.ZodArray<z.ZodObject<{
        key: z.ZodOptional<z.ZodString>;
        staticKey: z.ZodOptional<z.ZodString>;
        dynamicKey: z.ZodOptional<z.ZodString>;
        units: z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>;
        limit: z.ZodOptional<z.ZodUnion<readonly [z.ZodNumber, z.ZodString]>>;
        duration: z.ZodOptional<z.ZodEnum<typeof RateLimitDuration>>;
    }, z.core.$strip>>>;
    worker_labels: z.ZodOptional<z.ZodRecord<z.ZodAny, z.ZodLazy<z.ZodOptional<z.ZodUnion<readonly [z.ZodString, z.ZodNumber, z.ZodObject<{
        value: z.ZodUnion<readonly [z.ZodString, z.ZodNumber]>;
        required: z.ZodOptional<z.ZodBoolean>;
        weight: z.ZodOptional<z.ZodNumber>;
        comparator: z.ZodOptional<z.ZodEnum<typeof WorkerLabelComparator>>;
    }, z.core.$strip>]>>>>>;
    backoff: z.ZodOptional<z.ZodObject<{
        factor: z.ZodOptional<z.ZodNumber>;
        maxSeconds: z.ZodOptional<z.ZodNumber>;
    }, z.core.$strip>>;
}, z.core.$strip>;
export type NextStep = {
    [key: string]: JsonValue;
};
type TriggerData = Record<string, Record<string, any>>;
type ChildRunOpts = RunOpts & {
    key?: string;
    sticky?: boolean;
};
interface ContextData<T, K> {
    input: T;
    triggers: TriggerData;
    parents: Record<string, any>;
    triggered_by: string;
    user_data: K;
    step_run_errors: Record<string, string>;
}
export declare class V0ContextWorker {
    private worker;
    constructor(worker: InternalWorker);
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
export declare class V0Context<T, K = {}> {
    data: ContextData<T, K>;
    input: T;
    controller: AbortController;
    action: Action;
    v0: LegacyHatchetClient;
    worker: V0ContextWorker;
    overridesData: Record<string, any>;
    logger: Logger;
    spawnIndex: number;
    constructor(action: Action, client: LegacyHatchetClient, worker: InternalWorker);
    get abortController(): AbortController;
    get cancelled(): boolean;
    /**
     * Retrieves the output of a parent task.
     * @param parentTask - The a CreateTaskOpts or string of the parent task name.
     * @returns The output of the specified parent task.
     * @throws An error if the task output is not found.
     */
    parentOutput<L extends OutputType>(parentTask: CreateWorkflowTaskOpts<any, L> | string): Promise<L>;
    /**
     * Get the output of a task.
     * @param task - The name of the task to get the output for.
     * @returns The output of the task.
     * @throws An error if the task output is not found.
     * @deprecated use ctx.parentOutput instead
     */
    stepOutput<L = NextStep>(step: string): L;
    /**
     * Returns errors from any task runs in the workflow.
     * @returns A record mapping task names to error messages.
     * @throws A warning if no errors are found (this method should be used in on-failure tasks).
     * @deprecated use ctx.errors() instead
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
     * Determines if the workflow was triggered by an event.
     * @returns True if the workflow was triggered by an event, otherwise false.
     */
    triggeredByEvent(): boolean;
    /**
     * Gets the input data for the current workflow.
     * @returns The input data for the workflow.
     * @deprecated use task input parameter instead
     */
    workflowInput(): T;
    /**
     * Gets the name of the current workflow.
     * @returns The name of the workflow.
     */
    workflowName(): string;
    /**
     * Gets the user data associated with the workflow.
     * @returns The user data.
     */
    userData(): K;
    /**
     * Gets the name of the current task.
     * @returns The name of the task.
     * @deprecated use ctx.taskName instead
     */
    stepName(): string;
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
     * Gets the ID of the current task run.
     * @returns The task run ID.
     */
    taskRunExternalId(): string;
    /**
     * Gets the ID of the current task run.
     * @returns The task run ID.
     * @deprecated use taskRunExternalId instead
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
     */
    log(message: string, level?: LogLevel): void;
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
    /**
     * Runs multiple children workflows in parallel without waiting for their results.
     * @param children - An array of  objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of workflow run references to the enqueued runs.
     */
    bulkRunNoWaitChildren<Q extends JsonObject = any, P extends JsonObject = any>(children: Array<{
        workflow: string | Workflow | WorkflowV1<Q, P>;
        input: Q;
        options?: ChildRunOpts;
    }>): Promise<WorkflowRunRef<P>[]>;
    /**
     * Runs multiple children workflows in parallel and waits for all results.
     * @param children - An array of objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of results from the children workflows.
     */
    bulkRunChildren<Q extends JsonObject = any, P extends JsonObject = any>(children: Array<{
        workflow: string | Workflow | WorkflowV1<Q, P>;
        input: Q;
        options?: ChildRunOpts;
    }>): Promise<P[]>;
    /**
     * Spawns multiple workflows.
     *
     * @param workflows - An array of objects containing the workflow name, input data, and options for each workflow.
     * @returns A list of references to the spawned workflow runs.
     * @deprecated Use bulkRunNoWaitChildren or bulkRunChildren instead.
     */
    spawnWorkflows<Q extends JsonObject = any, P extends JsonObject = any>(workflows: Array<{
        workflow: string | Workflow | WorkflowV1<Q, P>;
        input: Q;
        options?: ChildRunOpts;
    }>): Promise<WorkflowRunRef<P>[]>;
    /**
     * Runs a new workflow and waits for its result.
     *
     * @param workflow - The workflow to run (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - An options object containing key, sticky, priority, and additionalMetadata.
     * @returns The result of the workflow.
     */
    runChild<Q extends JsonObject, P extends JsonObject>(workflow: string | Workflow | WorkflowV1<Q, P> | TaskWorkflowDeclaration<Q, P>, input: Q, options?: ChildRunOpts): Promise<P>;
    /**
     * Enqueues a new workflow without waiting for its result.
     *
     * @param workflow - The workflow to enqueue (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - An options object containing key, sticky, priority, and additionalMetadata.
     * @returns A reference to the spawned workflow run.
     */
    runNoWaitChild<Q extends JsonObject, P extends JsonObject>(workflow: string | Workflow | WorkflowV1<Q, P>, input: Q, options?: ChildRunOpts): Promise<WorkflowRunRef<P>>;
    /**
     * Spawns a new workflow.
     *
     * @param workflow - The workflow to spawn (name, Workflow instance, or WorkflowV1 instance).
     * @param input - The input data for the workflow.
     * @param options - Additional options for spawning the workflow.
     * @returns A reference to the spawned workflow run.
     * @deprecated Use runChild or runNoWaitChild instead.
     */
    spawnWorkflow<Q extends JsonObject, P extends JsonObject>(workflow: string | Workflow | WorkflowV1<Q, P> | TaskWorkflowDeclaration<Q, P>, input: Q, options?: ChildRunOpts): Promise<WorkflowRunRef<P>>;
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
}
export declare class V0DurableContext<T, K = {}> extends V0Context<T, K> {
    waitKey: number;
    /**
     * Pauses execution for the specified duration.
     * Duration is "global" meaning it will wait in real time regardless of transient failures like worker restarts.
     * @param duration - The duration to sleep for.
     * @returns A promise that resolves when the sleep duration has elapsed.
     */
    sleepFor(duration: Duration, readableDataKey?: string): Promise<Record<string, any>>;
    /**
     * Pauses execution until the specified conditions are met.
     * Conditions are "global" meaning they will wait in real time regardless of transient failures like worker restarts.
     * @param conditions - The conditions to wait for.
     * @returns A promise that resolves with the event that satisfied the conditions.
     */
    waitFor(conditions: Conditions | Conditions[]): Promise<Record<string, any>>;
}
export type StepRunFunction<T, K> = (ctx: V0Context<T, K>) => Promise<NextStep | void> | NextStep | void;
/**
 * A step is a unit of work that can be run by a worker.
 * It is defined by a name, a function that returns the next step, and optional configuration.
 * @deprecated use hatchet.workflows.task factory instead
 */
export interface CreateStep<T, K> extends z.infer<typeof CreateStepSchema> {
    run: StepRunFunction<T, K>;
}
export declare function mapRateLimit(limits: CreateStep<any, any>['rate_limits']): CreateStepRateLimit[];
export {};
