import { CreateStepRateLimit, StickyStrategy } from '../../../protoc/workflows';
import { CreateWorkflowVersionRequest, TaskBatchConfig } from '../../../protoc/v1/workflows';
import type { DesiredWorkerLabels } from '../../../protoc/v1/shared/trigger';
import type { BaseWorkflowDeclaration, WorkflowDefinition } from '../../declaration';
import type { Concurrency, CreateWorkflowDurableTaskOpts, CreateWorkflowTaskOpts } from '../../task';
import { Duration } from '../duration';
export declare const ON_FAILURE_TASK_NAME = "on-failure-task";
export declare const ON_SUCCESS_TASK_NAME = "on-success-task";
export interface WorkflowProtoOptions {
    /** The namespace prefixed onto the workflow name and its event triggers. */
    namespace?: string;
    /**
     * When true the on-success task is left out, matching how a durable-only registration
     * behaves on the worker. Defaults to false.
     */
    durable?: boolean;
}
/**
 * Applies the namespace to a workflow definition and appends the on-success task, if the
 * workflow declares one, as a regular task whose parents are the leaves of the DAG.
 * Returns a new definition; the declaration is not mutated. Idempotent, so a definition
 * that was already normalized comes back unchanged.
 */
export declare function normalizeWorkflowDefinition(definition: WorkflowDefinition | BaseWorkflowDeclaration<any, any>, opts?: WorkflowProtoOptions): WorkflowDefinition;
export declare function mapStickyStrategyPb(sticky: WorkflowDefinition['sticky']): StickyStrategy | undefined;
/**
 * Builds the registration request for a workflow. Accepts a declaration or its
 * definition; the namespace is applied and the on-success task appended the same way
 * `normalizeWorkflowDefinition` does, so callers may pass either the raw declaration or
 * an already normalized definition.
 *
 * Action ids are `<workflow>:<task>`, lowercased (see `createActionId`).
 */
export declare function workflowToProto(definition: WorkflowDefinition | BaseWorkflowDeclaration<any, any>, opts?: WorkflowProtoOptions): CreateWorkflowVersionRequest;
export declare function mapWorkerLabelPb(in_: CreateWorkflowTaskOpts<any, any>['desiredWorkerLabels']): Record<string, DesiredWorkerLabels>;
/** The action id of a workflow's on-failure task. */
export declare function onFailureTaskName(workflow: Pick<WorkflowDefinition, 'name'>): string;
export type LeafableTask = CreateWorkflowTaskOpts<any, any> | CreateWorkflowDurableTaskOpts<any, any>;
export declare function getLeaves(tasks: LeafableTask[]): LeafableTask[];
export declare function isLeafTask(task: LeafableTask, allTasks: LeafableTask[]): boolean;
/** Durable tasks stay on the durable pool; slotCost applies only to the default pool. */
export declare function mapSlotRequestsPb(task: {
    slotRequests?: Record<string, number>;
    slotCost?: number;
}, isDurable: boolean): Record<string, number>;
export declare function mapRateLimitPb(limits: CreateWorkflowTaskOpts<any, any>['rateLimits']): CreateStepRateLimit[];
/** Batch tasks are only available on non-durable tasks; durable tasks never carry `batch`. */
export declare function batchOf(task: CreateWorkflowTaskOpts<any, any> | CreateWorkflowDurableTaskOpts<any, any>): CreateWorkflowTaskOpts<any, any>['batch'];
export declare function mapConcurrencyPb(entries: Concurrency[]): {
    expression: string;
    maxRuns: number | undefined;
    limitStrategy: import("../../task").ConcurrencyLimitStrategy | undefined;
    name: string | undefined;
    isTenantScoped: boolean | undefined;
    maxRunsExpression: string | undefined;
}[];
export declare function taskConcurrencyArr(task: {
    concurrency?: Concurrency | Concurrency[];
}, workflow: {
    taskDefaults?: {
        concurrency?: Concurrency | Concurrency[];
    };
}): Concurrency[];
export declare function assertValidConcurrencyArr(concurrency: Concurrency[] | undefined): void;
export declare function mapBatchConfigPb(batch: CreateWorkflowTaskOpts<any, any>['batch']): TaskBatchConfig | undefined;
export declare function resolveExecutionTimeout(task: {
    executionTimeout?: Duration;
    timeout?: Duration;
}, workflowDefaults?: {
    executionTimeout?: Duration;
}): string;
export declare function resolveScheduleTimeout(task: {
    scheduleTimeout?: Duration;
}, workflowDefaults?: {
    scheduleTimeout?: Duration;
}): string | undefined;
