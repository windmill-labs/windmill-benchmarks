/**
 * The edge entry point: everything needed to declare Hatchet tasks and run them from a
 * runtime that has no Hatchet client, such as a Cloudflare Worker or a Vercel Function.
 *
 * Nothing exported here imports from Node. `scripts/check-edge-entry.mjs` bundles this
 * module for a browser-like target and fails the build if a Node builtin sneaks in.
 *
 * ```typescript
 * import { declarations, workflowToProto } from './';
 *
 * const { task } = declarations();
 * const greet = task({ name: 'greet', fn: (input: { name: string }) => ({ hi: input.name }) });
 * const request = workflowToProto(greet, { namespace: 'prod_' });
 * ```
 * @module Edge
 */
export { BaseWorkflowDeclaration, CreateBatchTaskWorkflow, CreateDurableTaskWorkflow, CreateTaskWorkflow, CreateWorkflow, Priority, StickyStrategy, TaskWorkflowDeclaration, WorkflowDeclaration, } from '../v1/declaration';
export type { CreateBaseWorkflowOpts, CreateBatchTaskWorkflowOpts, CreateDurableTaskWorkflowOpts, CreateTaskWorkflowOpts, CreateWorkflowOpts, RunManyOpt, RunOpts, StickyStrategyInput, TaskDefaults, TaskOutput, TaskOutputType, WorkflowDefinition, } from '../v1/declaration';
export * from '../v1/task';
export * from '../v1/types';
export * from '../v1/client/duration';
export * from '../v1/conditions';
export { conditionsToPb, taskConditionsToPb } from '../v1/conditions/transformer';
export { applyNamespace } from '../util/apply-namespace';
export { createAction, createActionId, workflowNameFromAction, } from '../clients/dispatcher/action';
export type { Action, ActionKey } from '../clients/dispatcher/action';
export { Context, ContextWorker, DurableContext, computeMemoKey, } from '../v1/client/worker/context';
export type { SleepForOptions, SleepResult } from '../v1/client/worker/context';
export type { CancelBatchRequest, ContextRuntime, DesiredWorkerLabel, DurableContextOptions, DurableTransport, SpawnRunOptions, SpawnRunRequest, WorkerLabels, } from '../v1/client/worker/runtime';
export { isContextRuntime } from '../v1/client/worker/runtime';
export type { DurableTaskEventAck, DurableTaskEventLogEntryResult, DurableTaskEventMemoAck, DurableTaskEventRunAck, DurableTaskEventWaitForAck, DurableTaskRunAckEntryResult, DurableTaskSendEvent, MemoEvent, RunChildrenEvent, WaitForEvent, } from '../clients/listeners/durable-listener/durable-events';
export { Logger, LogLevelEnum } from '../util/logger/logger';
export type { LogExtra, LogLevel } from '../util/logger/logger';
export { ParentRunContextManager, parentRunContextManager, } from '../v1/parent-run-context-vars';
export type { ParentRunContext, ParentRunContextStorage, } from '../v1/parent-run-context-vars';
export { default as HatchetError, getErrorMessage, toHatchetError, } from '../util/errors/hatchet-error';
export { NonDeterminismError } from '../util/errors/non-determinism-error';
export { TaskRunTerminatedError, isTaskRunTerminatedError, } from '../util/errors/task-run-terminated-error';
export type { TaskRunTerminationReason } from '../util/errors/task-run-terminated-error';
export { AbortError, createAbortError, isAbortError, rethrowIfAborted, throwIfAborted, } from '../util/abort-error';
export { DEFAULT_DURABLE_TASK_EVICTION_POLICY, EvictionPolicy, } from '../v1/client/worker/eviction/eviction-policy';
export { MinEngineVersion, supportsEviction } from '../v1/client/worker/engine-version';
export { ON_FAILURE_TASK_NAME, ON_SUCCESS_TASK_NAME, normalizeWorkflowDefinition, onFailureTaskName, workflowToProto, } from '../v1/client/worker/workflow-proto';
export type { WorkflowProtoOptions } from '../v1/client/worker/workflow-proto';
export { CreateWorkflowVersionRequest } from '../protoc/v1/workflows';
export { AssignedAction } from '../protoc/dispatcher';
export { DurableTaskRequest, DurableTaskResponse } from '../protoc/v1/dispatcher';
export { declarations } from './declarations';
export type { Declarations } from './declarations';
