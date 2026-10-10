import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import type { CallContext, CallOptions } from 'nice-grpc-common';
import { TaskConditions } from './shared/condition';
import { DesiredWorkerLabels } from './shared/trigger';
export declare const protobufPackage = "v1";
export declare enum StickyStrategy {
    SOFT = 0,
    HARD = 1,
    UNRECOGNIZED = -1
}
export declare function stickyStrategyFromJSON(object: any): StickyStrategy;
export declare function stickyStrategyToJSON(object: StickyStrategy): string;
export declare enum RateLimitDuration {
    SECOND = 0,
    MINUTE = 1,
    HOUR = 2,
    DAY = 3,
    WEEK = 4,
    MONTH = 5,
    YEAR = 6,
    UNRECOGNIZED = -1
}
export declare function rateLimitDurationFromJSON(object: any): RateLimitDuration;
export declare function rateLimitDurationToJSON(object: RateLimitDuration): string;
export declare enum RunStatus {
    QUEUED = 0,
    RUNNING = 1,
    COMPLETED = 2,
    FAILED = 3,
    CANCELLED = 4,
    EVICTED = 5,
    UNRECOGNIZED = -1
}
export declare function runStatusFromJSON(object: any): RunStatus;
export declare function runStatusToJSON(object: RunStatus): string;
export declare enum IdempotencyMethod {
    TTL = 0,
    STATUS = 1,
    UNRECOGNIZED = -1
}
export declare function idempotencyMethodFromJSON(object: any): IdempotencyMethod;
export declare function idempotencyMethodToJSON(object: IdempotencyMethod): string;
export declare enum ConcurrencyLimitStrategy {
    CANCEL_IN_PROGRESS = 0,
    /** DROP_NEWEST - deprecated */
    DROP_NEWEST = 1,
    /** QUEUE_NEWEST - deprecated */
    QUEUE_NEWEST = 2,
    GROUP_ROUND_ROBIN = 3,
    CANCEL_NEWEST = 4,
    CANCEL_QUEUED_EXCEPT_NEWEST = 5,
    CANCEL_QUEUED_EXCEPT_OLDEST = 6,
    UNRECOGNIZED = -1
}
export declare function concurrencyLimitStrategyFromJSON(object: any): ConcurrencyLimitStrategy;
export declare function concurrencyLimitStrategyToJSON(object: ConcurrencyLimitStrategy): string;
export interface CancelTasksRequest {
    /** a list of external UUIDs */
    externalIds: string[];
    filter?: TasksFilter | undefined;
}
export interface ReplayTasksRequest {
    /** a list of external UUIDs */
    externalIds: string[];
    filter?: TasksFilter | undefined;
}
export interface TasksFilter {
    statuses: string[];
    since: Date | undefined;
    until?: Date | undefined;
    workflowIds: string[];
    additionalMetadata: string[];
}
export interface CancelTasksResponse {
    cancelledTasks: string[];
}
export interface ReplayTasksResponse {
    replayedTasks: string[];
}
export interface TriggerWorkflowRunRequest {
    workflowName: string;
    input: Uint8Array;
    additionalMetadata: Uint8Array;
    priority?: number | undefined;
    desiredWorkerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
}
export interface TriggerWorkflowRunRequest_DesiredWorkerLabelsEntry {
    key: string;
    value: DesiredWorkerLabels | undefined;
}
export interface TriggerWorkflowRunResponse {
    externalId: string;
}
export interface BranchDurableTaskRequest {
    /** (required) the external id (uuid) of the durable task */
    taskExternalId: string;
    /** (required) the node id to branch from */
    nodeId: number;
    /** (required) the branch id to branch from */
    branchId: number;
}
export interface BranchDurableTaskResponse {
    /** the external id of the durable task */
    taskExternalId: string;
    /** the node id of the new entry */
    nodeId: number;
    /** the branch id of the new entry */
    branchId: number;
}
/** CreateWorkflowVersionRequest represents options to create a workflow version. */
export interface CreateWorkflowVersionRequest {
    /** (required) the workflow name */
    name: string;
    /** (optional) the workflow description */
    description: string;
    /** (optional) the workflow version */
    version: string;
    /** (optional) event triggers for the workflow */
    eventTriggers: string[];
    /** (optional) cron triggers for the workflow */
    cronTriggers: string[];
    /** (required) the workflow jobs */
    tasks: CreateTaskOpts[];
    /** Deprecated: use concurrency_arr instead */
    concurrency: Concurrency | undefined;
    /** (optional) the input for the cron trigger */
    cronInput?: string | undefined;
    /** (optional) the job to run on failure */
    onFailureTask?: CreateTaskOpts | undefined;
    /** (optional) the sticky strategy for assigning tasks to workers */
    sticky?: StickyStrategy | undefined;
    /** (optional) the default priority for the workflow */
    defaultPriority?: number | undefined;
    /** (optional) the workflow concurrency options */
    concurrencyArr: Concurrency[];
    /** (optional) the default filters for the workflow */
    defaultFilters: DefaultFilter[];
    /** (optional) the JSON schema for the workflow input */
    inputJsonSchema?: Uint8Array | undefined;
    /** (optional) idempotency configuration for the workflow */
    idempotency?: IdempotencyConfig | undefined;
}
export interface IdempotencyConfig {
    /** a CEL expression for determining the idempotency key for workflow runs */
    expression: string;
    /** time-to-live for idempotency keys in milliseconds. if the method is `STATUS`, this is a "fallback" - the longest the key can live before it's evicted */
    ttlMs: number;
    /** the method to use for idempotency, defaults to TTL */
    method?: IdempotencyMethod | undefined;
}
export interface IdempotencyCollisionError {
    /** the external ID of the existing workflow run that caused the collision */
    existingRunExternalId: string;
    /** the external ID of the workflow run that caused the collision */
    collidingRunExternalId: string;
}
export interface BulkTriggerIdempotencyCollisionError {
    /** the external IDs of the successfully triggered workflow runs */
    successfulWorkflowRunExternalIds: string[];
    /** the idempotency collision errors */
    collisions: IdempotencyCollisionError[];
}
export interface DefaultFilter {
    /** (required) the CEL expression for the filter */
    expression: string;
    /** (required) the scope of the filter */
    scope: string;
    /** (optional) the payload for the filter, if any. A JSON object as a string. */
    payload?: Uint8Array | undefined;
}
/**
 * Concurrency declares one entry in a concurrency chain. Entries are processed in array
 * order, so a tenant-scoped entry may come before or after a workflow-scoped one.
 *
 * A tenant-scoped entry (is_tenant_scoped = true) defines (or updates in place) a strategy
 * shared across workflows, keyed by name: every task declaring the same name consumes the
 * same concurrency limit. Registrations whose chains order the same tenant-scoped
 * strategies inconsistently are rejected, since inconsistent orders can deadlock.
 */
export interface Concurrency {
    /** (required) the expression to use for concurrency */
    expression: string;
    /** (optional) the maximum number of concurrent workflow runs, default 1 */
    maxRuns?: number | undefined;
    /** (optional) the strategy to use when the concurrency limit is reached, default CANCEL_IN_PROGRESS */
    limitStrategy?: ConcurrencyLimitStrategy | undefined;
    /** (required when is_tenant_scoped) the strategy name; unique per tenant for tenant-scoped strategies */
    name?: string | undefined;
    /** (optional) when true, the entry is a tenant-scoped strategy shared across workflows, default false */
    isTenantScoped?: boolean | undefined;
    /** (optional) CEL expression over task input returning the max runs for that task's concurrency group; the group's effective limit is the value from its most recently created task. Overrides max_runs per group; a non-integer or negative result fails the task, and 0 holds the group until a newer task raises the limit */
    maxRunsExpression?: string | undefined;
}
export interface TaskBatchConfig {
    /** (required) maximum items per batch */
    batchMaxSize: number;
    /** (optional) time before batch flushes (milliseconds) */
    batchMaxIntervalMs?: number | undefined;
    /** (optional) partition key for fairness (prevents mixing tenants) */
    batchGroupKey?: string | undefined;
    /** (optional) concurrent batches per group */
    batchGroupMaxRuns?: number | undefined;
    /** (optional) when true, the handler returns one value broadcast to all callers; when false (default), the handler returns a dict keyed by step run id */
    broadcastOutput?: boolean | undefined;
}
/** CreateTaskOpts represents options to create a task. */
export interface CreateTaskOpts {
    /** (required) the task name */
    readableId: string;
    /** (required) the task action id */
    action: string;
    /** (optional) the task timeout */
    timeout: string;
    /** (optional) the task inputs, assuming string representation of JSON */
    inputs: string;
    /** (optional) the task parents. if none are passed in, this is a root task */
    parents: string[];
    /** (optional) the number of retries for the task, default 0 */
    retries: number;
    /** (optional) the rate limits for the task */
    rateLimits: CreateTaskRateLimit[];
    /** (optional) the desired worker affinity state for the task */
    workerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
    /** (optional) the retry backoff factor for the task */
    backoffFactor?: number | undefined;
    /** (optional) the maximum backoff time for the task */
    backoffMaxSeconds?: number | undefined;
    /** (optional) the task concurrency options */
    concurrency: Concurrency[];
    /** (optional) the task conditions for creating the task */
    conditions?: TaskConditions | undefined;
    /** (optional) the timeout for the schedule */
    scheduleTimeout?: string | undefined;
    /** (optional) whether the task is durable */
    isDurable: boolean;
    /** (optional) slot requests (slot_type -> units) */
    slotRequests: {
        [key: string]: number;
    };
    /** (optional) batch execution configuration */
    batch?: TaskBatchConfig | undefined;
}
export interface CreateTaskOpts_WorkerLabelsEntry {
    key: string;
    value: DesiredWorkerLabels | undefined;
}
export interface CreateTaskOpts_SlotRequestsEntry {
    key: string;
    value: number;
}
export interface CreateTaskRateLimit {
    /** (required) the key for the rate limit */
    key: string;
    /** (optional) the number of units this task consumes */
    units?: number | undefined;
    /** (optional) a CEL expression for determining the rate limit key */
    keyExpr?: string | undefined;
    /** (optional) a CEL expression for determining the number of units consumed */
    unitsExpr?: string | undefined;
    /** (optional) a CEL expression for determining the total amount of rate limit units */
    limitValuesExpr?: string | undefined;
    /** (optional) the default rate limit window to use for dynamic rate limits */
    duration?: RateLimitDuration | undefined;
}
/** CreateWorkflowVersionResponse represents the response after creating a workflow version. */
export interface CreateWorkflowVersionResponse {
    id: string;
    workflowId: string;
}
export interface GetRunDetailsRequest {
    /** (required) the external id (uuid) of the workflow run */
    externalId: string;
}
export interface TaskRunDetail {
    /** the external id (uuid) of the task run */
    externalId: string;
    /** the status of the task run */
    status: RunStatus;
    /** (optional) error message from the task run, if any */
    error?: string | undefined;
    /** (optional) the output payload for the task run */
    output?: Uint8Array | undefined;
    /** the readable id of the task */
    readableId: string;
    /** whether the task has been evicted from a worker (status will be RUNNING) */
    isEvicted: boolean;
}
export interface GetRunDetailsResponse {
    /** the input payload for the workflow run */
    input: Uint8Array;
    /** the status of the workflow run */
    status: RunStatus;
    /** map of task run external ids to their details */
    taskRuns: {
        [key: string]: TaskRunDetail;
    };
    /** indicates if the workflow run is done */
    done: boolean;
    /** (optional) additional metadata for the workflow run */
    additionalMetadata: Uint8Array;
    /** whether any task in this run has been evicted */
    isEvicted: boolean;
}
export interface GetRunDetailsResponse_TaskRunsEntry {
    key: string;
    value: TaskRunDetail | undefined;
}
export declare const CancelTasksRequest: MessageFns<CancelTasksRequest>;
export declare const ReplayTasksRequest: MessageFns<ReplayTasksRequest>;
export declare const TasksFilter: MessageFns<TasksFilter>;
export declare const CancelTasksResponse: MessageFns<CancelTasksResponse>;
export declare const ReplayTasksResponse: MessageFns<ReplayTasksResponse>;
export declare const TriggerWorkflowRunRequest: MessageFns<TriggerWorkflowRunRequest>;
export declare const TriggerWorkflowRunRequest_DesiredWorkerLabelsEntry: MessageFns<TriggerWorkflowRunRequest_DesiredWorkerLabelsEntry>;
export declare const TriggerWorkflowRunResponse: MessageFns<TriggerWorkflowRunResponse>;
export declare const BranchDurableTaskRequest: MessageFns<BranchDurableTaskRequest>;
export declare const BranchDurableTaskResponse: MessageFns<BranchDurableTaskResponse>;
export declare const CreateWorkflowVersionRequest: MessageFns<CreateWorkflowVersionRequest>;
export declare const IdempotencyConfig: MessageFns<IdempotencyConfig>;
export declare const IdempotencyCollisionError: MessageFns<IdempotencyCollisionError>;
export declare const BulkTriggerIdempotencyCollisionError: MessageFns<BulkTriggerIdempotencyCollisionError>;
export declare const DefaultFilter: MessageFns<DefaultFilter>;
export declare const Concurrency: MessageFns<Concurrency>;
export declare const TaskBatchConfig: MessageFns<TaskBatchConfig>;
export declare const CreateTaskOpts: MessageFns<CreateTaskOpts>;
export declare const CreateTaskOpts_WorkerLabelsEntry: MessageFns<CreateTaskOpts_WorkerLabelsEntry>;
export declare const CreateTaskOpts_SlotRequestsEntry: MessageFns<CreateTaskOpts_SlotRequestsEntry>;
export declare const CreateTaskRateLimit: MessageFns<CreateTaskRateLimit>;
export declare const CreateWorkflowVersionResponse: MessageFns<CreateWorkflowVersionResponse>;
export declare const GetRunDetailsRequest: MessageFns<GetRunDetailsRequest>;
export declare const TaskRunDetail: MessageFns<TaskRunDetail>;
export declare const GetRunDetailsResponse: MessageFns<GetRunDetailsResponse>;
export declare const GetRunDetailsResponse_TaskRunsEntry: MessageFns<GetRunDetailsResponse_TaskRunsEntry>;
/** AdminService represents a set of RPCs for admin management of tasks, workflows, etc. */
export type AdminServiceDefinition = typeof AdminServiceDefinition;
export declare const AdminServiceDefinition: {
    readonly name: "AdminService";
    readonly fullName: "v1.AdminService";
    readonly methods: {
        readonly putWorkflow: {
            readonly name: "PutWorkflow";
            readonly requestType: typeof CreateWorkflowVersionRequest;
            readonly requestStream: false;
            readonly responseType: typeof CreateWorkflowVersionResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly cancelTasks: {
            readonly name: "CancelTasks";
            readonly requestType: typeof CancelTasksRequest;
            readonly requestStream: false;
            readonly responseType: typeof CancelTasksResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly replayTasks: {
            readonly name: "ReplayTasks";
            readonly requestType: typeof ReplayTasksRequest;
            readonly requestStream: false;
            readonly responseType: typeof ReplayTasksResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly triggerWorkflowRun: {
            readonly name: "TriggerWorkflowRun";
            readonly requestType: typeof TriggerWorkflowRunRequest;
            readonly requestStream: false;
            readonly responseType: typeof TriggerWorkflowRunResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly getRunDetails: {
            readonly name: "GetRunDetails";
            readonly requestType: typeof GetRunDetailsRequest;
            readonly requestStream: false;
            readonly responseType: typeof GetRunDetailsResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly branchDurableTask: {
            readonly name: "BranchDurableTask";
            readonly requestType: typeof BranchDurableTaskRequest;
            readonly requestStream: false;
            readonly responseType: typeof BranchDurableTaskResponse;
            readonly responseStream: false;
            readonly options: {};
        };
    };
};
export interface AdminServiceImplementation<CallContextExt = {}> {
    putWorkflow(request: CreateWorkflowVersionRequest, context: CallContext & CallContextExt): Promise<DeepPartial<CreateWorkflowVersionResponse>>;
    cancelTasks(request: CancelTasksRequest, context: CallContext & CallContextExt): Promise<DeepPartial<CancelTasksResponse>>;
    replayTasks(request: ReplayTasksRequest, context: CallContext & CallContextExt): Promise<DeepPartial<ReplayTasksResponse>>;
    triggerWorkflowRun(request: TriggerWorkflowRunRequest, context: CallContext & CallContextExt): Promise<DeepPartial<TriggerWorkflowRunResponse>>;
    getRunDetails(request: GetRunDetailsRequest, context: CallContext & CallContextExt): Promise<DeepPartial<GetRunDetailsResponse>>;
    branchDurableTask(request: BranchDurableTaskRequest, context: CallContext & CallContextExt): Promise<DeepPartial<BranchDurableTaskResponse>>;
}
export interface AdminServiceClient<CallOptionsExt = {}> {
    putWorkflow(request: DeepPartial<CreateWorkflowVersionRequest>, options?: CallOptions & CallOptionsExt): Promise<CreateWorkflowVersionResponse>;
    cancelTasks(request: DeepPartial<CancelTasksRequest>, options?: CallOptions & CallOptionsExt): Promise<CancelTasksResponse>;
    replayTasks(request: DeepPartial<ReplayTasksRequest>, options?: CallOptions & CallOptionsExt): Promise<ReplayTasksResponse>;
    triggerWorkflowRun(request: DeepPartial<TriggerWorkflowRunRequest>, options?: CallOptions & CallOptionsExt): Promise<TriggerWorkflowRunResponse>;
    getRunDetails(request: DeepPartial<GetRunDetailsRequest>, options?: CallOptions & CallOptionsExt): Promise<GetRunDetailsResponse>;
    branchDurableTask(request: DeepPartial<BranchDurableTaskRequest>, options?: CallOptions & CallOptionsExt): Promise<BranchDurableTaskResponse>;
}
type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;
export type DeepPartial<T> = T extends Builtin ? T : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {} ? {
    [K in keyof T]?: DeepPartial<T[K]>;
} : Partial<T>;
export interface MessageFns<T> {
    encode(message: T, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): T;
    fromJSON(object: any): T;
    toJSON(message: T): unknown;
    create(base?: DeepPartial<T>): T;
    fromPartial(object: DeepPartial<T>): T;
}
export {};
