import type { GenEnum, GenFile, GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { TaskConditions } from "./shared/condition_pb";
import type { DesiredWorkerLabels } from "./shared/trigger_pb";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file v1/workflows.proto.
 */
export declare const file_v1_workflows: GenFile;
/**
 * @generated from message v1.CancelTasksRequest
 */
export type CancelTasksRequest = Message<"v1.CancelTasksRequest"> & {
    /**
     * a list of external UUIDs
     *
     * @generated from field: repeated string external_ids = 1;
     */
    externalIds: string[];
    /**
     * @generated from field: optional v1.TasksFilter filter = 2;
     */
    filter?: TasksFilter | undefined;
};
/**
 * Describes the message v1.CancelTasksRequest.
 * Use `create(CancelTasksRequestSchema)` to create a new message.
 */
export declare const CancelTasksRequestSchema: GenMessage<CancelTasksRequest>;
/**
 * @generated from message v1.ReplayTasksRequest
 */
export type ReplayTasksRequest = Message<"v1.ReplayTasksRequest"> & {
    /**
     * a list of external UUIDs
     *
     * @generated from field: repeated string external_ids = 1;
     */
    externalIds: string[];
    /**
     * @generated from field: optional v1.TasksFilter filter = 2;
     */
    filter?: TasksFilter | undefined;
};
/**
 * Describes the message v1.ReplayTasksRequest.
 * Use `create(ReplayTasksRequestSchema)` to create a new message.
 */
export declare const ReplayTasksRequestSchema: GenMessage<ReplayTasksRequest>;
/**
 * @generated from message v1.TasksFilter
 */
export type TasksFilter = Message<"v1.TasksFilter"> & {
    /**
     * @generated from field: repeated string statuses = 1;
     */
    statuses: string[];
    /**
     * @generated from field: google.protobuf.Timestamp since = 2;
     */
    since?: Timestamp | undefined;
    /**
     * @generated from field: optional google.protobuf.Timestamp until = 3;
     */
    until?: Timestamp | undefined;
    /**
     * @generated from field: repeated string workflow_ids = 4;
     */
    workflowIds: string[];
    /**
     * @generated from field: repeated string additional_metadata = 5;
     */
    additionalMetadata: string[];
};
/**
 * Describes the message v1.TasksFilter.
 * Use `create(TasksFilterSchema)` to create a new message.
 */
export declare const TasksFilterSchema: GenMessage<TasksFilter>;
/**
 * @generated from message v1.CancelTasksResponse
 */
export type CancelTasksResponse = Message<"v1.CancelTasksResponse"> & {
    /**
     * @generated from field: repeated string cancelled_tasks = 1;
     */
    cancelledTasks: string[];
};
/**
 * Describes the message v1.CancelTasksResponse.
 * Use `create(CancelTasksResponseSchema)` to create a new message.
 */
export declare const CancelTasksResponseSchema: GenMessage<CancelTasksResponse>;
/**
 * @generated from message v1.ReplayTasksResponse
 */
export type ReplayTasksResponse = Message<"v1.ReplayTasksResponse"> & {
    /**
     * @generated from field: repeated string replayed_tasks = 1;
     */
    replayedTasks: string[];
};
/**
 * Describes the message v1.ReplayTasksResponse.
 * Use `create(ReplayTasksResponseSchema)` to create a new message.
 */
export declare const ReplayTasksResponseSchema: GenMessage<ReplayTasksResponse>;
/**
 * @generated from message v1.TriggerWorkflowRunRequest
 */
export type TriggerWorkflowRunRequest = Message<"v1.TriggerWorkflowRunRequest"> & {
    /**
     * @generated from field: string workflow_name = 1;
     */
    workflowName: string;
    /**
     * @generated from field: bytes input = 2;
     */
    input: Uint8Array;
    /**
     * @generated from field: bytes additional_metadata = 3;
     */
    additionalMetadata: Uint8Array;
    /**
     * @generated from field: optional int32 priority = 4;
     */
    priority?: number | undefined;
    /**
     * @generated from field: map<string, v1.DesiredWorkerLabels> desired_worker_labels = 5;
     */
    desiredWorkerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
};
/**
 * Describes the message v1.TriggerWorkflowRunRequest.
 * Use `create(TriggerWorkflowRunRequestSchema)` to create a new message.
 */
export declare const TriggerWorkflowRunRequestSchema: GenMessage<TriggerWorkflowRunRequest>;
/**
 * @generated from message v1.TriggerWorkflowRunResponse
 */
export type TriggerWorkflowRunResponse = Message<"v1.TriggerWorkflowRunResponse"> & {
    /**
     * @generated from field: string external_id = 1;
     */
    externalId: string;
};
/**
 * Describes the message v1.TriggerWorkflowRunResponse.
 * Use `create(TriggerWorkflowRunResponseSchema)` to create a new message.
 */
export declare const TriggerWorkflowRunResponseSchema: GenMessage<TriggerWorkflowRunResponse>;
/**
 * @generated from message v1.BranchDurableTaskRequest
 */
export type BranchDurableTaskRequest = Message<"v1.BranchDurableTaskRequest"> & {
    /**
     * (required) the external id (uuid) of the durable task
     *
     * @generated from field: string task_external_id = 1;
     */
    taskExternalId: string;
    /**
     * (required) the node id to branch from
     *
     * @generated from field: int64 node_id = 2;
     */
    nodeId: bigint;
    /**
     * (required) the branch id to branch from
     *
     * @generated from field: int64 branch_id = 3;
     */
    branchId: bigint;
};
/**
 * Describes the message v1.BranchDurableTaskRequest.
 * Use `create(BranchDurableTaskRequestSchema)` to create a new message.
 */
export declare const BranchDurableTaskRequestSchema: GenMessage<BranchDurableTaskRequest>;
/**
 * @generated from message v1.BranchDurableTaskResponse
 */
export type BranchDurableTaskResponse = Message<"v1.BranchDurableTaskResponse"> & {
    /**
     * the external id of the durable task
     *
     * @generated from field: string task_external_id = 1;
     */
    taskExternalId: string;
    /**
     * the node id of the new entry
     *
     * @generated from field: int64 node_id = 2;
     */
    nodeId: bigint;
    /**
     * the branch id of the new entry
     *
     * @generated from field: int64 branch_id = 3;
     */
    branchId: bigint;
};
/**
 * Describes the message v1.BranchDurableTaskResponse.
 * Use `create(BranchDurableTaskResponseSchema)` to create a new message.
 */
export declare const BranchDurableTaskResponseSchema: GenMessage<BranchDurableTaskResponse>;
/**
 * CreateWorkflowVersionRequest represents options to create a workflow version.
 *
 * @generated from message v1.CreateWorkflowVersionRequest
 */
export type CreateWorkflowVersionRequest = Message<"v1.CreateWorkflowVersionRequest"> & {
    /**
     * (required) the workflow name
     *
     * @generated from field: string name = 1;
     */
    name: string;
    /**
     * (optional) the workflow description
     *
     * @generated from field: string description = 2;
     */
    description: string;
    /**
     * (optional) the workflow version
     *
     * @generated from field: string version = 3;
     */
    version: string;
    /**
     * (optional) event triggers for the workflow
     *
     * @generated from field: repeated string event_triggers = 4;
     */
    eventTriggers: string[];
    /**
     * (optional) cron triggers for the workflow
     *
     * @generated from field: repeated string cron_triggers = 5;
     */
    cronTriggers: string[];
    /**
     * (required) the workflow jobs
     *
     * @generated from field: repeated v1.CreateTaskOpts tasks = 6;
     */
    tasks: CreateTaskOpts[];
    /**
     * Deprecated: use concurrency_arr instead
     *
     * (optional) the workflow concurrency options
     *
     * @generated from field: v1.Concurrency concurrency = 7;
     */
    concurrency?: Concurrency | undefined;
    /**
     * (optional) the input for the cron trigger
     *
     * @generated from field: optional string cron_input = 8;
     */
    cronInput?: string | undefined;
    /**
     * (optional) the job to run on failure
     *
     * @generated from field: optional v1.CreateTaskOpts on_failure_task = 9;
     */
    onFailureTask?: CreateTaskOpts | undefined;
    /**
     * (optional) the sticky strategy for assigning tasks to workers
     *
     * @generated from field: optional v1.StickyStrategy sticky = 10;
     */
    sticky?: StickyStrategy | undefined;
    /**
     * (optional) the default priority for the workflow
     *
     * @generated from field: optional int32 default_priority = 11;
     */
    defaultPriority?: number | undefined;
    /**
     * (optional) the workflow concurrency options
     *
     * @generated from field: repeated v1.Concurrency concurrency_arr = 12;
     */
    concurrencyArr: Concurrency[];
    /**
     * (optional) the default filters for the workflow
     *
     * @generated from field: repeated v1.DefaultFilter default_filters = 13;
     */
    defaultFilters: DefaultFilter[];
    /**
     * (optional) the JSON schema for the workflow input
     *
     * @generated from field: optional bytes input_json_schema = 14;
     */
    inputJsonSchema?: Uint8Array | undefined;
    /**
     * (optional) idempotency configuration for the workflow
     *
     * @generated from field: optional v1.IdempotencyConfig idempotency = 15;
     */
    idempotency?: IdempotencyConfig | undefined;
};
/**
 * Describes the message v1.CreateWorkflowVersionRequest.
 * Use `create(CreateWorkflowVersionRequestSchema)` to create a new message.
 */
export declare const CreateWorkflowVersionRequestSchema: GenMessage<CreateWorkflowVersionRequest>;
/**
 * @generated from message v1.IdempotencyConfig
 */
export type IdempotencyConfig = Message<"v1.IdempotencyConfig"> & {
    /**
     * a CEL expression for determining the idempotency key for workflow runs
     *
     * @generated from field: string expression = 1;
     */
    expression: string;
    /**
     * time-to-live for idempotency keys in milliseconds. if the method is `STATUS`, this is a "fallback" - the longest the key can live before it's evicted
     *
     * @generated from field: int64 ttl_ms = 2;
     */
    ttlMs: bigint;
    /**
     * the method to use for idempotency, defaults to TTL
     *
     * @generated from field: optional v1.IdempotencyMethod method = 3;
     */
    method?: IdempotencyMethod | undefined;
};
/**
 * Describes the message v1.IdempotencyConfig.
 * Use `create(IdempotencyConfigSchema)` to create a new message.
 */
export declare const IdempotencyConfigSchema: GenMessage<IdempotencyConfig>;
/**
 * @generated from message v1.IdempotencyCollisionError
 */
export type IdempotencyCollisionError = Message<"v1.IdempotencyCollisionError"> & {
    /**
     * the external ID of the existing workflow run that caused the collision
     *
     * @generated from field: string existing_run_external_id = 1;
     */
    existingRunExternalId: string;
    /**
     * the external ID of the workflow run that caused the collision
     *
     * @generated from field: string colliding_run_external_id = 2;
     */
    collidingRunExternalId: string;
};
/**
 * Describes the message v1.IdempotencyCollisionError.
 * Use `create(IdempotencyCollisionErrorSchema)` to create a new message.
 */
export declare const IdempotencyCollisionErrorSchema: GenMessage<IdempotencyCollisionError>;
/**
 * @generated from message v1.BulkTriggerIdempotencyCollisionError
 */
export type BulkTriggerIdempotencyCollisionError = Message<"v1.BulkTriggerIdempotencyCollisionError"> & {
    /**
     * the external IDs of the successfully triggered workflow runs
     *
     * @generated from field: repeated string successful_workflow_run_external_ids = 1;
     */
    successfulWorkflowRunExternalIds: string[];
    /**
     * the idempotency collision errors
     *
     * @generated from field: repeated v1.IdempotencyCollisionError collisions = 2;
     */
    collisions: IdempotencyCollisionError[];
};
/**
 * Describes the message v1.BulkTriggerIdempotencyCollisionError.
 * Use `create(BulkTriggerIdempotencyCollisionErrorSchema)` to create a new message.
 */
export declare const BulkTriggerIdempotencyCollisionErrorSchema: GenMessage<BulkTriggerIdempotencyCollisionError>;
/**
 * @generated from message v1.DefaultFilter
 */
export type DefaultFilter = Message<"v1.DefaultFilter"> & {
    /**
     * (required) the CEL expression for the filter
     *
     * @generated from field: string expression = 1;
     */
    expression: string;
    /**
     * (required) the scope of the filter
     *
     * @generated from field: string scope = 2;
     */
    scope: string;
    /**
     * (optional) the payload for the filter, if any. A JSON object as a string.
     *
     * @generated from field: optional bytes payload = 3;
     */
    payload?: Uint8Array | undefined;
};
/**
 * Describes the message v1.DefaultFilter.
 * Use `create(DefaultFilterSchema)` to create a new message.
 */
export declare const DefaultFilterSchema: GenMessage<DefaultFilter>;
/**
 * Concurrency declares one entry in a concurrency chain. Entries are processed in array
 * order, so a tenant-scoped entry may come before or after a workflow-scoped one.
 *
 * A tenant-scoped entry (is_tenant_scoped = true) defines (or updates in place) a strategy
 * shared across workflows, keyed by name: every task declaring the same name consumes the
 * same concurrency limit. Registrations whose chains order the same tenant-scoped
 * strategies inconsistently are rejected, since inconsistent orders can deadlock.
 *
 * @generated from message v1.Concurrency
 */
export type Concurrency = Message<"v1.Concurrency"> & {
    /**
     * (required) the expression to use for concurrency
     *
     * @generated from field: string expression = 1;
     */
    expression: string;
    /**
     * (optional) the maximum number of concurrent workflow runs, default 1
     *
     * @generated from field: optional int32 max_runs = 2;
     */
    maxRuns?: number | undefined;
    /**
     * (optional) the strategy to use when the concurrency limit is reached, default CANCEL_IN_PROGRESS
     *
     * @generated from field: optional v1.ConcurrencyLimitStrategy limit_strategy = 3;
     */
    limitStrategy?: ConcurrencyLimitStrategy | undefined;
    /**
     * (required when is_tenant_scoped) the strategy name; unique per tenant for tenant-scoped strategies
     *
     * @generated from field: optional string name = 4;
     */
    name?: string | undefined;
    /**
     * (optional) when true, the entry is a tenant-scoped strategy shared across workflows, default false
     *
     * @generated from field: optional bool is_tenant_scoped = 5;
     */
    isTenantScoped?: boolean | undefined;
    /**
     * (optional) CEL expression over task input returning the max runs for that task's concurrency group; the group's effective limit is the value from its most recently created task. Overrides max_runs per group; a non-integer or negative result fails the task, and 0 holds the group until a newer task raises the limit
     *
     * @generated from field: optional string max_runs_expression = 6;
     */
    maxRunsExpression?: string | undefined;
};
/**
 * Describes the message v1.Concurrency.
 * Use `create(ConcurrencySchema)` to create a new message.
 */
export declare const ConcurrencySchema: GenMessage<Concurrency>;
/**
 * @generated from message v1.TaskBatchConfig
 */
export type TaskBatchConfig = Message<"v1.TaskBatchConfig"> & {
    /**
     * (required) maximum items per batch
     *
     * @generated from field: int32 batch_max_size = 1;
     */
    batchMaxSize: number;
    /**
     * (optional) time before batch flushes (milliseconds)
     *
     * @generated from field: optional int32 batch_max_interval_ms = 2;
     */
    batchMaxIntervalMs?: number | undefined;
    /**
     * (optional) partition key for fairness (prevents mixing tenants)
     *
     * @generated from field: optional string batch_group_key = 3;
     */
    batchGroupKey?: string | undefined;
    /**
     * (optional) concurrent batches per group
     *
     * @generated from field: optional int32 batch_group_max_runs = 4;
     */
    batchGroupMaxRuns?: number | undefined;
    /**
     * (optional) when true, the handler returns one value broadcast to all callers; when false (default), the handler returns a dict keyed by step run id
     *
     * @generated from field: optional bool broadcast_output = 5;
     */
    broadcastOutput?: boolean | undefined;
};
/**
 * Describes the message v1.TaskBatchConfig.
 * Use `create(TaskBatchConfigSchema)` to create a new message.
 */
export declare const TaskBatchConfigSchema: GenMessage<TaskBatchConfig>;
/**
 * CreateTaskOpts represents options to create a task.
 *
 * @generated from message v1.CreateTaskOpts
 */
export type CreateTaskOpts = Message<"v1.CreateTaskOpts"> & {
    /**
     * (required) the task name
     *
     * @generated from field: string readable_id = 1;
     */
    readableId: string;
    /**
     * (required) the task action id
     *
     * @generated from field: string action = 2;
     */
    action: string;
    /**
     * (optional) the task timeout
     *
     * @generated from field: string timeout = 3;
     */
    timeout: string;
    /**
     * (optional) the task inputs, assuming string representation of JSON
     *
     * @generated from field: string inputs = 4;
     */
    inputs: string;
    /**
     * (optional) the task parents. if none are passed in, this is a root task
     *
     * @generated from field: repeated string parents = 5;
     */
    parents: string[];
    /**
     * (optional) the number of retries for the task, default 0
     *
     * @generated from field: int32 retries = 6;
     */
    retries: number;
    /**
     * (optional) the rate limits for the task
     *
     * @generated from field: repeated v1.CreateTaskRateLimit rate_limits = 7;
     */
    rateLimits: CreateTaskRateLimit[];
    /**
     * (optional) the desired worker affinity state for the task
     *
     * @generated from field: map<string, v1.DesiredWorkerLabels> worker_labels = 8;
     */
    workerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
    /**
     * (optional) the retry backoff factor for the task
     *
     * @generated from field: optional float backoff_factor = 9;
     */
    backoffFactor?: number | undefined;
    /**
     * (optional) the maximum backoff time for the task
     *
     * @generated from field: optional int32 backoff_max_seconds = 10;
     */
    backoffMaxSeconds?: number | undefined;
    /**
     * (optional) the task concurrency options
     *
     * @generated from field: repeated v1.Concurrency concurrency = 11;
     */
    concurrency: Concurrency[];
    /**
     * (optional) the task conditions for creating the task
     *
     * @generated from field: optional v1.TaskConditions conditions = 12;
     */
    conditions?: TaskConditions | undefined;
    /**
     * (optional) the timeout for the schedule
     *
     * @generated from field: optional string schedule_timeout = 13;
     */
    scheduleTimeout?: string | undefined;
    /**
     * (optional) whether the task is durable
     *
     * @generated from field: bool is_durable = 14;
     */
    isDurable: boolean;
    /**
     * (optional) slot requests (slot_type -> units)
     *
     * @generated from field: map<string, int32> slot_requests = 15;
     */
    slotRequests: {
        [key: string]: number;
    };
    /**
     * (optional) batch execution configuration
     *
     * @generated from field: optional v1.TaskBatchConfig batch = 16;
     */
    batch?: TaskBatchConfig | undefined;
};
/**
 * Describes the message v1.CreateTaskOpts.
 * Use `create(CreateTaskOptsSchema)` to create a new message.
 */
export declare const CreateTaskOptsSchema: GenMessage<CreateTaskOpts>;
/**
 * @generated from message v1.CreateTaskRateLimit
 */
export type CreateTaskRateLimit = Message<"v1.CreateTaskRateLimit"> & {
    /**
     * (required) the key for the rate limit
     *
     * @generated from field: string key = 1;
     */
    key: string;
    /**
     * (optional) the number of units this task consumes
     *
     * @generated from field: optional int32 units = 2;
     */
    units?: number | undefined;
    /**
     * (optional) a CEL expression for determining the rate limit key
     *
     * @generated from field: optional string key_expr = 3;
     */
    keyExpr?: string | undefined;
    /**
     * (optional) a CEL expression for determining the number of units consumed
     *
     * @generated from field: optional string units_expr = 4;
     */
    unitsExpr?: string | undefined;
    /**
     * (optional) a CEL expression for determining the total amount of rate limit units
     *
     * @generated from field: optional string limit_values_expr = 5;
     */
    limitValuesExpr?: string | undefined;
    /**
     * (optional) the default rate limit window to use for dynamic rate limits
     *
     * @generated from field: optional v1.RateLimitDuration duration = 6;
     */
    duration?: RateLimitDuration | undefined;
};
/**
 * Describes the message v1.CreateTaskRateLimit.
 * Use `create(CreateTaskRateLimitSchema)` to create a new message.
 */
export declare const CreateTaskRateLimitSchema: GenMessage<CreateTaskRateLimit>;
/**
 * CreateWorkflowVersionResponse represents the response after creating a workflow version.
 *
 * @generated from message v1.CreateWorkflowVersionResponse
 */
export type CreateWorkflowVersionResponse = Message<"v1.CreateWorkflowVersionResponse"> & {
    /**
     * @generated from field: string id = 1;
     */
    id: string;
    /**
     * @generated from field: string workflow_id = 2;
     */
    workflowId: string;
};
/**
 * Describes the message v1.CreateWorkflowVersionResponse.
 * Use `create(CreateWorkflowVersionResponseSchema)` to create a new message.
 */
export declare const CreateWorkflowVersionResponseSchema: GenMessage<CreateWorkflowVersionResponse>;
/**
 * @generated from message v1.GetRunDetailsRequest
 */
export type GetRunDetailsRequest = Message<"v1.GetRunDetailsRequest"> & {
    /**
     * (required) the external id (uuid) of the workflow run
     *
     * @generated from field: string external_id = 1;
     */
    externalId: string;
};
/**
 * Describes the message v1.GetRunDetailsRequest.
 * Use `create(GetRunDetailsRequestSchema)` to create a new message.
 */
export declare const GetRunDetailsRequestSchema: GenMessage<GetRunDetailsRequest>;
/**
 * @generated from message v1.TaskRunDetail
 */
export type TaskRunDetail = Message<"v1.TaskRunDetail"> & {
    /**
     * the external id (uuid) of the task run
     *
     * @generated from field: string external_id = 1;
     */
    externalId: string;
    /**
     * the status of the task run
     *
     * @generated from field: v1.RunStatus status = 2;
     */
    status: RunStatus;
    /**
     * (optional) error message from the task run, if any
     *
     * @generated from field: optional string error = 3;
     */
    error?: string | undefined;
    /**
     * (optional) the output payload for the task run
     *
     * @generated from field: optional bytes output = 4;
     */
    output?: Uint8Array | undefined;
    /**
     * the readable id of the task
     *
     * @generated from field: string readable_id = 5;
     */
    readableId: string;
    /**
     * whether the task has been evicted from a worker (status will be RUNNING)
     *
     * @generated from field: bool is_evicted = 6;
     */
    isEvicted: boolean;
};
/**
 * Describes the message v1.TaskRunDetail.
 * Use `create(TaskRunDetailSchema)` to create a new message.
 */
export declare const TaskRunDetailSchema: GenMessage<TaskRunDetail>;
/**
 * @generated from message v1.GetRunDetailsResponse
 */
export type GetRunDetailsResponse = Message<"v1.GetRunDetailsResponse"> & {
    /**
     * the input payload for the workflow run
     *
     * @generated from field: bytes input = 1;
     */
    input: Uint8Array;
    /**
     * the status of the workflow run
     *
     * @generated from field: v1.RunStatus status = 2;
     */
    status: RunStatus;
    /**
     * map of task run external ids to their details
     *
     * @generated from field: map<string, v1.TaskRunDetail> task_runs = 3;
     */
    taskRuns: {
        [key: string]: TaskRunDetail;
    };
    /**
     * indicates if the workflow run is done
     *
     * @generated from field: bool done = 4;
     */
    done: boolean;
    /**
     * (optional) additional metadata for the workflow run
     *
     * @generated from field: bytes additional_metadata = 5;
     */
    additionalMetadata: Uint8Array;
    /**
     * whether any task in this run has been evicted
     *
     * @generated from field: bool is_evicted = 6;
     */
    isEvicted: boolean;
};
/**
 * Describes the message v1.GetRunDetailsResponse.
 * Use `create(GetRunDetailsResponseSchema)` to create a new message.
 */
export declare const GetRunDetailsResponseSchema: GenMessage<GetRunDetailsResponse>;
/**
 * @generated from enum v1.StickyStrategy
 */
export declare enum StickyStrategy {
    /**
     * @generated from enum value: SOFT = 0;
     */
    SOFT = 0,
    /**
     * @generated from enum value: HARD = 1;
     */
    HARD = 1
}
/**
 * Describes the enum v1.StickyStrategy.
 */
export declare const StickyStrategySchema: GenEnum<StickyStrategy>;
/**
 * @generated from enum v1.RateLimitDuration
 */
export declare enum RateLimitDuration {
    /**
     * @generated from enum value: SECOND = 0;
     */
    SECOND = 0,
    /**
     * @generated from enum value: MINUTE = 1;
     */
    MINUTE = 1,
    /**
     * @generated from enum value: HOUR = 2;
     */
    HOUR = 2,
    /**
     * @generated from enum value: DAY = 3;
     */
    DAY = 3,
    /**
     * @generated from enum value: WEEK = 4;
     */
    WEEK = 4,
    /**
     * @generated from enum value: MONTH = 5;
     */
    MONTH = 5,
    /**
     * @generated from enum value: YEAR = 6;
     */
    YEAR = 6
}
/**
 * Describes the enum v1.RateLimitDuration.
 */
export declare const RateLimitDurationSchema: GenEnum<RateLimitDuration>;
/**
 * @generated from enum v1.RunStatus
 */
export declare enum RunStatus {
    /**
     * @generated from enum value: QUEUED = 0;
     */
    QUEUED = 0,
    /**
     * @generated from enum value: RUNNING = 1;
     */
    RUNNING = 1,
    /**
     * @generated from enum value: COMPLETED = 2;
     */
    COMPLETED = 2,
    /**
     * @generated from enum value: FAILED = 3;
     */
    FAILED = 3,
    /**
     * @generated from enum value: CANCELLED = 4;
     */
    CANCELLED = 4,
    /**
     * @generated from enum value: EVICTED = 5;
     */
    EVICTED = 5
}
/**
 * Describes the enum v1.RunStatus.
 */
export declare const RunStatusSchema: GenEnum<RunStatus>;
/**
 * @generated from enum v1.IdempotencyMethod
 */
export declare enum IdempotencyMethod {
    /**
     * @generated from enum value: TTL = 0;
     */
    TTL = 0,
    /**
     * @generated from enum value: STATUS = 1;
     */
    STATUS = 1
}
/**
 * Describes the enum v1.IdempotencyMethod.
 */
export declare const IdempotencyMethodSchema: GenEnum<IdempotencyMethod>;
/**
 * @generated from enum v1.ConcurrencyLimitStrategy
 */
export declare enum ConcurrencyLimitStrategy {
    /**
     * @generated from enum value: CANCEL_IN_PROGRESS = 0;
     */
    CANCEL_IN_PROGRESS = 0,
    /**
     * deprecated
     *
     * @generated from enum value: DROP_NEWEST = 1;
     */
    DROP_NEWEST = 1,
    /**
     * deprecated
     *
     * @generated from enum value: QUEUE_NEWEST = 2;
     */
    QUEUE_NEWEST = 2,
    /**
     * @generated from enum value: GROUP_ROUND_ROBIN = 3;
     */
    GROUP_ROUND_ROBIN = 3,
    /**
     * @generated from enum value: CANCEL_NEWEST = 4;
     */
    CANCEL_NEWEST = 4,
    /**
     * @generated from enum value: CANCEL_QUEUED_EXCEPT_NEWEST = 5;
     */
    CANCEL_QUEUED_EXCEPT_NEWEST = 5,
    /**
     * @generated from enum value: CANCEL_QUEUED_EXCEPT_OLDEST = 6;
     */
    CANCEL_QUEUED_EXCEPT_OLDEST = 6
}
/**
 * Describes the enum v1.ConcurrencyLimitStrategy.
 */
export declare const ConcurrencyLimitStrategySchema: GenEnum<ConcurrencyLimitStrategy>;
/**
 * AdminService represents a set of RPCs for admin management of tasks, workflows, etc.
 *
 * @generated from service v1.AdminService
 */
export declare const AdminService: GenService<{
    /**
     * @generated from rpc v1.AdminService.PutWorkflow
     */
    putWorkflow: {
        methodKind: "unary";
        input: typeof CreateWorkflowVersionRequestSchema;
        output: typeof CreateWorkflowVersionResponseSchema;
    };
    /**
     * @generated from rpc v1.AdminService.CancelTasks
     */
    cancelTasks: {
        methodKind: "unary";
        input: typeof CancelTasksRequestSchema;
        output: typeof CancelTasksResponseSchema;
    };
    /**
     * @generated from rpc v1.AdminService.ReplayTasks
     */
    replayTasks: {
        methodKind: "unary";
        input: typeof ReplayTasksRequestSchema;
        output: typeof ReplayTasksResponseSchema;
    };
    /**
     * @generated from rpc v1.AdminService.TriggerWorkflowRun
     */
    triggerWorkflowRun: {
        methodKind: "unary";
        input: typeof TriggerWorkflowRunRequestSchema;
        output: typeof TriggerWorkflowRunResponseSchema;
    };
    /**
     * @generated from rpc v1.AdminService.GetRunDetails
     */
    getRunDetails: {
        methodKind: "unary";
        input: typeof GetRunDetailsRequestSchema;
        output: typeof GetRunDetailsResponseSchema;
    };
    /**
     * @generated from rpc v1.AdminService.BranchDurableTask
     */
    branchDurableTask: {
        methodKind: "unary";
        input: typeof BranchDurableTaskRequestSchema;
        output: typeof BranchDurableTaskResponseSchema;
    };
}>;
