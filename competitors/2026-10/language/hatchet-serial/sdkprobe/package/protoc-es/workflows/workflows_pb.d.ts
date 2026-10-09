import type { GenEnum, GenFile, GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { DesiredWorkerLabels, TriggerWorkflowRequest, TriggerWorkflowRequestSchema } from "../v1/shared/trigger_pb";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file workflows/workflows.proto.
 */
export declare const file_workflows_workflows: GenFile;
/**
 * @generated from message PutWorkflowRequest
 */
export type PutWorkflowRequest = Message<"PutWorkflowRequest"> & {
    /**
     * @generated from field: CreateWorkflowVersionOpts opts = 1;
     */
    opts?: CreateWorkflowVersionOpts | undefined;
};
/**
 * Describes the message PutWorkflowRequest.
 * Use `create(PutWorkflowRequestSchema)` to create a new message.
 */
export declare const PutWorkflowRequestSchema: GenMessage<PutWorkflowRequest>;
/**
 * CreateWorkflowVersionOpts represents options to create a workflow version.
 *
 * @generated from message CreateWorkflowVersionOpts
 */
export type CreateWorkflowVersionOpts = Message<"CreateWorkflowVersionOpts"> & {
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
     * (required) the workflow version
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
     * (optional) scheduled triggers for the workflow
     *
     * @generated from field: repeated google.protobuf.Timestamp scheduled_triggers = 6;
     */
    scheduledTriggers: Timestamp[];
    /**
     * (required) the workflow jobs
     *
     * @generated from field: repeated CreateWorkflowJobOpts jobs = 7;
     */
    jobs: CreateWorkflowJobOpts[];
    /**
     * (optional) the workflow concurrency options
     *
     * @generated from field: WorkflowConcurrencyOpts concurrency = 8;
     */
    concurrency?: WorkflowConcurrencyOpts | undefined;
    /**
     * (optional) the timeout for the schedule
     *
     * @generated from field: optional string schedule_timeout = 9;
     */
    scheduleTimeout?: string | undefined;
    /**
     * (optional) the input for the cron trigger
     *
     * @generated from field: optional string cron_input = 10;
     */
    cronInput?: string | undefined;
    /**
     * (optional) the job to run on failure
     *
     * @generated from field: optional CreateWorkflowJobOpts on_failure_job = 11;
     */
    onFailureJob?: CreateWorkflowJobOpts | undefined;
    /**
     * (optional) the sticky strategy for assigning tasks to workers
     *
     * @generated from field: optional StickyStrategy sticky = 12;
     */
    sticky?: StickyStrategy | undefined;
    /**
     * (optional) the kind of workflow
     *
     * @generated from field: optional WorkflowKind kind = 13;
     */
    kind?: WorkflowKind | undefined;
    /**
     * (optional) the priority of the workflow
     *
     * @generated from field: optional int32 default_priority = 14;
     */
    defaultPriority?: number | undefined;
};
/**
 * Describes the message CreateWorkflowVersionOpts.
 * Use `create(CreateWorkflowVersionOptsSchema)` to create a new message.
 */
export declare const CreateWorkflowVersionOptsSchema: GenMessage<CreateWorkflowVersionOpts>;
/**
 * @generated from message WorkflowConcurrencyOpts
 */
export type WorkflowConcurrencyOpts = Message<"WorkflowConcurrencyOpts"> & {
    /**
     * (optional) the action id for getting the concurrency group
     *
     * @generated from field: optional string action = 1;
     */
    action?: string | undefined;
    /**
     * (optional) the maximum number of concurrent workflow runs, default 1
     *
     * @generated from field: optional int32 max_runs = 2;
     */
    maxRuns?: number | undefined;
    /**
     * (optional) the strategy to use when the concurrency limit is reached, default CANCEL_IN_PROGRESS
     *
     * @generated from field: optional ConcurrencyLimitStrategy limit_strategy = 3;
     */
    limitStrategy?: ConcurrencyLimitStrategy | undefined;
    /**
     * (optional) the expression to use for concurrency
     *
     * @generated from field: optional string expression = 4;
     */
    expression?: string | undefined;
};
/**
 * Describes the message WorkflowConcurrencyOpts.
 * Use `create(WorkflowConcurrencyOptsSchema)` to create a new message.
 */
export declare const WorkflowConcurrencyOptsSchema: GenMessage<WorkflowConcurrencyOpts>;
/**
 * CreateWorkflowJobOpts represents options to create a workflow job.
 *
 * @generated from message CreateWorkflowJobOpts
 */
export type CreateWorkflowJobOpts = Message<"CreateWorkflowJobOpts"> & {
    /**
     * (required) the job name
     *
     * @generated from field: string name = 1;
     */
    name: string;
    /**
     * (optional) the job description
     *
     * @generated from field: string description = 2;
     */
    description: string;
    /**
     * (required) the job tasks
     *
     * @generated from field: repeated CreateWorkflowStepOpts steps = 4;
     */
    steps: CreateWorkflowStepOpts[];
};
/**
 * Describes the message CreateWorkflowJobOpts.
 * Use `create(CreateWorkflowJobOptsSchema)` to create a new message.
 */
export declare const CreateWorkflowJobOptsSchema: GenMessage<CreateWorkflowJobOpts>;
/**
 * CreateWorkflowStepOpts represents options to create a workflow task.
 *
 * @generated from message CreateWorkflowStepOpts
 */
export type CreateWorkflowStepOpts = Message<"CreateWorkflowStepOpts"> & {
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
     * (optional) the custom task user data, assuming string representation of JSON
     *
     * @generated from field: string user_data = 6;
     */
    userData: string;
    /**
     * (optional) the number of retries for the task, default 0
     *
     * @generated from field: int32 retries = 7;
     */
    retries: number;
    /**
     * (optional) the rate limits for the task
     *
     * @generated from field: repeated CreateStepRateLimit rate_limits = 8;
     */
    rateLimits: CreateStepRateLimit[];
    /**
     * (optional) the desired worker affinity state for the task
     *
     * @generated from field: map<string, v1.DesiredWorkerLabels> worker_labels = 9;
     */
    workerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
    /**
     * (optional) the retry backoff factor for the task
     *
     * @generated from field: optional float backoff_factor = 10;
     */
    backoffFactor?: number | undefined;
    /**
     * (optional) the maximum backoff time for the task
     *
     * @generated from field: optional int32 backoff_max_seconds = 11;
     */
    backoffMaxSeconds?: number | undefined;
};
/**
 * Describes the message CreateWorkflowStepOpts.
 * Use `create(CreateWorkflowStepOptsSchema)` to create a new message.
 */
export declare const CreateWorkflowStepOptsSchema: GenMessage<CreateWorkflowStepOpts>;
/**
 * @generated from message CreateStepRateLimit
 */
export type CreateStepRateLimit = Message<"CreateStepRateLimit"> & {
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
     * @generated from field: optional RateLimitDuration duration = 6;
     */
    duration?: RateLimitDuration | undefined;
};
/**
 * Describes the message CreateStepRateLimit.
 * Use `create(CreateStepRateLimitSchema)` to create a new message.
 */
export declare const CreateStepRateLimitSchema: GenMessage<CreateStepRateLimit>;
/**
 * ListWorkflowsRequest is the request for ListWorkflows.
 *
 * @generated from message ListWorkflowsRequest
 */
export type ListWorkflowsRequest = Message<"ListWorkflowsRequest"> & {};
/**
 * Describes the message ListWorkflowsRequest.
 * Use `create(ListWorkflowsRequestSchema)` to create a new message.
 */
export declare const ListWorkflowsRequestSchema: GenMessage<ListWorkflowsRequest>;
/**
 * @generated from message ScheduleWorkflowRequest
 */
export type ScheduleWorkflowRequest = Message<"ScheduleWorkflowRequest"> & {
    /**
     * @generated from field: string name = 1;
     */
    name: string;
    /**
     * @generated from field: repeated google.protobuf.Timestamp schedules = 2;
     */
    schedules: Timestamp[];
    /**
     * (optional) the input data for the workflow
     *
     * @generated from field: string input = 3;
     */
    input: string;
    /**
     * (optional) the parent workflow run id
     *
     * @generated from field: optional string parent_id = 4;
     */
    parentId?: string | undefined;
    /**
     * (optional) the parent task external run id
     *
     * @generated from field: optional string parent_task_run_external_id = 5;
     */
    parentTaskRunExternalId?: string | undefined;
    /**
     * (optional) the index of the child workflow. if this is set, matches on the index or the
     * child key will be a no-op, even if the schedule has changed.
     *
     * @generated from field: optional int32 child_index = 6;
     */
    childIndex?: number | undefined;
    /**
     * (optional) the key for the child. if this is set, matches on the index or the
     * child key will be a no-op, even if the schedule has changed.
     *
     * @generated from field: optional string child_key = 7;
     */
    childKey?: string | undefined;
    /**
     * (optional) the additional metadata for the workflow
     *
     * @generated from field: optional string additional_metadata = 8;
     */
    additionalMetadata?: string | undefined;
    /**
     * (optional) the priority of the workflow
     *
     * @generated from field: optional int32 priority = 9;
     */
    priority?: number | undefined;
};
/**
 * Describes the message ScheduleWorkflowRequest.
 * Use `create(ScheduleWorkflowRequestSchema)` to create a new message.
 */
export declare const ScheduleWorkflowRequestSchema: GenMessage<ScheduleWorkflowRequest>;
/**
 * ScheduledWorkflow represents a scheduled workflow.
 *
 * @generated from message ScheduledWorkflow
 */
export type ScheduledWorkflow = Message<"ScheduledWorkflow"> & {
    /**
     * @generated from field: string id = 1;
     */
    id: string;
    /**
     * @generated from field: google.protobuf.Timestamp trigger_at = 2;
     */
    triggerAt?: Timestamp | undefined;
};
/**
 * Describes the message ScheduledWorkflow.
 * Use `create(ScheduledWorkflowSchema)` to create a new message.
 */
export declare const ScheduledWorkflowSchema: GenMessage<ScheduledWorkflow>;
/**
 * WorkflowVersion represents the WorkflowVersion model.
 *
 * @generated from message WorkflowVersion
 */
export type WorkflowVersion = Message<"WorkflowVersion"> & {
    /**
     * @generated from field: string id = 1;
     */
    id: string;
    /**
     * @generated from field: google.protobuf.Timestamp created_at = 2;
     */
    createdAt?: Timestamp | undefined;
    /**
     * @generated from field: google.protobuf.Timestamp updated_at = 3;
     */
    updatedAt?: Timestamp | undefined;
    /**
     * @generated from field: string version = 5;
     */
    version: string;
    /**
     * @generated from field: int64 order = 6;
     */
    order: bigint;
    /**
     * @generated from field: string workflow_id = 7;
     */
    workflowId: string;
    /**
     * @generated from field: repeated ScheduledWorkflow scheduled_workflows = 8;
     */
    scheduledWorkflows: ScheduledWorkflow[];
};
/**
 * Describes the message WorkflowVersion.
 * Use `create(WorkflowVersionSchema)` to create a new message.
 */
export declare const WorkflowVersionSchema: GenMessage<WorkflowVersion>;
/**
 * WorkflowTriggerEventRef represents the WorkflowTriggerEventRef model.
 *
 * @generated from message WorkflowTriggerEventRef
 */
export type WorkflowTriggerEventRef = Message<"WorkflowTriggerEventRef"> & {
    /**
     * @generated from field: string parent_id = 1;
     */
    parentId: string;
    /**
     * @generated from field: string event_key = 2;
     */
    eventKey: string;
};
/**
 * Describes the message WorkflowTriggerEventRef.
 * Use `create(WorkflowTriggerEventRefSchema)` to create a new message.
 */
export declare const WorkflowTriggerEventRefSchema: GenMessage<WorkflowTriggerEventRef>;
/**
 * WorkflowTriggerCronRef represents the WorkflowTriggerCronRef model.
 *
 * @generated from message WorkflowTriggerCronRef
 */
export type WorkflowTriggerCronRef = Message<"WorkflowTriggerCronRef"> & {
    /**
     * @generated from field: string parent_id = 1;
     */
    parentId: string;
    /**
     * @generated from field: string cron = 2;
     */
    cron: string;
};
/**
 * Describes the message WorkflowTriggerCronRef.
 * Use `create(WorkflowTriggerCronRefSchema)` to create a new message.
 */
export declare const WorkflowTriggerCronRefSchema: GenMessage<WorkflowTriggerCronRef>;
/**
 * @generated from message BulkTriggerWorkflowRequest
 */
export type BulkTriggerWorkflowRequest = Message<"BulkTriggerWorkflowRequest"> & {
    /**
     * @generated from field: repeated v1.TriggerWorkflowRequest workflows = 1;
     */
    workflows: TriggerWorkflowRequest[];
};
/**
 * Describes the message BulkTriggerWorkflowRequest.
 * Use `create(BulkTriggerWorkflowRequestSchema)` to create a new message.
 */
export declare const BulkTriggerWorkflowRequestSchema: GenMessage<BulkTriggerWorkflowRequest>;
/**
 * @generated from message BulkTriggerWorkflowResponse
 */
export type BulkTriggerWorkflowResponse = Message<"BulkTriggerWorkflowResponse"> & {
    /**
     * @generated from field: repeated string workflow_run_ids = 1;
     */
    workflowRunIds: string[];
};
/**
 * Describes the message BulkTriggerWorkflowResponse.
 * Use `create(BulkTriggerWorkflowResponseSchema)` to create a new message.
 */
export declare const BulkTriggerWorkflowResponseSchema: GenMessage<BulkTriggerWorkflowResponse>;
/**
 * @generated from message TriggerWorkflowResponse
 */
export type TriggerWorkflowResponse = Message<"TriggerWorkflowResponse"> & {
    /**
     * @generated from field: string workflow_run_id = 1;
     */
    workflowRunId: string;
};
/**
 * Describes the message TriggerWorkflowResponse.
 * Use `create(TriggerWorkflowResponseSchema)` to create a new message.
 */
export declare const TriggerWorkflowResponseSchema: GenMessage<TriggerWorkflowResponse>;
/**
 * @generated from message PutRateLimitRequest
 */
export type PutRateLimitRequest = Message<"PutRateLimitRequest"> & {
    /**
     * (required) the global key for the rate limit
     *
     * @generated from field: string key = 1;
     */
    key: string;
    /**
     * (required) the max limit for the rate limit (per unit of time)
     *
     * @generated from field: int32 limit = 2;
     */
    limit: number;
    /**
     * (required) the duration of time for the rate limit (second|minute|hour)
     *
     * @generated from field: RateLimitDuration duration = 3;
     */
    duration: RateLimitDuration;
};
/**
 * Describes the message PutRateLimitRequest.
 * Use `create(PutRateLimitRequestSchema)` to create a new message.
 */
export declare const PutRateLimitRequestSchema: GenMessage<PutRateLimitRequest>;
/**
 * @generated from message PutRateLimitResponse
 */
export type PutRateLimitResponse = Message<"PutRateLimitResponse"> & {};
/**
 * Describes the message PutRateLimitResponse.
 * Use `create(PutRateLimitResponseSchema)` to create a new message.
 */
export declare const PutRateLimitResponseSchema: GenMessage<PutRateLimitResponse>;
/**
 * @generated from enum StickyStrategy
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
 * Describes the enum StickyStrategy.
 */
export declare const StickyStrategySchema: GenEnum<StickyStrategy>;
/**
 * @generated from enum WorkflowKind
 */
export declare enum WorkflowKind {
    /**
     * @generated from enum value: FUNCTION = 0;
     */
    FUNCTION = 0,
    /**
     * @generated from enum value: DURABLE = 1;
     */
    DURABLE = 1,
    /**
     * @generated from enum value: DAG = 2;
     */
    DAG = 2
}
/**
 * Describes the enum WorkflowKind.
 */
export declare const WorkflowKindSchema: GenEnum<WorkflowKind>;
/**
 * @generated from enum ConcurrencyLimitStrategy
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
    CANCEL_NEWEST = 4
}
/**
 * Describes the enum ConcurrencyLimitStrategy.
 */
export declare const ConcurrencyLimitStrategySchema: GenEnum<ConcurrencyLimitStrategy>;
/**
 * @generated from enum RateLimitDuration
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
 * Describes the enum RateLimitDuration.
 */
export declare const RateLimitDurationSchema: GenEnum<RateLimitDuration>;
/**
 * WorkflowService represents a set of RPCs for managing workflows.
 *
 * @generated from service WorkflowService
 */
export declare const WorkflowService: GenService<{
    /**
     * @generated from rpc WorkflowService.PutWorkflow
     */
    putWorkflow: {
        methodKind: "unary";
        input: typeof PutWorkflowRequestSchema;
        output: typeof WorkflowVersionSchema;
    };
    /**
     * @generated from rpc WorkflowService.ScheduleWorkflow
     */
    scheduleWorkflow: {
        methodKind: "unary";
        input: typeof ScheduleWorkflowRequestSchema;
        output: typeof WorkflowVersionSchema;
    };
    /**
     * @generated from rpc WorkflowService.TriggerWorkflow
     */
    triggerWorkflow: {
        methodKind: "unary";
        input: typeof TriggerWorkflowRequestSchema;
        output: typeof TriggerWorkflowResponseSchema;
    };
    /**
     * @generated from rpc WorkflowService.BulkTriggerWorkflow
     */
    bulkTriggerWorkflow: {
        methodKind: "unary";
        input: typeof BulkTriggerWorkflowRequestSchema;
        output: typeof BulkTriggerWorkflowResponseSchema;
    };
    /**
     * @generated from rpc WorkflowService.PutRateLimit
     */
    putRateLimit: {
        methodKind: "unary";
        input: typeof PutRateLimitRequestSchema;
        output: typeof PutRateLimitResponseSchema;
    };
}>;
