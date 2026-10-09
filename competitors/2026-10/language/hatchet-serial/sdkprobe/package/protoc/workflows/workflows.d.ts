import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import type { CallContext, CallOptions } from 'nice-grpc-common';
import { DesiredWorkerLabels, TriggerWorkflowRequest } from '../v1/shared/trigger';
export declare const protobufPackage = "";
export declare enum StickyStrategy {
    SOFT = 0,
    HARD = 1,
    UNRECOGNIZED = -1
}
export declare function stickyStrategyFromJSON(object: any): StickyStrategy;
export declare function stickyStrategyToJSON(object: StickyStrategy): string;
export declare enum WorkflowKind {
    FUNCTION = 0,
    DURABLE = 1,
    DAG = 2,
    UNRECOGNIZED = -1
}
export declare function workflowKindFromJSON(object: any): WorkflowKind;
export declare function workflowKindToJSON(object: WorkflowKind): string;
export declare enum ConcurrencyLimitStrategy {
    CANCEL_IN_PROGRESS = 0,
    /** DROP_NEWEST - deprecated */
    DROP_NEWEST = 1,
    /** QUEUE_NEWEST - deprecated */
    QUEUE_NEWEST = 2,
    GROUP_ROUND_ROBIN = 3,
    CANCEL_NEWEST = 4,
    UNRECOGNIZED = -1
}
export declare function concurrencyLimitStrategyFromJSON(object: any): ConcurrencyLimitStrategy;
export declare function concurrencyLimitStrategyToJSON(object: ConcurrencyLimitStrategy): string;
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
export interface PutWorkflowRequest {
    opts: CreateWorkflowVersionOpts | undefined;
}
/** CreateWorkflowVersionOpts represents options to create a workflow version. */
export interface CreateWorkflowVersionOpts {
    /** (required) the workflow name */
    name: string;
    /** (optional) the workflow description */
    description: string;
    /** (required) the workflow version */
    version: string;
    /** (optional) event triggers for the workflow */
    eventTriggers: string[];
    /** (optional) cron triggers for the workflow */
    cronTriggers: string[];
    /** (optional) scheduled triggers for the workflow */
    scheduledTriggers: Date[];
    /** (required) the workflow jobs */
    jobs: CreateWorkflowJobOpts[];
    /** (optional) the workflow concurrency options */
    concurrency: WorkflowConcurrencyOpts | undefined;
    /** (optional) the timeout for the schedule */
    scheduleTimeout?: string | undefined;
    /** (optional) the input for the cron trigger */
    cronInput?: string | undefined;
    /** (optional) the job to run on failure */
    onFailureJob?: CreateWorkflowJobOpts | undefined;
    /** (optional) the sticky strategy for assigning tasks to workers */
    sticky?: StickyStrategy | undefined;
    /** (optional) the kind of workflow */
    kind?: WorkflowKind | undefined;
    /** (optional) the priority of the workflow */
    defaultPriority?: number | undefined;
}
export interface WorkflowConcurrencyOpts {
    /** (optional) the action id for getting the concurrency group */
    action?: string | undefined;
    /** (optional) the maximum number of concurrent workflow runs, default 1 */
    maxRuns?: number | undefined;
    /** (optional) the strategy to use when the concurrency limit is reached, default CANCEL_IN_PROGRESS */
    limitStrategy?: ConcurrencyLimitStrategy | undefined;
    /** (optional) the expression to use for concurrency */
    expression?: string | undefined;
}
/** CreateWorkflowJobOpts represents options to create a workflow job. */
export interface CreateWorkflowJobOpts {
    /** (required) the job name */
    name: string;
    /** (optional) the job description */
    description: string;
    /** (required) the job tasks */
    steps: CreateWorkflowStepOpts[];
}
/** CreateWorkflowStepOpts represents options to create a workflow task. */
export interface CreateWorkflowStepOpts {
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
    /** (optional) the custom task user data, assuming string representation of JSON */
    userData: string;
    /** (optional) the number of retries for the task, default 0 */
    retries: number;
    /** (optional) the rate limits for the task */
    rateLimits: CreateStepRateLimit[];
    /** (optional) the desired worker affinity state for the task */
    workerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
    /** (optional) the retry backoff factor for the task */
    backoffFactor?: number | undefined;
    /** (optional) the maximum backoff time for the task */
    backoffMaxSeconds?: number | undefined;
}
export interface CreateWorkflowStepOpts_WorkerLabelsEntry {
    key: string;
    value: DesiredWorkerLabels | undefined;
}
export interface CreateStepRateLimit {
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
/** ListWorkflowsRequest is the request for ListWorkflows. */
export interface ListWorkflowsRequest {
}
export interface ScheduleWorkflowRequest {
    name: string;
    schedules: Date[];
    /** (optional) the input data for the workflow */
    input: string;
    /** (optional) the parent workflow run id */
    parentId?: string | undefined;
    /** (optional) the parent task external run id */
    parentTaskRunExternalId?: string | undefined;
    /**
     * (optional) the index of the child workflow. if this is set, matches on the index or the
     * child key will be a no-op, even if the schedule has changed.
     */
    childIndex?: number | undefined;
    /**
     * (optional) the key for the child. if this is set, matches on the index or the
     * child key will be a no-op, even if the schedule has changed.
     */
    childKey?: string | undefined;
    /** (optional) the additional metadata for the workflow */
    additionalMetadata?: string | undefined;
    /** (optional) the priority of the workflow */
    priority?: number | undefined;
}
/** ScheduledWorkflow represents a scheduled workflow. */
export interface ScheduledWorkflow {
    id: string;
    triggerAt: Date | undefined;
}
/** WorkflowVersion represents the WorkflowVersion model. */
export interface WorkflowVersion {
    id: string;
    createdAt: Date | undefined;
    updatedAt: Date | undefined;
    version: string;
    order: number;
    workflowId: string;
    scheduledWorkflows: ScheduledWorkflow[];
}
/** WorkflowTriggerEventRef represents the WorkflowTriggerEventRef model. */
export interface WorkflowTriggerEventRef {
    parentId: string;
    eventKey: string;
}
/** WorkflowTriggerCronRef represents the WorkflowTriggerCronRef model. */
export interface WorkflowTriggerCronRef {
    parentId: string;
    cron: string;
}
export interface BulkTriggerWorkflowRequest {
    workflows: TriggerWorkflowRequest[];
}
export interface BulkTriggerWorkflowResponse {
    workflowRunIds: string[];
}
export interface TriggerWorkflowResponse {
    workflowRunId: string;
}
export interface PutRateLimitRequest {
    /** (required) the global key for the rate limit */
    key: string;
    /** (required) the max limit for the rate limit (per unit of time) */
    limit: number;
    /** (required) the duration of time for the rate limit (second|minute|hour) */
    duration: RateLimitDuration;
}
export interface PutRateLimitResponse {
}
export declare const PutWorkflowRequest: MessageFns<PutWorkflowRequest>;
export declare const CreateWorkflowVersionOpts: MessageFns<CreateWorkflowVersionOpts>;
export declare const WorkflowConcurrencyOpts: MessageFns<WorkflowConcurrencyOpts>;
export declare const CreateWorkflowJobOpts: MessageFns<CreateWorkflowJobOpts>;
export declare const CreateWorkflowStepOpts: MessageFns<CreateWorkflowStepOpts>;
export declare const CreateWorkflowStepOpts_WorkerLabelsEntry: MessageFns<CreateWorkflowStepOpts_WorkerLabelsEntry>;
export declare const CreateStepRateLimit: MessageFns<CreateStepRateLimit>;
export declare const ListWorkflowsRequest: MessageFns<ListWorkflowsRequest>;
export declare const ScheduleWorkflowRequest: MessageFns<ScheduleWorkflowRequest>;
export declare const ScheduledWorkflow: MessageFns<ScheduledWorkflow>;
export declare const WorkflowVersion: MessageFns<WorkflowVersion>;
export declare const WorkflowTriggerEventRef: MessageFns<WorkflowTriggerEventRef>;
export declare const WorkflowTriggerCronRef: MessageFns<WorkflowTriggerCronRef>;
export declare const BulkTriggerWorkflowRequest: MessageFns<BulkTriggerWorkflowRequest>;
export declare const BulkTriggerWorkflowResponse: MessageFns<BulkTriggerWorkflowResponse>;
export declare const TriggerWorkflowResponse: MessageFns<TriggerWorkflowResponse>;
export declare const PutRateLimitRequest: MessageFns<PutRateLimitRequest>;
export declare const PutRateLimitResponse: MessageFns<PutRateLimitResponse>;
/** WorkflowService represents a set of RPCs for managing workflows. */
export type WorkflowServiceDefinition = typeof WorkflowServiceDefinition;
export declare const WorkflowServiceDefinition: {
    readonly name: "WorkflowService";
    readonly fullName: "WorkflowService";
    readonly methods: {
        readonly putWorkflow: {
            readonly name: "PutWorkflow";
            readonly requestType: typeof PutWorkflowRequest;
            readonly requestStream: false;
            readonly responseType: typeof WorkflowVersion;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly scheduleWorkflow: {
            readonly name: "ScheduleWorkflow";
            readonly requestType: typeof ScheduleWorkflowRequest;
            readonly requestStream: false;
            readonly responseType: typeof WorkflowVersion;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly triggerWorkflow: {
            readonly name: "TriggerWorkflow";
            readonly requestType: typeof TriggerWorkflowRequest;
            readonly requestStream: false;
            readonly responseType: typeof TriggerWorkflowResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly bulkTriggerWorkflow: {
            readonly name: "BulkTriggerWorkflow";
            readonly requestType: typeof BulkTriggerWorkflowRequest;
            readonly requestStream: false;
            readonly responseType: typeof BulkTriggerWorkflowResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly putRateLimit: {
            readonly name: "PutRateLimit";
            readonly requestType: typeof PutRateLimitRequest;
            readonly requestStream: false;
            readonly responseType: typeof PutRateLimitResponse;
            readonly responseStream: false;
            readonly options: {};
        };
    };
};
export interface WorkflowServiceImplementation<CallContextExt = {}> {
    putWorkflow(request: PutWorkflowRequest, context: CallContext & CallContextExt): Promise<DeepPartial<WorkflowVersion>>;
    scheduleWorkflow(request: ScheduleWorkflowRequest, context: CallContext & CallContextExt): Promise<DeepPartial<WorkflowVersion>>;
    triggerWorkflow(request: TriggerWorkflowRequest, context: CallContext & CallContextExt): Promise<DeepPartial<TriggerWorkflowResponse>>;
    bulkTriggerWorkflow(request: BulkTriggerWorkflowRequest, context: CallContext & CallContextExt): Promise<DeepPartial<BulkTriggerWorkflowResponse>>;
    putRateLimit(request: PutRateLimitRequest, context: CallContext & CallContextExt): Promise<DeepPartial<PutRateLimitResponse>>;
}
export interface WorkflowServiceClient<CallOptionsExt = {}> {
    putWorkflow(request: DeepPartial<PutWorkflowRequest>, options?: CallOptions & CallOptionsExt): Promise<WorkflowVersion>;
    scheduleWorkflow(request: DeepPartial<ScheduleWorkflowRequest>, options?: CallOptions & CallOptionsExt): Promise<WorkflowVersion>;
    triggerWorkflow(request: DeepPartial<TriggerWorkflowRequest>, options?: CallOptions & CallOptionsExt): Promise<TriggerWorkflowResponse>;
    bulkTriggerWorkflow(request: DeepPartial<BulkTriggerWorkflowRequest>, options?: CallOptions & CallOptionsExt): Promise<BulkTriggerWorkflowResponse>;
    putRateLimit(request: DeepPartial<PutRateLimitRequest>, options?: CallOptions & CallOptionsExt): Promise<PutRateLimitResponse>;
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
