import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import type { CallContext, CallOptions } from 'nice-grpc-common';
export declare const protobufPackage = "";
export declare enum SDKS {
    UNKNOWN = 0,
    GO = 1,
    PYTHON = 2,
    TYPESCRIPT = 3,
    RUBY = 4,
    UNRECOGNIZED = -1
}
export declare function sDKSFromJSON(object: any): SDKS;
export declare function sDKSToJSON(object: SDKS): string;
export declare enum ActionType {
    START_STEP_RUN = 0,
    CANCEL_STEP_RUN = 1,
    START_GET_GROUP_KEY = 2,
    START_BATCH = 3,
    UNRECOGNIZED = -1
}
export declare function actionTypeFromJSON(object: any): ActionType;
export declare function actionTypeToJSON(object: ActionType): string;
export declare enum GroupKeyActionEventType {
    GROUP_KEY_EVENT_TYPE_UNKNOWN = 0,
    GROUP_KEY_EVENT_TYPE_STARTED = 1,
    GROUP_KEY_EVENT_TYPE_COMPLETED = 2,
    GROUP_KEY_EVENT_TYPE_FAILED = 3,
    UNRECOGNIZED = -1
}
export declare function groupKeyActionEventTypeFromJSON(object: any): GroupKeyActionEventType;
export declare function groupKeyActionEventTypeToJSON(object: GroupKeyActionEventType): string;
export declare enum StepActionEventType {
    STEP_EVENT_TYPE_UNKNOWN = 0,
    STEP_EVENT_TYPE_STARTED = 1,
    STEP_EVENT_TYPE_COMPLETED = 2,
    STEP_EVENT_TYPE_FAILED = 3,
    STEP_EVENT_TYPE_ACKNOWLEDGED = 4,
    STEP_EVENT_TYPE_CANCELLED = 5,
    UNRECOGNIZED = -1
}
export declare function stepActionEventTypeFromJSON(object: any): StepActionEventType;
export declare function stepActionEventTypeToJSON(object: StepActionEventType): string;
export declare enum ResourceType {
    RESOURCE_TYPE_UNKNOWN = 0,
    RESOURCE_TYPE_STEP_RUN = 1,
    RESOURCE_TYPE_WORKFLOW_RUN = 2,
    UNRECOGNIZED = -1
}
export declare function resourceTypeFromJSON(object: any): ResourceType;
export declare function resourceTypeToJSON(object: ResourceType): string;
export declare enum ResourceEventType {
    RESOURCE_EVENT_TYPE_UNKNOWN = 0,
    RESOURCE_EVENT_TYPE_STARTED = 1,
    RESOURCE_EVENT_TYPE_COMPLETED = 2,
    RESOURCE_EVENT_TYPE_FAILED = 3,
    RESOURCE_EVENT_TYPE_CANCELLED = 4,
    RESOURCE_EVENT_TYPE_TIMED_OUT = 5,
    RESOURCE_EVENT_TYPE_STREAM = 6,
    UNRECOGNIZED = -1
}
export declare function resourceEventTypeFromJSON(object: any): ResourceEventType;
export declare function resourceEventTypeToJSON(object: ResourceEventType): string;
export declare enum WorkflowRunEventType {
    WORKFLOW_RUN_EVENT_TYPE_FINISHED = 0,
    UNRECOGNIZED = -1
}
export declare function workflowRunEventTypeFromJSON(object: any): WorkflowRunEventType;
export declare function workflowRunEventTypeToJSON(object: WorkflowRunEventType): string;
export interface WorkerLabels {
    /** value of the label */
    strValue?: string | undefined;
    intValue?: number | undefined;
}
export interface RuntimeInfo {
    sdkVersion?: string | undefined;
    language?: SDKS | undefined;
    languageVersion?: string | undefined;
    os?: string | undefined;
    extra?: string | undefined;
}
export interface WorkerRegisterRequest {
    /** the name of the worker */
    workerName: string;
    /** a list of actions that this worker can run */
    actions: string[];
    /** (optional) the services for this worker */
    services: string[];
    /**
     * (optional) the number of default slots this worker can handle
     * deprecated: use slot_config instead
     */
    slots?: number | undefined;
    /** (optional) worker labels (i.e. state or other metadata) */
    labels: {
        [key: string]: WorkerLabels;
    };
    /** (optional) webhookId is the id of the webhook that the worker is associated with (if any) */
    webhookId?: string | undefined;
    /** (optional) information regarding the runtime environment of the worker */
    runtimeInfo?: RuntimeInfo | undefined;
    /** (optional) slot config for this worker (slot_type -> units) */
    slotConfig: {
        [key: string]: number;
    };
}
export interface WorkerRegisterRequest_LabelsEntry {
    key: string;
    value: WorkerLabels | undefined;
}
export interface WorkerRegisterRequest_SlotConfigEntry {
    key: string;
    value: number;
}
export interface WorkerRegisterResponse {
    /** the tenant id */
    tenantId: string;
    /** the id of the worker */
    workerId: string;
    /** the name of the worker */
    workerName: string;
}
export interface UpsertWorkerLabelsRequest {
    /** the name of the worker */
    workerId: string;
    /** (optional) the worker labels */
    labels: {
        [key: string]: WorkerLabels;
    };
}
export interface UpsertWorkerLabelsRequest_LabelsEntry {
    key: string;
    value: WorkerLabels | undefined;
}
export interface UpsertWorkerLabelsResponse {
    /** the tenant id */
    tenantId: string;
    /** the id of the worker */
    workerId: string;
}
export interface AssignedAction {
    /** the tenant id */
    tenantId: string;
    /** the workflow run id (optional) */
    workflowRunId: string;
    /** the get group key run id (optional) */
    getGroupKeyRunId: string;
    /** the job id */
    jobId: string;
    /** the job name */
    jobName: string;
    /** the job run id */
    jobRunId: string;
    /** the task id */
    taskId: string;
    /** the task external run id */
    taskRunExternalId: string;
    /** the action id */
    actionId: string;
    /** the action type */
    actionType: ActionType;
    /** the action payload */
    actionPayload: string;
    /** the task name */
    taskName: string;
    /** the count number of the retry attempt */
    retryCount: number;
    /** (optional) additional metadata set on the workflow */
    additionalMetadata?: string | undefined;
    /** (optional) the child workflow index (if this is a child workflow) */
    childWorkflowIndex?: number | undefined;
    /** (optional) the child workflow key (if this is a child workflow) */
    childWorkflowKey?: string | undefined;
    /** (optional) the parent workflow run id (if this is a child workflow) */
    parentWorkflowRunId?: string | undefined;
    /** (optional) the priority of the run */
    priority: number;
    /** (optional) the workflow id */
    workflowId?: string | undefined;
    /** (optional) the workflow version id */
    workflowVersionId?: string | undefined;
    /** (optional) the invocation count for durable task events (required for durable events, otherwise null) */
    durableTaskInvocationCount?: number | undefined;
    /** (optional) the external id of the event that triggered this workflow run (if any) */
    triggeringEventExternalId?: string | undefined;
    /** (optional) the key of the event that triggered this workflow run (if any) */
    triggeringEventKey?: string | undefined;
    /** (optional) the batch identifier if this task is part of a batch */
    batchId?: string | undefined;
    /** (optional) the total number of tasks in the batch */
    batchSize?: number | undefined;
    /** (optional) the index of the task within the batch */
    batchIndex?: number | undefined;
    /** (optional) payload for starting a batch execution */
    batchStartPayload?: BatchStartPayload | undefined;
    /** (optional) the partition key used for batching */
    batchKey?: string | undefined;
    /** (optional) inserted_at of the assigned task. The DAG operator uses this to prune durable log partitions. */
    taskInsertedAt?: Date | undefined;
}
export interface BatchStartPayload {
    triggerReason: string;
    triggerTime: Date | undefined;
    expectedSize: number;
}
export interface WorkerListenRequest {
    /** the id of the worker */
    workerId: string;
}
export interface WorkerUnsubscribeRequest {
    /** the id of the worker */
    workerId: string;
}
export interface WorkerUnsubscribeResponse {
    /** the tenant id to unsubscribe from */
    tenantId: string;
    /** the id of the worker */
    workerId: string;
}
export interface GroupKeyActionEvent {
    /** the id of the worker */
    workerId: string;
    /** the id of the job */
    workflowRunId: string;
    getGroupKeyRunId: string;
    /** the action id */
    actionId: string;
    eventTimestamp: Date | undefined;
    /** the task event type */
    eventType: GroupKeyActionEventType;
    /** the event payload */
    eventPayload: string;
}
export interface StepActionEvent {
    /** the id of the worker */
    workerId: string;
    /** the id of the job */
    jobId: string;
    /** the job run id */
    jobRunId: string;
    /** the id of the task */
    taskId: string;
    /** the task external run id */
    taskRunExternalId: string;
    /** the action id */
    actionId: string;
    eventTimestamp: Date | undefined;
    /** the task event type */
    eventType: StepActionEventType;
    /** the event payload */
    eventPayload: string;
    /** the retry count */
    retryCount?: number | undefined;
    /** a flag indicating if the task should _not_ be retried */
    shouldNotRetry?: boolean | undefined;
}
export interface BatchActionEventItem {
    /** the task external run id */
    taskRunExternalId: string;
    /** the event payload (e.g. serialized error for FAILED; unused for STARTED/CANCELLED) */
    eventPayload: string;
    /** the retry count (falls back to the task's current retry count if unset) */
    retryCount?: number | undefined;
    /** a flag indicating if the task should _not_ be retried (FAILED only) */
    shouldNotRetry?: boolean | undefined;
}
export interface BatchActionEvent {
    /** the id of the worker */
    workerId: string;
    /** the id of the job (shared across all items in the batch) */
    jobId: string;
    /** the action id (shared across all items in the batch) */
    actionId: string;
    /** (optional) the batch identifier, for observability */
    batchId?: string | undefined;
    eventTimestamp: Date | undefined;
    /** the task event type -- applies uniformly to every item in this batch */
    eventType: StepActionEventType;
    /** the items in this batch */
    items: BatchActionEventItem[];
}
export interface ActionEventResponse {
    /** the tenant id */
    tenantId: string;
    /** the id of the worker */
    workerId: string;
}
export interface SubscribeToWorkflowEventsRequest {
    /** the id of the workflow run */
    workflowRunId?: string | undefined;
    /** the key of the additional meta field to subscribe to */
    additionalMetaKey?: string | undefined;
    /** the value of the additional meta field to subscribe to */
    additionalMetaValue?: string | undefined;
}
export interface SubscribeToWorkflowRunsRequest {
    /** the id of the workflow run */
    workflowRunId: string;
}
export interface WorkflowEvent {
    /** the id of the workflow run */
    workflowRunId: string;
    resourceType: ResourceType;
    eventType: ResourceEventType;
    resourceId: string;
    eventTimestamp: Date | undefined;
    /** the event payload */
    eventPayload: string;
    /**
     * whether this is the last event for the workflow run - server
     * will hang up the connection but clients might want to case
     */
    hangup: boolean;
    /** (optional) the max number of retries this task can handle */
    taskRetries?: number | undefined;
    /** (optional) the retry count of this task */
    retryCount?: number | undefined;
    eventIndex?: number | undefined;
}
export interface WorkflowRunEvent {
    /** the id of the workflow run */
    workflowRunId: string;
    eventType: WorkflowRunEventType;
    eventTimestamp: Date | undefined;
    results: StepRunResult[];
}
export interface StepRunResult {
    taskRunExternalId: string;
    taskName: string;
    jobRunId: string;
    error?: string | undefined;
    output?: string | undefined;
}
export interface OverridesData {
    /** the task run id */
    taskRunExternalId: string;
    /** the path of the data to set */
    path: string;
    /** the value to set */
    value: string;
    /** the filename of the caller */
    callerFilename: string;
}
export interface OverridesDataResponse {
}
export interface HeartbeatRequest {
    /** the id of the worker */
    workerId: string;
    /** heartbeatAt is the time the worker sent the heartbeat */
    heartbeatAt: Date | undefined;
}
export interface HeartbeatResponse {
}
export interface RefreshTimeoutRequest {
    /** the id of the task run to release */
    taskRunExternalId: string;
    incrementTimeoutBy: string;
}
export interface RefreshTimeoutResponse {
    timeoutAt: Date | undefined;
}
export interface ReleaseSlotRequest {
    /** the id of the task run to release */
    taskRunExternalId: string;
}
export interface ReleaseSlotResponse {
}
export interface RestoreEvictedTaskRequest {
    taskRunExternalId: string;
}
export interface RestoreEvictedTaskResponse {
    requeued: boolean;
}
export interface GetVersionRequest {
}
export interface GetVersionResponse {
    version: string;
}
export declare const WorkerLabels: MessageFns<WorkerLabels>;
export declare const RuntimeInfo: MessageFns<RuntimeInfo>;
export declare const WorkerRegisterRequest: MessageFns<WorkerRegisterRequest>;
export declare const WorkerRegisterRequest_LabelsEntry: MessageFns<WorkerRegisterRequest_LabelsEntry>;
export declare const WorkerRegisterRequest_SlotConfigEntry: MessageFns<WorkerRegisterRequest_SlotConfigEntry>;
export declare const WorkerRegisterResponse: MessageFns<WorkerRegisterResponse>;
export declare const UpsertWorkerLabelsRequest: MessageFns<UpsertWorkerLabelsRequest>;
export declare const UpsertWorkerLabelsRequest_LabelsEntry: MessageFns<UpsertWorkerLabelsRequest_LabelsEntry>;
export declare const UpsertWorkerLabelsResponse: MessageFns<UpsertWorkerLabelsResponse>;
export declare const AssignedAction: MessageFns<AssignedAction>;
export declare const BatchStartPayload: MessageFns<BatchStartPayload>;
export declare const WorkerListenRequest: MessageFns<WorkerListenRequest>;
export declare const WorkerUnsubscribeRequest: MessageFns<WorkerUnsubscribeRequest>;
export declare const WorkerUnsubscribeResponse: MessageFns<WorkerUnsubscribeResponse>;
export declare const GroupKeyActionEvent: MessageFns<GroupKeyActionEvent>;
export declare const StepActionEvent: MessageFns<StepActionEvent>;
export declare const BatchActionEventItem: MessageFns<BatchActionEventItem>;
export declare const BatchActionEvent: MessageFns<BatchActionEvent>;
export declare const ActionEventResponse: MessageFns<ActionEventResponse>;
export declare const SubscribeToWorkflowEventsRequest: MessageFns<SubscribeToWorkflowEventsRequest>;
export declare const SubscribeToWorkflowRunsRequest: MessageFns<SubscribeToWorkflowRunsRequest>;
export declare const WorkflowEvent: MessageFns<WorkflowEvent>;
export declare const WorkflowRunEvent: MessageFns<WorkflowRunEvent>;
export declare const StepRunResult: MessageFns<StepRunResult>;
export declare const OverridesData: MessageFns<OverridesData>;
export declare const OverridesDataResponse: MessageFns<OverridesDataResponse>;
export declare const HeartbeatRequest: MessageFns<HeartbeatRequest>;
export declare const HeartbeatResponse: MessageFns<HeartbeatResponse>;
export declare const RefreshTimeoutRequest: MessageFns<RefreshTimeoutRequest>;
export declare const RefreshTimeoutResponse: MessageFns<RefreshTimeoutResponse>;
export declare const ReleaseSlotRequest: MessageFns<ReleaseSlotRequest>;
export declare const ReleaseSlotResponse: MessageFns<ReleaseSlotResponse>;
export declare const RestoreEvictedTaskRequest: MessageFns<RestoreEvictedTaskRequest>;
export declare const RestoreEvictedTaskResponse: MessageFns<RestoreEvictedTaskResponse>;
export declare const GetVersionRequest: MessageFns<GetVersionRequest>;
export declare const GetVersionResponse: MessageFns<GetVersionResponse>;
export type DispatcherDefinition = typeof DispatcherDefinition;
export declare const DispatcherDefinition: {
    readonly name: "Dispatcher";
    readonly fullName: "Dispatcher";
    readonly methods: {
        readonly register: {
            readonly name: "Register";
            readonly requestType: typeof WorkerRegisterRequest;
            readonly requestStream: false;
            readonly responseType: typeof WorkerRegisterResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly listen: {
            readonly name: "Listen";
            readonly requestType: typeof WorkerListenRequest;
            readonly requestStream: false;
            readonly responseType: typeof AssignedAction;
            readonly responseStream: true;
            readonly options: {};
        };
        /**
         * ListenV2 is like listen, but implementation does not include heartbeats. This should only used by SDKs
         * against engine version v0.18.1+
         */
        readonly listenV2: {
            readonly name: "ListenV2";
            readonly requestType: typeof WorkerListenRequest;
            readonly requestStream: false;
            readonly responseType: typeof AssignedAction;
            readonly responseStream: true;
            readonly options: {};
        };
        /** Heartbeat is a method for workers to send heartbeats to the dispatcher */
        readonly heartbeat: {
            readonly name: "Heartbeat";
            readonly requestType: typeof HeartbeatRequest;
            readonly requestStream: false;
            readonly responseType: typeof HeartbeatResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly subscribeToWorkflowEvents: {
            readonly name: "SubscribeToWorkflowEvents";
            readonly requestType: typeof SubscribeToWorkflowEventsRequest;
            readonly requestStream: false;
            readonly responseType: typeof WorkflowEvent;
            readonly responseStream: true;
            readonly options: {};
        };
        readonly subscribeToWorkflowRuns: {
            readonly name: "SubscribeToWorkflowRuns";
            readonly requestType: typeof SubscribeToWorkflowRunsRequest;
            readonly requestStream: true;
            readonly responseType: typeof WorkflowRunEvent;
            readonly responseStream: true;
            readonly options: {};
        };
        readonly sendStepActionEvent: {
            readonly name: "SendStepActionEvent";
            readonly requestType: typeof StepActionEvent;
            readonly requestStream: false;
            readonly responseType: typeof ActionEventResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        /**
         * SendBatchActionEvent reports a single lifecycle event (STARTED, FAILED, or CANCELLED)
         * for every task in a batch in one call, instead of one SendStepActionEvent call per task.
         */
        readonly sendBatchActionEvent: {
            readonly name: "SendBatchActionEvent";
            readonly requestType: typeof BatchActionEvent;
            readonly requestStream: false;
            readonly responseType: typeof ActionEventResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly sendGroupKeyActionEvent: {
            readonly name: "SendGroupKeyActionEvent";
            readonly requestType: typeof GroupKeyActionEvent;
            readonly requestStream: false;
            readonly responseType: typeof ActionEventResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly putOverridesData: {
            readonly name: "PutOverridesData";
            readonly requestType: typeof OverridesData;
            readonly requestStream: false;
            readonly responseType: typeof OverridesDataResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly unsubscribe: {
            readonly name: "Unsubscribe";
            readonly requestType: typeof WorkerUnsubscribeRequest;
            readonly requestStream: false;
            readonly responseType: typeof WorkerUnsubscribeResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly refreshTimeout: {
            readonly name: "RefreshTimeout";
            readonly requestType: typeof RefreshTimeoutRequest;
            readonly requestStream: false;
            readonly responseType: typeof RefreshTimeoutResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly releaseSlot: {
            readonly name: "ReleaseSlot";
            readonly requestType: typeof ReleaseSlotRequest;
            readonly requestStream: false;
            readonly responseType: typeof ReleaseSlotResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly restoreEvictedTask: {
            readonly name: "RestoreEvictedTask";
            readonly requestType: typeof RestoreEvictedTaskRequest;
            readonly requestStream: false;
            readonly responseType: typeof RestoreEvictedTaskResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly upsertWorkerLabels: {
            readonly name: "UpsertWorkerLabels";
            readonly requestType: typeof UpsertWorkerLabelsRequest;
            readonly requestStream: false;
            readonly responseType: typeof UpsertWorkerLabelsResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        /**
         * GetVersion returns the dispatcher protocol version as a simple integer.
         * SDKs use this to determine feature support (e.g. slot_config registration).
         * Old engines that do not implement this RPC will return UNIMPLEMENTED.
         */
        readonly getVersion: {
            readonly name: "GetVersion";
            readonly requestType: typeof GetVersionRequest;
            readonly requestStream: false;
            readonly responseType: typeof GetVersionResponse;
            readonly responseStream: false;
            readonly options: {};
        };
    };
};
export interface DispatcherServiceImplementation<CallContextExt = {}> {
    register(request: WorkerRegisterRequest, context: CallContext & CallContextExt): Promise<DeepPartial<WorkerRegisterResponse>>;
    listen(request: WorkerListenRequest, context: CallContext & CallContextExt): ServerStreamingMethodResult<DeepPartial<AssignedAction>>;
    /**
     * ListenV2 is like listen, but implementation does not include heartbeats. This should only used by SDKs
     * against engine version v0.18.1+
     */
    listenV2(request: WorkerListenRequest, context: CallContext & CallContextExt): ServerStreamingMethodResult<DeepPartial<AssignedAction>>;
    /** Heartbeat is a method for workers to send heartbeats to the dispatcher */
    heartbeat(request: HeartbeatRequest, context: CallContext & CallContextExt): Promise<DeepPartial<HeartbeatResponse>>;
    subscribeToWorkflowEvents(request: SubscribeToWorkflowEventsRequest, context: CallContext & CallContextExt): ServerStreamingMethodResult<DeepPartial<WorkflowEvent>>;
    subscribeToWorkflowRuns(request: AsyncIterable<SubscribeToWorkflowRunsRequest>, context: CallContext & CallContextExt): ServerStreamingMethodResult<DeepPartial<WorkflowRunEvent>>;
    sendStepActionEvent(request: StepActionEvent, context: CallContext & CallContextExt): Promise<DeepPartial<ActionEventResponse>>;
    /**
     * SendBatchActionEvent reports a single lifecycle event (STARTED, FAILED, or CANCELLED)
     * for every task in a batch in one call, instead of one SendStepActionEvent call per task.
     */
    sendBatchActionEvent(request: BatchActionEvent, context: CallContext & CallContextExt): Promise<DeepPartial<ActionEventResponse>>;
    sendGroupKeyActionEvent(request: GroupKeyActionEvent, context: CallContext & CallContextExt): Promise<DeepPartial<ActionEventResponse>>;
    putOverridesData(request: OverridesData, context: CallContext & CallContextExt): Promise<DeepPartial<OverridesDataResponse>>;
    unsubscribe(request: WorkerUnsubscribeRequest, context: CallContext & CallContextExt): Promise<DeepPartial<WorkerUnsubscribeResponse>>;
    refreshTimeout(request: RefreshTimeoutRequest, context: CallContext & CallContextExt): Promise<DeepPartial<RefreshTimeoutResponse>>;
    releaseSlot(request: ReleaseSlotRequest, context: CallContext & CallContextExt): Promise<DeepPartial<ReleaseSlotResponse>>;
    restoreEvictedTask(request: RestoreEvictedTaskRequest, context: CallContext & CallContextExt): Promise<DeepPartial<RestoreEvictedTaskResponse>>;
    upsertWorkerLabels(request: UpsertWorkerLabelsRequest, context: CallContext & CallContextExt): Promise<DeepPartial<UpsertWorkerLabelsResponse>>;
    /**
     * GetVersion returns the dispatcher protocol version as a simple integer.
     * SDKs use this to determine feature support (e.g. slot_config registration).
     * Old engines that do not implement this RPC will return UNIMPLEMENTED.
     */
    getVersion(request: GetVersionRequest, context: CallContext & CallContextExt): Promise<DeepPartial<GetVersionResponse>>;
}
export interface DispatcherClient<CallOptionsExt = {}> {
    register(request: DeepPartial<WorkerRegisterRequest>, options?: CallOptions & CallOptionsExt): Promise<WorkerRegisterResponse>;
    listen(request: DeepPartial<WorkerListenRequest>, options?: CallOptions & CallOptionsExt): AsyncIterable<AssignedAction>;
    /**
     * ListenV2 is like listen, but implementation does not include heartbeats. This should only used by SDKs
     * against engine version v0.18.1+
     */
    listenV2(request: DeepPartial<WorkerListenRequest>, options?: CallOptions & CallOptionsExt): AsyncIterable<AssignedAction>;
    /** Heartbeat is a method for workers to send heartbeats to the dispatcher */
    heartbeat(request: DeepPartial<HeartbeatRequest>, options?: CallOptions & CallOptionsExt): Promise<HeartbeatResponse>;
    subscribeToWorkflowEvents(request: DeepPartial<SubscribeToWorkflowEventsRequest>, options?: CallOptions & CallOptionsExt): AsyncIterable<WorkflowEvent>;
    subscribeToWorkflowRuns(request: AsyncIterable<DeepPartial<SubscribeToWorkflowRunsRequest>>, options?: CallOptions & CallOptionsExt): AsyncIterable<WorkflowRunEvent>;
    sendStepActionEvent(request: DeepPartial<StepActionEvent>, options?: CallOptions & CallOptionsExt): Promise<ActionEventResponse>;
    /**
     * SendBatchActionEvent reports a single lifecycle event (STARTED, FAILED, or CANCELLED)
     * for every task in a batch in one call, instead of one SendStepActionEvent call per task.
     */
    sendBatchActionEvent(request: DeepPartial<BatchActionEvent>, options?: CallOptions & CallOptionsExt): Promise<ActionEventResponse>;
    sendGroupKeyActionEvent(request: DeepPartial<GroupKeyActionEvent>, options?: CallOptions & CallOptionsExt): Promise<ActionEventResponse>;
    putOverridesData(request: DeepPartial<OverridesData>, options?: CallOptions & CallOptionsExt): Promise<OverridesDataResponse>;
    unsubscribe(request: DeepPartial<WorkerUnsubscribeRequest>, options?: CallOptions & CallOptionsExt): Promise<WorkerUnsubscribeResponse>;
    refreshTimeout(request: DeepPartial<RefreshTimeoutRequest>, options?: CallOptions & CallOptionsExt): Promise<RefreshTimeoutResponse>;
    releaseSlot(request: DeepPartial<ReleaseSlotRequest>, options?: CallOptions & CallOptionsExt): Promise<ReleaseSlotResponse>;
    restoreEvictedTask(request: DeepPartial<RestoreEvictedTaskRequest>, options?: CallOptions & CallOptionsExt): Promise<RestoreEvictedTaskResponse>;
    upsertWorkerLabels(request: DeepPartial<UpsertWorkerLabelsRequest>, options?: CallOptions & CallOptionsExt): Promise<UpsertWorkerLabelsResponse>;
    /**
     * GetVersion returns the dispatcher protocol version as a simple integer.
     * SDKs use this to determine feature support (e.g. slot_config registration).
     * Old engines that do not implement this RPC will return UNIMPLEMENTED.
     */
    getVersion(request: DeepPartial<GetVersionRequest>, options?: CallOptions & CallOptionsExt): Promise<GetVersionResponse>;
}
type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;
export type DeepPartial<T> = T extends Builtin ? T : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {} ? {
    [K in keyof T]?: DeepPartial<T[K]>;
} : Partial<T>;
export type ServerStreamingMethodResult<Response> = {
    [Symbol.asyncIterator](): AsyncIterator<Response, void>;
};
export interface MessageFns<T> {
    encode(message: T, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): T;
    fromJSON(object: any): T;
    toJSON(message: T): unknown;
    create(base?: DeepPartial<T>): T;
    fromPartial(object: DeepPartial<T>): T;
}
export {};
