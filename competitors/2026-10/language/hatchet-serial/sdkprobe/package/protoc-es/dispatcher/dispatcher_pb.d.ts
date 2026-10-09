import type { GenEnum, GenFile, GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file dispatcher/dispatcher.proto.
 */
export declare const file_dispatcher_dispatcher: GenFile;
/**
 * @generated from message WorkerLabels
 */
export type WorkerLabels = Message<"WorkerLabels"> & {
    /**
     * value of the label
     *
     * @generated from field: optional string str_value = 1;
     */
    strValue?: string | undefined;
    /**
     * @generated from field: optional int32 int_value = 2;
     */
    intValue?: number | undefined;
};
/**
 * Describes the message WorkerLabels.
 * Use `create(WorkerLabelsSchema)` to create a new message.
 */
export declare const WorkerLabelsSchema: GenMessage<WorkerLabels>;
/**
 * @generated from message RuntimeInfo
 */
export type RuntimeInfo = Message<"RuntimeInfo"> & {
    /**
     * @generated from field: optional string sdk_version = 1;
     */
    sdkVersion?: string | undefined;
    /**
     * @generated from field: optional SDKS language = 2;
     */
    language?: SDKS | undefined;
    /**
     * @generated from field: optional string language_version = 3;
     */
    languageVersion?: string | undefined;
    /**
     * @generated from field: optional string os = 4;
     */
    os?: string | undefined;
    /**
     * @generated from field: optional string extra = 5;
     */
    extra?: string | undefined;
};
/**
 * Describes the message RuntimeInfo.
 * Use `create(RuntimeInfoSchema)` to create a new message.
 */
export declare const RuntimeInfoSchema: GenMessage<RuntimeInfo>;
/**
 * @generated from message WorkerRegisterRequest
 */
export type WorkerRegisterRequest = Message<"WorkerRegisterRequest"> & {
    /**
     * the name of the worker
     *
     * @generated from field: string worker_name = 1;
     */
    workerName: string;
    /**
     * a list of actions that this worker can run
     *
     * @generated from field: repeated string actions = 2;
     */
    actions: string[];
    /**
     * (optional) the services for this worker
     *
     * @generated from field: repeated string services = 3;
     */
    services: string[];
    /**
     * (optional) the number of default slots this worker can handle
     * deprecated: use slot_config instead
     *
     * @generated from field: optional int32 slots = 4;
     */
    slots?: number | undefined;
    /**
     * (optional) worker labels (i.e. state or other metadata)
     *
     * @generated from field: map<string, WorkerLabels> labels = 5;
     */
    labels: {
        [key: string]: WorkerLabels;
    };
    /**
     * (optional) webhookId is the id of the webhook that the worker is associated with (if any)
     *
     * @generated from field: optional string webhook_id = 6;
     */
    webhookId?: string | undefined;
    /**
     * (optional) information regarding the runtime environment of the worker
     *
     * @generated from field: optional RuntimeInfo runtime_info = 7;
     */
    runtimeInfo?: RuntimeInfo | undefined;
    /**
     * (optional) slot config for this worker (slot_type -> units)
     *
     * @generated from field: map<string, int32> slot_config = 9;
     */
    slotConfig: {
        [key: string]: number;
    };
};
/**
 * Describes the message WorkerRegisterRequest.
 * Use `create(WorkerRegisterRequestSchema)` to create a new message.
 */
export declare const WorkerRegisterRequestSchema: GenMessage<WorkerRegisterRequest>;
/**
 * @generated from message WorkerRegisterResponse
 */
export type WorkerRegisterResponse = Message<"WorkerRegisterResponse"> & {
    /**
     * the tenant id
     *
     * @generated from field: string tenant_id = 1;
     */
    tenantId: string;
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 2;
     */
    workerId: string;
    /**
     * the name of the worker
     *
     * @generated from field: string worker_name = 3;
     */
    workerName: string;
};
/**
 * Describes the message WorkerRegisterResponse.
 * Use `create(WorkerRegisterResponseSchema)` to create a new message.
 */
export declare const WorkerRegisterResponseSchema: GenMessage<WorkerRegisterResponse>;
/**
 * @generated from message UpsertWorkerLabelsRequest
 */
export type UpsertWorkerLabelsRequest = Message<"UpsertWorkerLabelsRequest"> & {
    /**
     * the name of the worker
     *
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
    /**
     * (optional) the worker labels
     *
     * @generated from field: map<string, WorkerLabels> labels = 2;
     */
    labels: {
        [key: string]: WorkerLabels;
    };
};
/**
 * Describes the message UpsertWorkerLabelsRequest.
 * Use `create(UpsertWorkerLabelsRequestSchema)` to create a new message.
 */
export declare const UpsertWorkerLabelsRequestSchema: GenMessage<UpsertWorkerLabelsRequest>;
/**
 * @generated from message UpsertWorkerLabelsResponse
 */
export type UpsertWorkerLabelsResponse = Message<"UpsertWorkerLabelsResponse"> & {
    /**
     * the tenant id
     *
     * @generated from field: string tenant_id = 1;
     */
    tenantId: string;
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 2;
     */
    workerId: string;
};
/**
 * Describes the message UpsertWorkerLabelsResponse.
 * Use `create(UpsertWorkerLabelsResponseSchema)` to create a new message.
 */
export declare const UpsertWorkerLabelsResponseSchema: GenMessage<UpsertWorkerLabelsResponse>;
/**
 * @generated from message AssignedAction
 */
export type AssignedAction = Message<"AssignedAction"> & {
    /**
     * the tenant id
     *
     * @generated from field: string tenant_id = 1;
     */
    tenantId: string;
    /**
     * the workflow run id (optional)
     *
     * @generated from field: string workflow_run_id = 2;
     */
    workflowRunId: string;
    /**
     * the get group key run id (optional)
     *
     * @generated from field: string get_group_key_run_id = 3;
     */
    getGroupKeyRunId: string;
    /**
     * the job id
     *
     * @generated from field: string job_id = 4;
     */
    jobId: string;
    /**
     * the job name
     *
     * @generated from field: string job_name = 5;
     */
    jobName: string;
    /**
     * the job run id
     *
     * @generated from field: string job_run_id = 6;
     */
    jobRunId: string;
    /**
     * the task id
     *
     * @generated from field: string task_id = 7;
     */
    taskId: string;
    /**
     * the task external run id
     *
     * @generated from field: string task_run_external_id = 8;
     */
    taskRunExternalId: string;
    /**
     * the action id
     *
     * @generated from field: string action_id = 9;
     */
    actionId: string;
    /**
     * the action type
     *
     * @generated from field: ActionType action_type = 10;
     */
    actionType: ActionType;
    /**
     * the action payload
     *
     * @generated from field: string action_payload = 11;
     */
    actionPayload: string;
    /**
     * the task name
     *
     * @generated from field: string task_name = 12;
     */
    taskName: string;
    /**
     * the count number of the retry attempt
     *
     * @generated from field: int32 retry_count = 13;
     */
    retryCount: number;
    /**
     * (optional) additional metadata set on the workflow
     *
     * @generated from field: optional string additional_metadata = 14;
     */
    additionalMetadata?: string | undefined;
    /**
     * (optional) the child workflow index (if this is a child workflow)
     *
     * @generated from field: optional int32 child_workflow_index = 15;
     */
    childWorkflowIndex?: number | undefined;
    /**
     * (optional) the child workflow key (if this is a child workflow)
     *
     * @generated from field: optional string child_workflow_key = 16;
     */
    childWorkflowKey?: string | undefined;
    /**
     * (optional) the parent workflow run id (if this is a child workflow)
     *
     * @generated from field: optional string parent_workflow_run_id = 17;
     */
    parentWorkflowRunId?: string | undefined;
    /**
     * (optional) the priority of the run
     *
     * @generated from field: int32 priority = 18;
     */
    priority: number;
    /**
     * (optional) the workflow id
     *
     * @generated from field: optional string workflow_id = 19;
     */
    workflowId?: string | undefined;
    /**
     * (optional) the workflow version id
     *
     * @generated from field: optional string workflow_version_id = 20;
     */
    workflowVersionId?: string | undefined;
    /**
     * (optional) the invocation count for durable task events (required for durable events, otherwise null)
     *
     * @generated from field: optional int32 durable_task_invocation_count = 21;
     */
    durableTaskInvocationCount?: number | undefined;
    /**
     * (optional) the external id of the event that triggered this workflow run (if any)
     *
     * @generated from field: optional string triggering_event_external_id = 22;
     */
    triggeringEventExternalId?: string | undefined;
    /**
     * (optional) the key of the event that triggered this workflow run (if any)
     *
     * @generated from field: optional string triggering_event_key = 23;
     */
    triggeringEventKey?: string | undefined;
    /**
     * (optional) the batch identifier if this task is part of a batch
     *
     * @generated from field: optional string batchId = 24;
     */
    batchId?: string | undefined;
    /**
     * (optional) the total number of tasks in the batch
     *
     * @generated from field: optional int32 batchSize = 25;
     */
    batchSize?: number | undefined;
    /**
     * (optional) the index of the task within the batch
     *
     * @generated from field: optional int32 batchIndex = 26;
     */
    batchIndex?: number | undefined;
    /**
     * (optional) payload for starting a batch execution
     *
     * @generated from field: optional BatchStartPayload batchStartPayload = 27;
     */
    batchStartPayload?: BatchStartPayload | undefined;
    /**
     * (optional) the partition key used for batching
     *
     * @generated from field: optional string batchKey = 28;
     */
    batchKey?: string | undefined;
    /**
     * (optional) inserted_at of the assigned task. The DAG operator uses this to prune durable log partitions.
     *
     * @generated from field: optional google.protobuf.Timestamp task_inserted_at = 29;
     */
    taskInsertedAt?: Timestamp | undefined;
};
/**
 * Describes the message AssignedAction.
 * Use `create(AssignedActionSchema)` to create a new message.
 */
export declare const AssignedActionSchema: GenMessage<AssignedAction>;
/**
 * @generated from message BatchStartPayload
 */
export type BatchStartPayload = Message<"BatchStartPayload"> & {
    /**
     * @generated from field: string triggerReason = 1;
     */
    triggerReason: string;
    /**
     * @generated from field: google.protobuf.Timestamp triggerTime = 2;
     */
    triggerTime?: Timestamp | undefined;
    /**
     * @generated from field: int32 expectedSize = 3;
     */
    expectedSize: number;
};
/**
 * Describes the message BatchStartPayload.
 * Use `create(BatchStartPayloadSchema)` to create a new message.
 */
export declare const BatchStartPayloadSchema: GenMessage<BatchStartPayload>;
/**
 * @generated from message WorkerListenRequest
 */
export type WorkerListenRequest = Message<"WorkerListenRequest"> & {
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
};
/**
 * Describes the message WorkerListenRequest.
 * Use `create(WorkerListenRequestSchema)` to create a new message.
 */
export declare const WorkerListenRequestSchema: GenMessage<WorkerListenRequest>;
/**
 * @generated from message WorkerUnsubscribeRequest
 */
export type WorkerUnsubscribeRequest = Message<"WorkerUnsubscribeRequest"> & {
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
};
/**
 * Describes the message WorkerUnsubscribeRequest.
 * Use `create(WorkerUnsubscribeRequestSchema)` to create a new message.
 */
export declare const WorkerUnsubscribeRequestSchema: GenMessage<WorkerUnsubscribeRequest>;
/**
 * @generated from message WorkerUnsubscribeResponse
 */
export type WorkerUnsubscribeResponse = Message<"WorkerUnsubscribeResponse"> & {
    /**
     * the tenant id to unsubscribe from
     *
     * @generated from field: string tenant_id = 1;
     */
    tenantId: string;
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 2;
     */
    workerId: string;
};
/**
 * Describes the message WorkerUnsubscribeResponse.
 * Use `create(WorkerUnsubscribeResponseSchema)` to create a new message.
 */
export declare const WorkerUnsubscribeResponseSchema: GenMessage<WorkerUnsubscribeResponse>;
/**
 * @generated from message GroupKeyActionEvent
 */
export type GroupKeyActionEvent = Message<"GroupKeyActionEvent"> & {
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
    /**
     * the id of the job
     *
     * @generated from field: string workflow_run_id = 2;
     */
    workflowRunId: string;
    /**
     * @generated from field: string get_group_key_run_id = 3;
     */
    getGroupKeyRunId: string;
    /**
     * the action id
     *
     * @generated from field: string action_id = 4;
     */
    actionId: string;
    /**
     * @generated from field: google.protobuf.Timestamp event_timestamp = 5;
     */
    eventTimestamp?: Timestamp | undefined;
    /**
     * the task event type
     *
     * @generated from field: GroupKeyActionEventType event_type = 6;
     */
    eventType: GroupKeyActionEventType;
    /**
     * the event payload
     *
     * @generated from field: string event_payload = 7;
     */
    eventPayload: string;
};
/**
 * Describes the message GroupKeyActionEvent.
 * Use `create(GroupKeyActionEventSchema)` to create a new message.
 */
export declare const GroupKeyActionEventSchema: GenMessage<GroupKeyActionEvent>;
/**
 * @generated from message StepActionEvent
 */
export type StepActionEvent = Message<"StepActionEvent"> & {
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
    /**
     * the id of the job
     *
     * @generated from field: string job_id = 2;
     */
    jobId: string;
    /**
     * the job run id
     *
     * @generated from field: string job_run_id = 3;
     */
    jobRunId: string;
    /**
     * the id of the task
     *
     * @generated from field: string task_id = 4;
     */
    taskId: string;
    /**
     * the task external run id
     *
     * @generated from field: string task_run_external_id = 5;
     */
    taskRunExternalId: string;
    /**
     * the action id
     *
     * @generated from field: string action_id = 6;
     */
    actionId: string;
    /**
     * @generated from field: google.protobuf.Timestamp event_timestamp = 7;
     */
    eventTimestamp?: Timestamp | undefined;
    /**
     * the task event type
     *
     * @generated from field: StepActionEventType event_type = 8;
     */
    eventType: StepActionEventType;
    /**
     * the event payload
     *
     * @generated from field: string event_payload = 9;
     */
    eventPayload: string;
    /**
     * the retry count
     *
     * @generated from field: optional int32 retry_count = 10;
     */
    retryCount?: number | undefined;
    /**
     * a flag indicating if the task should _not_ be retried
     *
     * @generated from field: optional bool should_not_retry = 11;
     */
    shouldNotRetry?: boolean | undefined;
};
/**
 * Describes the message StepActionEvent.
 * Use `create(StepActionEventSchema)` to create a new message.
 */
export declare const StepActionEventSchema: GenMessage<StepActionEvent>;
/**
 * @generated from message BatchActionEventItem
 */
export type BatchActionEventItem = Message<"BatchActionEventItem"> & {
    /**
     * the task external run id
     *
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
    /**
     * the event payload (e.g. serialized error for FAILED; unused for STARTED/CANCELLED)
     *
     * @generated from field: string event_payload = 2;
     */
    eventPayload: string;
    /**
     * the retry count (falls back to the task's current retry count if unset)
     *
     * @generated from field: optional int32 retry_count = 3;
     */
    retryCount?: number | undefined;
    /**
     * a flag indicating if the task should _not_ be retried (FAILED only)
     *
     * @generated from field: optional bool should_not_retry = 4;
     */
    shouldNotRetry?: boolean | undefined;
};
/**
 * Describes the message BatchActionEventItem.
 * Use `create(BatchActionEventItemSchema)` to create a new message.
 */
export declare const BatchActionEventItemSchema: GenMessage<BatchActionEventItem>;
/**
 * @generated from message BatchActionEvent
 */
export type BatchActionEvent = Message<"BatchActionEvent"> & {
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
    /**
     * the id of the job (shared across all items in the batch)
     *
     * @generated from field: string job_id = 2;
     */
    jobId: string;
    /**
     * the action id (shared across all items in the batch)
     *
     * @generated from field: string action_id = 3;
     */
    actionId: string;
    /**
     * (optional) the batch identifier, for observability
     *
     * @generated from field: optional string batch_id = 4;
     */
    batchId?: string | undefined;
    /**
     * @generated from field: google.protobuf.Timestamp event_timestamp = 5;
     */
    eventTimestamp?: Timestamp | undefined;
    /**
     * the task event type -- applies uniformly to every item in this batch
     *
     * @generated from field: StepActionEventType event_type = 6;
     */
    eventType: StepActionEventType;
    /**
     * the items in this batch
     *
     * @generated from field: repeated BatchActionEventItem items = 7;
     */
    items: BatchActionEventItem[];
};
/**
 * Describes the message BatchActionEvent.
 * Use `create(BatchActionEventSchema)` to create a new message.
 */
export declare const BatchActionEventSchema: GenMessage<BatchActionEvent>;
/**
 * @generated from message ActionEventResponse
 */
export type ActionEventResponse = Message<"ActionEventResponse"> & {
    /**
     * the tenant id
     *
     * @generated from field: string tenant_id = 1;
     */
    tenantId: string;
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 2;
     */
    workerId: string;
};
/**
 * Describes the message ActionEventResponse.
 * Use `create(ActionEventResponseSchema)` to create a new message.
 */
export declare const ActionEventResponseSchema: GenMessage<ActionEventResponse>;
/**
 * @generated from message SubscribeToWorkflowEventsRequest
 */
export type SubscribeToWorkflowEventsRequest = Message<"SubscribeToWorkflowEventsRequest"> & {
    /**
     * the id of the workflow run
     *
     * @generated from field: optional string workflow_run_id = 1;
     */
    workflowRunId?: string | undefined;
    /**
     * the key of the additional meta field to subscribe to
     *
     * @generated from field: optional string additional_meta_key = 2;
     */
    additionalMetaKey?: string | undefined;
    /**
     * the value of the additional meta field to subscribe to
     *
     * @generated from field: optional string additional_meta_value = 3;
     */
    additionalMetaValue?: string | undefined;
};
/**
 * Describes the message SubscribeToWorkflowEventsRequest.
 * Use `create(SubscribeToWorkflowEventsRequestSchema)` to create a new message.
 */
export declare const SubscribeToWorkflowEventsRequestSchema: GenMessage<SubscribeToWorkflowEventsRequest>;
/**
 * @generated from message SubscribeToWorkflowRunsRequest
 */
export type SubscribeToWorkflowRunsRequest = Message<"SubscribeToWorkflowRunsRequest"> & {
    /**
     * the id of the workflow run
     *
     * @generated from field: string workflow_run_id = 1;
     */
    workflowRunId: string;
};
/**
 * Describes the message SubscribeToWorkflowRunsRequest.
 * Use `create(SubscribeToWorkflowRunsRequestSchema)` to create a new message.
 */
export declare const SubscribeToWorkflowRunsRequestSchema: GenMessage<SubscribeToWorkflowRunsRequest>;
/**
 * @generated from message WorkflowEvent
 */
export type WorkflowEvent = Message<"WorkflowEvent"> & {
    /**
     * the id of the workflow run
     *
     * @generated from field: string workflow_run_id = 1;
     */
    workflowRunId: string;
    /**
     * @generated from field: ResourceType resource_type = 2;
     */
    resourceType: ResourceType;
    /**
     * @generated from field: ResourceEventType event_type = 3;
     */
    eventType: ResourceEventType;
    /**
     * @generated from field: string resource_id = 4;
     */
    resourceId: string;
    /**
     * @generated from field: google.protobuf.Timestamp event_timestamp = 5;
     */
    eventTimestamp?: Timestamp | undefined;
    /**
     * the event payload
     *
     * @generated from field: string event_payload = 6;
     */
    eventPayload: string;
    /**
     * whether this is the last event for the workflow run - server
     * will hang up the connection but clients might want to case
     *
     * @generated from field: bool hangup = 7;
     */
    hangup: boolean;
    /**
     * (optional) the max number of retries this task can handle
     *
     * @generated from field: optional int32 task_retries = 8;
     */
    taskRetries?: number | undefined;
    /**
     * (optional) the retry count of this task
     *
     * @generated from field: optional int32 retry_count = 9;
     */
    retryCount?: number | undefined;
    /**
     * @generated from field: optional int64 event_index = 10;
     */
    eventIndex?: bigint | undefined;
};
/**
 * Describes the message WorkflowEvent.
 * Use `create(WorkflowEventSchema)` to create a new message.
 */
export declare const WorkflowEventSchema: GenMessage<WorkflowEvent>;
/**
 * @generated from message WorkflowRunEvent
 */
export type WorkflowRunEvent = Message<"WorkflowRunEvent"> & {
    /**
     * the id of the workflow run
     *
     * @generated from field: string workflow_run_id = 1;
     */
    workflowRunId: string;
    /**
     * @generated from field: WorkflowRunEventType event_type = 2;
     */
    eventType: WorkflowRunEventType;
    /**
     * @generated from field: google.protobuf.Timestamp event_timestamp = 3;
     */
    eventTimestamp?: Timestamp | undefined;
    /**
     * @generated from field: repeated StepRunResult results = 4;
     */
    results: StepRunResult[];
};
/**
 * Describes the message WorkflowRunEvent.
 * Use `create(WorkflowRunEventSchema)` to create a new message.
 */
export declare const WorkflowRunEventSchema: GenMessage<WorkflowRunEvent>;
/**
 * @generated from message StepRunResult
 */
export type StepRunResult = Message<"StepRunResult"> & {
    /**
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
    /**
     * @generated from field: string task_name = 2;
     */
    taskName: string;
    /**
     * @generated from field: string job_run_id = 3;
     */
    jobRunId: string;
    /**
     * @generated from field: optional string error = 4;
     */
    error?: string | undefined;
    /**
     * @generated from field: optional string output = 5;
     */
    output?: string | undefined;
};
/**
 * Describes the message StepRunResult.
 * Use `create(StepRunResultSchema)` to create a new message.
 */
export declare const StepRunResultSchema: GenMessage<StepRunResult>;
/**
 * @generated from message OverridesData
 */
export type OverridesData = Message<"OverridesData"> & {
    /**
     * the task run id
     *
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
    /**
     * the path of the data to set
     *
     * @generated from field: string path = 2;
     */
    path: string;
    /**
     * the value to set
     *
     * @generated from field: string value = 3;
     */
    value: string;
    /**
     * the filename of the caller
     *
     * @generated from field: string caller_filename = 4;
     */
    callerFilename: string;
};
/**
 * Describes the message OverridesData.
 * Use `create(OverridesDataSchema)` to create a new message.
 */
export declare const OverridesDataSchema: GenMessage<OverridesData>;
/**
 * @generated from message OverridesDataResponse
 */
export type OverridesDataResponse = Message<"OverridesDataResponse"> & {};
/**
 * Describes the message OverridesDataResponse.
 * Use `create(OverridesDataResponseSchema)` to create a new message.
 */
export declare const OverridesDataResponseSchema: GenMessage<OverridesDataResponse>;
/**
 * @generated from message HeartbeatRequest
 */
export type HeartbeatRequest = Message<"HeartbeatRequest"> & {
    /**
     * the id of the worker
     *
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
    /**
     * heartbeatAt is the time the worker sent the heartbeat
     *
     * @generated from field: google.protobuf.Timestamp heartbeat_at = 2;
     */
    heartbeatAt?: Timestamp | undefined;
};
/**
 * Describes the message HeartbeatRequest.
 * Use `create(HeartbeatRequestSchema)` to create a new message.
 */
export declare const HeartbeatRequestSchema: GenMessage<HeartbeatRequest>;
/**
 * @generated from message HeartbeatResponse
 */
export type HeartbeatResponse = Message<"HeartbeatResponse"> & {};
/**
 * Describes the message HeartbeatResponse.
 * Use `create(HeartbeatResponseSchema)` to create a new message.
 */
export declare const HeartbeatResponseSchema: GenMessage<HeartbeatResponse>;
/**
 * @generated from message RefreshTimeoutRequest
 */
export type RefreshTimeoutRequest = Message<"RefreshTimeoutRequest"> & {
    /**
     * the id of the task run to release
     *
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
    /**
     * @generated from field: string increment_timeout_by = 2;
     */
    incrementTimeoutBy: string;
};
/**
 * Describes the message RefreshTimeoutRequest.
 * Use `create(RefreshTimeoutRequestSchema)` to create a new message.
 */
export declare const RefreshTimeoutRequestSchema: GenMessage<RefreshTimeoutRequest>;
/**
 * @generated from message RefreshTimeoutResponse
 */
export type RefreshTimeoutResponse = Message<"RefreshTimeoutResponse"> & {
    /**
     * @generated from field: google.protobuf.Timestamp timeout_at = 1;
     */
    timeoutAt?: Timestamp | undefined;
};
/**
 * Describes the message RefreshTimeoutResponse.
 * Use `create(RefreshTimeoutResponseSchema)` to create a new message.
 */
export declare const RefreshTimeoutResponseSchema: GenMessage<RefreshTimeoutResponse>;
/**
 * @generated from message ReleaseSlotRequest
 */
export type ReleaseSlotRequest = Message<"ReleaseSlotRequest"> & {
    /**
     * the id of the task run to release
     *
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
};
/**
 * Describes the message ReleaseSlotRequest.
 * Use `create(ReleaseSlotRequestSchema)` to create a new message.
 */
export declare const ReleaseSlotRequestSchema: GenMessage<ReleaseSlotRequest>;
/**
 * @generated from message ReleaseSlotResponse
 */
export type ReleaseSlotResponse = Message<"ReleaseSlotResponse"> & {};
/**
 * Describes the message ReleaseSlotResponse.
 * Use `create(ReleaseSlotResponseSchema)` to create a new message.
 */
export declare const ReleaseSlotResponseSchema: GenMessage<ReleaseSlotResponse>;
/**
 * @generated from message RestoreEvictedTaskRequest
 */
export type RestoreEvictedTaskRequest = Message<"RestoreEvictedTaskRequest"> & {
    /**
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
};
/**
 * Describes the message RestoreEvictedTaskRequest.
 * Use `create(RestoreEvictedTaskRequestSchema)` to create a new message.
 */
export declare const RestoreEvictedTaskRequestSchema: GenMessage<RestoreEvictedTaskRequest>;
/**
 * @generated from message RestoreEvictedTaskResponse
 */
export type RestoreEvictedTaskResponse = Message<"RestoreEvictedTaskResponse"> & {
    /**
     * @generated from field: bool requeued = 1;
     */
    requeued: boolean;
};
/**
 * Describes the message RestoreEvictedTaskResponse.
 * Use `create(RestoreEvictedTaskResponseSchema)` to create a new message.
 */
export declare const RestoreEvictedTaskResponseSchema: GenMessage<RestoreEvictedTaskResponse>;
/**
 * @generated from message GetVersionRequest
 */
export type GetVersionRequest = Message<"GetVersionRequest"> & {};
/**
 * Describes the message GetVersionRequest.
 * Use `create(GetVersionRequestSchema)` to create a new message.
 */
export declare const GetVersionRequestSchema: GenMessage<GetVersionRequest>;
/**
 * @generated from message GetVersionResponse
 */
export type GetVersionResponse = Message<"GetVersionResponse"> & {
    /**
     * @generated from field: string version = 1;
     */
    version: string;
};
/**
 * Describes the message GetVersionResponse.
 * Use `create(GetVersionResponseSchema)` to create a new message.
 */
export declare const GetVersionResponseSchema: GenMessage<GetVersionResponse>;
/**
 * @generated from enum SDKS
 */
export declare enum SDKS {
    /**
     * @generated from enum value: UNKNOWN = 0;
     */
    UNKNOWN = 0,
    /**
     * @generated from enum value: GO = 1;
     */
    GO = 1,
    /**
     * @generated from enum value: PYTHON = 2;
     */
    PYTHON = 2,
    /**
     * @generated from enum value: TYPESCRIPT = 3;
     */
    TYPESCRIPT = 3,
    /**
     * @generated from enum value: RUBY = 4;
     */
    RUBY = 4
}
/**
 * Describes the enum SDKS.
 */
export declare const SDKSSchema: GenEnum<SDKS>;
/**
 * @generated from enum ActionType
 */
export declare enum ActionType {
    /**
     * @generated from enum value: START_STEP_RUN = 0;
     */
    START_STEP_RUN = 0,
    /**
     * @generated from enum value: CANCEL_STEP_RUN = 1;
     */
    CANCEL_STEP_RUN = 1,
    /**
     * @generated from enum value: START_GET_GROUP_KEY = 2;
     */
    START_GET_GROUP_KEY = 2,
    /**
     * @generated from enum value: START_BATCH = 3;
     */
    START_BATCH = 3
}
/**
 * Describes the enum ActionType.
 */
export declare const ActionTypeSchema: GenEnum<ActionType>;
/**
 * @generated from enum GroupKeyActionEventType
 */
export declare enum GroupKeyActionEventType {
    /**
     * @generated from enum value: GROUP_KEY_EVENT_TYPE_UNKNOWN = 0;
     */
    GROUP_KEY_EVENT_TYPE_UNKNOWN = 0,
    /**
     * @generated from enum value: GROUP_KEY_EVENT_TYPE_STARTED = 1;
     */
    GROUP_KEY_EVENT_TYPE_STARTED = 1,
    /**
     * @generated from enum value: GROUP_KEY_EVENT_TYPE_COMPLETED = 2;
     */
    GROUP_KEY_EVENT_TYPE_COMPLETED = 2,
    /**
     * @generated from enum value: GROUP_KEY_EVENT_TYPE_FAILED = 3;
     */
    GROUP_KEY_EVENT_TYPE_FAILED = 3
}
/**
 * Describes the enum GroupKeyActionEventType.
 */
export declare const GroupKeyActionEventTypeSchema: GenEnum<GroupKeyActionEventType>;
/**
 * @generated from enum StepActionEventType
 */
export declare enum StepActionEventType {
    /**
     * @generated from enum value: STEP_EVENT_TYPE_UNKNOWN = 0;
     */
    STEP_EVENT_TYPE_UNKNOWN = 0,
    /**
     * @generated from enum value: STEP_EVENT_TYPE_STARTED = 1;
     */
    STEP_EVENT_TYPE_STARTED = 1,
    /**
     * @generated from enum value: STEP_EVENT_TYPE_COMPLETED = 2;
     */
    STEP_EVENT_TYPE_COMPLETED = 2,
    /**
     * @generated from enum value: STEP_EVENT_TYPE_FAILED = 3;
     */
    STEP_EVENT_TYPE_FAILED = 3,
    /**
     * @generated from enum value: STEP_EVENT_TYPE_ACKNOWLEDGED = 4;
     */
    STEP_EVENT_TYPE_ACKNOWLEDGED = 4,
    /**
     * @generated from enum value: STEP_EVENT_TYPE_CANCELLED = 5;
     */
    STEP_EVENT_TYPE_CANCELLED = 5
}
/**
 * Describes the enum StepActionEventType.
 */
export declare const StepActionEventTypeSchema: GenEnum<StepActionEventType>;
/**
 * @generated from enum ResourceType
 */
export declare enum ResourceType {
    /**
     * @generated from enum value: RESOURCE_TYPE_UNKNOWN = 0;
     */
    UNKNOWN = 0,
    /**
     * @generated from enum value: RESOURCE_TYPE_STEP_RUN = 1;
     */
    STEP_RUN = 1,
    /**
     * @generated from enum value: RESOURCE_TYPE_WORKFLOW_RUN = 2;
     */
    WORKFLOW_RUN = 2
}
/**
 * Describes the enum ResourceType.
 */
export declare const ResourceTypeSchema: GenEnum<ResourceType>;
/**
 * @generated from enum ResourceEventType
 */
export declare enum ResourceEventType {
    /**
     * @generated from enum value: RESOURCE_EVENT_TYPE_UNKNOWN = 0;
     */
    UNKNOWN = 0,
    /**
     * @generated from enum value: RESOURCE_EVENT_TYPE_STARTED = 1;
     */
    STARTED = 1,
    /**
     * @generated from enum value: RESOURCE_EVENT_TYPE_COMPLETED = 2;
     */
    COMPLETED = 2,
    /**
     * @generated from enum value: RESOURCE_EVENT_TYPE_FAILED = 3;
     */
    FAILED = 3,
    /**
     * @generated from enum value: RESOURCE_EVENT_TYPE_CANCELLED = 4;
     */
    CANCELLED = 4,
    /**
     * @generated from enum value: RESOURCE_EVENT_TYPE_TIMED_OUT = 5;
     */
    TIMED_OUT = 5,
    /**
     * @generated from enum value: RESOURCE_EVENT_TYPE_STREAM = 6;
     */
    STREAM = 6
}
/**
 * Describes the enum ResourceEventType.
 */
export declare const ResourceEventTypeSchema: GenEnum<ResourceEventType>;
/**
 * @generated from enum WorkflowRunEventType
 */
export declare enum WorkflowRunEventType {
    /**
     * @generated from enum value: WORKFLOW_RUN_EVENT_TYPE_FINISHED = 0;
     */
    FINISHED = 0
}
/**
 * Describes the enum WorkflowRunEventType.
 */
export declare const WorkflowRunEventTypeSchema: GenEnum<WorkflowRunEventType>;
/**
 * @generated from service Dispatcher
 */
export declare const Dispatcher: GenService<{
    /**
     * @generated from rpc Dispatcher.Register
     */
    register: {
        methodKind: "unary";
        input: typeof WorkerRegisterRequestSchema;
        output: typeof WorkerRegisterResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.Listen
     */
    listen: {
        methodKind: "server_streaming";
        input: typeof WorkerListenRequestSchema;
        output: typeof AssignedActionSchema;
    };
    /**
     * ListenV2 is like listen, but implementation does not include heartbeats. This should only used by SDKs
     * against engine version v0.18.1+
     *
     * @generated from rpc Dispatcher.ListenV2
     */
    listenV2: {
        methodKind: "server_streaming";
        input: typeof WorkerListenRequestSchema;
        output: typeof AssignedActionSchema;
    };
    /**
     * Heartbeat is a method for workers to send heartbeats to the dispatcher
     *
     * @generated from rpc Dispatcher.Heartbeat
     */
    heartbeat: {
        methodKind: "unary";
        input: typeof HeartbeatRequestSchema;
        output: typeof HeartbeatResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.SubscribeToWorkflowEvents
     */
    subscribeToWorkflowEvents: {
        methodKind: "server_streaming";
        input: typeof SubscribeToWorkflowEventsRequestSchema;
        output: typeof WorkflowEventSchema;
    };
    /**
     * @generated from rpc Dispatcher.SubscribeToWorkflowRuns
     */
    subscribeToWorkflowRuns: {
        methodKind: "bidi_streaming";
        input: typeof SubscribeToWorkflowRunsRequestSchema;
        output: typeof WorkflowRunEventSchema;
    };
    /**
     * @generated from rpc Dispatcher.SendStepActionEvent
     */
    sendStepActionEvent: {
        methodKind: "unary";
        input: typeof StepActionEventSchema;
        output: typeof ActionEventResponseSchema;
    };
    /**
     * SendBatchActionEvent reports a single lifecycle event (STARTED, FAILED, or CANCELLED)
     * for every task in a batch in one call, instead of one SendStepActionEvent call per task.
     *
     * @generated from rpc Dispatcher.SendBatchActionEvent
     */
    sendBatchActionEvent: {
        methodKind: "unary";
        input: typeof BatchActionEventSchema;
        output: typeof ActionEventResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.SendGroupKeyActionEvent
     */
    sendGroupKeyActionEvent: {
        methodKind: "unary";
        input: typeof GroupKeyActionEventSchema;
        output: typeof ActionEventResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.PutOverridesData
     */
    putOverridesData: {
        methodKind: "unary";
        input: typeof OverridesDataSchema;
        output: typeof OverridesDataResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.Unsubscribe
     */
    unsubscribe: {
        methodKind: "unary";
        input: typeof WorkerUnsubscribeRequestSchema;
        output: typeof WorkerUnsubscribeResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.RefreshTimeout
     */
    refreshTimeout: {
        methodKind: "unary";
        input: typeof RefreshTimeoutRequestSchema;
        output: typeof RefreshTimeoutResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.ReleaseSlot
     */
    releaseSlot: {
        methodKind: "unary";
        input: typeof ReleaseSlotRequestSchema;
        output: typeof ReleaseSlotResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.RestoreEvictedTask
     */
    restoreEvictedTask: {
        methodKind: "unary";
        input: typeof RestoreEvictedTaskRequestSchema;
        output: typeof RestoreEvictedTaskResponseSchema;
    };
    /**
     * @generated from rpc Dispatcher.UpsertWorkerLabels
     */
    upsertWorkerLabels: {
        methodKind: "unary";
        input: typeof UpsertWorkerLabelsRequestSchema;
        output: typeof UpsertWorkerLabelsResponseSchema;
    };
    /**
     * GetVersion returns the dispatcher protocol version as a simple integer.
     * SDKs use this to determine feature support (e.g. slot_config registration).
     * Old engines that do not implement this RPC will return UNIMPLEMENTED.
     *
     * @generated from rpc Dispatcher.GetVersion
     */
    getVersion: {
        methodKind: "unary";
        input: typeof GetVersionRequestSchema;
        output: typeof GetVersionResponseSchema;
    };
}>;
