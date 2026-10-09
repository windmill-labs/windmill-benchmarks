import type { GenEnum, GenFile, GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { DurableEventListenerConditions } from "./shared/condition_pb";
import type { TriggerWorkflowRequest } from "./shared/trigger_pb";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file v1/dispatcher.proto.
 */
export declare const file_v1_dispatcher: GenFile;
/**
 * @generated from message v1.DurableTaskRequestRegisterWorker
 */
export type DurableTaskRequestRegisterWorker = Message<"v1.DurableTaskRequestRegisterWorker"> & {
    /**
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
};
/**
 * Describes the message v1.DurableTaskRequestRegisterWorker.
 * Use `create(DurableTaskRequestRegisterWorkerSchema)` to create a new message.
 */
export declare const DurableTaskRequestRegisterWorkerSchema: GenMessage<DurableTaskRequestRegisterWorker>;
/**
 * @generated from message v1.DurableTaskResponseRegisterWorker
 */
export type DurableTaskResponseRegisterWorker = Message<"v1.DurableTaskResponseRegisterWorker"> & {
    /**
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
};
/**
 * Describes the message v1.DurableTaskResponseRegisterWorker.
 * Use `create(DurableTaskResponseRegisterWorkerSchema)` to create a new message.
 */
export declare const DurableTaskResponseRegisterWorkerSchema: GenMessage<DurableTaskResponseRegisterWorker>;
/**
 * @generated from message v1.DurableEventLogEntryRef
 */
export type DurableEventLogEntryRef = Message<"v1.DurableEventLogEntryRef"> & {
    /**
     * @generated from field: string durable_task_external_id = 1;
     */
    durableTaskExternalId: string;
    /**
     * @generated from field: int32 invocation_count = 2;
     */
    invocationCount: number;
    /**
     * @generated from field: int64 branch_id = 3;
     */
    branchId: bigint;
    /**
     * @generated from field: int64 node_id = 4;
     */
    nodeId: bigint;
};
/**
 * Describes the message v1.DurableEventLogEntryRef.
 * Use `create(DurableEventLogEntryRefSchema)` to create a new message.
 */
export declare const DurableEventLogEntryRefSchema: GenMessage<DurableEventLogEntryRef>;
/**
 * @generated from message v1.DurableTaskRunAckEntry
 */
export type DurableTaskRunAckEntry = Message<"v1.DurableTaskRunAckEntry"> & {
    /**
     * @generated from field: int64 node_id = 1;
     */
    nodeId: bigint;
    /**
     * @generated from field: int64 branch_id = 2;
     */
    branchId: bigint;
    /**
     * @generated from field: string workflow_run_external_id = 3;
     */
    workflowRunExternalId: string;
};
/**
 * Describes the message v1.DurableTaskRunAckEntry.
 * Use `create(DurableTaskRunAckEntrySchema)` to create a new message.
 */
export declare const DurableTaskRunAckEntrySchema: GenMessage<DurableTaskRunAckEntry>;
/**
 * @generated from message v1.DurableTaskEventMemoAckResponse
 */
export type DurableTaskEventMemoAckResponse = Message<"v1.DurableTaskEventMemoAckResponse"> & {
    /**
     * @generated from field: v1.DurableEventLogEntryRef ref = 1;
     */
    ref?: DurableEventLogEntryRef | undefined;
    /**
     * @generated from field: bool memo_already_existed = 2;
     */
    memoAlreadyExisted: boolean;
    /**
     * @generated from field: optional bytes memo_result_payload = 3;
     */
    memoResultPayload?: Uint8Array | undefined;
};
/**
 * Describes the message v1.DurableTaskEventMemoAckResponse.
 * Use `create(DurableTaskEventMemoAckResponseSchema)` to create a new message.
 */
export declare const DurableTaskEventMemoAckResponseSchema: GenMessage<DurableTaskEventMemoAckResponse>;
/**
 * @generated from message v1.DurableTaskEventTriggerRunsAckResponse
 */
export type DurableTaskEventTriggerRunsAckResponse = Message<"v1.DurableTaskEventTriggerRunsAckResponse"> & {
    /**
     * @generated from field: string durable_task_external_id = 1;
     */
    durableTaskExternalId: string;
    /**
     * @generated from field: int32 invocation_count = 2;
     */
    invocationCount: number;
    /**
     * @generated from field: repeated v1.DurableTaskRunAckEntry run_entries = 3;
     */
    runEntries: DurableTaskRunAckEntry[];
};
/**
 * Describes the message v1.DurableTaskEventTriggerRunsAckResponse.
 * Use `create(DurableTaskEventTriggerRunsAckResponseSchema)` to create a new message.
 */
export declare const DurableTaskEventTriggerRunsAckResponseSchema: GenMessage<DurableTaskEventTriggerRunsAckResponse>;
/**
 * @generated from message v1.DurableTaskEventWaitForAckResponse
 */
export type DurableTaskEventWaitForAckResponse = Message<"v1.DurableTaskEventWaitForAckResponse"> & {
    /**
     * @generated from field: v1.DurableEventLogEntryRef ref = 1;
     */
    ref?: DurableEventLogEntryRef | undefined;
};
/**
 * Describes the message v1.DurableTaskEventWaitForAckResponse.
 * Use `create(DurableTaskEventWaitForAckResponseSchema)` to create a new message.
 */
export declare const DurableTaskEventWaitForAckResponseSchema: GenMessage<DurableTaskEventWaitForAckResponse>;
/**
 * @generated from message v1.DurableTaskEventLogEntryCompletedResponse
 */
export type DurableTaskEventLogEntryCompletedResponse = Message<"v1.DurableTaskEventLogEntryCompletedResponse"> & {
    /**
     * @generated from field: v1.DurableEventLogEntryRef ref = 1;
     */
    ref?: DurableEventLogEntryRef | undefined;
    /**
     * @generated from field: bytes payload = 2;
     */
    payload: Uint8Array;
    /**
     * @generated from field: bool is_failure = 3;
     */
    isFailure: boolean;
    /**
     * @generated from field: optional string error_message = 4;
     */
    errorMessage?: string | undefined;
};
/**
 * Describes the message v1.DurableTaskEventLogEntryCompletedResponse.
 * Use `create(DurableTaskEventLogEntryCompletedResponseSchema)` to create a new message.
 */
export declare const DurableTaskEventLogEntryCompletedResponseSchema: GenMessage<DurableTaskEventLogEntryCompletedResponse>;
/**
 * @generated from message v1.DurableTaskEvictInvocationRequest
 */
export type DurableTaskEvictInvocationRequest = Message<"v1.DurableTaskEvictInvocationRequest"> & {
    /**
     * @generated from field: int32 invocation_count = 1;
     */
    invocationCount: number;
    /**
     * @generated from field: string durable_task_external_id = 2;
     */
    durableTaskExternalId: string;
    /**
     * @generated from field: optional string reason = 3;
     */
    reason?: string | undefined;
};
/**
 * Describes the message v1.DurableTaskEvictInvocationRequest.
 * Use `create(DurableTaskEvictInvocationRequestSchema)` to create a new message.
 */
export declare const DurableTaskEvictInvocationRequestSchema: GenMessage<DurableTaskEvictInvocationRequest>;
/**
 * Sent by the server after recording eviction for an evict_invocation request.
 *
 * @generated from message v1.DurableTaskEvictionAckResponse
 */
export type DurableTaskEvictionAckResponse = Message<"v1.DurableTaskEvictionAckResponse"> & {
    /**
     * @generated from field: int32 invocation_count = 1;
     */
    invocationCount: number;
    /**
     * @generated from field: string durable_task_external_id = 2;
     */
    durableTaskExternalId: string;
};
/**
 * Describes the message v1.DurableTaskEvictionAckResponse.
 * Use `create(DurableTaskEvictionAckResponseSchema)` to create a new message.
 */
export declare const DurableTaskEvictionAckResponseSchema: GenMessage<DurableTaskEvictionAckResponse>;
/**
 * @generated from message v1.DurableTaskAwaitedCompletedEntry
 */
export type DurableTaskAwaitedCompletedEntry = Message<"v1.DurableTaskAwaitedCompletedEntry"> & {
    /**
     * @generated from field: string durable_task_external_id = 1;
     */
    durableTaskExternalId: string;
    /**
     * @generated from field: int64 branch_id = 2;
     */
    branchId: bigint;
    /**
     * @generated from field: int64 node_id = 3;
     */
    nodeId: bigint;
    /**
     * @generated from field: int32 invocation_count = 4;
     */
    invocationCount: number;
};
/**
 * Describes the message v1.DurableTaskAwaitedCompletedEntry.
 * Use `create(DurableTaskAwaitedCompletedEntrySchema)` to create a new message.
 */
export declare const DurableTaskAwaitedCompletedEntrySchema: GenMessage<DurableTaskAwaitedCompletedEntry>;
/**
 * Sent by the server to notify a worker that its invocation is stale and should be cancelled.
 *
 * @generated from message v1.DurableTaskServerEvictNotice
 */
export type DurableTaskServerEvictNotice = Message<"v1.DurableTaskServerEvictNotice"> & {
    /**
     * @generated from field: string durable_task_external_id = 1;
     */
    durableTaskExternalId: string;
    /**
     * @generated from field: int32 invocation_count = 2;
     */
    invocationCount: number;
    /**
     * @generated from field: string reason = 3;
     */
    reason: string;
};
/**
 * Describes the message v1.DurableTaskServerEvictNotice.
 * Use `create(DurableTaskServerEvictNoticeSchema)` to create a new message.
 */
export declare const DurableTaskServerEvictNoticeSchema: GenMessage<DurableTaskServerEvictNotice>;
/**
 * @generated from message v1.DurableTaskWorkerStatusRequest
 */
export type DurableTaskWorkerStatusRequest = Message<"v1.DurableTaskWorkerStatusRequest"> & {
    /**
     * @generated from field: string worker_id = 1;
     */
    workerId: string;
    /**
     * @generated from field: repeated v1.DurableTaskAwaitedCompletedEntry waiting_entries = 2;
     */
    waitingEntries: DurableTaskAwaitedCompletedEntry[];
};
/**
 * Describes the message v1.DurableTaskWorkerStatusRequest.
 * Use `create(DurableTaskWorkerStatusRequestSchema)` to create a new message.
 */
export declare const DurableTaskWorkerStatusRequestSchema: GenMessage<DurableTaskWorkerStatusRequest>;
/**
 * @generated from message v1.DurableTaskCompleteMemoRequest
 */
export type DurableTaskCompleteMemoRequest = Message<"v1.DurableTaskCompleteMemoRequest"> & {
    /**
     * @generated from field: v1.DurableEventLogEntryRef ref = 1;
     */
    ref?: DurableEventLogEntryRef | undefined;
    /**
     * @generated from field: bytes payload = 2;
     */
    payload: Uint8Array;
    /**
     * @generated from field: bytes memo_key = 3;
     */
    memoKey: Uint8Array;
};
/**
 * Describes the message v1.DurableTaskCompleteMemoRequest.
 * Use `create(DurableTaskCompleteMemoRequestSchema)` to create a new message.
 */
export declare const DurableTaskCompleteMemoRequestSchema: GenMessage<DurableTaskCompleteMemoRequest>;
/**
 * @generated from message v1.DurableTaskMemoRequest
 */
export type DurableTaskMemoRequest = Message<"v1.DurableTaskMemoRequest"> & {
    /**
     * The invocation_count is a monotonically increasing count that uniquely identifies an "attempt"
     * at running a durable task. Each time the task is started, it gets a new invocation count (which has)
     * incremented by one since the previous invocation. This allows the server (and the worker) to have a way of
     * differentiating between different attempts of the same task running in different places, to prevent race conditions
     * and other problems from duplication. It also allows for older invocations to be evicted cleanly
     *
     * @generated from field: int32 invocation_count = 1;
     */
    invocationCount: number;
    /**
     * @generated from field: string durable_task_external_id = 2;
     */
    durableTaskExternalId: string;
    /**
     * @generated from field: bytes key = 3;
     */
    key: Uint8Array;
    /**
     * optional payload because we can send a memo request to check if a memo already exists
     *
     * @generated from field: optional bytes payload = 4;
     */
    payload?: Uint8Array | undefined;
};
/**
 * Describes the message v1.DurableTaskMemoRequest.
 * Use `create(DurableTaskMemoRequestSchema)` to create a new message.
 */
export declare const DurableTaskMemoRequestSchema: GenMessage<DurableTaskMemoRequest>;
/**
 * @generated from message v1.DurableTaskTriggerRunsRequest
 */
export type DurableTaskTriggerRunsRequest = Message<"v1.DurableTaskTriggerRunsRequest"> & {
    /**
     * The invocation_count is a monotonically increasing count that uniquely identifies an "attempt"
     * at running a durable task. Each time the task is started, it gets a new invocation count (which has)
     * incremented by one since the previous invocation. This allows the server (and the worker) to have a way of
     * differentiating between different attempts of the same task running in different places, to prevent race conditions
     * and other problems from duplication. It also allows for older invocations to be evicted cleanly
     *
     * @generated from field: int32 invocation_count = 1;
     */
    invocationCount: number;
    /**
     * @generated from field: string durable_task_external_id = 2;
     */
    durableTaskExternalId: string;
    /**
     * @generated from field: repeated v1.TriggerWorkflowRequest trigger_opts = 3;
     */
    triggerOpts: TriggerWorkflowRequest[];
};
/**
 * Describes the message v1.DurableTaskTriggerRunsRequest.
 * Use `create(DurableTaskTriggerRunsRequestSchema)` to create a new message.
 */
export declare const DurableTaskTriggerRunsRequestSchema: GenMessage<DurableTaskTriggerRunsRequest>;
/**
 * @generated from message v1.DurableTaskWaitForRequest
 */
export type DurableTaskWaitForRequest = Message<"v1.DurableTaskWaitForRequest"> & {
    /**
     * The invocation_count is a monotonically increasing count that uniquely identifies an "attempt"
     * at running a durable task. Each time the task is started, it gets a new invocation count (which has)
     * incremented by one since the previous invocation. This allows the server (and the worker) to have a way of
     * differentiating between different attempts of the same task running in different places, to prevent race conditions
     * and other problems from duplication. It also allows for older invocations to be evicted cleanly
     *
     * @generated from field: int32 invocation_count = 1;
     */
    invocationCount: number;
    /**
     * @generated from field: string durable_task_external_id = 2;
     */
    durableTaskExternalId: string;
    /**
     * Fields for DURABLE_TASK_TRIGGER_KIND_WAIT_FOR
     *
     * @generated from field: optional v1.DurableEventListenerConditions wait_for_conditions = 3;
     */
    waitForConditions?: DurableEventListenerConditions | undefined;
    /**
     * An optional human-readable label for this wait, displayed in the dashboard.
     * Example: "Waiting for payment confirmation"
     *
     * @generated from field: optional string label = 4;
     */
    label?: string | undefined;
};
/**
 * Describes the message v1.DurableTaskWaitForRequest.
 * Use `create(DurableTaskWaitForRequestSchema)` to create a new message.
 */
export declare const DurableTaskWaitForRequestSchema: GenMessage<DurableTaskWaitForRequest>;
/**
 * @generated from message v1.DurableTaskRequest
 */
export type DurableTaskRequest = Message<"v1.DurableTaskRequest"> & {
    /**
     * @generated from oneof v1.DurableTaskRequest.message
     */
    message: {
        /**
         * @generated from field: v1.DurableTaskRequestRegisterWorker register_worker = 1;
         */
        value: DurableTaskRequestRegisterWorker;
        case: "registerWorker";
    } | {
        /**
         * @generated from field: v1.DurableTaskMemoRequest memo = 2;
         */
        value: DurableTaskMemoRequest;
        case: "memo";
    } | {
        /**
         * @generated from field: v1.DurableTaskTriggerRunsRequest trigger_runs = 3;
         */
        value: DurableTaskTriggerRunsRequest;
        case: "triggerRuns";
    } | {
        /**
         * @generated from field: v1.DurableTaskWaitForRequest wait_for = 4;
         */
        value: DurableTaskWaitForRequest;
        case: "waitFor";
    } | {
        /**
         * @generated from field: v1.DurableTaskEvictInvocationRequest evict_invocation = 5;
         */
        value: DurableTaskEvictInvocationRequest;
        case: "evictInvocation";
    } | {
        /**
         * @generated from field: v1.DurableTaskWorkerStatusRequest worker_status = 6;
         */
        value: DurableTaskWorkerStatusRequest;
        case: "workerStatus";
    } | {
        /**
         * @generated from field: v1.DurableTaskCompleteMemoRequest complete_memo = 7;
         */
        value: DurableTaskCompleteMemoRequest;
        case: "completeMemo";
    } | {
        case: undefined;
        value?: undefined;
    };
};
/**
 * Describes the message v1.DurableTaskRequest.
 * Use `create(DurableTaskRequestSchema)` to create a new message.
 */
export declare const DurableTaskRequestSchema: GenMessage<DurableTaskRequest>;
/**
 * @generated from message v1.DurableTaskErrorResponse
 */
export type DurableTaskErrorResponse = Message<"v1.DurableTaskErrorResponse"> & {
    /**
     * @generated from field: v1.DurableEventLogEntryRef ref = 1;
     */
    ref?: DurableEventLogEntryRef | undefined;
    /**
     * @generated from field: v1.DurableTaskErrorType error_type = 2;
     */
    errorType: DurableTaskErrorType;
    /**
     * @generated from field: string error_message = 3;
     */
    errorMessage: string;
};
/**
 * Describes the message v1.DurableTaskErrorResponse.
 * Use `create(DurableTaskErrorResponseSchema)` to create a new message.
 */
export declare const DurableTaskErrorResponseSchema: GenMessage<DurableTaskErrorResponse>;
/**
 * @generated from message v1.DurableTaskResponse
 */
export type DurableTaskResponse = Message<"v1.DurableTaskResponse"> & {
    /**
     * @generated from oneof v1.DurableTaskResponse.message
     */
    message: {
        /**
         * @generated from field: v1.DurableTaskResponseRegisterWorker register_worker = 1;
         */
        value: DurableTaskResponseRegisterWorker;
        case: "registerWorker";
    } | {
        /**
         * @generated from field: v1.DurableTaskEventMemoAckResponse memo_ack = 2;
         */
        value: DurableTaskEventMemoAckResponse;
        case: "memoAck";
    } | {
        /**
         * @generated from field: v1.DurableTaskEventTriggerRunsAckResponse trigger_runs_ack = 3;
         */
        value: DurableTaskEventTriggerRunsAckResponse;
        case: "triggerRunsAck";
    } | {
        /**
         * @generated from field: v1.DurableTaskEventWaitForAckResponse wait_for_ack = 4;
         */
        value: DurableTaskEventWaitForAckResponse;
        case: "waitForAck";
    } | {
        /**
         * @generated from field: v1.DurableTaskEventLogEntryCompletedResponse entry_completed = 5;
         */
        value: DurableTaskEventLogEntryCompletedResponse;
        case: "entryCompleted";
    } | {
        /**
         * @generated from field: v1.DurableTaskErrorResponse error = 6;
         */
        value: DurableTaskErrorResponse;
        case: "error";
    } | {
        /**
         * @generated from field: v1.DurableTaskEvictionAckResponse eviction_ack = 7;
         */
        value: DurableTaskEvictionAckResponse;
        case: "evictionAck";
    } | {
        /**
         * @generated from field: v1.DurableTaskServerEvictNotice server_evict = 8;
         */
        value: DurableTaskServerEvictNotice;
        case: "serverEvict";
    } | {
        case: undefined;
        value?: undefined;
    };
};
/**
 * Describes the message v1.DurableTaskResponse.
 * Use `create(DurableTaskResponseSchema)` to create a new message.
 */
export declare const DurableTaskResponseSchema: GenMessage<DurableTaskResponse>;
/**
 * @generated from message v1.RegisterDurableEventRequest
 */
export type RegisterDurableEventRequest = Message<"v1.RegisterDurableEventRequest"> & {
    /**
     * external uuid for the task run
     *
     * @generated from field: string task_id = 1;
     */
    taskId: string;
    /**
     * the signal key for the event
     *
     * @generated from field: string signal_key = 2;
     */
    signalKey: string;
    /**
     * the task conditions for creating the task
     *
     * @generated from field: v1.DurableEventListenerConditions conditions = 3;
     */
    conditions?: DurableEventListenerConditions | undefined;
};
/**
 * Describes the message v1.RegisterDurableEventRequest.
 * Use `create(RegisterDurableEventRequestSchema)` to create a new message.
 */
export declare const RegisterDurableEventRequestSchema: GenMessage<RegisterDurableEventRequest>;
/**
 * @generated from message v1.RegisterDurableEventResponse
 */
export type RegisterDurableEventResponse = Message<"v1.RegisterDurableEventResponse"> & {};
/**
 * Describes the message v1.RegisterDurableEventResponse.
 * Use `create(RegisterDurableEventResponseSchema)` to create a new message.
 */
export declare const RegisterDurableEventResponseSchema: GenMessage<RegisterDurableEventResponse>;
/**
 * @generated from message v1.ListenForDurableEventRequest
 */
export type ListenForDurableEventRequest = Message<"v1.ListenForDurableEventRequest"> & {
    /**
     * single listener per worker
     *
     * @generated from field: string task_id = 1;
     */
    taskId: string;
    /**
     * the match id for the listener
     *
     * @generated from field: string signal_key = 2;
     */
    signalKey: string;
};
/**
 * Describes the message v1.ListenForDurableEventRequest.
 * Use `create(ListenForDurableEventRequestSchema)` to create a new message.
 */
export declare const ListenForDurableEventRequestSchema: GenMessage<ListenForDurableEventRequest>;
/**
 * @generated from message v1.DurableEvent
 */
export type DurableEvent = Message<"v1.DurableEvent"> & {
    /**
     * @generated from field: string task_id = 1;
     */
    taskId: string;
    /**
     * @generated from field: string signal_key = 2;
     */
    signalKey: string;
    /**
     * the data for the event
     *
     * @generated from field: bytes data = 3;
     */
    data: Uint8Array;
};
/**
 * Describes the message v1.DurableEvent.
 * Use `create(DurableEventSchema)` to create a new message.
 */
export declare const DurableEventSchema: GenMessage<DurableEvent>;
/**
 * @generated from enum v1.DurableTaskErrorType
 */
export declare enum DurableTaskErrorType {
    /**
     * @generated from enum value: DURABLE_TASK_ERROR_TYPE_UNSPECIFIED = 0;
     */
    UNSPECIFIED = 0,
    /**
     * @generated from enum value: DURABLE_TASK_ERROR_TYPE_NONDETERMINISM = 1;
     */
    NONDETERMINISM = 1
}
/**
 * Describes the enum v1.DurableTaskErrorType.
 */
export declare const DurableTaskErrorTypeSchema: GenEnum<DurableTaskErrorType>;
/**
 * @generated from service v1.V1Dispatcher
 */
export declare const V1Dispatcher: GenService<{
    /**
     * @generated from rpc v1.V1Dispatcher.DurableTask
     */
    durableTask: {
        methodKind: "bidi_streaming";
        input: typeof DurableTaskRequestSchema;
        output: typeof DurableTaskResponseSchema;
    };
    /**
     * NOTE: deprecated after DurableEventLog is implemented
     *
     * @generated from rpc v1.V1Dispatcher.RegisterDurableEvent
     */
    registerDurableEvent: {
        methodKind: "unary";
        input: typeof RegisterDurableEventRequestSchema;
        output: typeof RegisterDurableEventResponseSchema;
    };
    /**
     * @generated from rpc v1.V1Dispatcher.ListenForDurableEvent
     */
    listenForDurableEvent: {
        methodKind: "bidi_streaming";
        input: typeof ListenForDurableEventRequestSchema;
        output: typeof DurableEventSchema;
    };
}>;
