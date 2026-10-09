import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import type { CallContext, CallOptions } from 'nice-grpc-common';
import { DurableEventListenerConditions } from './shared/condition';
import { TriggerWorkflowRequest } from './shared/trigger';
export declare const protobufPackage = "v1";
export declare enum DurableTaskErrorType {
    DURABLE_TASK_ERROR_TYPE_UNSPECIFIED = 0,
    DURABLE_TASK_ERROR_TYPE_NONDETERMINISM = 1,
    UNRECOGNIZED = -1
}
export declare function durableTaskErrorTypeFromJSON(object: any): DurableTaskErrorType;
export declare function durableTaskErrorTypeToJSON(object: DurableTaskErrorType): string;
export interface DurableTaskRequestRegisterWorker {
    workerId: string;
}
export interface DurableTaskResponseRegisterWorker {
    workerId: string;
}
export interface DurableEventLogEntryRef {
    durableTaskExternalId: string;
    invocationCount: number;
    branchId: number;
    nodeId: number;
}
export interface DurableTaskRunAckEntry {
    nodeId: number;
    branchId: number;
    workflowRunExternalId: string;
}
export interface DurableTaskEventMemoAckResponse {
    ref: DurableEventLogEntryRef | undefined;
    memoAlreadyExisted: boolean;
    memoResultPayload?: Uint8Array | undefined;
}
export interface DurableTaskEventTriggerRunsAckResponse {
    durableTaskExternalId: string;
    invocationCount: number;
    runEntries: DurableTaskRunAckEntry[];
}
export interface DurableTaskEventWaitForAckResponse {
    ref: DurableEventLogEntryRef | undefined;
}
export interface DurableTaskEventLogEntryCompletedResponse {
    ref: DurableEventLogEntryRef | undefined;
    payload: Uint8Array;
    isFailure: boolean;
    errorMessage?: string | undefined;
}
export interface DurableTaskEvictInvocationRequest {
    invocationCount: number;
    durableTaskExternalId: string;
    reason?: string | undefined;
}
/** Sent by the server after recording eviction for an evict_invocation request. */
export interface DurableTaskEvictionAckResponse {
    invocationCount: number;
    durableTaskExternalId: string;
}
export interface DurableTaskAwaitedCompletedEntry {
    durableTaskExternalId: string;
    branchId: number;
    nodeId: number;
    invocationCount: number;
}
/** Sent by the server to notify a worker that its invocation is stale and should be cancelled. */
export interface DurableTaskServerEvictNotice {
    durableTaskExternalId: string;
    invocationCount: number;
    reason: string;
}
export interface DurableTaskWorkerStatusRequest {
    workerId: string;
    waitingEntries: DurableTaskAwaitedCompletedEntry[];
}
export interface DurableTaskCompleteMemoRequest {
    ref: DurableEventLogEntryRef | undefined;
    payload: Uint8Array;
    memoKey: Uint8Array;
}
export interface DurableTaskMemoRequest {
    /**
     * The invocation_count is a monotonically increasing count that uniquely identifies an "attempt"
     * at running a durable task. Each time the task is started, it gets a new invocation count (which has)
     * incremented by one since the previous invocation. This allows the server (and the worker) to have a way of
     * differentiating between different attempts of the same task running in different places, to prevent race conditions
     * and other problems from duplication. It also allows for older invocations to be evicted cleanly
     */
    invocationCount: number;
    durableTaskExternalId: string;
    key: Uint8Array;
    /** optional payload because we can send a memo request to check if a memo already exists */
    payload?: Uint8Array | undefined;
}
export interface DurableTaskTriggerRunsRequest {
    /**
     * The invocation_count is a monotonically increasing count that uniquely identifies an "attempt"
     * at running a durable task. Each time the task is started, it gets a new invocation count (which has)
     * incremented by one since the previous invocation. This allows the server (and the worker) to have a way of
     * differentiating between different attempts of the same task running in different places, to prevent race conditions
     * and other problems from duplication. It also allows for older invocations to be evicted cleanly
     */
    invocationCount: number;
    durableTaskExternalId: string;
    triggerOpts: TriggerWorkflowRequest[];
}
export interface DurableTaskWaitForRequest {
    /**
     * The invocation_count is a monotonically increasing count that uniquely identifies an "attempt"
     * at running a durable task. Each time the task is started, it gets a new invocation count (which has)
     * incremented by one since the previous invocation. This allows the server (and the worker) to have a way of
     * differentiating between different attempts of the same task running in different places, to prevent race conditions
     * and other problems from duplication. It also allows for older invocations to be evicted cleanly
     */
    invocationCount: number;
    durableTaskExternalId: string;
    /** Fields for DURABLE_TASK_TRIGGER_KIND_WAIT_FOR */
    waitForConditions?: DurableEventListenerConditions | undefined;
    /**
     * An optional human-readable label for this wait, displayed in the dashboard.
     * Example: "Waiting for payment confirmation"
     */
    label?: string | undefined;
}
export interface DurableTaskRequest {
    registerWorker?: DurableTaskRequestRegisterWorker | undefined;
    memo?: DurableTaskMemoRequest | undefined;
    triggerRuns?: DurableTaskTriggerRunsRequest | undefined;
    waitFor?: DurableTaskWaitForRequest | undefined;
    evictInvocation?: DurableTaskEvictInvocationRequest | undefined;
    workerStatus?: DurableTaskWorkerStatusRequest | undefined;
    completeMemo?: DurableTaskCompleteMemoRequest | undefined;
}
export interface DurableTaskErrorResponse {
    ref: DurableEventLogEntryRef | undefined;
    errorType: DurableTaskErrorType;
    errorMessage: string;
}
export interface DurableTaskResponse {
    registerWorker?: DurableTaskResponseRegisterWorker | undefined;
    memoAck?: DurableTaskEventMemoAckResponse | undefined;
    triggerRunsAck?: DurableTaskEventTriggerRunsAckResponse | undefined;
    waitForAck?: DurableTaskEventWaitForAckResponse | undefined;
    entryCompleted?: DurableTaskEventLogEntryCompletedResponse | undefined;
    error?: DurableTaskErrorResponse | undefined;
    evictionAck?: DurableTaskEvictionAckResponse | undefined;
    serverEvict?: DurableTaskServerEvictNotice | undefined;
}
export interface RegisterDurableEventRequest {
    /** external uuid for the task run */
    taskId: string;
    /** the signal key for the event */
    signalKey: string;
    /** the task conditions for creating the task */
    conditions: DurableEventListenerConditions | undefined;
}
export interface RegisterDurableEventResponse {
}
export interface ListenForDurableEventRequest {
    /** single listener per worker */
    taskId: string;
    /** the match id for the listener */
    signalKey: string;
}
export interface DurableEvent {
    taskId: string;
    signalKey: string;
    /** the data for the event */
    data: Uint8Array;
}
export declare const DurableTaskRequestRegisterWorker: MessageFns<DurableTaskRequestRegisterWorker>;
export declare const DurableTaskResponseRegisterWorker: MessageFns<DurableTaskResponseRegisterWorker>;
export declare const DurableEventLogEntryRef: MessageFns<DurableEventLogEntryRef>;
export declare const DurableTaskRunAckEntry: MessageFns<DurableTaskRunAckEntry>;
export declare const DurableTaskEventMemoAckResponse: MessageFns<DurableTaskEventMemoAckResponse>;
export declare const DurableTaskEventTriggerRunsAckResponse: MessageFns<DurableTaskEventTriggerRunsAckResponse>;
export declare const DurableTaskEventWaitForAckResponse: MessageFns<DurableTaskEventWaitForAckResponse>;
export declare const DurableTaskEventLogEntryCompletedResponse: MessageFns<DurableTaskEventLogEntryCompletedResponse>;
export declare const DurableTaskEvictInvocationRequest: MessageFns<DurableTaskEvictInvocationRequest>;
export declare const DurableTaskEvictionAckResponse: MessageFns<DurableTaskEvictionAckResponse>;
export declare const DurableTaskAwaitedCompletedEntry: MessageFns<DurableTaskAwaitedCompletedEntry>;
export declare const DurableTaskServerEvictNotice: MessageFns<DurableTaskServerEvictNotice>;
export declare const DurableTaskWorkerStatusRequest: MessageFns<DurableTaskWorkerStatusRequest>;
export declare const DurableTaskCompleteMemoRequest: MessageFns<DurableTaskCompleteMemoRequest>;
export declare const DurableTaskMemoRequest: MessageFns<DurableTaskMemoRequest>;
export declare const DurableTaskTriggerRunsRequest: MessageFns<DurableTaskTriggerRunsRequest>;
export declare const DurableTaskWaitForRequest: MessageFns<DurableTaskWaitForRequest>;
export declare const DurableTaskRequest: MessageFns<DurableTaskRequest>;
export declare const DurableTaskErrorResponse: MessageFns<DurableTaskErrorResponse>;
export declare const DurableTaskResponse: MessageFns<DurableTaskResponse>;
export declare const RegisterDurableEventRequest: MessageFns<RegisterDurableEventRequest>;
export declare const RegisterDurableEventResponse: MessageFns<RegisterDurableEventResponse>;
export declare const ListenForDurableEventRequest: MessageFns<ListenForDurableEventRequest>;
export declare const DurableEvent: MessageFns<DurableEvent>;
export type V1DispatcherDefinition = typeof V1DispatcherDefinition;
export declare const V1DispatcherDefinition: {
    readonly name: "V1Dispatcher";
    readonly fullName: "v1.V1Dispatcher";
    readonly methods: {
        readonly durableTask: {
            readonly name: "DurableTask";
            readonly requestType: typeof DurableTaskRequest;
            readonly requestStream: true;
            readonly responseType: typeof DurableTaskResponse;
            readonly responseStream: true;
            readonly options: {};
        };
        /** NOTE: deprecated after DurableEventLog is implemented */
        readonly registerDurableEvent: {
            readonly name: "RegisterDurableEvent";
            readonly requestType: typeof RegisterDurableEventRequest;
            readonly requestStream: false;
            readonly responseType: typeof RegisterDurableEventResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly listenForDurableEvent: {
            readonly name: "ListenForDurableEvent";
            readonly requestType: typeof ListenForDurableEventRequest;
            readonly requestStream: true;
            readonly responseType: typeof DurableEvent;
            readonly responseStream: true;
            readonly options: {};
        };
    };
};
export interface V1DispatcherServiceImplementation<CallContextExt = {}> {
    durableTask(request: AsyncIterable<DurableTaskRequest>, context: CallContext & CallContextExt): ServerStreamingMethodResult<DeepPartial<DurableTaskResponse>>;
    /** NOTE: deprecated after DurableEventLog is implemented */
    registerDurableEvent(request: RegisterDurableEventRequest, context: CallContext & CallContextExt): Promise<DeepPartial<RegisterDurableEventResponse>>;
    listenForDurableEvent(request: AsyncIterable<ListenForDurableEventRequest>, context: CallContext & CallContextExt): ServerStreamingMethodResult<DeepPartial<DurableEvent>>;
}
export interface V1DispatcherClient<CallOptionsExt = {}> {
    durableTask(request: AsyncIterable<DeepPartial<DurableTaskRequest>>, options?: CallOptions & CallOptionsExt): AsyncIterable<DurableTaskResponse>;
    /** NOTE: deprecated after DurableEventLog is implemented */
    registerDurableEvent(request: DeepPartial<RegisterDurableEventRequest>, options?: CallOptions & CallOptionsExt): Promise<RegisterDurableEventResponse>;
    listenForDurableEvent(request: AsyncIterable<DeepPartial<ListenForDurableEventRequest>>, options?: CallOptions & CallOptionsExt): AsyncIterable<DurableEvent>;
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
