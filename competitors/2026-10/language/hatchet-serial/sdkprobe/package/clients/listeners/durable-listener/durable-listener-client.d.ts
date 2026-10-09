import { EventEmitter } from 'events';
import { Channel, ClientFactory } from 'nice-grpc';
import { ClientConfig } from '../../hatchet-client/client-config';
import { Logger } from '../../../util/logger';
import { V1DispatcherClient, DurableEvent, RegisterDurableEventResponse } from '../../../protoc/v1/dispatcher';
import { SleepMatchCondition, UserEventMatchCondition } from '../../../protoc/v1/shared/condition';
import { DurableTaskEventLogEntryResult, DurableTaskEventMemoAck, DurableTaskEventRunAck, DurableTaskEventWaitForAck, MemoEvent, RunChildrenEvent, WaitForEvent } from './durable-events';
export type { DurableTaskEventAck, DurableTaskEventLogEntryResult, DurableTaskEventMemoAck, DurableTaskEventRunAck, DurableTaskEventWaitForAck, DurableTaskRunAckEntryResult, DurableTaskSendEvent, MemoEvent, RunChildrenEvent, WaitForEvent, } from './durable-events';
export declare class DurableListenerClient {
    config: ClientConfig;
    client: V1DispatcherClient;
    logger: Logger;
    private _workerId;
    private _running;
    private _requestQueue;
    private _requestNotify;
    private _pendingEventAcks;
    private _pendingCallbacks;
    private _orderedCompletions;
    private _pendingEvictionAcks;
    private _newlyWaitingKeys;
    private _waitingSince;
    private _workerStatusSendScheduled;
    private _receiveAbort;
    private _statusInterval;
    private _startLock;
    private _consecutiveFailures;
    onServerEvict: ((durableTaskExternalId: string, invocationCount: number) => void) | undefined;
    constructor(config: ClientConfig, channel: Channel, factory: ClientFactory);
    get workerId(): string | undefined;
    start(workerId: string): Promise<void>;
    private _doStart;
    ensureStarted(workerId: string): Promise<void>;
    stop(): Promise<void>;
    private _connect;
    private _streamLoop;
    private _requestIterator;
    private _enqueueRequest;
    private _startStatusPolling;
    private _registerWaitingKey;
    private _scheduleWorkerStatusSend;
    private _enqueueWorkerStatusForNewlyWaitingKeys;
    private _enqueueWorkerStatusForLongPendingWaiters;
    private _enqueueFullWorkerStatus;
    private _enqueueWorkerStatusFor;
    private _failPendingAcks;
    private _failAllPending;
    private _handleResponse;
    sendEvent(durableTaskExternalId: string, invocationCount: number, event: RunChildrenEvent): Promise<DurableTaskEventRunAck>;
    sendEvent(durableTaskExternalId: string, invocationCount: number, event: WaitForEvent): Promise<DurableTaskEventWaitForAck>;
    sendEvent(durableTaskExternalId: string, invocationCount: number, event: MemoEvent): Promise<DurableTaskEventMemoAck>;
    private _sendRequestAndAwaitAck;
    private _sendRunChildrenEvent;
    private _drainsInProgress;
    private _drainRerunRequests;
    private _drainOrderedCompletions;
    private _drainLoop;
    consumeCallbackWithoutBlocking(durableTaskExternalId: string, invocationCount: number, branchId: number, nodeId: number): void;
    waitForCallback(durableTaskExternalId: string, invocationCount: number, branchId: number, nodeId: number, opts?: {
        signal?: AbortSignal;
    }): Promise<DurableTaskEventLogEntryResult>;
    cleanupTaskState(durableTaskExternalId: string, invocationCount: number): void;
    sendEvictInvocation(durableTaskExternalId: string, invocationCount: number, reason?: string): Promise<void>;
    sendMemoCompletedNotification(durableTaskExternalId: string, nodeId: number, branchId: number, invocationCount: number, memoKey: Uint8Array, memoResultPayload?: Uint8Array): Promise<void>;
    /**
     * @deprecated Legacy backward-compat: uses the old unary RegisterDurableEvent RPC.
     */
    registerDurableEvent(request: {
        taskId: string;
        signalKey: string;
        sleepConditions: Array<SleepMatchCondition>;
        userEventConditions: Array<UserEventMatchCondition>;
    }): Promise<RegisterDurableEventResponse>;
    /**
     * @deprecated Legacy backward-compat: uses the old streaming ListenForDurableEvent RPC.
     */
    subscribe(request: {
        taskId: string;
        signalKey: string;
    }): LegacyDurableEventStreamable;
    /**
     * @deprecated Legacy backward-compat: subscribes and waits for a single result.
     */
    result(request: {
        taskId: string;
        signalKey: string;
    }, opts?: {
        signal?: AbortSignal;
    }): Promise<DurableEvent>;
    private _legacyPooledListener;
}
/**
 * @deprecated Legacy support for the old streaming ListenForDurableEvent RPC.
 */
export declare class LegacyDurableEventStreamable {
    responseEmitter: EventEmitter<[never]>;
    private _onCleanup;
    constructor(onCleanup: () => void);
    get(opts?: {
        signal?: AbortSignal;
    }): Promise<DurableEvent>;
}
