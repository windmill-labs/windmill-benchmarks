import { EventEmitter } from 'events';
import { DurableEvent, RegisterDurableEventResponse } from '../../../protoc/v1/dispatcher';
import { SleepMatchCondition, UserEventMatchCondition } from '../../../protoc/v1/shared/condition';
import { DurableListenerClient } from './durable-listener-client';
export declare class DurableEventStreamable {
    listener: AsyncIterable<DurableEvent>;
    taskId: string;
    signalKey: string;
    subscriptionId: string;
    onCleanup: () => void;
    responseEmitter: EventEmitter<[never]>;
    constructor(listener: AsyncIterable<DurableEvent>, taskId: string, signalKey: string, subscriptionId: string, onCleanup: () => void);
    get(opts?: {
        signal?: AbortSignal;
    }): Promise<DurableEvent>;
}
export declare class DurableEventGrpcPooledListener {
    listener: AsyncIterable<DurableEvent> | undefined;
    requestEmitter: EventEmitter<[never]>;
    signal: AbortController;
    client: DurableListenerClient;
    subscribers: Record<string, DurableEventStreamable>;
    taskSignalKeyToSubscriptionIds: Record<string, string[]>;
    onFinish: () => void;
    private subscriptionCounter;
    private currRequester;
    private readonly DEFAULT_INTERRUPT_INTERVAL;
    constructor(client: DurableListenerClient, onFinish: () => void);
    private scheduleInterrupt;
    private init;
    private cleanupSubscription;
    subscribe(request: {
        taskId: string;
        signalKey: string;
    }): DurableEventStreamable;
    result(request: {
        taskId: string;
        signalKey: string;
    }, opts?: {
        signal?: AbortSignal;
    }): Promise<DurableEvent>;
    registerDurableEvent(request: {
        taskId: string;
        signalKey: string;
        sleepConditions: Array<SleepMatchCondition>;
        userEventConditions: Array<UserEventMatchCondition>;
    }): Promise<RegisterDurableEventResponse>;
    replayRequests(): void;
    private request;
}
