import { EventEmitter } from 'events';
import { WorkflowRunEvent, SubscribeToWorkflowRunsRequest } from '../../../protoc/dispatcher';
import { RunListenerClient } from './child-listener-client';
export declare class Streamable {
    listener: AsyncIterable<WorkflowRunEvent>;
    id: string;
    onCleanup: () => void;
    private cleanedUp;
    responseEmitter: EventEmitter<[never]>;
    constructor(listener: AsyncIterable<WorkflowRunEvent>, id: string, onCleanup: () => void);
    private cleanupOnce;
    get(opts?: {
        signal?: AbortSignal;
    }): Promise<WorkflowRunEvent>;
    stream(opts?: {
        signal?: AbortSignal;
    }): AsyncGenerator<WorkflowRunEvent, void, unknown>;
}
export declare class RunGrpcPooledListener {
    listener: AsyncIterable<WorkflowRunEvent> | undefined;
    requestEmitter: EventEmitter<[never]>;
    signal: AbortController;
    client: RunListenerClient;
    subscribers: Record<string, Streamable>;
    onFinish: () => void;
    constructor(client: RunListenerClient, onFinish: () => void);
    private init;
    subscribe(request: SubscribeToWorkflowRunsRequest): Streamable;
    replayRequests(): void;
    private request;
}
