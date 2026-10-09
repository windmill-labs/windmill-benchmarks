import { Channel, ClientFactory } from 'nice-grpc';
import { EventEmitter } from 'events';
import { DispatcherClient as PbDispatcherClient, DispatcherClient, WorkflowEvent } from '../../../protoc/dispatcher';
import { ClientConfig } from '../../hatchet-client/client-config';
import { Logger } from '../../../util/logger';
import { Api } from '../../rest';
import { RunGrpcPooledListener } from './pooled-child-listener-client';
export declare enum RunEventType {
    STEP_RUN_EVENT_TYPE_STARTED = "STEP_RUN_EVENT_TYPE_STARTED",
    STEP_RUN_EVENT_TYPE_COMPLETED = "STEP_RUN_EVENT_TYPE_COMPLETED",
    STEP_RUN_EVENT_TYPE_FAILED = "STEP_RUN_EVENT_TYPE_FAILED",
    STEP_RUN_EVENT_TYPE_CANCELLED = "STEP_RUN_EVENT_TYPE_CANCELLED",
    STEP_RUN_EVENT_TYPE_TIMED_OUT = "STEP_RUN_EVENT_TYPE_TIMED_OUT",
    STEP_RUN_EVENT_TYPE_STREAM = "STEP_RUN_EVENT_TYPE_STREAM",
    WORKFLOW_RUN_EVENT_TYPE_STARTED = "WORKFLOW_RUN_EVENT_TYPE_STARTED",
    WORKFLOW_RUN_EVENT_TYPE_COMPLETED = "WORKFLOW_RUN_EVENT_TYPE_COMPLETED",
    WORKFLOW_RUN_EVENT_TYPE_FAILED = "WORKFLOW_RUN_EVENT_TYPE_FAILED",
    WORKFLOW_RUN_EVENT_TYPE_CANCELLED = "WORKFLOW_RUN_EVENT_TYPE_CANCELLED",
    WORKFLOW_RUN_EVENT_TYPE_TIMED_OUT = "WORKFLOW_RUN_EVENT_TYPE_TIMED_OUT"
}
export interface StepRunEvent {
    type: RunEventType;
    payload: string;
    resourceId: string;
    workflowRunId: string;
}
export declare class RunEventListener {
    client: DispatcherClient;
    q: Array<StepRunEvent>;
    eventEmitter: EventEmitter<[never]>;
    pollInterval: ReturnType<typeof setInterval> | undefined;
    constructor(client: DispatcherClient);
    static forRunId(workflowRunId: string, client: DispatcherClient): RunEventListener;
    static forAdditionalMeta(key: string, value: string, client: DispatcherClient): RunEventListener;
    emit(event: StepRunEvent): void;
    listenForRunId(workflowRunId: string): Promise<void>;
    listenForAdditionalMeta(key: string, value: string): Promise<void>;
    listenLoop(listenerFactory: () => AsyncIterable<WorkflowEvent>): Promise<void>;
    retrySubscribe(listenerFactory: () => AsyncIterable<WorkflowEvent>): Promise<AsyncIterable<WorkflowEvent>>;
    stream(): AsyncGenerator<StepRunEvent, void, unknown>;
}
export declare class RunListenerClient {
    config: ClientConfig;
    client: PbDispatcherClient;
    logger: Logger;
    api: Api;
    pooledListener: RunGrpcPooledListener | undefined;
    constructor(config: ClientConfig, channel: Channel, factory: ClientFactory, api: Api);
    get(workflowRunId: string): import("./pooled-child-listener-client").Streamable;
    stream(workflowRunId: string): Promise<AsyncGenerator<StepRunEvent, void, unknown>>;
    streamByRunId(workflowRunId: string): Promise<AsyncGenerator<StepRunEvent, void, unknown>>;
    streamByAdditionalMeta(key: string, value: string): Promise<AsyncGenerator<StepRunEvent, void, unknown>>;
}
