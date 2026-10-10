import { DispatcherClient as PbDispatcherClient, AssignedAction } from '../../protoc/dispatcher';
import { ClientConfig } from '../hatchet-client/client-config';
import { Logger } from '../../util/logger';
import { DispatcherClient } from './dispatcher-client';
import { Heartbeat } from './heartbeat/heartbeat-controller';
declare enum ListenStrategy {
    LISTEN_STRATEGY_V1 = 1,
    LISTEN_STRATEGY_V2 = 2
}
export { createAction, createActionId, workflowNameFromAction } from './action';
export type { Action, ActionKey } from './action';
export declare class ActionListener {
    config: ClientConfig;
    client: PbDispatcherClient;
    workerId: string;
    logger: Logger;
    lastConnectionAttempt: number;
    retries: number;
    retryInterval: number;
    retryCount: number;
    done: boolean;
    listenStrategy: ListenStrategy;
    heartbeat: Heartbeat;
    abortController?: AbortController;
    constructor(client: DispatcherClient, workerId: string, retryInterval?: number, retryCount?: number);
    actions: () => AsyncGenerator<import("./action").Action, void, unknown>;
    setListenStrategy(strategy: ListenStrategy): Promise<void>;
    getListenStrategy(): Promise<ListenStrategy>;
    incrementRetries(): Promise<void>;
    getListenClient(): Promise<AsyncIterable<AssignedAction>>;
    stopStream(): void;
    unregister(): Promise<import("../../protoc/dispatcher").WorkerUnsubscribeResponse>;
}
