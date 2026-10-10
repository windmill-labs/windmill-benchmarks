import { Channel, ClientFactory } from 'nice-grpc';
import { DispatcherClient as PbDispatcherClient, StepActionEvent, BatchActionEvent, GroupKeyActionEvent, OverridesData, DeepPartial, WorkerLabels as PbWorkerAffinityConfig, RuntimeInfo } from '../../protoc/dispatcher';
import { ClientConfig } from '../hatchet-client/client-config';
import { Logger } from '../../util/logger';
import { SlotConfig } from '../../v1/slot-types';
import { ActionListener } from './action-listener';
import type { WorkerLabels } from '../../v1/client/worker/runtime';
export type { WorkerLabels } from '../../v1/client/worker/runtime';
interface GetActionListenerOptions {
    workerName: string;
    services: string[];
    actions: string[];
    slotConfig?: SlotConfig;
    /** @deprecated use slotConfig */
    slots?: number;
    /** @deprecated use slotConfig */
    durableSlots?: number;
    /** @deprecated use slots */
    maxRuns?: number;
    labels: WorkerLabels;
}
type StepActionEventInput = StepActionEvent & {
    /** @deprecated use taskId */
    stepId?: string;
    /** @deprecated use taskRunId */
    stepRunId?: string;
};
export declare class DispatcherClient {
    config: ClientConfig;
    client: PbDispatcherClient;
    logger: Logger;
    constructor(config: ClientConfig, channel: Channel, factory: ClientFactory);
    getRuntimeInfo(): RuntimeInfo;
    getActionListener(options: GetActionListenerOptions): Promise<ActionListener>;
    /**
     * Calls the GetVersion RPC. Returns the engine semantic version string.
     * Throws a gRPC error with code UNIMPLEMENTED on older engines.
     */
    getVersion(): Promise<string>;
    sendStepActionEvent(in_: StepActionEventInput): Promise<import("../../protoc/dispatcher").ActionEventResponse>;
    /**
     * Reports a lifecycle event (STARTED, COMPLETED, or FAILED) covering one or more
     * members of a batch task's execution.
     */
    sendBatchActionEvent(in_: BatchActionEvent): Promise<import("../../protoc/dispatcher").ActionEventResponse>;
    sendGroupKeyActionEvent(in_: GroupKeyActionEvent): Promise<import("../../protoc/dispatcher").ActionEventResponse>;
    putOverridesData(in_: DeepPartial<OverridesData>): Promise<void | import("../../protoc/dispatcher").OverridesDataResponse>;
    refreshTimeout(incrementTimeoutBy: string, taskRunExternalId: string): Promise<import("../../protoc/dispatcher").RefreshTimeoutResponse>;
    upsertWorkerLabels(workerId: string, labels: WorkerLabels): Promise<import("../../protoc/dispatcher").UpsertWorkerLabelsResponse>;
}
export declare function mapLabels(in_: WorkerLabels): Record<string, PbWorkerAffinityConfig>;
