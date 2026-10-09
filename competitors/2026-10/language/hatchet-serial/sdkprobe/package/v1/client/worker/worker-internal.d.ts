import { ActionListener } from '../../../clients/dispatcher/action-listener';
import { Action, ActionKey } from '../../../clients/dispatcher/action';
import { StepActionEvent, StepActionEventType, GroupKeyActionEvent, GroupKeyActionEventType, BatchActionEvent } from '../../../protoc/dispatcher';
import HatchetPromise from '../../../util/hatchet-promise/hatchet-promise';
import { Logger } from '../../../util/logger';
import { BaseWorkflowDeclaration, WorkflowDefinition, HatchetClient } from '../..';
import { WorkerLabels } from '../../../clients/dispatcher/dispatcher-client';
import { Context } from './context';
import { SlotConfig } from '../../slot-types';
import { DurableEvictionManager } from './eviction/eviction-manager';
import { EvictionPolicy } from './eviction/eviction-policy';
export { assertValidConcurrencyArr, mapBatchConfigPb, mapConcurrencyPb, mapRateLimitPb, mapSlotRequestsPb, resolveExecutionTimeout, resolveScheduleTimeout, taskConcurrencyArr, } from './workflow-proto';
export type ActionRegistry = Record<Action['actionId'], Function>;
export interface WorkerOpts {
    name: string;
    handleKill?: boolean;
    slots?: number;
    durableSlots?: number;
    labels?: WorkerLabels;
    healthPort?: number;
    enableHealthServer?: boolean;
}
export declare class InternalWorker {
    client: HatchetClient;
    name: string;
    workerId: string | undefined;
    killing: boolean;
    handle_kill: boolean;
    action_registry: ActionRegistry;
    durable_action_set: Set<string>;
    eviction_policies: Map<string, EvictionPolicy | undefined>;
    evictionManager: DurableEvictionManager | undefined;
    workflow_registry: Array<WorkflowDefinition>;
    listener: ActionListener | undefined;
    futures: Record<ActionKey, HatchetPromise<any>>;
    contexts: Record<ActionKey, Context<any, any>>;
    slots?: number;
    durableSlots?: number;
    slotConfig: SlotConfig;
    engineVersion: string | undefined;
    logger: Logger;
    registeredWorkflowPromises: Array<Promise<any>>;
    labels: WorkerLabels;
    healthPort: number;
    enableHealthServer: boolean;
    private healthServer;
    private status;
    private shutdownPromise;
    constructor(client: HatchetClient, options: {
        name: string;
        handleKill?: boolean;
        slots?: number;
        durableSlots?: number;
        slotConfig?: SlotConfig;
        labels?: WorkerLabels;
    });
    private initializeHealthServer;
    private getAvailableSlots;
    private getRegisteredActions;
    private getFilteredLabels;
    private setStatus;
    registerDurableActions(workflow: WorkflowDefinition): void;
    private registerActions;
    registerWorkflow(initWorkflow: BaseWorkflowDeclaration<any, any>, durable?: boolean): Promise<void>;
    private ensureEvictionManager;
    private cleanupRun;
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchHandleStartStepRun.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    handleStartStepRun(action: Action): Promise<Error | undefined>;
    getStepActionEvent(action: Action, eventType: StepActionEventType, shouldNotRetry: boolean, payload?: any, retryCount?: number): StepActionEvent;
    /**
     * Handles a START_BATCH action: invokes the registered batch task handler once with
     * every buffered item, then reports completion/failure back per-member (or, for
     * broadcast batch tasks, the same result for every member) via sendBatchActionEvent.
     */
    handleStartBatch(action: Action): Promise<Error | undefined>;
    /**
     * Reports the result of a successful batch handler invocation. result is expected to be
     * a Record<string, any> keyed by batch member id; each member's output is serialized
     * independently so that one member's serialization failure does not fail the whole
     * batch.
     */
    private sendBatchCompleted;
    /** Fails every member of the batch uniformly, e.g. when the handler itself throws. */
    private sendBatchFailureForAll;
    getBatchActionEvent(action: Action, eventType: StepActionEventType, items: {
        taskRunExternalId: string;
        eventPayload: string;
    }[]): BatchActionEvent;
    getGroupKeyActionEvent(action: Action, eventType: GroupKeyActionEventType, payload?: any): GroupKeyActionEvent;
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchHandleCancelStepRun.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    handleCancelStepRun(action: Action): Promise<void>;
    stop(): Promise<void>;
    exitGracefully(handleKill: boolean): Promise<void>;
    private shutdown;
    /**
     * Creates an action listener by registering the worker with the dispatcher.
     * Override in subclasses to change registration behavior (e.g. legacy engines).
     */
    protected createListener(): Promise<ActionListener>;
    start(): Promise<void>;
    handleAction(action: Action): Promise<void | Error>;
    upsertLabels(labels: WorkerLabels): Promise<WorkerLabels>;
}
