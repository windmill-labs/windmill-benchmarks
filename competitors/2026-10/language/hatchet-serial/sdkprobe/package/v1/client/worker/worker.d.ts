import { WorkerLabels } from '../../../clients/dispatcher/dispatcher-client';
import { BaseWorkflowDeclaration } from '../../declaration';
import type { LegacyWorkflow } from '../../../legacy/legacy-transformer';
import { HatchetClient } from '../..';
import { InternalWorker } from './worker-internal';
import { type WorkerSlotOptions } from './slot-utils';
/**
 * Options for creating a new hatchet worker
 * @interface CreateWorkerOpts
 */
export interface CreateWorkerOpts extends WorkerSlotOptions {
    /** (optional) Worker labels for affinity-based assignment */
    labels?: WorkerLabels;
    /** (optional) Whether to handle kill signals */
    handleKill?: boolean;
}
/**
 * HatchetWorker class for workflow execution runtime
 */
export declare class Worker {
    config: CreateWorkerOpts;
    name: string;
    _v1: HatchetClient;
    /** Internal reference to the underlying V0 worker implementation */
    _internal: InternalWorker;
    /** Set when connected to a legacy engine that needs dual-worker architecture */
    private _legacyWorker;
    /** Tracks all workflows registered after construction (via registerWorkflow/registerWorkflows) */
    private _registeredWorkflows;
    /**
     * Creates a new HatchetWorker instance
     * @param nonDurable - The V0 worker implementation
     */
    constructor(v1: HatchetClient, nonDurable: InternalWorker, config: CreateWorkerOpts, name: string);
    /**
     * Creates and initializes a new HatchetWorker
     * @param v0 - The HatchetClient instance
     * @param options - Worker creation options
     * @returns A new HatchetWorker instance
     */
    static create(v1: HatchetClient, name: string, options: CreateWorkerOpts): Promise<Worker>;
    /**
     * Registers workflows with the worker.
     * Accepts both v1 BaseWorkflowDeclaration and legacy Workflow objects.
     * Legacy workflows are automatically transformed and a deprecation warning is emitted.
     * @param workflows - Array of workflows to register
     * @returns Array of registered workflow promises
     */
    registerWorkflows(workflows?: Array<BaseWorkflowDeclaration<any, any> | LegacyWorkflow>): Promise<void>;
    /**
     * Registers a single workflow with the worker.
     * Accepts both v1 BaseWorkflowDeclaration and legacy Workflow objects.
     * Legacy workflows are automatically transformed and a deprecation warning is emitted.
     * @param workflow - The workflow to register
     * @returns A promise that resolves when the workflow is registered
     * @deprecated use registerWorkflows instead
     */
    registerWorkflow(workflow: BaseWorkflowDeclaration<any, any> | LegacyWorkflow): Promise<void>;
    /**
     * Starts the worker
     * @returns Promise that resolves when the worker is stopped or killed
     */
    start(): Promise<void>;
    private _checkEvictionSupport;
    /**
     * Stops the worker
     * @returns Promise that resolves when the worker stops
     */
    stop(): Promise<void>;
    /**
     * Updates or inserts worker labels
     * @param labels - Worker labels to update
     * @returns Promise that resolves when labels are updated
     */
    upsertLabels(labels: WorkerLabels): Promise<WorkerLabels>;
    /**
     * Get the labels for the worker
     * @returns The labels for the worker
     */
    getLabels(): WorkerLabels;
    isPaused(): Promise<boolean>;
    pause(): Promise<void> | Promise<import("../../../clients/rest/generated/data-contracts").Worker>;
    unpause(): Promise<void> | Promise<import("../../../clients/rest/generated/data-contracts").Worker>;
    /**
     * Waits until the worker has connected and registered with the server.
     * Polls every 200ms. Use after start() to avoid fixed sleeps before running workflows.
     */
    waitUntilReady(timeoutMs?: number): Promise<void>;
}
export { testingExports as __testing } from './slot-utils';
