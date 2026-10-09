import { ClientConfig } from '../../clients/hatchet-client/client-config';
import WorkflowRunRef from '../../util/workflow-run-ref';
import { RateLimitDuration, RunsClient } from './..';
import { RunListenerClient } from '../../clients/listeners/run-listener/child-listener-client';
import { Api } from '../../clients/rest/generated/Api';
import { WorkflowServiceClient } from '../../protoc/workflows';
import { AdminServiceClient, CreateWorkflowVersionRequest } from '../../protoc/v1/workflows';
import { type Transport } from '../../clients/transport';
import type { TriggerRun, TriggerRunOptions } from '../../core/types';
import { Logger } from '../../util/logger';
export type WorkflowRun<T = object> = {
    workflowName: string;
    input: T;
    options?: {
        parentId?: string | undefined;
        /**
         * (optional) the parent task run external id.
         *
         * This is the field understood by the workflows gRPC API (`parent_task_run_external_id`).
         */
        parentTaskRunExternalId?: string | undefined;
        /**
         * @deprecated Use `parentTaskRunExternalId` instead.
         * Kept for backward compatibility; will be mapped to `parentTaskRunExternalId`.
         */
        parentStepRunId?: string | undefined;
        childIndex?: number | undefined;
        childKey?: string | undefined;
        additionalMetadata?: Record<string, string> | undefined;
    };
};
export declare class AdminClient {
    config: ClientConfig;
    workflowsGrpc: WorkflowServiceClient;
    adminGrpc: AdminServiceClient;
    listenerClient: RunListenerClient;
    runs: RunsClient;
    logger: Logger;
    constructor(config: ClientConfig, api: Api, runs: RunsClient, transport?: Transport);
    /**
     * Creates a new workflow or updates an existing workflow via the v1 admin service.
     * @param workflow a workflow definition to create
     */
    putWorkflow(workflow: CreateWorkflowVersionRequest): Promise<import("../../protoc/v1/workflows").CreateWorkflowVersionResponse>;
    /**
     * Run a new instance of a workflow with the given input. This will create a new workflow run and return the ID of the
     * new run.
     * @param workflowName the name of the workflow to run
     * @param input an object containing the input to the workflow
     * @param options an object containing the options to run the workflow
     * @returns the ID of the new workflow run
     *
     * @important This method is instrumented by HatchetInstrumentor._patchRunWorkflow.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    runWorkflow<Q = object, P = object>(workflowName: string, input: Q, options?: TriggerRunOptions): Promise<WorkflowRunRef<P>>;
    /**
     * Run multiple workflows runs with the given input and options. This will create new workflow runs and return their IDs.
     * Order is preserved in the response.
     * @param workflowRuns an array of objects containing the workflow name, input, and options for each workflow run
     * @returns an array of workflow run references
     *
     * @important This method is instrumented by HatchetInstrumentor._patchRunWorkflows.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    runWorkflows<Q = object, P = object>(workflowRuns: Array<TriggerRun<Q>>, batchSize?: number): Promise<WorkflowRunRef<P>[]>;
    putRateLimit(key: string, limit: number, duration?: RateLimitDuration): Promise<void>;
}
