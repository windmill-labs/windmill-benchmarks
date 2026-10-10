import { Channel, ClientFactory } from 'nice-grpc';
import { CreateWorkflowVersionOpts, RateLimitDuration, WorkflowServiceClient } from '../../protoc/workflows';
import { ClientConfig } from '../hatchet-client/client-config';
import { Logger } from '../../util/logger';
import WorkflowRunRef from '../../util/workflow-run-ref';
import { AdminServiceClient, CreateWorkflowVersionRequest } from '../../protoc/v1/workflows';
import { Priority, RunsClient, WorkerLabelComparator } from '../../v1';
import { type Transport } from '../transport';
import { Api } from '../rest';
import { WebhookWorkerCreateRequest, WorkflowRunStatus, WorkflowRunStatusList } from '../rest/generated/data-contracts';
import { RunListenerClient } from '../listeners/run-listener/child-listener-client';
type DesiredWorkerLabelOpt = {
    value: string | number;
    required?: boolean;
    weight?: number;
    comparator?: WorkerLabelComparator;
};
type WorkflowMetricsQuery = {
    workflowId?: string;
    workflowName?: string;
    status?: WorkflowRunStatus;
    groupKey?: string;
};
export type WorkflowRun<T = object> = {
    workflowName: string;
    input: T;
    options?: {
        parentId?: string | undefined;
        /**
         * (optional) the parent task external run id.
         *
         * This is the field understood by the workflows gRPC API.
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
    client: WorkflowServiceClient;
    v1Client: AdminServiceClient;
    api: Api;
    tenantId: string;
    logger: Logger;
    listenerClient: RunListenerClient;
    workflows: RunsClient | undefined;
    /**
     * The positional `channel` and `factory` parameters are part of the public constructor
     * signature; the unary calls go over `transport` and the streaming clients own the channel.
     */
    constructor(config: ClientConfig, _channel: Channel, _factory: ClientFactory, api: Api, tenantId: string, listenerClient: RunListenerClient, workflows: RunsClient | undefined, transport?: Transport);
    /**
     * @deprecated use putWorkflow instead
     */
    put_workflow(opts: CreateWorkflowVersionOpts): Promise<import("../../protoc/workflows").WorkflowVersion>;
    /**
     * Creates a new workflow or updates an existing workflow. If the workflow already exists, Hatchet will automatically
     * determine if the workflow definition has changed and create a new version if necessary.
     * @param workflow a workflow definition to create
     */
    putWorkflow(workflow: CreateWorkflowVersionOpts): Promise<import("../../protoc/workflows").WorkflowVersion>;
    /**
     * Creates a new workflow or updates an existing workflow. If the workflow already exists, Hatchet will automatically
     * determine if the workflow definition has changed and create a new version if necessary.
     * @param workflow a workflow definition to create
     */
    putWorkflowV1(workflow: CreateWorkflowVersionRequest): Promise<import("../../protoc/v1/workflows").CreateWorkflowVersionResponse>;
    /**
     * @deprecated use hatchet.ratelimits.upsert instead
     */
    put_rate_limit(key: string, limit: number, duration: RateLimitDuration): Promise<void>;
    /**
     * @deprecated use hatchet.ratelimits.upsert instead
     */
    putRateLimit(key: string, limit: number, duration?: RateLimitDuration): Promise<void>;
    registerWebhook(data: WebhookWorkerCreateRequest): Promise<import("axios").AxiosResponse<import("../rest/generated/data-contracts").WebhookWorkerCreated, any, {}>>;
    /**
     * @deprecated use runWorkflow instead
     */
    run_workflow<T = object>(workflowName: string, input: T, options?: {
        parentId?: string | undefined;
        /**
         * (optional) the parent task external run id.
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
    }): Promise<WorkflowRunRef<object>>;
    /**
     * Run a new instance of a workflow with the given input. This will create a new workflow run and return the ID of the
     * new run.
     * @param workflowName the name of the workflow to run
     * @param input an object containing the input to the workflow
     * @param options an object containing the options to run the workflow
     * @returns the ID of the new workflow run
     */
    runWorkflow<Q = object, P = object>(workflowName: string, input: Q, options?: {
        parentId?: string | undefined;
        /**
         * (optional) the parent task external run id.
         *
         * This is the field understood by the workflows gRPC API.
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
        desiredWorkerId?: string | undefined;
        priority?: Priority;
        desiredWorkerLabels?: Record<string, DesiredWorkerLabelOpt>;
    }): WorkflowRunRef<P>;
    /**
     * Run multiple workflows runs with the given input and options. This will create new workflow runs and return their IDs.
     * Order is preserved in the response.
     * @param workflowRuns an array of objects containing the workflow name, input, and options for each workflow run
     * @returns an array of workflow run references
     */
    runWorkflows<Q = object, P = object>(workflowRuns: Array<{
        workflowName: string;
        input: Q;
        options?: {
            parentId?: string | undefined;
            /**
             * (optional) the parent task external run id.
             *
             * This is the field understood by the workflows gRPC API.
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
            desiredWorkerId?: string | undefined;
            priority?: Priority;
            desiredWorkerLabels?: Record<string, DesiredWorkerLabelOpt>;
        };
    }>): Promise<WorkflowRunRef<P>[]>;
    /**
     * @deprecated use listWorkflows instead
     */
    list_workflows(): Promise<import("../rest/generated/data-contracts").WorkflowList>;
    /**
     * List workflows in the tenant associated with the API token.
     * @returns a list of all workflows in the tenant
     */
    listWorkflows(): Promise<import("../rest/generated/data-contracts").WorkflowList>;
    /**
     * @deprecated use getWorkflow instead
     */
    get_workflow(workflowId: string): Promise<import("../rest/generated/data-contracts").Workflow>;
    /**
     * Get a workflow by its ID.
     * @param workflowId the workflow ID (**note:** this is not the same as the workflow version id)
     * @returns
     */
    getWorkflow(workflowId: string): Promise<import("../rest/generated/data-contracts").Workflow>;
    /**
     * @deprecated use getWorkflowVersion instead
     */
    get_workflow_version(workflowId: string, version?: string): Promise<import("../rest/generated/data-contracts").WorkflowVersion>;
    /**
     * Get a workflow version.
     * @param workflowId the workflow ID
     * @param version the version of the workflow to get. If not provided, the latest version will be returned.
     * @returns the workflow version
     */
    getWorkflowVersion(workflowId: string, version?: string): Promise<import("../rest/generated/data-contracts").WorkflowVersion>;
    /**
     * @deprecated use getWorkflowRun instead
     */
    get_workflow_run(workflowRunId: string): Promise<WorkflowRunRef<unknown>>;
    /**
     * Get a workflow run.
     * @param workflowRunId the id of the workflow run to get
     * @returns the workflow run
     */
    getWorkflowRun(workflowRunId: string): Promise<WorkflowRunRef<unknown>>;
    /**
     * @deprecated use listWorkflowRuns instead
     */
    list_workflow_runs(query: {
        offset?: number | undefined;
        limit?: number | undefined;
        eventId?: string | undefined;
        workflowId?: string | undefined;
        parentWorkflowRunId?: string | undefined;
        parentStepRunId?: string | undefined;
        statuses?: WorkflowRunStatusList | undefined;
        additionalMetadata?: string[] | undefined;
    }): Promise<import("../rest/generated/data-contracts").WorkflowRunList>;
    /**
     * List workflow runs in the tenant associated with the API token.
     * @param query the query to filter the list of workflow runs
     * @returns
     */
    listWorkflowRuns(query: {
        offset?: number | undefined;
        limit?: number | undefined;
        eventId?: string | undefined;
        workflowId?: string | undefined;
        parentWorkflowRunId?: string | undefined;
        parentStepRunId?: string | undefined;
        statuses?: WorkflowRunStatusList | undefined;
        additionalMetadata?: string[] | undefined;
    }): Promise<import("../rest/generated/data-contracts").WorkflowRunList>;
    /**
     * @deprecated use scheduleWorkflow instead
     */
    schedule_workflow(name: string, options?: {
        schedules?: Date[];
        input?: object;
    }): Promise<void>;
    /**
     * Schedule a workflow to run at a specific time or times.
     * @param name the name of the workflow to schedule
     * @param options an object containing the schedules to set
     * @param input an object containing the input to the workflow
     * @deprecated use hatchet.schedules.create instead
     */
    scheduleWorkflow(name: string, options?: {
        schedules?: Date[];
        input?: object;
    }): void;
    /**
     * @deprecated use getWorkflowMetrics instead
     */
    get_workflow_metrics(data: WorkflowMetricsQuery): Promise<import("axios").AxiosResponse<import("../rest/generated/data-contracts").WorkflowMetrics, any, {}>>;
    /**
     * Get the metrics for a workflow.
     *
     * @param workflowId the ID of the workflow to get metrics for
     * @param workflowName the name of the workflow to get metrics for
     * @param query an object containing query parameters to filter the metrics
     */
    getWorkflowMetrics({ workflowId, workflowName, status, groupKey }: WorkflowMetricsQuery): Promise<import("axios").AxiosResponse<import("../rest/generated/data-contracts").WorkflowMetrics, any, {}>>;
}
export {};
