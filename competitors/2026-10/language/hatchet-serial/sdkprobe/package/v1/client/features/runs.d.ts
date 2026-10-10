import WorkflowRunRef from '../../../util/workflow-run-ref';
import { RunListenerClient } from '../../../clients/listeners/run-listener/child-listener-client';
import { WorkflowsClient } from './workflows';
import { HatchetClient } from '../client';
import { runStatusToJSON } from '../../../core/run-detail';
import type { RunDetail, RunFilterBase, TaskRunDetail } from '../../../core/types';
export type { RunDetail, TaskRunDetail };
export { runStatusToJSON };
export type RunFilter = RunFilterBase & {
    workflowNames?: string[];
};
export type CancelRunOpts = {
    ids?: string[];
    filters?: RunFilter;
};
export type ReplayRunOpts = {
    ids?: string[];
    filters?: RunFilter;
};
export interface ListRunsOpts extends RunFilter {
    /**
     * The number to skip
     * @format int64
     */
    offset?: number;
    /**
     * The number to limit by
     * @format int64
     */
    limit?: number;
    /** A list of statuses to filter by */
    /**
     * The worker id to filter by
     * @format uuid
     * @minLength 36
     * @maxLength 36
     */
    workerId?: string;
    /** Whether to include DAGs or only to include tasks */
    onlyTasks: boolean;
    /**
     * The parent task run external id to filter by
     * @deprecated use parentTaskRunExternalId instead
     * @format uuid
     * @minLength 36
     * @maxLength 36
     */
    parentTaskExternalId?: string;
    /**
     * The parent task run external id to filter by
     * @format uuid
     * @minLength 36
     * @maxLength 36
     */
    parentTaskRunExternalId?: string;
    /**
     * The triggering event external id to filter by
     * @format uuid
     * @minLength 36
     * @maxLength 36
     */
    triggeringEventExternalId?: string;
    /** A flag for whether or not to include the input and output payloads in the response. Defaults to `true` if unset. */
    includePayloads?: boolean;
}
/**
 * The runs client is a client for interacting with task and workflow runs within Hatchet.
 */
export declare class RunsClient {
    api: HatchetClient['api'];
    tenantId: string;
    workflows: WorkflowsClient;
    listener: RunListenerClient;
    private _transport;
    private _adminRpc;
    constructor(client: HatchetClient);
    /** The v1 admin RPCs on the client's Connect transport, the same path the core client uses. */
    private get adminRpc();
    /**
     * Gets a task or workflow run by its ID.
     * @param run - The ID of the run to get.
     * @returns A promise that resolves to the run.
     */
    get<T = any>(run: string | WorkflowRunRef<T>): Promise<import("../../../clients/rest/generated/data-contracts").V1WorkflowRunDetails>;
    /**
     * Gets the status of a task or workflow run by its ID.
     * @param run - The ID of the run to get the status of.
     * @returns A promise that resolves to the status of the run.
     */
    get_status<T = any>(run: string | WorkflowRunRef<T>): Promise<import("../../../clients/rest/generated/data-contracts").V1TaskStatus>;
    /**
     * Gets run details
     * @param run - The workflow run ID (string) or a WorkflowRunRef.
     * @returns A promise resolving to GetRunDetailsResponse with task run statuses and outputs.
     */
    getDetails<T = any>(run: string | WorkflowRunRef<T>): Promise<RunDetail>;
    /**
     * Lists all task and workflow runs for the current tenant.
     * @param opts - The options for the list operation.
     * @returns A promise that resolves to the list of runs.
     */
    list(opts?: Partial<ListRunsOpts>): Promise<import("../../../clients/rest/generated/data-contracts").V1TaskSummaryList>;
    /**
     * Cancels a task or workflow run by its ID.
     * @param opts - The options for the cancel operation.
     * @returns A promise that resolves to the cancelled run.
     */
    cancel(opts: CancelRunOpts): Promise<import("axios").AxiosResponse<import("../../../clients/rest/generated/data-contracts").V1CancelledTasks, any, {}>>;
    /**
     * Replays a task or workflow run by its ID.
     * @param opts - The options for the replay operation.
     * @returns A promise that resolves to the replayed run.
     */
    replay(opts: ReplayRunOpts): Promise<import("axios").AxiosResponse<import("../../../clients/rest/generated/data-contracts").V1ReplayedTasks, any, {}>>;
    private prepareFilter;
    private prepareListFilter;
    /**
     * Restore an evicted durable task so it can resume execution.
     * @param taskExternalId - The external ID of the evicted task.
     */
    restoreTask(taskExternalId: string): Promise<import("axios").AxiosResponse<import("../../../clients/rest/generated/data-contracts").V1RestoreTaskResponse, any, {}>>;
    /**
     * Fork (reset) a durable task from a specific node, triggering re-execution from that point.
     * @param taskExternalId - The external ID of the durable task to reset.
     * @param nodeId - The node ID to replay from.
     */
    branchDurableTask(taskExternalId: string, nodeId: number, branchId?: number): Promise<import("axios").AxiosResponse<import("../../../clients/rest/generated/data-contracts").V1BranchDurableTaskResponse, any, {}>>;
    /**
     * Resolve the task external ID for a workflow run. For runs with multiple tasks,
     * returns the first task's external ID.
     * @param workflowRunId - The workflow run ID to look up.
     * @returns The task external ID.
     */
    getTaskExternalId(workflowRunId: string): Promise<string>;
    runRef<T extends Record<string, any> = any>(id: string): WorkflowRunRef<T>;
    /**
     * Subscribes to a stream of events for a task or workflow run by its ID.
     * @param workflowRunId - The ID of the run to subscribe to.
     * @returns A promise that resolves to the stream of events.
     */
    subscribeToStream(workflowRunId: string): AsyncIterableIterator<string>;
}
