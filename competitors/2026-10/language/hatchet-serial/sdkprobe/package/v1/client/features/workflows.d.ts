import { BaseWorkflowDeclaration, WorkflowDefinition } from '../..';
import type { LegacyWorkflow } from '../../../legacy/legacy-transformer';
import { WorkflowPauseScheduledCronRunQueueBehavior } from '../../../clients/rest/generated/data-contracts';
import { HatchetClient } from '../client';
import { Duration } from '../duration';
export { WorkflowPauseScheduledCronRunQueueBehavior };
export type PauseWorkflowOpts = {
    /** How long runs stay queued while the workflow is paused before they are dropped. */
    queueTTL: Duration;
    /** The behavior of cron runs triggered while the workflow is paused. Defaults to QUEUE. */
    pausedWorkflowCronRunQueueBehavior?: WorkflowPauseScheduledCronRunQueueBehavior;
    /** The behavior of scheduled runs triggered while the workflow is paused. Defaults to QUEUE. */
    pausedWorkflowScheduledRunQueueBehavior?: WorkflowPauseScheduledCronRunQueueBehavior;
};
export declare const workflowNameString: (workflow: string | WorkflowDefinition | BaseWorkflowDeclaration<any, any> | LegacyWorkflow) => string;
/**
 * The workflows client is a client for managing workflows programmatically within Hatchet.
 *
 * NOTE: that workflows are the declaration, not the individual runs. If you're looking for runs, use the RunsClient instead.
 *
 */
export declare class WorkflowsClient {
    api: HatchetClient['api'];
    tenantId: string;
    private workflowCache;
    private cacheTTL;
    constructor(client: HatchetClient, cacheTTL?: number);
    /**
     * Gets the workflow ID from a workflow name, ID, or object.
     * If the input is not a valid UUID, it will look up the workflow by name.
     * @param workflow - The workflow name, ID, or object.
     * @returns The workflow ID as a string.
     */
    getWorkflowIdFromName(workflow: string | WorkflowDefinition | BaseWorkflowDeclaration<any, any> | LegacyWorkflow): Promise<string>;
    /**
     * Get a workflow by its name, ID, or object.
     * @param workflow - The workflow name, ID, or object.
     * @returns A promise that resolves to the workflow.
     */
    get(workflow: string | BaseWorkflowDeclaration<any, any> | LegacyWorkflow): Promise<import("../../../clients/rest/generated/data-contracts").Workflow>;
    /**
     * List all workflows in the tenant.
     * @param opts - The options for the list operation.
     * @returns A promise that resolves to the list of workflows.
     */
    list(opts?: Parameters<typeof this.api.workflowList>[1]): Promise<import("../../../clients/rest/generated/data-contracts").WorkflowList>;
    /**
     * Delete a workflow by its name, ID, or object.
     * @param workflow - The workflow name, ID, or object.
     * @returns A promise that resolves to the deleted workflow.
     */
    delete(workflow: string | BaseWorkflowDeclaration<any, any> | LegacyWorkflow): Promise<void>;
    /**
     * Pause a workflow. While paused, new runs of the workflow are queued but not started.
     * @param workflow - The workflow name, ID, or object.
     * @param opts - The options for the pause operation.
     * @returns A promise that resolves to the updated workflow.
     */
    pause(workflow: string | BaseWorkflowDeclaration<any, any> | LegacyWorkflow, opts: PauseWorkflowOpts): Promise<import("../../../clients/rest/generated/data-contracts").Workflow>;
    /**
     * Unpause a workflow.
     * @param workflow - The workflow name, ID, or object.
     * @returns A promise that resolves to the updated workflow.
     */
    unpause(workflow: string | BaseWorkflowDeclaration<any, any> | LegacyWorkflow): Promise<import("../../../clients/rest/generated/data-contracts").Workflow>;
    private update;
}
