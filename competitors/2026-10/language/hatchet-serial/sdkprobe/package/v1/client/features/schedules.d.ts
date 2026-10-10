import { ScheduledWorkflows, ScheduledWorkflowsBulkDeleteFilter, ScheduledWorkflowsBulkDeleteResponse, ScheduledWorkflowsBulkUpdateResponse, ScheduledWorkflowsList } from '../../../clients/rest/generated/data-contracts';
import { z } from 'zod/v4';
import type { Workflow } from '../../../legacy/workflow';
import { BaseWorkflowDeclaration, WorkflowDefinition } from '../..';
import { HatchetClient } from '../client';
import { WorkflowsClient } from './workflows';
/**
 * Schema for creating a Scheduled Run Trigger.
 * @internal
 */
export declare const CreateScheduledRunTriggerSchema: z.ZodObject<{
    triggerAt: z.ZodCoercedDate<unknown>;
    input: z.ZodOptional<z.ZodRecord<z.ZodAny, z.ZodAny>>;
    additionalMetadata: z.ZodOptional<z.ZodRecord<z.ZodString, z.ZodString>>;
    priority: z.ZodOptional<z.ZodNumber>;
}, z.core.$strip>;
/**
 * Type representing the input for creating a Cron.
 * @internal
 */
export type CreateScheduledRunInput = z.infer<typeof CreateScheduledRunTriggerSchema>;
/**
 * Schema for updating (rescheduling) a Scheduled Run Trigger.
 * @internal
 */
export declare const UpdateScheduledRunTriggerSchema: z.ZodObject<{
    triggerAt: z.ZodCoercedDate<unknown>;
}, z.core.$strip>;
export type UpdateScheduledRunInput = z.infer<typeof UpdateScheduledRunTriggerSchema>;
/**
 * The scheduled client is a client for managing scheduled workflows within Hatchet
 */
export declare class ScheduleClient {
    api: HatchetClient['api'];
    tenantId: string;
    workflows: WorkflowsClient;
    namespace: string | undefined;
    constructor(client: HatchetClient);
    /**
     * Retrieves the Scheduled Run ID from a ScheduledRun object or a string.
     * @param scheduledRun - The ScheduledRun object or Scheduled Run ID as a string.
     * @returns The Scheduled Run ID as a string.
     */
    private getScheduledRunId;
    /**
     * Creates a new Scheduled Run.
     * @param workflow - The workflow name or Workflow object.
     * @param scheduledRun - The input data for creating the Scheduled Run.
     * @returns A promise that resolves to the created ScheduledWorkflows object.
     * @throws Will throw an error if the input is invalid or the API call fails.
     *
     * @important This method is instrumented by HatchetInstrumentor._patchScheduleCreate.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    create(workflow: string | Workflow, cron: CreateScheduledRunInput): Promise<ScheduledWorkflows>;
    /**
     * Updates (reschedules) an existing Scheduled Run.
     * @param scheduledRun - The Scheduled Run ID as a string or ScheduledWorkflows object.
     * @param update - The update payload (currently only triggerAt).
     * @returns A promise that resolves to the updated ScheduledWorkflows object.
     */
    update(scheduledRun: string | ScheduledWorkflows, update: UpdateScheduledRunInput): Promise<ScheduledWorkflows>;
    /**
     * Deletes an existing Scheduled Run.
     * @param scheduledRun - The Scheduled Run ID as a string or ScheduledWorkflows object.
     * @returns A promise that resolves when the Scheduled Run is deleted.
     */
    delete(scheduledRun: string | ScheduledWorkflows): Promise<void>;
    /**
     * Lists all Cron Triggers based on the provided query parameters.
     * @param query - Query parameters for listing Scheduled Runs.
     * @returns A promise that resolves to a ScheduledWorkflowsList object.
     */
    list(query: Parameters<typeof this.api.workflowScheduledList>[1] & {
        workflow?: string | Workflow | WorkflowDefinition | BaseWorkflowDeclaration<any, any>;
    }): Promise<ScheduledWorkflowsList>;
    /**
     * Retrieves a specific Scheduled Run by its ID.
     * @param scheduledRun - The Scheduled Run ID as a string or ScheduledWorkflows object.
     * @returns A promise that resolves to the ScheduledWorkflows object.
     */
    get(scheduledRun: string | ScheduledWorkflows): Promise<ScheduledWorkflows>;
    /**
     * Bulk deletes scheduled runs (by explicit IDs and/or a filter).
     * @param opts - Either `scheduledRuns` (ids/objects) and/or a server-side filter.
     * @returns A promise that resolves to deleted ids + per-id errors.
     */
    bulkDelete(opts: {
        scheduledRuns?: Array<string | ScheduledWorkflows>;
        filter?: ScheduledWorkflowsBulkDeleteFilter;
    }): Promise<ScheduledWorkflowsBulkDeleteResponse>;
    /**
     * Bulk updates (reschedules) scheduled runs.
     * @param updates - List of id/object + new triggerAt.
     * @returns A promise that resolves to updated ids + per-id errors.
     */
    bulkUpdate(updates: Array<{
        scheduledRun: string | ScheduledWorkflows;
        triggerAt: Date | string;
    }>): Promise<ScheduledWorkflowsBulkUpdateResponse>;
}
