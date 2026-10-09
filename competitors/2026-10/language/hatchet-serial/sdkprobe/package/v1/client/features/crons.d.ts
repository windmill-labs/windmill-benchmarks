import { CronWorkflows, CronWorkflowsList } from '../../../clients/rest/generated/data-contracts';
import { z } from 'zod/v4';
import { BaseWorkflowDeclaration } from '../..';
import type { LegacyWorkflow } from '../../../legacy/legacy-transformer';
import { HatchetClient } from '../client';
import { WorkflowsClient } from './workflows';
/**
 * Schema for creating a Cron Trigger.
 * @internal
 */
export declare const CreateCronTriggerSchema: z.ZodObject<{
    name: z.ZodString;
    expression: z.ZodString;
    input: z.ZodOptional<z.ZodRecord<z.ZodAny, z.ZodAny>>;
    additionalMetadata: z.ZodOptional<z.ZodRecord<z.ZodString, z.ZodString>>;
    priority: z.ZodOptional<z.ZodNumber>;
}, z.core.$strip>;
/**
 * Type representing the input for creating a Cron.
 * @internal
 */
export type CreateCronInput = z.infer<typeof CreateCronTriggerSchema>;
/**
 * The cron client is a client for managing cron workflows within Hatchet.
 */
export declare class CronClient {
    api: HatchetClient['api'];
    tenantId: string;
    workflows: WorkflowsClient;
    namespace: string | undefined;
    constructor(client: HatchetClient);
    /**
     * Retrieves the Cron ID from a CronWorkflows object or a string.
     * @param cron - The CronWorkflows object or Cron ID as a string.
     * @returns The Cron ID as a string.
     */
    private getCronId;
    /**
     * Creates a new Cron workflow.
     * @param workflow - The workflow identifier or Workflow object.
     * @param cron - The input data for creating the Cron Trigger.
     * @returns A promise that resolves to the created CronWorkflows object.
     * @throws Will throw an error if the input is invalid or the API call fails.
     */
    create(workflow: string | BaseWorkflowDeclaration<any, any> | LegacyWorkflow, cron: CreateCronInput): Promise<CronWorkflows>;
    /**
     * Deletes an existing Cron Trigger.
     * @param cron - The Cron Trigger ID as a string or CronWorkflows object.
     * @returns A promise that resolves when the Cron Trigger is deleted.
     */
    delete(cron: string | CronWorkflows): Promise<void>;
    /**
     * Lists all Cron Triggers based on the provided query parameters.
     * @param query - Query parameters for listing Cron Triggers.
     * @returns A promise that resolves to a CronWorkflowsList object.
     */
    list(query: Parameters<typeof this.api.cronWorkflowList>[1] & {
        workflow?: string | BaseWorkflowDeclaration<any, any> | LegacyWorkflow;
    }): Promise<CronWorkflowsList>;
    /**
     * Retrieves a specific Cron Trigger by its ID.
     * @param cron - The Cron Trigger ID as a string or CronWorkflows object.
     * @returns A promise that resolves to the CronWorkflows object.
     */
    get(cron: string | CronWorkflows): Promise<CronWorkflows>;
}
