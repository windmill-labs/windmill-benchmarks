import { V1CreateWebhookRequestBase, V1UpdateWebhookRequest, V1Webhook, V1WebhookList, V1WebhookSourceName, V1WebhookAPIKeyAuth, V1WebhookBasicAuth, V1WebhookHMACAuth } from '../../../clients/rest/generated/data-contracts';
import { HatchetClient } from '../client';
export type CreateWebhookOptions = V1CreateWebhookRequestBase & {
    auth: V1WebhookBasicAuth | V1WebhookAPIKeyAuth | V1WebhookHMACAuth;
};
/**
 * Client for managing incoming webhooks in Hatchet.
 *
 * Webhooks allow external systems to trigger Hatchet workflows by sending
 * HTTP requests to dedicated endpoints. This enables real-time integration
 * with third-party services like GitHub, Stripe, Slack, or any system that
 * can send webhook events.
 */
export declare class WebhooksClient {
    api: HatchetClient['api'];
    tenantId: string;
    constructor(client: HatchetClient);
    /**
     * Lists all webhooks for the current tenant.
     * @param options - The options for the list operation.
     * @returns A promise that resolves to the list of webhooks.
     */
    list(options?: {
        limit?: number;
        offset?: number;
        webhookNames?: string[];
        sourceNames?: V1WebhookSourceName[];
    }): Promise<V1WebhookList>;
    /**
     * Gets a webhook by its name.
     * @param webhookName - The name of the webhook to get.
     * @returns A promise that resolves to the webhook.
     */
    get(webhookName: string): Promise<V1Webhook>;
    /**
     * Creates a new webhook.
     * @param request - The request options for the create operation.
     * @returns A promise that resolves to the created webhook.
     */
    create(request: CreateWebhookOptions): Promise<V1Webhook>;
    /**
     * Updates a webhook by its name.
     * @param webhookName - The name of the webhook to update.
     * @param options - The options for the update operation.
     * @returns A promise that resolves to the updated webhook.
     */
    update(webhookName: string, options?: Partial<V1UpdateWebhookRequest>): Promise<V1Webhook>;
    /**
     * Deletes a webhook by its name.
     * @param webhookName - The name of the webhook to delete.
     * @returns A promise that resolves to the deleted webhook.
     */
    delete(webhookName: string): Promise<V1Webhook>;
}
