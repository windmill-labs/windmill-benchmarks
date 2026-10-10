import { TaskStats } from '../../../clients/rest/generated/data-contracts';
import { HatchetClient } from '../client';
export type TaskStatusMetrics = {
    cancelled: number;
    completed: number;
    failed: number;
    queued: number;
    running: number;
};
/**
 * The metrics client is a client for reading metrics out of Hatchet’s metrics API.
 */
export declare class MetricsClient {
    tenantId: string;
    api: HatchetClient['api'];
    constructor(client: HatchetClient);
    /**
     * Returns aggregate task run counts grouped by status (queued, running, completed, failed, cancelled)
     * @param query - Filters for the metrics query (e.g. `since`, `until`, `workflow_ids`).
     * @param requestParams - Optional request-level overrides (headers, signal, etc.).
     * @returns Counts per status for the matched task runs.
     */
    getTaskStatusMetrics(query: Parameters<typeof this.api.v1TaskListStatusMetrics>[1], requestParams?: Parameters<typeof this.api.v1TaskListStatusMetrics>[2]): Promise<TaskStatusMetrics>;
    /**
     * Returns the queue metrics for the current tenant.
     * @param opts - The options for the request.
     * @returns The queue metrics for the current tenant.
     */
    getQueueMetrics(opts?: Parameters<typeof this.api.tenantGetStepRunQueueMetrics>[1]): Promise<import("../../../clients/rest/generated/data-contracts").TenantStepRunQueueMetrics>;
    /**
     * Scrape Prometheus metrics for the tenant.
     * @returns The metrics in Prometheus text format.
     */
    scrapePrometheusMetrics(): Promise<string>;
    /**
     * Get task statistics for the tenant.
     * @returns A record mapping task names to their statistics.
     */
    getTaskStats(): Promise<TaskStats>;
}
