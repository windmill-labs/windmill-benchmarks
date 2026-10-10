"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MetricsClient = void 0;
/**
 * The metrics client is a client for reading metrics out of Hatchet’s metrics API.
 */
class MetricsClient {
    constructor(client) {
        this.tenantId = client.tenantId;
        this.api = client.api;
    }
    /**
     * Returns aggregate task run counts grouped by status (queued, running, completed, failed, cancelled)
     * @param query - Filters for the metrics query (e.g. `since`, `until`, `workflow_ids`).
     * @param requestParams - Optional request-level overrides (headers, signal, etc.).
     * @returns Counts per status for the matched task runs.
     */
    getTaskStatusMetrics(query, requestParams) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.v1TaskListStatusMetrics(this.tenantId, query, requestParams);
            return data.reduce((acc, curr) => {
                acc[curr.status.toLowerCase()] = curr.count;
                return acc;
            }, {});
        });
    }
    /**
     * Returns the queue metrics for the current tenant.
     * @param opts - The options for the request.
     * @returns The queue metrics for the current tenant.
     */
    getQueueMetrics(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.tenantGetStepRunQueueMetrics(this.tenantId, opts);
            return data;
        });
    }
    /**
     * Scrape Prometheus metrics for the tenant.
     * @returns The metrics in Prometheus text format.
     */
    scrapePrometheusMetrics() {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.tenantGetPrometheusMetrics(this.tenantId);
            return data;
        });
    }
    /**
     * Get task statistics for the tenant.
     * @returns A record mapping task names to their statistics.
     */
    getTaskStats() {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.tenantGetTaskStats(this.tenantId);
            return data;
        });
    }
}
exports.MetricsClient = MetricsClient;
