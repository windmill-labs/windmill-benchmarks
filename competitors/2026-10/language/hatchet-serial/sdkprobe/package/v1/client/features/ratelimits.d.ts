import { RateLimitDuration } from '../../../protoc/workflows';
import { RateLimitOrderByField, RateLimitOrderByDirection } from '../../../clients/rest/generated/data-contracts';
import { HatchetClient } from '../client';
export { RateLimitDuration, RateLimitOrderByField, RateLimitOrderByDirection };
export type CreateRateLimitOpts = {
    key: string;
    limit: number;
    duration?: RateLimitDuration;
};
/**
 * The rate limits client is a wrapper for Hatchet’s gRPC API that makes it easier to work with rate limits in Hatchet.
 */
export declare class RatelimitsClient {
    api: HatchetClient['api'];
    admin: HatchetClient['admin'];
    tenantId: string;
    constructor(client: HatchetClient);
    /**
     * Upserts a rate limit for the current tenant.
     * @param opts - The options for the upsert operation.
     * @returns A promise that resolves to the key of the upserted rate limit.
     */
    upsert(opts: CreateRateLimitOpts): Promise<string>;
    /**
     * Lists all rate limits for the current tenant.
     * @param opts - The options for the list operation.
     * @returns A promise that resolves to the list of rate limits.
     */
    list(opts: Parameters<typeof this.api.rateLimitList>[1]): Promise<import("../../../clients/rest/generated/data-contracts").RateLimitList>;
}
