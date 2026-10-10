import { type RetrierConfig } from '../../util/retrier';
import type { Logger } from '../../util/logger/logger';
import type { WorkflowServiceClient } from '../../protoc/workflows/workflows';
import type { CreateRateLimitOpts } from '../types';
export interface RateLimitsClientDeps {
    rpc: WorkflowServiceClient;
    logger: Logger;
    retrier?: RetrierConfig;
}
/** Tenant-wide rate limits that tasks declare against by key. */
export declare class RateLimitsClient {
    private readonly deps;
    constructor(deps: RateLimitsClientDeps);
    /**
     * Creates or updates a rate limit.
     * @returns the rate limit's key
     */
    put(opts: CreateRateLimitOpts): Promise<string>;
}
