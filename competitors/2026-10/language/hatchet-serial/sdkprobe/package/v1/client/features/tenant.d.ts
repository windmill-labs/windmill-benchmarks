import { Tenant } from '../../../clients/rest/generated/data-contracts';
import { HatchetClient } from '../client';
/**
 * Client for managing Tenants
 */
export declare class TenantClient {
    api: HatchetClient['api'];
    tenantId: string;
    constructor(client: HatchetClient);
    /**
     * Retrieves the current tenant.
     * @returns The Tenant object.
     */
    get(): Promise<Tenant>;
}
