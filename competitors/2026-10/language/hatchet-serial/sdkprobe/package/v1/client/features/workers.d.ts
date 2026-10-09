import { HatchetClient } from '../client';
/**
 * The workers client is a client for managing workers programmatically within Hatchet.
 */
export declare class WorkersClient {
    api: HatchetClient['api'];
    tenantId: string;
    constructor(client: HatchetClient);
    /**
     * Get a worker by its ID.
     * @param workerId - The ID of the worker to get.
     * @returns A promise that resolves to the worker.
     */
    get(workerId: string): Promise<import("../../../clients/rest/generated/data-contracts").Worker>;
    /**
     * List all workers in the tenant.
     * @returns A promise that resolves to the list of workers.
     */
    list(): Promise<import("../../../clients/rest/generated/data-contracts").WorkerList>;
    /**
     * Check if a worker is paused.
     * @param workerId - The ID of the worker to check.
     * @returns A promise that resolves to true if the worker is paused, false otherwise.
     */
    isPaused(workerId: string): Promise<boolean>;
    /**
     * Pause a worker.
     * @param workerId - The ID of the worker to pause.
     * @returns A promise that resolves to the paused worker.
     */
    pause(workerId: string): Promise<import("../../../clients/rest/generated/data-contracts").Worker>;
    /**
     * Unpause a worker.
     * @param workerId - The ID of the worker to unpause.
     * @returns A promise that resolves to the unpaused worker.
     */
    unpause(workerId: string): Promise<import("../../../clients/rest/generated/data-contracts").Worker>;
}
