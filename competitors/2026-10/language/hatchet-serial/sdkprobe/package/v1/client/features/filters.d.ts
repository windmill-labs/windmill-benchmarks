import { HatchetClient } from '../client';
export type WorkflowIdScopePair = {
    workflowId: string;
    scope: string;
};
/**
 * The filters client is a client for interacting with Hatchet's filters API.
 */
export declare class FiltersClient {
    tenantId: string;
    api: HatchetClient['api'];
    constructor(client: HatchetClient);
    /**
     * Lists all filters.
     * @param opts - The options for the list operation.
     * @param opts.limit - The number of filters to return.
     * @param opts.offset - The number of filters to skip before returning the result set.
     * @param opts.workflowIds - A list of workflow IDs to filter by.
     * @param opts.scopes - A list of scopes to filter by.
     * @returns A promise that resolves to the list of filters.
     */
    list(opts?: {
        limit?: number;
        offset?: number;
        workflowIds?: string[];
        scopes?: string[];
    }): Promise<import("../../../clients/rest/generated/data-contracts").V1FilterList>;
    /**
     * Gets a filter by its ID.
     * @param filterId - The ID of the filter to get.
     * @returns A promise that resolves to the filter.
     */
    get(filterId: Parameters<typeof this.api.v1FilterGet>[1]): Promise<import("../../../clients/rest/generated/data-contracts").V1Filter>;
    /**
     * Creates a new filter.
     * @param opts - The options for the create operation.
     * @returns A promise that resolves to the created filter.
     */
    create(opts: Parameters<typeof this.api.v1FilterCreate>[1]): Promise<import("../../../clients/rest/generated/data-contracts").V1Filter>;
    /**
     * Deletes a filter by its ID.
     * @param filterId - The ID of the filter to delete.
     * @returns A promise that resolves to the deleted filter.
     */
    delete(filterId: Parameters<typeof this.api.v1FilterDelete>[1]): Promise<import("../../../clients/rest/generated/data-contracts").V1Filter>;
    /**
     * Updates a filter by its ID.
     * @param filterId - The ID of the filter to update.
     * @param updates - The updates to apply to the filter.
     * @returns A promise that resolves to the updated filter.
     */
    update(filterId: Parameters<typeof this.api.v1FilterDelete>[1], updates: Parameters<typeof this.api.v1FilterUpdate>[2]): Promise<import("../../../clients/rest/generated/data-contracts").V1Filter>;
}
