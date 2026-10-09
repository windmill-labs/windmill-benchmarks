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
exports.FiltersClient = void 0;
/**
 * The filters client is a client for interacting with Hatchet's filters API.
 */
class FiltersClient {
    constructor(client) {
        this.tenantId = client.tenantId;
        this.api = client.api;
    }
    /**
     * Lists all filters.
     * @param opts - The options for the list operation.
     * @param opts.limit - The number of filters to return.
     * @param opts.offset - The number of filters to skip before returning the result set.
     * @param opts.workflowIds - A list of workflow IDs to filter by.
     * @param opts.scopes - A list of scopes to filter by.
     * @returns A promise that resolves to the list of filters.
     */
    list(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.v1FilterList(this.tenantId, {
                limit: opts === null || opts === void 0 ? void 0 : opts.limit,
                offset: opts === null || opts === void 0 ? void 0 : opts.offset,
                workflowIds: opts === null || opts === void 0 ? void 0 : opts.workflowIds,
                scopes: opts === null || opts === void 0 ? void 0 : opts.scopes,
            });
            return data;
        });
    }
    /**
     * Gets a filter by its ID.
     * @param filterId - The ID of the filter to get.
     * @returns A promise that resolves to the filter.
     */
    get(filterId) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.v1FilterGet(this.tenantId, filterId);
            return data;
        });
    }
    /**
     * Creates a new filter.
     * @param opts - The options for the create operation.
     * @returns A promise that resolves to the created filter.
     */
    create(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.v1FilterCreate(this.tenantId, opts);
            return data;
        });
    }
    /**
     * Deletes a filter by its ID.
     * @param filterId - The ID of the filter to delete.
     * @returns A promise that resolves to the deleted filter.
     */
    delete(filterId) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.v1FilterDelete(this.tenantId, filterId);
            return data;
        });
    }
    /**
     * Updates a filter by its ID.
     * @param filterId - The ID of the filter to update.
     * @param updates - The updates to apply to the filter.
     * @returns A promise that resolves to the updated filter.
     */
    update(filterId, updates) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.v1FilterUpdate(this.tenantId, filterId, updates);
            return data;
        });
    }
}
exports.FiltersClient = FiltersClient;
