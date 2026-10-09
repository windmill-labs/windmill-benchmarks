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
exports.V1LogLineOrderByDirection = exports.V1LogLineLevel = exports.LogsClient = void 0;
const data_contracts_1 = require("../../../clients/rest/generated/data-contracts");
Object.defineProperty(exports, "V1LogLineLevel", { enumerable: true, get: function () { return data_contracts_1.V1LogLineLevel; } });
Object.defineProperty(exports, "V1LogLineOrderByDirection", { enumerable: true, get: function () { return data_contracts_1.V1LogLineOrderByDirection; } });
/**
 * The logs client is a client for interacting with Hatchet's logs API.
 */
class LogsClient {
    constructor(client) {
        this.api = client.api;
        this.tenantId = client.tenantId;
    }
    /**
     * Lists the logs for a given task run.
     * @param taskRunId - The ID of the task run to list logs for.
     * @param opts - The options filter for the list operation.
     * @returns A promise that resolves to the list of logs.
     */
    list(taskRunId, opts) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            const { data } = yield this.api.v1LogLineList(taskRunId, {
                limit: opts === null || opts === void 0 ? void 0 : opts.limit,
                since: (_a = opts === null || opts === void 0 ? void 0 : opts.since) === null || _a === void 0 ? void 0 : _a.toISOString(),
                until: (_b = opts === null || opts === void 0 ? void 0 : opts.until) === null || _b === void 0 ? void 0 : _b.toISOString(),
                search: opts === null || opts === void 0 ? void 0 : opts.search,
                levels: opts === null || opts === void 0 ? void 0 : opts.levels,
                order_by_direction: opts === null || opts === void 0 ? void 0 : opts.orderByDirection,
                attempt: opts === null || opts === void 0 ? void 0 : opts.attempt,
            });
            return data;
        });
    }
}
exports.LogsClient = LogsClient;
