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
exports.RatelimitsClient = exports.RateLimitOrderByDirection = exports.RateLimitOrderByField = exports.RateLimitDuration = void 0;
const workflows_1 = require("../../../protoc/workflows");
Object.defineProperty(exports, "RateLimitDuration", { enumerable: true, get: function () { return workflows_1.RateLimitDuration; } });
const data_contracts_1 = require("../../../clients/rest/generated/data-contracts");
Object.defineProperty(exports, "RateLimitOrderByField", { enumerable: true, get: function () { return data_contracts_1.RateLimitOrderByField; } });
Object.defineProperty(exports, "RateLimitOrderByDirection", { enumerable: true, get: function () { return data_contracts_1.RateLimitOrderByDirection; } });
/**
 * The rate limits client is a wrapper for Hatchet’s gRPC API that makes it easier to work with rate limits in Hatchet.
 */
class RatelimitsClient {
    constructor(client) {
        this.api = client.api;
        this.admin = client.admin;
        this.tenantId = client.tenantId;
    }
    /**
     * Upserts a rate limit for the current tenant.
     * @param opts - The options for the upsert operation.
     * @returns A promise that resolves to the key of the upserted rate limit.
     */
    upsert(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.admin.putRateLimit(opts.key, opts.limit, opts.duration);
            return opts.key;
        });
    }
    /**
     * Lists all rate limits for the current tenant.
     * @param opts - The options for the list operation.
     * @returns A promise that resolves to the list of rate limits.
     */
    list(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.rateLimitList(this.tenantId, opts);
            return data;
        });
    }
}
exports.RatelimitsClient = RatelimitsClient;
