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
exports.RateLimitsClient = void 0;
const retrier_1 = require("../../util/retrier");
/** Tenant-wide rate limits that tasks declare against by key. */
class RateLimitsClient {
    constructor(deps) {
        this.deps = deps;
    }
    /**
     * Creates or updates a rate limit.
     * @returns the rate limit's key
     */
    put(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            yield (0, retrier_1.retrier)(() => this.deps.rpc.putRateLimit({ key: opts.key, limit: opts.limit, duration: opts.duration }), this.deps.logger, this.deps.retrier);
            return opts.key;
        });
    }
}
exports.RateLimitsClient = RateLimitsClient;
