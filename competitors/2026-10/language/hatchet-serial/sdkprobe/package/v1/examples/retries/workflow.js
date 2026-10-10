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
exports.withBackoff = exports.retriesWithCount = exports.retries = void 0;
const hatchet_client_1 = require("../hatchet-client");
// > Simple Step Retries
exports.retries = hatchet_client_1.hatchet.task({
    name: 'retries',
    retries: 3,
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        throw new Error('intentional failure');
    }),
});
// !!
// > Retries with Count
exports.retriesWithCount = hatchet_client_1.hatchet.task({
    name: 'retries-with-count',
    retries: 3,
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // > Get the current retry count
        const retryCount = ctx.retryCount();
        ctx.logger.info(`Retry count: ${retryCount}`);
        if (retryCount < 2) {
            throw new Error('intentional failure');
        }
        return {
            message: 'success',
        };
    }),
});
// !!
// > Retries with Backoff
exports.withBackoff = hatchet_client_1.hatchet.task({
    name: 'with-backoff',
    retries: 10,
    backoff: {
        // 👀 Maximum number of seconds to wait between retries
        maxSeconds: 10,
        // 👀 Factor to increase the wait time between retries.
        // This sequence will be 2s, 4s, 8s, 10s, 10s, 10s... due to the maxSeconds limit
        factor: 2,
    },
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        throw new Error('intentional failure');
    }),
});
// !!
