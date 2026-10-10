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
exports.bulkReplayTest3 = exports.bulkReplayTest2 = exports.bulkReplayTest1 = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.bulkReplayTest1 = hatchet_client_1.hatchet.task({
    name: 'bulk-replay-test-1',
    retries: 1,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info('retrying bulk replay test task', { retryCount: ctx.retryCount() });
        if (ctx.retryCount() === 0) {
            throw new Error('This is a test error to trigger a retry.');
        }
    }),
});
exports.bulkReplayTest2 = hatchet_client_1.hatchet.task({
    name: 'bulk-replay-test-2',
    retries: 1,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info('retrying bulk replay test task', { retryCount: ctx.retryCount() });
        if (ctx.retryCount() === 0) {
            throw new Error('This is a test error to trigger a retry.');
        }
    }),
});
exports.bulkReplayTest3 = hatchet_client_1.hatchet.task({
    name: 'bulk-replay-test-3',
    retries: 1,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info('retrying bulk replay test task', { retryCount: ctx.retryCount() });
        if (ctx.retryCount() === 0) {
            throw new Error('This is a test error to trigger a retry.');
        }
    }),
});
