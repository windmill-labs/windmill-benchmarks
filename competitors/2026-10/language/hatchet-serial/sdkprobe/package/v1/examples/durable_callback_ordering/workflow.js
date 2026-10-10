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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.callbackOrderingRoot = exports.callbackOrderingMid = exports.callbackOrderingLeaf = exports.ROOT_DEFAULTS = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
const WORKFLOW_PREFIX = 'durable-callback-ordering';
const EVICTION_POLICY = {
    ttl: 250,
    allowCapacityEviction: true,
    priority: 0,
};
exports.ROOT_DEFAULTS = {
    durables: 4,
    branches: 8,
    childDelayMs: 1500,
    delayStepMs: 3,
};
exports.callbackOrderingLeaf = hatchet_client_1.hatchet.task({
    name: `${WORKFLOW_PREFIX}-leaf`,
    executionTimeout: '1m',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(input.delayMs);
        return { mid: input.mid, branch: input.branch, generation: input.generation };
    }),
});
exports.callbackOrderingMid = hatchet_client_1.hatchet.durableTask({
    name: `${WORKFLOW_PREFIX}-mid`,
    executionTimeout: '5m',
    retries: 0,
    evictionPolicy: EVICTION_POLICY,
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // Staggered first-generation children complete out of spawn order, so each
        // branch's second-generation spawn is emitted in completion order. Replays
        // after eviction must re-deliver those completions in the recorded order or
        // the re-emitted spawn sequence diverges from the event log.
        const branch = (branchIndex) => __awaiter(void 0, void 0, void 0, function* () {
            const firstDelayMs = input.childDelayMs + (input.branches - branchIndex - 1) * input.delayStepMs;
            const first = yield exports.callbackOrderingLeaf.run({
                mid: input.mid,
                branch: branchIndex,
                delayMs: firstDelayMs,
                generation: 1,
            });
            const second = yield exports.callbackOrderingLeaf.run({
                mid: input.mid,
                branch: first.branch,
                delayMs: input.childDelayMs,
                generation: 2,
            });
            return second.branch;
        });
        const completed = yield Promise.all(Array.from({ length: input.branches }, (_, branchIndex) => branch(branchIndex)));
        return {
            mid: input.mid,
            completedBranches: completed,
            invocationCount: ctx.invocationCount,
        };
    }),
});
// The root is never evicted. The ordering property under test lives in the
// mids, and the worker evicts one waiting run per server round trip, oldest
// wait first: evictable roots register before every mid and would consume the
// sweep for as long as it takes to ack all of them before any mid is reached.
exports.callbackOrderingRoot = hatchet_client_1.hatchet.durableTask({
    name: `${WORKFLOW_PREFIX}-root`,
    executionTimeout: '10m',
    retries: 0,
    evictionPolicy: { ttl: undefined, allowCapacityEviction: false, priority: 0 },
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        var _a;
        const durables = (_a = input.durables) !== null && _a !== void 0 ? _a : exports.ROOT_DEFAULTS.durables;
        const results = yield Promise.all(Array.from({ length: durables }, (_, midIndex) => {
            var _a, _b, _c;
            return exports.callbackOrderingMid.run({
                mid: midIndex,
                branches: (_a = input.branches) !== null && _a !== void 0 ? _a : exports.ROOT_DEFAULTS.branches,
                childDelayMs: (_b = input.childDelayMs) !== null && _b !== void 0 ? _b : exports.ROOT_DEFAULTS.childDelayMs,
                delayStepMs: (_c = input.delayStepMs) !== null && _c !== void 0 ? _c : exports.ROOT_DEFAULTS.delayStepMs,
            });
        }));
        return {
            rootInvocationCount: ctx.invocationCount,
            midInvocationCounts: results.map((item) => item.invocationCount),
            completedMids: results.map((item) => item.mid),
        };
    }),
});
