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
exports.nonEvictableSleep = exports.capacityEvictableSleep = exports.CAPACITY_SLEEP_SECONDS = exports.evictableChildBulkSpawn = exports.bulkChildTask = exports.multipleEviction = exports.evictableChildSpawn = exports.evictableMemoThenWaitForEvent = exports.MEMO_EVENT_KEY = exports.evictableWaitForEvent = exports.evictableSleepForGracefulTermination = exports.evictableSleep = exports.childTask = exports.EVENT_KEY = exports.LONG_SLEEP_SECONDS = exports.EVICTION_TTL_SECONDS = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
exports.EVICTION_TTL_SECONDS = 5;
exports.LONG_SLEEP_SECONDS = 15;
exports.EVENT_KEY = 'durable-eviction:event';
// > Eviction Policy
const EVICTION_POLICY = {
    ttl: `${exports.EVICTION_TTL_SECONDS}s`,
    allowCapacityEviction: true,
    priority: 0,
};
// !!
exports.childTask = hatchet_client_1.hatchet.task({
    name: 'eviction-child-task',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(exports.LONG_SLEEP_SECONDS * 1000);
        return { child_status: 'completed' };
    }),
});
// > Evictable Sleep
exports.evictableSleep = hatchet_client_1.hatchet.durableTask({
    name: 'evictable-sleep',
    executionTimeout: '5m',
    evictionPolicy: EVICTION_POLICY,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.sleepFor(`${exports.LONG_SLEEP_SECONDS}s`);
        return { status: 'completed' };
    }),
});
// !!
// NOTE: DO NOT REGISTER ON E2E TEST WORKER
exports.evictableSleepForGracefulTermination = hatchet_client_1.hatchet.durableTask({
    name: 'evictable-sleep-for-graceful-termination',
    executionTimeout: '5m',
    evictionPolicy: {
        ttl: `30m`,
        allowCapacityEviction: true,
        priority: 0,
    },
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.sleepFor(`5m`);
        return { status: 'completed' };
    }),
});
exports.evictableWaitForEvent = hatchet_client_1.hatchet.durableTask({
    name: 'evictable-wait-for-event',
    executionTimeout: '5m',
    evictionPolicy: EVICTION_POLICY,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.waitForEvent(exports.EVENT_KEY, 'true');
        return { status: 'completed' };
    }),
});
exports.MEMO_EVENT_KEY = 'durable-eviction:memo-event';
exports.evictableMemoThenWaitForEvent = hatchet_client_1.hatchet.durableTask({
    name: 'evictable-memo-then-wait-for-event',
    executionTimeout: '5m',
    evictionPolicy: EVICTION_POLICY,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // now() records a memo node. On replay after eviction, the server
        // re-delivers that node's completion, which never has a callback waiter
        // and must not block the event completion queued behind it.
        const memoizedNow = yield ctx.now();
        yield ctx.waitForEvent(exports.MEMO_EVENT_KEY, 'true');
        return { status: 'completed', memoizedNow: memoizedNow.toISOString() };
    }),
});
exports.evictableChildSpawn = hatchet_client_1.hatchet.durableTask({
    name: 'evictable-child-spawn',
    executionTimeout: '5m',
    evictionPolicy: EVICTION_POLICY,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const childResult = yield exports.childTask.run({});
        return { child: childResult, status: 'completed' };
    }),
});
exports.multipleEviction = hatchet_client_1.hatchet.durableTask({
    name: 'multiple-eviction',
    executionTimeout: '5m',
    evictionPolicy: EVICTION_POLICY,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.sleepFor(`${exports.LONG_SLEEP_SECONDS}s`);
        yield ctx.sleepFor(`${exports.LONG_SLEEP_SECONDS}s`);
        return { status: 'completed' };
    }),
});
exports.bulkChildTask = hatchet_client_1.hatchet.task({
    name: 'eviction-bulk-child-task',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(input.sleepSeconds * 1000);
        return { sleepSeconds: input.sleepSeconds, status: 'completed' };
    }),
});
exports.evictableChildBulkSpawn = hatchet_client_1.hatchet.durableTask({
    name: 'evictable-child-bulk-spawn',
    executionTimeout: '5m',
    evictionPolicy: EVICTION_POLICY,
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const inputs = Array.from({ length: 3 }, (_, i) => ({
            sleepSeconds: (exports.EVICTION_TTL_SECONDS + 5) * (i + 1),
        }));
        const childResults = yield exports.bulkChildTask.run(inputs);
        return { child_results: childResults, status: 'completed' };
    }),
});
exports.CAPACITY_SLEEP_SECONDS = 20;
exports.capacityEvictableSleep = hatchet_client_1.hatchet.durableTask({
    name: 'capacity-evictable-sleep',
    executionTimeout: '5m',
    evictionPolicy: {
        ttl: undefined,
        allowCapacityEviction: true,
        priority: 0,
    },
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.sleepFor(`${exports.CAPACITY_SLEEP_SECONDS}s`);
        return { status: 'completed' };
    }),
});
// > Non Evictable Sleep
exports.nonEvictableSleep = hatchet_client_1.hatchet.durableTask({
    name: 'non-evictable-sleep',
    executionTimeout: '5m',
    evictionPolicy: {
        ttl: undefined,
        allowCapacityEviction: false,
        priority: 0,
    },
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.sleepFor('10s');
        return { status: 'completed' };
    }),
});
// !!
