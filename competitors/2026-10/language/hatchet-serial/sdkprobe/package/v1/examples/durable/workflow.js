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
exports.durableSpawnDag = exports.errorRaisingDurableParent = exports.errorRaisingTask = exports.dagChildWorkflow = exports.memoNowCaching = exports.waitForTwoEventsSecondPushedFirst = exports.waitForOrEventLookback = exports.waitForEventLookback = exports.LOOKBACK_WINDOW = exports.durableReplayReset = exports.REPLAY_RESET_MEMOIZED_MAX_SECONDS = exports.REPLAY_RESET_SLEEP_SECONDS = exports.durableNonDeterminism = exports.durableWithExplicitSpawn = exports.durableSleepEventSpawn = exports.durableWithBulkSpawn = exports.durableWithSpawn = exports.spawnChildTask = exports.waitForSleepTwice = exports.durableWorkflow = exports.SLEEP_TIME = exports.SLEEP_TIME_SECONDS = exports.EVENT_KEY = void 0;
const v4_1 = require("zod/v4");
const conditions_1 = require("../../conditions");
const non_determinism_error_1 = require("../../../util/errors/non-determinism-error");
const sleep_1 = __importDefault(require("../../../util/sleep"));
const duration_1 = require("../../client/duration");
const hatchet_client_1 = require("../hatchet-client");
exports.EVENT_KEY = 'durable-example:event';
exports.SLEEP_TIME_SECONDS = 2;
exports.SLEEP_TIME = `${exports.SLEEP_TIME_SECONDS}s`;
// > Create a durable workflow
exports.durableWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'durable-workflow',
});
// !!
exports.durableWorkflow.task({
    name: 'ephemeral_task',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info('Running non-durable task');
    }),
});
exports.durableWorkflow.durableTask({
    name: 'durable_task',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info('Waiting for sleep');
        const sleepResult = yield ctx.sleepFor(exports.SLEEP_TIME, { label: 'waiting for sleep' });
        ctx.logger.info('Sleep finished');
        ctx.logger.info('Waiting for event');
        const event = yield ctx.waitForEvent(exports.EVENT_KEY, 'true');
        ctx.logger.info('Event received');
        return {
            status: 'success',
            event: event,
            sleep_duration_ms: sleepResult.durationMs,
        };
    }),
});
function extractKeyAndEventId(waitResult) {
    // DurableContext.waitFor currently returns the CREATE payload directly.
    // The shape is typically `{ [readableDataKey]: { [eventId]: ... } }`.
    const obj = waitResult;
    if (obj && typeof obj === 'object') {
        const [key] = Object.keys(obj);
        const inner = obj[key];
        if (inner && typeof inner === 'object' && !Array.isArray(inner)) {
            const [eventId] = Object.keys(inner);
            if (eventId) {
                return { key, eventId };
            }
        }
        if (key) {
            return { key: 'CREATE', eventId: key };
        }
    }
    return { key: 'CREATE', eventId: '' };
}
exports.durableWorkflow.durableTask({
    name: 'wait_for_or_group_1',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const start = Date.now();
        const waitResult = yield ctx.waitFor((0, conditions_1.Or)(new conditions_1.SleepCondition(exports.SLEEP_TIME, 'sleep'), new conditions_1.UserEventCondition(exports.EVENT_KEY, '', 'event')));
        const { key, eventId } = extractKeyAndEventId(waitResult);
        return {
            runtime: Math.round((Date.now() - start) / 1000),
            key,
            eventId,
        };
    }),
});
exports.durableWorkflow.durableTask({
    name: 'wait_for_or_group_2',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const start = Date.now();
        const waitResult = yield ctx.waitFor((0, conditions_1.Or)(new conditions_1.SleepCondition(`${6 * exports.SLEEP_TIME_SECONDS}s`, 'sleep'), new conditions_1.UserEventCondition(exports.EVENT_KEY, '', 'event')));
        const { key, eventId } = extractKeyAndEventId(waitResult);
        return {
            runtime: Math.round((Date.now() - start) / 1000),
            key,
            eventId,
        };
    }),
});
exports.durableWorkflow.durableTask({
    name: 'wait_for_multi_sleep',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const start = Date.now();
        // sleep 3 times
        for (let i = 0; i < 3; i += 1) {
            yield ctx.sleepFor(exports.SLEEP_TIME);
        }
        return { runtime: Math.round((Date.now() - start) / 1000) };
    }),
});
exports.waitForSleepTwice = hatchet_client_1.hatchet.durableTask({
    name: 'wait-for-sleep-twice',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            const start = Date.now();
            yield ctx.sleepFor(exports.SLEEP_TIME);
            return { runtime: Math.round((Date.now() - start) / 1000) };
        }
        catch (e) {
            return { runtime: -1 };
        }
    }),
});
// --- Spawn child from durable task ---
exports.spawnChildTask = hatchet_client_1.hatchet.task({
    name: 'spawn-child-task',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        var _a;
        return { message: `hello from child ${(_a = input.n) !== null && _a !== void 0 ? _a : 1}` };
    }),
});
exports.durableWithSpawn = hatchet_client_1.hatchet.durableTask({
    name: 'durable-with-spawn',
    executionTimeout: '10s',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const childResult = yield exports.spawnChildTask.run({});
        return { child_output: childResult };
    }),
});
exports.durableWithBulkSpawn = hatchet_client_1.hatchet.durableTask({
    name: 'durable-with-bulk-spawn',
    executionTimeout: '10m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        var _a;
        const n = (_a = input.n) !== null && _a !== void 0 ? _a : 10;
        const inputs = Array.from({ length: n }, (_, i) => ({ n: i }));
        const childResults = yield exports.spawnChildTask.run(inputs);
        return { child_outputs: childResults };
    }),
});
exports.durableSleepEventSpawn = hatchet_client_1.hatchet.durableTask({
    name: 'durable-sleep-event-spawn',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const start = Date.now();
        yield ctx.sleepFor(exports.SLEEP_TIME);
        yield ctx.waitForEvent(exports.EVENT_KEY, 'true');
        const childResult = yield exports.spawnChildTask.run({});
        return {
            runtime: (Date.now() - start) / 1000,
            child_output: childResult,
        };
    }),
});
// --- Spawn child using explicit ctx.spawnChild ---
exports.durableWithExplicitSpawn = hatchet_client_1.hatchet.durableTask({
    name: 'durable-with-explicit-spawn',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const childResult = yield ctx.spawnChild(exports.spawnChildTask, {});
        return { child_output: childResult };
    }),
});
// --- Non-determinism detection ---
exports.durableNonDeterminism = hatchet_client_1.hatchet.durableTask({
    name: 'durable-non-determinism',
    executionTimeout: '10s',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const sleepTime = ctx.invocationCount * 2;
        try {
            yield ctx.sleepFor(`${sleepTime}s`);
        }
        catch (e) {
            if (e instanceof non_determinism_error_1.NonDeterminismError) {
                return {
                    attempt_number: ctx.invocationCount,
                    sleep_time: sleepTime,
                    non_determinism_detected: true,
                    node_id: e.nodeId,
                };
            }
            throw e;
        }
        return {
            attempt_number: ctx.invocationCount,
            sleep_time: sleepTime,
            non_determinism_detected: false,
        };
    }),
});
// --- Replay reset ---
exports.REPLAY_RESET_SLEEP_SECONDS = 3;
/** Max duration (seconds) for a replayed/memoized step; above this we treat it as a real sleep. */
exports.REPLAY_RESET_MEMOIZED_MAX_SECONDS = 5;
const REPLAY_RESET_SLEEP = `${exports.REPLAY_RESET_SLEEP_SECONDS}s`;
exports.durableReplayReset = hatchet_client_1.hatchet.durableTask({
    name: 'durable-replay-reset',
    executionTimeout: '20s',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        let start = Date.now();
        yield ctx.sleepFor(REPLAY_RESET_SLEEP);
        const sleep1Duration = (Date.now() - start) / 1000;
        start = Date.now();
        yield ctx.sleepFor(REPLAY_RESET_SLEEP);
        const sleep2Duration = (Date.now() - start) / 1000;
        start = Date.now();
        yield ctx.sleepFor(REPLAY_RESET_SLEEP);
        const sleep3Duration = (Date.now() - start) / 1000;
        return {
            sleep_1_duration: sleep1Duration,
            sleep_2_duration: sleep2Duration,
            sleep_3_duration: sleep3Duration,
        };
    }),
});
exports.LOOKBACK_WINDOW = '1m';
const lookbackEventPayloadSchema = v4_1.z.object({
    order: v4_1.z.string(),
    user_id: v4_1.z.number(),
});
const twoEventsPayloadSchema = v4_1.z.object({
    order: v4_1.z.string(),
});
exports.waitForEventLookback = hatchet_client_1.hatchet.durableTask({
    name: 'wait-for-event-lookback',
    executionTimeout: '10m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const start = Date.now();
        // > Wait for event with lookback
        const event = yield ctx.waitForEvent('user:create', `input.user_id == ${input.userId}`, lookbackEventPayloadSchema, `user_id:${input.userId}`, '1m');
        // !!
        return {
            elapsed: (Date.now() - start) / 1000,
            event,
        };
    }),
});
exports.waitForOrEventLookback = hatchet_client_1.hatchet.durableTask({
    name: 'wait-for-or-event-lookback',
    executionTimeout: '10m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const start = Date.now();
        const now = yield ctx.now();
        const considerEventsSince = new Date(now.getTime() - (0, duration_1.durationToMs)(exports.LOOKBACK_WINDOW)).toISOString();
        yield ctx.waitFor((0, conditions_1.Or)(new conditions_1.SleepCondition(exports.SLEEP_TIME), new conditions_1.UserEventCondition(exports.EVENT_KEY, '', undefined, undefined, input.scope, considerEventsSince)));
        return {
            elapsed: (Date.now() - start) / 1000,
        };
    }),
});
exports.waitForTwoEventsSecondPushedFirst = hatchet_client_1.hatchet.durableTask({
    name: 'wait-for-two-events-second-pushed-first',
    executionTimeout: '10m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const start = Date.now();
        const event1 = yield ctx.waitForEvent('key1', undefined, twoEventsPayloadSchema, input.scope, exports.LOOKBACK_WINDOW);
        const event2 = yield ctx.waitForEvent('key2', undefined, twoEventsPayloadSchema, input.scope, exports.LOOKBACK_WINDOW);
        return {
            elapsed: (Date.now() - start) / 1000,
            event1,
            event2,
        };
    }),
});
exports.memoNowCaching = hatchet_client_1.hatchet.durableTask({
    name: 'memo-now-caching',
    executionTimeout: '10m',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const now = yield ctx.now();
        return { start_time: now.toISOString() };
    }),
});
// --- Spawn DAG from durable task ---
exports.dagChildWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'dag-child-workflow-ts',
});
const dagChild1 = exports.dagChildWorkflow.task({
    name: 'dag-child-1',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(1000);
        return { result: 'child1' };
    }),
});
exports.dagChildWorkflow.task({
    name: 'dag-child-2',
    parents: [dagChild1],
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(2000);
        return { result: 'child2' };
    }),
});
// --- Error propagation test ---
exports.errorRaisingTask = hatchet_client_1.hatchet.task({
    name: 'error-raising-task',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        throw new Error(input.error_message);
    }),
});
exports.errorRaisingDurableParent = hatchet_client_1.hatchet.durableTask({
    name: 'error-raising-durable-parent',
    executionTimeout: '30s',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        let childRaised = false;
        let childErrorStr = null;
        const ref = yield exports.errorRaisingTask.runNoWait(input);
        try {
            yield ref.output;
        }
        catch (e) {
            childRaised = true;
            childErrorStr = e instanceof Error ? e.message : String(e);
        }
        return {
            child_raised: childRaised,
            child_error_str: childErrorStr,
            child_run_external_id: yield ref.runId,
            parent_run_external_id: ctx.workflowRunId(),
        };
    }),
});
exports.durableSpawnDag = hatchet_client_1.hatchet.durableTask({
    name: 'durable-spawn-dag',
    executionTimeout: '10s',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const sleepStart = Date.now();
        const sleepResult = yield ctx.sleepFor(exports.SLEEP_TIME);
        const sleepDuration = (Date.now() - sleepStart) / 1000;
        const spawnStart = Date.now();
        const spawnResult = yield exports.dagChildWorkflow.run({});
        const spawnDuration = (Date.now() - spawnStart) / 1000;
        return {
            sleep_duration: sleepDuration,
            sleep_duration_ms: sleepResult.durationMs,
            spawn_duration: spawnDuration,
            spawn_result: spawnResult,
        };
    }),
});
