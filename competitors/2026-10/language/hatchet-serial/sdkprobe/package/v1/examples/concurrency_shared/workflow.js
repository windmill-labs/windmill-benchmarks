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
exports.concurrencySharedMixedWorkflow = exports.concurrencySharedWorkflowB = exports.concurrencySharedWorkflowA = exports.sharedLimit = exports.SLEEP_TIME_MS = void 0;
const v1_1 = require("../..");
const hatchet_client_1 = require("../hatchet-client");
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
exports.SLEEP_TIME_MS = 1500;
// > Shared Concurrency Strategy
// A tenant-scoped strategy is shared across workflows: every task declaring the same name
// consumes the same concurrency limit. The definition rides on workflow registration and
// re-registering the name updates it in place.
exports.sharedLimit = {
    name: 'ts-example-shared-limit',
    isTenantScoped: true,
    expression: 'input.group',
    maxRuns: 1,
    limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
};
const runWindowTask = () => __awaiter(void 0, void 0, void 0, function* () {
    const startMs = Date.now();
    yield sleep(exports.SLEEP_TIME_MS);
    return { startMs, endMs: Date.now() };
});
exports.concurrencySharedWorkflowA = hatchet_client_1.hatchet.workflow({
    name: 'concurrency-shared-a',
});
exports.concurrencySharedWorkflowA.task({
    name: 'shared-task',
    concurrency: [exports.sharedLimit],
    fn: runWindowTask,
});
exports.concurrencySharedWorkflowB = hatchet_client_1.hatchet.workflow({
    name: 'concurrency-shared-b',
});
exports.concurrencySharedWorkflowB.task({
    name: 'shared-task',
    concurrency: [exports.sharedLimit],
    fn: runWindowTask,
});
// !!
// > Mixed Inline And Shared Concurrency
// A single task can combine a workflow-scoped inline strategy with a shared strategy;
// both limits apply at once.
exports.concurrencySharedMixedWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'concurrency-shared-mixed',
});
exports.concurrencySharedMixedWorkflow.task({
    name: 'shared-task',
    concurrency: [
        {
            expression: 'input.inline',
            maxRuns: 1,
            limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        },
        exports.sharedLimit,
    ],
    fn: runWindowTask,
});
// !!
