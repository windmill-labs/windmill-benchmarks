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
exports.concurrencyWorkflowLevelWorkflow = exports.NAME_MAX_RUNS = exports.DIGIT_MAX_RUNS = exports.SLEEP_TIME_MS = void 0;
const v1_1 = require("../..");
const hatchet_client_1 = require("../hatchet-client");
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
exports.SLEEP_TIME_MS = 2000;
exports.DIGIT_MAX_RUNS = 8;
exports.NAME_MAX_RUNS = 3;
exports.concurrencyWorkflowLevelWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'concurrency-workflow-level',
    concurrency: [
        {
            expression: 'input.digit',
            maxRuns: exports.DIGIT_MAX_RUNS,
            limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        },
        {
            expression: 'input.name',
            maxRuns: exports.NAME_MAX_RUNS,
            limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        },
    ],
});
exports.concurrencyWorkflowLevelWorkflow.task({
    name: 'task_1',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        yield sleep(exports.SLEEP_TIME_MS);
        return {};
    }),
});
exports.concurrencyWorkflowLevelWorkflow.task({
    name: 'task_2',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        yield sleep(exports.SLEEP_TIME_MS);
        return {};
    }),
});
