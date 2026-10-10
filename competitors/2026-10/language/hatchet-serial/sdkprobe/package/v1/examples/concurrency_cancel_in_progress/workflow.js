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
exports.concurrencyCancelInProgressWorkflow = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const v1_1 = require("../..");
const hatchet_client_1 = require("../hatchet-client");
// > Cancel In Progress
exports.concurrencyCancelInProgressWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'concurrencycancelinprogress',
    concurrency: {
        expression: 'input.group',
        maxRuns: 1,
        limitStrategy: v1_1.ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
    },
});
// !!
const step1 = exports.concurrencyCancelInProgressWorkflow.task({
    name: 'step1',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        for (let i = 0; i < 50; i += 1) {
            yield (0, sleep_1.default)(100, ctx.abortController.signal);
        }
        return {};
    }),
});
exports.concurrencyCancelInProgressWorkflow.task({
    name: 'step2',
    parents: [step1],
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        for (let i = 0; i < 50; i += 1) {
            yield (0, sleep_1.default)(100, ctx.abortController.signal);
        }
        return {};
    }),
});
