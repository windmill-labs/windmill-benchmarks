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
exports.concurrencyDynamicWorkflow = void 0;
const v1_1 = require("../..");
const hatchet_client_1 = require("../hatchet-client");
// > Dynamic Max Runs
// maxRuns accepts a number or a CEL expression string. With an expression, each
// concurrency group's limit is computed from the task's input.
exports.concurrencyDynamicWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'concurrency-dynamic',
});
exports.concurrencyDynamicWorkflow.task({
    name: 'dynamic-task',
    concurrency: [
        {
            expression: 'input.account',
            maxRuns: "input.tier == 'premium' ? 10 : 1",
            limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        },
    ],
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () { return ({ account: input.account }); }),
});
// !!
