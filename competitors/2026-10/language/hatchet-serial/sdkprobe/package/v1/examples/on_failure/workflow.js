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
exports.failureWorkflow = exports.ERROR_TEXT = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.ERROR_TEXT = 'step1 failed';
// > On Failure Task
// This workflow will fail because `step1` throws. We define an `onFailure` handler to run cleanup.
exports.failureWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'on-failure-workflow',
});
exports.failureWorkflow.task({
    name: 'step1',
    executionTimeout: '1s',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        throw new Error(exports.ERROR_TEXT);
    }),
});
// 👀 After the workflow fails, this special step will run
exports.failureWorkflow.onFailure({
    name: 'on_failure',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.logger.info(`onFailure for run: ${ctx.workflowRunId()}`);
        ctx.logger.info('upstream errors', { errors: ctx.errors() });
        return {
            status: 'success',
        };
    }),
});
// !!
