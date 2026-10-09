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
exports.abortSignal = exports.cancellationWorkflow = void 0;
const axios_1 = __importDefault(require("axios"));
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
// > Self-cancelling workflow (mirrors Python example)
exports.cancellationWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'CancelWorkflow',
});
exports.cancellationWorkflow.task({
    name: 'self-cancel',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(2000, ctx.abortController.signal);
        // Cancel the current task run (server-side) and optimistically abort local execution.
        yield ctx.cancel();
        // If cancellation didn't stop execution yet, keep waiting but cooperatively.
        yield (0, sleep_1.default)(10000, ctx.abortController.signal);
        return { error: 'Task should have been cancelled' };
    }),
});
exports.cancellationWorkflow.task({
    name: 'check-flag',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        for (let i = 0; i < 3; i += 1) {
            yield (0, sleep_1.default)(1000, ctx.abortController.signal);
            if (ctx.cancelled) {
                throw new Error('Cancelled');
            }
        }
        return { error: 'Task should have been cancelled' };
    }),
});
// !!
// > Abort Signal
exports.abortSignal = hatchet_client_1.hatchet.task({
    name: 'abort-signal',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            const response = yield axios_1.default.get('https://api.example.com/data', {
                signal: ctx.abortController.signal,
            });
            // Handle the response
        }
        catch (error) {
            if (axios_1.default.isCancel(error)) {
                // Request was canceled
                ctx.logger.info('Request canceled');
            }
            else {
                // Handle other errors
            }
        }
    }),
});
// !!
// see ./worker.ts and ./run.ts for how to run the workflow
