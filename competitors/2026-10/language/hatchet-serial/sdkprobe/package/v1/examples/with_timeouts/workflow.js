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
exports.refreshTimeout = exports.withTimeouts = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
// > Execution Timeout
exports.withTimeouts = hatchet_client_1.hatchet.task({
    name: 'with-timeouts',
    // time the task can wait in the queue before it is cancelled
    scheduleTimeout: '10s',
    // time the task can run before it is cancelled
    executionTimeout: '10s',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // wait 15 seconds
        yield (0, sleep_1.default)(15000);
        // get the abort controller
        const { abortController } = ctx;
        // if the abort controller is aborted, throw an error
        if (abortController.signal.aborted) {
            throw new Error('cancelled');
        }
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    }),
});
// !!
// > Refresh Timeout
exports.refreshTimeout = hatchet_client_1.hatchet.task({
    name: 'refresh-timeout',
    executionTimeout: '10s',
    scheduleTimeout: '10s',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // adds 15 seconds to the execution timeout
        ctx.refreshTimeout('15s');
        yield (0, sleep_1.default)(15000);
        // get the abort controller
        const { abortController } = ctx;
        // now this condition will not be met
        // if the abort controller is aborted, throw an error
        if (abortController.signal.aborted) {
            throw new Error('cancelled');
        }
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    }),
});
// !!
