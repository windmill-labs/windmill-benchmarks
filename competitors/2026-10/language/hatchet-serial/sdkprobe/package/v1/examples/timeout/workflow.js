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
exports.refreshTimeoutTask = exports.timeoutTask = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
// > Execution Timeout
// Mirrors Python `examples/timeout/test_timeout.py::test_execution_timeout`
exports.timeoutTask = hatchet_client_1.hatchet.task({
    name: 'timeout',
    executionTimeout: '3s',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            yield (0, sleep_1.default)(10 * 1000, ctx.abortController.signal);
        }
        catch (_a) {
            throw new Error('Task was cancelled');
        }
        return {
            status: 'success',
        };
    }),
});
// !!
// > Refresh Timeout
// Mirrors Python `examples/timeout/test_timeout.py::test_run_refresh_timeout`
exports.refreshTimeoutTask = hatchet_client_1.hatchet.task({
    name: 'refresh-timeout',
    executionTimeout: '3s',
    scheduleTimeout: '5s',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        ctx.refreshTimeout('5s');
        try {
            yield (0, sleep_1.default)(4000, ctx.abortController.signal);
        }
        catch (_a) {
            throw new Error('cancelled');
        }
        return {
            status: 'success',
            message: input.Message.toLowerCase(),
        };
    }),
});
// !!
