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
exports.abortSignal = exports.cancellation = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const axios_1 = __importDefault(require("axios"));
const hatchet_client_1 = require("../hatchet-client");
// > Declaring a Task
exports.cancellation = hatchet_client_1.hatchet.task({
    name: 'cancellation',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(10 * 1000);
        if (ctx.cancelled) {
            throw new Error('Task was cancelled');
        }
        return {
            Completed: true,
        };
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
