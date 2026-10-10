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
exports.cancellation = void 0;
// > Declaring a Task
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
// (optional) Define the input type for the workflow
exports.cancellation = hatchet_client_1.hatchet.task({
    name: 'cancellation',
    executionTimeout: '3s',
    fn: (_1, _a) => __awaiter(void 0, [_1, _a], void 0, function* (_, { cancelled }) {
        yield (0, sleep_1.default)(10 * 1000);
        if (cancelled) {
            throw new Error('Task was cancelled');
        }
        return {
            Completed: true,
        };
    }),
});
// !!
// see ./worker.ts and ./run.ts for how to run the workflow
