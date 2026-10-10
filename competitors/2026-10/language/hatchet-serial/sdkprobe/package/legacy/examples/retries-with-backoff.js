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
const sdk_1 = __importDefault(require("../../sdk"));
const hatchet = sdk_1.default.init();
let numRetries = 0;
// > Backoff
const workflow = {
    // ... normal workflow definition
    id: 'retries-with-backoff',
    description: 'Backoff',
    // ,
    steps: [
        {
            name: 'backoff-step',
            // ... step definition
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                if (numRetries < 5) {
                    numRetries += 1;
                    throw new Error('failed');
                }
                return { backoff: 'completed' };
            }),
            // ,
            retries: 10,
            // 👀 Backoff configuration
            backoff: {
                // 👀 Maximum number of seconds to wait between retries
                maxSeconds: 60,
                // 👀 Factor to increase the wait time between retries.
                // This sequence will be 2s, 4s, 8s, 16s, 32s, 60s... due to the maxSeconds limit
                factor: 2,
            },
        },
    ],
};
// !!
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet.worker('backoff-worker');
        yield worker.registerWorkflow(workflow);
        worker.start();
    });
}
main();
