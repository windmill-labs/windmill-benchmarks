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
const sdk_1 = __importDefault(require("../../../../sdk"));
const hatchet = sdk_1.default.init();
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
const workflow = {
    id: 'concurrency-example',
    description: 'test',
    on: {
        event: 'concurrency:create',
    },
    concurrency: {
        name: 'user-concurrency',
        key: (ctx) => ctx.workflowInput().userId,
    },
    steps: [
        {
            name: 'step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const { data } = ctx.workflowInput();
                const { signal } = ctx.controller;
                if (signal.aborted) {
                    throw new Error('step1 was aborted');
                }
                console.log('starting step1 and waiting 5 seconds...', data);
                yield sleep(5000);
                if (signal.aborted) {
                    throw new Error('step1 was aborted');
                }
                // NOTE: the AbortController signal can be passed to many http libraries to cancel active requests
                // fetch(url, { signal })
                // axios.get(url, { signal })
                console.log('executed step1!');
                return { step1: `step1 results for ${data}!` };
            }),
        },
        {
            name: 'step2',
            parents: ['step1'],
            run: (ctx) => {
                console.log('executed step2 after step1 returned ', ctx.stepOutput('step1'));
                return { step2: 'step2 results!' };
            },
        },
    ],
};
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet.worker('example-worker');
        yield worker.registerWorkflow(workflow);
        worker.start();
    });
}
main();
