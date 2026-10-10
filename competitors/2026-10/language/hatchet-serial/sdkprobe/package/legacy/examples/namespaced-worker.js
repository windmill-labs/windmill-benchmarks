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
const hatchet = sdk_1.default.init({
    namespace: 'example-namespace',
});
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
const workflow = {
    id: 'simple-workflow',
    description: 'test',
    on: {
        event: 'user:create',
    },
    steps: [
        {
            name: 'step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                console.log('starting step1 with the following input', ctx.workflowInput());
                console.log('waiting 5 seconds...');
                yield sleep(5000);
                console.log('executed step1!');
                return { step1: 'step1 results!' };
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
