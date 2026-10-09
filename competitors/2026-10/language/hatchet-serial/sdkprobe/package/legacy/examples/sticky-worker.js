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
const workflow_1 = require("../../workflow");
const hatchet = sdk_1.default.init();
// > StickyWorker
const workflow = {
    id: 'sticky-workflow',
    description: 'test',
    steps: [
        {
            name: 'step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const results = [];
                for (let i = 0; i < 50; i++) {
                    const result = yield ctx.spawnWorkflow(childWorkflow, {}, { sticky: true });
                    results.push(result.result());
                }
                console.log('Spawned 50 child workflows');
                console.log('Results:', yield Promise.all(results));
                return { step1: 'step1 results!' };
            }),
        },
    ],
};
// !!
// > StickyChild
const childWorkflow = {
    id: 'child-sticky-workflow',
    description: 'test',
    // 👀 Specify a sticky strategy when declaring the workflow
    sticky: workflow_1.StickyStrategy.HARD,
    steps: [
        {
            name: 'child-step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const workerId = ctx.worker.id();
                console.log(`1: Worker ID: ${workerId}`);
                return { childStep1: `${workerId}` };
            }),
        },
        {
            name: 'child-step2',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const workerId = ctx.worker.id();
                console.log(`2: Worker ID: ${workerId}`);
                return { childStep2: `${workerId}` };
            }),
        },
    ],
};
// !!
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker1 = yield hatchet.worker('sticky-worker-1');
        yield worker1.registerWorkflow(workflow);
        yield worker1.registerWorkflow(childWorkflow);
        worker1.start();
        const worker2 = yield hatchet.worker('sticky-worker-2');
        yield worker2.registerWorkflow(workflow);
        yield worker2.registerWorkflow(childWorkflow);
        worker2.start();
    });
}
main();
