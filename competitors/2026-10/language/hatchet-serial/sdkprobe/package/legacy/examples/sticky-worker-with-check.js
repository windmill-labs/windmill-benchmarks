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
const workflow = {
    id: 'sticky-workflow',
    description: 'test',
    steps: [
        {
            name: 'step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const results = [];
                const count = 57;
                hardChildWorkerId = undefined; // we reset this - if we run this multiple times at the same time it will break
                for (let i = 0; i < count; i++) {
                    const result = yield ctx.spawnWorkflow(childWorkflow, {}, { sticky: true });
                    results.push(result.output);
                    const result2 = yield ctx.spawnWorkflow(softChildWorkflow, {}, { sticky: true });
                    results.push(result2.output);
                }
                console.log('Spawned ', count, ' child workflows of each type');
                console.log('Results:', yield Promise.all(results));
                return { step1: 'step1 results!' };
            }),
        },
    ],
};
let hardChildWorkerId;
const childWorkflow = {
    id: 'child-sticky-workflow',
    description: 'test',
    sticky: workflow_1.StickyStrategy.HARD,
    steps: [
        {
            name: 'child-step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const workerId = ctx.worker.id();
                console.log(`1: Worker ID: ${workerId}`);
                if (!hardChildWorkerId) {
                    hardChildWorkerId = workerId;
                }
                else if (hardChildWorkerId !== workerId) {
                    throw new Error(`Expected worker ID ${hardChildWorkerId} but got ${workerId}`);
                }
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
const softChildWorkflow = {
    id: 'child-sticky-workflow-soft',
    description: 'test',
    sticky: workflow_1.StickyStrategy.SOFT,
    steps: [
        {
            name: 'child-step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const workerId = ctx.worker.id();
                console.log(`1: Worker ID: ${workerId}`);
                return { childStep1: `SOFT ${workerId}` };
            }),
        },
        {
            name: 'child-step2',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const workerId = ctx.worker.id();
                console.log(`2: Worker ID: ${workerId}`);
                return { childStep2: `SOFT ${workerId}` };
            }),
        },
    ],
};
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker1 = yield hatchet.worker('sticky-worker-1');
        yield worker1.registerWorkflow(workflow);
        yield worker1.registerWorkflow(childWorkflow);
        yield worker1.registerWorkflow(softChildWorkflow);
        worker1.start();
        const worker2 = yield hatchet.worker('sticky-worker-2');
        yield worker2.registerWorkflow(workflow);
        yield worker2.registerWorkflow(childWorkflow);
        yield worker2.registerWorkflow(softChildWorkflow);
        worker2.start();
    });
}
main();
