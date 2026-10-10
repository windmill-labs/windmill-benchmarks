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
const v1_1 = require("../../v1");
const sdk_1 = __importDefault(require("../../sdk"));
const hatchet = sdk_1.default.init();
// > AffinityWorkflow
const workflow = {
    id: 'affinity-workflow',
    description: 'test',
    steps: [
        {
            name: 'step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const results = [];
                for (let i = 0; i < 50; i++) {
                    const result = yield ctx.spawnWorkflow(childWorkflow.id, {});
                    results.push(result.output);
                }
                console.log('Spawned 50 child workflows');
                console.log('Results:', yield Promise.all(results));
                return { step1: 'step1 results!' };
            }),
        },
    ],
};
// !!
const childWorkflow = {
    id: 'child-affinity-workflow',
    description: 'test',
    steps: [
        {
            name: 'child-step1',
            worker_labels: {
                model: {
                    value: 'xyz',
                    required: true,
                },
            },
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                console.log('starting child-step1 with the following input', ctx.workflowInput());
                return { childStep1: 'childStep1 results!' };
            }),
        },
        {
            name: 'child-step2',
            worker_labels: {
                memory: {
                    value: 512,
                    required: true,
                    comparator: v1_1.WorkerLabelComparator.LESS_THAN,
                },
            },
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                console.log('starting child-step2 with the following input', ctx.workflowInput());
                return { childStep2: 'childStep2 results!' };
            }),
        },
    ],
};
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > AffinityWorker
        const worker1 = yield hatchet.worker('affinity-worker-1', {
            labels: {
                model: 'abc',
                memory: 1024,
            },
        });
        // !!
        yield worker1.registerWorkflow(workflow);
        yield worker1.registerWorkflow(childWorkflow);
        worker1.start();
        const worker2 = yield hatchet.worker('affinity-worker-2', {
            labels: {
                model: 'xyz',
                memory: 512,
            },
        });
        yield worker2.registerWorkflow(workflow);
        yield worker2.registerWorkflow(childWorkflow);
        worker2.start();
    });
}
main();
