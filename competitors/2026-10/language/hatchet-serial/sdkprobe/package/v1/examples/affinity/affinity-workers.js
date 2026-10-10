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
Object.defineProperty(exports, "__esModule", { value: true });
const v1_1 = require("../..");
const hatchet_client_1 = require("../hatchet-client");
// > AffinityWorkflow
const workflow = hatchet_client_1.hatchet.workflow({
    name: 'affinity-workflow',
    description: 'test',
});
workflow.task({
    name: 'step1',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const results = [];
        for (let i = 0; i < 50; i++) {
            const result = yield childWorkflow.run({});
            results.push(result);
        }
        ctx.logger.info('Spawned 50 child workflows');
        ctx.logger.info('Results', { results });
        return { step1: 'step1 results!' };
    }),
});
// !!
// > Task with labels
const childWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'child-affinity-workflow',
    description: 'test',
});
childWorkflow.task({
    name: 'child-step1',
    desiredWorkerLabels: {
        model: {
            value: 'xyz',
            required: true,
        },
    },
    fn: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
        return { childStep1: 'childStep1 results!' };
    }),
});
// !!
childWorkflow.task({
    name: 'child-step2',
    desiredWorkerLabels: {
        memory: {
            value: 512,
            required: true,
            comparator: v1_1.WorkerLabelComparator.LESS_THAN,
        },
    },
    fn: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
        return { childStep2: 'childStep2 results!' };
    }),
});
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > AffinityWorker
        const worker1 = yield hatchet_client_1.hatchet.worker('affinity-worker-1', {
            labels: {
                model: 'abc',
                memory: 1024,
            },
        });
        // !!
        yield worker1.registerWorkflow(workflow);
        yield worker1.registerWorkflow(childWorkflow);
        worker1.start();
        const worker2 = yield hatchet_client_1.hatchet.worker('affinity-worker-2', {
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
