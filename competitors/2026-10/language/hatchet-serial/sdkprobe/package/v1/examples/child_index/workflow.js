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
exports.orchestratorTask = exports.scenarioTask = exports.childIndexParent = exports.childIndexChild = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
exports.childIndexChild = hatchet_client_1.hatchet.task({
    name: 'child-index-child',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { tag: input.tag };
    }),
});
exports.childIndexParent = hatchet_client_1.hatchet.workflow({
    name: 'child-index-parent',
});
exports.childIndexParent.task({
    name: 'mixed-spawn',
    executionTimeout: '3m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const { n } = input;
        const ctxResults = [];
        for (let i = 0; i < n; i++) {
            const result = yield ctx.runChild(exports.childIndexChild, { tag: `ctx-${i}` });
            ctxResults.push(result.tag);
        }
        const refs = yield Promise.all(Array.from({ length: n }, (_, i) => exports.childIndexChild.runNoWait({ tag: `rnw-${i}` })));
        const runIds = yield Promise.all(refs.map((r) => r.runId));
        const outputs = yield Promise.all(refs.map((r) => r.output));
        const rnwResults = outputs.map((o) => o.tag);
        return {
            ctxResults,
            rnwResults,
            runIds,
        };
    }),
});
function buildTree() {
    // Root → Fund → Portfolio → Portfolio → Project structure
    // ~30 total nodes to keep the test manageable
    return {
        id: 'fund-root',
        children: [
            {
                id: 'fund-a',
                children: [
                    {
                        id: 'portfolio-a1',
                        children: [
                            { id: 'project-a1a', children: [] },
                            { id: 'project-a1b', children: [] },
                            { id: 'project-a1c', children: [] },
                        ],
                    },
                    {
                        id: 'portfolio-a2',
                        children: [
                            { id: 'project-a2a', children: [] },
                            { id: 'project-a2b', children: [] },
                        ],
                    },
                ],
            },
            {
                id: 'fund-b',
                children: [
                    {
                        id: 'portfolio-b1',
                        children: [
                            { id: 'project-b1a', children: [] },
                            { id: 'project-b1b', children: [] },
                            { id: 'project-b1c', children: [] },
                            { id: 'project-b1d', children: [] },
                        ],
                    },
                    {
                        id: 'portfolio-b2',
                        children: [
                            { id: 'project-b2a', children: [] },
                            { id: 'project-b2b', children: [] },
                        ],
                    },
                ],
            },
            {
                id: 'fund-c',
                children: [
                    {
                        id: 'portfolio-c1',
                        children: [
                            { id: 'project-c1a', children: [] },
                            { id: 'project-c1b', children: [] },
                            { id: 'project-c1c', children: [] },
                        ],
                    },
                ],
            },
        ],
    };
}
function countNodes(node) {
    return 1 + node.children.reduce((sum, c) => sum + countNodes(c), 0);
}
exports.scenarioTask = hatchet_client_1.hatchet.task({
    name: 'child-index-scenario',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(100);
        return { scenarioId: input.scenarioId, status: 'completed' };
    }),
});
exports.orchestratorTask = hatchet_client_1.hatchet.task({
    name: 'child-index-orchestrator',
    executionTimeout: '5m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const tree = buildTree();
        const totalExpected = countNodes(tree);
        const collectedRunIds = [];
        const collectedScenarios = [];
        // Recursive bottom-up traversal — mirrors Evan's runChildScenarioFn pattern.
        // Process children first, wait for completion, then process this node.
        function processNode(node) {
            return __awaiter(this, void 0, void 0, function* () {
                for (const child of node.children) {
                    yield processNode(child);
                }
                const runRef = yield exports.scenarioTask.runNoWait({
                    scenarioId: node.id,
                });
                const runId = yield runRef.runId;
                collectedRunIds.push(runId);
                collectedScenarios.push(node.id);
                yield runRef.output;
            });
        }
        yield processNode(tree);
        const uniqueRunIds = new Set(collectedRunIds);
        return {
            total: collectedRunIds.length,
            expected: totalExpected,
            uniqueRunIds: uniqueRunIds.size,
            hasDuplicates: uniqueRunIds.size !== collectedRunIds.length,
            scenarios: collectedScenarios,
        };
    }),
});
