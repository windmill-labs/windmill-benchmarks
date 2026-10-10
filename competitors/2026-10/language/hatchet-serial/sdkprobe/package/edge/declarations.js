"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.declarations = declarations;
const declaration_1 = require("../v1/declaration");
/**
 * Returns the declaration factories bound to no client, so tasks can be declared where
 * a `HatchetClient` cannot exist (edge runtimes, or code that must not read
 * `HATCHET_CLIENT_TOKEN`).
 *
 * ```typescript
 * const { task, durableTask, workflow, batchTask } = declarations();
 * ```
 */
function declarations() {
    return {
        task: (options) => (0, declaration_1.CreateTaskWorkflow)(options),
        durableTask: (options) => (0, declaration_1.CreateDurableTaskWorkflow)(options),
        workflow: (options) => (0, declaration_1.CreateWorkflow)(options),
        batchTask: (options) => (0, declaration_1.CreateBatchTaskWorkflow)(options),
    };
}
