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
const hatchet_client_1 = require("../hatchet-client");
// > Enqueuing a Workflow (Fire and Forget)
const workflow_1 = require("./workflow");
// ...
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // 👀 Enqueue the workflow
        const run = yield workflow_1.simple.runNoWait({
            Message: 'hello',
        });
        // 👀 Get the run ID of the workflow
        const runId = yield run.getWorkflowRunId();
        // It may be helpful to store the run ID of the workflow
        // in a database or other persistent storage for later use
        console.log(runId);
        // !!
        // > Subscribing to results
        // the return object of the enqueue method is a WorkflowRunRef which includes a listener for the result of the workflow
        const result = yield run.output;
        console.log(result);
        // if you need to subscribe to the result of the workflow at a later time, you can use the runRef method and the stored runId
        const ref = hatchet_client_1.hatchet.runRef(runId);
        const result2 = yield ref.output;
        console.log(result2);
        // !!
    });
}
if (require.main === module) {
    main();
}
