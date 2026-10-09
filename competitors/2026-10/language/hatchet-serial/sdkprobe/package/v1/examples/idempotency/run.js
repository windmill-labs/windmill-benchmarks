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
const workflow_1 = require("./workflow");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > trigger
        const ref1 = yield workflow_1.idempotentTask.runNoWait({ id: '123' });
        let runId2;
        try {
            const ref2 = yield workflow_1.idempotentTask.runNoWait({ id: '123' });
            runId2 = yield ref2.getWorkflowRunId();
        }
        catch (e) {
            if (e instanceof v1_1.IdempotencyCollisionError) {
                console.log(`Run with external ID ${e.existingRunExternalId} already exists for this idempotency key`);
                runId2 = e.existingRunExternalId;
            }
            else {
                throw e;
            }
        }
        const res1 = yield ref1.result();
        console.log(`Result: ${JSON.stringify(res1)}, run ID: ${runId2}`);
        // !!
    });
}
main().catch(console.error);
