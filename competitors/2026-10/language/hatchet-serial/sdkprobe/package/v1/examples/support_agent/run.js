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
const workflow_1 = require("./workflow");
// > Trigger the workflow
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const input = {
            ticketId: 'ticket-42',
            customerEmail: 'alice@example.com',
            subject: 'Login broken',
            body: "I can't log in since this morning.",
        };
        // Start the support agent workflow
        const ref = yield workflow_1.supportAgent.runNoWait(input);
        const runId = yield ref.getWorkflowRunId();
        console.log(`Started workflow run: ${runId}`);
        // Push a customer reply event (scoped to this ticket)
        console.log('Pushing customer reply event...');
        yield hatchet_client_1.hatchet.events.push(workflow_1.REPLY_EVENT_KEY, { message: 'I cleared my cookies and it works now. Thanks!' }, { scope: input.ticketId });
        // Wait for the workflow to complete
        const result = yield ref.output;
        console.log('Workflow completed:', result);
    });
}
// !!
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => {
        process.exit(0);
    });
}
