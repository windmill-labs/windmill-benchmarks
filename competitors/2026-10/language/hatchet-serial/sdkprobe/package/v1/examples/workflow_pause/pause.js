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
const pausableWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'pausable-workflow',
});
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Pause a workflow
        yield pausableWorkflow.pause({
            queueTTL: '24h',
            pausedWorkflowCronRunQueueBehavior: v1_1.WorkflowPauseScheduledCronRunQueueBehavior.DROP,
            pausedWorkflowScheduledRunQueueBehavior: v1_1.WorkflowPauseScheduledCronRunQueueBehavior.QUEUE,
        });
        // !!
        // > Unpause a workflow
        yield pausableWorkflow.unpause();
        // !!
    });
}
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => process.exit(0));
}
