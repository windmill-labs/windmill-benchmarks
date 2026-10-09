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
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Create a Scheduled Run
        const runAt = new Date(new Date().setHours(12, 0, 0, 0) + 24 * 60 * 60 * 1000);
        const scheduled = yield workflow_1.simple.schedule(runAt, {
            Message: 'hello',
        });
        // 👀 Get the scheduled run ID of the workflow
        // it may be helpful to store the scheduled run ID of the workflow
        // in a database or other persistent storage for later use
        const scheduledRunId = scheduled.metadata.id;
        console.log(scheduledRunId);
        // !!
        // > Reschedule a Scheduled Run
        yield hatchet_client_1.hatchet.scheduled.update(scheduledRunId, {
            triggerAt: new Date(Date.now() + 60 * 60 * 1000),
        });
        // !!
        // > Delete a Scheduled Run
        yield hatchet_client_1.hatchet.scheduled.delete(scheduled);
        // !!
        // > List Scheduled Runs
        const scheduledRuns = yield hatchet_client_1.hatchet.scheduled.list({
            workflow: workflow_1.simple,
        });
        console.log(scheduledRuns);
        // !!
        // > Bulk Delete Scheduled Runs
        yield hatchet_client_1.hatchet.scheduled.bulkDelete({
            scheduledRuns: [scheduledRunId],
        });
        // !!
        // > Bulk Reschedule Scheduled Runs
        yield hatchet_client_1.hatchet.scheduled.bulkUpdate([
            { scheduledRun: scheduledRunId, triggerAt: new Date(Date.now() + 2 * 60 * 60 * 1000) },
        ]);
        // !!
    });
}
if (require.main === module) {
    main();
}
