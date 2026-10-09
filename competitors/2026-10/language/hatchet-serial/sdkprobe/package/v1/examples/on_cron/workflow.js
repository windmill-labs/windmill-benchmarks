"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.onCron = void 0;
const hatchet_client_1 = require("../hatchet-client");
// > Run Workflow on Cron
exports.onCron = hatchet_client_1.hatchet.workflow({
    name: 'on-cron-workflow',
    on: {
        // 👀 add a cron expression to run the workflow every 15 minutes
        cron: '*/15 * * * *',
    },
});
// !!
exports.onCron.task({
    name: 'job',
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
