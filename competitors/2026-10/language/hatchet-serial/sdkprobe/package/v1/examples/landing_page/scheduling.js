"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const flow_control_1 = require("./flow-control");
// > Schedules and Crons
const tomorrow = new Date(Date.now() + 1000 * 60 * 60 * 24);
const scheduled = flow_control_1.simple.schedule(tomorrow, {
    Message: 'Hello, World!',
});
const cron = flow_control_1.simple.cron('every-day', '0 0 * * *', {
    Message: 'Hello, World!',
});
// !!
