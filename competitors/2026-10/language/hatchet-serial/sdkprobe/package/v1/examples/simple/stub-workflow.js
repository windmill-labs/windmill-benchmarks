"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.simple = void 0;
// > Declaring an External Workflow Reference
const hatchet_client_1 = require("../hatchet-client");
// declare the workflow with the same name as the
// workflow name on the worker
exports.simple = hatchet_client_1.hatchet.workflow({
    name: 'simple',
});
// you can use all the same run methods on the stub
// with full type-safety
exports.simple.run({ Message: 'Hello, World!' });
exports.simple.runNoWait({ Message: 'Hello, World!' });
exports.simple.schedule(new Date(), { Message: 'Hello, World!' });
exports.simple.cron('my-cron', '0 0 * * *', { Message: 'Hello, World!' });
// !!
