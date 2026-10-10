"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.nonRetryableWorkflow = void 0;
const task_1 = require("../../task");
const hatchet_client_1 = require("../hatchet-client");
exports.nonRetryableWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'no-retry-workflow',
});
// > Non-retrying task
const shouldNotRetry = exports.nonRetryableWorkflow.task({
    name: 'should-not-retry',
    fn: () => {
        throw new task_1.NonRetryableError('This task should not retry');
    },
    retries: 1,
});
// !!
// Create a task that should retry
const shouldRetryWrongErrorType = exports.nonRetryableWorkflow.task({
    name: 'should-retry-wrong-error-type',
    fn: () => {
        throw new Error('This task should not retry');
    },
    retries: 1,
});
const shouldNotRetrySuccessfulTask = exports.nonRetryableWorkflow.task({
    name: 'should-not-retry-successful-task',
    fn: () => { },
});
