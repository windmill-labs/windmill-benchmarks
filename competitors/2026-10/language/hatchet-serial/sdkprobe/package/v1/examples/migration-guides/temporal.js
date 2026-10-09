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
exports.orderWorkflow = exports.callModel = exports.syncCustomer = exports.weeklyReport = exports.packOrder = exports.processItem = exports.approvalFlow = exports.orderFollowUp = exports.chargeOrderWithRetries = exports.onboardingFlow = exports.sendFollowupEmail = exports.sendWelcomeEmail = exports.processOrder = exports.fulfillOrder = exports.chargeOrder = exports.validateOrder = exports.hatchet = void 0;
// > Hatchet worker
const v1_1 = require("../..");
exports.hatchet = v1_1.HatchetClient.init();
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield exports.hatchet.worker('order-worker', {
            workflows: [exports.validateOrder, exports.chargeOrder, exports.fulfillOrder, exports.processOrder],
            slots: 10,
        });
        yield worker.start();
    });
}
const payments = {
    charge: (orderId) => __awaiter(void 0, void 0, void 0, function* () { return 2500 + orderId.length; }),
};
const warehouse = {
    reserve: (orderId) => __awaiter(void 0, void 0, void 0, function* () { return orderId.length > 0; }),
    ship: (orderId) => __awaiter(void 0, void 0, void 0, function* () { return `shp_${orderId}`; }),
    pack: (item) => __awaiter(void 0, void 0, void 0, function* () { return item.length > 0; }),
};
const emails = {
    send: (address, template) => __awaiter(void 0, void 0, void 0, function* () { return address.includes('@') && template.length > 0; }),
};
const models = {
    complete: (prompt) => __awaiter(void 0, void 0, void 0, function* () { return prompt.toUpperCase(); }),
};
const crm = {
    sync: (customerId) => __awaiter(void 0, void 0, void 0, function* () { return customerId.length; }),
};
const reports = {
    build: (kind) => __awaiter(void 0, void 0, void 0, function* () { return kind.length; }),
};
exports.validateOrder = exports.hatchet.task({
    name: 'validate-order',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // > Hatchet context logging
        ctx.logger.info(`validating order ${input.orderId}`);
        // !!
        return { valid: yield warehouse.reserve(input.orderId) };
    }),
});
exports.chargeOrder = exports.hatchet.task({
    name: 'charge-order',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const amountCents = yield payments.charge(input.orderId);
        return { charged: amountCents > 0, amountCents };
    }),
});
// !!
exports.fulfillOrder = exports.hatchet.task({
    name: 'fulfill-order',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const shipmentId = yield warehouse.ship(input.orderId);
        return { fulfilled: true, shipmentId };
    }),
});
// > Hatchet workflow as durable task
exports.processOrder = exports.hatchet.durableTask({
    name: 'ProcessOrder',
    executionTimeout: '10m',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield exports.validateOrder.run(input);
        yield exports.chargeOrder.run(input);
        return exports.fulfillOrder.run(input);
    }),
});
// !!
exports.sendWelcomeEmail = exports.hatchet.task({
    name: 'send-welcome-email',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return ({
            sent: yield emails.send(input.email, 'welcome'),
        });
    }),
});
exports.sendFollowupEmail = exports.hatchet.task({
    name: 'send-followup-email',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return ({
            sent: yield emails.send(input.email, 'followup'),
        });
    }),
});
// > Hatchet durable task with sleep
exports.onboardingFlow = exports.hatchet.durableTask({
    name: 'OnboardingFlow',
    // The timeout has to cover the whole wall-clock span of the run, sleeps included.
    executionTimeout: '168h',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield exports.sendWelcomeEmail.run(input);
        yield ctx.sleepFor('72h');
        return exports.sendFollowupEmail.run(input);
    }),
});
// !!
function invoke(input) {
    return __awaiter(this, void 0, void 0, function* () {
        // > Hatchet task invocation
        const run = yield exports.processOrder.runNoWait(input);
        // It may be helpful to store this run id somewhere durable.
        const runId = yield run.getWorkflowRunId();
        const result = yield run.output;
        // !!
        return { runId, result };
    });
}
// > Hatchet retries and timeouts
exports.chargeOrderWithRetries = exports.hatchet.task({
    name: 'charge-order-with-retries',
    retries: 10,
    backoff: {
        // Factor to increase the wait time between retries.
        factor: 2,
        // Maximum number of seconds to wait between retries.
        maxSeconds: 10,
    },
    executionTimeout: '30s',
    scheduleTimeout: '10m',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const amountCents = yield payments.charge(input.orderId);
        return { charged: amountCents > 0, amountCents };
    }),
});
// !!
exports.orderFollowUp = exports.hatchet.durableTask({
    name: 'OrderFollowUp',
    executionTimeout: '48h',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // > Hatchet durable sleep
        yield ctx.sleepFor('24h');
        // !!
        return { sent: yield emails.send(`${input.orderId}@example.com`, 'order-followup') };
    }),
});
function grantApproval(input) {
    return __awaiter(this, void 0, void 0, function* () {
        // > Hatchet event push
        yield exports.hatchet.events.push('approval:granted', {
            correlationId: input.correlationId,
        });
        // !!
    });
}
// > Hatchet durable event wait
exports.approvalFlow = exports.hatchet.durableTask({
    name: 'ApprovalFlow',
    executionTimeout: '10m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // The expression is compiled as CEL, so correlate on an id that cannot contain a quote.
        yield ctx.waitForEvent('approval:granted', `input.correlationId == '${input.correlationId}'`);
        return exports.fulfillOrder.run(input);
    }),
});
exports.processItem = exports.hatchet.task({
    name: 'process-item',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return ({
            packed: yield warehouse.pack(input.item),
        });
    }),
});
// > Hatchet fan out children
exports.packOrder = exports.hatchet.durableTask({
    name: 'PackOrder',
    executionTimeout: '30m',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const results = yield Promise.all(input.items.map((item) => exports.processItem.run({ item })));
        return { packed: results.filter((result) => result.packed).length };
    }),
});
// > Hatchet cron declaration
exports.weeklyReport = exports.hatchet.workflow({
    name: 'weekly-report',
    on: {
        cron: '0 9 * * 1',
    },
});
exports.weeklyReport.task({
    name: 'generate',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { rows: yield reports.build(input.kind) };
    }),
});
// !!
function schedule() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Hatchet runtime schedules
        // A recurring schedule, created at runtime.
        yield exports.hatchet.crons.create('weekly-report', {
            name: 'weekly-report-acme',
            expression: '0 9 * * 1',
            input: { kind: 'weekly' },
        });
        // A one-shot future run.
        yield exports.hatchet.schedules.create('weekly-report', {
            triggerAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
            input: { kind: 'weekly' },
        });
        // !!
    });
}
// > Hatchet concurrency and rate limits
const v1_2 = require("../..");
// One in-flight run per customer, newest cancels the oldest.
exports.syncCustomer = exports.hatchet.workflow({
    name: 'SyncCustomer',
    concurrency: {
        expression: 'input.customerId',
        maxRuns: 1,
        limitStrategy: v1_2.ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
    },
});
exports.syncCustomer.task({
    name: 'sync',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { synced: yield crm.sync(input.customerId) };
    }),
});
// A global budget shared by every worker, not per-process.
exports.callModel = exports.hatchet.task({
    name: 'call-model',
    rateLimits: [
        {
            staticKey: 'openai',
            units: 1,
        },
    ],
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { completion: yield models.complete(input.prompt) };
    }),
});
// > Hatchet DAG workflow
exports.orderWorkflow = exports.hatchet.workflow({
    name: 'ProcessOrderDag',
});
const validate = exports.orderWorkflow.task({
    name: 'validate',
    executionTimeout: '30s',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { valid: yield warehouse.reserve(input.orderId) };
    }),
});
const charge = exports.orderWorkflow.task({
    name: 'charge',
    parents: [validate],
    executionTimeout: '30s',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const validated = yield ctx.parentOutput(validate);
        if (!validated.valid) {
            return { charged: false, amountCents: 0 };
        }
        const amountCents = yield payments.charge(input.orderId);
        return { charged: true, amountCents };
    }),
});
exports.orderWorkflow.task({
    name: 'fulfill',
    parents: [charge],
    executionTimeout: '30s',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const charged = yield ctx.parentOutput(charge);
        if (!charged.charged) {
            return { fulfilled: false, shipmentId: '' };
        }
        return { fulfilled: true, shipmentId: yield warehouse.ship(input.orderId) };
    }),
});
// !!
