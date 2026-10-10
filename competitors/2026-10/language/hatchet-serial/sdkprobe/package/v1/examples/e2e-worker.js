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
/**
 * Shared e2e worker - registers all v1 example workflows for e2e tests.
 * Spawned by jest.e2e-global-setup and killed by jest.e2e-global-teardown.
 * Set HATCHET_CLIENT_WORKER_HEALTHCHECK_ENABLED=true and
 * HATCHET_CLIENT_WORKER_HEALTHCHECK_PORT before running.
 */
const hatchet_client_1 = require("./hatchet-client");
const workflow_1 = require("./bulk_fanout/workflow");
const workflow_2 = require("./bulk_operations/workflow");
const cancellation_workflow_1 = require("./cancellation/cancellation-workflow");
const complex_workflow_1 = require("./conditions/complex-workflow");
const workflow_3 = require("./concurrency_cancel_in_progress/workflow");
const workflow_4 = require("./concurrency_cancel_newest/workflow");
const workflow_5 = require("./concurrency_cancel_queued_except_newest/workflow");
const workflow_6 = require("./concurrency_cancel_queued_except_oldest/workflow");
const workflow_7 = require("./concurrency_multiple_keys/workflow");
const workflow_8 = require("./concurrency_shared/workflow");
const workflow_9 = require("./concurrency_workflow_level/workflow");
const workflow_10 = require("./dag/workflow");
const workflow_11 = require("./durable/workflow");
const workflow_12 = require("./durable_event/workflow");
const workflow_13 = require("./durable_eviction/workflow");
const workflow_14 = require("./durable_callback_ordering/workflow");
const workflow_15 = require("./durable_sleep/workflow");
const workflow_16 = require("./logger/workflow");
const workflow_17 = require("./non_retryable/workflow");
const workflow_18 = require("./on_failure/workflow");
const workflow_19 = require("./idempotency/workflow");
const workflow_20 = require("./on_event/workflow");
const workflow_21 = require("./return_exceptions/workflow");
const workflow_22 = require("./run_details/workflow");
const e2e_workflows_1 = require("./simple/e2e-workflows");
const workflow_23 = require("./batch_assign/workflow");
const workflow_24 = require("./streaming/workflow");
const workflow_25 = require("./subscribe_to_stream/workflow");
const workflow_26 = require("./timeout/workflow");
const workflow_27 = require("./webhooks/workflow");
const workflow_28 = require("./child_index/workflow");
const workflow_29 = require("./support_agent/workflow");
const workflow_30 = require("./welcome_email/workflow");
const workflow_31 = require("./pdf_pipeline/workflow");
const workflows = [
    workflow_1.bulkChild,
    workflow_1.bulkParentWorkflow,
    workflow_2.bulkReplayTest1,
    workflow_2.bulkReplayTest2,
    workflow_2.bulkReplayTest3,
    cancellation_workflow_1.cancellationWorkflow,
    complex_workflow_1.taskConditionWorkflow,
    workflow_3.concurrencyCancelInProgressWorkflow,
    workflow_4.concurrencyCancelNewestWorkflow,
    workflow_5.concurrencyCancelQueuedExceptNewestWorkflow,
    workflow_6.concurrencyCancelQueuedExceptOldestWorkflow,
    workflow_7.concurrencyMultipleKeysWorkflow,
    workflow_8.concurrencySharedWorkflowA,
    workflow_8.concurrencySharedWorkflowB,
    workflow_8.concurrencySharedMixedWorkflow,
    workflow_9.concurrencyWorkflowLevelWorkflow,
    workflow_10.dag,
    workflow_11.durableWorkflow,
    workflow_11.waitForSleepTwice,
    workflow_11.spawnChildTask,
    workflow_11.durableWithSpawn,
    workflow_11.durableWithBulkSpawn,
    workflow_11.durableSleepEventSpawn,
    workflow_11.durableWithExplicitSpawn,
    workflow_11.durableNonDeterminism,
    workflow_11.durableReplayReset,
    workflow_11.dagChildWorkflow,
    workflow_11.durableSpawnDag,
    workflow_11.waitForEventLookback,
    workflow_11.waitForOrEventLookback,
    workflow_11.waitForTwoEventsSecondPushedFirst,
    workflow_11.errorRaisingTask,
    workflow_11.errorRaisingDurableParent,
    workflow_12.durableEvent,
    workflow_12.durableEventWithFilter,
    workflow_14.callbackOrderingLeaf,
    workflow_14.callbackOrderingMid,
    workflow_14.callbackOrderingRoot,
    workflow_15.durableSleep,
    workflow_13.evictableSleep,
    workflow_13.evictableWaitForEvent,
    workflow_13.evictableMemoThenWaitForEvent,
    workflow_13.evictableChildSpawn,
    workflow_13.multipleEviction,
    workflow_13.nonEvictableSleep,
    workflow_13.childTask,
    workflow_13.bulkChildTask,
    workflow_13.evictableChildBulkSpawn,
    (0, workflow_16.createLoggingWorkflow)(hatchet_client_1.hatchet),
    workflow_17.nonRetryableWorkflow,
    workflow_18.failureWorkflow,
    workflow_19.idempotentTask,
    workflow_19.idempotentTaskShortWindow,
    workflow_20.lower,
    workflow_21.returnExceptionsTask,
    workflow_22.runDetailTestWorkflow,
    e2e_workflows_1.helloWorld,
    e2e_workflows_1.helloWorldDurable,
    workflow_24.streamingTask,
    workflow_25.dagStream,
    workflow_25.longStream,
    workflow_26.timeoutTask,
    workflow_26.refreshTimeoutTask,
    workflow_27.webhookWorkflow,
    workflow_28.childIndexChild,
    workflow_28.childIndexParent,
    workflow_28.scenarioTask,
    workflow_28.orchestratorTask,
    workflow_29.supportAgent,
    workflow_29.triageTicket,
    workflow_29.generateReply,
    workflow_29.escalateTicket,
    workflow_30.welcomeEmail,
    workflow_31.pdfPipeline,
    workflow_23.batchSimple,
    workflow_23.batchKeyed,
    workflow_23.batchKeyedFailable,
    workflow_23.batchKeyedInterval,
    workflow_23.batchLarge,
    workflow_23.batchSingle,
    workflow_23.batchOrdered,
    workflow_23.batchBroadcast,
    workflow_23.batchCancel,
    workflow_23.child,
    workflow_23.childBatch,
    workflow_23.batchChildSpawn,
    workflow_23.batchChildBatchSpawn,
];
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet_client_1.hatchet.worker('e2e-test-worker', {
            workflows,
            slots: 100,
        });
        void worker.start();
        yield worker.waitUntilReady(30000);
        console.log('[e2e-worker] Worker registered and ready');
        yield new Promise(() => { });
    });
}
main().catch((err) => {
    console.error('e2e-worker failed:', err);
    process.exit(1);
});
