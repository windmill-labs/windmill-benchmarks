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
exports.createWorkerContextRuntime = createWorkerContextRuntime;
/**
 * `ContextRuntime` for a worker process: adapts the `HatchetClient` and `InternalWorker`
 * a task runs on to the seam `Context` consumes.
 */
const dispatcher_1 = require("../../../protoc/dispatcher");
// The event client's LogLevel enum has the same string values as the logger's LogLevel
// union; casting keeps the event client (and its gRPC imports) out of this module's
// runtime graph.
function toEventLogLevel(level) {
    if (level === undefined || level === 'OFF') {
        return undefined;
    }
    return level;
}
function createWorkerContextRuntime(client, worker) {
    return {
        client,
        get namespace() {
            return client.config.namespace;
        },
        logger: (name) => client.config.logger(name, client.config.log_level),
        cancelRun: (taskRunExternalId) => __awaiter(this, void 0, void 0, function* () {
            yield client.runs.cancel({ ids: [taskRunExternalId] });
        }),
        cancelBatch: (_a) => __awaiter(this, [_a], void 0, function* ({ workerId, jobId, actionId, batchId, memberIds }) {
            yield client.dispatcher.sendBatchActionEvent({
                workerId,
                jobId,
                actionId,
                batchId,
                eventTimestamp: new Date(),
                eventType: dispatcher_1.StepActionEventType.STEP_EVENT_TYPE_CANCELLED,
                items: memberIds.map((id) => ({ taskRunExternalId: id, eventPayload: '' })),
            });
        }),
        putLog: (taskRunExternalId, message, level, retryCount, extra) => __awaiter(this, void 0, void 0, function* () {
            yield client.events.putLog(taskRunExternalId, message, toEventLogLevel(level), retryCount, extra);
        }),
        refreshTimeout: (taskRunExternalId, incrementBy) => __awaiter(this, void 0, void 0, function* () {
            yield client.dispatcher.refreshTimeout(incrementBy, taskRunExternalId);
        }),
        releaseSlot: (taskRunExternalId) => __awaiter(this, void 0, void 0, function* () {
            yield client.dispatcher.client.releaseSlot({ taskRunExternalId });
        }),
        putStream: (taskRunExternalId, data, index) => __awaiter(this, void 0, void 0, function* () {
            yield client.events.putStream(taskRunExternalId, data, index);
        }),
        runWorkflow: (workflowName, input, options) => client.admin.runWorkflow(workflowName, input, options),
        runWorkflows: (runs) => client.admin.runWorkflows(runs),
        workerId: () => worker.workerId,
        hasWorkflow: (workflowName) => !!worker.workflow_registry.find((workflow) => 'id' in workflow ? workflow.id === workflowName : workflow.name === workflowName),
        workerLabels: () => worker.labels,
        upsertWorkerLabels: (labels) => worker.upsertLabels(labels),
    };
}
