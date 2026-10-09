"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildTriggerWorkflowRequest = buildTriggerWorkflowRequest;
exports.isAlreadyExists = isAlreadyExists;
exports.extractExistingRunId = extractExistingRunId;
exports.extractBulkTriggerCollision = extractBulkTriggerCollision;
const connect_1 = require("@connectrpc/connect");
const idempotency_collision_error_1 = require("../../util/errors/idempotency-collision-error");
const bulk_trigger_idempotency_collision_error_1 = require("../../util/errors/bulk-trigger-idempotency-collision-error");
const workflows_pb_1 = require("../../protoc-es/v1/workflows_pb");
const apply_namespace_1 = require("../../util/apply-namespace");
/**
 * One builder for both clients, so a run triggered from the core client is indistinguishable
 * to the engine from one triggered by the Node client.
 */
function buildTriggerWorkflowRequest(workflowName, input, options, namespace) {
    var _a;
    const opts = options !== null && options !== void 0 ? options : {};
    return {
        name: (0, apply_namespace_1.applyNamespace)(workflowName, namespace).toLowerCase(),
        input: JSON.stringify(input),
        parentId: opts.parentId,
        parentTaskRunExternalId: (_a = opts.parentTaskRunExternalId) !== null && _a !== void 0 ? _a : opts.parentStepRunId,
        childIndex: opts.childIndex,
        childKey: opts.childKey,
        additionalMetadata: opts.additionalMetadata
            ? JSON.stringify(opts.additionalMetadata)
            : undefined,
        desiredWorkerId: opts.desiredWorkerId,
        priority: opts.priority,
        desiredWorkerLabels: opts.desiredWorkerLabels
            ? convertDesiredWorkerLabels(opts.desiredWorkerLabels)
            : {},
    };
}
function convertDesiredWorkerLabels(labels) {
    return Object.fromEntries(Object.entries(labels).map(([key, label]) => [
        key,
        {
            strValue: typeof label.value === 'string' ? label.value : undefined,
            intValue: typeof label.value === 'number' ? label.value : undefined,
            required: label.required,
            weight: label.weight,
            comparator: label.comparator,
        },
    ]));
}
/**
 * The engine answers a trigger whose idempotency key is already taken with `ALREADY_EXISTS`
 * and attaches the collision details to the status. The Connect client decodes those details
 * from the trailer, so the error carries everything needed to build the SDK's collision errors.
 */
function isAlreadyExists(e) {
    return e instanceof connect_1.ConnectError && e.code === connect_1.Code.AlreadyExists;
}
function extractExistingRunId(e) {
    var _a;
    const [detail] = e.findDetails(workflows_pb_1.IdempotencyCollisionErrorSchema);
    return (_a = detail === null || detail === void 0 ? void 0 : detail.existingRunExternalId) !== null && _a !== void 0 ? _a : '';
}
function extractBulkTriggerCollision(e) {
    const [detail] = e.findDetails(workflows_pb_1.BulkTriggerIdempotencyCollisionErrorSchema);
    if (!detail)
        return null;
    return new bulk_trigger_idempotency_collision_error_1.BulkTriggerIdempotencyCollisionError(detail.successfulWorkflowRunExternalIds, detail.collisions.map((c) => new idempotency_collision_error_1.IdempotencyCollisionError(c.existingRunExternalId)));
}
