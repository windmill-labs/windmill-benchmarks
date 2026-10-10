"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LogLevel = void 0;
exports.createEventsRpc = createEventsRpc;
exports.buildPushEventRequest = buildPushEventRequest;
exports.buildBulkPushEventRequest = buildBulkPushEventRequest;
const events_1 = require("../../protoc/events/events");
const events_pb_1 = require("../../protoc-es/events/events_pb");
const ts_proto_client_1 = require("../transport/ts-proto-client");
const apply_namespace_1 = require("../../util/apply-namespace");
const parent_run_context_vars_1 = require("../../v1/parent-run-context-vars");
/** The levels a task run log line is stored with. */
var LogLevel;
(function (LogLevel) {
    LogLevel["INFO"] = "INFO";
    LogLevel["WARN"] = "WARN";
    LogLevel["ERROR"] = "ERROR";
    LogLevel["DEBUG"] = "DEBUG";
})(LogLevel || (exports.LogLevel = LogLevel = {}));
/**
 * The `EventsService` client, every RPC of the generated `EventsServiceClient` interface,
 * served by a Connect client on the given transport. The Node and core clients both call
 * through it.
 */
function createEventsRpc(transport) {
    return (0, ts_proto_client_1.createTsProtoClient)(events_1.EventsServiceDefinition, events_pb_1.EventsService, transport);
}
/**
 * When an event is pushed from inside a task, the run that pushed it is recorded in the
 * event's metadata so the dashboard can link the two.
 */
function injectSourceInfo(metadata) {
    const ctx = parent_run_context_vars_1.parentRunContextManager.getContext();
    if (!(ctx === null || ctx === void 0 ? void 0 : ctx.parentId) || !(ctx === null || ctx === void 0 ? void 0 : ctx.parentTaskRunExternalId)) {
        return metadata;
    }
    return Object.assign(Object.assign({}, metadata), { hatchet__source_workflow_run_id: ctx.parentId, hatchet__source_step_run_id: ctx.parentTaskRunExternalId });
}
/**
 * One builder for both clients, so an event pushed from the core client is indistinguishable
 * to the engine from one pushed by the Node client.
 */
function buildPushEventRequest(type, input, options, namespace) {
    var _a;
    const enhancedMetadata = injectSourceInfo((_a = options.additionalMetadata) !== null && _a !== void 0 ? _a : {});
    return {
        key: (0, apply_namespace_1.applyNamespace)(type, namespace),
        payload: JSON.stringify(input),
        eventTimestamp: new Date(),
        additionalMetadata: Object.keys(enhancedMetadata).length > 0 ? JSON.stringify(enhancedMetadata) : undefined,
        priority: options.priority,
        scope: options.scope,
    };
}
/**
 * Builds the `BulkPush` request for events of one type. Each event's own metadata, priority
 * and scope win over the shared options.
 */
function buildBulkPushEventRequest(type, inputs, options, namespace) {
    const namespacedType = (0, apply_namespace_1.applyNamespace)(type, namespace);
    return {
        events: inputs.map((input) => {
            var _a, _b, _c, _d;
            const baseMeta = (_b = (_a = input.additionalMetadata) !== null && _a !== void 0 ? _a : options.additionalMetadata) !== null && _b !== void 0 ? _b : {};
            const enhanced = injectSourceInfo(baseMeta);
            return {
                key: namespacedType,
                payload: JSON.stringify(input.payload),
                eventTimestamp: new Date(),
                additionalMetadata: Object.keys(enhanced).length > 0 ? JSON.stringify(enhanced) : undefined,
                priority: (_c = input.priority) !== null && _c !== void 0 ? _c : options.priority,
                scope: (_d = input.scope) !== null && _d !== void 0 ? _d : options.scope,
            };
        }),
    };
}
