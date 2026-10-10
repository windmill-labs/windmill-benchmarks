"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createWorkflowsRpc = createWorkflowsRpc;
exports.createV1AdminRpc = createV1AdminRpc;
const workflows_1 = require("../../protoc/workflows/workflows");
const workflows_pb_1 = require("../../protoc-es/workflows/workflows_pb");
const workflows_2 = require("../../protoc/v1/workflows");
const workflows_pb_2 = require("../../protoc-es/v1/workflows_pb");
const ts_proto_client_1 = require("../transport/ts-proto-client");
/**
 * The `WorkflowService` client, every RPC of the generated `WorkflowServiceClient` interface,
 * served by a Connect client on the given transport.
 */
function createWorkflowsRpc(transport) {
    return (0, ts_proto_client_1.createTsProtoClient)(workflows_1.WorkflowServiceDefinition, workflows_pb_1.WorkflowService, transport);
}
/**
 * The v1 `AdminService` client, every RPC of the generated `AdminServiceClient` interface,
 * served by a Connect client on the given transport.
 */
function createV1AdminRpc(transport) {
    return (0, ts_proto_client_1.createTsProtoClient)(workflows_2.AdminServiceDefinition, workflows_pb_2.AdminService, transport);
}
