"use strict";
/**
 * The edge entry point: everything needed to declare Hatchet tasks and run them from a
 * runtime that has no Hatchet client, such as a Cloudflare Worker or a Vercel Function.
 *
 * Nothing exported here imports from Node. `scripts/check-edge-entry.mjs` bundles this
 * module for a browser-like target and fails the build if a Node builtin sneaks in.
 *
 * ```typescript
 * import { declarations, workflowToProto } from './index.js';
 *
 * const { task } = declarations();
 * const greet = task({ name: 'greet', fn: (input: { name: string }) => ({ hi: input.name }) });
 * const request = workflowToProto(greet, { namespace: 'prod_' });
 * ```
 * @module Edge
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.declarations = exports.DurableTaskResponse = exports.DurableTaskRequest = exports.AssignedAction = exports.CreateWorkflowVersionRequest = exports.workflowToProto = exports.onFailureTaskName = exports.normalizeWorkflowDefinition = exports.ON_SUCCESS_TASK_NAME = exports.ON_FAILURE_TASK_NAME = exports.supportsEviction = exports.MinEngineVersion = exports.DEFAULT_DURABLE_TASK_EVICTION_POLICY = exports.throwIfAborted = exports.rethrowIfAborted = exports.isAbortError = exports.createAbortError = exports.AbortError = exports.isTaskRunTerminatedError = exports.TaskRunTerminatedError = exports.NonDeterminismError = exports.toHatchetError = exports.getErrorMessage = exports.HatchetError = exports.parentRunContextManager = exports.ParentRunContextManager = exports.LogLevelEnum = exports.Logger = exports.isContextRuntime = exports.computeMemoKey = exports.DurableContext = exports.ContextWorker = exports.Context = exports.workflowNameFromAction = exports.createActionId = exports.createAction = exports.applyNamespace = exports.taskConditionsToPb = exports.conditionsToPb = exports.WorkflowDeclaration = exports.TaskWorkflowDeclaration = exports.StickyStrategy = exports.Priority = exports.CreateWorkflow = exports.CreateTaskWorkflow = exports.CreateDurableTaskWorkflow = exports.CreateBatchTaskWorkflow = exports.BaseWorkflowDeclaration = void 0;
// Declarations
var declaration_1 = require("../v1/declaration");
Object.defineProperty(exports, "BaseWorkflowDeclaration", { enumerable: true, get: function () { return declaration_1.BaseWorkflowDeclaration; } });
Object.defineProperty(exports, "CreateBatchTaskWorkflow", { enumerable: true, get: function () { return declaration_1.CreateBatchTaskWorkflow; } });
Object.defineProperty(exports, "CreateDurableTaskWorkflow", { enumerable: true, get: function () { return declaration_1.CreateDurableTaskWorkflow; } });
Object.defineProperty(exports, "CreateTaskWorkflow", { enumerable: true, get: function () { return declaration_1.CreateTaskWorkflow; } });
Object.defineProperty(exports, "CreateWorkflow", { enumerable: true, get: function () { return declaration_1.CreateWorkflow; } });
Object.defineProperty(exports, "Priority", { enumerable: true, get: function () { return declaration_1.Priority; } });
Object.defineProperty(exports, "StickyStrategy", { enumerable: true, get: function () { return declaration_1.StickyStrategy; } });
Object.defineProperty(exports, "TaskWorkflowDeclaration", { enumerable: true, get: function () { return declaration_1.TaskWorkflowDeclaration; } });
Object.defineProperty(exports, "WorkflowDeclaration", { enumerable: true, get: function () { return declaration_1.WorkflowDeclaration; } });
// Tasks, types, durations and conditions
__exportStar(require("../v1/task"), exports);
__exportStar(require("../v1/types"), exports);
__exportStar(require("../v1/client/duration"), exports);
__exportStar(require("../v1/conditions"), exports);
var transformer_1 = require("../v1/conditions/transformer");
Object.defineProperty(exports, "conditionsToPb", { enumerable: true, get: function () { return transformer_1.conditionsToPb; } });
Object.defineProperty(exports, "taskConditionsToPb", { enumerable: true, get: function () { return transformer_1.taskConditionsToPb; } });
// Naming
var apply_namespace_1 = require("../util/apply-namespace");
Object.defineProperty(exports, "applyNamespace", { enumerable: true, get: function () { return apply_namespace_1.applyNamespace; } });
var action_1 = require("../clients/dispatcher/action");
Object.defineProperty(exports, "createAction", { enumerable: true, get: function () { return action_1.createAction; } });
Object.defineProperty(exports, "createActionId", { enumerable: true, get: function () { return action_1.createActionId; } });
Object.defineProperty(exports, "workflowNameFromAction", { enumerable: true, get: function () { return action_1.workflowNameFromAction; } });
// Contexts and the runtime seams they depend on
var context_1 = require("../v1/client/worker/context");
Object.defineProperty(exports, "Context", { enumerable: true, get: function () { return context_1.Context; } });
Object.defineProperty(exports, "ContextWorker", { enumerable: true, get: function () { return context_1.ContextWorker; } });
Object.defineProperty(exports, "DurableContext", { enumerable: true, get: function () { return context_1.DurableContext; } });
Object.defineProperty(exports, "computeMemoKey", { enumerable: true, get: function () { return context_1.computeMemoKey; } });
var runtime_1 = require("../v1/client/worker/runtime");
Object.defineProperty(exports, "isContextRuntime", { enumerable: true, get: function () { return runtime_1.isContextRuntime; } });
var logger_1 = require("../util/logger/logger");
Object.defineProperty(exports, "Logger", { enumerable: true, get: function () { return logger_1.Logger; } });
Object.defineProperty(exports, "LogLevelEnum", { enumerable: true, get: function () { return logger_1.LogLevelEnum; } });
var parent_run_context_vars_1 = require("../v1/parent-run-context-vars");
Object.defineProperty(exports, "ParentRunContextManager", { enumerable: true, get: function () { return parent_run_context_vars_1.ParentRunContextManager; } });
Object.defineProperty(exports, "parentRunContextManager", { enumerable: true, get: function () { return parent_run_context_vars_1.parentRunContextManager; } });
// Errors
var hatchet_error_1 = require("../util/errors/hatchet-error");
Object.defineProperty(exports, "HatchetError", { enumerable: true, get: function () { return __importDefault(hatchet_error_1).default; } });
Object.defineProperty(exports, "getErrorMessage", { enumerable: true, get: function () { return hatchet_error_1.getErrorMessage; } });
Object.defineProperty(exports, "toHatchetError", { enumerable: true, get: function () { return hatchet_error_1.toHatchetError; } });
var non_determinism_error_1 = require("../util/errors/non-determinism-error");
Object.defineProperty(exports, "NonDeterminismError", { enumerable: true, get: function () { return non_determinism_error_1.NonDeterminismError; } });
var task_run_terminated_error_1 = require("../util/errors/task-run-terminated-error");
Object.defineProperty(exports, "TaskRunTerminatedError", { enumerable: true, get: function () { return task_run_terminated_error_1.TaskRunTerminatedError; } });
Object.defineProperty(exports, "isTaskRunTerminatedError", { enumerable: true, get: function () { return task_run_terminated_error_1.isTaskRunTerminatedError; } });
var abort_error_1 = require("../util/abort-error");
Object.defineProperty(exports, "AbortError", { enumerable: true, get: function () { return abort_error_1.AbortError; } });
Object.defineProperty(exports, "createAbortError", { enumerable: true, get: function () { return abort_error_1.createAbortError; } });
Object.defineProperty(exports, "isAbortError", { enumerable: true, get: function () { return abort_error_1.isAbortError; } });
Object.defineProperty(exports, "rethrowIfAborted", { enumerable: true, get: function () { return abort_error_1.rethrowIfAborted; } });
Object.defineProperty(exports, "throwIfAborted", { enumerable: true, get: function () { return abort_error_1.throwIfAborted; } });
// Eviction policy
var eviction_policy_1 = require("../v1/client/worker/eviction/eviction-policy");
Object.defineProperty(exports, "DEFAULT_DURABLE_TASK_EVICTION_POLICY", { enumerable: true, get: function () { return eviction_policy_1.DEFAULT_DURABLE_TASK_EVICTION_POLICY; } });
var engine_version_1 = require("../v1/client/worker/engine-version");
Object.defineProperty(exports, "MinEngineVersion", { enumerable: true, get: function () { return engine_version_1.MinEngineVersion; } });
Object.defineProperty(exports, "supportsEviction", { enumerable: true, get: function () { return engine_version_1.supportsEviction; } });
// Registration
var workflow_proto_1 = require("../v1/client/worker/workflow-proto");
Object.defineProperty(exports, "ON_FAILURE_TASK_NAME", { enumerable: true, get: function () { return workflow_proto_1.ON_FAILURE_TASK_NAME; } });
Object.defineProperty(exports, "ON_SUCCESS_TASK_NAME", { enumerable: true, get: function () { return workflow_proto_1.ON_SUCCESS_TASK_NAME; } });
Object.defineProperty(exports, "normalizeWorkflowDefinition", { enumerable: true, get: function () { return workflow_proto_1.normalizeWorkflowDefinition; } });
Object.defineProperty(exports, "onFailureTaskName", { enumerable: true, get: function () { return workflow_proto_1.onFailureTaskName; } });
Object.defineProperty(exports, "workflowToProto", { enumerable: true, get: function () { return workflow_proto_1.workflowToProto; } });
// Wire types
var workflows_1 = require("../protoc/v1/workflows");
Object.defineProperty(exports, "CreateWorkflowVersionRequest", { enumerable: true, get: function () { return workflows_1.CreateWorkflowVersionRequest; } });
var dispatcher_1 = require("../protoc/dispatcher");
Object.defineProperty(exports, "AssignedAction", { enumerable: true, get: function () { return dispatcher_1.AssignedAction; } });
var dispatcher_2 = require("../protoc/v1/dispatcher");
Object.defineProperty(exports, "DurableTaskRequest", { enumerable: true, get: function () { return dispatcher_2.DurableTaskRequest; } });
Object.defineProperty(exports, "DurableTaskResponse", { enumerable: true, get: function () { return dispatcher_2.DurableTaskResponse; } });
var declarations_1 = require("./declarations");
Object.defineProperty(exports, "declarations", { enumerable: true, get: function () { return declarations_1.declarations; } });
