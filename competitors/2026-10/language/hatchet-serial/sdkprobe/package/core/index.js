"use strict";
/**
 * The core entry point: the application client for runtimes without gRPC. It triggers runs,
 * pushes events, waits for results by polling and manages workflows over unary Connect calls
 * on a fetch transport, and it is configured from an explicit object only.
 *
 * Nothing exported here imports from Node. `scripts/check-edge-entry.mjs` bundles this module
 * for a browser-like target and fails the build if a Node builtin, `axios` or a gRPC package
 * sneaks in.
 *
 * ```typescript
 * import { Hatchet } from './index.js';
 *
 * const hatchet = new Hatchet({ token: env.HATCHET_CLIENT_TOKEN });
 * const ref = await hatchet.runNoWait('greet', { name: 'world' });
 * const output = await ref.result({ timeoutMs: 30_000 });
 * ```
 * @module Core
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.isAbortError = exports.AbortError = exports.BulkTriggerPartialError = exports.BulkTriggerIdempotencyCollisionError = exports.IdempotencyCollisionError = exports.toHatchetError = exports.getErrorMessage = exports.HatchetError = exports.V1TaskStatus = exports.WorkerLabelComparator = exports.RateLimitDuration = exports.Priority = exports.LogLevel = exports.DEFAULT_MAX_MESSAGE_BYTES = exports.createAuthInterceptor = exports.resolveServerUrl = exports.limitMessageSizes = exports.createFetchTransport = exports.StreamsClient = exports.MAX_LOG_LINE_CHARS = exports.LogsClient = exports.RateLimitsClient = exports.WorkflowsClient = exports.RunsClient = exports.EventsClient = exports.consoleLogger = exports.ConsoleLogger = exports.runStatusToJSON = exports.parseTaskOutput = exports.toRunDetail = exports.MAX_POLL_INTERVAL_MS = exports.INITIAL_POLL_INTERVAL_MS = exports.WorkflowRunRef = exports.Hatchet = exports.HatchetCore = exports.CORE_ENTRY_VERSION = void 0;
/** Bumped whenever the surface exported from this entry changes incompatibly. */
exports.CORE_ENTRY_VERSION = 1;
var client_1 = require("./client");
Object.defineProperty(exports, "HatchetCore", { enumerable: true, get: function () { return client_1.HatchetCore; } });
Object.defineProperty(exports, "Hatchet", { enumerable: true, get: function () { return client_1.HatchetCore; } });
var run_ref_1 = require("./run-ref");
Object.defineProperty(exports, "WorkflowRunRef", { enumerable: true, get: function () { return run_ref_1.WorkflowRunRef; } });
Object.defineProperty(exports, "INITIAL_POLL_INTERVAL_MS", { enumerable: true, get: function () { return run_ref_1.INITIAL_POLL_INTERVAL_MS; } });
Object.defineProperty(exports, "MAX_POLL_INTERVAL_MS", { enumerable: true, get: function () { return run_ref_1.MAX_POLL_INTERVAL_MS; } });
var run_detail_1 = require("./run-detail");
Object.defineProperty(exports, "toRunDetail", { enumerable: true, get: function () { return run_detail_1.toRunDetail; } });
Object.defineProperty(exports, "parseTaskOutput", { enumerable: true, get: function () { return run_detail_1.parseTaskOutput; } });
Object.defineProperty(exports, "runStatusToJSON", { enumerable: true, get: function () { return run_detail_1.runStatusToJSON; } });
var logger_1 = require("./logger");
Object.defineProperty(exports, "ConsoleLogger", { enumerable: true, get: function () { return logger_1.ConsoleLogger; } });
Object.defineProperty(exports, "consoleLogger", { enumerable: true, get: function () { return logger_1.consoleLogger; } });
// Feature clients
var events_1 = require("./features/events");
Object.defineProperty(exports, "EventsClient", { enumerable: true, get: function () { return events_1.EventsClient; } });
var runs_1 = require("./features/runs");
Object.defineProperty(exports, "RunsClient", { enumerable: true, get: function () { return runs_1.RunsClient; } });
var workflows_1 = require("./features/workflows");
Object.defineProperty(exports, "WorkflowsClient", { enumerable: true, get: function () { return workflows_1.WorkflowsClient; } });
var rate_limits_1 = require("./features/rate-limits");
Object.defineProperty(exports, "RateLimitsClient", { enumerable: true, get: function () { return rate_limits_1.RateLimitsClient; } });
var logs_1 = require("./features/logs");
Object.defineProperty(exports, "LogsClient", { enumerable: true, get: function () { return logs_1.LogsClient; } });
Object.defineProperty(exports, "MAX_LOG_LINE_CHARS", { enumerable: true, get: function () { return logs_1.MAX_LOG_LINE_CHARS; } });
var streams_1 = require("./features/streams");
Object.defineProperty(exports, "StreamsClient", { enumerable: true, get: function () { return streams_1.StreamsClient; } });
// Transport
var fetch_transport_1 = require("../clients/transport/fetch-transport");
Object.defineProperty(exports, "createFetchTransport", { enumerable: true, get: function () { return fetch_transport_1.createFetchTransport; } });
Object.defineProperty(exports, "limitMessageSizes", { enumerable: true, get: function () { return fetch_transport_1.limitMessageSizes; } });
Object.defineProperty(exports, "resolveServerUrl", { enumerable: true, get: function () { return fetch_transport_1.resolveServerUrl; } });
var transport_1 = require("../clients/transport/transport");
Object.defineProperty(exports, "createAuthInterceptor", { enumerable: true, get: function () { return transport_1.createAuthInterceptor; } });
Object.defineProperty(exports, "DEFAULT_MAX_MESSAGE_BYTES", { enumerable: true, get: function () { return transport_1.DEFAULT_MAX_MESSAGE_BYTES; } });
// Wire enums and errors callers switch on
var rpc_1 = require("../clients/event/rpc");
Object.defineProperty(exports, "LogLevel", { enumerable: true, get: function () { return rpc_1.LogLevel; } });
var declaration_1 = require("../v1/declaration");
Object.defineProperty(exports, "Priority", { enumerable: true, get: function () { return declaration_1.Priority; } });
var workflows_2 = require("../protoc/workflows/workflows");
Object.defineProperty(exports, "RateLimitDuration", { enumerable: true, get: function () { return workflows_2.RateLimitDuration; } });
var trigger_1 = require("../protoc/v1/shared/trigger");
Object.defineProperty(exports, "WorkerLabelComparator", { enumerable: true, get: function () { return trigger_1.WorkerLabelComparator; } });
var data_contracts_1 = require("../clients/rest/generated/data-contracts");
Object.defineProperty(exports, "V1TaskStatus", { enumerable: true, get: function () { return data_contracts_1.V1TaskStatus; } });
var hatchet_error_1 = require("../util/errors/hatchet-error");
Object.defineProperty(exports, "HatchetError", { enumerable: true, get: function () { return __importDefault(hatchet_error_1).default; } });
Object.defineProperty(exports, "getErrorMessage", { enumerable: true, get: function () { return hatchet_error_1.getErrorMessage; } });
Object.defineProperty(exports, "toHatchetError", { enumerable: true, get: function () { return hatchet_error_1.toHatchetError; } });
var idempotency_collision_error_1 = require("../util/errors/idempotency-collision-error");
Object.defineProperty(exports, "IdempotencyCollisionError", { enumerable: true, get: function () { return idempotency_collision_error_1.IdempotencyCollisionError; } });
var bulk_trigger_idempotency_collision_error_1 = require("../util/errors/bulk-trigger-idempotency-collision-error");
Object.defineProperty(exports, "BulkTriggerIdempotencyCollisionError", { enumerable: true, get: function () { return bulk_trigger_idempotency_collision_error_1.BulkTriggerIdempotencyCollisionError; } });
var bulk_trigger_partial_error_1 = require("../util/errors/bulk-trigger-partial-error");
Object.defineProperty(exports, "BulkTriggerPartialError", { enumerable: true, get: function () { return bulk_trigger_partial_error_1.BulkTriggerPartialError; } });
var abort_error_1 = require("../util/abort-error");
Object.defineProperty(exports, "AbortError", { enumerable: true, get: function () { return abort_error_1.AbortError; } });
Object.defineProperty(exports, "isAbortError", { enumerable: true, get: function () { return abort_error_1.isAbortError; } });
