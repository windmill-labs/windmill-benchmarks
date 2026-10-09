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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.HatchetCore = void 0;
const hatchet_error_1 = __importDefault(require("../util/errors/hatchet-error"));
const idempotency_collision_error_1 = require("../util/errors/idempotency-collision-error");
const bulk_trigger_partial_error_1 = require("../util/errors/bulk-trigger-partial-error");
const fetch_transport_1 = require("../clients/transport/fetch-transport");
const rpc_1 = require("../clients/admin/rpc");
const trigger_request_1 = require("../clients/admin/trigger-request");
const rpc_2 = require("../clients/event/rpc");
const workflows_1 = require("../protoc/workflows");
const token_1 = require("../util/config-loader/token");
const batch_1 = require("../util/batch");
const retrier_1 = require("../util/retrier");
const declaration_1 = require("../v1/declaration");
const events_1 = require("./features/events");
const logs_1 = require("./features/logs");
const rate_limits_1 = require("./features/rate-limits");
const runs_1 = require("./features/runs");
const streams_1 = require("./features/streams");
const workflows_2 = require("./features/workflows");
const logger_1 = require("./logger");
const run_ref_1 = require("./run-ref");
const BULK_TRIGGER_BATCH_SIZE = 500;
const BULK_TRIGGER_MAX_BYTES = 4 * 1024 * 1024;
/**
 * The application client for runtimes without gRPC: it triggers runs, pushes events, waits
 * for results and manages workflows over unary Connect calls on a fetch transport, so it runs
 * in Cloudflare Workers, Vercel Functions, Deno, Bun, browsers and Node alike.
 *
 * It is configured from an explicit object only. Nothing is read from the environment or a
 * config file, and no worker can be started from it; workers stay on the Node client.
 *
 * ```typescript
 * import { Hatchet } from './index.js';
 *
 * const hatchet = new Hatchet({ token: env.HATCHET_CLIENT_TOKEN });
 * const output = await hatchet.run('greet', { name: 'world' });
 * ```
 */
class HatchetCore {
    constructor(config) {
        var _a, _b, _c;
        if (!config.token) {
            throw new hatchet_error_1.default('a token is required to create a Hatchet client');
        }
        const tenantId = (0, token_1.getTenantIdFromJWT)(config.token);
        // a caller-supplied transport owns its base URL, auth and limits, so no address is resolved
        const serverUrl = config.transport ? ((_a = config.serverUrl) !== null && _a !== void 0 ? _a : '') : resolveServerUrl(config);
        const namespace = normalizeNamespace(config.namespace);
        const logger = (_b = config.logger) !== null && _b !== void 0 ? _b : logger_1.consoleLogger;
        this.config = {
            serverUrl,
            tenantId,
            namespace,
            logLevel: config.logLevel,
            retrier: config.retrier,
        };
        this.logger = logger('HatchetCore', config.logLevel);
        this.transport =
            (_c = config.transport) !== null && _c !== void 0 ? _c : (0, fetch_transport_1.createFetchTransport)({
                token: config.token,
                serverUrl,
                tls: config.tls,
                fetch: config.fetch,
                maxReceiveMessageBytes: config.maxReceiveMessageBytes,
                maxSendMessageBytes: config.maxSendMessageBytes,
            });
        this.workflowsRpc = (0, rpc_1.createWorkflowsRpc)(this.transport);
        this.adminRpc = (0, rpc_1.createV1AdminRpc)(this.transport);
        this.eventsRpc = (0, rpc_2.createEventsRpc)(this.transport);
        const shared = { logger: this.logger, namespace, retrier: config.retrier };
        this.events = new events_1.EventsClient(Object.assign({ rpc: this.eventsRpc }, shared));
        this.runs = new runs_1.RunsClient(this.adminRpc);
        this.workflows = new workflows_2.WorkflowsClient(Object.assign({ rpc: this.adminRpc }, shared));
        this.rateLimits = new rate_limits_1.RateLimitsClient(Object.assign({ rpc: this.workflowsRpc }, shared));
        this.logs = new logs_1.LogsClient(this.eventsRpc);
        this.streams = new streams_1.StreamsClient(this.eventsRpc);
    }
    /** The same as `new HatchetCore(config)`, mirroring the Node client's `init`. */
    static init(config) {
        return new HatchetCore(config);
    }
    /** The tenant the token belongs to. */
    get tenantId() {
        return this.config.tenantId;
    }
    /**
     * Triggers a run and returns a reference to it without waiting.
     *
     * The options are the Node client's `admin.runWorkflow` options and go to the engine as
     * is: the workflow name is namespaced and lowercased and the input is JSON. A task
     * declaration's reference resolves to the task's own output.
     */
    runNoWait(workflow_1, input_1) {
        return __awaiter(this, arguments, void 0, function* (workflow, input, options = {}) {
            var _a;
            const request = (0, trigger_request_1.buildTriggerWorkflowRequest)(workflowName(workflow), input, options, this.config.namespace);
            try {
                const response = yield (0, retrier_1.retrier)(() => this.workflowsRpc.triggerWorkflow(request), this.logger, Object.assign(Object.assign({}, this.config.retrier), { shouldRetry: (e) => !(0, trigger_request_1.isAlreadyExists)(e) }));
                return new run_ref_1.WorkflowRunRef(response.workflowRunId, this.runs, options.parentId, (_a = options._standaloneTaskName) !== null && _a !== void 0 ? _a : standaloneTaskName(workflow));
            }
            catch (e) {
                if ((0, trigger_request_1.isAlreadyExists)(e)) {
                    throw new idempotency_collision_error_1.IdempotencyCollisionError((0, trigger_request_1.extractExistingRunId)(e));
                }
                throw new hatchet_error_1.default(e instanceof Error ? e.message : String(e));
            }
        });
    }
    /** Triggers a run and waits for its output; see `WorkflowRunRef.result()` for the wait. */
    run(workflow_1, input_1) {
        return __awaiter(this, arguments, void 0, function* (workflow, input, options = {}) {
            const ref = yield this.runNoWait(workflow, input, options);
            return ref.result();
        });
    }
    /**
     * Triggers many runs of one workflow in batches of 500 (or 4 MB) over `BulkTriggerWorkflow`
     * and returns their references in input order. `runs` takes the `RunManyOpt` entries the
     * declarations' `runMany` takes.
     *
     * Each batch is one call, so a failure part way through leaves the earlier batches' runs
     * created. That failure rejects with a `BulkTriggerPartialError` whose `refs` are those runs'
     * references and whose `cause` is the batch's own error, so the caller can keep or cancel
     * them instead of retrying the whole request and creating them twice. A failure of the
     * first batch rejects with the batch's error itself: a
     * `BulkTriggerIdempotencyCollisionError` on an idempotency key collision, otherwise a
     * `HatchetError`.
     */
    runManyNoWait(workflow, runs) {
        return __awaiter(this, void 0, void 0, function* () {
            const name = workflowName(workflow);
            const taskName = standaloneTaskName(workflow);
            const requests = runs.map((run) => (0, trigger_request_1.buildTriggerWorkflowRequest)(name, run.input, runOptsToTrigger(run.opts), this.config.namespace));
            const batches = (0, batch_1.batch)(requests, BULK_TRIGGER_BATCH_SIZE, BULK_TRIGGER_MAX_BYTES);
            const refs = [];
            for (const { batchIndex, payloads } of batches) {
                const request = workflows_1.BulkTriggerWorkflowRequest.create({ workflows: payloads });
                let response;
                try {
                    response = yield (0, retrier_1.retrier)(() => this.workflowsRpc.bulkTriggerWorkflow(request), this.logger, Object.assign(Object.assign({}, this.config.retrier), { shouldRetry: (e) => !(0, trigger_request_1.isAlreadyExists)(e) }));
                }
                catch (e) {
                    const error = bulkTriggerError(e);
                    throw batchIndex === 0 ? error : new bulk_trigger_partial_error_1.BulkTriggerPartialError(refs, batchIndex, error);
                }
                refs.push(...response.workflowRunIds.map((id) => new run_ref_1.WorkflowRunRef(id, this.runs, undefined, taskName)));
            }
            return refs;
        });
    }
    /**
     * Triggers many runs of one workflow and waits for all of their outputs, in input order.
     *
     * When any entry sets `opts.returnExceptions`, a failed run does not reject the whole
     * wait: its slot holds an `Error` instead (a task error list is joined with `; `), the way
     * a declaration's `runMany` behaves on the Node client.
     */
    runMany(workflow, runs) {
        return __awaiter(this, void 0, void 0, function* () {
            const refs = yield this.runManyNoWait(workflow, runs);
            const results = refs.map((ref) => ref.result());
            if (runs.some((run) => { var _a; return (_a = run.opts) === null || _a === void 0 ? void 0 : _a.returnExceptions; })) {
                const settled = yield Promise.allSettled(results);
                return settled.map((s) => (s.status === 'fulfilled' ? s.value : asError(s.reason)));
            }
            return Promise.all(results);
        });
    }
    /** A reference to an existing run, to wait on, cancel or replay it. */
    runRef(id) {
        return this.runs.runRef(id);
    }
}
exports.HatchetCore = HatchetCore;
function workflowName(workflow) {
    return typeof workflow === 'string' ? workflow : workflow.name;
}
function standaloneTaskName(workflow) {
    return workflow instanceof declaration_1.TaskWorkflowDeclaration ? workflow._standalone_task_name : undefined;
}
/**
 * The error one bulk trigger batch raises: the collision details when the engine attached
 * them, otherwise a `HatchetError`.
 */
function bulkTriggerError(e) {
    if ((0, trigger_request_1.isAlreadyExists)(e)) {
        const collision = (0, trigger_request_1.extractBulkTriggerCollision)(e);
        if (collision)
            return collision;
    }
    return new hatchet_error_1.default(e instanceof Error ? e.message : String(e));
}
/**
 * The rejection of one run as `runMany` returns it under `returnExceptions`: an `Error` as is,
 * a task error list joined with `; `, anything else as its string; the Node declaration's
 * normalization.
 */
function asError(reason) {
    if (reason instanceof Error)
        return reason;
    return new Error(Array.isArray(reason) ? reason.join('; ') : String(reason));
}
/**
 * Maps a declaration's `RunOpts` onto the trigger options. `sticky` only means something
 * inside a worker task and is dropped; `returnExceptions` is read by `runMany` itself.
 */
function runOptsToTrigger(opts) {
    if (!opts)
        return undefined;
    return {
        additionalMetadata: opts.additionalMetadata,
        priority: opts.priority,
        childKey: opts.childKey,
        desiredWorkerLabels: opts.desiredWorkerLabels,
    };
}
/**
 * The engine's base URL: `serverUrl` checked against the TLS strategy, `hostPort` with the
 * TLS strategy, or the `grpc_broadcast_address` the token was issued with.
 */
function resolveServerUrl(config) {
    if (config.serverUrl || config.hostPort) {
        return (0, fetch_transport_1.resolveServerUrl)(config);
    }
    const hostPort = (0, token_1.getGrpcBroadcastAddressFromJWT)(config.token);
    if (!hostPort) {
        throw new hatchet_error_1.default('the token carries no grpc_broadcast_address claim; set serverUrl or hostPort on the client config');
    }
    return (0, fetch_transport_1.resolveServerUrl)({ hostPort, tls: config.tls });
}
/** Namespaces end in an underscore and are lowercase, as the Node client's config loader makes them. */
function normalizeNamespace(namespace) {
    if (!namespace)
        return '';
    return (namespace.endsWith('_') ? namespace : `${namespace}_`).toLowerCase();
}
