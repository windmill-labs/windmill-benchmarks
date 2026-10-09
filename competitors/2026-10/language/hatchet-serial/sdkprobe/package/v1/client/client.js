"use strict";
/**
 * This is the TypeScript SDK reference, documenting methods available for interacting with Hatchet resources.
 * Check out the [user guide](https://docs.hatchet.run/v1) for an introduction to getting your first tasks running.
 *
 * @module Hatchet TypeScript SDK Reference
 */
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
exports.HatchetClient = void 0;
const hatchet_client_1 = require("../../clients/hatchet-client");
const rest_1 = __importDefault(require("../../clients/rest"));
const config_loader_1 = require("../../util/config-loader");
const hatchet_logger_1 = require("../../clients/hatchet-client/hatchet-logger");
const v4_1 = require("zod/v4");
const event_client_1 = require("../../clients/event/event-client");
const dispatcher_client_1 = require("../../clients/dispatcher/dispatcher-client");
const child_listener_client_1 = require("../../clients/listeners/run-listener/child-listener-client");
const durable_listener_client_1 = require("../../clients/listeners/durable-listener/durable-listener-client");
const grpc_helpers_1 = require("../../util/grpc-helpers");
const transport_1 = require("../../clients/transport");
const nice_grpc_1 = require("nice-grpc");
const declaration_1 = require("../declaration");
const legacy_transformer_1 = require("../../legacy/legacy-transformer");
const worker_1 = require("./worker/worker");
const features_1 = require("./features");
const admin_1 = require("./admin");
/**
 * HatchetV1 implements the main client interface for interacting with the Hatchet workflow engine.
 * It provides methods for creating and executing workflows, as well as managing workers.
 *
 * @template GlobalInput - Global input type required by all tasks. Set via `init<T>()`. Defaults to `{}`.
 * @template MiddlewareBefore - Extra fields merged into task input by pre-middleware hooks. Inferred from middleware config.
 * @template MiddlewareAfter - Extra fields merged into task output by post-middleware hooks. Inferred from middleware config.
 */
class HatchetClient {
    /**
     * @deprecated v0 client will be removed in a future release, please upgrade to v1
     * @hidden
     */
    get v0() {
        if (!this._v0) {
            // The legacy facade shares this client's unary transport rather than building its own.
            this._v0 = new hatchet_client_1.LegacyHatchetClient(this._config, Object.assign(Object.assign({}, this._options), { transport: this._transport }), this._axiosConfig, this.runs, this._listener, this.events, this.dispatcher, this.logger, this.durableListener);
        }
        return this._v0;
    }
    /**
     * Creates a new Hatchet client instance.
     * @param config - Optional configuration for the client
     * @param options - Optional client options
     * @param axiosConfig - Optional Axios configuration for HTTP requests
     * @internal
     */
    constructor(config, options, axiosConfig) {
        var _a, _b;
        try {
            const loaded = config_loader_1.ConfigLoader.loadClientConfig(config, {
                path: options === null || options === void 0 ? void 0 : options.config_path,
            });
            const valid = hatchet_client_1.ClientConfigSchema.parse(loaded);
            let logConstructor = config === null || config === void 0 ? void 0 : config.logger;
            if (logConstructor == null) {
                logConstructor = hatchet_logger_1.DEFAULT_LOGGER;
            }
            const clientConfig = Object.assign(Object.assign({}, valid), { logger: logConstructor });
            this._config = clientConfig;
            this.tenantId = clientConfig.tenant_id;
            this.logger = clientConfig.logger('HatchetClient', clientConfig.log_level);
            this._api = (0, rest_1.default)(clientConfig.api_url, clientConfig.token, axiosConfig);
            this._clientFactory = (0, nice_grpc_1.createClientFactory)().use((0, grpc_helpers_1.addTokenMiddleware)(this.config.token));
            this._credentials =
                (_a = options === null || options === void 0 ? void 0 : options.credentials) !== null && _a !== void 0 ? _a : config_loader_1.ConfigLoader.createCredentials(this.config.tls_config);
            this._transport = (_b = options === null || options === void 0 ? void 0 : options.transport) !== null && _b !== void 0 ? _b : (0, transport_1.createNodeTransport)(this.config);
            this._listener = new child_listener_client_1.RunListenerClient(this.config, (0, grpc_helpers_1.channelFactory)(this.config, this._credentials), this._clientFactory, this.api);
            this._options = options;
            this._axiosConfig = axiosConfig;
        }
        catch (e) {
            if (e instanceof v4_1.z.ZodError) {
                throw new Error(`Invalid client config: ${e.message}`, { cause: e });
            }
            throw e;
        }
        try {
            this.tenant
                .get()
                .then((tenant) => {
                if (tenant.version !== 'V1') {
                    this.config
                        .logger('client-init', event_client_1.LogLevel.INFO)
                        .warn('🚨⚠️‼️ YOU ARE USING A V0 ENGINE WITH A V1 SDK, WHICH IS NOT SUPPORTED. PLEASE UPGRADE YOUR ENGINE TO V1.🚨⚠️‼️');
                }
            })
                .catch(() => {
                // Do nothing here
            });
        }
        catch (_c) {
            // Do nothing here
        }
    }
    /**
     * Static factory method to create a new Hatchet client instance.
     * @template T - Global input type required by all tasks created from this client. Defaults to `{}`.
     * @template U - Global output type required by all tasks created from this client. Defaults to `{}`.
     * @param config - Optional configuration for the client.
     * @param options - Optional client options.
     * @param axiosConfig - Optional Axios configuration for HTTP requests.
     * @returns A new Hatchet client instance. Chain `.withMiddleware()` to attach typed middleware.
     * @internal
     */
    static init(config, options, axiosConfig) {
        return new HatchetClient(config, options, axiosConfig);
    }
    /**
     * Attaches middleware to this client and returns a re-typed instance
     * with inferred pre/post middleware types.
     *
     * Use this after `init<T, U>()` to get full middleware return-type inference
     * that TypeScript can't provide when global types are explicitly set on `init`.
     * @internal
     */
    withMiddleware(middleware) {
        const existing = this._config.middleware || {};
        const toArray = (v) => {
            if (v == null) {
                return [];
            }
            if (Array.isArray(v)) {
                return [...v];
            }
            return [v];
        };
        this._config.middleware = {
            before: [...toArray(existing.before), ...toArray(middleware.before)],
            after: [...toArray(existing.after), ...toArray(middleware.after)],
        };
        return this;
    }
    get config() {
        return this._config;
    }
    /**
     * Creates a new workflow definition.
     * @template I - The input type for the workflow
     * @template O - The return type of the workflow
     * @param options - Configuration options for creating the workflow
     * @returns A new Workflow instance
     * @note It is possible to create an orphaned workflow if no client is available using @hatchet/client CreateWorkflow
     */
    workflow(options) {
        return (0, declaration_1.CreateWorkflow)(options, this);
    }
    /**
     * Implementation of the task method.
     */
    task(options) {
        return (0, declaration_1.CreateTaskWorkflow)(options, this);
    }
    /**
     * Implementation of the batchTask method.
     */
    batchTask(options) {
        return (0, declaration_1.CreateBatchTaskWorkflow)(options, this);
    }
    /**
     * Implementation of the durableTask method.
     */
    durableTask(options) {
        return (0, declaration_1.CreateDurableTaskWorkflow)(options, this);
    }
    /**
     * Triggers a workflow run without waiting for completion.
     * @template I - The input type for the workflow
     * @template O - The return type of the workflow
     * @param workflow - The workflow to run, either as a Workflow instance or workflow name
     * @param input - The input data for the workflow
     * @param options - Configuration options for the workflow run
     * @returns A WorkflowRunRef containing the run ID and methods to interact with the run
     */
    runNoWait(workflow_1, input_1) {
        return __awaiter(this, arguments, void 0, function* (workflow, input, options = {}) {
            const name = (0, legacy_transformer_1.getWorkflowName)(workflow);
            return this.admin.runWorkflow(name, input, options);
        });
    }
    /**
     * Triggers a workflow run and waits for the result.
     * @template I - The input type for the workflow
     * @template O - The return type of the workflow
     * @param workflow - The workflow to run, either as a Workflow instance or workflow name
     * @param input - The input data for the workflow
     * @alias run
     * @param options - Configuration options for the workflow run
     * @returns A promise that resolves with the workflow result
     */
    runAndWait(workflow_1, input_1) {
        return __awaiter(this, arguments, void 0, function* (workflow, input, options = {}) {
            return this.run(workflow, input, options);
        });
    }
    /**
     * Triggers a workflow run and waits for the result.
     * @template I - The input type for the workflow
     * @template O - The return type of the workflow
     * @param workflow - The workflow to run, either as a Workflow instance or workflow name
     * @param input - The input data for the workflow
     * @param options - Configuration options for the workflow run
     * @returns A promise that resolves with the workflow result
     */
    run(workflow_1, input_1) {
        return __awaiter(this, arguments, void 0, function* (workflow, input, options = {}) {
            const run = yield this.runNoWait(workflow, input, options);
            return run.output;
        });
    }
    /**
     * Get the CEL client for debugging CEL expressions
     * @returns A CEL client instance
     * @internal
     */
    get cel() {
        if (!this._cel) {
            this._cel = new features_1.CELClient(this);
        }
        return this._cel;
    }
    /**
     * Get the cron client for creating and managing cron workflow runs
     * @returns A cron client instance
     */
    get crons() {
        if (!this._crons) {
            this._crons = new features_1.CronClient(this);
        }
        return this._crons;
    }
    /**
     * Get the cron client for creating and managing cron workflow runs
     * @returns A cron client instance
     * @deprecated use client.crons instead
     * @hidden
     */
    get cron() {
        return this.crons;
    }
    /**
     * Get the schedules client for creating and managing scheduled workflow runs
     * @returns A schedules client instance
     */
    get scheduled() {
        if (!this._scheduled) {
            this._scheduled = new features_1.ScheduleClient(this);
        }
        return this._scheduled;
    }
    /**
     * Get the schedule client for creating and managing scheduled workflow runs
     * @returns A schedule client instance
     * @deprecated use client.scheduled instead
     * @hidden
     */
    get schedule() {
        return this.scheduled;
    }
    /**
     * @alias scheduled
     */
    get schedules() {
        return this.scheduled;
    }
    /**
     * Get the dispatcher client for sending action events and managing worker registration
     * @returns A dispatcher client instance
     * @internal
     */
    get dispatcher() {
        if (!this._dispatcher) {
            this._dispatcher = new dispatcher_client_1.DispatcherClient(this._config, (0, grpc_helpers_1.channelFactory)(this._config, this._credentials), this._clientFactory);
        }
        return this._dispatcher;
    }
    /**
     * Get the event client for creating and managing event workflow runs
     * @returns A event client instance
     */
    get events() {
        if (!this._event) {
            this._event = new event_client_1.EventClient(this._config, (0, grpc_helpers_1.channelFactory)(this._config, this._credentials), this._clientFactory, this.api, this._transport);
        }
        return this._event;
    }
    /**
     * Get the durable listener client for managing durable event subscriptions
     * @returns A durable listener client instance
     * @internal
     */
    get durableListener() {
        if (!this._durableListener) {
            this._durableListener = new durable_listener_client_1.DurableListenerClient(this._config, (0, grpc_helpers_1.channelFactory)(this._config, this._credentials), this._clientFactory);
        }
        return this._durableListener;
    }
    /**
     * Get the run listener client for streaming workflow run results
     * @returns A run listener client instance
     * @internal
     */
    get listener() {
        return this._listener;
    }
    /**
     * The Connect transport the client's unary RPCs go through
     * @internal
     */
    get transport() {
        return this._transport;
    }
    /**
     * @deprecated use client.events instead
     * @hidden
     */
    get event() {
        return this.events;
    }
    /**
     * Get the metrics client for creating and managing metrics
     * @returns A metrics client instance
     */
    get metrics() {
        if (!this._metrics) {
            this._metrics = new features_1.MetricsClient(this);
        }
        return this._metrics;
    }
    /**
     * Get the filters client for creating and managing filters
     * @returns A filters client instance
     */
    get filters() {
        if (!this._filters) {
            this._filters = new features_1.FiltersClient(this);
        }
        return this._filters;
    }
    /**
     * Get the tenant client for managing tenants
     * @returns A tenant client instance
     */
    get tenant() {
        if (!this._tenant) {
            this._tenant = new features_1.TenantClient(this);
        }
        return this._tenant;
    }
    /**
     * Get the webhooks client for creating and managing webhooks
     * @returns A webhooks client instance
     */
    get webhooks() {
        if (!this._webhooks) {
            this._webhooks = new features_1.WebhooksClient(this);
        }
        return this._webhooks;
    }
    /**
     * Get the logs client for creating and managing logs
     * @returns A logs client instance
     */
    get logs() {
        if (!this._logs) {
            this._logs = new features_1.LogsClient(this);
        }
        return this._logs;
    }
    /**
     * Get the rate limits client for creating and managing rate limits
     * @returns A rate limits client instance
     */
    get ratelimits() {
        if (!this._ratelimits) {
            this._ratelimits = new features_1.RatelimitsClient(this);
        }
        return this._ratelimits;
    }
    /**
     * Get the runs client for creating and managing runs
     * @returns A runs client instance
     */
    get runs() {
        if (!this._runs) {
            this._runs = new features_1.RunsClient(this);
        }
        return this._runs;
    }
    /**
     * Get the streams client for publishing to and reading from durable,
     * topic-based streams
     * @returns A streams client instance
     */
    get streams() {
        if (!this._streams) {
            this._streams = new features_1.StreamsClient(this);
        }
        return this._streams;
    }
    /**
     * Get the workflows client for creating and managing workflows
     * @returns A workflows client instance
     */
    get workflows() {
        if (!this._workflows) {
            this._workflows = new features_1.WorkflowsClient(this);
        }
        return this._workflows;
    }
    /**
     * Get the tasks client for creating and managing tasks
     * @returns A tasks client instance
     */
    get tasks() {
        return this.workflows;
    }
    /**
     * Get the workers client for creating and managing workers
     * @returns A workers client instance
     */
    get workers() {
        if (!this._workers) {
            this._workers = new features_1.WorkersClient(this);
        }
        return this._workers;
    }
    /**
     * Get the API client for making HTTP requests to the Hatchet API
     * Note: This is not recommended for general use, but is available for advanced scenarios
     * @returns A API client instance
     */
    get api() {
        return this._api;
    }
    /**
     * Get the admin client for creating and managing workflows
     * @returns A admin client instance
     * @internal
     */
    get admin() {
        if (!this._admin) {
            this._admin = new admin_1.AdminClient(this._config, this.api, this.runs, this._transport);
        }
        return this._admin;
    }
    /**
     * Creates a new worker instance for processing workflow tasks.
     * @param options - Configuration options for creating the worker
     * @returns A promise that resolves with a new HatchetWorker instance
     */
    worker(name, options) {
        const opts = typeof options === 'number' ? { slots: options } : (options !== null && options !== void 0 ? options : {});
        return worker_1.Worker.create(this, name, opts);
    }
    runRef(id) {
        return this.runs.runRef(id);
    }
}
exports.HatchetClient = HatchetClient;
