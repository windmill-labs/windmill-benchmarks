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
exports.AdminClient = void 0;
const hatchet_error_1 = __importDefault(require("../../util/errors/hatchet-error"));
const idempotency_collision_error_1 = require("../../util/errors/idempotency-collision-error");
const workflow_run_ref_1 = __importDefault(require("../../util/workflow-run-ref"));
const nice_grpc_1 = require("nice-grpc");
const grpc_helpers_1 = require("../../util/grpc-helpers");
const config_loader_1 = require("../../util/config-loader");
const child_listener_client_1 = require("../../clients/listeners/run-listener/child-listener-client");
const workflows_1 = require("../../protoc/workflows");
const rpc_1 = require("../../clients/admin/rpc");
const trigger_request_1 = require("../../clients/admin/trigger-request");
const transport_1 = require("../../clients/transport");
const retrier_1 = require("../../util/retrier");
const batch_1 = require("../../util/batch");
class AdminClient {
    constructor(config, api, runs, transport = (0, transport_1.createNodeTransport)(config)) {
        this.config = config;
        this.logger = config.logger(`Admin`, config.log_level);
        this.workflowsGrpc = (0, rpc_1.createWorkflowsRpc)(transport);
        this.adminGrpc = (0, rpc_1.createV1AdminRpc)(transport);
        // The run listener streams, which stay on the nice-grpc channel.
        const channel = (0, grpc_helpers_1.channelFactory)(config, config_loader_1.ConfigLoader.createCredentials(config.tls_config));
        const factory = (0, nice_grpc_1.createClientFactory)().use((0, grpc_helpers_1.addTokenMiddleware)(config.token));
        this.listenerClient = new child_listener_client_1.RunListenerClient(config, channel, factory, api);
        this.runs = runs;
    }
    /**
     * Creates a new workflow or updates an existing workflow via the v1 admin service.
     * @param workflow a workflow definition to create
     */
    putWorkflow(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                return yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.adminGrpc.putWorkflow(workflow); }), this.logger);
            }
            catch (e) {
                throw new hatchet_error_1.default(e.message);
            }
        });
    }
    /**
     * Run a new instance of a workflow with the given input. This will create a new workflow run and return the ID of the
     * new run.
     * @param workflowName the name of the workflow to run
     * @param input an object containing the input to the workflow
     * @param options an object containing the options to run the workflow
     * @returns the ID of the new workflow run
     *
     * @important This method is instrumented by HatchetInstrumentor._patchRunWorkflow.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    runWorkflow(workflowName, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const request = (0, trigger_request_1.buildTriggerWorkflowRequest)(workflowName, input, options, this.config.namespace);
                const resp = yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.workflowsGrpc.triggerWorkflow(request); }), this.logger, Object.assign(Object.assign({}, this.config.retrier), { shouldRetry: (e) => !(0, trigger_request_1.isAlreadyExists)(e) }));
                const id = resp.workflowRunId;
                const ref = new workflow_run_ref_1.default(id, this.listenerClient, this.runs, options === null || options === void 0 ? void 0 : options.parentId, options === null || options === void 0 ? void 0 : options._standaloneTaskName);
                yield ref.getWorkflowRunId();
                return ref;
            }
            catch (e) {
                if ((0, trigger_request_1.isAlreadyExists)(e)) {
                    throw new idempotency_collision_error_1.IdempotencyCollisionError((0, trigger_request_1.extractExistingRunId)(e));
                }
                throw new hatchet_error_1.default(e instanceof Error ? e.message : String(e));
            }
        });
    }
    /**
     * Run multiple workflows runs with the given input and options. This will create new workflow runs and return their IDs.
     * Order is preserved in the response.
     * @param workflowRuns an array of objects containing the workflow name, input, and options for each workflow run
     * @returns an array of workflow run references
     *
     * @important This method is instrumented by HatchetInstrumentor._patchRunWorkflows.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    runWorkflows(workflowRuns_1) {
        return __awaiter(this, arguments, void 0, function* (workflowRuns, batchSize = 500) {
            var _a;
            // Prepare workflows to be triggered in bulk
            const workflowRequests = workflowRuns.map(({ workflowName, input, options }) => (0, trigger_request_1.buildTriggerWorkflowRequest)(workflowName, input, options, this.config.namespace));
            const batches = (0, batch_1.batch)(workflowRequests, batchSize, (_a = this.config.grpc_max_send_message_length) !== null && _a !== void 0 ? _a : 4 * 1024 * 1024);
            this.logger.debug(`batching ${batches.length} batches`);
            try {
                const results = [];
                // for loop to ensure serial execution of batches
                for (const { payloads, originalIndices, batchIndex } of batches) {
                    const request = workflows_1.BulkTriggerWorkflowRequest.create({
                        workflows: payloads,
                    });
                    // Call the bulk trigger workflow method for this batch
                    const bulkTriggerWorkflowResponse = yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.workflowsGrpc.bulkTriggerWorkflow(request); }), this.logger, Object.assign(Object.assign({}, this.config.retrier), { shouldRetry: (e) => !(0, trigger_request_1.isAlreadyExists)(e) }));
                    this.logger.debug(`batch ${batchIndex + 1} of ${batches.length}`);
                    // Map the results back to their original indices
                    const batchResults = bulkTriggerWorkflowResponse.workflowRunIds.map((resp, index) => {
                        const originalIndex = originalIndices[index];
                        const { options } = workflowRuns[originalIndex];
                        return new workflow_run_ref_1.default(resp, this.listenerClient, this.runs, options === null || options === void 0 ? void 0 : options.parentId, options === null || options === void 0 ? void 0 : options._standaloneTaskName);
                    });
                    results.push(...batchResults);
                }
                return results;
            }
            catch (e) {
                if ((0, trigger_request_1.isAlreadyExists)(e)) {
                    const collision = (0, trigger_request_1.extractBulkTriggerCollision)(e);
                    if (collision)
                        throw collision;
                }
                throw new hatchet_error_1.default(e instanceof Error ? e.message : String(e));
            }
        });
    }
    putRateLimit(key, limit, duration) {
        return __awaiter(this, void 0, void 0, function* () {
            const request = {
                key,
                limit,
                duration,
            };
            yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.workflowsGrpc.putRateLimit(request); }), this.logger, this.config.retrier);
        });
    }
}
exports.AdminClient = AdminClient;
