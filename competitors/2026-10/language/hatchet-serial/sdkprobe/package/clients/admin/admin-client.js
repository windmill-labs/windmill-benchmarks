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
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AdminClient = void 0;
const workflows_1 = require("../../protoc/workflows");
const hatchet_error_1 = require("../../util/errors/hatchet-error");
const v0_deprecation_warning_1 = require("../../util/v0-deprecation-warning");
const retrier_1 = require("../../util/retrier");
const workflow_run_ref_1 = __importDefault(require("../../util/workflow-run-ref"));
const apply_namespace_1 = require("../../util/apply-namespace");
const transport_1 = require("../transport");
const rpc_1 = require("./rpc");
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
class AdminClient {
    /**
     * The positional `channel` and `factory` parameters are part of the public constructor
     * signature; the unary calls go over `transport` and the streaming clients own the channel.
     */
    constructor(config, _channel, _factory, api, tenantId, listenerClient, workflows, transport = (0, transport_1.createNodeTransport)(config)) {
        this.config = config;
        this.client = (0, rpc_1.createWorkflowsRpc)(transport);
        this.v1Client = (0, rpc_1.createV1AdminRpc)(transport);
        this.api = api;
        this.tenantId = tenantId;
        this.logger = config.logger(`Admin`, config.log_level);
        this.listenerClient = listenerClient;
        this.workflows = workflows;
    }
    /**
     * @deprecated use putWorkflow instead
     */
    put_workflow(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.putWorkflow(opts);
        });
    }
    /**
     * Creates a new workflow or updates an existing workflow. If the workflow already exists, Hatchet will automatically
     * determine if the workflow definition has changed and create a new version if necessary.
     * @param workflow a workflow definition to create
     */
    putWorkflow(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            // a v0 workflow definition sent straight to the engine, the one v0 use that never
            // reaches the legacy transformer
            (0, v0_deprecation_warning_1.emitV0RemovedWarning)('workflow');
            try {
                return yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.client.putWorkflow({ opts: workflow }); }), this.logger);
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    /**
     * Creates a new workflow or updates an existing workflow. If the workflow already exists, Hatchet will automatically
     * determine if the workflow definition has changed and create a new version if necessary.
     * @param workflow a workflow definition to create
     */
    putWorkflowV1(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                return yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.v1Client.putWorkflow(workflow); }), this.logger);
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    /**
     * @deprecated use hatchet.ratelimits.upsert instead
     */
    put_rate_limit(key, limit, duration) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.putRateLimit(key, limit, duration);
        });
    }
    /**
     * @deprecated use hatchet.ratelimits.upsert instead
     */
    putRateLimit(key_1, limit_1) {
        return __awaiter(this, arguments, void 0, function* (key, limit, duration = workflows_1.RateLimitDuration.SECOND) {
            try {
                yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () {
                    return this.client.putRateLimit({
                        key,
                        limit,
                        duration,
                    });
                }), this.logger, this.config.retrier);
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    registerWebhook(data) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.api.webhookCreate(this.tenantId, data);
        });
    }
    /**
     * @deprecated use runWorkflow instead
     */
    run_workflow(workflowName, input, options) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.runWorkflow(workflowName, input, options);
        });
    }
    /**
     * Run a new instance of a workflow with the given input. This will create a new workflow run and return the ID of the
     * new run.
     * @param workflowName the name of the workflow to run
     * @param input an object containing the input to the workflow
     * @param options an object containing the options to run the workflow
     * @returns the ID of the new workflow run
     */
    runWorkflow(workflowName, input, options) {
        const computedName = (0, apply_namespace_1.applyNamespace)(workflowName, this.config.namespace);
        try {
            const inputStr = JSON.stringify(input);
            const opts = options !== null && options !== void 0 ? options : {};
            const { additionalMetadata, parentStepRunId, parentTaskRunExternalId, desiredWorkerLabels } = opts, rest = __rest(opts, ["additionalMetadata", "parentStepRunId", "parentTaskRunExternalId", "desiredWorkerLabels"]);
            const resp = this.client.triggerWorkflow(Object.assign(Object.assign({ name: computedName, input: inputStr }, rest), { 
                // API expects `parentTaskRunExternalId`; accept the old name as an alias.
                parentTaskRunExternalId: parentTaskRunExternalId !== null && parentTaskRunExternalId !== void 0 ? parentTaskRunExternalId : parentStepRunId, additionalMetadata: additionalMetadata ? JSON.stringify(additionalMetadata) : undefined, priority: opts.priority, desiredWorkerLabels: desiredWorkerLabels
                    ? convertDesiredWorkerLabels(desiredWorkerLabels)
                    : {} }));
            return new workflow_run_ref_1.default(resp, this.listenerClient, this.workflows, options === null || options === void 0 ? void 0 : options.parentId);
        }
        catch (e) {
            throw (0, hatchet_error_1.toHatchetError)(e);
        }
    }
    /**
     * Run multiple workflows runs with the given input and options. This will create new workflow runs and return their IDs.
     * Order is preserved in the response.
     * @param workflowRuns an array of objects containing the workflow name, input, and options for each workflow run
     * @returns an array of workflow run references
     */
    runWorkflows(workflowRuns) {
        // Prepare workflows to be triggered in bulk
        const workflowRequests = workflowRuns.map(({ workflowName, input, options }) => {
            const computedName = (0, apply_namespace_1.applyNamespace)(workflowName, this.config.namespace);
            const inputStr = JSON.stringify(input);
            const opts = options !== null && options !== void 0 ? options : {};
            const { additionalMetadata, parentStepRunId, parentTaskRunExternalId, desiredWorkerLabels } = opts, rest = __rest(opts, ["additionalMetadata", "parentStepRunId", "parentTaskRunExternalId", "desiredWorkerLabels"]);
            return Object.assign(Object.assign({ name: computedName, input: inputStr }, rest), { parentTaskRunExternalId: parentTaskRunExternalId !== null && parentTaskRunExternalId !== void 0 ? parentTaskRunExternalId : parentStepRunId, additionalMetadata: additionalMetadata ? JSON.stringify(additionalMetadata) : undefined, desiredWorkerLabels: desiredWorkerLabels
                    ? convertDesiredWorkerLabels(desiredWorkerLabels)
                    : {} });
        });
        try {
            // Call the bulk trigger workflow method
            const bulkTriggerWorkflowResponse = this.client.bulkTriggerWorkflow(workflows_1.BulkTriggerWorkflowRequest.create({
                workflows: workflowRequests,
            }));
            return bulkTriggerWorkflowResponse.then((res) => {
                return res.workflowRunIds.map((resp, index) => {
                    const { options } = workflowRuns[index];
                    return new workflow_run_ref_1.default(resp, this.listenerClient, this.workflows, options === null || options === void 0 ? void 0 : options.parentId);
                });
            });
        }
        catch (e) {
            throw (0, hatchet_error_1.toHatchetError)(e);
        }
    }
    /**
     * @deprecated use listWorkflows instead
     */
    list_workflows() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.listWorkflows();
        });
    }
    /**
     * List workflows in the tenant associated with the API token.
     * @returns a list of all workflows in the tenant
     */
    listWorkflows() {
        return __awaiter(this, void 0, void 0, function* () {
            const res = yield this.api.workflowList(this.tenantId);
            return res.data;
        });
    }
    /**
     * @deprecated use getWorkflow instead
     */
    get_workflow(workflowId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getWorkflow(workflowId);
        });
    }
    /**
     * Get a workflow by its ID.
     * @param workflowId the workflow ID (**note:** this is not the same as the workflow version id)
     * @returns
     */
    getWorkflow(workflowId) {
        return __awaiter(this, void 0, void 0, function* () {
            const res = yield this.api.workflowGet(workflowId);
            return res.data;
        });
    }
    /**
     * @deprecated use getWorkflowVersion instead
     */
    get_workflow_version(workflowId, version) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getWorkflowVersion(workflowId, version);
        });
    }
    /**
     * Get a workflow version.
     * @param workflowId the workflow ID
     * @param version the version of the workflow to get. If not provided, the latest version will be returned.
     * @returns the workflow version
     */
    getWorkflowVersion(workflowId, version) {
        return __awaiter(this, void 0, void 0, function* () {
            const res = yield this.api.workflowVersionGet(workflowId, {
                version,
            });
            return res.data;
        });
    }
    /**
     * @deprecated use getWorkflowRun instead
     */
    get_workflow_run(workflowRunId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getWorkflowRun(workflowRunId);
        });
    }
    /**
     * Get a workflow run.
     * @param workflowRunId the id of the workflow run to get
     * @returns the workflow run
     */
    getWorkflowRun(workflowRunId) {
        return __awaiter(this, void 0, void 0, function* () {
            return new workflow_run_ref_1.default(workflowRunId, this.listenerClient, this.workflows);
        });
    }
    /**
     * @deprecated use listWorkflowRuns instead
     */
    list_workflow_runs(query) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.listWorkflowRuns(query);
        });
    }
    /**
     * List workflow runs in the tenant associated with the API token.
     * @param query the query to filter the list of workflow runs
     * @returns
     */
    listWorkflowRuns(query) {
        return __awaiter(this, void 0, void 0, function* () {
            const res = yield this.api.workflowRunList(this.tenantId, query);
            return res.data;
        });
    }
    /**
     * @deprecated use scheduleWorkflow instead
     */
    schedule_workflow(name, options) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.scheduleWorkflow(name, options);
        });
    }
    /**
     * Schedule a workflow to run at a specific time or times.
     * @param name the name of the workflow to schedule
     * @param options an object containing the schedules to set
     * @param input an object containing the input to the workflow
     * @deprecated use hatchet.schedules.create instead
     */
    scheduleWorkflow(name, options) {
        try {
            const computedName = (0, apply_namespace_1.applyNamespace)(name, this.config.namespace);
            let input;
            if (options === null || options === void 0 ? void 0 : options.input) {
                input = JSON.stringify(options.input);
            }
            this.client.scheduleWorkflow({
                name: computedName,
                schedules: options === null || options === void 0 ? void 0 : options.schedules,
                input,
            });
        }
        catch (e) {
            throw (0, hatchet_error_1.toHatchetError)(e);
        }
    }
    /**
     * @deprecated use getWorkflowMetrics instead
     */
    get_workflow_metrics(data) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getWorkflowMetrics(data);
        });
    }
    /**
     * Get the metrics for a workflow.
     *
     * @param workflowId the ID of the workflow to get metrics for
     * @param workflowName the name of the workflow to get metrics for
     * @param query an object containing query parameters to filter the metrics
     */
    getWorkflowMetrics({ workflowId, workflowName, status, groupKey }) {
        const params = {
            status,
            groupKey,
        };
        if (workflowName) {
            this.listWorkflows().then((res) => {
                var _a;
                const workflow = (_a = res.rows) === null || _a === void 0 ? void 0 : _a.find((row) => row.name === workflowName);
                if (workflow) {
                    return this.api.workflowGetMetrics(workflow.metadata.id, params);
                }
                throw new Error(`Workflow ${workflowName} not found`);
            });
        }
        else if (workflowId) {
            return this.api.workflowGetMetrics(workflowId, params);
        }
        throw new Error('Must provide either a workflowId or workflowName');
    }
}
exports.AdminClient = AdminClient;
