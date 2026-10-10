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
exports.WorkflowsClient = exports.workflowNameString = exports.WorkflowPauseScheduledCronRunQueueBehavior = void 0;
const legacy_transformer_1 = require("../../../legacy/legacy-transformer");
const uuid_1 = require("../../../util/uuid");
const data_contracts_1 = require("../../../clients/rest/generated/data-contracts");
Object.defineProperty(exports, "WorkflowPauseScheduledCronRunQueueBehavior", { enumerable: true, get: function () { return data_contracts_1.WorkflowPauseScheduledCronRunQueueBehavior; } });
const duration_1 = require("../duration");
const workflowNameString = (workflow) => {
    if (typeof workflow === 'string') {
        return workflow;
    }
    if (typeof workflow === 'object' && 'name' in workflow) {
        return workflow.name;
    }
    if ((0, legacy_transformer_1.isLegacyWorkflow)(workflow)) {
        (0, legacy_transformer_1.warnLegacyWorkflow)();
        return workflow.id;
    }
    throw new Error('Invalid workflow: must be a string, Workflow object, or WorkflowDefinition object');
};
exports.workflowNameString = workflowNameString;
/**
 * The workflows client is a client for managing workflows programmatically within Hatchet.
 *
 * NOTE: that workflows are the declaration, not the individual runs. If you're looking for runs, use the RunsClient instead.
 *
 */
class WorkflowsClient {
    constructor(client, cacheTTL) {
        // Add a cache for workflows
        this.workflowCache = new Map();
        // Default cache TTL: 5 minutes (in ms)
        this.cacheTTL = 5 * 60 * 1000;
        this.api = client.api;
        this.tenantId = client.tenantId;
        // Allow custom cache TTL if provided
        if (cacheTTL !== undefined) {
            this.cacheTTL = cacheTTL;
        }
    }
    /**
     * Gets the workflow ID from a workflow name, ID, or object.
     * If the input is not a valid UUID, it will look up the workflow by name.
     * @param workflow - The workflow name, ID, or object.
     * @returns The workflow ID as a string.
     */
    getWorkflowIdFromName(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            const str = (() => {
                if (typeof workflow === 'string') {
                    return workflow;
                }
                if (typeof workflow === 'object' && 'name' in workflow) {
                    return workflow.name;
                }
                throw new Error('Invalid workflow: must be a string, Workflow object, or WorkflowDefinition object');
            })();
            if (!(0, uuid_1.isValidUUID)(str)) {
                const wf = yield this.get(str);
                if (!wf) {
                    throw new Error('Invalid workflow ID: must be a valid UUID');
                }
                return wf.metadata.id;
            }
            return str;
        });
    }
    /**
     * Get a workflow by its name, ID, or object.
     * @param workflow - The workflow name, ID, or object.
     * @returns A promise that resolves to the workflow.
     */
    get(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            // Get workflow name string
            const name = (0, exports.workflowNameString)(workflow);
            // Check cache first
            const cached = this.workflowCache.get(name);
            if (cached && cached.expiry > Date.now()) {
                return cached.workflow;
            }
            // If not in cache or expired, fetch from API
            try {
                // Since the API only supports listing with a name filter,
                // we'll use the list endpoint with a name filter
                const { data } = yield this.api.workflowList(this.tenantId, {
                    name,
                });
                if (data && data.rows && data.rows.length > 0) {
                    const wf = (_a = data.rows.find((row) => row.name === name)) !== null && _a !== void 0 ? _a : data.rows[0];
                    this.workflowCache.set(name, {
                        workflow: wf,
                        expiry: Date.now() + this.cacheTTL,
                    });
                    return wf;
                }
                throw new Error(`Workflow with name ${name} not found`);
            }
            catch (error) {
                // Clear cache on error
                this.workflowCache.delete(name);
                throw error;
            }
        });
    }
    /**
     * List all workflows in the tenant.
     * @param opts - The options for the list operation.
     * @returns A promise that resolves to the list of workflows.
     */
    list(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.workflowList(this.tenantId, opts);
            return data;
        });
    }
    /**
     * Delete a workflow by its name, ID, or object.
     * @param workflow - The workflow name, ID, or object.
     * @returns A promise that resolves to the deleted workflow.
     */
    delete(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            const name = (0, exports.workflowNameString)(workflow);
            try {
                // Get the workflow first to find its ID
                const workflowObj = yield this.get(name);
                if (!workflowObj || !workflowObj.metadata || !workflowObj.metadata.id) {
                    throw new Error(`Could not find workflow with name ${name}`);
                }
                const { data } = yield this.api.workflowDelete(workflowObj.metadata.id);
                // Remove from cache after deletion
                this.workflowCache.delete(name);
                return data;
            }
            catch (error) {
                // Clear cache on error
                this.workflowCache.delete(name);
                throw error;
            }
        });
    }
    /**
     * Pause a workflow. While paused, new runs of the workflow are queued but not started.
     * @param workflow - The workflow name, ID, or object.
     * @param opts - The options for the pause operation.
     * @returns A promise that resolves to the updated workflow.
     */
    pause(workflow, opts) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            return this.update(workflow, {
                pause: {
                    action: 'pause',
                    pausedWorkflowQueueTTL: (0, duration_1.durationToString)(opts.queueTTL),
                    pausedWorkflowCronRunQueueBehavior: (_a = opts.pausedWorkflowCronRunQueueBehavior) !== null && _a !== void 0 ? _a : data_contracts_1.WorkflowPauseScheduledCronRunQueueBehavior.QUEUE,
                    pausedWorkflowScheduledRunQueueBehavior: (_b = opts.pausedWorkflowScheduledRunQueueBehavior) !== null && _b !== void 0 ? _b : data_contracts_1.WorkflowPauseScheduledCronRunQueueBehavior.QUEUE,
                },
            });
        });
    }
    /**
     * Unpause a workflow.
     * @param workflow - The workflow name, ID, or object.
     * @returns A promise that resolves to the updated workflow.
     */
    unpause(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.update(workflow, { pause: { action: 'unpause' } });
        });
    }
    update(workflow, request) {
        return __awaiter(this, void 0, void 0, function* () {
            const name = (0, exports.workflowNameString)(workflow);
            const workflowId = yield this.getWorkflowIdFromName(name);
            try {
                const { data } = yield this.api.workflowUpdate(workflowId, request);
                this.workflowCache.delete(name);
                this.workflowCache.delete(data.name);
                return data;
            }
            catch (error) {
                this.workflowCache.delete(name);
                throw error;
            }
        });
    }
}
exports.WorkflowsClient = WorkflowsClient;
