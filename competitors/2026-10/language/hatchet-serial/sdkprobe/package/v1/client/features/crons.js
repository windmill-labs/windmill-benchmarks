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
Object.defineProperty(exports, "__esModule", { value: true });
exports.CronClient = exports.CreateCronTriggerSchema = void 0;
const v4_1 = require("zod/v4");
const axios_1 = require("axios");
const uuid_1 = require("../../../util/uuid");
const apply_namespace_1 = require("../../../util/apply-namespace");
const workflows_1 = require("./workflows");
/**
 * Schema for creating a Cron Trigger.
 * @internal
 */
exports.CreateCronTriggerSchema = v4_1.z.object({
    name: v4_1.z.string(),
    expression: v4_1.z.string(),
    input: v4_1.z.record(v4_1.z.any(), v4_1.z.any()).optional(),
    additionalMetadata: v4_1.z.record(v4_1.z.string(), v4_1.z.string()).optional(),
    priority: v4_1.z.number().optional(),
});
/**
 * The cron client is a client for managing cron workflows within Hatchet.
 */
class CronClient {
    constructor(client) {
        this.api = client.api;
        this.tenantId = client.tenantId;
        this.workflows = new workflows_1.WorkflowsClient(client);
        this.namespace = client.config.namespace;
    }
    /**
     * Retrieves the Cron ID from a CronWorkflows object or a string.
     * @param cron - The CronWorkflows object or Cron ID as a string.
     * @returns The Cron ID as a string.
     */
    getCronId(cron) {
        const str = typeof cron === 'string' ? cron : cron.metadata.id;
        if (!(0, uuid_1.isValidUUID)(str)) {
            throw new Error('Invalid cron ID: must be a valid UUID');
        }
        return str;
    }
    /**
     * Creates a new Cron workflow.
     * @param workflow - The workflow identifier or Workflow object.
     * @param cron - The input data for creating the Cron Trigger.
     * @returns A promise that resolves to the created CronWorkflows object.
     * @throws Will throw an error if the input is invalid or the API call fails.
     */
    create(workflow, cron) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b, _c;
            const workflowId = (0, apply_namespace_1.applyNamespace)((0, workflows_1.workflowNameString)(workflow), this.namespace).toLowerCase();
            // Validate cron input with zod schema
            try {
                const parsedCron = exports.CreateCronTriggerSchema.parse(cron);
                const response = yield this.api.cronWorkflowTriggerCreate(this.tenantId, workflowId, {
                    cronName: parsedCron.name,
                    cronExpression: parsedCron.expression,
                    input: (_a = parsedCron.input) !== null && _a !== void 0 ? _a : {},
                    additionalMetadata: (_b = parsedCron.additionalMetadata) !== null && _b !== void 0 ? _b : {},
                    priority: parsedCron.priority,
                });
                return response.data;
            }
            catch (err) {
                if (err instanceof v4_1.z.ZodError) {
                    throw new Error(`Invalid cron input: ${err.message}`, { cause: err });
                }
                if (err instanceof axios_1.AxiosError) {
                    throw new Error(JSON.stringify((_c = err.response) === null || _c === void 0 ? void 0 : _c.data.errors), { cause: err });
                }
                throw err;
            }
        });
    }
    /**
     * Deletes an existing Cron Trigger.
     * @param cron - The Cron Trigger ID as a string or CronWorkflows object.
     * @returns A promise that resolves when the Cron Trigger is deleted.
     */
    delete(cron) {
        return __awaiter(this, void 0, void 0, function* () {
            const cronId = this.getCronId(cron);
            yield this.api.workflowCronDelete(this.tenantId, cronId);
        });
    }
    /**
     * Lists all Cron Triggers based on the provided query parameters.
     * @param query - Query parameters for listing Cron Triggers.
     * @returns A promise that resolves to a CronWorkflowsList object.
     */
    list(query) {
        return __awaiter(this, void 0, void 0, function* () {
            const { workflow } = query, rest = __rest(query, ["workflow"]);
            if (workflow) {
                const workflowId = yield this.workflows.getWorkflowIdFromName((0, apply_namespace_1.applyNamespace)((0, workflows_1.workflowNameString)(workflow), this.namespace).toLowerCase());
                rest.workflowId = workflowId;
            }
            const response = yield this.api.cronWorkflowList(this.tenantId, rest);
            return response.data;
        });
    }
    /**
     * Retrieves a specific Cron Trigger by its ID.
     * @param cron - The Cron Trigger ID as a string or CronWorkflows object.
     * @returns A promise that resolves to the CronWorkflows object.
     */
    get(cron) {
        return __awaiter(this, void 0, void 0, function* () {
            const cronId = this.getCronId(cron);
            const response = yield this.api.workflowCronGet(this.tenantId, cronId);
            return response.data;
        });
    }
}
exports.CronClient = CronClient;
