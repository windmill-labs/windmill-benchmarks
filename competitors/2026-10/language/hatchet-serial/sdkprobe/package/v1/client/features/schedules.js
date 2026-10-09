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
exports.ScheduleClient = exports.UpdateScheduledRunTriggerSchema = exports.CreateScheduledRunTriggerSchema = void 0;
const v4_1 = require("zod/v4");
const axios_1 = require("axios");
const uuid_1 = require("../../../util/uuid");
const apply_namespace_1 = require("../../../util/apply-namespace");
const workflows_1 = require("./workflows");
/**
 * Schema for creating a Scheduled Run Trigger.
 * @internal
 */
exports.CreateScheduledRunTriggerSchema = v4_1.z.object({
    triggerAt: v4_1.z.coerce.date(),
    input: v4_1.z.record(v4_1.z.any(), v4_1.z.any()).optional(),
    additionalMetadata: v4_1.z.record(v4_1.z.string(), v4_1.z.string()).optional(),
    priority: v4_1.z.number().optional(),
});
/**
 * Schema for updating (rescheduling) a Scheduled Run Trigger.
 * @internal
 */
exports.UpdateScheduledRunTriggerSchema = v4_1.z.object({
    triggerAt: v4_1.z.coerce.date(),
});
/**
 * The scheduled client is a client for managing scheduled workflows within Hatchet
 */
class ScheduleClient {
    constructor(client) {
        this.api = client.api;
        this.tenantId = client.tenantId;
        this.workflows = new workflows_1.WorkflowsClient(client);
        this.namespace = client.config.namespace;
    }
    /**
     * Retrieves the Scheduled Run ID from a ScheduledRun object or a string.
     * @param scheduledRun - The ScheduledRun object or Scheduled Run ID as a string.
     * @returns The Scheduled Run ID as a string.
     */
    getScheduledRunId(scheduledRun) {
        const str = typeof scheduledRun === 'string' ? scheduledRun : scheduledRun.metadata.id;
        if (!(0, uuid_1.isValidUUID)(str)) {
            throw new Error('Invalid scheduled run ID: must be a valid UUID');
        }
        return str;
    }
    /**
     * Creates a new Scheduled Run.
     * @param workflow - The workflow name or Workflow object.
     * @param scheduledRun - The input data for creating the Scheduled Run.
     * @returns A promise that resolves to the created ScheduledWorkflows object.
     * @throws Will throw an error if the input is invalid or the API call fails.
     *
     * @important This method is instrumented by HatchetInstrumentor._patchScheduleCreate.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    create(workflow, cron) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b, _c;
            const workflowId = (0, apply_namespace_1.applyNamespace)((0, workflows_1.workflowNameString)(workflow), this.namespace).toLowerCase();
            // Validate cron input with zod schema
            try {
                const parsedCron = exports.CreateScheduledRunTriggerSchema.parse(cron);
                const response = yield this.api.scheduledWorkflowRunCreate(this.tenantId, workflowId, {
                    input: (_a = parsedCron.input) !== null && _a !== void 0 ? _a : {},
                    additionalMetadata: (_b = parsedCron.additionalMetadata) !== null && _b !== void 0 ? _b : {},
                    triggerAt: parsedCron.triggerAt.toISOString(),
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
     * Updates (reschedules) an existing Scheduled Run.
     * @param scheduledRun - The Scheduled Run ID as a string or ScheduledWorkflows object.
     * @param update - The update payload (currently only triggerAt).
     * @returns A promise that resolves to the updated ScheduledWorkflows object.
     */
    update(scheduledRun, update) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            const scheduledRunId = this.getScheduledRunId(scheduledRun);
            try {
                const parsed = exports.UpdateScheduledRunTriggerSchema.parse(update);
                const response = yield this.api.workflowScheduledUpdate(this.tenantId, scheduledRunId, {
                    triggerAt: parsed.triggerAt.toISOString(),
                });
                return response.data;
            }
            catch (err) {
                if (err instanceof v4_1.z.ZodError) {
                    throw new Error(`Invalid update input: ${err.message}`, { cause: err });
                }
                if (err instanceof axios_1.AxiosError) {
                    throw new Error(JSON.stringify((_a = err.response) === null || _a === void 0 ? void 0 : _a.data.errors), { cause: err });
                }
                throw err;
            }
        });
    }
    /**
     * Deletes an existing Scheduled Run.
     * @param scheduledRun - The Scheduled Run ID as a string or ScheduledWorkflows object.
     * @returns A promise that resolves when the Scheduled Run is deleted.
     */
    delete(scheduledRun) {
        return __awaiter(this, void 0, void 0, function* () {
            const scheduledRunId = this.getScheduledRunId(scheduledRun);
            yield this.api.workflowScheduledDelete(this.tenantId, scheduledRunId);
        });
    }
    /**
     * Lists all Cron Triggers based on the provided query parameters.
     * @param query - Query parameters for listing Scheduled Runs.
     * @returns A promise that resolves to a ScheduledWorkflowsList object.
     */
    list(query) {
        return __awaiter(this, void 0, void 0, function* () {
            const { workflow } = query, rest = __rest(query, ["workflow"]);
            if (workflow) {
                const workflowId = yield this.workflows.getWorkflowIdFromName((0, apply_namespace_1.applyNamespace)((0, workflows_1.workflowNameString)(workflow), this.namespace).toLowerCase());
                rest.workflowId = workflowId;
            }
            const response = yield this.api.workflowScheduledList(this.tenantId, rest);
            return response.data;
        });
    }
    /**
     * Retrieves a specific Scheduled Run by its ID.
     * @param scheduledRun - The Scheduled Run ID as a string or ScheduledWorkflows object.
     * @returns A promise that resolves to the ScheduledWorkflows object.
     */
    get(scheduledRun) {
        return __awaiter(this, void 0, void 0, function* () {
            const scheduledRunId = this.getScheduledRunId(scheduledRun);
            const response = yield this.api.workflowScheduledGet(this.tenantId, scheduledRunId);
            return response.data;
        });
    }
    /**
     * Bulk deletes scheduled runs (by explicit IDs and/or a filter).
     * @param opts - Either `scheduledRuns` (ids/objects) and/or a server-side filter.
     * @returns A promise that resolves to deleted ids + per-id errors.
     */
    bulkDelete(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            const scheduledWorkflowRunIds = (_a = opts.scheduledRuns) === null || _a === void 0 ? void 0 : _a.map((r) => this.getScheduledRunId(r));
            const response = yield this.api.workflowScheduledBulkDelete(this.tenantId, {
                scheduledWorkflowRunIds,
                filter: opts.filter,
            });
            return response.data;
        });
    }
    /**
     * Bulk updates (reschedules) scheduled runs.
     * @param updates - List of id/object + new triggerAt.
     * @returns A promise that resolves to updated ids + per-id errors.
     */
    bulkUpdate(updates) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.api.workflowScheduledBulkUpdate(this.tenantId, {
                updates: updates.map((u) => ({
                    id: this.getScheduledRunId(u.scheduledRun),
                    triggerAt: new Date(u.triggerAt).toISOString(),
                })),
            });
            return response.data;
        });
    }
}
exports.ScheduleClient = ScheduleClient;
