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
exports.RunsClient = void 0;
const run_detail_1 = require("../run-detail");
const run_ref_1 = require("../run-ref");
/**
 * Task and workflow runs, over the v1 `AdminService`. Everything here is a unary call, so
 * it works from any runtime the client runs on; listing runs is a REST feature of the Node
 * client and is not available here.
 */
class RunsClient {
    constructor(rpc) {
        this.rpc = rpc;
    }
    /**
     * Gets a run's state: its status, whether it is done, its input and metadata, and each of
     * its tasks with their status, output and error.
     * @param run - The run id or a reference to the run.
     * @param options - A signal or deadline for the call.
     */
    get(run, options) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getDetails(run, options);
        });
    }
    /** @alias get */
    getDetails(run, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const externalId = typeof run === 'string' ? run : yield run.getWorkflowRunId();
            return (0, run_detail_1.toRunDetail)(yield this.rpc.getRunDetails({ externalId }, options));
        });
    }
    /**
     * Cancels runs by id, or every run matching the filter when no ids are given.
     * @returns the ids of the cancelled tasks
     */
    cancel(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            return this.rpc.cancelTasks({
                externalIds: (_a = opts.ids) !== null && _a !== void 0 ? _a : [],
                filter: opts.ids ? undefined : prepareFilter((_b = opts.filters) !== null && _b !== void 0 ? _b : {}),
            });
        });
    }
    /**
     * Replays runs by id, or every run matching the filter when no ids are given.
     * @returns the ids of the replayed tasks
     */
    replay(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            return this.rpc.replayTasks({
                externalIds: (_a = opts.ids) !== null && _a !== void 0 ? _a : [],
                filter: opts.ids ? undefined : prepareFilter((_b = opts.filters) !== null && _b !== void 0 ? _b : {}),
            });
        });
    }
    /** A reference to an existing run, to wait on, cancel or replay it. */
    runRef(id) {
        return new run_ref_1.WorkflowRunRef(id, this);
    }
}
exports.RunsClient = RunsClient;
/** Builds the proto filter; `since` defaults to one hour ago, as the Node client's does. */
function prepareFilter(filter) {
    var _a, _b, _c, _d;
    return {
        since: (_a = filter.since) !== null && _a !== void 0 ? _a : new Date(Date.now() - 60 * 60 * 1000),
        until: filter.until,
        statuses: (_b = filter.statuses) !== null && _b !== void 0 ? _b : [],
        workflowIds: (_c = filter.workflowIds) !== null && _c !== void 0 ? _c : [],
        additionalMetadata: Object.entries((_d = filter.additionalMetadata) !== null && _d !== void 0 ? _d : {}).map(([key, value]) => `${key}:${value}`),
    };
}
