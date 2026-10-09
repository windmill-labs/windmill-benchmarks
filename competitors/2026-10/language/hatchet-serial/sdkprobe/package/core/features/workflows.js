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
exports.WorkflowsClient = void 0;
const hatchet_error_1 = __importDefault(require("../../util/errors/hatchet-error"));
const retrier_1 = require("../../util/retrier");
const workflow_proto_1 = require("../../v1/client/worker/workflow-proto");
/** Registers workflow definitions with the engine. */
class WorkflowsClient {
    constructor(deps) {
        this.deps = deps;
    }
    /**
     * Creates or updates a workflow. A declaration from `/edge` (or its definition) is
     * converted with the client's namespace the way a worker registers it; a
     * `CreateWorkflowVersionRequest` is sent as given.
     */
    put(workflow) {
        return __awaiter(this, void 0, void 0, function* () {
            const request = isWireRequest(workflow)
                ? workflow
                : (0, workflow_proto_1.workflowToProto)(workflow, { namespace: this.deps.namespace });
            try {
                return yield (0, retrier_1.retrier)(() => this.deps.rpc.putWorkflow(request), this.deps.logger, this.deps.retrier);
            }
            catch (e) {
                throw new hatchet_error_1.default(e instanceof Error ? e.message : String(e));
            }
        });
    }
}
exports.WorkflowsClient = WorkflowsClient;
function isWireRequest(workflow) {
    return 'tasks' in workflow && Array.isArray(workflow.tasks);
}
