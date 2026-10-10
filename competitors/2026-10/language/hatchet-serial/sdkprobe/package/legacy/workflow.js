"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.CreateWorkflowSchema = exports.StickyStrategy = exports.HatchetTimeoutSchema = exports.WorkflowConcurrency = exports.ConcurrencyLimitStrategy = void 0;
const z = __importStar(require("zod/v4"));
const step_1 = require("./step");
const workflows_1 = require("../protoc/v1/workflows");
const CronConfigSchema = z.object({
    cron: z.string(),
    event: z.undefined().optional(),
});
const EventConfigSchema = z.object({
    cron: z.undefined().optional(),
    event: z.string(),
});
const OnConfigSchema = z.union([CronConfigSchema, EventConfigSchema]).optional();
const StepsSchema = z.array(step_1.CreateStepSchema);
exports.ConcurrencyLimitStrategy = workflows_1.ConcurrencyLimitStrategy;
exports.WorkflowConcurrency = z.object({
    name: z.string(),
    maxRuns: z.number().optional(),
    limitStrategy: z.enum(exports.ConcurrencyLimitStrategy).optional(),
    expression: z.string().optional(),
});
exports.HatchetTimeoutSchema = z.string();
exports.StickyStrategy = workflows_1.StickyStrategy;
exports.CreateWorkflowSchema = z.object({
    id: z.string(),
    description: z.string(),
    version: z.string().optional(),
    sticky: z.union([z.enum(workflows_1.StickyStrategy), z.enum(['soft', 'hard'])]).optional(),
    scheduleTimeout: z.string().optional(),
    /**
     * @deprecated Workflow timeout is deprecated. Use step timeouts instead.
     */
    timeout: exports.HatchetTimeoutSchema.optional(),
    on: OnConfigSchema,
    steps: StepsSchema,
    onFailure: step_1.CreateStepSchema === null || step_1.CreateStepSchema === void 0 ? void 0 : step_1.CreateStepSchema.optional(),
});
