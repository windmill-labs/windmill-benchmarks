"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClientConfigSchema = exports.OpenTelemetryConfigSchema = exports.RetrierConfigSchema = void 0;
const v4_1 = require("zod/v4");
exports.RetrierConfigSchema = v4_1.z.object({
    maxAttempts: v4_1.z.number().int().positive().optional(),
    initialInterval: v4_1.z.number().positive().optional(),
    maxJitter: v4_1.z.number().nonnegative().optional(),
});
const ClientTLSConfigSchema = v4_1.z.object({
    tls_strategy: v4_1.z.enum(['tls', 'mtls', 'none']).optional(),
    cert_file: v4_1.z.string().optional(),
    ca_file: v4_1.z.string().optional(),
    key_file: v4_1.z.string().optional(),
    server_name: v4_1.z.string().optional(),
});
const HealthcheckConfigSchema = v4_1.z.object({
    enabled: v4_1.z.boolean().optional().default(false),
    port: v4_1.z.number().optional().default(8001),
});
exports.OpenTelemetryConfigSchema = v4_1.z.object({
    /**
     * List of attribute keys to exclude from spans.
     * Useful for filtering sensitive or verbose data like payloads.
     */
    excludedAttributes: v4_1.z.array(v4_1.z.string()).optional().default([]),
    /**
     * If true, includes the task name in the span name for task run spans.
     * e.g., "hatchet.start_step_run.my_task" instead of "hatchet.start_step_run"
     */
    includeTaskNameInSpanName: v4_1.z.boolean().optional().default(false),
    /**
     * If true, a child `hatchet.run_workflow` span is created for each item in a
     * bulk run (`runWorkflows`), nested under the parent `hatchet.run_workflows`
     * span, and each item's traceparent points at its own span. Defaults to false
     * to preserve the existing span structure for downstream OpenTelemetry
     * collectors.
     */
    individualRunSpansForBulkRun: v4_1.z.boolean().optional().default(false),
});
const TaskMiddlewareSchema = v4_1.z
    .object({
    before: v4_1.z.any().optional(),
    after: v4_1.z.any().optional(),
})
    .optional();
const DurationMsSchema = v4_1.z.number().int().nonnegative().finite();
exports.ClientConfigSchema = v4_1.z.object({
    token: v4_1.z.string(),
    tls_config: ClientTLSConfigSchema,
    healthcheck: HealthcheckConfigSchema.optional(),
    host_port: v4_1.z.string(),
    api_url: v4_1.z.string(),
    log_level: v4_1.z.enum(['OFF', 'DEBUG', 'INFO', 'WARN', 'ERROR']).optional(),
    tenant_id: v4_1.z.string(),
    namespace: v4_1.z.string().optional(),
    otel: exports.OpenTelemetryConfigSchema.optional(),
    middleware: TaskMiddlewareSchema,
    cancellation_grace_period: DurationMsSchema.optional().default(1000),
    cancellation_warning_threshold: DurationMsSchema.optional().default(300),
    grpc_max_recv_message_length: v4_1.z
        .number()
        .int()
        .positive()
        .optional()
        .default(4 * 1024 * 1024),
    grpc_max_send_message_length: v4_1.z
        .number()
        .int()
        .positive()
        .optional()
        .default(4 * 1024 * 1024),
    retrier: exports.RetrierConfigSchema.optional(),
});
