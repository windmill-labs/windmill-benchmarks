"use strict";
/**
 * Hatchet OTLP Exporter
 *
 * Creates an OTLP gRPC trace exporter that sends spans to the Hatchet engine's
 * collector endpoint, and a SpanProcessor that injects hatchet.* attributes
 * into all child spans so they are queryable by the same attributes as the parent.
 *
 * This mirrors the Go SDK's EnableHatchetCollector() and the Python SDK's
 * enable_hatchet_otel_collector option.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.createHatchetSpanProcessor = createHatchetSpanProcessor;
const hatchet_span_context_1 = require("./hatchet-span-context");
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let otelDiag;
try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    otelDiag = require('@opentelemetry/api').diag;
}
catch (_a) {
    // best-effort
}
try {
    require.resolve('@opentelemetry/exporter-trace-otlp-grpc');
    require.resolve('@opentelemetry/sdk-trace-base');
    require.resolve('@opentelemetry/core');
}
catch (_b) {
    throw new Error('To use HatchetInstrumentor with enableHatchetCollector, you must install: ' +
        'npm install @opentelemetry/exporter-trace-otlp-grpc @opentelemetry/sdk-trace-base @opentelemetry/core');
}
/* eslint-disable @typescript-eslint/no-require-imports */
// prettier-ignore
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-grpc');
// prettier-ignore
const sdkTraceBase = require('@opentelemetry/sdk-trace-base');
// prettier-ignore
const { ExportResultCode } = require('@opentelemetry/core');
/* eslint-enable @typescript-eslint/no-require-imports */
const { BatchSpanProcessor } = sdkTraceBase;
const GRPC_STATUS_UNIMPLEMENTED = 12;
const RETRY_AFTER_MS = 5 * 60 * 1000;
class HatchetExporterWrapper {
    constructor(inner) {
        this.retryAt = 0;
        this.inner = inner;
    }
    export(spans, resultCallback) {
        var _a;
        if (this.retryAt > 0 && Date.now() < this.retryAt) {
            resultCallback({ code: ExportResultCode.SUCCESS });
            return;
        }
        try {
            this.inner.export(spans, (result) => {
                var _a;
                if (result.code !== ExportResultCode.SUCCESS && result.error) {
                    const err = result.error;
                    if (err.code === GRPC_STATUS_UNIMPLEMENTED ||
                        ((_a = err.message) === null || _a === void 0 ? void 0 : _a.toString().includes('UNIMPLEMENTED'))) {
                        this.retryAt = Date.now() + RETRY_AFTER_MS;
                        resultCallback({ code: ExportResultCode.SUCCESS });
                        return;
                    }
                }
                this.retryAt = 0;
                resultCallback(result);
            });
        }
        catch (e) {
            if (e instanceof TypeError && ((_a = e.message) === null || _a === void 0 ? void 0 : _a.includes("reading 'name'"))) {
                otelDiag === null || otelDiag === void 0 ? void 0 : otelDiag.error('hatchet instrumentation: OpenTelemetry package version mismatch. ' +
                    '@opentelemetry/exporter-trace-otlp-grpc and @opentelemetry/sdk-trace-base must be ' +
                    'from the same release set (1.x + 0.5x.x, or 2.x + 0.20x.x). ' +
                    'See https://github.com/open-telemetry/opentelemetry-js#version-compatibility');
                resultCallback({ code: ExportResultCode.SUCCESS });
                return;
            }
            throw e;
        }
    }
    shutdown() {
        return this.inner.shutdown();
    }
    forceFlush() {
        var _a, _b, _c;
        return (_c = (_b = (_a = this.inner).forceFlush) === null || _b === void 0 ? void 0 : _b.call(_a)) !== null && _c !== void 0 ? _c : Promise.resolve();
    }
}
/**
 * HatchetAttributeSpanProcessor wraps a BatchSpanProcessor and injects
 * hatchet.* attributes into every span created within a step run context.
 * This ensures child spans are queryable by the same attributes (e.g.
 * hatchet.step_run_id) as the parent span.
 */
class HatchetAttributeSpanProcessor extends BatchSpanProcessor {
    onStart(span) {
        var _a;
        const attrs = hatchet_span_context_1.hatchetSpanAttributes.getStore();
        if (attrs) {
            const existing = (_a = span.attributes) !== null && _a !== void 0 ? _a : {};
            const filtered = {};
            for (const [key, value] of Object.entries(attrs)) {
                if (!(key in existing)) {
                    filtered[key] = value;
                }
            }
            if (Object.keys(filtered).length > 0) {
                span.setAttributes(filtered);
            }
        }
        super.onStart(span, undefined);
    }
    onEnd(span) {
        super.onEnd(span);
    }
}
function createHatchetExporter(config) {
    const insecure = config.tls_config.tls_strategy === 'none';
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const grpc = require('@grpc/grpc-js');
    const metadata = new grpc.Metadata();
    metadata.set('authorization', `Bearer ${config.token}`);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const opts = {
        url: `${insecure ? 'http' : 'https'}://${config.host_port}`,
        metadata,
    };
    if (!insecure && config.tls_config.ca_file) {
        try {
            /* eslint-disable @typescript-eslint/no-require-imports */
            const fs = require('fs');
            const { ChannelCredentials } = require('nice-grpc');
            /* eslint-enable @typescript-eslint/no-require-imports */
            const rootCerts = fs.readFileSync(config.tls_config.ca_file);
            opts.credentials = ChannelCredentials.createSsl(rootCerts);
        }
        catch (_a) {
            // Fall through to default TLS handling
        }
    }
    return new OTLPTraceExporter(opts);
}
/**
 * Creates a SpanProcessor that sends spans to the Hatchet engine's
 * collector endpoint using the same connection settings as the Hatchet client.
 * Pass the returned processor to BasicTracerProvider's `spanProcessors` option.
 */
function createHatchetSpanProcessor(config, bspConfig) {
    const inner = createHatchetExporter(config);
    const exporter = new HatchetExporterWrapper(inner);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new HatchetAttributeSpanProcessor(exporter, {
        scheduledDelayMillis: bspConfig === null || bspConfig === void 0 ? void 0 : bspConfig.scheduledDelayMillis,
        maxExportBatchSize: bspConfig === null || bspConfig === void 0 ? void 0 : bspConfig.maxExportBatchSize,
        maxQueueSize: bspConfig === null || bspConfig === void 0 ? void 0 : bspConfig.maxQueueSize,
    });
}
