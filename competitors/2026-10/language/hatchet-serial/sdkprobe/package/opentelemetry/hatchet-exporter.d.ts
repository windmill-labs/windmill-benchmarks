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
import type { ClientConfig } from '../clients/hatchet-client/client-config';
declare const BatchSpanProcessor: typeof import("@opentelemetry/sdk-trace-base").BatchSpanProcessor;
type ReadableSpan = import('@opentelemetry/sdk-trace-base').ReadableSpan;
type SdkSpan = import('@opentelemetry/sdk-trace-base').Span;
/**
 * HatchetAttributeSpanProcessor wraps a BatchSpanProcessor and injects
 * hatchet.* attributes into every span created within a step run context.
 * This ensures child spans are queryable by the same attributes (e.g.
 * hatchet.step_run_id) as the parent span.
 */
declare class HatchetAttributeSpanProcessor extends BatchSpanProcessor {
    onStart(span: SdkSpan): void;
    onEnd(span: ReadableSpan): void;
}
export interface HatchetBspConfig {
    scheduledDelayMillis?: number;
    maxExportBatchSize?: number;
    maxQueueSize?: number;
}
/**
 * Creates a SpanProcessor that sends spans to the Hatchet engine's
 * collector endpoint using the same connection settings as the Hatchet client.
 * Pass the returned processor to BasicTracerProvider's `spanProcessors` option.
 */
export declare function createHatchetSpanProcessor(config: ClientConfig, bspConfig?: HatchetBspConfig): InstanceType<typeof HatchetAttributeSpanProcessor>;
export {};
