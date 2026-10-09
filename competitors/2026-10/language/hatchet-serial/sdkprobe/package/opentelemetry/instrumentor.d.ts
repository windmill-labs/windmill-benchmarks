/**
 * Hatchet OpenTelemetry Instrumentor
 *
 * This module provides automatic instrumentation for Hatchet SDK operations
 * including workflow runs, event pushes, and step executions.
 *
 * The instrumentor follows the OpenTelemetry instrumentation pattern,
 * patching module prototypes to automatically instrument all instances.
 */
import type { InstrumentationConfig } from '@opentelemetry/instrumentation';
import type { ClientConfig } from '../clients/hatchet-client/client-config';
import { OpenTelemetryConfig } from './types';
import type { HatchetBspConfig } from './hatchet-exporter';
declare const InstrumentationBase: typeof import("@opentelemetry/instrumentation").InstrumentationBase, InstrumentationNodeModuleDefinition: typeof import("@opentelemetry/instrumentation").InstrumentationNodeModuleDefinition;
type HatchetInstrumentationConfig = OpenTelemetryConfig & InstrumentationConfig & {
    /**
     * Enable sending traces to the Hatchet engine's OTLP collector (default: true).
     * Requires @opentelemetry/exporter-trace-otlp-grpc and @opentelemetry/sdk-trace-base.
     * Connection settings (endpoint, token, TLS) are read from the provided clientConfig
     * or from the same environment variables used by the Hatchet client.
     * Set to false to disable.
     */
    enableHatchetCollector?: boolean;
    /**
     * The Hatchet ClientConfig to use for the collector connection.
     * If not provided and enableHatchetCollector is true, config will be loaded
     * from environment variables / .hatchet.yaml.
     */
    clientConfig?: ClientConfig;
    /**
     * Configuration for the BatchSpanProcessor that sends spans to the Hatchet collector.
     */
    bspConfig?: HatchetBspConfig;
};
/**
 * HatchetInstrumentor provides OpenTelemetry instrumentation for Hatchet SDK v1.
 *
 * It automatically instruments:
 * - Workflow runs (runWorkflow, runWorkflows)
 * - Scheduled workflow runs (schedules.create)
 * - Event pushes (push, bulkPush)
 * - Step executions (handleStartStepRun, handleCancelStepRun)
 *
 * Traceparent context is automatically propagated through metadata.
 *
 * The instrumentor uses the global tracer/meter providers by default.
 * Use `setTracerProvider()` and `setMeterProvider()` to configure custom providers.
 */
export declare class HatchetInstrumentor extends InstrumentationBase<HatchetInstrumentationConfig> {
    constructor(config?: Partial<HatchetInstrumentationConfig>);
    /**
     * Sets up the Hatchet OTLP exporter on the current TracerProvider.
     * Loads client config from environment if not provided.
     */
    private _setupHatchetCollector;
    setConfig(config?: Partial<HatchetInstrumentationConfig>): void;
    private readonly _getConfig;
    protected init(): InstanceType<typeof InstrumentationNodeModuleDefinition>[];
    private patchEventClient;
    private unpatchEventClient;
    private _patchPushEvent;
    private _patchBulkPushEvent;
    private patchAdminClient;
    private unpatchAdminClient;
    private _patchRunWorkflow;
    private _patchRunWorkflows;
    private patchWorker;
    private unpatchWorker;
    private _patchHandleStartStepRun;
    private _patchHandleCancelStepRun;
    private patchScheduleClient;
    private unpatchScheduleClient;
    private _patchScheduleCreate;
    private patchDurableContext;
    private unpatchDurableContext;
    private _patchWaitFor;
}
export default HatchetInstrumentor;
