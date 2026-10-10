"use strict";
/**
 * Hatchet OpenTelemetry Instrumentor
 *
 * This module provides automatic instrumentation for Hatchet SDK operations
 * including workflow runs, event pushes, and step executions.
 *
 * The instrumentor follows the OpenTelemetry instrumentation pattern,
 * patching module prototypes to automatically instrument all instances.
 */
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
exports.HatchetInstrumentor = void 0;
const version_1 = require("../version");
const action_listener_1 = require("../clients/dispatcher/action-listener");
const opentelemetry_1 = require("../util/opentelemetry");
const parse_1 = require("../util/parse");
const types_1 = require("./types");
const hatchet_span_context_1 = require("./hatchet-span-context");
try {
    require.resolve('@opentelemetry/api');
    require.resolve('@opentelemetry/instrumentation');
}
catch (_a) {
    throw new Error('To use HatchetInstrumentor, you must install OpenTelemetry packages: npm install @opentelemetry/api @opentelemetry/instrumentation');
}
/* eslint-disable @typescript-eslint/no-require-imports */
const otelApi = require('@opentelemetry/api');
const otelInstrumentation = require('@opentelemetry/instrumentation');
/* eslint-enable @typescript-eslint/no-require-imports */
const { context, propagation, SpanKind, SpanStatusCode, diag, trace } = otelApi;
const { InstrumentationBase, InstrumentationNodeModuleDefinition, InstrumentationNodeModuleFile, isWrapped, } = otelInstrumentation;
const INSTRUMENTOR_NAME = '@hatchet-dev/typescript-sdk';
// FIXME: refactor version check to use the new pattern introduced in #2954
const SUPPORTED_VERSIONS = ['>=1.16.0'];
function extractContext(carrier) {
    return propagation.extract(context.active(), carrier !== null && carrier !== void 0 ? carrier : {});
}
function injectContext(carrier) {
    propagation.inject(context.active(), carrier);
}
function injectSourceInfo(carrier) {
    const store = hatchet_span_context_1.hatchetSpanAttributes.getStore();
    if (!store)
        return;
    const wfRunId = store['hatchet.workflow_run_id'];
    const stepRunId = store['hatchet.step_run_id'];
    if (typeof wfRunId === 'string' && typeof stepRunId === 'string') {
        carrier['hatchet__source_workflow_run_id'] = wfRunId;
        carrier['hatchet__source_step_run_id'] = stepRunId;
    }
}
function getActionOtelAttributes(action, excludedAttributes = [], workerId) {
    const attributes = {
        [opentelemetry_1.OTelAttribute.TENANT_ID]: action.tenantId,
        [opentelemetry_1.OTelAttribute.WORKER_ID]: workerId,
        [opentelemetry_1.OTelAttribute.WORKFLOW_RUN_ID]: action.workflowRunId,
        [opentelemetry_1.OTelAttribute.STEP_ID]: action.taskId,
        [opentelemetry_1.OTelAttribute.STEP_RUN_ID]: action.taskRunExternalId,
        [opentelemetry_1.OTelAttribute.RETRY_COUNT]: action.retryCount,
        [opentelemetry_1.OTelAttribute.PARENT_WORKFLOW_RUN_ID]: action.parentWorkflowRunId,
        [opentelemetry_1.OTelAttribute.CHILD_WORKFLOW_INDEX]: action.childWorkflowIndex,
        [opentelemetry_1.OTelAttribute.CHILD_WORKFLOW_KEY]: action.childWorkflowKey,
        [opentelemetry_1.OTelAttribute.ACTION_PAYLOAD]: action.actionPayload,
        [opentelemetry_1.OTelAttribute.WORKFLOW_NAME]: (0, action_listener_1.workflowNameFromAction)(action),
        [opentelemetry_1.OTelAttribute.ACTION_NAME]: action.actionId,
        [opentelemetry_1.OTelAttribute.STEP_NAME]: action.taskName,
        [opentelemetry_1.OTelAttribute.WORKFLOW_ID]: action.workflowId,
        [opentelemetry_1.OTelAttribute.WORKFLOW_VERSION_ID]: action.workflowVersionId,
    };
    const filtered = { instrumentor: 'hatchet' };
    for (const [key, value] of Object.entries(attributes)) {
        if (!excludedAttributes.includes(key) && value !== undefined && value !== '') {
            filtered[key] = value;
        }
    }
    return filtered;
}
function filterAttributes(attributes, excludedAttributes = []) {
    const filtered = { instrumentor: 'hatchet' };
    for (const [key, value] of Object.entries(attributes)) {
        if (!excludedAttributes.includes(key) &&
            value !== undefined &&
            value !== null &&
            value !== '' &&
            value !== '{}' &&
            value !== '[]') {
            filtered[`hatchet.${key}`] =
                typeof value === 'object' ? JSON.stringify(value) : value;
        }
    }
    return filtered;
}
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
class HatchetInstrumentor extends InstrumentationBase {
    constructor(config = {}) {
        const mergedConfig = Object.assign(Object.assign({}, types_1.DEFAULT_CONFIG), config);
        super(INSTRUMENTOR_NAME, version_1.HATCHET_VERSION, mergedConfig);
        this._getConfig = () => this.getConfig();
        if (mergedConfig.enableHatchetCollector) {
            this._setupHatchetCollector(config.clientConfig, config.bspConfig);
        }
    }
    /**
     * Sets up the Hatchet OTLP exporter on the current TracerProvider.
     * Loads client config from environment if not provided.
     */
    _setupHatchetCollector(clientConfig, bspConfig) {
        try {
            /* eslint-disable @typescript-eslint/no-require-imports */
            const { createHatchetSpanProcessor } = require('./hatchet-exporter.js');
            let config = clientConfig;
            if (!config) {
                const { ConfigLoader } = require('../util/config-loader/config-loader');
                config = ConfigLoader.loadClientConfig();
            }
            const processor = createHatchetSpanProcessor(config, bspConfig);
            try {
                const sdkTrace = require('@opentelemetry/sdk-trace-base');
                /* eslint-enable @typescript-eslint/no-require-imports */
                const globalProvider = otelApi.trace.getTracerProvider();
                if (!(globalProvider instanceof sdkTrace.BasicTracerProvider)) {
                    const sdkTracerProvider = new sdkTrace.BasicTracerProvider({
                        spanProcessors: [processor],
                    });
                    otelApi.trace.setGlobalTracerProvider(sdkTracerProvider);
                }
                else {
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    globalProvider.addSpanProcessor(processor);
                }
            }
            catch (_a) {
                diag.warn('hatchet instrumentation: @opentelemetry/sdk-trace-base is required for enableHatchetCollector');
                return;
            }
            diag.info('hatchet instrumentation: Hatchet OTLP collector enabled');
        }
        catch (e) {
            diag.warn(`hatchet instrumentation: Failed to set up Hatchet collector: ${e}`);
        }
    }
    setConfig(config = {}) {
        super.setConfig(Object.assign(Object.assign({}, types_1.DEFAULT_CONFIG), config));
    }
    init() {
        const eventClientModuleFile = new InstrumentationNodeModuleFile('@hatchet-dev/typescript-sdk/clients/event/event-client.js', SUPPORTED_VERSIONS, this.patchEventClient.bind(this), this.unpatchEventClient.bind(this));
        const adminClientModuleFile = new InstrumentationNodeModuleFile('@hatchet-dev/typescript-sdk/v1/client/admin.js', SUPPORTED_VERSIONS, this.patchAdminClient.bind(this), this.unpatchAdminClient.bind(this));
        const scheduleClientModuleFile = new InstrumentationNodeModuleFile('@hatchet-dev/typescript-sdk/v1/client/features/schedules.js', SUPPORTED_VERSIONS, this.patchScheduleClient.bind(this), this.unpatchScheduleClient.bind(this));
        const workerModuleFile = new InstrumentationNodeModuleFile('@hatchet-dev/typescript-sdk/v1/client/worker/worker-internal.js', SUPPORTED_VERSIONS, this.patchWorker.bind(this), this.unpatchWorker.bind(this));
        const durableContextModuleFile = new InstrumentationNodeModuleFile('@hatchet-dev/typescript-sdk/v1/client/worker/context.js', SUPPORTED_VERSIONS, this.patchDurableContext.bind(this), this.unpatchDurableContext.bind(this));
        const moduleDefinition = new InstrumentationNodeModuleDefinition(INSTRUMENTOR_NAME, SUPPORTED_VERSIONS, undefined, undefined, [
            eventClientModuleFile,
            adminClientModuleFile,
            workerModuleFile,
            scheduleClientModuleFile,
            durableContextModuleFile,
        ]);
        return [moduleDefinition];
    }
    patchEventClient(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.EventClient) === null || _a === void 0 ? void 0 : _a.prototype)) {
            diag.debug('hatchet instrumentation: EventClient not found in module exports');
            return moduleExports;
        }
        this._patchPushEvent(exports.EventClient.prototype);
        this._patchBulkPushEvent(exports.EventClient.prototype);
        return moduleExports;
    }
    unpatchEventClient(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.EventClient) === null || _a === void 0 ? void 0 : _a.prototype)) {
            return moduleExports;
        }
        if (isWrapped(exports.EventClient.prototype.push)) {
            this._unwrap(exports.EventClient.prototype, 'push');
        }
        if (isWrapped(exports.EventClient.prototype.bulkPush)) {
            this._unwrap(exports.EventClient.prototype, 'bulkPush');
        }
        return moduleExports;
    }
    _patchPushEvent(prototype) {
        if (isWrapped(prototype.push)) {
            this._unwrap(prototype, 'push');
        }
        const { tracer, _getConfig: getConfig } = this;
        this._wrap(prototype, 'push', (original) => {
            return function wrappedPush(type, input, options = {}) {
                const attributes = filterAttributes({
                    [opentelemetry_1.OTelAttribute.EVENT_KEY]: type,
                    [opentelemetry_1.OTelAttribute.ACTION_PAYLOAD]: JSON.stringify(input),
                    [opentelemetry_1.OTelAttribute.ADDITIONAL_METADATA]: options.additionalMetadata
                        ? JSON.stringify(options.additionalMetadata)
                        : undefined,
                    [opentelemetry_1.OTelAttribute.PRIORITY]: options.priority,
                    [opentelemetry_1.OTelAttribute.FILTER_SCOPE]: options.scope,
                }, getConfig().excludedAttributes);
                return tracer.startActiveSpan('hatchet.push_event', {
                    kind: SpanKind.PRODUCER,
                    attributes,
                }, (span) => {
                    var _a;
                    const enhancedMetadata = Object.assign({}, ((_a = options.additionalMetadata) !== null && _a !== void 0 ? _a : {}));
                    injectContext(enhancedMetadata);
                    injectSourceInfo(enhancedMetadata);
                    const enhancedOptions = Object.assign(Object.assign({}, options), { additionalMetadata: enhancedMetadata });
                    const result = original.call(this, type, input, enhancedOptions);
                    return result.finally(() => {
                        span.end();
                    });
                });
            };
        });
    }
    _patchBulkPushEvent(prototype) {
        if (isWrapped(prototype.bulkPush)) {
            this._unwrap(prototype, 'bulkPush');
        }
        const { tracer, _getConfig: getConfig } = this;
        this._wrap(prototype, 'bulkPush', (original) => {
            return function wrappedBulkPush(type, inputs, options = {}) {
                const attributes = filterAttributes({
                    [opentelemetry_1.OTelAttribute.EVENT_KEY]: type,
                    [opentelemetry_1.OTelAttribute.ACTION_PAYLOAD]: JSON.stringify(inputs),
                    [opentelemetry_1.OTelAttribute.ADDITIONAL_METADATA]: options.additionalMetadata
                        ? JSON.stringify(options.additionalMetadata)
                        : undefined,
                    [opentelemetry_1.OTelAttribute.PRIORITY]: options.priority,
                }, getConfig().excludedAttributes);
                return tracer.startActiveSpan('hatchet.bulk_push_event', {
                    kind: SpanKind.PRODUCER,
                    attributes,
                }, (span) => {
                    const enhancedInputs = inputs.map((input) => {
                        var _a;
                        const enhancedMetadata = Object.assign({}, ((_a = input.additionalMetadata) !== null && _a !== void 0 ? _a : {}));
                        injectContext(enhancedMetadata);
                        injectSourceInfo(enhancedMetadata);
                        return Object.assign(Object.assign({}, input), { additionalMetadata: enhancedMetadata });
                    });
                    const result = original.call(this, type, enhancedInputs, options);
                    return result.finally(() => {
                        span.end();
                    });
                });
            };
        });
    }
    patchAdminClient(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.AdminClient) === null || _a === void 0 ? void 0 : _a.prototype)) {
            diag.debug('hatchet instrumentation: AdminClient not found in module exports');
            return moduleExports;
        }
        this._patchRunWorkflow(exports.AdminClient.prototype);
        this._patchRunWorkflows(exports.AdminClient.prototype);
        return moduleExports;
    }
    unpatchAdminClient(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.AdminClient) === null || _a === void 0 ? void 0 : _a.prototype)) {
            return moduleExports;
        }
        if (isWrapped(exports.AdminClient.prototype.runWorkflow)) {
            this._unwrap(exports.AdminClient.prototype, 'runWorkflow');
        }
        if (isWrapped(exports.AdminClient.prototype.runWorkflows)) {
            this._unwrap(exports.AdminClient.prototype, 'runWorkflows');
        }
        return moduleExports;
    }
    _patchRunWorkflow(prototype) {
        if (isWrapped(prototype.runWorkflow)) {
            this._unwrap(prototype, 'runWorkflow');
        }
        const { tracer, _getConfig: getConfig } = this;
        this._wrap(prototype, 'runWorkflow', (original) => {
            return function wrappedRunWorkflow(workflowName, input, options) {
                return __awaiter(this, void 0, void 0, function* () {
                    const attributes = filterAttributes({
                        [opentelemetry_1.OTelAttribute.WORKFLOW_NAME]: workflowName,
                        [opentelemetry_1.OTelAttribute.ACTION_PAYLOAD]: JSON.stringify(input),
                        [opentelemetry_1.OTelAttribute.PARENT_ID]: options === null || options === void 0 ? void 0 : options.parentId,
                        [opentelemetry_1.OTelAttribute.PARENT_STEP_RUN_ID]: options === null || options === void 0 ? void 0 : options.parentStepRunId,
                        [opentelemetry_1.OTelAttribute.CHILD_INDEX]: options === null || options === void 0 ? void 0 : options.childIndex,
                        [opentelemetry_1.OTelAttribute.CHILD_KEY]: options === null || options === void 0 ? void 0 : options.childKey,
                        [opentelemetry_1.OTelAttribute.ADDITIONAL_METADATA]: (options === null || options === void 0 ? void 0 : options.additionalMetadata)
                            ? JSON.stringify(options.additionalMetadata)
                            : undefined,
                        [opentelemetry_1.OTelAttribute.PRIORITY]: options === null || options === void 0 ? void 0 : options.priority,
                        [opentelemetry_1.OTelAttribute.DESIRED_WORKER_ID]: options === null || options === void 0 ? void 0 : options.desiredWorkerId,
                    }, getConfig().excludedAttributes);
                    return tracer.startActiveSpan('hatchet.run_workflow', {
                        kind: SpanKind.PRODUCER,
                        attributes,
                    }, (span) => {
                        var _a;
                        const enhancedMetadata = Object.assign({}, ((_a = options === null || options === void 0 ? void 0 : options.additionalMetadata) !== null && _a !== void 0 ? _a : {}));
                        injectContext(enhancedMetadata);
                        const enhancedOptions = Object.assign(Object.assign({}, options), { additionalMetadata: enhancedMetadata });
                        return original
                            .call(this, workflowName, input, enhancedOptions)
                            .catch((error) => {
                            span.recordException(error);
                            span.setStatus({ code: SpanStatusCode.ERROR, message: error === null || error === void 0 ? void 0 : error.message });
                            throw error;
                        })
                            .finally(() => {
                            span.end();
                        });
                    });
                });
            };
        });
    }
    _patchRunWorkflows(prototype) {
        if (isWrapped(prototype.runWorkflows)) {
            this._unwrap(prototype, 'runWorkflows');
        }
        const { tracer, _getConfig: getConfig } = this;
        this._wrap(prototype, 'runWorkflows', (original) => {
            return function wrappedRunWorkflows(workflowRuns, batchSize) {
                return __awaiter(this, void 0, void 0, function* () {
                    const attributes = filterAttributes({
                        [opentelemetry_1.OTelAttribute.WORKFLOW_NAME]: JSON.stringify(workflowRuns.map((r) => r.workflowName)),
                        [opentelemetry_1.OTelAttribute.ACTION_PAYLOAD]: JSON.stringify(workflowRuns),
                    }, getConfig().excludedAttributes);
                    return tracer.startActiveSpan('hatchet.run_workflows', {
                        kind: SpanKind.PRODUCER,
                        attributes,
                    }, (batchSpan) => {
                        const individualSpans = getConfig().individualRunSpansForBulkRun;
                        const itemSpans = [];
                        let enhancedWorkflowRuns;
                        try {
                            enhancedWorkflowRuns = workflowRuns.map((run) => {
                                var _a, _b, _c, _d, _e, _f, _g, _h, _j;
                                const enhancedMetadata = Object.assign({}, ((_b = (_a = run.options) === null || _a === void 0 ? void 0 : _a.additionalMetadata) !== null && _b !== void 0 ? _b : {}));
                                if (individualSpans) {
                                    const itemAttributes = filterAttributes({
                                        [opentelemetry_1.OTelAttribute.WORKFLOW_NAME]: run.workflowName,
                                        [opentelemetry_1.OTelAttribute.ACTION_PAYLOAD]: JSON.stringify(run.input),
                                        [opentelemetry_1.OTelAttribute.PARENT_ID]: (_c = run.options) === null || _c === void 0 ? void 0 : _c.parentId,
                                        [opentelemetry_1.OTelAttribute.PARENT_STEP_RUN_ID]: (_d = run.options) === null || _d === void 0 ? void 0 : _d.parentStepRunId,
                                        [opentelemetry_1.OTelAttribute.CHILD_INDEX]: (_e = run.options) === null || _e === void 0 ? void 0 : _e.childIndex,
                                        [opentelemetry_1.OTelAttribute.CHILD_KEY]: (_f = run.options) === null || _f === void 0 ? void 0 : _f.childKey,
                                        [opentelemetry_1.OTelAttribute.ADDITIONAL_METADATA]: ((_g = run.options) === null || _g === void 0 ? void 0 : _g.additionalMetadata)
                                            ? JSON.stringify(run.options.additionalMetadata)
                                            : undefined,
                                        [opentelemetry_1.OTelAttribute.PRIORITY]: (_h = run.options) === null || _h === void 0 ? void 0 : _h.priority,
                                        [opentelemetry_1.OTelAttribute.DESIRED_WORKER_ID]: (_j = run.options) === null || _j === void 0 ? void 0 : _j.desiredWorkerId,
                                    }, getConfig().excludedAttributes);
                                    const itemSpan = tracer.startSpan('hatchet.run_workflow', {
                                        kind: SpanKind.PRODUCER,
                                        attributes: itemAttributes,
                                    });
                                    itemSpans.push(itemSpan);
                                    propagation.inject(trace.setSpan(context.active(), itemSpan), enhancedMetadata);
                                }
                                else {
                                    // Legacy behaviour: inject the active (batch) span context so the
                                    // span structure seen by downstream collectors is unchanged.
                                    injectContext(enhancedMetadata);
                                }
                                return Object.assign(Object.assign({}, run), { options: Object.assign(Object.assign({}, run.options), { additionalMetadata: enhancedMetadata }) });
                            });
                        }
                        catch (error) {
                            const err = error;
                            batchSpan.recordException(err);
                            batchSpan.setStatus({ code: SpanStatusCode.ERROR, message: err === null || err === void 0 ? void 0 : err.message });
                            itemSpans.forEach((s) => {
                                s.recordException(err);
                                s.setStatus({ code: SpanStatusCode.ERROR, message: err === null || err === void 0 ? void 0 : err.message });
                                s.end();
                            });
                            batchSpan.end();
                            throw error;
                        }
                        try {
                            return original
                                .call(this, enhancedWorkflowRuns, batchSize)
                                .catch((error) => {
                                batchSpan.recordException(error);
                                batchSpan.setStatus({ code: SpanStatusCode.ERROR, message: error === null || error === void 0 ? void 0 : error.message });
                                itemSpans.forEach((s) => {
                                    s.recordException(error);
                                    s.setStatus({ code: SpanStatusCode.ERROR, message: error === null || error === void 0 ? void 0 : error.message });
                                });
                                throw error;
                            })
                                .finally(() => {
                                itemSpans.forEach((s) => s.end());
                                batchSpan.end();
                            });
                        }
                        catch (error) {
                            const err = error;
                            batchSpan.recordException(err);
                            batchSpan.setStatus({ code: SpanStatusCode.ERROR, message: err === null || err === void 0 ? void 0 : err.message });
                            itemSpans.forEach((s) => {
                                s.recordException(err);
                                s.setStatus({ code: SpanStatusCode.ERROR, message: err === null || err === void 0 ? void 0 : err.message });
                                s.end();
                            });
                            batchSpan.end();
                            throw error;
                        }
                    });
                });
            };
        });
    }
    patchWorker(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.InternalWorker) === null || _a === void 0 ? void 0 : _a.prototype)) {
            diag.debug('hatchet instrumentation: InternalWorker not found in module exports');
            return moduleExports;
        }
        this._patchHandleStartStepRun(exports.InternalWorker.prototype);
        this._patchHandleCancelStepRun(exports.InternalWorker.prototype);
        return moduleExports;
    }
    unpatchWorker(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.InternalWorker) === null || _a === void 0 ? void 0 : _a.prototype)) {
            return moduleExports;
        }
        if (isWrapped(exports.InternalWorker.prototype.handleStartStepRun)) {
            this._unwrap(exports.InternalWorker.prototype, 'handleStartStepRun');
        }
        if (isWrapped(exports.InternalWorker.prototype.handleCancelStepRun)) {
            this._unwrap(exports.InternalWorker.prototype, 'handleCancelStepRun');
        }
        return moduleExports;
    }
    // IMPORTANT: Keep this wrapper's signature in sync with InternalWorker.handleStartStepRun
    _patchHandleStartStepRun(prototype) {
        if (isWrapped(prototype.handleStartStepRun)) {
            this._unwrap(prototype, 'handleStartStepRun');
        }
        const { tracer, _getConfig: getConfig } = this;
        this._wrap(prototype, 'handleStartStepRun', (original) => {
            return function wrappedHandleStartStepRun(action) {
                return __awaiter(this, void 0, void 0, function* () {
                    const additionalMetadata = action.additionalMetadata
                        ? (0, parse_1.parseJSON)(action.additionalMetadata)
                        : undefined;
                    const parentContext = extractContext(additionalMetadata);
                    const attributes = getActionOtelAttributes(action, getConfig().excludedAttributes, this.workerId);
                    let spanName = 'hatchet.start_step_run';
                    if (getConfig().includeTaskNameInSpanName) {
                        spanName += `.${action.actionId}`;
                    }
                    // Store hatchet.* attributes in async context so the SpanProcessor
                    // can inject them into child spans (mirrors Go/Python attribute propagation).
                    const hatchetAttrs = {};
                    for (const [key, value] of Object.entries(attributes)) {
                        if (value !== undefined) {
                            hatchetAttrs[`hatchet.${key}`] = value;
                        }
                    }
                    (0, hatchet_span_context_1.setHatchetSpanAttributes)(hatchetAttrs);
                    return tracer.startActiveSpan(spanName, {
                        kind: SpanKind.CONSUMER,
                        attributes,
                    }, parentContext, (span) => {
                        return original
                            .call(this, action)
                            .then((taskError) => {
                            if (taskError instanceof Error) {
                                span.recordException(taskError);
                                span.setStatus({ code: SpanStatusCode.ERROR, message: taskError.message });
                            }
                            else {
                                span.setStatus({ code: SpanStatusCode.OK });
                            }
                            return taskError;
                        })
                            .finally(() => {
                            span.end();
                        });
                    });
                });
            };
        });
    }
    _patchHandleCancelStepRun(prototype) {
        if (isWrapped(prototype.handleCancelStepRun)) {
            this._unwrap(prototype, 'handleCancelStepRun');
        }
        const { tracer } = this;
        this._wrap(prototype, 'handleCancelStepRun', (original) => {
            return function wrappedHandleCancelStepRun(action) {
                return __awaiter(this, void 0, void 0, function* () {
                    const attributes = {
                        instrumentor: 'hatchet',
                        [`hatchet.${opentelemetry_1.OTelAttribute.STEP_RUN_ID}`]: action.taskRunExternalId,
                    };
                    return tracer.startActiveSpan('hatchet.cancel_step_run', {
                        kind: SpanKind.CONSUMER,
                        attributes,
                    }, (span) => {
                        const result = original.call(this, action);
                        return result.finally(() => {
                            span.end();
                        });
                    });
                });
            };
        });
    }
    patchScheduleClient(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.ScheduleClient) === null || _a === void 0 ? void 0 : _a.prototype)) {
            diag.debug('hatchet instrumentation: ScheduleClient not found in module exports');
            return moduleExports;
        }
        this._patchScheduleCreate(exports.ScheduleClient.prototype);
        return moduleExports;
    }
    unpatchScheduleClient(moduleExports, _moduleVersion) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.ScheduleClient) === null || _a === void 0 ? void 0 : _a.prototype)) {
            return moduleExports;
        }
        if (isWrapped(exports.ScheduleClient.prototype.create)) {
            this._unwrap(exports.ScheduleClient.prototype, 'create');
        }
        return moduleExports;
    }
    // IMPORTANT: Keep this wrapper's signature in sync with ScheduleClient.create
    _patchScheduleCreate(prototype) {
        if (isWrapped(prototype.create)) {
            this._unwrap(prototype, 'create');
        }
        const { tracer, _getConfig: getConfig } = this;
        this._wrap(prototype, 'create', (original) => {
            return function wrappedCreate(workflow, input) {
                return __awaiter(this, void 0, void 0, function* () {
                    const triggerAtIso = input.triggerAt instanceof Date
                        ? input.triggerAt.toISOString()
                        : new Date(input.triggerAt).toISOString();
                    const attributes = filterAttributes({
                        [opentelemetry_1.OTelAttribute.WORKFLOW_NAME]: workflow,
                        [opentelemetry_1.OTelAttribute.RUN_AT_TIMESTAMPS]: JSON.stringify([triggerAtIso]),
                        [opentelemetry_1.OTelAttribute.ACTION_PAYLOAD]: JSON.stringify(input.input),
                        [opentelemetry_1.OTelAttribute.ADDITIONAL_METADATA]: input.additionalMetadata
                            ? JSON.stringify(input.additionalMetadata)
                            : undefined,
                        [opentelemetry_1.OTelAttribute.PRIORITY]: input.priority,
                    }, getConfig().excludedAttributes);
                    return tracer.startActiveSpan('hatchet.schedule_workflow', {
                        kind: SpanKind.PRODUCER,
                        attributes,
                    }, (span) => {
                        var _a;
                        // Inject traceparent into additionalMetadata for context propagation
                        const enhancedMetadata = Object.assign({}, ((_a = input.additionalMetadata) !== null && _a !== void 0 ? _a : {}));
                        injectContext(enhancedMetadata);
                        const enhancedInput = Object.assign(Object.assign({}, input), { additionalMetadata: enhancedMetadata });
                        return original
                            .call(this, workflow, enhancedInput)
                            .catch((error) => {
                            span.recordException(error);
                            span.setStatus({ code: SpanStatusCode.ERROR, message: error === null || error === void 0 ? void 0 : error.message });
                            throw error;
                        })
                            .finally(() => {
                            span.end();
                        });
                    });
                });
            };
        });
    }
    // --- DurableContext patching ---
    patchDurableContext(moduleExports) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.DurableContext) === null || _a === void 0 ? void 0 : _a.prototype)) {
            return moduleExports;
        }
        this._patchWaitFor(exports.DurableContext.prototype);
        return moduleExports;
    }
    unpatchDurableContext(moduleExports) {
        var _a;
        const exports = moduleExports;
        if (!((_a = exports === null || exports === void 0 ? void 0 : exports.DurableContext) === null || _a === void 0 ? void 0 : _a.prototype)) {
            return;
        }
        if (isWrapped(exports.DurableContext.prototype.waitFor)) {
            this._unwrap(exports.DurableContext.prototype, 'waitFor');
        }
    }
    // IMPORTANT: Keep this wrapper's signature in sync with DurableContext.waitFor
    _patchWaitFor(prototype) {
        if (isWrapped(prototype.waitFor)) {
            this._unwrap(prototype, 'waitFor');
        }
        const { tracer } = this;
        this._wrap(prototype, 'waitFor', (original) => {
            return function wrappedWaitFor(...args) {
                return __awaiter(this, void 0, void 0, function* () {
                    var _a, _b;
                    return tracer.startActiveSpan('hatchet.durable.wait_for', {
                        kind: SpanKind.INTERNAL,
                        attributes: {
                            instrumentor: 'hatchet',
                            'hatchet.step_run_id': (_b = (_a = this.action) === null || _a === void 0 ? void 0 : _a.taskRunExternalId) !== null && _b !== void 0 ? _b : '',
                        },
                    }, (span) => __awaiter(this, void 0, void 0, function* () {
                        try {
                            const result = yield original.apply(this, args);
                            span.setStatus({ code: SpanStatusCode.OK });
                            return result;
                        }
                        catch (error) {
                            if (error instanceof Error) {
                                span.recordException(error);
                                span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
                            }
                            throw error;
                        }
                        finally {
                            span.end();
                        }
                    }));
                });
            };
        });
    }
}
exports.HatchetInstrumentor = HatchetInstrumentor;
exports.default = HatchetInstrumentor;
