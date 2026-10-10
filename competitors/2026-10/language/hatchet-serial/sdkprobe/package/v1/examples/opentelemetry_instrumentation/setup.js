"use strict";
/**
 * Shared OTel instrumentor setup.
 *
 * Import and call initOtel() once at process startup (in both worker and run)
 * before using the tracer.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.initOtel = initOtel;
exports.getTracer = getTracer;
/* eslint-disable @typescript-eslint/no-require-imports */
const { registerInstrumentations } = require('@opentelemetry/instrumentation');
const { trace } = require('@opentelemetry/api');
const opentelemetry_1 = require("../../../opentelemetry");
function initOtel() {
    registerInstrumentations({
        instrumentations: [
            new opentelemetry_1.HatchetInstrumentor({
                includeTaskNameInSpanName: true,
            }),
        ],
    });
}
function getTracer(name) {
    return trace.getTracer(name);
}
