"use strict";
/**
 * Shared async context for propagating hatchet.* attributes to child spans.
 *
 * Uses AsyncLocalStorage to store hatchet attributes from the active task run
 * span so that HatchetAttributeSpanProcessor can inject them into all child
 * spans created within the same async context.
 *
 * This mirrors the Go SDK's context.WithValue approach and the Python SDK's
 * ContextVar approach.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.hatchetSpanAttributes = void 0;
exports.setHatchetSpanAttributes = setHatchetSpanAttributes;
const async_hooks_1 = require("async_hooks");
/**
 * AsyncLocalStorage that holds hatchet.* attributes from the active
 * hatchet task run span so they can be injected into child spans.
 */
exports.hatchetSpanAttributes = new async_hooks_1.AsyncLocalStorage();
/**
 * Store hatchet attributes in async context so the SpanProcessor
 * can inject them into child spans.
 */
function setHatchetSpanAttributes(attrs) {
    exports.hatchetSpanAttributes.enterWith(attrs);
}
