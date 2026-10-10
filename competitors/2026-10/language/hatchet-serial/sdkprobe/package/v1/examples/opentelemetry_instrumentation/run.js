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
const api_1 = require("@opentelemetry/api");
const hatchet_client_1 = require("../hatchet-client");
const setup_1 = require("./setup");
const worker_1 = require("./worker");
(0, setup_1.initOtel)();
const tracer = (0, setup_1.getTracer)('otel-instrumentation-triggers');
function runWorkflow() {
    return __awaiter(this, void 0, void 0, function* () {
        return tracer.startActiveSpan('trigger_order_processing', (span) => __awaiter(this, void 0, void 0, function* () {
            try {
                const workflowRun = yield hatchet_client_1.hatchet.admin.runWorkflow(worker_1.orderWorkflow.name, {
                    orderId: 'order-123',
                    customerId: 'cust-456',
                    amount: 4999,
                });
                const runId = yield workflowRun.getWorkflowRunId();
                console.log(`Started workflow run: ${runId}`);
                const result = yield workflowRun.output;
                span.setStatus({ code: api_1.SpanStatusCode.OK, message: 'Order processed' });
                console.log('Order completed:', result);
            }
            catch (error) {
                const errorMessage = Array.isArray(error)
                    ? error.join(', ')
                    : (error === null || error === void 0 ? void 0 : error.message) || String(error);
                console.error('Order processing failed:', errorMessage);
                span.recordException(error);
                span.setStatus({ code: api_1.SpanStatusCode.ERROR, message: errorMessage });
            }
            finally {
                span.end();
            }
        }));
    });
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        yield runWorkflow();
        console.log('Waiting for spans to be exported...');
        yield new Promise((resolve) => setTimeout(resolve, 5000));
        process.exit(0);
    });
}
if (require.main === module) {
    main().catch(console.error);
}
