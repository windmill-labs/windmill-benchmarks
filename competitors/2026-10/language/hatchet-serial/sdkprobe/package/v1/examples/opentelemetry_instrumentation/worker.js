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
exports.orderWorkflow = void 0;
const hatchet_client_1 = require("../hatchet-client");
const setup_1 = require("./setup");
const api_1 = require("@opentelemetry/api");
(0, setup_1.initOtel)();
const tracer = (0, setup_1.getTracer)('otel-instrumentation-worker');
exports.orderWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'otel-order-processing-ts',
});
const validateOrder = exports.orderWorkflow.task({
    name: 'validate-order',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield tracer.startActiveSpan('order.validate.schema', (span) => __awaiter(void 0, void 0, void 0, function* () {
            yield new Promise((resolve) => setTimeout(resolve, 10));
            span.setAttribute('order.id', input.orderId);
            span.end();
        }));
        yield tracer.startActiveSpan('order.validate.fraud-check', (span) => __awaiter(void 0, void 0, void 0, function* () {
            yield new Promise((resolve) => setTimeout(resolve, 20));
            span.setAttribute('fraud.score', 0.05);
            span.setAttribute('fraud.decision', 'allow');
            span.end();
        }));
        return { valid: true, orderId: input.orderId };
    }),
});
const chargePayment = exports.orderWorkflow.task({
    name: 'charge-payment',
    parents: [validateOrder],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const validated = yield ctx.parentOutput(validateOrder);
        return tracer.startActiveSpan('payment.process', (paySpan) => __awaiter(void 0, void 0, void 0, function* () {
            try {
                yield tracer.startActiveSpan('payment.tokenize-card', (span) => __awaiter(void 0, void 0, void 0, function* () {
                    yield new Promise((resolve) => setTimeout(resolve, 15));
                    span.setAttribute('payment.provider', 'stripe');
                    span.end();
                }));
                yield tracer.startActiveSpan('payment.charge', (span) => __awaiter(void 0, void 0, void 0, function* () {
                    yield new Promise((resolve) => setTimeout(resolve, 30));
                    span.setAttribute('payment.amount_cents', input.amount);
                    span.setAttribute('payment.currency', 'USD');
                    span.end();
                }));
                paySpan.setStatus({ code: api_1.SpanStatusCode.OK });
                return {
                    transactionId: `txn-${validated.orderId}`,
                    charged: input.amount,
                };
            }
            finally {
                paySpan.end();
            }
        }));
    }),
});
const reserveInventory = exports.orderWorkflow.task({
    name: 'reserve-inventory',
    parents: [validateOrder],
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield tracer.startActiveSpan('inventory.check-availability', (span) => __awaiter(void 0, void 0, void 0, function* () {
            yield new Promise((resolve) => setTimeout(resolve, 10));
            span.setAttribute('inventory.sku_count', 3);
            span.setAttribute('inventory.all_available', true);
            span.end();
        }));
        yield tracer.startActiveSpan('inventory.reserve', (span) => __awaiter(void 0, void 0, void 0, function* () {
            yield new Promise((resolve) => setTimeout(resolve, 15));
            span.setAttribute('inventory.warehouse', 'us-east-1');
            span.end();
        }));
        return {
            reservationId: `res-${input.orderId}`,
            itemsReserved: 3,
        };
    }),
});
exports.orderWorkflow.task({
    name: 'send-confirmation',
    parents: [chargePayment, reserveInventory],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const payment = yield ctx.parentOutput(chargePayment);
        const inventory = yield ctx.parentOutput(reserveInventory);
        yield tracer.startActiveSpan('notification.render-template', (span) => __awaiter(void 0, void 0, void 0, function* () {
            yield new Promise((resolve) => setTimeout(resolve, 5));
            span.setAttribute('template.name', 'order-confirmation');
            span.end();
        }));
        yield tracer.startActiveSpan('notification.send-email', (span) => __awaiter(void 0, void 0, void 0, function* () {
            yield new Promise((resolve) => setTimeout(resolve, 20));
            span.setAttribute('email.to', 'customer@example.com');
            span.setAttribute('email.provider', 'sendgrid');
            span.end();
        }));
        return {
            sent: true,
            transactionId: payment.transactionId,
            reservationId: inventory.reservationId,
        };
    }),
});
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet_client_1.hatchet.worker('otel-instrumentation-worker-ts', {
            workflows: [exports.orderWorkflow],
        });
        yield worker.start();
    });
}
if (require.main === module) {
    main().catch(console.error);
}
