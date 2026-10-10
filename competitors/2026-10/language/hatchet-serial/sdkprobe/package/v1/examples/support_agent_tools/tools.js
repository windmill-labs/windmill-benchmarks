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
exports.createTicket = exports.checkOrderStatus = exports.lookupCustomer = void 0;
exports.createLookupCustomerToolClaude = createLookupCustomerToolClaude;
exports.createCheckOrderStatusToolClaude = createCheckOrderStatusToolClaude;
exports.createTicketToolClaude = createTicketToolClaude;
exports.createLookupCustomerToolOpenai = createLookupCustomerToolOpenai;
exports.createCheckOrderStatusToolOpenai = createCheckOrderStatusToolOpenai;
exports.createTicketToolOpenai = createTicketToolOpenai;
// > Setup
const v4_1 = require("zod/v4");
const hatchet_client_1 = require("../hatchet-client");
// !!
// > Models
const CustomerLookupInput = v4_1.z.object({
    customerId: v4_1.z.string(),
});
const OrderStatusInput = v4_1.z.object({
    orderId: v4_1.z.string(),
});
const CreateTicketInput = v4_1.z.object({
    customerId: v4_1.z.string(),
    orderId: v4_1.z.string(),
    subject: v4_1.z.string(),
    body: v4_1.z.string(),
    priority: v4_1.z.string(),
});
// !!
// > Fixture data
const CUSTOMERS = {
    'C-100': {
        customerId: 'C-100',
        name: 'Alice Martin',
        email: 'alice@example.com',
        plan: 'business',
        accountStatus: 'active',
        defaultOrderId: 'ORD-9987',
        supportTier: 'priority',
    },
};
const ORDERS = {
    'ORD-9987': {
        orderId: 'ORD-9987',
        status: 'delayed',
        lastUpdated: '2026-05-20T14:30:00Z',
        estimatedDelivery: '2026-05-28',
        knownIssue: 'Carrier reported weather delay at regional hub',
        carrier: 'FastShip',
        trackingNumber: 'FS-482910',
    },
};
// !!
// > Lookup customer
exports.lookupCustomer = hatchet_client_1.hatchet.task({
    name: 'lookup-customer',
    inputValidator: CustomerLookupInput,
    description: 'Look up a customer by ID and return their profile, plan, and support tier.',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const customer = CUSTOMERS[input.customerId];
        if (!customer) {
            return {
                customerId: input.customerId,
                name: 'Unknown',
                email: 'unknown@example.com',
                plan: 'none',
                accountStatus: 'not_found',
                defaultOrderId: '',
                supportTier: 'standard',
            };
        }
        return customer;
    }),
});
// !!
// > Check order status
exports.checkOrderStatus = hatchet_client_1.hatchet.task({
    name: 'check-order-status',
    inputValidator: OrderStatusInput,
    description: 'Check the current status, carrier, and any known issues for an order.',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const order = ORDERS[input.orderId];
        if (!order) {
            return {
                orderId: input.orderId,
                status: 'not_found',
                lastUpdated: '',
                estimatedDelivery: '',
                knownIssue: null,
                carrier: 'unknown',
                trackingNumber: '',
            };
        }
        return order;
    }),
});
// !!
// > Create ticket
exports.createTicket = hatchet_client_1.hatchet.task({
    name: 'create-ticket',
    inputValidator: CreateTicketInput,
    description: 'Create a support ticket for a customer issue and return the ticket ID and routing.',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        const ticketId = `TICKET-${input.customerId}-001`;
        return {
            ticketId,
            status: 'open',
            priority: input.priority,
            routingTeam: 'shipping-support',
            summary: `Ticket ${ticketId} created for ${input.customerId} regarding order ${input.orderId}: ${input.subject}`,
        };
    }),
});
// !!
// > Create Claude tools
function createLookupCustomerToolClaude() {
    return exports.lookupCustomer.mcpTool('claude');
}
function createCheckOrderStatusToolClaude() {
    return exports.checkOrderStatus.mcpTool('claude');
}
function createTicketToolClaude() {
    return exports.createTicket.mcpTool('claude');
}
// !!
// > Create openai tools
function createLookupCustomerToolOpenai() {
    return exports.lookupCustomer.mcpTool('openai');
}
function createCheckOrderStatusToolOpenai() {
    return exports.checkOrderStatus.mcpTool('openai');
}
function createTicketToolOpenai() {
    return exports.createTicket.mcpTool('openai');
}
// !!
