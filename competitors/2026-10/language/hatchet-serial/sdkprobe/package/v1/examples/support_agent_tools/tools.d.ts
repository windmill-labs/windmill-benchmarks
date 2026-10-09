type CustomerInfo = {
    customerId: string;
    name: string;
    email: string;
    plan: string;
    accountStatus: string;
    defaultOrderId: string;
    supportTier: string;
};
type OrderStatus = {
    orderId: string;
    status: string;
    lastUpdated: string;
    estimatedDelivery: string;
    knownIssue: string | null;
    carrier: string;
    trackingNumber: string;
};
type TicketResult = {
    ticketId: string;
    status: string;
    priority: string;
    routingTeam: string;
    summary: string;
};
export declare const lookupCustomer: import("../..").TaskWorkflowDeclaration<{
    customerId: string;
}, CustomerInfo, {}, {}, {}, {}>;
export declare const checkOrderStatus: import("../..").TaskWorkflowDeclaration<{
    orderId: string;
}, OrderStatus, {}, {}, {}, {}>;
export declare const createTicket: import("../..").TaskWorkflowDeclaration<{
    customerId: string;
    orderId: string;
    subject: string;
    body: string;
    priority: string;
}, TicketResult, {}, {}, {}, {}>;
export declare function createLookupCustomerToolClaude(): import("@anthropic-ai/claude-agent-sdk").SdkMcpToolDefinition;
export declare function createCheckOrderStatusToolClaude(): import("@anthropic-ai/claude-agent-sdk").SdkMcpToolDefinition;
export declare function createTicketToolClaude(): import("@anthropic-ai/claude-agent-sdk").SdkMcpToolDefinition;
export declare function createLookupCustomerToolOpenai(): import("@openai/agents").FunctionTool;
export declare function createCheckOrderStatusToolOpenai(): import("@openai/agents").FunctionTool;
export declare function createTicketToolOpenai(): import("@openai/agents").FunctionTool;
export {};
