import { HatchetClient } from '../..';
export declare const hatchet: HatchetClient<{}, {}, {}, {}>;
export type OrderInput = {
    orderId: string;
    correlationId: string;
};
export type ValidateOutput = {
    valid: boolean;
};
export type FulfillOutput = {
    fulfilled: boolean;
    shipmentId: string;
};
export type SignupInput = {
    userId: string;
    email: string;
};
export type EmailOutput = {
    sent: boolean;
};
export declare const validateOrder: import("../..").TaskWorkflowDeclaration<OrderInput, ValidateOutput, {}, {}, {}, {}>;
export type ChargeOutput = {
    charged: boolean;
    amountCents: number;
};
export declare const chargeOrder: import("../..").TaskWorkflowDeclaration<OrderInput, ChargeOutput, {}, {}, {}, {}>;
export declare const fulfillOrder: import("../..").TaskWorkflowDeclaration<OrderInput, FulfillOutput, {}, {}, {}, {}>;
export declare const processOrder: import("../..").TaskWorkflowDeclaration<OrderInput, FulfillOutput, {}, {}, {}, {}>;
export declare const sendWelcomeEmail: import("../..").TaskWorkflowDeclaration<SignupInput, EmailOutput, {}, {}, {}, {}>;
export declare const sendFollowupEmail: import("../..").TaskWorkflowDeclaration<SignupInput, EmailOutput, {}, {}, {}, {}>;
export declare const onboardingFlow: import("../..").TaskWorkflowDeclaration<SignupInput, EmailOutput, {}, {}, {}, {}>;
export declare const chargeOrderWithRetries: import("../..").TaskWorkflowDeclaration<OrderInput, ChargeOutput, {}, {}, {}, {}>;
export declare const orderFollowUp: import("../..").TaskWorkflowDeclaration<OrderInput, EmailOutput, {}, {}, {}, {}>;
export declare const approvalFlow: import("../..").TaskWorkflowDeclaration<OrderInput, FulfillOutput, {}, {}, {}, {}>;
export type ItemInput = {
    item: string;
};
export type ItemOutput = {
    packed: boolean;
};
export declare const processItem: import("../..").TaskWorkflowDeclaration<ItemInput, ItemOutput, {}, {}, {}, {}>;
export declare const packOrder: import("../..").TaskWorkflowDeclaration<{
    items: string[];
}, {
    packed: number;
}, {}, {}, {}, {}>;
export type ReportInput = {
    kind: string;
};
export type ReportOutput = {
    generate: {
        rows: number;
    };
};
export declare const weeklyReport: import("../..").WorkflowDeclaration<ReportInput, ReportOutput, {}>;
export type SyncInput = {
    customerId: string;
};
export type SyncOutput = {
    sync: {
        synced: number;
    };
};
export type PromptInput = {
    prompt: string;
};
export declare const syncCustomer: import("../..").WorkflowDeclaration<SyncInput, SyncOutput, {}>;
export declare const callModel: import("../..").TaskWorkflowDeclaration<PromptInput, {
    completion: string;
}, {}, {}, {}, {}>;
export type OrderDagOutput = {
    validate: ValidateOutput;
    charge: ChargeOutput;
    fulfill: FulfillOutput;
};
export declare const orderWorkflow: import("../..").WorkflowDeclaration<OrderInput, OrderDagOutput, {}>;
