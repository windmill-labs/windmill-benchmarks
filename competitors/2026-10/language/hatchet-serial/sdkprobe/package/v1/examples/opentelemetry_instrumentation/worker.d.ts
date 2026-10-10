type OrderInput = {
    orderId: string;
    customerId: string;
    amount: number;
};
export declare const orderWorkflow: import("../..").WorkflowDeclaration<OrderInput, {}, {}>;
export {};
