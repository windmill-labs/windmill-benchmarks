export declare const EVENT_KEY = "ts-e2e:idempotency-example";
export type IdempotencyInput = {
    id: string;
};
export declare const idempotentTask: import("../..").TaskWorkflowDeclaration<IdempotencyInput, {
    result: string;
}, {}, {}, {}, {}>;
export declare const idempotentTaskShortWindow: import("../..").TaskWorkflowDeclaration<IdempotencyInput, {
    result: string;
}, {}, {}, {}, {}>;
export declare const idempotentStatusBasedTask: import("../..").TaskWorkflowDeclaration<IdempotencyInput, {
    result: string;
}, {}, {}, {}, {}>;
