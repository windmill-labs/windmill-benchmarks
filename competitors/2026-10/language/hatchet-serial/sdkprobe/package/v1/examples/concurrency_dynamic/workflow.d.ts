export type WorkflowInput = {
    account: string;
    tier: string;
};
export type WorkflowOutput = {
    'dynamic-task': {
        account: string;
    };
};
export declare const concurrencyDynamicWorkflow: import("../..").WorkflowDeclaration<WorkflowInput, WorkflowOutput, {}>;
