import type { EmptyTaskOutput } from '../concurrency-types';
export type WorkflowInput = {
    group: string;
};
export type WorkflowOutput = {
    step1: EmptyTaskOutput;
    step2: EmptyTaskOutput;
};
export declare const concurrencyCancelQueuedExceptOldestWorkflow: import("../..").WorkflowDeclaration<WorkflowInput, WorkflowOutput, {}>;
