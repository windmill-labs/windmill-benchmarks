import type { EmptyTaskOutput } from '../concurrency-types';
export declare const SLEEP_TIME_MS = 500;
export declare const DIGIT_MAX_RUNS = 8;
export declare const NAME_MAX_RUNS = 3;
export type WorkflowInput = {
    name: string;
    digit: string;
};
export type WorkflowOutput = {
    concurrency_task: EmptyTaskOutput;
};
export declare const concurrencyMultipleKeysWorkflow: import("../..").WorkflowDeclaration<WorkflowInput, WorkflowOutput, {}>;
