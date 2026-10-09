import type { EmptyTaskOutput } from '../concurrency-types';
export declare const SLEEP_TIME_MS = 2000;
export declare const DIGIT_MAX_RUNS = 8;
export declare const NAME_MAX_RUNS = 3;
export type WorkflowInput = {
    name: string;
    digit: string;
};
export type WorkflowOutput = {
    task_1: EmptyTaskOutput;
    task_2: EmptyTaskOutput;
};
export declare const concurrencyWorkflowLevelWorkflow: import("../..").WorkflowDeclaration<WorkflowInput, WorkflowOutput, {}>;
