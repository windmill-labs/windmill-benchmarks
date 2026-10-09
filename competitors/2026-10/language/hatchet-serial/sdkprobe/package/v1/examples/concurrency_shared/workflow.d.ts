import { Concurrency } from '../..';
export declare const SLEEP_TIME_MS = 1500;
export type WorkflowInput = {
    group: string;
    inline?: string;
};
export type RunWindow = {
    startMs: number;
    endMs: number;
};
export type WorkflowOutput = {
    'shared-task': RunWindow;
};
export declare const sharedLimit: Concurrency;
export declare const concurrencySharedWorkflowA: import("../..").WorkflowDeclaration<WorkflowInput, WorkflowOutput, {}>;
export declare const concurrencySharedWorkflowB: import("../..").WorkflowDeclaration<WorkflowInput, WorkflowOutput, {}>;
export declare const concurrencySharedMixedWorkflow: import("../..").WorkflowDeclaration<WorkflowInput, WorkflowOutput, {}>;
