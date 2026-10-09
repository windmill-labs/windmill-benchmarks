export type LeafInput = {
    mid: number;
    branch: number;
    delayMs: number;
    generation: number;
};
export type LeafOutput = {
    mid: number;
    branch: number;
    generation: number;
};
export type MidInput = {
    mid: number;
    branches: number;
    childDelayMs: number;
    delayStepMs: number;
};
export type MidOutput = {
    mid: number;
    completedBranches: number[];
    invocationCount: number;
};
export type RootInput = {
    durables?: number;
    branches?: number;
    childDelayMs?: number;
    delayStepMs?: number;
};
export type RootOutput = {
    rootInvocationCount: number;
    midInvocationCounts: number[];
    completedMids: number[];
};
export declare const ROOT_DEFAULTS: {
    durables: number;
    branches: number;
    childDelayMs: number;
    delayStepMs: number;
};
export declare const callbackOrderingLeaf: import("../..").TaskWorkflowDeclaration<LeafInput, LeafOutput, {}, {}, {}, {}>;
export declare const callbackOrderingMid: import("../..").TaskWorkflowDeclaration<MidInput, MidOutput, {}, {}, {}, {}>;
export declare const callbackOrderingRoot: import("../..").TaskWorkflowDeclaration<RootInput, RootOutput, {}, {}, {}, {}>;
