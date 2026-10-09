type AffinityOutput = {
    affinity_t1: {
        worker_id: string | undefined;
    };
    affinity_t2: {
        worker_id: string | undefined;
    };
};
export declare const affinityExampleTask: import("../..").WorkflowDeclaration<{}, AffinityOutput, {}>;
export {};
