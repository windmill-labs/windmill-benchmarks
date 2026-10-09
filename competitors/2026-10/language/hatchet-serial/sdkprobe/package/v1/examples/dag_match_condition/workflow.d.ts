type DagInput = {};
type DagOutput = {
    'first-task': {
        Completed: boolean;
    };
    'second-task': {
        Completed: boolean;
    };
};
export declare const dagWithConditions: import("../..").WorkflowDeclaration<DagInput, DagOutput, {}>;
export {};
