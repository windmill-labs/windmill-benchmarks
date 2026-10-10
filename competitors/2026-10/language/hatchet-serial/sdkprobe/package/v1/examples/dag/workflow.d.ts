type DagInput = {
    Message: string;
};
type DagOutput = {
    reverse: {
        Original: string;
        Transformed: string;
    };
};
export declare const dag: import("../..").WorkflowDeclaration<DagInput, DagOutput, {}>;
export {};
