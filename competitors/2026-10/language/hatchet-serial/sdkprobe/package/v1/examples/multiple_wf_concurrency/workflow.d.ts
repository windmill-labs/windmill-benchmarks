type SimpleInput = {
    Message: string;
    GroupKey: string;
};
type SimpleOutput = {
    'to-lower': {
        TransformedMessage: string;
    };
};
export declare const multiConcurrency: import("../..").WorkflowDeclaration<SimpleInput, SimpleOutput, {}>;
export {};
