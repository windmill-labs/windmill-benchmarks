type SimpleInput = {
    Message: string;
    GroupKey: string;
};
type SimpleOutput = {
    'to-lower': {
        TransformedMessage: string;
    };
};
export declare const simpleConcurrency: import("../..").WorkflowDeclaration<SimpleInput, SimpleOutput, {}>;
export declare const multipleConcurrencyKeys: import("../..").WorkflowDeclaration<SimpleInput, SimpleOutput, {}>;
export {};
