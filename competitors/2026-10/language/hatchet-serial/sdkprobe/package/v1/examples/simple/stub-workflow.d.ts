export type SimpleInput = {
    Message: string;
};
export type SimpleOutput = {
    'to-lower': {
        TransformedMessage: string;
    };
};
export declare const simple: import("../..").WorkflowDeclaration<SimpleInput, SimpleOutput, {}>;
