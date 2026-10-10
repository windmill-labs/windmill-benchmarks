export type Input = {
    Message: string;
    ShouldSkip: boolean;
};
export declare const SIMPLE_EVENT = "simple-event:create";
type LowerOutput = {
    lower: {
        TransformedMessage: string;
    };
};
export declare const lower: import("../..").WorkflowDeclaration<Input, LowerOutput, {}>;
export declare const lowerWithFilter: import("../..").WorkflowDeclaration<Input, LowerOutput, {}>;
type UpperOutput = {
    upper: {
        TransformedMessage: string;
    };
};
export declare const upper: import("../..").WorkflowDeclaration<Input, UpperOutput, {}>;
export {};
