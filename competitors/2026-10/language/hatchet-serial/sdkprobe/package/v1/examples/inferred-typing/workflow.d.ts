type SimpleInput = {
    Message: string;
};
type SimpleOutput = {
    TransformedMessage: string;
};
export declare const declaredType: import("../..").TaskWorkflowDeclaration<SimpleInput, SimpleOutput, {}, {}, {}, {}>;
export declare const inferredType: import("../..").TaskWorkflowDeclaration<SimpleInput, {
    TransformedMessage: string;
}, {}, {}, {}, {}>;
export declare const inferredTypeDurable: import("../..").TaskWorkflowDeclaration<SimpleInput, {
    TransformedMessage: string;
}, {}, {}, {}, {}>;
export declare const crazyWorkflow: import("../..").WorkflowDeclaration<any, any, {}>;
export {};
