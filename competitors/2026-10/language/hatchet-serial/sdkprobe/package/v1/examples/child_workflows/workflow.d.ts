type ChildInput = {
    N: number;
};
export declare const child: import("../..").TaskWorkflowDeclaration<ChildInput, {
    Value: number;
}, {}, {}, {}, {}>;
type ParentInput = {
    N: number;
};
export declare const parent: import("../..").TaskWorkflowDeclaration<ParentInput, {
    Result: number;
}, {}, {}, {}, {}>;
export declare const parentSingleChild: import("../..").TaskWorkflowDeclaration<import("../..").UnknownInputType, {
    Result: number;
}, {}, {}, {}, {}>;
export declare const withErrorHandling: import("../..").TaskWorkflowDeclaration<import("../..").UnknownInputType, {
    Result: number;
}, {}, {}, {}, {}>;
export {};
