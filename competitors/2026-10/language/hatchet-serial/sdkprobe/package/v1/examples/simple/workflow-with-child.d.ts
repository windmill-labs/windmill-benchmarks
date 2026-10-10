export type ChildInput = {
    Message: string;
};
export type ParentInput = {
    Message: string;
};
export declare const child: import("../..").WorkflowDeclaration<ChildInput, {}, {}>;
export declare const child1: import("../..").CreateWorkflowTaskOpts<ChildInput, {
    TransformedMessage: string;
}>;
export declare const child2: import("../..").CreateWorkflowTaskOpts<ChildInput, {
    TransformedMessage: string;
}>;
export declare const child3: import("../..").CreateWorkflowTaskOpts<ChildInput, {
    TransformedMessage: string;
}>;
export declare const parent: import("../..").TaskWorkflowDeclaration<ParentInput, {
    TransformedMessage: string;
}, {}, {}, {}, {}>;
