export type ChildInput = {
    Message: string;
};
export type ParentInput = {
    Message: string;
};
export declare const child: import("../..").TaskWorkflowDeclaration<ChildInput, {
    TransformedMessage: string;
}, {}, {}, {}, {}>;
export declare const parent: import("../..").TaskWorkflowDeclaration<ParentInput, {
    TransformedMessage: string;
}, {}, {}, {}, {}>;
