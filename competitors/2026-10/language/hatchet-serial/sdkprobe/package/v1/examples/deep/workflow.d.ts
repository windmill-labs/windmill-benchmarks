type SimpleInput = {
    Message: string;
    N: number;
};
type Output = {
    transformer: {
        Sum: number;
    };
};
export declare const child1: import("../..").WorkflowDeclaration<SimpleInput, Output, {}>;
export declare const child2: import("../..").WorkflowDeclaration<SimpleInput, Output, {}>;
export declare const child3: import("../..").WorkflowDeclaration<SimpleInput, Output, {}>;
export declare const child4: import("../..").WorkflowDeclaration<SimpleInput, Output, {}>;
export declare const child5: import("../..").WorkflowDeclaration<SimpleInput, Output, {}>;
export declare const parent: import("../..").WorkflowDeclaration<SimpleInput, {
    parent: Output["transformer"];
}, {}>;
export {};
