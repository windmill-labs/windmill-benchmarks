export type SimpleInput = {
    Message: string;
};
export declare const timeoutTask: import("../..").TaskWorkflowDeclaration<SimpleInput, {
    status: string;
}, {}, {}, {}, {}>;
export declare const refreshTimeoutTask: import("../..").TaskWorkflowDeclaration<SimpleInput, {
    status: string;
    message: string;
}, {}, {}, {}, {}>;
