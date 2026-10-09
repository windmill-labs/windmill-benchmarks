type TaskInput = {
    message: string;
};
type TaskOutput = {
    message: string;
};
export declare const taskWithMiddleware: import("../..").TaskWorkflowDeclaration<TaskInput, TaskOutput, import("./client").GlobalInputType, import("./client").GlobalOutputType, {
    dependency: string;
    first: number;
    second: number;
}, {
    additionalData: number;
    extra: number;
}>;
export {};
