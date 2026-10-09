export type Input = {
    Message: string;
};
type OnCronOutput = {
    job: {
        TransformedMessage: string;
    };
};
export declare const onCron: import("../..").WorkflowDeclaration<Input, OnCronOutput, {}>;
export {};
