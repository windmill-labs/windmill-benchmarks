import { WorkflowInputType, WorkflowOutputType } from '../..';
interface DagInput extends WorkflowInputType {
    Message: string;
}
interface DagOutput extends WorkflowOutputType {
    reverse: {
        Original: string;
        Transformed: string;
    };
}
export declare const dag: import("../..").WorkflowDeclaration<DagInput, DagOutput, {}>;
export {};
