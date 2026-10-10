import HatchetError from './hatchet-error';
export declare class NonDeterminismError extends HatchetError {
    taskExternalId: string;
    invocationCount: number;
    nodeId: number;
    constructor(taskExternalId: string, invocationCount: number, nodeId: number, message: string);
}
