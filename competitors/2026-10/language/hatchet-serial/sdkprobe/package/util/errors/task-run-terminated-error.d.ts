export type TaskRunTerminationReason = 'cancelled' | 'evicted';
export declare class TaskRunTerminatedError extends Error {
    readonly reason: TaskRunTerminationReason;
    constructor(reason: TaskRunTerminationReason, message?: string);
}
export declare function isTaskRunTerminatedError(err: unknown): err is TaskRunTerminatedError;
