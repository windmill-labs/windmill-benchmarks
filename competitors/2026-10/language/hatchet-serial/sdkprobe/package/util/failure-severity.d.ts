export type FailureSeverity = 'silent' | 'warn' | 'error';
export declare function classifyRepeatedFailure(isTransient: boolean, attempt: number, threshold: number): FailureSeverity;
