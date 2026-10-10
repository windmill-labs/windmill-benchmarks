import { IdempotencyCollisionError } from './idempotency-collision-error';
export declare class BulkTriggerIdempotencyCollisionError extends Error {
    successfulWorkflowRunExternalIds: string[];
    collisions: IdempotencyCollisionError[];
    constructor(successfulWorkflowRunExternalIds: string[], collisions: IdempotencyCollisionError[]);
}
