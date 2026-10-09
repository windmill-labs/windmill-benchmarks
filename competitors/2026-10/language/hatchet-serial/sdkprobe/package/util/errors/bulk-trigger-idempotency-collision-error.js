"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BulkTriggerIdempotencyCollisionError = void 0;
class BulkTriggerIdempotencyCollisionError extends Error {
    constructor(successfulWorkflowRunExternalIds, collisions) {
        super('idempotency key collision in bulk trigger');
        this.name = 'BulkTriggerIdempotencyCollisionError';
        this.successfulWorkflowRunExternalIds = successfulWorkflowRunExternalIds;
        this.collisions = collisions;
        Object.setPrototypeOf(this, new.target.prototype);
    }
}
exports.BulkTriggerIdempotencyCollisionError = BulkTriggerIdempotencyCollisionError;
