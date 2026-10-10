import HatchetError from './hatchet-error';
export declare class IdempotencyCollisionError extends HatchetError {
    existingRunExternalId: string;
    constructor(existingRunExternalId: string);
}
