import HatchetError from './hatchet-error';
export declare class EvictionNotSupportedError extends HatchetError {
    engineVersion: string | undefined;
    constructor(engineVersion?: string);
}
