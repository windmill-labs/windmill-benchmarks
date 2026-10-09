/** Returns a string message from an unknown value (e.g. from a catch block). */
export declare function getErrorMessage(e: unknown): string;
declare class HatchetError extends Error {
    constructor(message: string, options?: {
        cause?: unknown;
    });
}
export declare function toHatchetError(e: unknown, defaultMessageOrOptions?: string | {
    defaultMessage?: string;
    prefix?: string;
}): HatchetError;
export default HatchetError;
