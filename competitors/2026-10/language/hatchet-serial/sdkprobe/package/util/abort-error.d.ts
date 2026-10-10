export declare class AbortError extends Error {
    readonly code = "ABORT_ERR";
    constructor(message?: string);
}
export declare function createAbortError(message?: string): AbortError;
export declare function isAbortError(err: unknown): err is Error;
/**
 * Helper to be used inside broad `catch` blocks so cancellation isn't accidentally swallowed.
 *
 * Example:
 * ```ts
 * try { ... } catch (e) { rethrowIfAborted(e); ... }
 * ```
 */
export declare function rethrowIfAborted(err: unknown): void;
export type ThrowIfAbortedOpts = {
    /**
     * Optional: called before throwing when the signal is aborted.
     * This lets callsites attach logging without coupling this util to a logger implementation.
     */
    warn?: (message: string) => void;
    /**
     * If true, emits a generic warning intended for "trigger/enqueue" paths.
     */
    isTrigger?: boolean;
    /**
     * Optional context used to make warnings consistent, e.g. "task run <id>".
     */
    context?: string;
    /**
     * Message used when the AbortSignal doesn't provide a reason.
     */
    defaultMessage?: string;
};
/**
 * Throws an AbortError if the provided signal is aborted.
 *
 * Notes:
 * - In JS/TS, `catch` can swallow any thrown value, so this is best-effort.
 * - We prefer throwing the signal's `reason` when it is already an Error.
 */
export declare function throwIfAborted(signal: AbortSignal | undefined, optsOrDefaultMessage?: ThrowIfAbortedOpts | string): void;
