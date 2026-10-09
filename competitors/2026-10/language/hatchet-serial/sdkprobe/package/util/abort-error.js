"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AbortError = void 0;
exports.createAbortError = createAbortError;
exports.isAbortError = isAbortError;
exports.rethrowIfAborted = rethrowIfAborted;
exports.throwIfAborted = throwIfAborted;
class AbortError extends Error {
    constructor(message = 'Operation aborted') {
        super(message);
        this.code = 'ABORT_ERR';
        this.name = 'AbortError';
    }
}
exports.AbortError = AbortError;
function createAbortError(message = 'Operation aborted') {
    return new AbortError(message);
}
function isAbortError(err) {
    if (err instanceof AbortError) {
        return true;
    }
    return (err instanceof Error &&
        (err.name === 'AbortError' || err.code === 'ABORT_ERR'));
}
/**
 * Helper to be used inside broad `catch` blocks so cancellation isn't accidentally swallowed.
 *
 * Example:
 * ```ts
 * try { ... } catch (e) { rethrowIfAborted(e); ... }
 * ```
 */
function rethrowIfAborted(err) {
    if (isAbortError(err)) {
        throw err;
    }
}
/**
 * Throws an AbortError if the provided signal is aborted.
 *
 * Notes:
 * - In JS/TS, `catch` can swallow any thrown value, so this is best-effort.
 * - We prefer throwing the signal's `reason` when it is already an Error.
 */
function throwIfAborted(signal, optsOrDefaultMessage = 'Operation cancelled by AbortSignal') {
    var _a, _b;
    if (!(signal === null || signal === void 0 ? void 0 : signal.aborted)) {
        return;
    }
    const opts = typeof optsOrDefaultMessage === 'string'
        ? { defaultMessage: optsOrDefaultMessage }
        : (optsOrDefaultMessage !== null && optsOrDefaultMessage !== void 0 ? optsOrDefaultMessage : {});
    if (opts.isTrigger) {
        const ctx = opts.context ? `${opts.context} ` : '';
        (_a = opts.warn) === null || _a === void 0 ? void 0 : _a.call(opts, `Cancellation: ${ctx}attempted to enqueue/trigger work after cancellation was signaled. ` +
            `This usually means an AbortError was caught and not propagated. ` +
            `See https://docs.hatchet.run/home/cancellation`);
    }
    const { reason } = signal;
    if (reason instanceof Error) {
        throw reason;
    }
    if (typeof reason === 'string' && reason.length > 0) {
        throw createAbortError(reason);
    }
    throw createAbortError((_b = opts.defaultMessage) !== null && _b !== void 0 ? _b : 'Operation cancelled by AbortSignal');
}
