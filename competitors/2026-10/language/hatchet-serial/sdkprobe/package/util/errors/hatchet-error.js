"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getErrorMessage = getErrorMessage;
exports.toHatchetError = toHatchetError;
/** Returns a string message from an unknown value (e.g. from a catch block). */
function getErrorMessage(e) {
    return e instanceof Error ? e.message : String(e);
}
class HatchetError extends Error {
    constructor(message, options) {
        super(message, options);
        this.name = 'HatchetError';
    }
}
function toHatchetError(e, defaultMessageOrOptions = 'An error occurred') {
    var _a;
    if (e instanceof HatchetError) {
        return e;
    }
    const opts = typeof defaultMessageOrOptions === 'string'
        ? { defaultMessage: defaultMessageOrOptions }
        : defaultMessageOrOptions;
    const defaultMessage = (_a = opts.defaultMessage) !== null && _a !== void 0 ? _a : 'An error occurred';
    let message = getErrorMessage(e) || defaultMessage;
    if (opts.prefix) {
        message = opts.prefix + message;
    }
    return new HatchetError(message, { cause: e });
}
exports.default = HatchetError;
