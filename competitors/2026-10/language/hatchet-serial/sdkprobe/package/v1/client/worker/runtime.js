"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isContextRuntime = isContextRuntime;
function isContextRuntime(value) {
    if (typeof value !== 'object' || value === null)
        return false;
    const candidate = value;
    return typeof candidate.logger === 'function' && typeof candidate.workerId === 'function';
}
