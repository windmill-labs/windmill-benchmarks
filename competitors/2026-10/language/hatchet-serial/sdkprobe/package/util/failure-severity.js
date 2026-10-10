"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.classifyRepeatedFailure = classifyRepeatedFailure;
function classifyRepeatedFailure(isTransient, attempt, threshold) {
    if (!isTransient) {
        return 'error';
    }
    if (attempt >= threshold) {
        return 'error';
    }
    if (attempt > 1) {
        return 'warn';
    }
    return 'silent';
}
