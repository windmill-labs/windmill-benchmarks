"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NonDeterminismError = void 0;
const hatchet_error_1 = __importDefault(require("./hatchet-error"));
class NonDeterminismError extends hatchet_error_1.default {
    constructor(taskExternalId, invocationCount, nodeId, message) {
        const detail = message
            ? message
            : `Non-determinism detected in task ${taskExternalId} on invocation ${invocationCount} at node ${nodeId}`;
        super(`${detail}\n` +
            `Check out our documentation for more details on expectations of durable tasks: https://docs.hatchet.run/v1/patterns`);
        this.name = 'NonDeterminismError';
        this.taskExternalId = taskExternalId;
        this.invocationCount = invocationCount;
        this.nodeId = nodeId;
    }
}
exports.NonDeterminismError = NonDeterminismError;
