"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.IdempotencyCollisionError = void 0;
const hatchet_error_1 = __importDefault(require("./hatchet-error"));
class IdempotencyCollisionError extends hatchet_error_1.default {
    constructor(existingRunExternalId) {
        super(`idempotency key collision: existing run ${existingRunExternalId} already exists`);
        this.name = 'IdempotencyCollisionError';
        this.existingRunExternalId = existingRunExternalId;
        Object.setPrototypeOf(this, new.target.prototype);
    }
}
exports.IdempotencyCollisionError = IdempotencyCollisionError;
