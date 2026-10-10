"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.BulkTriggerPartialError = void 0;
const hatchet_error_1 = __importStar(require("./hatchet-error"));
/**
 * A bulk trigger that failed after at least one of its batches was accepted. The runs the
 * accepted batches created exist and are running; a caller that retries the whole request
 * would create them again, so `refs` carries their references, in input order, for the
 * caller to keep or cancel. `cause` is the error the failed batch raised, which is a
 * `BulkTriggerIdempotencyCollisionError` when that batch collided on an idempotency key.
 */
class BulkTriggerPartialError extends hatchet_error_1.default {
    constructor(refs, failedBatchIndex, cause) {
        super(`bulk trigger failed at batch ${failedBatchIndex} after ${refs.length} runs were created: ${(0, hatchet_error_1.getErrorMessage)(cause)}`, { cause });
        this.name = 'BulkTriggerPartialError';
        this.refs = refs;
        this.failedBatchIndex = failedBatchIndex;
        this.cause = cause;
        Object.setPrototypeOf(this, new.target.prototype);
    }
}
exports.BulkTriggerPartialError = BulkTriggerPartialError;
