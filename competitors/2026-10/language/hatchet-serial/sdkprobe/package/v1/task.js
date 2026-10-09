"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NonRetryableError = exports.WorkerLabelComparator = exports.ConcurrencyLimitStrategy = void 0;
const workflows_1 = require("../protoc/v1/workflows");
Object.defineProperty(exports, "ConcurrencyLimitStrategy", { enumerable: true, get: function () { return workflows_1.ConcurrencyLimitStrategy; } });
const trigger_1 = require("../protoc/v1/shared/trigger");
Object.defineProperty(exports, "WorkerLabelComparator", { enumerable: true, get: function () { return trigger_1.WorkerLabelComparator; } });
class NonRetryableError extends Error {
    constructor(message) {
        super(message);
        this.name = 'NonRetryableError';
        Object.setPrototypeOf(this, new.target.prototype);
    }
}
exports.NonRetryableError = NonRetryableError;
