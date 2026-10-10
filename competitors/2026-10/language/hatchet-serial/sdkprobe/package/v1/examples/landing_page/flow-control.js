"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.simple = void 0;
const workflows_1 = require("../../../protoc/v1/workflows");
const hatchet_client_1 = require("../hatchet-client");
// > Process what you can handle
exports.simple = hatchet_client_1.hatchet.task({
    name: 'simple',
    concurrency: {
        expression: 'input.user_id',
        limitStrategy: workflows_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        maxRuns: 1,
    },
    rateLimits: [
        {
            key: 'api_throttle',
            units: 1,
        },
    ],
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
// !!
