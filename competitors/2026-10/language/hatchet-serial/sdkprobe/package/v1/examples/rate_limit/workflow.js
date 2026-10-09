"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const workflows_1 = require("../../../protoc/v1/workflows");
const hatchet_client_1 = require("../hatchet-client");
// > Upsert Rate Limit
hatchet_client_1.hatchet.ratelimits.upsert({
    key: 'api-service-rate-limit',
    limit: 10,
    duration: workflows_1.RateLimitDuration.SECOND,
});
// !!
// > Static
const RATE_LIMIT_KEY = 'api-service-rate-limit';
const task1 = hatchet_client_1.hatchet.task({
    name: 'task1',
    rateLimits: [
        {
            staticKey: RATE_LIMIT_KEY,
            units: 1,
        },
    ],
    fn: (_input, ctx) => {
        ctx.logger.info('executed task1');
    },
});
// !!
// > Dynamic
const task2 = hatchet_client_1.hatchet.task({
    name: 'task2',
    fn: (input, ctx) => {
        ctx.logger.info(`executed task2 for user: ${input.userId}`);
    },
    rateLimits: [
        {
            dynamicKey: 'input.userId',
            units: 1,
            limit: 10,
            duration: workflows_1.RateLimitDuration.MINUTE,
        },
    ],
});
// !!
