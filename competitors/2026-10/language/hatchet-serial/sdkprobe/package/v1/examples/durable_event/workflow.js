"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.durableEventWithLookback = exports.durableEventWithFilter = exports.durableEvent = exports.SCOPE = exports.EVENT_KEY = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.EVENT_KEY = 'user:update';
exports.SCOPE = 'user-1234';
// > Durable Event
exports.durableEvent = hatchet_client_1.hatchet.durableTask({
    name: 'durable-event',
    executionTimeout: '10m',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const res = yield ctx.waitForEvent(exports.EVENT_KEY);
        ctx.logger.info('res', { res });
        return {
            Value: 'done',
        };
    }),
});
// !!
exports.durableEventWithFilter = hatchet_client_1.hatchet.durableTask({
    name: 'durable-event-with-filter',
    executionTimeout: '10m',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // > Durable Event With Filter
        const res = yield ctx.waitForEvent(exports.EVENT_KEY, "input.userId == '1234'");
        // !!
        ctx.logger.info('res', { res });
        return {
            Value: 'done',
        };
    }),
});
// !!
// > Durable Event With Lookback
exports.durableEventWithLookback = hatchet_client_1.hatchet.durableTask({
    name: 'durable-event-with-lookback',
    executionTimeout: '10m',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const res = yield ctx.waitForEvent(exports.EVENT_KEY, undefined, undefined, exports.SCOPE, '1m');
        ctx.logger.info('res', { res });
        return {
            Value: 'done',
        };
    }),
});
// !!
