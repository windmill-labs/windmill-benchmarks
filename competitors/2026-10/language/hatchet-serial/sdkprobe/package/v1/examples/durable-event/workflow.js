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
exports.durableEventWithFilter = exports.durableEvent = void 0;
// import sleep from '../../../util/sleep.js';
const hatchet_client_1 = require("../hatchet-client");
// > Durable Event
exports.durableEvent = hatchet_client_1.hatchet.durableTask({
    name: 'durable-event',
    executionTimeout: '10m',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const res = yield ctx.waitForEvent('user:update');
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
        const res = yield ctx.waitForEvent('user:update', "input.userId == '1234'");
        // !!
        ctx.logger.info('res', { res });
        return {
            Value: 'done',
        };
    }),
});
// !!
