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
exports.idempotentStatusBasedTask = exports.idempotentTaskShortWindow = exports.idempotentTask = exports.EVENT_KEY = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.EVENT_KEY = 'ts-e2e:idempotency-example';
// > idempotency
exports.idempotentTask = hatchet_client_1.hatchet.task({
    name: 'ts-e2e-idempotent-task',
    idempotency: {
        strategy: 'ttl',
        expression: 'input.id',
        ttlMs: 60000,
    },
    onEvents: [exports.EVENT_KEY],
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { result: `Hello, world from task ${input.id}` };
    }),
});
// !!
exports.idempotentTaskShortWindow = hatchet_client_1.hatchet.task({
    name: 'ts-e2e-idempotent-task-short-window',
    idempotency: {
        strategy: 'ttl',
        expression: 'input.id',
        ttlMs: 2000,
    },
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { result: `Hello, world from task ${input.id}` };
    }),
});
// > status-based-idempotency
exports.idempotentStatusBasedTask = hatchet_client_1.hatchet.task({
    name: 'ts-e2e-idempotent-status-based-task',
    idempotency: {
        strategy: 'status',
        expression: 'input.id',
        fallbackTtlMs: 10000,
    },
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { result: `Hello, world from task ${input.id}` };
    }),
});
// !!
