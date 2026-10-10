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
exports.multipleConcurrencyKeys = exports.simpleConcurrency = void 0;
const v1_1 = require("../..");
const hatchet_client_1 = require("../hatchet-client");
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
// > Concurrency Strategy With Key
exports.simpleConcurrency = hatchet_client_1.hatchet.workflow({
    name: 'simple-concurrency',
    concurrency: {
        maxRuns: 1,
        limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        expression: 'input.GroupKey',
    },
});
// !!
exports.simpleConcurrency.task({
    name: 'to-lower',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield sleep(Math.floor(Math.random() * (1000 - 200 + 1)) + 200);
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    }),
});
// > Multiple Concurrency Keys
exports.multipleConcurrencyKeys = hatchet_client_1.hatchet.workflow({
    name: 'simple-concurrency',
    concurrency: [
        {
            maxRuns: 1,
            limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
            expression: 'input.Tier',
        },
        {
            maxRuns: 1,
            limitStrategy: v1_1.ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
            expression: 'input.Account',
        },
    ],
});
// !!
exports.multipleConcurrencyKeys.task({
    name: 'to-lower',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        yield sleep(Math.floor(Math.random() * (1000 - 200 + 1)) + 200);
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    }),
});
