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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.runDetailTestWorkflow = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const hatchet_client_1 = require("../hatchet-client");
exports.runDetailTestWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'run-detail-test',
});
const step1 = exports.runDetailTestWorkflow.task({
    name: 'step1',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        return ({
            random_number: Math.floor(Math.random() * 100) + 1,
        });
    }),
});
const step2 = exports.runDetailTestWorkflow.task({
    name: 'step2',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        yield (0, sleep_1.default)(5000);
        return {
            random_number: Math.floor(Math.random() * 100) + 1,
        };
    }),
});
exports.runDetailTestWorkflow.task({
    name: 'cancel_step',
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        yield ctx.cancel();
        yield (0, sleep_1.default)(10000);
        return {};
    }),
});
exports.runDetailTestWorkflow.task({
    name: 'fail_step',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        throw new Error('Intentional Failure');
    }),
});
const step3 = exports.runDetailTestWorkflow.task({
    name: 'step3',
    parents: [step1, step2],
    fn: (_input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const one = (yield ctx.parentOutput(step1)).random_number;
        const two = (yield ctx.parentOutput(step2)).random_number;
        return { sum: one + two };
    }),
});
exports.runDetailTestWorkflow.task({
    name: 'step4',
    parents: [step1, step3],
    fn: () => __awaiter(void 0, void 0, void 0, function* () { return ({ step4: 'step4' }); }),
});
