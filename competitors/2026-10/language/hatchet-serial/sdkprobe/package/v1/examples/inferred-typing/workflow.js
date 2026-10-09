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
exports.crazyWorkflow = exports.inferredTypeDurable = exports.inferredType = exports.declaredType = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.declaredType = hatchet_client_1.hatchet.task({
    name: 'declared-type',
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
exports.inferredType = hatchet_client_1.hatchet.task({
    name: 'inferred-type',
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toUpperCase(),
        };
    },
});
exports.inferredTypeDurable = hatchet_client_1.hatchet.durableTask({
    name: 'inferred-type-durable',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // await ctx.sleepFor('5s');
        return {
            TransformedMessage: input.Message.toUpperCase(),
        };
    }),
});
exports.crazyWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'crazy-workflow',
});
const step1 = exports.crazyWorkflow.task(exports.declaredType);
// crazyWorkflow.task(inferredTypeDurable);
exports.crazyWorkflow.task(Object.assign({ parents: [step1] }, exports.inferredType.taskDef));
