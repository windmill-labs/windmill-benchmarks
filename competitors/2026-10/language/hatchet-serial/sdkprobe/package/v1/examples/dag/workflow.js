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
exports.dag = void 0;
const hatchet_client_1 = require("../hatchet-client");
// !!
// > Declaring a DAG Workflow
// First, we declare the workflow
exports.dag = hatchet_client_1.hatchet.workflow({
    name: 'simple',
});
// !!
// > First task
// Next, we declare the tasks bound to the workflow
const toLower = exports.dag.task({
    name: 'to-lower',
    fn: (input) => {
        return {
            TransformedMessage: input.Message.toLowerCase(),
        };
    },
});
// !!
// > Second task with parent
exports.dag.task({
    name: 'reverse',
    parents: [toLower],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const lower = yield ctx.parentOutput(toLower);
        return {
            Original: input.Message,
            Transformed: lower.TransformedMessage.split('').reverse().join(''),
        };
    }),
});
// !!
// > Accessing Parent Outputs
exports.dag.task({
    name: 'task-with-parent-output',
    parents: [toLower],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const lower = yield ctx.parentOutput(toLower);
        return {
            Original: input.Message,
            Transformed: lower.TransformedMessage.split('').reverse().join(''),
        };
    }),
});
// !!
