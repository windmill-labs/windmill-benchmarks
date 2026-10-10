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
// > Declaring a DAG Workflow
// First, we declare the workflow
exports.dag = hatchet_client_1.hatchet.workflow({
    name: 'simple',
});
const reverse = exports.dag.task({
    name: 'reverse',
    fn: (input) => {
        return {
            Original: input.Message,
            Transformed: input.Message.split('').reverse().join(''),
        };
    },
});
exports.dag.task({
    name: 'to-lower',
    parents: [reverse],
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const r = yield ctx.parentOutput(reverse);
        return {
            reverse: {
                Original: r.Transformed,
                Transformed: r.Transformed.toLowerCase(),
            },
        };
    }),
});
