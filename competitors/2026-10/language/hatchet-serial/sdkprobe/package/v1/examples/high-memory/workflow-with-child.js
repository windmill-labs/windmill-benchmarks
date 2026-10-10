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
exports.parent = exports.child = void 0;
// ❓ Declaring a Task
const hatchet_client_1 = require("../hatchet-client");
exports.child = hatchet_client_1.hatchet.task({
    name: 'child',
    fn: (input) => {
        const largePayload = new Array(1024 * 1024).fill('a').join('');
        return {
            TransformedMessage: largePayload,
        };
    },
});
exports.parent = hatchet_client_1.hatchet.task({
    name: 'parent',
    executionTimeout: '10m',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // lets generate large payload 1 mb
        const largePayload = new Array(1024 * 1024).fill('a').join('');
        // Send the large payload 100 times
        const num = 1000;
        const children = [];
        for (let i = 0; i < num; i += 1) {
            children.push({
                workflow: exports.child,
                input: {
                    Message: `Iteration ${i + 1}: ${largePayload}`,
                },
            });
        }
        yield ctx.bulkRunNoWaitChildren(children);
        return {
            TransformedMessage: 'done',
        };
    }),
});
// !!
// see ./worker.ts and ./run.ts for how to run the workflow
