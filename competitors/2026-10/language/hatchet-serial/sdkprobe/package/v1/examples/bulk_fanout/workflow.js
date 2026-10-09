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
exports.bulkParentWorkflow = exports.bulkChild = void 0;
const hatchet_client_1 = require("../hatchet-client");
exports.bulkChild = hatchet_client_1.hatchet.task({
    name: 'bulk-child',
    fn: (input) => __awaiter(void 0, void 0, void 0, function* () {
        return { i: input.i };
    }),
});
exports.bulkParentWorkflow = hatchet_client_1.hatchet.workflow({
    name: 'bulk-parent',
});
exports.bulkParentWorkflow.task({
    name: 'spawn',
    fn: (input, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        const typed = input;
        const children = Array.from({ length: typed.n }, (_, i) => ({
            workflow: exports.bulkChild,
            input: { i },
        }));
        const results = yield ctx.bulkRunChildren(children);
        return { results };
    }),
});
