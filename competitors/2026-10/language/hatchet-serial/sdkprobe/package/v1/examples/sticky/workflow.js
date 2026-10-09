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
exports.sticky = void 0;
const v1_1 = require("../..");
const hatchet_client_1 = require("../hatchet-client");
const workflow_1 = require("../child_workflows/workflow");
// > Sticky Task
exports.sticky = hatchet_client_1.hatchet.task({
    name: 'sticky',
    retries: 3,
    sticky: v1_1.StickyStrategy.SOFT,
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // specify a child workflow to run on the same worker
        const result = yield workflow_1.child.run({
            N: 1,
        }, { sticky: true });
        return {
            result,
        };
    }),
});
// !!
