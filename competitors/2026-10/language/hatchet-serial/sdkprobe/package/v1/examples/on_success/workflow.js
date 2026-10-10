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
exports.onSuccessDag = void 0;
const hatchet_client_1 = require("../hatchet-client");
// > On Success DAG
exports.onSuccessDag = hatchet_client_1.hatchet.workflow({
    name: 'on-success-dag',
});
exports.onSuccessDag.task({
    name: 'always-succeed',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        return {
            'always-succeed': 'success',
        };
    }),
});
exports.onSuccessDag.task({
    name: 'always-succeed2',
    fn: () => __awaiter(void 0, void 0, void 0, function* () {
        return {
            'always-succeed': 'success',
        };
    }),
});
// 👀 onSuccess handler will run if all tasks in the workflow succeed
exports.onSuccessDag.onSuccess({
    fn: (_, ctx) => {
        ctx.logger.info(`onSuccess for run: ${ctx.workflowRunId()}`);
        return {
            'on-success': 'success',
        };
    },
});
// !!
