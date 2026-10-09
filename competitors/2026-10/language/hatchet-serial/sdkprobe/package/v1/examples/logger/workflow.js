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
exports.createLoggingWorkflow = createLoggingWorkflow;
function createLoggingWorkflow(client) {
    const wf = client.workflow({ name: 'logging-workflow' });
    wf.task({
        name: 'root_logger',
        fn: () => __awaiter(this, void 0, void 0, function* () {
            for (let i = 0; i < 12; i += 1) {
                console.info(`executed step1 - ${i}`);
                console.info({ step1: 'step1' });
                // keep this fast for e2e
            }
            return { status: 'success' };
        }),
    });
    wf.task({
        name: 'context_logger',
        fn: (_input, ctx) => __awaiter(this, void 0, void 0, function* () {
            for (let i = 0; i < 12; i += 1) {
                // Python uses ctx.log; TS has both ctx.log (deprecated) and ctx.logger.*
                yield ctx.log(`executed step1 - ${i}`);
                yield ctx.log(JSON.stringify({ step1: 'step1' }));
            }
            return { status: 'success' };
        }),
    });
    return wf;
}
