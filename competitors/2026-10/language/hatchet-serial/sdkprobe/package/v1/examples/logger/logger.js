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
const hatchet_client_1 = require("../hatchet-client");
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
// > Logger
const workflow = hatchet_client_1.hatchet.workflow({
    name: 'logger-example',
    description: 'test',
    on: {
        event: 'user:create',
    },
});
workflow.task({
    name: 'logger-step1',
    fn: (_, ctx) => __awaiter(void 0, void 0, void 0, function* () {
        // log in a for loop
        for (let i = 0; i < 10; i++) {
            ctx.logger.info(`log message ${i}`);
            yield sleep(200);
        }
        return { step1: 'completed step run' };
    }),
});
// !!
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet_client_1.hatchet.worker('logger-worker', {
            slots: 1,
            workflows: [workflow],
        });
        yield worker.start();
    });
}
main();
