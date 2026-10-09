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
const workflow_1 = require("./workflow");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Bulk Run a Task
        const res = yield workflow_1.simple.run([
            {
                Message: 'HeLlO WoRlD',
            },
            {
                Message: 'Hello MoOn',
            },
        ]);
        // 👀 Access the results of the Task
        console.log(res[0].TransformedMessage);
        console.log(res[1].TransformedMessage);
        // !!
        // > Bulk Run a Task with runMany
        const runManyRes = yield workflow_1.simple.runMany([
            {
                input: {
                    Message: 'HeLlO WoRlD',
                },
            },
            {
                input: {
                    Message: 'Hello MoOn',
                },
                opts: {
                    priority: 3,
                },
            },
        ]);
        console.log(runManyRes[0].TransformedMessage);
        console.log(runManyRes[1].TransformedMessage);
        // !!
        // > Bulk Run Tasks from within a Task
        const parent = hatchet_client_1.hatchet.task({
            name: 'simple',
            fn: (input, ctx) => __awaiter(this, void 0, void 0, function* () {
                // Bulk run two tasks in parallel
                const child = yield ctx.bulkRunChildren([
                    {
                        workflow: workflow_1.simple,
                        input: {
                            Message: 'Hello, World!',
                        },
                    },
                    {
                        workflow: workflow_1.simple,
                        input: {
                            Message: 'Hello, Moon!',
                        },
                    },
                ]);
                return {
                    TransformedMessage: `${child[0].TransformedMessage} ${child[1].TransformedMessage}`,
                };
            }),
        });
        // !!
    });
}
if (require.main === module) {
    main();
}
