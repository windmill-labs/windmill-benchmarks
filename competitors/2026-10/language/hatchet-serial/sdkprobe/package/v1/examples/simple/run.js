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
exports.extra = extra;
const hatchet_client_1 = require("../hatchet-client");
const workflow_1 = require("./workflow");
const workflow_with_child_1 = require("./workflow-with-child");
const zod_1 = require("./zod");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Running a Task
        const res = yield workflow_with_child_1.parent.run({
            Message: 'HeLlO WoRlD',
        }, {
            additionalMetadata: {
                test: 'test',
            },
        });
        const res3 = yield zod_1.simpleWithZod.run({
            Message: 'HeLlO WoRlD',
        });
        console.log(res3.TransformedMessage);
        // 👀 Access the results of the Task
        console.log(res.TransformedMessage);
        // !!
    });
}
function extra() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Running Multiple Tasks
        const res1 = workflow_1.simple.run({
            Message: 'HeLlO WoRlD',
        });
        const res2 = workflow_1.simple.run({
            Message: 'Hello MoOn',
        });
        const results = yield Promise.all([res1, res2]);
        console.log(results[0].TransformedMessage);
        console.log(results[1].TransformedMessage);
        // !!
        // > Spawning Tasks from within a Task
        const parentTask = hatchet_client_1.hatchet.task({
            name: 'parent',
            fn: (input, ctx) => __awaiter(this, void 0, void 0, function* () {
                // Simply the task and it will be spawned from the parent task
                const child = yield workflow_1.simple.run({
                    Message: 'HeLlO WoRlD',
                });
                return {
                    result: child.TransformedMessage,
                };
            }),
        });
        // !!
        // > Run with metadata
        const withMetadata = workflow_1.simple.run({
            Message: 'HeLlO WoRlD',
        }, {
            additionalMetadata: {
                source: 'api', // Arbitrary key-value pair
            },
        });
        // !!
    });
}
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => {
        process.exit(0);
    });
}
