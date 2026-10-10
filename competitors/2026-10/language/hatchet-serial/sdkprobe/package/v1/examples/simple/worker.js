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
// > Declaring a Worker
const hatchet_client_1 = require("../hatchet-client");
const workflow_1 = require("./workflow");
const workflow_with_child_1 = require("./workflow-with-child");
const zod_1 = require("./zod");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet_client_1.hatchet.worker('simple-worker', {
            // 👀 Declare the workflows that the worker can execute
            workflows: [workflow_1.simple, zod_1.simpleWithZod, workflow_with_child_1.parent, workflow_with_child_1.child],
            // 👀 Declare the number of concurrent task runs the worker can accept
            slots: 100,
        });
        yield worker.start();
    });
}
if (require.main === module) {
    main();
}
// !!
