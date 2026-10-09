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
const workflow_1 = require("./workflow");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Run methods
        const input = { Message: 'Hello, World!' };
        // run now
        const result = yield workflow_1.simple.run(input);
        const runReference = yield workflow_1.simple.runNoWait(input);
        // or in the future
        const runAt = new Date(new Date().setHours(12, 0, 0, 0) + 24 * 60 * 60 * 1000);
        const scheduled = yield workflow_1.simple.schedule(runAt, input);
        const cron = yield workflow_1.simple.cron('simple-daily', '0 0 * * *', input);
        // !!
    });
}
function runFlavors() {
    return __awaiter(this, void 0, void 0, function* () {
        // > Run method flavors
        const input = { Message: 'Hello, World!' };
        // Run workflow and wait for the result
        const result = yield workflow_1.simple.run(input);
        // Enqueue workflow to be executed asynchronously
        const runReference = yield workflow_1.simple.runNoWait(input);
        // !!
    });
}
