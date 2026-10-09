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
const v1_1 = require("../..");
const workflow_1 = require("./workflow");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            console.log('running priority workflow');
            // > Run a Task with a Priority
            const run = workflow_1.priority.run(new Date(Date.now() + 60 * 60 * 1000), { priority: v1_1.Priority.HIGH });
            // !!
            // > Schedule and cron
            const scheduled = workflow_1.priority.schedule(new Date(Date.now() + 60 * 60 * 1000), {}, { priority: v1_1.Priority.HIGH });
            const delayed = workflow_1.priority.delay(60 * 60 * 1000, {}, { priority: v1_1.Priority.HIGH });
            const cron = workflow_1.priority.cron(`daily-cron-${Math.random()}`, '0 0 * * *', {}, { priority: v1_1.Priority.HIGH });
            // !!
            const [scheduledResult, delayedResult] = yield Promise.all([scheduled, delayed]);
            console.log('scheduledResult', scheduledResult);
            console.log('delayedResult', delayedResult);
            // !!
        }
        catch (e) {
            console.log('error', e);
        }
    });
}
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => process.exit(0));
}
