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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sdk_1 = __importDefault(require("../../../sdk"));
const simple_worker_1 = require("../simple-worker");
const hatchet = sdk_1.default.init();
// This example assumes you have a worker already running
// and registered simpleWorkflow to it
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // ? Create
        // You can create dynamic scheduled runs programmatically via the API
        const createdScheduledRun = yield hatchet.schedules.create(simple_worker_1.simpleWorkflow, // workflow object or string workflow id
        {
            triggerAt: new Date(Date.now() + 1000 * 60 * 60 * 24), // 24 hours from now
            input: {
                name: 'John Doe',
            },
            additionalMetadata: {
                customerId: '123',
            },
        });
        const { id } = createdScheduledRun.metadata; // id which you can later use to reference the scheduled run
        // !!
        // ? Get
        // You can get a specific scheduled run by passing in the scheduled run id
        const scheduledRun = yield hatchet.schedules.get(id);
        // !!
        // ? Delete
        // You can delete a scheduled run by passing the scheduled run object
        // or a scheduled run Id to the delete method
        yield hatchet.schedules.delete(scheduledRun);
        // !!
        // ? List
        // You can list all scheduled runs by passing in a query object
        const scheduledRunList = yield hatchet.schedules.list({
            offset: 0,
            limit: 10,
        });
        // !!
    });
}
main();
