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
const cron_worker_1 = require("./cron-worker");
const hatchet = sdk_1.default.init();
// This example assumes you have a worker already running
// and registered the cron workflow to it
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // ? Create
        // You can create dynamic cron triggers programmatically via the API
        const createdCron = yield hatchet.crons.create(cron_worker_1.simpleCronWorkflow, // workflow object or string workflow id
        {
            name: 'customer-a-daily-report', // friendly name for the cron trigger
            expression: '0 12 * * *', // every day at noon
            input: {
                name: 'John Doe',
            },
            additionalMetadata: {
                customerId: '123',
            },
        });
        const { id } = createdCron.metadata; // id which you can later use to reference the cron trigger
        // !!
        // ? Get
        // You can get a specific cron trigger by passing in the cron trigger id
        const cron = yield hatchet.crons.get(id);
        // !!
        // ? Delete
        // You can delete a cron trigger by passing the cron object
        // or a cron Id to the delete method
        yield hatchet.crons.delete(cron);
        // !!
        // ? List
        // You can list all cron triggers by passing in a query object
        const cronList = yield hatchet.crons.list({
            offset: 0,
            limit: 10,
        });
        // !!
    });
}
main();
