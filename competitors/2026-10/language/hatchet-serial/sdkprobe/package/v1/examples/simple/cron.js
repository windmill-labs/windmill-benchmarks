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
        // > Create
        const cron = yield workflow_1.simple.cron('simple-daily', '0 0 * * *', {
            Message: 'hello',
        });
        // it may be useful to save the cron id for later
        const cronId = cron.metadata.id;
        // !!
        console.log(cron.metadata.id);
        // > Delete
        yield hatchet_client_1.hatchet.crons.delete(cronId);
        // !!
        // > List
        const crons = yield hatchet_client_1.hatchet.crons.list({
            workflow: workflow_1.simple,
        });
        // !!
        console.log(crons);
    });
}
if (require.main === module) {
    main();
}
