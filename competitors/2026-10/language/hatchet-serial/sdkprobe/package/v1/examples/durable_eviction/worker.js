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
        const worker = yield hatchet_client_1.hatchet.worker('eviction-worker', {
            workflows: [
                workflow_1.evictableSleep,
                workflow_1.evictableWaitForEvent,
                workflow_1.evictableChildSpawn,
                workflow_1.evictableChildBulkSpawn,
                workflow_1.multipleEviction,
                workflow_1.nonEvictableSleep,
                workflow_1.childTask,
                workflow_1.bulkChildTask,
                workflow_1.evictableSleepForGracefulTermination,
            ],
        });
        yield worker.start();
    });
}
if (require.main === module) {
    main();
}
