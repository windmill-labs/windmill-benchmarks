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
/**
 * Dedicated worker for capacity-eviction e2e tests.
 *
 * Runs with durableSlots=1 so that a single waiting durable task triggers
 * capacity pressure and gets evicted (even with ttl=undefined).
 */
const hatchet_client_1 = require("../hatchet-client");
const workflow_1 = require("./workflow");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet_client_1.hatchet.worker('capacity-eviction-worker', {
            durableSlots: 1,
            workflows: [workflow_1.capacityEvictableSleep],
        });
        yield worker.start();
    });
}
if (require.main === module) {
    main();
}
