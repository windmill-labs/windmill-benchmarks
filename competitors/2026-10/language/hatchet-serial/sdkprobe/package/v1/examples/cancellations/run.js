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
// > Running a Task with Results
const sleep_1 = __importDefault(require("../../../util/sleep"));
const workflow_1 = require("./workflow");
const hatchet_client_1 = require("../hatchet-client");
// ...
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const run = yield workflow_1.cancellation.runNoWait({});
        const run1 = yield workflow_1.cancellation.runNoWait({});
        yield (0, sleep_1.default)(1000);
        yield run.cancel();
        const res = yield run.output;
        const res1 = yield run1.output;
        console.log('canceled', res);
        console.log('completed', res1);
        yield (0, sleep_1.default)(1000);
        yield run.replay();
        const resReplay = yield run.output;
        console.log(resReplay);
        const run2 = yield workflow_1.cancellation.runNoWait({}, { additionalMetadata: { test: 'abc' } });
        const run4 = yield workflow_1.cancellation.runNoWait({}, { additionalMetadata: { test: 'test' } });
        yield (0, sleep_1.default)(1000);
        yield hatchet_client_1.hatchet.runs.cancel({
            filters: {
                since: new Date(Date.now() - 60 * 60),
                additionalMetadata: { test: 'test' },
            },
        });
        const res3 = yield Promise.all([run2.output, run4.output]);
        console.log(res3);
        // !!
    });
}
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => process.exit(0));
}
