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
const sdk_1 = __importDefault(require("../../sdk"));
const hatchet = sdk_1.default.init();
// > OnFailure Step
// This workflow will fail because the step will throw an error
// we define an onFailure step to handle this case
const workflow = {
    // ... normal workflow definition
    id: 'on-failure-example',
    description: 'test',
    on: {
        event: 'user:create',
    },
    // ,
    steps: [
        {
            name: 'step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                // 👀 this step will always throw an error
                throw new Error('Step 1 failed');
            }),
        },
    ],
    // 👀 After the workflow fails, this special step will run
    onFailure: {
        name: 'on-failure-step',
        run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
            // 👀 we can do things like perform cleanup logic
            // or notify a user here
            // 👀 you can access the error from the failed step(s) like this
            console.log(ctx.errors());
            return { onFailure: 'step' };
        }),
    },
};
// !!
// > OnFailure With Details
// Coming soon to TypeScript! https://github.com/hatchet-dev/hatchet-typescript/issues/447
// !!
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet.worker('example-worker', 1);
        yield worker.registerWorkflow(workflow);
        worker.start();
    });
}
main();
