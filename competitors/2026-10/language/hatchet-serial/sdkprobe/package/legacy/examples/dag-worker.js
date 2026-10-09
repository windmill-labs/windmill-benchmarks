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
const hatchet = sdk_1.default.init({
    log_level: 'OFF',
});
const sleep = (ms) => new Promise((resolve) => {
    setTimeout(resolve, ms);
});
const workflow = {
    id: 'dag-example',
    description: 'test',
    on: {
        event: 'user:create',
    },
    steps: [
        {
            name: 'dag-step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                console.log('executed step1!');
                yield sleep(5000);
                return { step1: 'step1' };
            }),
        },
        {
            name: 'dag-step2',
            parents: ['dag-step1'],
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                console.log('executed step2!');
                yield sleep(5000);
                return { step2: 'step2' };
            }),
        },
        {
            name: 'dag-step3',
            parents: ['dag-step1', 'dag-step2'],
            run: (ctx) => {
                console.log('executed step3!');
                return { step3: 'step3' };
            },
        },
        {
            name: 'dag-step4',
            parents: ['dag-step1', 'dag-step3'],
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                yield sleep(5000);
                // simulate a really slow network call
                setTimeout(() => __awaiter(void 0, void 0, void 0, function* () {
                    yield sleep(1000);
                }), 5000);
                return { step4: 'step4' };
            }),
        },
    ],
};
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet.worker('example-worker', 1);
        yield worker.registerWorkflow(workflow);
        worker.start();
    });
}
main();
