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
const workflows_1 = require("../../../protoc/workflows");
const sdk_1 = __importDefault(require("../../../sdk"));
const hatchet = sdk_1.default.init();
const workflow = {
    id: 'rate-limit-workflow',
    description: 'test',
    on: {
        event: 'rate-limit:create',
    },
    steps: [
        {
            name: 'dynamic',
            rate_limits: [
                {
                    dynamicKey: 'input.group',
                    units: 1,
                    limit: 10,
                    duration: workflows_1.RateLimitDuration.DAY,
                },
            ],
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                console.log('starting step1 with the following input and a dynamic rate limit', ctx.workflowInput());
                return { step1: 'step1 results!' };
            }),
        },
        {
            name: 'static',
            rate_limits: [{ key: 'test-limit', units: 1 }],
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                console.log('starting step1 with the following input and a static rate limit', ctx.workflowInput());
                return { step1: 'step1 results!' };
            }),
        },
    ],
};
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        yield hatchet.admin.putRateLimit('test-limit', 1, workflows_1.RateLimitDuration.MINUTE);
        const worker = yield hatchet.worker('example-worker');
        yield worker.registerWorkflow(workflow);
        worker.start();
    });
}
main();
