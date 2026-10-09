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
exports.simple = void 0;
exports.simple = {
    id: 'legacy-workflow',
    description: 'test',
    on: {
        event: 'user:create',
    },
    steps: [
        {
            name: 'step1',
            run: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
                const input = ctx.workflowInput();
                return { step1: `original input: ${input.Message}` };
            }),
        },
        {
            name: 'step2',
            parents: ['step1'],
            run: (ctx) => {
                const step1Output = ctx.stepOutput('step1');
                return { step2: `step1 output: ${step1Output.step1}` };
            },
        },
    ],
};
