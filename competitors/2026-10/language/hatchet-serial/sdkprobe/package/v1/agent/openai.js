"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
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
exports.OpenAIToolFunc = void 0;
const z = __importStar(require("zod/v4"));
const OpenAIToolFunc = (runnable) => {
    // Check both requirements before loading @openai/agents — it crashes at load time with Zod v3.
    // z is imported from 'zod', so '_zod' in z.string() reflects the user's actual installed version.
    const hasZodV4 = '_zod' in z.string();
    let hasOpenAIAgents = true;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let tool;
    try {
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        ({ tool } = require('@openai/agents'));
    }
    catch (_a) {
        hasOpenAIAgents = false;
    }
    if (!hasZodV4 || !hasOpenAIAgents) {
        throw new Error("To use Hatchet's OpenAI agent SDK integration, you must install Zod v4 and @openai/agents: npm install zod@^4.0.0 @openai/agents");
    }
    if (!runnable.definition.inputValidator) {
        throw new Error('inputValidator must be defined');
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const inputValidatorV4 = runnable.definition.inputValidator;
    const { description } = runnable.definition;
    if (description === undefined) {
        throw new Error('Runnable description must be defined');
    }
    return tool({
        name: runnable.name,
        description: description,
        parameters: z.toJSONSchema(inputValidatorV4),
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        execute: (input) => __awaiter(void 0, void 0, void 0, function* () {
            const result = yield runnable.run(input);
            return JSON.stringify(result);
        }),
    });
};
exports.OpenAIToolFunc = OpenAIToolFunc;
