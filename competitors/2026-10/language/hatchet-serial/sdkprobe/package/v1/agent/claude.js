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
exports.ClaudeToolFunc = void 0;
const z = __importStar(require("zod/v4"));
const ClaudeToolFunc = (runnable, annotations) => {
    const hasZodV4 = '_zod' in z.string();
    let hasClaudeAgentSdk = true;
    try {
        require.resolve('@anthropic-ai/claude-agent-sdk');
    }
    catch (_a) {
        hasClaudeAgentSdk = false;
    }
    if (!hasZodV4 || !hasClaudeAgentSdk) {
        throw new Error("To use Hatchet's Claude agent SDK integration, you must install Zod v4 and @anthropic-ai/claude-agent-sdk: npm install zod@^4.0.0 @anthropic-ai/claude-agent-sdk @modelcontextprotocol/sdk");
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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handler = (args, _) => __awaiter(void 0, void 0, void 0, function* () {
        const result = yield runnable.run(args);
        return {
            content: [{ type: 'text', text: JSON.stringify(result) }],
        };
    });
    return {
        annotations: annotations,
        description: description,
        handler: handler,
        name: runnable.name,
        inputSchema: inputValidatorV4.shape,
    };
};
exports.ClaudeToolFunc = ClaudeToolFunc;
