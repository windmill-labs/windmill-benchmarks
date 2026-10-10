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
const tools_1 = require("./tools");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const lookupCustomerTool = (0, tools_1.createLookupCustomerToolOpenai)();
        const checkOrderStatusTool = (0, tools_1.createCheckOrderStatusToolOpenai)();
        const ticketTool = (0, tools_1.createTicketToolOpenai)();
        // Dynamic import — @openai/agents crashes at load time with Zod v3, so we delay
        // the import until after mcpTool has already verified Zod v4 is available.
        const { Agent, run } = yield Promise.resolve().then(() => __importStar(require('@openai/agents')));
        const agent = new Agent({
            name: 'support-agent',
            tools: [lookupCustomerTool, checkOrderStatusTool, ticketTool],
        });
        const result = yield run(agent, 'Customer C-100 says order ORD-9987 has not arrived. ' +
            'Look up the customer, check the order status, and create a ' +
            'support ticket if the order has a known issue or delayed delivery. ' +
            'If you create a ticket, use priority "high", subject ' +
            '"Delayed order ORD-9987", and a body that summarizes the known ' +
            'carrier delay. Then summarize what happened.');
        console.log(result.finalOutput);
    });
}
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => {
        process.exit(0);
    });
}
