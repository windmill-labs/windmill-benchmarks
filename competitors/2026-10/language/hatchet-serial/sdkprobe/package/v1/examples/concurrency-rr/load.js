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
function generateRandomString(length) {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const groupCount = 2;
        const runsPerGroup = 20000;
        const BATCH_SIZE = 400;
        const workflowRuns = [];
        for (let i = 0; i < groupCount; i++) {
            for (let j = 0; j < runsPerGroup; j++) {
                workflowRuns.push({
                    workflowName: workflow_1.simpleConcurrency.definition.name,
                    input: {
                        Message: generateRandomString(10),
                        GroupKey: `group-${i}`,
                    },
                });
            }
        }
        // Shuffle the workflow runs array
        for (let i = workflowRuns.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [workflowRuns[i], workflowRuns[j]] = [workflowRuns[j], workflowRuns[i]];
        }
        // Process workflows in batches
        for (let i = 0; i < workflowRuns.length; i += BATCH_SIZE) {
            const batch = workflowRuns.slice(i, i + BATCH_SIZE);
            yield hatchet_client_1.hatchet.admin.runWorkflows(batch);
        }
    });
}
if (require.main === module) {
    main().then(() => process.exit(0));
}
