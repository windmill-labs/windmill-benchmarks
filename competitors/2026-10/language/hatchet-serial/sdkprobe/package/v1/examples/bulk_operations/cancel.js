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
const data_contracts_1 = require("../../../clients/rest/generated/data-contracts");
const hatchet_client_1 = require("../hatchet-client");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        var _a, _b, _c;
        // > Setup
        const workflows = yield hatchet_client_1.hatchet.workflows.list();
        if (!((_a = workflows.rows) === null || _a === void 0 ? void 0 : _a.length)) {
            throw new Error('no workflows found');
        }
        const [workflow] = workflows.rows;
        // !!
        // > List runs
        const workflowRuns = yield hatchet_client_1.hatchet.runs.list({
            workflowNames: [workflow.name],
        });
        // !!
        // > Cancel by run ids
        const runIds = (_c = (_b = workflowRuns.rows) === null || _b === void 0 ? void 0 : _b.map((run) => run.metadata.id)) !== null && _c !== void 0 ? _c : [];
        // to replay runs by their ids, use `hatchet.runs.replay` instead
        yield hatchet_client_1.hatchet.runs.cancel({ ids: runIds });
        // !!
        // > Cancel by filters
        // to replay runs matching filters, use `hatchet.runs.replay` instead
        yield hatchet_client_1.hatchet.runs.cancel({
            filters: {
                since: new Date(Date.now() - 24 * 60 * 60 * 1000),
                until: new Date(),
                statuses: [data_contracts_1.V1TaskStatus.RUNNING],
                workflowNames: [workflow.name],
                additionalMetadata: { key: 'value' },
            },
        });
        // !!
    });
}
if (require.main === module) {
    main()
        .catch(console.error)
        .finally(() => process.exit(0));
}
