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
        var _a;
        const { runs } = hatchet_client_1.hatchet;
        // > API operations
        // list all failed runs
        const allFailedRuns = yield runs.list({
            statuses: [data_contracts_1.V1TaskStatus.FAILED],
        });
        // replay by ids
        yield runs.replay({ ids: (_a = allFailedRuns.rows) === null || _a === void 0 ? void 0 : _a.map((r) => r.metadata.id) });
        // or you can run bulk operations with filters directly
        yield runs.cancel({
            filters: {
                since: new Date('2025-03-27'),
                additionalMetadata: { user: '123' },
            },
        });
        // !!
    });
}
