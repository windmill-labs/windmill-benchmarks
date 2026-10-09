"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runStatusToJSON = void 0;
exports.parseTaskOutput = parseTaskOutput;
exports.toRunDetail = toRunDetail;
const data_contracts_1 = require("../clients/rest/generated/data-contracts");
const workflows_1 = require("../protoc/v1/workflows");
Object.defineProperty(exports, "runStatusToJSON", { enumerable: true, get: function () { return workflows_1.runStatusToJSON; } });
// EVICTED is not in V1TaskStatus; treat as RUNNING per the proto comment.
const PROTO_STATUS_MAP = {
    [workflows_1.RunStatus.QUEUED]: data_contracts_1.V1TaskStatus.QUEUED,
    [workflows_1.RunStatus.RUNNING]: data_contracts_1.V1TaskStatus.RUNNING,
    [workflows_1.RunStatus.COMPLETED]: data_contracts_1.V1TaskStatus.COMPLETED,
    [workflows_1.RunStatus.FAILED]: data_contracts_1.V1TaskStatus.FAILED,
    [workflows_1.RunStatus.CANCELLED]: data_contracts_1.V1TaskStatus.CANCELLED,
    [workflows_1.RunStatus.EVICTED]: data_contracts_1.V1TaskStatus.RUNNING,
    [workflows_1.RunStatus.UNRECOGNIZED]: data_contracts_1.V1TaskStatus.RUNNING,
};
function decodeText(b) {
    return (b === null || b === void 0 ? void 0 : b.length) ? new TextDecoder().decode(b) : undefined;
}
/**
 * The forgiving decode `getDetails` shapes payloads with: absent, `null` and malformed JSON
 * all read as `null`, so inspecting a run never throws.
 */
function decodeJson(text) {
    if (text === undefined)
        return null;
    try {
        return JSON.parse(text);
    }
    catch (_a) {
        return null;
    }
}
/**
 * A task's output the way the Node client's `result()` reads it (`JSON.parse(output || '{}')`):
 * nothing stored is `{}`, a stored `null` is `null`, and stored text that is not JSON throws
 * the `SyntaxError`, so a corrupt output is never mistaken for an empty one.
 */
function parseTaskOutput(task) {
    var _a;
    if (task.rawOutput === undefined) {
        return (_a = task.output) !== null && _a !== void 0 ? _a : {};
    }
    // The forgiving decode already parsed valid JSON; only a `null` result needs the strict
    // parse to tell a stored `null` from text that failed to parse.
    return task.output !== null ? task.output : JSON.parse(task.rawOutput);
}
/**
 * Converts a `GetRunDetails` response into the `RunDetail` both clients return: statuses
 * become `V1TaskStatus` values and the JSON payloads are decoded.
 */
function toRunDetail(raw) {
    var _a;
    return {
        status: (_a = PROTO_STATUS_MAP[raw.status]) !== null && _a !== void 0 ? _a : data_contracts_1.V1TaskStatus.RUNNING,
        done: raw.done,
        input: decodeJson(decodeText(raw.input)),
        additionalMetadata: decodeJson(decodeText(raw.additionalMetadata)),
        isEvicted: raw.isEvicted,
        taskRuns: Object.fromEntries(Object.entries(raw.taskRuns).map(([id, tr]) => {
            var _a;
            const rawOutput = decodeText(tr.output);
            return [
                id,
                {
                    externalId: tr.externalId,
                    readableId: tr.readableId,
                    status: (_a = PROTO_STATUS_MAP[tr.status]) !== null && _a !== void 0 ? _a : data_contracts_1.V1TaskStatus.RUNNING,
                    output: decodeJson(rawOutput),
                    rawOutput,
                    error: tr.error,
                    isEvicted: tr.isEvicted,
                },
            ];
        })),
    };
}
