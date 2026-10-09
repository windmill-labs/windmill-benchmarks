"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseJSON = parseJSON;
const hatchet_error_1 = require("./errors/hatchet-error");
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function parseJSON(json) {
    try {
        const firstParse = JSON.parse(json);
        // Hatchet engine versions <=0.14.0 return JSON as a quoted string which needs to be parsed again.
        // This is a workaround for that issue, but will not be needed in future versions.
        try {
            return JSON.parse(firstParse);
        }
        catch (_a) {
            return firstParse;
        }
    }
    catch (e) {
        throw new Error(`Could not parse JSON: ${(0, hatchet_error_1.getErrorMessage)(e)}`, { cause: e });
    }
}
