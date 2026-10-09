"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MinEngineVersion = void 0;
exports.supportsEviction = supportsEviction;
const deprecation_1 = require("./deprecated/deprecation");
exports.MinEngineVersion = {
    SLOT_CONFIG: 'v0.78.23',
    DURABLE_EVICTION: 'v0.80.0',
};
function supportsEviction(engineVersion) {
    if (!engineVersion)
        return false;
    return !(0, deprecation_1.semverLessThan)(engineVersion, exports.MinEngineVersion.DURABLE_EVICTION);
}
