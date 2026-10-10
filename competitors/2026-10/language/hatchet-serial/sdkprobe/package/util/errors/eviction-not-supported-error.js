"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.EvictionNotSupportedError = void 0;
const v1_1 = require("../../v1");
const hatchet_error_1 = __importDefault(require("./hatchet-error"));
class EvictionNotSupportedError extends hatchet_error_1.default {
    constructor(engineVersion) {
        const versionInfo = engineVersion ? ` (engine ${engineVersion})` : '';
        super(`Durable eviction is not supported by the connected engine${versionInfo}. ` +
            `Please upgrade your Hatchet engine to ${v1_1.MinEngineVersion.DURABLE_EVICTION} or later.`);
        this.name = 'EvictionNotSupportedError';
        this.engineVersion = engineVersion;
    }
}
exports.EvictionNotSupportedError = EvictionNotSupportedError;
