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
exports.isLegacyDurableTransport = isLegacyDurableTransport;
exports.waitForPreEviction = waitForPreEviction;
/**
 * Pre-eviction fallback for DurableContext.
 *
 * Supports engines older than MinEngineVersion.DURABLE_EVICTION.
 * Remove this module when support for those engines is dropped.
 */
const conditions_1 = require("../../../conditions");
const transformer_1 = require("../../../conditions/transformer");
const condition_1 = require("../../../../protoc/v1/shared/condition");
function isLegacyDurableTransport(value) {
    const candidate = value;
    return (typeof (candidate === null || candidate === void 0 ? void 0 : candidate.registerDurableEvent) === 'function' && typeof (candidate === null || candidate === void 0 ? void 0 : candidate.result) === 'function');
}
function waitForPreEviction(durableListener, taskRunExternalId, waitKey, conditions, namespace, signal) {
    return __awaiter(this, void 0, void 0, function* () {
        const pbConditions = (0, transformer_1.conditionsToPb)((0, conditions_1.Render)(condition_1.Action.CREATE, conditions), namespace);
        const key = `waitFor-${waitKey}`;
        yield durableListener.registerDurableEvent({
            taskId: taskRunExternalId,
            signalKey: key,
            sleepConditions: pbConditions.sleepConditions,
            userEventConditions: pbConditions.userEventConditions,
        });
        const event = yield durableListener.result({ taskId: taskRunExternalId, signalKey: key }, { signal });
        const eventData = event.data instanceof Uint8Array ? new TextDecoder().decode(event.data) : event.data;
        const res = JSON.parse(eventData);
        return { result: res.CREATE, nextWaitKey: waitKey + 1 };
    });
}
