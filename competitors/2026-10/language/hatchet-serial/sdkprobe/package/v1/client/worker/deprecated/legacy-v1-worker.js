"use strict";
/**
 * Legacy V1Worker subclass that registers with the old `slots` proto field
 * instead of `slotConfig`. Used when connected to pre-slot-config engines.
 */
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
exports.LegacyV1Worker = void 0;
const worker_internal_1 = require("../worker-internal");
const legacy_registration_1 = require("./legacy-registration");
class LegacyV1Worker extends worker_internal_1.InternalWorker {
    constructor(client, options, legacySlots) {
        super(client, options);
        this._legacySlotCount = legacySlots;
    }
    /**
     * Override registration to use the legacy `slots` proto field.
     */
    createListener() {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, legacy_registration_1.legacyGetActionListener)(this.client.dispatcher, {
                workerName: this.name,
                services: ['default'],
                actions: Object.keys(this.action_registry),
                slots: this._legacySlotCount,
                labels: this.labels,
            });
        });
    }
}
exports.LegacyV1Worker = LegacyV1Worker;
