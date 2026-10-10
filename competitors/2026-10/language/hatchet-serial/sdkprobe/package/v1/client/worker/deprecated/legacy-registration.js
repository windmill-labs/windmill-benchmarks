"use strict";
/**
 * Legacy worker registration using the deprecated `slots` proto field
 * instead of `slotConfig`. For backward compatibility with engines
 * that do not support multiple slot types.
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
exports.legacyGetActionListener = legacyGetActionListener;
const dispatcher_client_1 = require("../../../../clients/dispatcher/dispatcher-client");
const action_listener_1 = require("../../../../clients/dispatcher/action-listener");
/**
 * Registers a worker using the legacy `slots` proto field instead of `slotConfig`.
 */
function legacyGetActionListener(dispatcher, options) {
    return __awaiter(this, void 0, void 0, function* () {
        const registration = yield dispatcher.client.register({
            workerName: options.workerName,
            services: options.services,
            actions: options.actions,
            slots: options.slots,
            labels: options.labels ? (0, dispatcher_client_1.mapLabels)(options.labels) : undefined,
            runtimeInfo: dispatcher.getRuntimeInfo(),
        });
        return new action_listener_1.ActionListener(dispatcher, registration.workerId);
    });
}
