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
exports.EventsClient = void 0;
const hatchet_error_1 = require("../../util/errors/hatchet-error");
const retrier_1 = require("../../util/retrier");
const rpc_1 = require("../../clients/event/rpc");
/** Pushes events that trigger event-bound workflows. */
class EventsClient {
    constructor(deps) {
        this.deps = deps;
    }
    /**
     * Pushes one event. The key is namespaced the way the Node client namespaces it.
     * @returns the stored event
     */
    push(type_1, input_1) {
        return __awaiter(this, arguments, void 0, function* (type, input, options = {}) {
            const request = (0, rpc_1.buildPushEventRequest)(type, input, options, this.deps.namespace);
            try {
                const event = yield (0, retrier_1.retrier)(() => this.deps.rpc.push(request), this.deps.logger, this.deps.retrier);
                this.deps.logger.debug(`Event pushed: ${request.key}`);
                return event;
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    /**
     * Pushes many events of one type in a single call.
     * @returns the stored events
     */
    bulkPush(type_1, inputs_1) {
        return __awaiter(this, arguments, void 0, function* (type, inputs, options = {}) {
            var _a, _b;
            const request = (0, rpc_1.buildBulkPushEventRequest)(type, inputs, options, this.deps.namespace);
            try {
                const events = yield (0, retrier_1.retrier)(() => this.deps.rpc.bulkPush(request), this.deps.logger, this.deps.retrier);
                this.deps.logger.debug(`Bulk events pushed for type: ${(_b = (_a = request.events[0]) === null || _a === void 0 ? void 0 : _a.key) !== null && _b !== void 0 ? _b : type}`);
                return events;
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
}
exports.EventsClient = EventsClient;
