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
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DispatcherClient = void 0;
exports.mapLabels = mapLabels;
const dispatcher_1 = require("../../protoc/dispatcher");
const hatchet_error_1 = require("../../util/errors/hatchet-error");
const retrier_1 = require("../../util/retrier");
const version_1 = require("../../version");
const slot_types_1 = require("../../v1/slot-types");
const action_listener_1 = require("./action-listener");
class DispatcherClient {
    constructor(config, channel, factory) {
        this.config = config;
        this.client = factory.create(dispatcher_1.DispatcherDefinition, channel);
        this.logger = config.logger(`Dispatcher`, config.log_level);
    }
    getRuntimeInfo() {
        return {
            sdkVersion: version_1.HATCHET_VERSION,
            language: dispatcher_1.SDKS.TYPESCRIPT,
            languageVersion: process.version,
            os: process.platform,
        };
    }
    getActionListener(options) {
        return __awaiter(this, void 0, void 0, function* () {
            // Register the worker
            const slotConfig = options.slotConfig ||
                (options.slots || options.durableSlots || options.maxRuns
                    ? Object.assign(Object.assign({}, (options.slots || options.maxRuns
                        ? { [slot_types_1.SlotType.Default]: options.slots || options.maxRuns || 0 }
                        : {})), (options.durableSlots ? { [slot_types_1.SlotType.Durable]: options.durableSlots } : {})) : undefined);
            const registration = yield this.client.register({
                workerName: options.workerName,
                services: options.services,
                actions: options.actions,
                slotConfig,
                labels: options.labels ? mapLabels(options.labels) : undefined,
                runtimeInfo: this.getRuntimeInfo(),
            });
            return new action_listener_1.ActionListener(this, registration.workerId);
        });
    }
    /**
     * Calls the GetVersion RPC. Returns the engine semantic version string.
     * Throws a gRPC error with code UNIMPLEMENTED on older engines.
     */
    getVersion() {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.client.getVersion({});
            return response.version;
        });
    }
    sendStepActionEvent(in_) {
        return __awaiter(this, void 0, void 0, function* () {
            const { taskId, taskRunExternalId } = in_, rest = __rest(in_, ["taskId", "taskRunExternalId"]);
            const event = Object.assign(Object.assign({}, rest), { taskId: taskId !== null && taskId !== void 0 ? taskId : '', taskRunExternalId: taskRunExternalId !== null && taskRunExternalId !== void 0 ? taskRunExternalId : '' });
            try {
                return yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.client.sendStepActionEvent(event); }), this.logger);
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    /**
     * Reports a lifecycle event (STARTED, COMPLETED, or FAILED) covering one or more
     * members of a batch task's execution.
     */
    sendBatchActionEvent(in_) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                return yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.client.sendBatchActionEvent(in_); }), this.logger);
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    sendGroupKeyActionEvent(in_) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                return yield (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.client.sendGroupKeyActionEvent(in_); }), this.logger);
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    putOverridesData(in_) {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () { return this.client.putOverridesData(in_); }), this.logger).catch((e) => {
                this.logger.warn(`Could not put overrides data: ${e.message}`);
            });
        });
    }
    refreshTimeout(incrementTimeoutBy, taskRunExternalId) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                return this.client.refreshTimeout({
                    taskRunExternalId,
                    incrementTimeoutBy,
                });
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
    upsertWorkerLabels(workerId, labels) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                return yield this.client.upsertWorkerLabels({
                    workerId,
                    labels: mapLabels(labels),
                });
            }
            catch (e) {
                throw (0, hatchet_error_1.toHatchetError)(e);
            }
        });
    }
}
exports.DispatcherClient = DispatcherClient;
function mapLabels(in_) {
    return Object.entries(in_).reduce((acc, [key, value]) => (Object.assign(Object.assign({}, acc), { [key]: {
            strValue: typeof value === 'string' ? value : undefined,
            intValue: typeof value === 'number' ? value : undefined,
        } })), {});
}
