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
exports.EventClient = exports.LogLevel = void 0;
const transport_1 = require("../transport");
const hatchet_error_1 = require("../../util/errors/hatchet-error");
const retrier_1 = require("../../util/retrier");
const apply_namespace_1 = require("../../util/apply-namespace");
const rpc_1 = require("./rpc");
Object.defineProperty(exports, "LogLevel", { enumerable: true, get: function () { return rpc_1.LogLevel; } });
class EventClient {
    // The positional channel and factory parameters are part of the public constructor signature;
    // unary event calls go over the transport and do not use them.
    constructor(config, _channel, _factory, api, transport = (0, transport_1.createNodeTransport)(config)) {
        this.config = config;
        this.client = (0, rpc_1.createEventsRpc)(transport);
        this.logger = config.logger(`Dispatcher`, config.log_level);
        this.retrier = retrier_1.retrier;
        this.api = api;
        this.tenantId = config.tenant_id;
    }
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchPushEvent.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    push(type, input, options = {}) {
        const namespacedType = (0, apply_namespace_1.applyNamespace)(type, this.config.namespace);
        const req = (0, rpc_1.buildPushEventRequest)(type, input, options, this.config.namespace);
        return this.retrier(() => __awaiter(this, void 0, void 0, function* () { return this.client.push(req); }), this.logger, this.config.retrier)
            .then((result) => {
            this.logger.info(`Event pushed: ${namespacedType}`);
            return result;
        })
            .catch((e) => {
            throw (0, hatchet_error_1.toHatchetError)(e);
        });
    }
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchBulkPushEvent.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    bulkPush(type, inputs, options = {}) {
        const namespacedType = (0, apply_namespace_1.applyNamespace)(type, this.config.namespace);
        const req = (0, rpc_1.buildBulkPushEventRequest)(type, inputs, options, this.config.namespace);
        return this.retrier(() => __awaiter(this, void 0, void 0, function* () { return this.client.bulkPush(req); }), this.logger, this.config.retrier)
            .then((result) => {
            this.logger.info(`Bulk events pushed for type: ${namespacedType}`);
            return result;
        })
            .catch((e) => {
            throw (0, hatchet_error_1.toHatchetError)(e);
        });
    }
    putLog(taskRunExternalId, log, level, taskRetryCount, metadata) {
        return __awaiter(this, void 0, void 0, function* () {
            const createdAt = new Date();
            if (log.length > 1000) {
                this.logger.warn(`log is too long, skipping: ${log.length} characters`);
                return;
            }
            //  fire and forget the log
            yield this.client
                .putLog({
                taskRunExternalId,
                createdAt,
                message: log,
                level: level || rpc_1.LogLevel.INFO,
                taskRetryCount,
                metadata: metadata ? JSON.stringify(metadata) : undefined,
            })
                .catch((e) => {
                this.logger.warn(`Could not put log: ${(0, hatchet_error_1.getErrorMessage)(e)}`);
            });
        });
    }
    putStream(taskRunExternalId, data, index) {
        return __awaiter(this, void 0, void 0, function* () {
            const createdAt = new Date();
            let dataBytes;
            if (typeof data === 'string') {
                dataBytes = new TextEncoder().encode(data);
            }
            else if (data instanceof Uint8Array) {
                dataBytes = data;
            }
            else {
                throw new Error('Invalid data type. Expected string or Uint8Array.');
            }
            (0, retrier_1.retrier)(() => __awaiter(this, void 0, void 0, function* () {
                return this.client.putStreamEvent({
                    taskRunExternalId,
                    createdAt,
                    message: dataBytes,
                    eventIndex: index,
                });
            }), this.logger).catch((e) => {
                this.logger.warn(`Could not put log: ${(0, hatchet_error_1.getErrorMessage)(e)}`);
            });
        });
    }
    list(opts) {
        return __awaiter(this, void 0, void 0, function* () {
            const { data } = yield this.api.v1EventList(this.tenantId, opts);
            return data;
        });
    }
}
exports.EventClient = EventClient;
