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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LogsClient = exports.MAX_LOG_LINE_CHARS = void 0;
const rpc_1 = require("../../clients/event/rpc");
const hatchet_error_1 = __importDefault(require("../../util/errors/hatchet-error"));
/** The longest log line accepted, in characters; the Node client's `putLog` holds the same limit. */
exports.MAX_LOG_LINE_CHARS = 1000;
/** Writes log lines against a task run, the way `ctx.log` does from a worker. */
class LogsClient {
    constructor(rpc) {
        this.rpc = rpc;
    }
    /**
     * Stores one log line for a task run. Unlike the Node client's fire-and-forget `putLog`,
     * this resolves once the engine has the line and rejects when it does not, since a
     * serverless runtime may end the invocation as soon as the handler returns.
     *
     * A line over `MAX_LOG_LINE_CHARS` (1,000) characters rejects with a `HatchetError` that
     * states the length, where the Node client warns and drops the line; the message itself is
     * never part of the error.
     */
    put(taskRunExternalId_1, message_1) {
        return __awaiter(this, arguments, void 0, function* (taskRunExternalId, message, options = {}) {
            var _a;
            if (message.length > exports.MAX_LOG_LINE_CHARS) {
                throw new hatchet_error_1.default(`log line is ${message.length} characters, over the ${exports.MAX_LOG_LINE_CHARS}-character limit`);
            }
            return this.rpc.putLog({
                taskRunExternalId,
                createdAt: new Date(),
                message,
                level: (_a = options.level) !== null && _a !== void 0 ? _a : rpc_1.LogLevel.INFO,
                taskRetryCount: options.retryCount,
                metadata: options.metadata ? JSON.stringify(options.metadata) : undefined,
            });
        });
    }
}
exports.LogsClient = LogsClient;
