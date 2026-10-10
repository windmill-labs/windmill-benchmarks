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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Heartbeat = exports.STOP_HEARTBEAT = exports.isHeartbeatMessage = void 0;
const path_1 = __importDefault(require("path"));
const thread_helper_1 = require("../../../util/thread-helper");
const v4_1 = require("zod/v4");
const HeartbeatMessageSchema = v4_1.z.object({
    type: v4_1.z.enum(['info', 'warn', 'error', 'debug']),
    message: v4_1.z.string(),
});
const isHeartbeatMessage = (message) => HeartbeatMessageSchema.safeParse(message).success;
exports.isHeartbeatMessage = isHeartbeatMessage;
exports.STOP_HEARTBEAT = 'stop';
class Heartbeat {
    constructor(client, workerId) {
        this.config = client.config;
        this.client = client.client;
        this.workerId = workerId;
        this.logger = client.config.logger(`HeartbeatController`, this.config.log_level);
    }
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.heartbeatWorker) {
                const _a = this.config, { middleware: _m, logger: _l } = _a, clonableConfig = __rest(_a, ["middleware", "logger"]);
                this.heartbeatWorker = (0, thread_helper_1.runThreaded)(path_1.default.join(__dirname, './heartbeat-worker'), {
                    workerData: {
                        config: clonableConfig,
                        workerId: this.workerId,
                    },
                });
                this.heartbeatWorker.on('message', (message) => {
                    if (!(0, exports.isHeartbeatMessage)(message)) {
                        return;
                    }
                    this.logger[message.type](message.message);
                });
            }
        });
    }
    stop() {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            (_a = this.heartbeatWorker) === null || _a === void 0 ? void 0 : _a.postMessage(exports.STOP_HEARTBEAT);
            (_b = this.heartbeatWorker) === null || _b === void 0 ? void 0 : _b.terminate();
            this.heartbeatWorker = undefined;
        });
    }
}
exports.Heartbeat = Heartbeat;
