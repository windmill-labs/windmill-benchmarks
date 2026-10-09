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
const worker_threads_1 = require("worker_threads");
const hatchet_client_1 = require("../../hatchet-client");
const config_loader_1 = require("../../../util/config-loader");
const nice_grpc_1 = require("nice-grpc");
const hatchet_error_1 = require("../../../util/errors/hatchet-error");
const grpc_error_1 = require("../../../util/grpc-error");
const grpc_helpers_1 = require("../../../util/grpc-helpers");
const dispatcher_client_1 = require("../dispatcher-client");
const heartbeat_controller_1 = require("./heartbeat-controller");
const heartbeat_severity_1 = require("./heartbeat-severity");
const HEARTBEAT_INTERVAL = 4000;
// Fail fast on a stalled connection: without a deadline, a heartbeat RPC on a dead
// TCP connection can hang for 50+ seconds, well past the engine's 30s reassignment
// window, while setInterval stacks overlapping in-flight requests behind it.
const HEARTBEAT_TIMEOUT = 5000;
const postMessage = (message) => {
    worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage(message);
};
class HeartbeatWorker {
    constructor(config, workerId) {
        this.timeLastHeartbeat = new Date().getTime();
        this.missedHeartbeats = 0;
        this.workerId = workerId;
        this.logger = new hatchet_client_1.HatchetLogger(`HeartbeatThread`, config.log_level);
        this.logger.debug('Heartbeat thread starting...');
        const credentials = config_loader_1.ConfigLoader.createCredentials(config.tls_config);
        const clientFactory = (0, nice_grpc_1.createClientFactory)().use((0, grpc_helpers_1.addTokenMiddleware)(config.token));
        const dispatcher = new dispatcher_client_1.DispatcherClient(Object.assign(Object.assign({}, config), { logger: (ctx, level) => new hatchet_client_1.HatchetLogger(ctx, level) }), (0, grpc_helpers_1.channelFactory)(config, credentials), clientFactory);
        this.client = dispatcher.client;
        postMessage({
            type: 'debug',
            message: 'Heartbeat thread started.',
        });
    }
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.heartbeatInterval) {
                return;
            }
            const beat = () => __awaiter(this, void 0, void 0, function* () {
                try {
                    this.logger.debug('Heartbeat sending...');
                    postMessage({
                        type: 'debug',
                        message: 'Heartbeat sending...',
                    });
                    yield this.client.heartbeat({
                        workerId: this.workerId,
                        heartbeatAt: new Date(),
                    }, { signal: AbortSignal.timeout(HEARTBEAT_TIMEOUT) });
                    const now = new Date().getTime();
                    const actualInterval = now - this.timeLastHeartbeat;
                    if (actualInterval > HEARTBEAT_INTERVAL * 1.2) {
                        const message = `Heartbeat interval delay (${actualInterval}ms >> ${HEARTBEAT_INTERVAL}ms)`;
                        this.logger.debug(message);
                        postMessage({
                            type: 'warn',
                            message,
                        });
                    }
                    this.logger.debug(`Heartbeat sent ${actualInterval}ms ago`);
                    postMessage({
                        type: 'debug',
                        message: `Heartbeat sent ${actualInterval}ms ago`,
                    });
                    this.timeLastHeartbeat = now;
                    this.missedHeartbeats = 0;
                }
                catch (e) {
                    if ((0, grpc_error_1.getGrpcErrorCode)(e) === nice_grpc_1.Status.UNIMPLEMENTED) {
                        // break out of interval
                        const message = 'Heartbeat not implemented, closing heartbeat';
                        this.logger.debug(message);
                        postMessage({
                            type: 'error',
                            message,
                        });
                        this.stop();
                        return;
                    }
                    this.missedHeartbeats += 1;
                    const message = `Failed to send heartbeat: ${(0, hatchet_error_1.getErrorMessage)(e)}`;
                    this.logger.debug(message);
                    const severity = (0, heartbeat_severity_1.classifyHeartbeatFailure)(e, this.missedHeartbeats);
                    if (severity !== 'silent') {
                        postMessage({
                            type: severity,
                            message,
                        });
                    }
                }
            });
            // start with a heartbeat
            yield beat();
            this.heartbeatInterval = setInterval(beat, HEARTBEAT_INTERVAL);
        });
    }
    stop() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = undefined;
        }
    }
}
const heartbeat = new HeartbeatWorker(worker_threads_1.workerData.config, worker_threads_1.workerData.workerId);
heartbeat.start();
worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.on('message', (msg) => {
    if (msg === heartbeat_controller_1.STOP_HEARTBEAT) {
        heartbeat.stop();
    }
});
