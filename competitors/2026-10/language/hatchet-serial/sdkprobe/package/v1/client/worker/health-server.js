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
exports.HealthServer = exports.workerStatus = void 0;
const node_http_1 = require("node:http");
exports.workerStatus = {
    INITIALIZED: 'INITIALIZED',
    STARTING: 'STARTING',
    HEALTHY: 'HEALTHY',
    UNHEALTHY: 'UNHEALTHY',
};
class HealthServer {
    constructor(port, getStatus, workerName, getSlots, getActions, getLabels, logger) {
        this.port = port;
        this.getStatus = getStatus;
        this.workerName = workerName;
        this.getSlots = getSlots;
        this.getActions = getActions;
        this.getLabels = getLabels;
        this.logger = logger;
        this.server = null;
        this.register = null;
        this.workerStatusGauge = null;
        this.workerSlotsGauge = null;
        this.workerActionsGauge = null;
        this.metricsInitialized = false;
        this.initializeMetrics();
    }
    handleRequest(req, res) {
        return __awaiter(this, void 0, void 0, function* () {
            const url = req.url || '';
            if (url === '/health' && req.method === 'GET') {
                yield this.handleHealth(res);
            }
            else if (url === '/readyz' && req.method === 'GET') {
                this.handleReadyz(res);
            }
            else if (url === '/livez' && req.method === 'GET') {
                this.handleLivez(res);
            }
            else if (url === '/metrics' && req.method === 'GET') {
                yield this.handleMetrics(res);
            }
            else {
                res.writeHead(404, { 'Content-Type': 'text/plain' });
                res.end('Not Found');
            }
        });
    }
    handleHealth(res) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = {
                status: this.getStatus(),
                name: this.workerName,
                slots: this.getSlots(),
                actions: this.getActions(),
                labels: this.getLabels(),
                nodeVersion: process.version,
            };
            // Always return 200 for compatibility with consumers that inspect the JSON
            // status. Use /readyz when the HTTP status must indicate readiness.
            res.writeHead(200, { 'Content-Type': 'application/json' });
            yield res.end(JSON.stringify(response));
        });
    }
    // Kubernetes-style readiness probe: 200 only when the worker is HEALTHY
    // (registered and holding an action listener), 503 otherwise. Unlike
    // /health, this is a plain status-code contract a vanilla `httpGet` probe
    // can consume directly, with no body to parse.
    handleReadyz(res) {
        const ready = this.getStatus() === exports.workerStatus.HEALTHY;
        res.writeHead(ready ? 200 : 503, { 'Content-Type': 'text/plain' });
        res.end(ready ? 'ok' : 'not ready');
    }
    // Kubernetes-style liveness probe: 200 whenever this handler can run at
    // all, deliberately independent of Hatchet connectivity. A worker that has
    // temporarily lost its action listener (UNHEALTHY) should be pulled from
    // rotation via /readyz, not killed and restarted via /livez — restarting
    // does nothing to fix an upstream Hatchet outage and drops in-flight work.
    // /livez only needs to fail when the process itself is deadlocked/wedged,
    // which this HTTP response reaching the caller already disproves.
    handleLivez(res) {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('ok');
    }
    initializeMetrics() {
        try {
            // THIS IS AN OPTIONAL DEPENDENCY
            // eslint-disable-next-line @typescript-eslint/no-require-imports
            const { Registry, Gauge, collectDefaultMetrics } = require('prom-client');
            this.register = new Registry();
            collectDefaultMetrics({ register: this.register });
            this.workerStatusGauge = new Gauge({
                name: 'hatchet_worker_status',
                help: 'Current status of the Hatchet worker',
                registers: [this.register],
                collect: () => {
                    this.workerStatusGauge.set(this.getStatus() === exports.workerStatus.HEALTHY ? 1 : 0);
                },
            });
            this.workerSlotsGauge = new Gauge({
                name: 'hatchet_worker_slots',
                help: 'Total slots available on the worker',
                registers: [this.register],
                collect: () => {
                    this.workerSlotsGauge.set(this.getSlots());
                },
            });
            this.workerActionsGauge = new Gauge({
                name: 'hatchet_worker_actions',
                help: 'Number of registered actions on the worker',
                registers: [this.register],
                collect: () => {
                    this.workerActionsGauge.set(this.getActions().length);
                },
            });
            this.metricsInitialized = true;
        }
        catch (_a) {
            this.metricsInitialized = false;
            this.logger.error('Metrics initialization failed - prom-client dependency not installed');
        }
    }
    handleMetrics(res) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.metricsInitialized || !this.register) {
                this.logger.error('Metrics initialization failed - prom-client dependency not installed');
                res.writeHead(503, { 'Content-Type': 'text/plain' });
                res.end('Metrics initialization failed');
                return;
            }
            try {
                const metrics = yield this.register.metrics();
                res.writeHead(200, { 'Content-Type': this.register.contentType });
                res.end(metrics);
            }
            catch (error) {
                this.logger.error(`Error generating metrics: ${error}`);
                res.writeHead(500, { 'Content-Type': 'text/plain' });
                res.end('Error generating metrics');
            }
        });
    }
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                try {
                    this.server = (0, node_http_1.createServer)((req, res) => {
                        this.handleRequest(req, res);
                    });
                    this.server.listen(this.port, '0.0.0.0', () => {
                        this.logger.info(`Health check server running on port ${this.port}`);
                        resolve();
                    });
                    this.server.on('error', (error) => {
                        this.logger.error(`Failed to start health check server: ${error.message}`);
                        reject(error);
                    });
                }
                catch (error) {
                    this.logger.error(`Failed to start health check server: ${error}`);
                    reject(error);
                }
            });
        });
    }
    stop() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.server) {
                return new Promise((resolve) => {
                    this.server.close(() => {
                        this.logger.info('Health check server stopped!');
                        resolve();
                    });
                });
            }
            return Promise.resolve();
        });
    }
}
exports.HealthServer = HealthServer;
