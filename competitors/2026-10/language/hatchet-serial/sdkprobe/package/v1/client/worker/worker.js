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
exports.__testing = exports.Worker = void 0;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const declaration_1 = require("../../declaration");
const legacy_transformer_1 = require("../../../legacy/legacy-transformer");
const worker_internal_1 = require("./worker-internal");
const slot_utils_1 = require("./slot-utils");
const deprecated_1 = require("./deprecated");
const engine_version_1 = require("./engine-version");
/**
 * HatchetWorker class for workflow execution runtime
 */
class Worker {
    /**
     * Creates a new HatchetWorker instance
     * @param nonDurable - The V0 worker implementation
     */
    constructor(v1, nonDurable, config, name) {
        /** Tracks all workflows registered after construction (via registerWorkflow/registerWorkflows) */
        this._registeredWorkflows = [];
        this._v1 = v1;
        this._internal = nonDurable;
        this.config = config;
        this.name = name;
    }
    /**
     * Creates and initializes a new HatchetWorker
     * @param v0 - The HatchetClient instance
     * @param options - Worker creation options
     * @returns A new HatchetWorker instance
     */
    static create(v1, name, options) {
        return __awaiter(this, void 0, void 0, function* () {
            // Normalize any legacy workflows before resolving worker options
            const normalizedOptions = Object.assign(Object.assign({}, options), { workflows: options.workflows ? (0, legacy_transformer_1.normalizeWorkflows)(options.workflows) : undefined });
            const resolvedOptions = (0, slot_utils_1.resolveWorkerOptions)(normalizedOptions);
            const opts = Object.assign({ name }, resolvedOptions);
            const internalWorker = new worker_internal_1.InternalWorker(v1, opts);
            const worker = new Worker(v1, internalWorker, normalizedOptions, name);
            yield worker.registerWorkflows(normalizedOptions.workflows);
            return worker;
        });
    }
    /**
     * Registers workflows with the worker.
     * Accepts both v1 BaseWorkflowDeclaration and legacy Workflow objects.
     * Legacy workflows are automatically transformed and a deprecation warning is emitted.
     * @param workflows - Array of workflows to register
     * @returns Array of registered workflow promises
     */
    registerWorkflows(workflows) {
        return __awaiter(this, void 0, void 0, function* () {
            const normalized = workflows ? (0, legacy_transformer_1.normalizeWorkflows)(workflows) : [];
            for (const wf of normalized) {
                yield this._internal.registerWorkflow(wf);
                if (wf.definition._durableTasks.length > 0) {
                    this._internal.registerDurableActions(wf.definition);
                }
            }
        });
    }
    /**
     * Registers a single workflow with the worker.
     * Accepts both v1 BaseWorkflowDeclaration and legacy Workflow objects.
     * Legacy workflows are automatically transformed and a deprecation warning is emitted.
     * @param workflow - The workflow to register
     * @returns A promise that resolves when the workflow is registered
     * @deprecated use registerWorkflows instead
     */
    registerWorkflow(workflow) {
        return this.registerWorkflows([workflow]);
    }
    /**
     * Starts the worker
     * @returns Promise that resolves when the worker is stopped or killed
     */
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            // Check engine version and fall back to legacy dual-worker mode if needed
            if (yield (0, deprecated_1.isLegacyEngine)(this._v1)) {
                // Include workflows registered after construction (via registerWorkflow/registerWorkflows)
                // so the legacy worker picks them up.
                const legacyConfig = Object.assign(Object.assign({}, this.config), { workflows: this._registeredWorkflows.length
                        ? this._registeredWorkflows
                        : this.config.workflows });
                this._legacyWorker = yield deprecated_1.LegacyDualWorker.create(this._v1, this.name, legacyConfig);
                return this._legacyWorker.start();
            }
            const engineVersion = yield (0, deprecated_1.fetchEngineVersion)(this._v1).catch(() => undefined);
            this._checkEvictionSupport(engineVersion);
            this._internal.engineVersion = engineVersion;
            return this._internal.start();
        });
    }
    _checkEvictionSupport(engineVersion) {
        if ((0, engine_version_1.supportsEviction)(engineVersion))
            return;
        const workflows = (this.config.workflows || []);
        const tasksWithEviction = [];
        for (const wf of workflows) {
            if (!(wf instanceof declaration_1.BaseWorkflowDeclaration))
                continue;
            for (const task of wf.definition._durableTasks) {
                if (task.evictionPolicy) {
                    tasksWithEviction.push(`${wf.definition.name}:${task.name}`);
                }
            }
        }
        if (tasksWithEviction.length === 0)
            return;
        const names = tasksWithEviction.join(', ');
        const logger = this._v1.config.logger('Worker', this._v1.config.log_level);
        (0, deprecated_1.emitDeprecationNotice)('pre-eviction-engine', `Engine ${engineVersion || 'unknown'} does not support durable eviction ` +
            `(requires >= ${engine_version_1.MinEngineVersion.DURABLE_EVICTION}). ` +
            `Eviction policies will be ignored for tasks: ${names}. ` +
            `Please upgrade your Hatchet engine.`, new Date('2026-03-01T00:00:00Z'), logger);
    }
    /**
     * Stops the worker
     * @returns Promise that resolves when the worker stops
     */
    stop() {
        if (this._legacyWorker) {
            return this._legacyWorker.stop();
        }
        return this._internal.stop();
    }
    /**
     * Updates or inserts worker labels
     * @param labels - Worker labels to update
     * @returns Promise that resolves when labels are updated
     */
    upsertLabels(labels) {
        return this._internal.upsertLabels(labels);
    }
    /**
     * Get the labels for the worker
     * @returns The labels for the worker
     */
    getLabels() {
        return this._internal.labels;
    }
    isPaused() {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            if (!((_a = this._internal) === null || _a === void 0 ? void 0 : _a.workerId)) {
                return false;
            }
            return this._v1.workers.isPaused(this._internal.workerId);
        });
    }
    // TODO docstrings
    pause() {
        var _a;
        if (!((_a = this._internal) === null || _a === void 0 ? void 0 : _a.workerId)) {
            return Promise.resolve();
        }
        return this._v1.workers.pause(this._internal.workerId);
    }
    unpause() {
        var _a;
        if (!((_a = this._internal) === null || _a === void 0 ? void 0 : _a.workerId)) {
            return Promise.resolve();
        }
        return this._v1.workers.unpause(this._internal.workerId);
    }
    /**
     * Waits until the worker has connected and registered with the server.
     * Polls every 200ms. Use after start() to avoid fixed sleeps before running workflows.
     */
    waitUntilReady() {
        return __awaiter(this, arguments, void 0, function* (timeoutMs = 10000) {
            var _a;
            if (this._legacyWorker) {
                yield (0, sleep_1.default)(2000);
                return;
            }
            const pollInterval = 200;
            const start = Date.now();
            while (Date.now() - start < timeoutMs) {
                // start() may asynchronously detect a legacy engine and set _legacyWorker
                // after waitUntilReady has already entered this loop
                if (this._legacyWorker) {
                    yield (0, sleep_1.default)(2000);
                    return;
                }
                if ((_a = this._internal) === null || _a === void 0 ? void 0 : _a.workerId)
                    return;
                yield (0, sleep_1.default)(pollInterval);
            }
            throw new Error(`Worker ${this.name} did not become ready within ${timeoutMs}ms`);
        });
    }
}
exports.Worker = Worker;
var slot_utils_2 = require("./slot-utils");
Object.defineProperty(exports, "__testing", { enumerable: true, get: function () { return slot_utils_2.testingExports; } });
