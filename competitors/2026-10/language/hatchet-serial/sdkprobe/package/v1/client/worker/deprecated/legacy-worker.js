"use strict";
/**
 * Legacy dual-worker implementation for pre-slot-config engines.
 *
 * When connected to an older Hatchet engine that does not support multiple slot types,
 * this module provides the old worker start flow which creates separate durable and
 * non-durable workers, each registered with the legacy `slots` proto field.
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
exports.LegacyDualWorker = void 0;
exports.fetchEngineVersion = fetchEngineVersion;
exports.isLegacyEngine = isLegacyEngine;
const nice_grpc_1 = require("nice-grpc");
const declaration_1 = require("../../../declaration");
const legacy_v1_worker_1 = require("./legacy-v1-worker");
const deprecation_1 = require("./deprecation");
const legacy_transformer_1 = require("../../../../legacy/legacy-transformer");
const engine_version_1 = require("../engine-version");
const grpc_error_1 = require("../../../../util/grpc-error");
const DEFAULT_DEFAULT_SLOTS = 100;
const DEFAULT_DURABLE_SLOTS = 1000;
/** The date when slot_config support was released. */
const LEGACY_ENGINE_START = new Date('2026-02-12T00:00:00Z');
const LEGACY_ENGINE_MESSAGE = 'Connected to an older Hatchet engine that does not support multiple slot types. ' +
    'Falling back to legacy worker registration. ' +
    'Please upgrade your Hatchet engine to the latest version.';
/**
 * Fetches the engine version from the dispatcher.
 * Returns the semver string, or undefined if the engine is too old to support GetVersion.
 */
function fetchEngineVersion(v1) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const version = yield v1.dispatcher.getVersion();
            return version || undefined;
        }
        catch (e) {
            if ((0, grpc_error_1.getGrpcErrorCode)(e) == nice_grpc_1.Status.UNIMPLEMENTED) {
                return undefined;
            }
            throw e;
        }
    });
}
/**
 * Checks if the connected engine is legacy by comparing its semantic version
 * against the minimum required version for slot_config support.
 * Returns true if the engine is legacy, false otherwise.
 * Emits a time-aware deprecation notice when a legacy engine is detected.
 */
function isLegacyEngine(v1) {
    return __awaiter(this, void 0, void 0, function* () {
        const version = yield fetchEngineVersion(v1).catch((e) => {
            if ((0, grpc_error_1.getGrpcErrorCode)(e) === nice_grpc_1.Status.UNIMPLEMENTED) {
                return undefined;
            }
            throw e;
        });
        if (!version || (0, deprecation_1.semverLessThan)(version, engine_version_1.MinEngineVersion.SLOT_CONFIG)) {
            const logger = v1.config.logger('Worker', v1.config.log_level);
            (0, deprecation_1.emitDeprecationNotice)('legacy-engine', LEGACY_ENGINE_MESSAGE, LEGACY_ENGINE_START, logger, {
                errorDays: 180,
            });
            return true;
        }
        return false;
    });
}
/**
 * LegacyDualWorker manages two V1Worker instances (nonDurable + durable)
 * for engines that don't support slot_config.
 * Uses the legacy `slots` proto field (maxRuns) instead of `slotConfig`.
 */
class LegacyDualWorker {
    constructor(name, nonDurable, durable) {
        this.name = name;
        this.nonDurable = nonDurable;
        this.durable = durable;
    }
    /**
     * Creates a legacy dual-worker setup from the given options.
     * Workers are created with legacy registration (old `slots` proto field).
     */
    static create(v1, name, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const defaultSlots = options.slots || options.maxRuns || DEFAULT_DEFAULT_SLOTS;
            const durableSlots = options.durableSlots || DEFAULT_DURABLE_SLOTS;
            // Create the non-durable worker with legacy registration
            const nonDurable = new legacy_v1_worker_1.LegacyV1Worker(v1, { name, labels: options.labels, handleKill: options.handleKill }, defaultSlots);
            // Check if any workflows have durable tasks
            let hasDurableTasks = false;
            for (const wf of options.workflows || []) {
                if (wf instanceof declaration_1.BaseWorkflowDeclaration) {
                    if (wf.definition._durableTasks.length > 0) {
                        hasDurableTasks = true;
                        break;
                    }
                }
            }
            let durableWorker;
            if (hasDurableTasks) {
                // Create the durable worker with legacy registration
                durableWorker = new legacy_v1_worker_1.LegacyV1Worker(v1, { name: `${name}-durable`, labels: options.labels, handleKill: options.handleKill }, durableSlots);
            }
            const legacyWorker = new LegacyDualWorker(name, nonDurable, durableWorker);
            // Register workflows on appropriate workers
            for (const wf of options.workflows || []) {
                if (wf instanceof declaration_1.BaseWorkflowDeclaration) {
                    if (wf.definition._durableTasks.length > 0 && durableWorker) {
                        yield durableWorker.registerWorkflow(wf);
                        durableWorker.registerDurableActions(wf.definition);
                    }
                    else {
                        yield nonDurable.registerWorkflow(wf);
                    }
                }
                else {
                    // fallback to v0 client for backwards compatibility
                    yield nonDurable.registerWorkflow((0, legacy_transformer_1.transformLegacyWorkflow)(wf));
                }
            }
            return legacyWorker;
        });
    }
    /**
     * Starts both workers using Promise.all.
     */
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            const promises = [this.nonDurable.start()];
            if (this.durable) {
                promises.push(this.durable.start());
            }
            yield Promise.all(promises);
        });
    }
    /**
     * Stops both workers.
     */
    stop() {
        return __awaiter(this, void 0, void 0, function* () {
            const promises = [this.nonDurable.stop()];
            if (this.durable) {
                promises.push(this.durable.stop());
            }
            yield Promise.all(promises);
        });
    }
}
exports.LegacyDualWorker = LegacyDualWorker;
