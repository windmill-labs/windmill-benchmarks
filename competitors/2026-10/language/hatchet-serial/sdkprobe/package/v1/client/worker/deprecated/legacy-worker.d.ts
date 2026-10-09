/**
 * Legacy dual-worker implementation for pre-slot-config engines.
 *
 * When connected to an older Hatchet engine that does not support multiple slot types,
 * this module provides the old worker start flow which creates separate durable and
 * non-durable workers, each registered with the legacy `slots` proto field.
 */
import { HatchetClient } from '../../..';
import { CreateWorkerOpts } from '../worker';
import { LegacyV1Worker } from './legacy-v1-worker';
/**
 * Fetches the engine version from the dispatcher.
 * Returns the semver string, or undefined if the engine is too old to support GetVersion.
 */
export declare function fetchEngineVersion(v1: HatchetClient): Promise<string | undefined>;
/**
 * Checks if the connected engine is legacy by comparing its semantic version
 * against the minimum required version for slot_config support.
 * Returns true if the engine is legacy, false otherwise.
 * Emits a time-aware deprecation notice when a legacy engine is detected.
 */
export declare function isLegacyEngine(v1: HatchetClient): Promise<boolean>;
/**
 * LegacyDualWorker manages two V1Worker instances (nonDurable + durable)
 * for engines that don't support slot_config.
 * Uses the legacy `slots` proto field (maxRuns) instead of `slotConfig`.
 */
export declare class LegacyDualWorker {
    private nonDurable;
    private durable;
    private name;
    constructor(name: string, nonDurable: LegacyV1Worker, durable?: LegacyV1Worker);
    /**
     * Creates a legacy dual-worker setup from the given options.
     * Workers are created with legacy registration (old `slots` proto field).
     */
    static create(v1: HatchetClient, name: string, options: CreateWorkerOpts): Promise<LegacyDualWorker>;
    /**
     * Starts both workers using Promise.all.
     */
    start(): Promise<void>;
    /**
     * Stops both workers.
     */
    stop(): Promise<void>;
}
