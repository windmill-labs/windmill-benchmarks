import { Logger } from '../../../../util/logger';
import { EvictionPolicy } from './eviction-policy';
import { ActionKey, DurableEvictionCache, DurableRunRecord } from './eviction-cache';
export interface DurableEvictionConfig {
    /** How often we try selecting an eviction candidate. Default: 1000ms */
    checkIntervalMs?: number;
    /** How many slots to reserve from capacity-based eviction decisions. Default: 0 */
    reserveSlots?: number;
    /** Avoid immediately evicting runs that just entered a wait. Default: 10000ms */
    minWaitForCapacityEvictionMs?: number;
}
export declare const DEFAULT_DURABLE_EVICTION_CONFIG: Required<DurableEvictionConfig>;
export declare class DurableEvictionManager {
    private _durableSlots;
    private _cancelLocal;
    private _requestEvictionWithAck;
    private _config;
    private _cache;
    private _logger;
    private _timer;
    private _ticking;
    constructor(opts: {
        durableSlots: number;
        cancelLocal: (key: ActionKey, invocationCount: number) => void;
        requestEvictionWithAck: (key: ActionKey, rec: DurableRunRecord) => Promise<void>;
        config?: DurableEvictionConfig;
        cache?: DurableEvictionCache;
        logger: Logger;
    });
    get cache(): DurableEvictionCache;
    start(): void;
    stop(): void;
    registerRun(key: ActionKey, taskRunExternalId: string, invocationCount: number, evictionPolicy: EvictionPolicy | undefined): void;
    unregisterRun(key: ActionKey): void;
    markWaiting(key: ActionKey, waitKind: string, resourceId: string): void;
    markActive(key: ActionKey): void;
    private _evictRun;
    private _tickSafe;
    private _tick;
    handleServerEviction(taskRunExternalId: string, invocationCount: number): void;
    evictAllWaiting(): Promise<number>;
}
