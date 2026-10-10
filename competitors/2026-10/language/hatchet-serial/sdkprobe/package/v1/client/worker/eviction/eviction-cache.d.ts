import { ActionKey } from '../../../../clients/dispatcher/action-listener';
import { EvictionPolicy } from './eviction-policy';
export type { ActionKey };
export declare enum EvictionCause {
    TTL_EXCEEDED = "ttl_exceeded",
    CAPACITY_PRESSURE = "capacity_pressure",
    WORKER_SHUTDOWN = "worker_shutdown"
}
export interface DurableRunRecord {
    key: ActionKey;
    taskRunExternalId: string;
    invocationCount: number;
    evictionPolicy: EvictionPolicy | undefined;
    registeredAt: number;
    waitingSince: number | undefined;
    waitKind: string | undefined;
    waitResourceId: string | undefined;
    _waitCount: number;
    evictionReason: string | undefined;
}
export declare class DurableEvictionCache {
    private _runs;
    registerRun(key: ActionKey, taskRunExternalId: string, invocationCount: number, now: number, evictionPolicy: EvictionPolicy | undefined): void;
    unregisterRun(key: ActionKey): void;
    get(key: ActionKey): DurableRunRecord | undefined;
    getAllWaiting(): DurableRunRecord[];
    findKeyByTaskRunExternalId(taskRunExternalId: string): ActionKey | undefined;
    markWaiting(key: ActionKey, now: number, waitKind: string, resourceId: string): void;
    markActive(key: ActionKey): void;
    selectEvictionCandidate(now: number, durableSlots: number, reserveSlots: number, minWaitForCapacityEvictionMs: number): ActionKey | undefined;
    private _hasCapacityPressure;
}
export declare function buildEvictionReason(cause: EvictionCause, rec: DurableRunRecord): string;
