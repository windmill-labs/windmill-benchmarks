import type { DurableContext } from './client/worker/context';
export interface ParentRunContext {
    parentId: string;
    /**
     * External ID of the parent task/step run.
     */
    parentTaskRunExternalId: string;
    desiredWorkerId: string;
    childIndex?: number;
    /**
     * (optional) AbortSignal inherited by nested `run()` calls.
     * Used to cancel local "wait for result" subscriptions when the parent task is cancelled.
     */
    signal?: AbortSignal;
    /**
     * Present when the current task is running in durable mode.
     * Used by child `run()` calls to route through `spawnChild` instead of a fresh trigger.
     */
    durableContext?: DurableContext<unknown, unknown>;
}
/**
 * Where the manager keeps the context of the task currently executing. Node workers
 * install an `AsyncLocalStorage` backed store (see `parent-run-context-storage.ts`);
 * runtimes without async context tracking keep the default store, which never has a
 * context.
 */
export interface ParentRunContextStorage {
    run<T>(context: ParentRunContext, fn: () => T): T;
    getStore(): ParentRunContext | undefined;
}
export declare class ParentRunContextManager {
    private storage;
    constructor(storage?: ParentRunContextStorage);
    /**
     * Replaces the store used for subsequent `runWithContext` and `getContext` calls.
     */
    useStorage(storage: ParentRunContextStorage): void;
    runWithContext<T>(opts: ParentRunContext, fn: () => T): T;
    incrementChildIndex(n: number): void;
    getContext(): ParentRunContext | undefined;
}
export declare const parentRunContextManager: ParentRunContextManager;
