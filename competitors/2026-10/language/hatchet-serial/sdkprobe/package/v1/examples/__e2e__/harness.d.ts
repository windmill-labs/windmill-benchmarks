import { HatchetClient } from '../..';
import type { BaseWorkflowDeclaration } from '../..';
import { Worker } from '../../client/worker/worker';
export declare function requireEnv(name: string): string;
export declare function makeE2EClient(): HatchetClient;
export declare function makeTestScope(prefix?: string): string;
export declare function startWorker({ client, name, workflows, slots, }: {
    client: HatchetClient;
    name: string;
    workflows: Array<BaseWorkflowDeclaration<any, any>>;
    slots?: number;
}): Promise<Worker>;
export declare function stopWorker(worker: Worker | undefined): Promise<void>;
/**
 * Checks whether the connected engine supports durable eviction.
 * Call from beforeAll / beforeEach and skip tests when false.
 */
export declare function checkDurableEvictionSupport(client: HatchetClient): Promise<boolean>;
export declare function poll<T>(fn: () => Promise<T>, { timeoutMs, intervalMs, shouldStop, label, }: {
    timeoutMs?: number;
    intervalMs?: number;
    shouldStop: (value: T) => boolean;
    label?: string;
}): Promise<T>;
