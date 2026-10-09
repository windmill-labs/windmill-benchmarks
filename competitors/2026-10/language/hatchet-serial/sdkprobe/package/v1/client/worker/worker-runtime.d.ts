import type { HatchetClient } from '../client';
import type { InternalWorker } from './worker-internal';
import type { ContextRuntime } from './runtime';
/** A `ContextRuntime` that also exposes the client it wraps, for `ctx.v1`. */
export interface WorkerContextRuntime extends ContextRuntime {
    readonly client: HatchetClient;
}
export declare function createWorkerContextRuntime(client: HatchetClient, worker: InternalWorker): WorkerContextRuntime;
