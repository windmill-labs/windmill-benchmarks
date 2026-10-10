import { ParentRunContextStorage } from './parent-run-context-vars';
/**
 * `AsyncLocalStorage` backed store for the parent run context, so a task's `run()`,
 * `spawnChild` and event pushes see the task they were called from across awaits.
 */
export declare function createAsyncLocalParentRunContextStorage(): ParentRunContextStorage;
/**
 * Installs the `AsyncLocalStorage` store on the shared manager. Idempotent; the worker
 * calls it at module load so it is in place before any task runs.
 */
export declare function installAsyncLocalParentRunContext(): void;
