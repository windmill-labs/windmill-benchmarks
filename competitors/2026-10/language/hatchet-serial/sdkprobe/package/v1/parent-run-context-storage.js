"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createAsyncLocalParentRunContextStorage = createAsyncLocalParentRunContextStorage;
exports.installAsyncLocalParentRunContext = installAsyncLocalParentRunContext;
const async_hooks_1 = require("async_hooks");
const parent_run_context_vars_1 = require("./parent-run-context-vars");
/**
 * `AsyncLocalStorage` backed store for the parent run context, so a task's `run()`,
 * `spawnChild` and event pushes see the task they were called from across awaits.
 */
function createAsyncLocalParentRunContextStorage() {
    const storage = new async_hooks_1.AsyncLocalStorage();
    return {
        run: (context, fn) => storage.run(context, fn),
        getStore: () => storage.getStore(),
    };
}
let installed = false;
/**
 * Installs the `AsyncLocalStorage` store on the shared manager. Idempotent; the worker
 * calls it at module load so it is in place before any task runs.
 */
function installAsyncLocalParentRunContext() {
    if (installed)
        return;
    parent_run_context_vars_1.parentRunContextManager.useStorage(createAsyncLocalParentRunContextStorage());
    installed = true;
}
