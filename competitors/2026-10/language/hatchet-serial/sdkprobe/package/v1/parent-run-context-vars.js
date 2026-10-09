"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parentRunContextManager = exports.ParentRunContextManager = void 0;
const noContextStorage = {
    run: (_context, fn) => fn(),
    getStore: () => undefined,
};
class ParentRunContextManager {
    constructor(storage = noContextStorage) {
        this.storage = storage;
    }
    /**
     * Replaces the store used for subsequent `runWithContext` and `getContext` calls.
     */
    useStorage(storage) {
        this.storage = storage;
    }
    runWithContext(opts, fn) {
        return this.storage.run(Object.assign({}, opts), fn);
    }
    incrementChildIndex(n) {
        var _a;
        const parentRunContext = this.getContext();
        if (parentRunContext) {
            // Mutate in place, do NOT use enterWith here.
            // storage.run() gives every async descendant the same object reference,
            // so direct mutation is visible across all await boundaries within the
            // same task execution.  enterWith would replace the object, and the new
            // object's updates would be invisible to parent async contexts after an await.
            parentRunContext.childIndex = ((_a = parentRunContext.childIndex) !== null && _a !== void 0 ? _a : 0) + n;
        }
    }
    getContext() {
        return this.storage.getStore();
    }
}
exports.ParentRunContextManager = ParentRunContextManager;
// Export a default instance for backward compatibility and convenience
exports.parentRunContextManager = new ParentRunContextManager();
