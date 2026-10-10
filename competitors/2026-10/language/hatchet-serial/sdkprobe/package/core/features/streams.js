"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.StreamsClient = void 0;
/** Publishes stream chunks from a task run, the way `ctx.putStream` does from a worker. */
class StreamsClient {
    constructor(rpc) {
        this.rpc = rpc;
    }
    /**
     * Publishes one chunk on a task run's stream. `index` orders chunks that are published
     * concurrently. Resolves once the engine has the chunk.
     */
    put(taskRunExternalId, data, index) {
        const message = typeof data === 'string' ? new TextEncoder().encode(data) : data;
        return this.rpc.putStreamEvent({
            taskRunExternalId,
            createdAt: new Date(),
            message,
            eventIndex: index,
        });
    }
}
exports.StreamsClient = StreamsClient;
