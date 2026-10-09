import type { EventsServiceClient, PutStreamEventResponse } from '../../protoc/events/events';
/** Publishes stream chunks from a task run, the way `ctx.putStream` does from a worker. */
export declare class StreamsClient {
    private readonly rpc;
    constructor(rpc: EventsServiceClient);
    /**
     * Publishes one chunk on a task run's stream. `index` orders chunks that are published
     * concurrently. Resolves once the engine has the chunk.
     */
    put(taskRunExternalId: string, data: string | Uint8Array, index?: number): Promise<PutStreamEventResponse>;
}
