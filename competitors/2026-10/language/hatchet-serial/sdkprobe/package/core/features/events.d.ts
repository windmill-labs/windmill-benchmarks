import { type RetrierConfig } from '../../util/retrier';
import type { Logger } from '../../util/logger/logger';
import type { Event, Events, EventsServiceClient } from '../../protoc/events/events';
import type { EventWithMetadata, PushEventOptions } from '../types';
export interface EventsClientDeps {
    rpc: EventsServiceClient;
    logger: Logger;
    namespace?: string;
    retrier?: RetrierConfig;
}
/** Pushes events that trigger event-bound workflows. */
export declare class EventsClient {
    private readonly deps;
    constructor(deps: EventsClientDeps);
    /**
     * Pushes one event. The key is namespaced the way the Node client namespaces it.
     * @returns the stored event
     */
    push<T>(type: string, input: T, options?: PushEventOptions): Promise<Event>;
    /**
     * Pushes many events of one type in a single call.
     * @returns the stored events
     */
    bulkPush<T>(type: string, inputs: EventWithMetadata<T>[], options?: PushEventOptions): Promise<Events>;
}
