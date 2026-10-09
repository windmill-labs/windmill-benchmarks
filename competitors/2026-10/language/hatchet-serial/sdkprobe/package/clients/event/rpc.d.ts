import { BulkPushEventRequest, EventsServiceClient, PushEventRequest } from '../../protoc/events/events';
import type { Transport } from '../transport/transport';
import type { EventWithMetadata, PushEventOptions } from '../../core/types';
/** The levels a task run log line is stored with. */
export declare enum LogLevel {
    INFO = "INFO",
    WARN = "WARN",
    ERROR = "ERROR",
    DEBUG = "DEBUG"
}
/**
 * The `EventsService` client, every RPC of the generated `EventsServiceClient` interface,
 * served by a Connect client on the given transport. The Node and core clients both call
 * through it.
 */
export declare function createEventsRpc(transport: Transport): EventsServiceClient;
/**
 * One builder for both clients, so an event pushed from the core client is indistinguishable
 * to the engine from one pushed by the Node client.
 */
export declare function buildPushEventRequest<T>(type: string, input: T, options: PushEventOptions, namespace: string | undefined): PushEventRequest;
/**
 * Builds the `BulkPush` request for events of one type. Each event's own metadata, priority
 * and scope win over the shared options.
 */
export declare function buildBulkPushEventRequest<T>(type: string, inputs: EventWithMetadata<T>[], options: PushEventOptions, namespace: string | undefined): BulkPushEventRequest;
