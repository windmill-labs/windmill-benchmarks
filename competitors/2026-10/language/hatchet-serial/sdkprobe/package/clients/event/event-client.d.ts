import { Channel, ClientFactory } from 'nice-grpc';
import { EventsServiceClient } from '../../protoc/events/events';
import { type Transport } from '../transport';
import { ClientConfig } from '../hatchet-client/client-config';
import { Logger } from '../../util/logger';
import { retrier } from '../../util/retrier';
import { HatchetClient } from '../../v1';
import type { EventWithMetadata, PushEventOptions } from '../../core/types';
import { LogLevel } from './rpc';
export type { EventWithMetadata, PushEventOptions };
export { LogLevel };
export declare class EventClient {
    config: ClientConfig;
    client: EventsServiceClient;
    retrier: typeof retrier;
    api: HatchetClient['api'];
    tenantId: string;
    logger: Logger;
    constructor(config: ClientConfig, _channel: Channel, _factory: ClientFactory, api: HatchetClient['api'], transport?: Transport);
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchPushEvent.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    push<T>(type: string, input: T, options?: PushEventOptions): Promise<import("../../protoc/events/events").Event>;
    /**
     * @important This method is instrumented by HatchetInstrumentor._patchBulkPushEvent.
     * Keep the signature in sync with the instrumentor wrapper.
     */
    bulkPush<T>(type: string, inputs: EventWithMetadata<T>[], options?: PushEventOptions): Promise<import("../../protoc/events/events").Events>;
    putLog(taskRunExternalId: string, log: string, level?: LogLevel, taskRetryCount?: number, metadata?: Record<string, unknown>): Promise<void>;
    putStream(taskRunExternalId: string, data: string | Uint8Array, index: number | undefined): Promise<void>;
    list(opts?: Parameters<typeof this.api.v1EventList>[1]): Promise<import("../rest/generated/data-contracts").V1EventList>;
}
