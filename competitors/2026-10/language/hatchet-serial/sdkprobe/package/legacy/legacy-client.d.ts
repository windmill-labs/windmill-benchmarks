import { EventClient } from '../clients/event/event-client';
import { DispatcherClient } from '../clients/dispatcher/dispatcher-client';
import { AdminClient } from '../clients/admin/admin-client';
import { ChannelCredentials } from 'nice-grpc';
import { AxiosRequestConfig } from 'axios';
import { Logger } from '../util/logger';
import { RunsClient } from '../v1';
import { ClientConfig } from '../clients/hatchet-client/client-config';
import { RunListenerClient } from '../clients/listeners/run-listener/child-listener-client';
import { Api } from '../clients/rest/generated/Api';
import { DurableListenerClient } from '../clients/listeners/durable-listener/durable-listener-client';
import { type Transport } from '../clients/transport';
export interface HatchetClientOptions {
    config_path?: string;
    /** Credentials for the streaming gRPC channel. Unary calls derive their TLS settings from `tls_config`. */
    credentials?: ChannelCredentials;
    /** The transport unary calls go over; defaults to the Node gRPC transport built from the config. */
    transport?: Transport;
}
export declare class LegacyHatchetClient {
    config: ClientConfig;
    credentials: ChannelCredentials;
    event: EventClient;
    dispatcher: DispatcherClient;
    admin: AdminClient;
    api: Api;
    runs: RunsClient | undefined;
    listener: RunListenerClient;
    tenantId: string;
    durableListener: DurableListenerClient;
    logger: Logger;
    constructor(config?: Partial<ClientConfig>, options?: HatchetClientOptions, axiosOpts?: AxiosRequestConfig, runs?: RunsClient, listener?: RunListenerClient, event?: EventClient, dispatcher?: DispatcherClient, logger?: Logger, durableListener?: DurableListenerClient);
    static init(config?: Partial<ClientConfig>, options?: HatchetClientOptions, axiosConfig?: AxiosRequestConfig): LegacyHatchetClient;
}
