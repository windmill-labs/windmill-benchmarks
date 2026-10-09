import { ClientConfig } from '../clients/hatchet-client';
import { ChannelCredentials, CompatServiceDefinition } from 'nice-grpc';
import { ClientMiddlewareCall, CallOptions } from 'nice-grpc-common';
export declare const channelFactory: (config: ClientConfig, credentials: ChannelCredentials) => import("nice-grpc").Channel;
export declare const addTokenMiddleware: (token: string) => <Request, Response>(call: ClientMiddlewareCall<Request, Response>, options: CallOptions) => AsyncGenerator<Awaited<Response>, Awaited<Response> | undefined, undefined>;
export declare const createGrpcClient: <T extends CompatServiceDefinition>(config: ClientConfig, serviceDefinition: T) => {
    factory: import("nice-grpc").ClientFactory<{}>;
    channel: import("nice-grpc").Channel;
    client: import("nice-grpc").RawClient<import("nice-grpc").NormalizedServiceDefinition<T>, {}>;
};
