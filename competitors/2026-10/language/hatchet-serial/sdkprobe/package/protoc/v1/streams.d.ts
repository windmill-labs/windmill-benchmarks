import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import type { CallContext, CallOptions } from 'nice-grpc-common';
export declare const protobufPackage = "v1";
export interface PublishStreamMessageRequest {
    namespace: string;
    topic: string;
    payload: Uint8Array;
    producerId: string;
    producerSeq: number;
    /**
     * instead of payload: a ref returned by the stream payload upload endpoint,
     * for payloads too large for one gRPC message
     */
    payloadRef: string;
}
export interface PublishStreamMessageResponse {
}
export interface SubscribeStreamRequest {
    namespace: string;
    topic: string;
    cursor?: string | undefined;
}
export interface StreamMessage {
    entries: StreamEntry[];
    hangup: boolean;
    cursor: string;
}
export interface StreamEntry {
    payload: Uint8Array;
    cursor: string;
    createdAt: Date | undefined;
    /** instead of payload: fetch it from the stream payload endpoint by this ref */
    payloadRef: string;
}
export interface GetStreamTopicMetadataRequest {
    namespace: string;
    topic: string;
}
/** Counts and the latest message cover only what's within the tenant's retention. */
export interface StreamTopicMetadata {
    namespace: string;
    topic: string;
    tenantId: string;
    /** resumes a subscription after the newest retained message; unset when none is retained */
    latestCursor?: string | undefined;
    messageCount: number;
    /** when the newest retained message was stored; unset when none is retained */
    lastPublishedAt: Date | undefined;
}
export declare const PublishStreamMessageRequest: MessageFns<PublishStreamMessageRequest>;
export declare const PublishStreamMessageResponse: MessageFns<PublishStreamMessageResponse>;
export declare const SubscribeStreamRequest: MessageFns<SubscribeStreamRequest>;
export declare const StreamMessage: MessageFns<StreamMessage>;
export declare const StreamEntry: MessageFns<StreamEntry>;
export declare const GetStreamTopicMetadataRequest: MessageFns<GetStreamTopicMetadataRequest>;
export declare const StreamTopicMetadata: MessageFns<StreamTopicMetadata>;
export type V1StreamsDefinition = typeof V1StreamsDefinition;
export declare const V1StreamsDefinition: {
    readonly name: "V1Streams";
    readonly fullName: "v1.V1Streams";
    readonly methods: {
        readonly publish: {
            readonly name: "Publish";
            readonly requestType: typeof PublishStreamMessageRequest;
            readonly requestStream: false;
            readonly responseType: typeof PublishStreamMessageResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly subscribe: {
            readonly name: "Subscribe";
            readonly requestType: typeof SubscribeStreamRequest;
            readonly requestStream: false;
            readonly responseType: typeof StreamMessage;
            readonly responseStream: true;
            readonly options: {};
        };
        readonly getTopicMetadata: {
            readonly name: "GetTopicMetadata";
            readonly requestType: typeof GetStreamTopicMetadataRequest;
            readonly requestStream: false;
            readonly responseType: typeof StreamTopicMetadata;
            readonly responseStream: false;
            readonly options: {};
        };
    };
};
export interface V1StreamsServiceImplementation<CallContextExt = {}> {
    publish(request: PublishStreamMessageRequest, context: CallContext & CallContextExt): Promise<DeepPartial<PublishStreamMessageResponse>>;
    subscribe(request: SubscribeStreamRequest, context: CallContext & CallContextExt): ServerStreamingMethodResult<DeepPartial<StreamMessage>>;
    getTopicMetadata(request: GetStreamTopicMetadataRequest, context: CallContext & CallContextExt): Promise<DeepPartial<StreamTopicMetadata>>;
}
export interface V1StreamsClient<CallOptionsExt = {}> {
    publish(request: DeepPartial<PublishStreamMessageRequest>, options?: CallOptions & CallOptionsExt): Promise<PublishStreamMessageResponse>;
    subscribe(request: DeepPartial<SubscribeStreamRequest>, options?: CallOptions & CallOptionsExt): AsyncIterable<StreamMessage>;
    getTopicMetadata(request: DeepPartial<GetStreamTopicMetadataRequest>, options?: CallOptions & CallOptionsExt): Promise<StreamTopicMetadata>;
}
type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;
export type DeepPartial<T> = T extends Builtin ? T : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {} ? {
    [K in keyof T]?: DeepPartial<T[K]>;
} : Partial<T>;
export type ServerStreamingMethodResult<Response> = {
    [Symbol.asyncIterator](): AsyncIterator<Response, void>;
};
export interface MessageFns<T> {
    encode(message: T, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): T;
    fromJSON(object: any): T;
    toJSON(message: T): unknown;
    create(base?: DeepPartial<T>): T;
    fromPartial(object: DeepPartial<T>): T;
}
export {};
