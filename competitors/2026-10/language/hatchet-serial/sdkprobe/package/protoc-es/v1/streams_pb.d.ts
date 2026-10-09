import type { GenFile, GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file v1/streams.proto.
 */
export declare const file_v1_streams: GenFile;
/**
 * @generated from message v1.PublishStreamMessageRequest
 */
export type PublishStreamMessageRequest = Message<"v1.PublishStreamMessageRequest"> & {
    /**
     * @generated from field: string namespace = 1;
     */
    namespace: string;
    /**
     * @generated from field: string topic = 2;
     */
    topic: string;
    /**
     * @generated from field: bytes payload = 3;
     */
    payload: Uint8Array;
    /**
     * @generated from field: string producer_id = 4;
     */
    producerId: string;
    /**
     * @generated from field: int64 producer_seq = 5;
     */
    producerSeq: bigint;
    /**
     * instead of payload: a ref returned by the stream payload upload endpoint,
     * for payloads too large for one gRPC message
     *
     * @generated from field: string payload_ref = 6;
     */
    payloadRef: string;
};
/**
 * Describes the message v1.PublishStreamMessageRequest.
 * Use `create(PublishStreamMessageRequestSchema)` to create a new message.
 */
export declare const PublishStreamMessageRequestSchema: GenMessage<PublishStreamMessageRequest>;
/**
 * @generated from message v1.PublishStreamMessageResponse
 */
export type PublishStreamMessageResponse = Message<"v1.PublishStreamMessageResponse"> & {};
/**
 * Describes the message v1.PublishStreamMessageResponse.
 * Use `create(PublishStreamMessageResponseSchema)` to create a new message.
 */
export declare const PublishStreamMessageResponseSchema: GenMessage<PublishStreamMessageResponse>;
/**
 * @generated from message v1.SubscribeStreamRequest
 */
export type SubscribeStreamRequest = Message<"v1.SubscribeStreamRequest"> & {
    /**
     * @generated from field: string namespace = 1;
     */
    namespace: string;
    /**
     * @generated from field: string topic = 2;
     */
    topic: string;
    /**
     * @generated from field: optional string cursor = 3;
     */
    cursor?: string | undefined;
};
/**
 * Describes the message v1.SubscribeStreamRequest.
 * Use `create(SubscribeStreamRequestSchema)` to create a new message.
 */
export declare const SubscribeStreamRequestSchema: GenMessage<SubscribeStreamRequest>;
/**
 * @generated from message v1.StreamMessage
 */
export type StreamMessage = Message<"v1.StreamMessage"> & {
    /**
     * @generated from field: repeated v1.StreamEntry entries = 1;
     */
    entries: StreamEntry[];
    /**
     * @generated from field: bool hangup = 2;
     */
    hangup: boolean;
    /**
     * @generated from field: string cursor = 3;
     */
    cursor: string;
};
/**
 * Describes the message v1.StreamMessage.
 * Use `create(StreamMessageSchema)` to create a new message.
 */
export declare const StreamMessageSchema: GenMessage<StreamMessage>;
/**
 * @generated from message v1.StreamEntry
 */
export type StreamEntry = Message<"v1.StreamEntry"> & {
    /**
     * @generated from field: bytes payload = 1;
     */
    payload: Uint8Array;
    /**
     * @generated from field: string cursor = 2;
     */
    cursor: string;
    /**
     * @generated from field: google.protobuf.Timestamp created_at = 3;
     */
    createdAt?: Timestamp | undefined;
    /**
     * instead of payload: fetch it from the stream payload endpoint by this ref
     *
     * @generated from field: string payload_ref = 4;
     */
    payloadRef: string;
};
/**
 * Describes the message v1.StreamEntry.
 * Use `create(StreamEntrySchema)` to create a new message.
 */
export declare const StreamEntrySchema: GenMessage<StreamEntry>;
/**
 * @generated from message v1.GetStreamTopicMetadataRequest
 */
export type GetStreamTopicMetadataRequest = Message<"v1.GetStreamTopicMetadataRequest"> & {
    /**
     * @generated from field: string namespace = 1;
     */
    namespace: string;
    /**
     * @generated from field: string topic = 2;
     */
    topic: string;
};
/**
 * Describes the message v1.GetStreamTopicMetadataRequest.
 * Use `create(GetStreamTopicMetadataRequestSchema)` to create a new message.
 */
export declare const GetStreamTopicMetadataRequestSchema: GenMessage<GetStreamTopicMetadataRequest>;
/**
 * Counts and the latest message cover only what's within the tenant's retention.
 *
 * @generated from message v1.StreamTopicMetadata
 */
export type StreamTopicMetadata = Message<"v1.StreamTopicMetadata"> & {
    /**
     * @generated from field: string namespace = 1;
     */
    namespace: string;
    /**
     * @generated from field: string topic = 2;
     */
    topic: string;
    /**
     * @generated from field: string tenant_id = 3;
     */
    tenantId: string;
    /**
     * resumes a subscription after the newest retained message; unset when none is retained
     *
     * @generated from field: optional string latest_cursor = 4;
     */
    latestCursor?: string | undefined;
    /**
     * @generated from field: int64 message_count = 5;
     */
    messageCount: bigint;
    /**
     * when the newest retained message was stored; unset when none is retained
     *
     * @generated from field: google.protobuf.Timestamp last_published_at = 6;
     */
    lastPublishedAt?: Timestamp | undefined;
};
/**
 * Describes the message v1.StreamTopicMetadata.
 * Use `create(StreamTopicMetadataSchema)` to create a new message.
 */
export declare const StreamTopicMetadataSchema: GenMessage<StreamTopicMetadata>;
/**
 * @generated from service v1.V1Streams
 */
export declare const V1Streams: GenService<{
    /**
     * @generated from rpc v1.V1Streams.Publish
     */
    publish: {
        methodKind: "unary";
        input: typeof PublishStreamMessageRequestSchema;
        output: typeof PublishStreamMessageResponseSchema;
    };
    /**
     * @generated from rpc v1.V1Streams.Subscribe
     */
    subscribe: {
        methodKind: "server_streaming";
        input: typeof SubscribeStreamRequestSchema;
        output: typeof StreamMessageSchema;
    };
    /**
     * @generated from rpc v1.V1Streams.GetTopicMetadata
     */
    getTopicMetadata: {
        methodKind: "unary";
        input: typeof GetStreamTopicMetadataRequestSchema;
        output: typeof StreamTopicMetadataSchema;
    };
}>;
