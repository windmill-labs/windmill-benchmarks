import type { GenFile, GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file events/events.proto.
 */
export declare const file_events_events: GenFile;
/**
 * @generated from message Event
 */
export type Event = Message<"Event"> & {
    /**
     * the tenant id
     *
     * @generated from field: string tenant_id = 1;
     */
    tenantId: string;
    /**
     * the id of the event
     *
     * @generated from field: string event_id = 2;
     */
    eventId: string;
    /**
     * the key for the event
     *
     * @generated from field: string key = 3;
     */
    key: string;
    /**
     * the payload for the event
     *
     * @generated from field: string payload = 4;
     */
    payload: string;
    /**
     * when the event was generated
     *
     * @generated from field: google.protobuf.Timestamp event_timestamp = 5;
     */
    eventTimestamp?: Timestamp | undefined;
    /**
     * the additional metadata for the event
     *
     * @generated from field: optional string additional_metadata = 6;
     */
    additionalMetadata?: string | undefined;
    /**
     * the scope associated with this filter. Used for subsetting candidate filters at evaluation time
     *
     * @generated from field: optional string scope = 7;
     */
    scope?: string | undefined;
};
/**
 * Describes the message Event.
 * Use `create(EventSchema)` to create a new message.
 */
export declare const EventSchema: GenMessage<Event>;
/**
 * @generated from message Events
 */
export type Events = Message<"Events"> & {
    /**
     * @generated from field: repeated Event events = 1;
     */
    events: Event[];
};
/**
 * Describes the message Events.
 * Use `create(EventsSchema)` to create a new message.
 */
export declare const EventsSchema: GenMessage<Events>;
/**
 * @generated from message PutLogRequest
 */
export type PutLogRequest = Message<"PutLogRequest"> & {
    /**
     * the task external run id for the request
     *
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
    /**
     * when the log line was created
     *
     * @generated from field: google.protobuf.Timestamp created_at = 2;
     */
    createdAt?: Timestamp | undefined;
    /**
     * the log line message
     *
     * @generated from field: string message = 3;
     */
    message: string;
    /**
     * the log line level
     *
     * @generated from field: optional string level = 4;
     */
    level?: string | undefined;
    /**
     * associated log line metadata
     *
     * @generated from field: string metadata = 5;
     */
    metadata: string;
    /**
     * the retry count of the task run
     *
     * @generated from field: optional int32 task_retry_count = 6;
     */
    taskRetryCount?: number | undefined;
};
/**
 * Describes the message PutLogRequest.
 * Use `create(PutLogRequestSchema)` to create a new message.
 */
export declare const PutLogRequestSchema: GenMessage<PutLogRequest>;
/**
 * @generated from message PutLogResponse
 */
export type PutLogResponse = Message<"PutLogResponse"> & {};
/**
 * Describes the message PutLogResponse.
 * Use `create(PutLogResponseSchema)` to create a new message.
 */
export declare const PutLogResponseSchema: GenMessage<PutLogResponse>;
/**
 * @generated from message PutStreamEventRequest
 */
export type PutStreamEventRequest = Message<"PutStreamEventRequest"> & {
    /**
     * the task external run id for the request
     *
     * @generated from field: string task_run_external_id = 1;
     */
    taskRunExternalId: string;
    /**
     * when the stream event was created
     *
     * @generated from field: google.protobuf.Timestamp created_at = 2;
     */
    createdAt?: Timestamp | undefined;
    /**
     * the stream event message
     *
     * @generated from field: bytes message = 3;
     */
    message: Uint8Array;
    /**
     * associated stream event metadata
     *
     * @generated from field: string metadata = 5;
     */
    metadata: string;
    /**
     * @generated from field: optional int64 event_index = 6;
     */
    eventIndex?: bigint | undefined;
};
/**
 * Describes the message PutStreamEventRequest.
 * Use `create(PutStreamEventRequestSchema)` to create a new message.
 */
export declare const PutStreamEventRequestSchema: GenMessage<PutStreamEventRequest>;
/**
 * @generated from message PutStreamEventResponse
 */
export type PutStreamEventResponse = Message<"PutStreamEventResponse"> & {};
/**
 * Describes the message PutStreamEventResponse.
 * Use `create(PutStreamEventResponseSchema)` to create a new message.
 */
export declare const PutStreamEventResponseSchema: GenMessage<PutStreamEventResponse>;
/**
 * @generated from message BulkPushEventRequest
 */
export type BulkPushEventRequest = Message<"BulkPushEventRequest"> & {
    /**
     * @generated from field: repeated PushEventRequest events = 1;
     */
    events: PushEventRequest[];
};
/**
 * Describes the message BulkPushEventRequest.
 * Use `create(BulkPushEventRequestSchema)` to create a new message.
 */
export declare const BulkPushEventRequestSchema: GenMessage<BulkPushEventRequest>;
/**
 * @generated from message PushEventRequest
 */
export type PushEventRequest = Message<"PushEventRequest"> & {
    /**
     * the key for the event
     *
     * @generated from field: string key = 1;
     */
    key: string;
    /**
     * the payload for the event
     *
     * @generated from field: string payload = 2;
     */
    payload: string;
    /**
     * when the event was generated
     *
     * @generated from field: google.protobuf.Timestamp event_timestamp = 3;
     */
    eventTimestamp?: Timestamp | undefined;
    /**
     * metadata for the event
     *
     * @generated from field: optional string additional_metadata = 4;
     */
    additionalMetadata?: string | undefined;
    /**
     * @generated from field: optional int32 priority = 5;
     */
    priority?: number | undefined;
    /**
     * the scope associated with this filter. Used for subsetting candidate filters at evaluation time
     *
     * @generated from field: optional string scope = 6;
     */
    scope?: string | undefined;
};
/**
 * Describes the message PushEventRequest.
 * Use `create(PushEventRequestSchema)` to create a new message.
 */
export declare const PushEventRequestSchema: GenMessage<PushEventRequest>;
/**
 * @generated from message ReplayEventRequest
 */
export type ReplayEventRequest = Message<"ReplayEventRequest"> & {
    /**
     * the event id to replay
     *
     * @generated from field: string event_id = 1;
     */
    eventId: string;
};
/**
 * Describes the message ReplayEventRequest.
 * Use `create(ReplayEventRequestSchema)` to create a new message.
 */
export declare const ReplayEventRequestSchema: GenMessage<ReplayEventRequest>;
/**
 * @generated from service EventsService
 */
export declare const EventsService: GenService<{
    /**
     * @generated from rpc EventsService.Push
     */
    push: {
        methodKind: "unary";
        input: typeof PushEventRequestSchema;
        output: typeof EventSchema;
    };
    /**
     * @generated from rpc EventsService.BulkPush
     */
    bulkPush: {
        methodKind: "unary";
        input: typeof BulkPushEventRequestSchema;
        output: typeof EventsSchema;
    };
    /**
     * @generated from rpc EventsService.ReplaySingleEvent
     */
    replaySingleEvent: {
        methodKind: "unary";
        input: typeof ReplayEventRequestSchema;
        output: typeof EventSchema;
    };
    /**
     * @generated from rpc EventsService.PutLog
     */
    putLog: {
        methodKind: "unary";
        input: typeof PutLogRequestSchema;
        output: typeof PutLogResponseSchema;
    };
    /**
     * @generated from rpc EventsService.PutStreamEvent
     */
    putStreamEvent: {
        methodKind: "unary";
        input: typeof PutStreamEventRequestSchema;
        output: typeof PutStreamEventResponseSchema;
    };
}>;
