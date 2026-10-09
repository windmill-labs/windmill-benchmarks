import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import type { CallContext, CallOptions } from 'nice-grpc-common';
export declare const protobufPackage = "";
export interface Event {
    /** the tenant id */
    tenantId: string;
    /** the id of the event */
    eventId: string;
    /** the key for the event */
    key: string;
    /** the payload for the event */
    payload: string;
    /** when the event was generated */
    eventTimestamp: Date | undefined;
    /** the additional metadata for the event */
    additionalMetadata?: string | undefined;
    /** the scope associated with this filter. Used for subsetting candidate filters at evaluation time */
    scope?: string | undefined;
}
export interface Events {
    events: Event[];
}
export interface PutLogRequest {
    /** the task external run id for the request */
    taskRunExternalId: string;
    /** when the log line was created */
    createdAt: Date | undefined;
    /** the log line message */
    message: string;
    /** the log line level */
    level?: string | undefined;
    /** associated log line metadata */
    metadata: string;
    /** the retry count of the task run */
    taskRetryCount?: number | undefined;
}
export interface PutLogResponse {
}
export interface PutStreamEventRequest {
    /** the task external run id for the request */
    taskRunExternalId: string;
    /** when the stream event was created */
    createdAt: Date | undefined;
    /** the stream event message */
    message: Uint8Array;
    /** associated stream event metadata */
    metadata: string;
    eventIndex?: number | undefined;
}
export interface PutStreamEventResponse {
}
export interface BulkPushEventRequest {
    events: PushEventRequest[];
}
export interface PushEventRequest {
    /** the key for the event */
    key: string;
    /** the payload for the event */
    payload: string;
    /** when the event was generated */
    eventTimestamp: Date | undefined;
    /** metadata for the event */
    additionalMetadata?: string | undefined;
    priority?: number | undefined;
    /** the scope associated with this filter. Used for subsetting candidate filters at evaluation time */
    scope?: string | undefined;
}
export interface ReplayEventRequest {
    /** the event id to replay */
    eventId: string;
}
export declare const Event: MessageFns<Event>;
export declare const Events: MessageFns<Events>;
export declare const PutLogRequest: MessageFns<PutLogRequest>;
export declare const PutLogResponse: MessageFns<PutLogResponse>;
export declare const PutStreamEventRequest: MessageFns<PutStreamEventRequest>;
export declare const PutStreamEventResponse: MessageFns<PutStreamEventResponse>;
export declare const BulkPushEventRequest: MessageFns<BulkPushEventRequest>;
export declare const PushEventRequest: MessageFns<PushEventRequest>;
export declare const ReplayEventRequest: MessageFns<ReplayEventRequest>;
export type EventsServiceDefinition = typeof EventsServiceDefinition;
export declare const EventsServiceDefinition: {
    readonly name: "EventsService";
    readonly fullName: "EventsService";
    readonly methods: {
        readonly push: {
            readonly name: "Push";
            readonly requestType: typeof PushEventRequest;
            readonly requestStream: false;
            readonly responseType: typeof Event;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly bulkPush: {
            readonly name: "BulkPush";
            readonly requestType: typeof BulkPushEventRequest;
            readonly requestStream: false;
            readonly responseType: typeof Events;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly replaySingleEvent: {
            readonly name: "ReplaySingleEvent";
            readonly requestType: typeof ReplayEventRequest;
            readonly requestStream: false;
            readonly responseType: typeof Event;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly putLog: {
            readonly name: "PutLog";
            readonly requestType: typeof PutLogRequest;
            readonly requestStream: false;
            readonly responseType: typeof PutLogResponse;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly putStreamEvent: {
            readonly name: "PutStreamEvent";
            readonly requestType: typeof PutStreamEventRequest;
            readonly requestStream: false;
            readonly responseType: typeof PutStreamEventResponse;
            readonly responseStream: false;
            readonly options: {};
        };
    };
};
export interface EventsServiceImplementation<CallContextExt = {}> {
    push(request: PushEventRequest, context: CallContext & CallContextExt): Promise<DeepPartial<Event>>;
    bulkPush(request: BulkPushEventRequest, context: CallContext & CallContextExt): Promise<DeepPartial<Events>>;
    replaySingleEvent(request: ReplayEventRequest, context: CallContext & CallContextExt): Promise<DeepPartial<Event>>;
    putLog(request: PutLogRequest, context: CallContext & CallContextExt): Promise<DeepPartial<PutLogResponse>>;
    putStreamEvent(request: PutStreamEventRequest, context: CallContext & CallContextExt): Promise<DeepPartial<PutStreamEventResponse>>;
}
export interface EventsServiceClient<CallOptionsExt = {}> {
    push(request: DeepPartial<PushEventRequest>, options?: CallOptions & CallOptionsExt): Promise<Event>;
    bulkPush(request: DeepPartial<BulkPushEventRequest>, options?: CallOptions & CallOptionsExt): Promise<Events>;
    replaySingleEvent(request: DeepPartial<ReplayEventRequest>, options?: CallOptions & CallOptionsExt): Promise<Event>;
    putLog(request: DeepPartial<PutLogRequest>, options?: CallOptions & CallOptionsExt): Promise<PutLogResponse>;
    putStreamEvent(request: DeepPartial<PutStreamEventRequest>, options?: CallOptions & CallOptionsExt): Promise<PutStreamEventResponse>;
}
type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;
export type DeepPartial<T> = T extends Builtin ? T : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {} ? {
    [K in keyof T]?: DeepPartial<T[K]>;
} : Partial<T>;
export interface MessageFns<T> {
    encode(message: T, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): T;
    fromJSON(object: any): T;
    toJSON(message: T): unknown;
    create(base?: DeepPartial<T>): T;
    fromPartial(object: DeepPartial<T>): T;
}
export {};
