import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import { type CallContext, type CallOptions } from 'nice-grpc-common';
export declare const protobufPackage = "";
export interface CancelTasksRequest {
    tasks: TaskIdRetryCount[];
    filter?: TasksFilter | undefined;
}
export interface ReplayTasksRequest {
    tasks: TaskIdRetryCount[];
    filter?: TasksFilter | undefined;
}
export interface TasksFilter {
    statuses: string[];
    since: Date | undefined;
    until?: Date | undefined;
    workflowIds: string[];
    additionalMetadata: string[];
}
export interface TaskIdRetryCount {
    taskId: number;
    retryCount: number;
}
export interface CancelTasksResponse {
    cancelledTasks: TaskIdRetryCount[];
}
export interface ReplayTasksResponse {
    replayedTasks: TaskIdRetryCount[];
}
export declare const CancelTasksRequest: MessageFns<CancelTasksRequest>;
export declare const ReplayTasksRequest: MessageFns<ReplayTasksRequest>;
export declare const TasksFilter: MessageFns<TasksFilter>;
export declare const TaskIdRetryCount: MessageFns<TaskIdRetryCount>;
export declare const CancelTasksResponse: MessageFns<CancelTasksResponse>;
export declare const ReplayTasksResponse: MessageFns<ReplayTasksResponse>;
/** AdminService represents a set of RPCs for admin management of tasks, workflows, etc. */
export type AdminServiceDefinition = typeof AdminServiceDefinition;
export declare const AdminServiceDefinition: {
    readonly name: "AdminService";
    readonly fullName: "AdminService";
    readonly methods: {
        readonly cancelTasks: {
            readonly name: "CancelTasks";
            readonly requestType: MessageFns<CancelTasksRequest>;
            readonly requestStream: false;
            readonly responseType: MessageFns<CancelTasksResponse>;
            readonly responseStream: false;
            readonly options: {};
        };
        readonly replayTasks: {
            readonly name: "ReplayTasks";
            readonly requestType: MessageFns<ReplayTasksRequest>;
            readonly requestStream: false;
            readonly responseType: MessageFns<ReplayTasksResponse>;
            readonly responseStream: false;
            readonly options: {};
        };
    };
};
export interface AdminServiceImplementation<CallContextExt = {}> {
    cancelTasks(request: CancelTasksRequest, context: CallContext & CallContextExt): Promise<DeepPartial<CancelTasksResponse>>;
    replayTasks(request: ReplayTasksRequest, context: CallContext & CallContextExt): Promise<DeepPartial<ReplayTasksResponse>>;
}
export interface AdminServiceClient<CallOptionsExt = {}> {
    cancelTasks(request: DeepPartial<CancelTasksRequest>, options?: CallOptions & CallOptionsExt): Promise<CancelTasksResponse>;
    replayTasks(request: DeepPartial<ReplayTasksRequest>, options?: CallOptions & CallOptionsExt): Promise<ReplayTasksResponse>;
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
