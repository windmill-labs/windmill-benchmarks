import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
export declare const protobufPackage = "v1";
export declare enum Action {
    CREATE = 0,
    QUEUE = 1,
    CANCEL = 2,
    SKIP = 3,
    UNRECOGNIZED = -1
}
export declare function actionFromJSON(object: any): Action;
export declare function actionToJSON(object: Action): string;
export interface BaseMatchCondition {
    readableDataKey: string;
    action: Action;
    /** a UUID defining the OR group for this condition */
    orGroupId: string;
    expression: string;
}
export interface ParentOverrideMatchCondition {
    base: BaseMatchCondition | undefined;
    parentReadableId: string;
}
export interface SleepMatchCondition {
    base: BaseMatchCondition | undefined;
    /** a duration string indicating how long to sleep */
    sleepFor: string;
}
export interface UserEventMatchCondition {
    base: BaseMatchCondition | undefined;
    userEventKey: string;
    /** an optional scope for the user event condition (similar to scopes on event filters) */
    eventScope?: string | undefined;
    considerEventsSince?: Date | undefined;
}
export interface TaskConditions {
    parentOverrideConditions: ParentOverrideMatchCondition[];
    sleepConditions: SleepMatchCondition[];
    userEventConditions: UserEventMatchCondition[];
}
export interface DurableEventListenerConditions {
    sleepConditions: SleepMatchCondition[];
    userEventConditions: UserEventMatchCondition[];
}
export declare const BaseMatchCondition: MessageFns<BaseMatchCondition>;
export declare const ParentOverrideMatchCondition: MessageFns<ParentOverrideMatchCondition>;
export declare const SleepMatchCondition: MessageFns<SleepMatchCondition>;
export declare const UserEventMatchCondition: MessageFns<UserEventMatchCondition>;
export declare const TaskConditions: MessageFns<TaskConditions>;
export declare const DurableEventListenerConditions: MessageFns<DurableEventListenerConditions>;
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
