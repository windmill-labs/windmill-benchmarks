import type { GenEnum, GenFile, GenMessage } from "@bufbuild/protobuf/codegenv2";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file v1/shared/condition.proto.
 */
export declare const file_v1_shared_condition: GenFile;
/**
 * @generated from message v1.BaseMatchCondition
 */
export type BaseMatchCondition = Message<"v1.BaseMatchCondition"> & {
    /**
     * @generated from field: string readable_data_key = 1;
     */
    readableDataKey: string;
    /**
     * @generated from field: v1.Action action = 2;
     */
    action: Action;
    /**
     * a UUID defining the OR group for this condition
     *
     * @generated from field: string or_group_id = 3;
     */
    orGroupId: string;
    /**
     * @generated from field: string expression = 4;
     */
    expression: string;
};
/**
 * Describes the message v1.BaseMatchCondition.
 * Use `create(BaseMatchConditionSchema)` to create a new message.
 */
export declare const BaseMatchConditionSchema: GenMessage<BaseMatchCondition>;
/**
 * @generated from message v1.ParentOverrideMatchCondition
 */
export type ParentOverrideMatchCondition = Message<"v1.ParentOverrideMatchCondition"> & {
    /**
     * @generated from field: v1.BaseMatchCondition base = 1;
     */
    base?: BaseMatchCondition | undefined;
    /**
     * @generated from field: string parent_readable_id = 2;
     */
    parentReadableId: string;
};
/**
 * Describes the message v1.ParentOverrideMatchCondition.
 * Use `create(ParentOverrideMatchConditionSchema)` to create a new message.
 */
export declare const ParentOverrideMatchConditionSchema: GenMessage<ParentOverrideMatchCondition>;
/**
 * @generated from message v1.SleepMatchCondition
 */
export type SleepMatchCondition = Message<"v1.SleepMatchCondition"> & {
    /**
     * @generated from field: v1.BaseMatchCondition base = 1;
     */
    base?: BaseMatchCondition | undefined;
    /**
     * a duration string indicating how long to sleep
     *
     * @generated from field: string sleep_for = 2;
     */
    sleepFor: string;
};
/**
 * Describes the message v1.SleepMatchCondition.
 * Use `create(SleepMatchConditionSchema)` to create a new message.
 */
export declare const SleepMatchConditionSchema: GenMessage<SleepMatchCondition>;
/**
 * @generated from message v1.UserEventMatchCondition
 */
export type UserEventMatchCondition = Message<"v1.UserEventMatchCondition"> & {
    /**
     * @generated from field: v1.BaseMatchCondition base = 1;
     */
    base?: BaseMatchCondition | undefined;
    /**
     * @generated from field: string user_event_key = 2;
     */
    userEventKey: string;
    /**
     * an optional scope for the user event condition (similar to scopes on event filters)
     *
     * @generated from field: optional string event_scope = 3;
     */
    eventScope?: string | undefined;
    /**
     * @generated from field: optional google.protobuf.Timestamp consider_events_since = 4;
     */
    considerEventsSince?: Timestamp | undefined;
};
/**
 * Describes the message v1.UserEventMatchCondition.
 * Use `create(UserEventMatchConditionSchema)` to create a new message.
 */
export declare const UserEventMatchConditionSchema: GenMessage<UserEventMatchCondition>;
/**
 * @generated from message v1.TaskConditions
 */
export type TaskConditions = Message<"v1.TaskConditions"> & {
    /**
     * @generated from field: repeated v1.ParentOverrideMatchCondition parent_override_conditions = 1;
     */
    parentOverrideConditions: ParentOverrideMatchCondition[];
    /**
     * @generated from field: repeated v1.SleepMatchCondition sleep_conditions = 2;
     */
    sleepConditions: SleepMatchCondition[];
    /**
     * @generated from field: repeated v1.UserEventMatchCondition user_event_conditions = 3;
     */
    userEventConditions: UserEventMatchCondition[];
};
/**
 * Describes the message v1.TaskConditions.
 * Use `create(TaskConditionsSchema)` to create a new message.
 */
export declare const TaskConditionsSchema: GenMessage<TaskConditions>;
/**
 * @generated from message v1.DurableEventListenerConditions
 */
export type DurableEventListenerConditions = Message<"v1.DurableEventListenerConditions"> & {
    /**
     * @generated from field: repeated v1.SleepMatchCondition sleep_conditions = 1;
     */
    sleepConditions: SleepMatchCondition[];
    /**
     * @generated from field: repeated v1.UserEventMatchCondition user_event_conditions = 2;
     */
    userEventConditions: UserEventMatchCondition[];
};
/**
 * Describes the message v1.DurableEventListenerConditions.
 * Use `create(DurableEventListenerConditionsSchema)` to create a new message.
 */
export declare const DurableEventListenerConditionsSchema: GenMessage<DurableEventListenerConditions>;
/**
 * @generated from enum v1.Action
 */
export declare enum Action {
    /**
     * @generated from enum value: CREATE = 0;
     */
    CREATE = 0,
    /**
     * @generated from enum value: QUEUE = 1;
     */
    QUEUE = 1,
    /**
     * @generated from enum value: CANCEL = 2;
     */
    CANCEL = 2,
    /**
     * @generated from enum value: SKIP = 3;
     */
    SKIP = 3
}
/**
 * Describes the enum v1.Action.
 */
export declare const ActionSchema: GenEnum<Action>;
