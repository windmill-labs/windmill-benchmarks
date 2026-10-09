import type { GenEnum, GenFile, GenMessage } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file v1/shared/trigger.proto.
 */
export declare const file_v1_shared_trigger: GenFile;
/**
 * @generated from message v1.DesiredWorkerLabels
 */
export type DesiredWorkerLabels = Message<"v1.DesiredWorkerLabels"> & {
    /**
     * value of the affinity
     *
     * @generated from field: optional string str_value = 1;
     */
    strValue?: string | undefined;
    /**
     * @generated from field: optional int32 int_value = 2;
     */
    intValue?: number | undefined;
    /**
     * *
     * (optional) Specifies whether the affinity setting is required.
     * If required, the worker will not accept actions that do not have a truthy affinity setting.
     *
     * Defaults to false.
     *
     * @generated from field: optional bool required = 3;
     */
    required?: boolean | undefined;
    /**
     * *
     * (optional) Specifies the comparator for the affinity setting.
     * If not set, the default is EQUAL.
     *
     * @generated from field: optional v1.WorkerLabelComparator comparator = 4;
     */
    comparator?: WorkerLabelComparator | undefined;
    /**
     * *
     * (optional) Specifies the weight of the affinity setting.
     * If not set, the default is 100.
     *
     * @generated from field: optional int32 weight = 5;
     */
    weight?: number | undefined;
};
/**
 * Describes the message v1.DesiredWorkerLabels.
 * Use `create(DesiredWorkerLabelsSchema)` to create a new message.
 */
export declare const DesiredWorkerLabelsSchema: GenMessage<DesiredWorkerLabels>;
/**
 * @generated from message v1.TriggerWorkflowRequest
 */
export type TriggerWorkflowRequest = Message<"v1.TriggerWorkflowRequest"> & {
    /**
     * @generated from field: string name = 1;
     */
    name: string;
    /**
     * (optional) the input data for the workflow
     *
     * @generated from field: string input = 2;
     */
    input: string;
    /**
     * (optional) the parent workflow run id
     *
     * @generated from field: optional string parent_id = 3;
     */
    parentId?: string | undefined;
    /**
     * (optional) the parent task external run id
     *
     * @generated from field: optional string parent_task_run_external_id = 4;
     */
    parentTaskRunExternalId?: string | undefined;
    /**
     * (optional) the index of the child workflow. if this is set, matches on the index or the
     * child key will return an existing workflow run if the parent id, parent task run id, and
     * child index/key match an existing workflow run.
     *
     * @generated from field: optional int32 child_index = 5;
     */
    childIndex?: number | undefined;
    /**
     * (optional) the key for the child. if this is set, matches on the index or the
     * child key will return an existing workflow run if the parent id, parent task run id, and
     * child index/key match an existing workflow run.
     *
     * @generated from field: optional string child_key = 6;
     */
    childKey?: string | undefined;
    /**
     * (optional) additional metadata for the workflow
     *
     * @generated from field: optional string additional_metadata = 7;
     */
    additionalMetadata?: string | undefined;
    /**
     * (optional) desired worker id for the workflow run,
     * requires the workflow definition to have a sticky strategy
     *
     * @generated from field: optional string desired_worker_id = 8;
     */
    desiredWorkerId?: string | undefined;
    /**
     * (optional) override for the priority of the workflow tasks, will set all tasks to this priority
     *
     * @generated from field: optional int32 priority = 9;
     */
    priority?: number | undefined;
    /**
     * (optional) the desired worker labels for the workflow run, which will be used to determine which workers can pick up the workflow's tasks. if not set, defaults to an empty set of labels, which means any worker can pick up the tasks.
     *
     * @generated from field: map<string, v1.DesiredWorkerLabels> desired_worker_labels = 10;
     */
    desiredWorkerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
};
/**
 * Describes the message v1.TriggerWorkflowRequest.
 * Use `create(TriggerWorkflowRequestSchema)` to create a new message.
 */
export declare const TriggerWorkflowRequestSchema: GenMessage<TriggerWorkflowRequest>;
/**
 * @generated from enum v1.WorkerLabelComparator
 */
export declare enum WorkerLabelComparator {
    /**
     * @generated from enum value: EQUAL = 0;
     */
    EQUAL = 0,
    /**
     * @generated from enum value: NOT_EQUAL = 1;
     */
    NOT_EQUAL = 1,
    /**
     * @generated from enum value: GREATER_THAN = 2;
     */
    GREATER_THAN = 2,
    /**
     * @generated from enum value: GREATER_THAN_OR_EQUAL = 3;
     */
    GREATER_THAN_OR_EQUAL = 3,
    /**
     * @generated from enum value: LESS_THAN = 4;
     */
    LESS_THAN = 4,
    /**
     * @generated from enum value: LESS_THAN_OR_EQUAL = 5;
     */
    LESS_THAN_OR_EQUAL = 5
}
/**
 * Describes the enum v1.WorkerLabelComparator.
 */
export declare const WorkerLabelComparatorSchema: GenEnum<WorkerLabelComparator>;
