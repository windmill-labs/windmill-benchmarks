import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
export declare const protobufPackage = "v1";
export declare enum WorkerLabelComparator {
    EQUAL = 0,
    NOT_EQUAL = 1,
    GREATER_THAN = 2,
    GREATER_THAN_OR_EQUAL = 3,
    LESS_THAN = 4,
    LESS_THAN_OR_EQUAL = 5,
    UNRECOGNIZED = -1
}
export declare function workerLabelComparatorFromJSON(object: any): WorkerLabelComparator;
export declare function workerLabelComparatorToJSON(object: WorkerLabelComparator): string;
export interface DesiredWorkerLabels {
    /** value of the affinity */
    strValue?: string | undefined;
    intValue?: number | undefined;
    /**
     * (optional) Specifies whether the affinity setting is required.
     * If required, the worker will not accept actions that do not have a truthy affinity setting.
     *
     * Defaults to false.
     */
    required?: boolean | undefined;
    /**
     * (optional) Specifies the comparator for the affinity setting.
     * If not set, the default is EQUAL.
     */
    comparator?: WorkerLabelComparator | undefined;
    /**
     * (optional) Specifies the weight of the affinity setting.
     * If not set, the default is 100.
     */
    weight?: number | undefined;
}
export interface TriggerWorkflowRequest {
    name: string;
    /** (optional) the input data for the workflow */
    input: string;
    /** (optional) the parent workflow run id */
    parentId?: string | undefined;
    /** (optional) the parent task external run id */
    parentTaskRunExternalId?: string | undefined;
    /**
     * (optional) the index of the child workflow. if this is set, matches on the index or the
     * child key will return an existing workflow run if the parent id, parent task run id, and
     * child index/key match an existing workflow run.
     */
    childIndex?: number | undefined;
    /**
     * (optional) the key for the child. if this is set, matches on the index or the
     * child key will return an existing workflow run if the parent id, parent task run id, and
     * child index/key match an existing workflow run.
     */
    childKey?: string | undefined;
    /** (optional) additional metadata for the workflow */
    additionalMetadata?: string | undefined;
    /**
     * (optional) desired worker id for the workflow run,
     * requires the workflow definition to have a sticky strategy
     */
    desiredWorkerId?: string | undefined;
    /** (optional) override for the priority of the workflow tasks, will set all tasks to this priority */
    priority?: number | undefined;
    /** (optional) the desired worker labels for the workflow run, which will be used to determine which workers can pick up the workflow's tasks. if not set, defaults to an empty set of labels, which means any worker can pick up the tasks. */
    desiredWorkerLabels: {
        [key: string]: DesiredWorkerLabels;
    };
}
export interface TriggerWorkflowRequest_DesiredWorkerLabelsEntry {
    key: string;
    value: DesiredWorkerLabels | undefined;
}
export declare const DesiredWorkerLabels: MessageFns<DesiredWorkerLabels>;
export declare const TriggerWorkflowRequest: MessageFns<TriggerWorkflowRequest>;
export declare const TriggerWorkflowRequest_DesiredWorkerLabelsEntry: MessageFns<TriggerWorkflowRequest_DesiredWorkerLabelsEntry>;
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
