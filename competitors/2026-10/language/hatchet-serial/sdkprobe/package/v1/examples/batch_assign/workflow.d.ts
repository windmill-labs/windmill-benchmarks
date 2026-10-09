type SimpleInput = {
    message: string;
};
type SimpleOutput = {
    transformed_message: string;
};
type KeyedInput = {
    message: string;
    group: string;
};
type KeyedFailableInput = {
    message: string;
    group: string | number;
};
type KeyedOutput = {
    batch_key?: string;
    batch_size?: number;
    unique_keys?: number;
    uppercase: string;
};
type LargePayloadInput = {
    data: string;
};
type LargeOutput = {
    batch_id: string;
    received: boolean;
    batch_size: number;
    data_length: number;
};
type SingleOutput = {
    original: string;
    batch_size: number;
};
type OrderedInput = {
    index: number;
};
type OrderedOutput = {
    index: number;
};
type BroadcastOutput = {
    sum: number;
};
type ChildOutput = {
    message_len: number;
};
type ChildBatchOutput = {
    out: Record<string, SimpleInput>;
};
export declare const batchSimple: import("../..").TaskWorkflowDeclaration<SimpleInput, Record<string, SimpleOutput>, {}, {}, {}, {}>;
export declare const batchKeyed: import("../..").TaskWorkflowDeclaration<KeyedInput, Record<string, KeyedOutput>, {}, {}, {}, {}>;
export declare const batchKeyedFailable: import("../..").TaskWorkflowDeclaration<KeyedFailableInput, Record<string, KeyedOutput>, {}, {}, {}, {}>;
export declare const batchKeyedInterval: import("../..").TaskWorkflowDeclaration<KeyedInput, Record<string, KeyedOutput>, {}, {}, {}, {}>;
export declare const batchLarge: import("../..").TaskWorkflowDeclaration<LargePayloadInput, Record<string, LargeOutput>, {}, {}, {}, {}>;
export declare const batchSingle: import("../..").TaskWorkflowDeclaration<SimpleInput, Record<string, SingleOutput>, {}, {}, {}, {}>;
export declare const batchOrdered: import("../..").TaskWorkflowDeclaration<OrderedInput, Record<string, OrderedOutput>, {}, {}, {}, {}>;
export declare const batchBroadcast: import("../..").TaskWorkflowDeclaration<SimpleInput, BroadcastOutput, {}, {}, {}, {}>;
export declare const batchCancel: import("../..").TaskWorkflowDeclaration<SimpleInput, {}, {}, {}, {}, {}>;
export declare const child: import("../..").TaskWorkflowDeclaration<SimpleInput, ChildOutput, {}, {}, {}, {}>;
export declare const childBatch: import("../..").TaskWorkflowDeclaration<SimpleInput, ChildBatchOutput, {}, {}, {}, {}>;
export declare const batchChildSpawn: import("../..").TaskWorkflowDeclaration<SimpleInput, Record<string, ChildOutput>, {}, {}, {}, {}>;
export declare const batchChildBatchSpawn: import("../..").TaskWorkflowDeclaration<SimpleInput, Record<string, ChildBatchOutput>, {}, {}, {}, {}>;
export {};
