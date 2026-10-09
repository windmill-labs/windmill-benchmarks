export declare const EVICTION_TTL_SECONDS = 5;
export declare const LONG_SLEEP_SECONDS = 15;
export declare const EVENT_KEY = "durable-eviction:event";
export declare const childTask: import("../..").TaskWorkflowDeclaration<import("../..").UnknownInputType, {
    child_status: string;
}, {}, {}, {}, {}>;
export declare const evictableSleep: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    status: string;
}, {}, {}, {}, {}>;
export declare const evictableSleepForGracefulTermination: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    status: string;
}, {}, {}, {}, {}>;
export declare const evictableWaitForEvent: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    status: string;
}, {}, {}, {}, {}>;
export declare const MEMO_EVENT_KEY = "durable-eviction:memo-event";
export declare const evictableMemoThenWaitForEvent: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    status: string;
    memoizedNow: string;
}, {}, {}, {}, {}>;
export declare const evictableChildSpawn: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    child: {
        child_status: string;
    };
    status: string;
}, {}, {}, {}, {}>;
export declare const multipleEviction: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    status: string;
}, {}, {}, {}, {}>;
export declare const bulkChildTask: import("../..").TaskWorkflowDeclaration<{
    sleepSeconds: number;
}, {
    sleepSeconds: number;
    status: string;
}, {}, {}, {}, {}>;
export declare const evictableChildBulkSpawn: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    child_results: {
        sleepSeconds: number;
        status: string;
    }[];
    status: string;
}, {}, {}, {}, {}>;
export declare const CAPACITY_SLEEP_SECONDS = 20;
export declare const capacityEvictableSleep: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    status: string;
}, {}, {}, {}, {}>;
export declare const nonEvictableSleep: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    status: string;
}, {}, {}, {}, {}>;
