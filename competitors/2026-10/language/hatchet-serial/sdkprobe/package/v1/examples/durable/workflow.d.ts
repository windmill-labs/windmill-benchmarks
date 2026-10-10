export declare const EVENT_KEY = "durable-example:event";
export declare const SLEEP_TIME_SECONDS = 2;
export declare const SLEEP_TIME: "2s";
export declare const durableWorkflow: import("../..").WorkflowDeclaration<import("../..").UnknownInputType, {}, {}>;
export declare const waitForSleepTwice: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    runtime: number;
}, {}, {}, {}, {}>;
export declare const spawnChildTask: import("../..").TaskWorkflowDeclaration<{
    n?: number;
}, {
    message: string;
}, {}, {}, {}, {}>;
export declare const durableWithSpawn: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    child_output: {
        message: string;
    };
}, {}, {}, {}, {}>;
export declare const durableWithBulkSpawn: import("../..").TaskWorkflowDeclaration<{
    n?: number;
}, {
    child_outputs: {
        message: string;
    }[];
}, {}, {}, {}, {}>;
export declare const durableSleepEventSpawn: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    runtime: number;
    child_output: {
        message: string;
    };
}, {}, {}, {}, {}>;
export declare const durableWithExplicitSpawn: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    child_output: {
        message: string;
    };
}, {}, {}, {}, {}>;
export declare const durableNonDeterminism: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    attempt_number: number;
    sleep_time: number;
    non_determinism_detected: true;
    node_id: number;
} | {
    attempt_number: number;
    sleep_time: number;
    non_determinism_detected: false;
    node_id?: undefined;
}, {}, {}, {}, {}>;
export declare const REPLAY_RESET_SLEEP_SECONDS = 3;
/** Max duration (seconds) for a replayed/memoized step; above this we treat it as a real sleep. */
export declare const REPLAY_RESET_MEMOIZED_MAX_SECONDS = 5;
export declare const durableReplayReset: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    sleep_1_duration: number;
    sleep_2_duration: number;
    sleep_3_duration: number;
}, {}, {}, {}, {}>;
export declare const LOOKBACK_WINDOW: "1m";
export declare const waitForEventLookback: import("../..").TaskWorkflowDeclaration<{
    userId: number;
}, {
    elapsed: number;
    event: {
        order: string;
        user_id: number;
    };
}, {}, {}, {}, {}>;
export declare const waitForOrEventLookback: import("../..").TaskWorkflowDeclaration<{
    scope: string;
}, {
    elapsed: number;
}, {}, {}, {}, {}>;
export declare const waitForTwoEventsSecondPushedFirst: import("../..").TaskWorkflowDeclaration<{
    scope: string;
}, {
    elapsed: number;
    event1: {
        order: string;
    };
    event2: {
        order: string;
    };
}, {}, {}, {}, {}>;
export declare const memoNowCaching: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    start_time: string;
}, {}, {}, {}, {}>;
export declare const dagChildWorkflow: import("../..").WorkflowDeclaration<import("../..").UnknownInputType, {}, {}>;
export declare const errorRaisingTask: import("../..").TaskWorkflowDeclaration<{
    error_message: string;
}, never, {}, {}, {}, {}>;
export declare const errorRaisingDurableParent: import("../..").TaskWorkflowDeclaration<{
    error_message: string;
}, {
    child_raised: boolean;
    child_error_str: string | null;
    child_run_external_id: string;
    parent_run_external_id: string;
}, {}, {}, {}, {}>;
export declare const durableSpawnDag: import("../..").TaskWorkflowDeclaration<import("../..").JsonObject, {
    sleep_duration: number;
    sleep_duration_ms: number;
    spawn_duration: number;
    spawn_result: {};
}, {}, {}, {}, {}>;
