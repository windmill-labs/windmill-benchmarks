import { RunListenerClient, StepRunEvent } from '../clients/listeners/run-listener/child-listener-client';
import { RunsClient } from '../v1';
type EventualWorkflowRunId = string | Promise<string> | Promise<{
    workflowRunId: string;
}>;
export declare class DedupeViolationErr extends Error {
    constructor(message: string);
}
export default class WorkflowRunRef<T> {
    workflowRunId: EventualWorkflowRunId;
    parentWorkflowRunId?: string;
    private client;
    private runs;
    _standaloneTaskName?: string;
    /**
     * Optional default AbortSignal used for listener-backed waits (e.g. `.result()`).
     * This is primarily set when a run is spawned from within a task so cancellations propagate
     * without manually threading `{ signal }` everywhere.
     */
    defaultSignal?: AbortSignal;
    constructor(workflowRunId: string | Promise<string> | Promise<{
        workflowRunId: string;
    }>, client: RunListenerClient, runsClient?: RunsClient, parentWorkflowRunId?: string, standaloneTaskName?: string, defaultSignal?: AbortSignal);
    get runId(): Promise<string>;
    getWorkflowRunId(): Promise<string>;
    stream(): Promise<AsyncGenerator<StepRunEvent, void, unknown>>;
    get output(): Promise<T>;
    /**
     * @alias output
     * @deprecated use output
     */
    result(): Promise<T>;
    toJSON(): Promise<string>;
    cancel(): Promise<void>;
    replay(): Promise<void>;
}
export {};
