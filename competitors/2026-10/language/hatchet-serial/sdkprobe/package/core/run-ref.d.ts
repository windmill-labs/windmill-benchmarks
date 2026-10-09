import type { CallOptions, ResultOptions, RunDetail } from './types';
/** What a run reference needs from the client that created it. */
export interface RunRefClient {
    getDetails(runId: string, options?: CallOptions): Promise<RunDetail>;
    cancel(opts: {
        ids: string[];
    }): Promise<unknown>;
    replay(opts: {
        ids: string[];
    }): Promise<unknown>;
}
/** The first wait between two `GetRunDetails` polls. */
export declare const INITIAL_POLL_INTERVAL_MS = 250;
/** The longest wait between two polls; the interval doubles until it gets here. */
export declare const MAX_POLL_INTERVAL_MS = 5000;
/**
 * A reference to a triggered run. `result()` polls `GetRunDetails` until the run is done,
 * which is the one way to wait for a run that works on every platform the core client runs
 * on: no stream is held open, so a Worker request or a browser tab can wait on it.
 */
export declare class WorkflowRunRef<T> {
    private readonly runs;
    readonly workflowRunId: string;
    parentWorkflowRunId?: string;
    _standaloneTaskName?: string;
    /**
     * The signal `result()` waits with when none is passed, so a run spawned from inside a task
     * stops being awaited when the task is cancelled.
     */
    defaultSignal?: AbortSignal;
    constructor(workflowRunId: string, runs: RunRefClient, parentWorkflowRunId?: string, standaloneTaskName?: string, defaultSignal?: AbortSignal);
    get runId(): Promise<string>;
    getWorkflowRunId(): Promise<string>;
    /** The run's result; the same as `result()` with no options. */
    get output(): Promise<T>;
    /** The run's current state. */
    details(): Promise<RunDetail>;
    /**
     * Waits for the run to finish and returns its output: the standalone task's output for a
     * task, or the outputs keyed by task name for a workflow.
     *
     * The wait polls with backoff, starting at 250 ms and capped at 5 s with jitter. It is
     * unbounded unless `timeoutMs` is set (rejects with a `HatchetError`) or `signal` fires
     * (rejects with an `AbortError`). Both bound the poll in flight as well as the waits
     * between polls, so a stalled `GetRunDetails` cannot hold the result past the deadline.
     *
     * A failed or cancelled run rejects the way the Node client's `result()` does: with the
     * array of the tasks' error messages when any task reported one. When no task reported an
     * error the run resolves with the tasks' outputs whatever its status, as it does on the
     * Node client, since that is the shape an explicit cancellation leaves. A run with no
     * tasks at all rejects with an `Error` naming the run's status.
     */
    result(options?: ResultOptions): Promise<T>;
    private resolveResult;
    /**
     * The outputs keyed by task name, or the standalone task's own output, each read the way
     * the Node client reads them; a task output that is not JSON throws its `SyntaxError`.
     */
    private outputs;
    toJSON(): Promise<string>;
    cancel(): Promise<void>;
    replay(): Promise<void>;
}
