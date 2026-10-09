import { LogLevel } from '../../clients/event/rpc';
import type { EventsServiceClient, PutLogResponse } from '../../protoc/events/events';
/** The longest log line accepted, in characters; the Node client's `putLog` holds the same limit. */
export declare const MAX_LOG_LINE_CHARS = 1000;
export interface PutLogOptions {
    /** Defaults to `INFO`. */
    level?: LogLevel;
    /** The attempt the line belongs to, when the task has retried. */
    retryCount?: number;
    metadata?: Record<string, unknown>;
}
/** Writes log lines against a task run, the way `ctx.log` does from a worker. */
export declare class LogsClient {
    private readonly rpc;
    constructor(rpc: EventsServiceClient);
    /**
     * Stores one log line for a task run. Unlike the Node client's fire-and-forget `putLog`,
     * this resolves once the engine has the line and rejects when it does not, since a
     * serverless runtime may end the invocation as soon as the handler returns.
     *
     * A line over `MAX_LOG_LINE_CHARS` (1,000) characters rejects with a `HatchetError` that
     * states the length, where the Node client warns and drops the line; the message itself is
     * never part of the error.
     */
    put(taskRunExternalId: string, message: string, options?: PutLogOptions): Promise<PutLogResponse>;
}
