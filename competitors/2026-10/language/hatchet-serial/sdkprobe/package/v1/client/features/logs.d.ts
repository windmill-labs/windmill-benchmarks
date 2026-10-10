import { V1LogLineLevel, V1LogLineOrderByDirection } from '../../../clients/rest/generated/data-contracts';
import { HatchetClient } from '../client';
/**
 * The options for the list logs operation.
 */
export type ListLogsOpts = {
    /** The maximum number of log lines to return. */
    limit?: number;
    /** Return only logs after this date. */
    since?: Date;
    /** Return only logs before this date. */
    until?: Date;
    /** Filter logs by a search string. */
    search?: string;
    /** Filter logs by log level. */
    levels?: V1LogLineLevel[];
    /** The direction to order the logs by. */
    orderByDirection?: V1LogLineOrderByDirection;
    /** Filter logs by attempt number. */
    attempt?: number;
};
/**
 * The logs client is a client for interacting with Hatchet's logs API.
 */
export declare class LogsClient {
    tenantId: string;
    api: HatchetClient['api'];
    constructor(client: HatchetClient);
    /**
     * Lists the logs for a given task run.
     * @param taskRunId - The ID of the task run to list logs for.
     * @param opts - The options filter for the list operation.
     * @returns A promise that resolves to the list of logs.
     */
    list(taskRunId: string, opts?: ListLogsOpts): Promise<import("../../../clients/rest/generated/data-contracts").V1LogLineList>;
}
export { V1LogLineLevel, V1LogLineOrderByDirection };
