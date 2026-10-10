import type { AdminServiceClient } from '../../protoc/v1/workflows';
import { WorkflowRunRef } from '../run-ref';
import type { CallOptions, CancelRunOpts, ReplayRunOpts, RunDetail } from '../types';
/**
 * Task and workflow runs, over the v1 `AdminService`. Everything here is a unary call, so
 * it works from any runtime the client runs on; listing runs is a REST feature of the Node
 * client and is not available here.
 */
export declare class RunsClient {
    private readonly rpc;
    constructor(rpc: AdminServiceClient);
    /**
     * Gets a run's state: its status, whether it is done, its input and metadata, and each of
     * its tasks with their status, output and error.
     * @param run - The run id or a reference to the run.
     * @param options - A signal or deadline for the call.
     */
    get<T = unknown>(run: string | WorkflowRunRef<T>, options?: CallOptions): Promise<RunDetail>;
    /** @alias get */
    getDetails<T = unknown>(run: string | WorkflowRunRef<T>, options?: CallOptions): Promise<RunDetail>;
    /**
     * Cancels runs by id, or every run matching the filter when no ids are given.
     * @returns the ids of the cancelled tasks
     */
    cancel(opts: CancelRunOpts): Promise<import("../../protoc/v1/workflows").CancelTasksResponse>;
    /**
     * Replays runs by id, or every run matching the filter when no ids are given.
     * @returns the ids of the replayed tasks
     */
    replay(opts: ReplayRunOpts): Promise<import("../../protoc/v1/workflows").ReplayTasksResponse>;
    /** A reference to an existing run, to wait on, cancel or replay it. */
    runRef<T = unknown>(id: string): WorkflowRunRef<T>;
}
