import type { Transport } from '@connectrpc/connect';
import type { Logger } from '../util/logger/logger';
import { BaseWorkflowDeclaration, type RunManyOpt } from '../v1/declaration';
import type { InputType, OutputType, UnknownInputType } from '../v1/types';
import { EventsClient } from './features/events';
import { LogsClient } from './features/logs';
import { RateLimitsClient } from './features/rate-limits';
import { RunsClient } from './features/runs';
import { StreamsClient } from './features/streams';
import { WorkflowsClient } from './features/workflows';
import { WorkflowRunRef } from './run-ref';
import type { CoreClientConfig, TriggerRunOptions } from './types';
/** A workflow to run: a declaration from `/edge`, anything with a `name`, or the name itself. */
export type WorkflowRef<I extends InputType = UnknownInputType, O extends OutputType = void> = BaseWorkflowDeclaration<I, O> | {
    name: string;
} | string;
/**
 * The resolved client configuration: what the client was built with, plus the values derived
 * from it.
 */
export interface ResolvedCoreConfig {
    serverUrl: string;
    tenantId: string;
    namespace: string;
    logLevel: CoreClientConfig['logLevel'];
    retrier: CoreClientConfig['retrier'];
}
/**
 * The application client for runtimes without gRPC: it triggers runs, pushes events, waits
 * for results and manages workflows over unary Connect calls on a fetch transport, so it runs
 * in Cloudflare Workers, Vercel Functions, Deno, Bun, browsers and Node alike.
 *
 * It is configured from an explicit object only. Nothing is read from the environment or a
 * config file, and no worker can be started from it; workers stay on the Node client.
 *
 * ```typescript
 * import { Hatchet } from './';
 *
 * const hatchet = new Hatchet({ token: env.HATCHET_CLIENT_TOKEN });
 * const output = await hatchet.run('greet', { name: 'world' });
 * ```
 */
export declare class HatchetCore {
    readonly config: ResolvedCoreConfig;
    /** The transport every call goes through. */
    readonly transport: Transport;
    readonly logger: Logger;
    /** Pushes events. */
    readonly events: EventsClient;
    /** Gets, cancels and replays runs. */
    readonly runs: RunsClient;
    /** Registers workflows. */
    readonly workflows: WorkflowsClient;
    /** Creates and updates rate limits. */
    readonly rateLimits: RateLimitsClient;
    /** Writes task run log lines. */
    readonly logs: LogsClient;
    /** Publishes task run stream chunks. */
    readonly streams: StreamsClient;
    private readonly workflowsRpc;
    private readonly adminRpc;
    private readonly eventsRpc;
    constructor(config: CoreClientConfig);
    /** The same as `new HatchetCore(config)`, mirroring the Node client's `init`. */
    static init(config: CoreClientConfig): HatchetCore;
    /** The tenant the token belongs to. */
    get tenantId(): string;
    /**
     * Triggers a run and returns a reference to it without waiting.
     *
     * The options are the Node client's `admin.runWorkflow` options and go to the engine as
     * is: the workflow name is namespaced and lowercased and the input is JSON. A task
     * declaration's reference resolves to the task's own output.
     */
    runNoWait<I extends InputType = UnknownInputType, O extends OutputType = void>(workflow: WorkflowRef<I, O>, input: I, options?: TriggerRunOptions): Promise<WorkflowRunRef<O>>;
    /** Triggers a run and waits for its output; see `WorkflowRunRef.result()` for the wait. */
    run<I extends InputType = UnknownInputType, O extends OutputType = void>(workflow: WorkflowRef<I, O>, input: I, options?: TriggerRunOptions): Promise<O>;
    /**
     * Triggers many runs of one workflow in batches of 500 (or 4 MB) over `BulkTriggerWorkflow`
     * and returns their references in input order. `runs` takes the `RunManyOpt` entries the
     * declarations' `runMany` takes.
     *
     * Each batch is one call, so a failure part way through leaves the earlier batches' runs
     * created. That failure rejects with a `BulkTriggerPartialError` whose `refs` are those runs'
     * references and whose `cause` is the batch's own error, so the caller can keep or cancel
     * them instead of retrying the whole request and creating them twice. A failure of the
     * first batch rejects with the batch's error itself: a
     * `BulkTriggerIdempotencyCollisionError` on an idempotency key collision, otherwise a
     * `HatchetError`.
     */
    runManyNoWait<I extends InputType = UnknownInputType, O extends OutputType = void>(workflow: WorkflowRef<I, O>, runs: RunManyOpt<I>[]): Promise<WorkflowRunRef<O>[]>;
    /**
     * Triggers many runs of one workflow and waits for all of their outputs, in input order.
     *
     * When any entry sets `opts.returnExceptions`, a failed run does not reject the whole
     * wait: its slot holds an `Error` instead (a task error list is joined with `; `), the way
     * a declaration's `runMany` behaves on the Node client.
     */
    runMany<I extends InputType = UnknownInputType, O extends OutputType = void>(workflow: WorkflowRef<I, O>, runs: RunManyOpt<I>[]): Promise<O[]>;
    /** A reference to an existing run, to wait on, cancel or replay it. */
    runRef<T = unknown>(id: string): WorkflowRunRef<T>;
}
