import { HatchetClient } from '../client';
export type StreamEvent = {
    /** the message payload */
    payload: Uint8Array;
    /** resumes events() right after this event */
    cursor: string;
    /** when the message was stored */
    createdAt?: Date;
};
/** Covers only messages within the tenant's retention. */
export type StreamTopicMetadata = {
    namespace: string;
    topic: string;
    tenantId: string;
    messageCount: number;
    /** resumes events() after the newest retained message; undefined when none is retained */
    latestCursor?: string;
    /** when the newest retained message was stored; undefined when none is retained */
    lastPublishedAt?: Date;
};
export type StreamCallOptions = {
    namespace?: string;
    cursor?: string;
    /** ends an events() iteration cleanly, without throwing */
    signal?: AbortSignal;
};
/**
 * StreamsClient publishes to and reads from durable topics. Unlike
 * `runs.subscribeToStream`, topics are stored, independent of any run, and
 * readers can resume from a cursor.
 */
export declare class StreamsClient {
    private _config;
    private _client;
    private _grpc;
    private producers;
    private publishChains;
    constructor(client: HatchetClient);
    private get grpc();
    private static withRetries;
    private uploadPayload;
    private fetchPayload;
    private publishOrdered;
    /**
     * Resolves once the message is stored. A client's messages to a topic are
     * delivered in the order publish() was called.
     * @param topic - the topic to publish to
     * @param message - the message payload
     * @param options - optional namespace override
     */
    publish(topic: string, message: Uint8Array | string, options?: StreamCallOptions): Promise<void>;
    /**
     * Describes topic's retained messages. Rejects with a NOT_FOUND gRPC error
     * for a topic nothing was published to.
     * @param topic - the topic to describe
     * @param options - optional namespace override
     */
    topicMetadata(topic: string, options?: Pick<StreamCallOptions, 'namespace'>): Promise<StreamTopicMetadata>;
    /**
     * Yields topic's messages after options.cursor, or from the oldest retained.
     * If the connection drops, it resubscribes from the last message yielded, so
     * nothing is skipped or repeated. Ends when the server hangs up an idle
     * subscription, options.signal aborts, or the caller stops iterating; throws
     * on errors a retry can't fix, such as an expired cursor.
     * @param topic - the topic to read from
     * @param options - optional namespace override, resume cursor, and abort signal
     */
    events(topic: string, options?: StreamCallOptions): AsyncIterable<StreamEvent>;
}
