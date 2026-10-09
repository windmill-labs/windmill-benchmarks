import { Logger } from '../../../util/logger';
import { DispatcherClient as PbDispatcherClient } from '../../../protoc/dispatcher';
import { Worker } from 'worker_threads';
import { ClientConfig } from '../../hatchet-client';
import { DispatcherClient } from '../dispatcher-client';
import { z } from 'zod/v4';
declare const HeartbeatMessageSchema: z.ZodObject<{
    type: z.ZodEnum<{
        error: "error";
        info: "info";
        warn: "warn";
        debug: "debug";
    }>;
    message: z.ZodString;
}, z.core.$strip>;
export type HeartbeatMessage = z.infer<typeof HeartbeatMessageSchema>;
export declare const isHeartbeatMessage: (message: unknown) => message is HeartbeatMessage;
export declare const STOP_HEARTBEAT = "stop";
export declare class Heartbeat {
    config: ClientConfig;
    client: PbDispatcherClient;
    workerId: string;
    logger: Logger;
    heartbeatWorker: Worker | undefined;
    constructor(client: DispatcherClient, workerId: string);
    start(): Promise<void>;
    stop(): Promise<void>;
}
export {};
