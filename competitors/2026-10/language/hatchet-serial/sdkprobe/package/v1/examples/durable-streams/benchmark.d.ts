import { HatchetClient } from '../..';
import { StreamEvent } from '../../client/features/streams';
export declare const MiB: number;
export declare const maxInlinePayloadBytes: number;
export declare const describeBenchmark: jest.Describe;
export type LatencyStats = {
    count: number;
    avgMs: number;
    p50Ms: number;
    p99Ms: number;
    maxMs: number;
};
export declare function latencyStats(durationsMs: number[]): LatencyStats;
export declare function formatLatency(stats: LatencyStats, digits?: number): string;
export declare function formatSize(bytes: number): string;
export type PublishLatencyStats = LatencyStats & {
    throughputPerSec: number;
};
/**
 * Times `count` sequential publishes, after `warmupCount` unmeasured ones
 * absorb connection setup and topic creation.
 */
export declare function measurePublishLatency(hatchet: HatchetClient, topic: string, payload: Uint8Array | string, count: number, warmupCount?: number): Promise<PublishLatencyStats>;
export declare function formatPublishLatency(stats: PublishLatencyStats): string;
export type LiveSubscription = {
    /** resolves with the next message delivered after the call */
    nextEvent: () => Promise<StreamEvent>;
    close: () => Promise<void>;
};
/**
 * Subscribes to topic and resolves once the subscription is live, so a
 * benchmark's timings don't include subscribe setup.
 */
export declare function openLiveSubscription(hatchet: HatchetClient, topic: string): Promise<LiveSubscription>;
