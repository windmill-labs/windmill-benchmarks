import { Duration } from '../../duration';
export type EvictionPolicy = {
    /**
     * Maximum continuous waiting duration before TTL-eligible eviction.
     * `undefined` means no TTL-based eviction.
     */
    ttl?: Duration;
    /**
     * Whether this task may be evicted under durable-slot pressure.
     * @default true
     */
    allowCapacityEviction?: boolean;
    /**
     * Lower values are evicted first when multiple candidates exist.
     * @default 0
     */
    priority?: number;
};
export declare const DEFAULT_DURABLE_TASK_EVICTION_POLICY: EvictionPolicy;
