import { Logger } from './logger';
export declare const DEFAULT_RETRY_INTERVAL = 0.1;
export declare const DEFAULT_RETRY_COUNT = 8;
export declare const DEFAULT_MAX_JITTER = 100;
export interface RetrierConfig {
    maxAttempts?: number;
    initialInterval?: number;
    maxJitter?: number;
    shouldRetry?: (e: unknown) => boolean;
}
export declare function retrier<T>(fn: () => Promise<T>, logger: Logger, config?: RetrierConfig): Promise<T>;
