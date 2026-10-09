import { LogExtra, Logger, LogLevel } from '../../util/logger';
export declare const DEFAULT_LOGGER: (context: string, logLevel?: LogLevel) => HatchetLogger;
type UtilKeys = 'trace';
export declare class HatchetLogger implements Logger {
    private logLevel;
    private context;
    constructor(context: string, logLevel?: LogLevel);
    private log;
    debug(message: string): Promise<void>;
    info(message: string): Promise<void>;
    green(message: string): Promise<void>;
    warn(message: string, error?: Error): Promise<void>;
    error(message: string, error?: Error): Promise<void>;
    util(key: UtilKeys, message: string, extra?: LogExtra): void | Promise<void>;
}
export {};
