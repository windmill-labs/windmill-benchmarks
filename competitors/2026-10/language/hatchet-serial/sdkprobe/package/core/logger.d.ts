import { Logger, LogLevel } from '../util/logger/logger';
/**
 * The logger the core client uses when none is configured: level-filtered `console` output
 * with the context in brackets. It reads nothing from the process, so it runs anywhere the
 * client does.
 */
export declare class ConsoleLogger implements Logger {
    private readonly context;
    /** The lowest level written; `OFF` sets it above every level, so nothing is. */
    private readonly threshold;
    constructor(context: string, logLevel?: LogLevel);
    private log;
    debug(message: string): void;
    info(message: string): void;
    green(message: string): void;
    warn(message: string, error?: Error): void;
    error(message: string, error?: Error): void;
}
export declare const consoleLogger: (context: string, logLevel?: LogLevel) => Logger;
