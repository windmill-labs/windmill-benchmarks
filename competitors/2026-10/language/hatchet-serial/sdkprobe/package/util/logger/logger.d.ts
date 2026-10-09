import { JsonObject } from '../../v1/types';
export type LogExtra = JsonObject;
export declare abstract class Logger {
    abstract debug(message: string, extra?: LogExtra): void | Promise<void>;
    abstract info(message: string, extra?: LogExtra): void | Promise<void>;
    abstract green(message: string, extra?: LogExtra): void | Promise<void>;
    abstract warn(message: string, error?: Error, extra?: LogExtra): void | Promise<void>;
    abstract error(message: string, error?: Error, extra?: LogExtra): void | Promise<void>;
    abstract util?(key: string, message: string, extra?: LogExtra): void | Promise<void>;
}
export type LogLevel = 'OFF' | 'DEBUG' | 'INFO' | 'WARN' | 'ERROR';
export declare enum LogLevelEnum {
    OFF = -1,
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3
}
