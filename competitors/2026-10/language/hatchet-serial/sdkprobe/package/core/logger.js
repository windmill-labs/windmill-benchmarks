"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.consoleLogger = exports.ConsoleLogger = void 0;
const logger_1 = require("../util/logger/logger");
/**
 * The logger the core client uses when none is configured: level-filtered `console` output
 * with the context in brackets. It reads nothing from the process, so it runs anywhere the
 * client does.
 */
class ConsoleLogger {
    constructor(context, logLevel = 'INFO') {
        this.context = context;
        this.threshold = logLevel === 'OFF' ? Number.POSITIVE_INFINITY : logger_1.LogLevelEnum[logLevel];
    }
    log(level, message, error) {
        if (logger_1.LogLevelEnum[level] < this.threshold)
            return;
        const write = level === 'ERROR'
            ? console.error
            : level === 'WARN'
                ? console.warn
                : level === 'DEBUG'
                    ? console.debug
                    : console.info;
        write(`[${level}/${this.context}] ${error ? `${message} ${error}` : message}`);
    }
    debug(message) {
        this.log('DEBUG', message);
    }
    info(message) {
        this.log('INFO', message);
    }
    green(message) {
        this.log('INFO', message);
    }
    warn(message, error) {
        this.log('WARN', message, error);
    }
    error(message, error) {
        this.log('ERROR', message, error);
    }
}
exports.ConsoleLogger = ConsoleLogger;
const consoleLogger = (context, logLevel) => new ConsoleLogger(context, logLevel);
exports.consoleLogger = consoleLogger;
