"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.HatchetLogger = exports.DEFAULT_LOGGER = void 0;
const logger_1 = require("../../util/logger");
const DEFAULT_LOGGER = (context, logLevel) => new HatchetLogger(context, logLevel);
exports.DEFAULT_LOGGER = DEFAULT_LOGGER;
class HatchetLogger {
    constructor(context, logLevel = 'INFO') {
        this.logLevel = logLevel;
        this.context = context;
    }
    log(level, message, color = '37') {
        var _a;
        // OFF disables all logger output, regardless of message level.
        if (this.logLevel !== 'OFF' && logger_1.LogLevelEnum[level] >= logger_1.LogLevelEnum[this.logLevel]) {
            const time = new Date().toLocaleString('en-US', {
                month: '2-digit',
                day: '2-digit',
                year: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            });
            const logFn = {
                ERROR: console.error,
                WARN: console.warn,
                INFO: console.info,
                DEBUG: console.debug,
            };
            ((_a = logFn[level]) !== null && _a !== void 0 ? _a : console.log)(`🪓 ${process.pid} | ${time} ${color && `\x1b[${color || ''}m`} [${level}/${this.context}] ${message}\x1b[0m`);
        }
    }
    debug(message) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.log('DEBUG', message, '35');
        });
    }
    info(message) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.log('INFO', message);
        });
    }
    green(message) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.log('INFO', message, '32');
        });
    }
    warn(message, error) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.log('WARN', error ? `${message} ${error}` : message, '93');
        });
    }
    error(message, error) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.log('ERROR', error ? `${message} ${error}` : message, '91');
        });
    }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    util(key, message, extra) {
        if (key === 'trace') {
            this.log('INFO', `trace: ${message}`, '35');
        }
    }
}
exports.HatchetLogger = HatchetLogger;
