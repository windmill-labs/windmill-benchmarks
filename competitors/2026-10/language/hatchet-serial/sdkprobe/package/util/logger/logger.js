"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LogLevelEnum = exports.Logger = void 0;
class Logger {
}
exports.Logger = Logger;
var LogLevelEnum;
(function (LogLevelEnum) {
    LogLevelEnum[LogLevelEnum["OFF"] = -1] = "OFF";
    LogLevelEnum[LogLevelEnum["DEBUG"] = 0] = "DEBUG";
    LogLevelEnum[LogLevelEnum["INFO"] = 1] = "INFO";
    LogLevelEnum[LogLevelEnum["WARN"] = 2] = "WARN";
    LogLevelEnum[LogLevelEnum["ERROR"] = 3] = "ERROR";
})(LogLevelEnum || (exports.LogLevelEnum = LogLevelEnum = {}));
