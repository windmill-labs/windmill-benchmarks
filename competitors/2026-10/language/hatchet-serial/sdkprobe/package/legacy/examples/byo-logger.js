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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const pino_1 = __importDefault(require("pino"));
const sdk_1 = __importDefault(require("../../sdk"));
// > Create Pino logger
const logger = (0, pino_1.default)();
class PinoLogger {
    constructor(context, logLevel = 'DEBUG') {
        this.logLevel = logLevel;
        this.context = context;
    }
    debug(message, extra) {
        logger.debug(extra, message);
    }
    info(message, extra) {
        logger.info(extra, message);
    }
    green(message, extra) {
        logger.info(extra, `%c${message}`);
    }
    warn(message, error, extra) {
        logger.warn(extra, `${message} ${error}`);
    }
    error(message, error, extra) {
        logger.error(extra, `${message} ${error}`);
    }
    // optional util method
    util(key, message, extra) {
        // for example you may want to expose a trace method
        if (key === 'trace') {
            logger.info(extra, 'trace');
        }
    }
}
const hatchet = sdk_1.default.init({
    log_level: 'DEBUG',
    logger: (ctx, level) => new PinoLogger(ctx, level),
});
// !!
// > Use the logger
const workflow = hatchet.task({
    name: 'byo-logger-example',
    fn: (ctx) => __awaiter(void 0, void 0, void 0, function* () {
        for (let i = 0; i < 5; i++) {
            logger.info(`log message ${i}`);
        }
        return { step1: 'completed step run' };
    }),
});
// !!
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const worker = yield hatchet.worker('byo-logger-worker', {
            workflows: [workflow],
        });
        worker.start();
    });
}
main();
