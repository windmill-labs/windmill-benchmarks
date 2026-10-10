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
exports.DEFAULT_MAX_JITTER = exports.DEFAULT_RETRY_COUNT = exports.DEFAULT_RETRY_INTERVAL = void 0;
exports.retrier = retrier;
const sleep_1 = __importDefault(require("./sleep"));
exports.DEFAULT_RETRY_INTERVAL = 0.1; // seconds
exports.DEFAULT_RETRY_COUNT = 8;
exports.DEFAULT_MAX_JITTER = 100; // milliseconds
function retrier(fn, logger, config) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a, _b, _c, _d;
        const retries = Math.max(1, (_a = config === null || config === void 0 ? void 0 : config.maxAttempts) !== null && _a !== void 0 ? _a : exports.DEFAULT_RETRY_COUNT);
        const interval = (_b = config === null || config === void 0 ? void 0 : config.initialInterval) !== null && _b !== void 0 ? _b : exports.DEFAULT_RETRY_INTERVAL;
        const maxJitter = (_c = config === null || config === void 0 ? void 0 : config.maxJitter) !== null && _c !== void 0 ? _c : exports.DEFAULT_MAX_JITTER;
        const shouldRetry = (_d = config === null || config === void 0 ? void 0 : config.shouldRetry) !== null && _d !== void 0 ? _d : (() => true);
        let lastError;
        for (let i = 0; i < retries; i++) {
            try {
                return yield fn();
            }
            catch (e) {
                if (!shouldRetry(e)) {
                    throw e;
                }
                lastError = e instanceof Error ? e : new Error(String(e));
                logger.error(`Error: ${lastError.message}`);
                if (i < retries - 1) {
                    const exponentialDelay = interval * 2 ** i * 1000;
                    const jitter = Math.random() * maxJitter;
                    yield (0, sleep_1.default)(exponentialDelay + jitter);
                }
            }
        }
        throw lastError;
    });
}
