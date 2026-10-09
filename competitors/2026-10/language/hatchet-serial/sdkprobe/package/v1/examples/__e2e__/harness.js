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
exports.requireEnv = requireEnv;
exports.makeE2EClient = makeE2EClient;
exports.makeTestScope = makeTestScope;
exports.startWorker = startWorker;
exports.stopWorker = stopWorker;
exports.checkDurableEvictionSupport = checkDurableEvictionSupport;
exports.poll = poll;
const sleep_1 = __importDefault(require("../../../util/sleep"));
const crypto_1 = require("crypto");
const v1_1 = require("../..");
const engine_version_1 = require("../../client/worker/engine-version");
const legacy_worker_1 = require("../../client/worker/deprecated/legacy-worker");
function requireEnv(name) {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing required environment variable ${name}. ` +
            `E2E tests require a configured Hatchet instance and credentials.`);
    }
    return value;
}
function makeE2EClient() {
    // ConfigLoader requires a token; this makes the failure message obvious.
    requireEnv('HATCHET_CLIENT_TOKEN');
    return v1_1.HatchetClient.init();
}
function makeTestScope(prefix = 'ts_e2e') {
    return `${prefix}_${(0, crypto_1.randomUUID)()}`;
}
function startWorker(_a) {
    return __awaiter(this, arguments, void 0, function* ({ client, name, workflows, slots = 50, }) {
        const worker = yield client.worker(name, { workflows, slots });
        void worker.start();
        yield worker.waitUntilReady(10000);
        return worker;
    });
}
function stopWorker(worker) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!worker) {
            return;
        }
        yield worker.stop();
        yield (0, sleep_1.default)(300);
    });
}
/**
 * Checks whether the connected engine supports durable eviction.
 * Call from beforeAll / beforeEach and skip tests when false.
 */
function checkDurableEvictionSupport(client) {
    return __awaiter(this, void 0, void 0, function* () {
        const version = yield (0, legacy_worker_1.fetchEngineVersion)(client).catch(() => undefined);
        return (0, engine_version_1.supportsEviction)(version);
    });
}
function poll(fn_1, _a) {
    return __awaiter(this, arguments, void 0, function* (fn, { timeoutMs = 30000, intervalMs = 100, shouldStop, label = 'poll', }) {
        const start = Date.now();
        while (true) {
            try {
                const value = yield fn();
                if (shouldStop(value)) {
                    return value;
                }
            }
            catch (_b) {
                // retry on transient errors
            }
            if (Date.now() - start > timeoutMs) {
                throw new Error(`Timed out waiting for ${label} after ${timeoutMs}ms`);
            }
            yield (0, sleep_1.default)(intervalMs);
        }
    });
}
